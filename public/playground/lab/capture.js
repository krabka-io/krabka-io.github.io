// The network capture: every frame the world sends, kept in this tab.
//
// The world records each frame when it is sent, with the time the link will
// deliver it and up to 64 KiB of its payload (`World::drain_wire`). The page
// drains it every animation frame into this store, which pairs each Kafka
// request with its response by connection and correlation id and times the
// exchange in lab milliseconds:
//
//   rtt     request sent by the client → response delivered to it
//   server  request delivered to the server → response sent by it
//   network rtt − server: the two link traversals and any queueing
//
// The store keeps frames up to a byte budget and evicts the oldest. It says
// what it lost: `dropped` frames the world's bounded buffer overwrote before
// the page read them, `evicted` frames this store let go.
//
// Exports: pcapng (raw IPv4/TCP, timestamps in lab milliseconds, so
// Wireshark's Kafka dissector opens it), JSON (every frame, base64), CSV (one
// row per exchange).

import { nodeIp } from "./external.js";
import { KAFKA_PORTS, peekRequest, peekCorrelation, decodeFrame } from "./kafka-decode.js";

export const DEFAULT_BUDGET = 64 * 1024 * 1024;

function b64bytes(text) {
  if (!text) return null;
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// A connection is its client end (the port-0 endpoint) and the id that client gave it:
// two clients can both have a connection 7 to one broker.
export function clientOf(f) {
  return f.src.port === 0 ? f.src : f.dst;
}
export function connKey(f) {
  const c = clientOf(f);
  return `${c.node}/${f.conn}`;
}

export function percentile(sorted, p) {
  if (!sorted.length) return null;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[i];
}

export class Capture {
  constructor({ budget = DEFAULT_BUDGET, onChange = () => {} } = {}) {
    this.budget = budget;
    this.onChange = onChange;
    this.running = true;
    this.clear();
  }

  clear() {
    this.frames = [];
    this.exchanges = [];
    this.pending = new Map();
    this.byConn = new Map();
    this.bytes = 0;
    this.seq = 0;
    this.dropped = 0;
    this.evicted = 0;
    this.ignored = 0;
    this.scanQueue = [];
    this.version = (this.version || 0) + 1;
    this.onChange();
  }

  // Frames from `World::drain_wire`, in send order.
  add(raw, dropped = 0) {
    this.dropped += dropped;
    if (!this.running) {
      this.ignored += raw.length;
      return;
    }
    for (const r of raw) {
      const bytes = r.kind === "data" ? b64bytes(r.bytes) : null;
      const f = { seq: ++this.seq, at: r.at, deliverAt: r.deliver_at, src: r.src, dst: r.dst, conn: r.conn, kind: r.kind, size: r.size, label: r.label, bytes, role: null, exchange: null };
      this.frames.push(f);
      this.bytes += bytes ? bytes.length : 0;
      const key = connKey(r);
      f.key = key;
      if (r.kind === "open") this.byConn.set(key, { client: r.src, server: r.dst, opened: r.at, closed: null, frames: 0, bytes: [0, 0] });
      const conn = this.byConn.get(key);
      if (conn) {
        conn.frames++;
        if (r.kind === "close") conn.closed = r.at;
        if (bytes) conn.bytes[r.src.node === conn.client.node && r.src.port === conn.client.port ? 0 : 1] += r.size;
      }
      if (bytes) this.pair(f);
    }
    while (this.bytes > this.budget && this.frames.length > 1) this.evictOldest();
    this.version++;
    this.onChange();
  }

  pair(f) {
    if (KAFKA_PORTS.has(f.dst.port)) {
      const peek = peekRequest(f.bytes);
      if (!peek) return;
      f.role = "request";
      const ex = { id: this.exchanges.length ? this.exchanges[this.exchanges.length - 1].id + 1 : 1, conn: f.conn, client: f.src, server: f.dst, apiKey: peek.apiKey, version: peek.version, corr: peek.corr, req: f, resp: null, rtt: null, serverMs: null, errors: null, problems: null };
      f.exchange = ex;
      this.exchanges.push(ex);
      this.pending.set(`${f.key}:${peek.corr}`, ex);
    } else if (KAFKA_PORTS.has(f.src.port)) {
      f.role = "response";
      const corr = peekCorrelation(f.bytes);
      const key = `${f.key}:${corr}`;
      const ex = this.pending.get(key);
      if (!ex) return;
      this.pending.delete(key);
      ex.resp = f;
      ex.rtt = f.deliverAt - ex.req.at;
      ex.serverMs = f.at - ex.req.deliverAt;
      f.exchange = ex;
      this.scanQueue.push(ex);
    }
  }

  evictOldest() {
    const f = this.frames.shift();
    this.evicted++;
    this.bytes -= f.bytes ? f.bytes.length : 0;
    // An exchange goes with its request; a response alone cannot be decoded.
    if (f.role === "request" && this.exchanges[0]?.req === f) {
      const ex = this.exchanges.shift();
      if (!ex.resp) this.pending.delete(`${ex.req.key}:${ex.corr}`);
    }
  }

  // Decode queued responses for their error codes, a few milliseconds at a
  // time, so the list can flag failed exchanges without decoding on demand.
  async scan(budgetMs = 8) {
    const until = performance.now() + budgetMs;
    let changed = false;
    while (this.scanQueue.length && performance.now() < until) {
      const ex = this.scanQueue.shift();
      if (!ex.resp?.bytes) continue;
      try {
        const d = await decodeFrame(ex.resp.bytes, { size: ex.resp.size, request: false, answers: ex });
        ex.errors = [...new Set(d.errors.map((e) => e.name))];
        ex.problems = d.problems;
      } catch (err) {
        ex.problems = [err.message];
      }
      changed = true;
    }
    if (changed) {
      this.version++;
      this.onChange();
    }
    return this.scanQueue.length;
  }

  span() {
    if (!this.frames.length) return null;
    return [this.frames[0].at, this.frames[this.frames.length - 1].at];
  }

  // Counts, bytes and RTT percentiles per API, and traffic per link.
  stats(exchanges, frames, apiName) {
    const byApi = new Map();
    for (const ex of exchanges) {
      let s = byApi.get(ex.apiKey);
      if (!s) byApi.set(ex.apiKey, (s = { apiKey: ex.apiKey, name: apiName(ex.apiKey), count: 0, answered: 0, failed: 0, reqBytes: 0, respBytes: 0, rtts: [], servers: [] }));
      s.count++;
      s.reqBytes += ex.req.size;
      if (ex.resp) {
        s.answered++;
        s.respBytes += ex.resp.size;
        s.rtts.push(ex.rtt);
        s.servers.push(ex.serverMs);
      }
      if (ex.errors?.length) s.failed++;
    }
    for (const s of byApi.values()) {
      s.rtts.sort((a, b) => a - b);
      s.servers.sort((a, b) => a - b);
    }
    const byLink = new Map();
    for (const f of frames) {
      const [a, b] = f.src.node <= f.dst.node ? [f.src.node, f.dst.node] : [f.dst.node, f.src.node];
      const key = `${a}-${b}`;
      let s = byLink.get(key);
      if (!s) byLink.set(key, (s = { a, b, frames: 0, ab: 0, ba: 0, first: f.at, last: f.at, buckets: new Map() }));
      s.frames++;
      if (f.src.node === a) s.ab += f.size;
      else s.ba += f.size;
      s.last = f.at;
      const bucket = Math.floor(f.at / 1000);
      s.buckets.set(bucket, (s.buckets.get(bucket) || 0) + f.size);
    }
    return { byApi: [...byApi.values()].sort((x, y) => y.count - x.count), byLink: [...byLink.values()].sort((x, y) => y.ab + y.ba - (x.ab + x.ba)) };
  }

  // ---- exports ----

  toJson(frames, meta) {
    const enc = (bytes) => {
      if (!bytes) return null;
      let bin = "";
      for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
      return btoa(bin);
    };
    return JSON.stringify({
      format: "krabka-cluster-lab-capture/1",
      ...meta,
      timeUnit: "lab milliseconds since the scenario started",
      dropped: this.dropped,
      evicted: this.evicted,
      frames: frames.map((f) => ({ at: f.at, deliverAt: f.deliverAt, src: f.src, dst: f.dst, conn: f.conn, kind: f.kind, size: f.size, captured: f.bytes ? f.bytes.length : 0, label: f.label, bytes: enc(f.bytes) })),
    });
  }

  toCsv(exchanges, apiName, nodeName) {
    const q = (s) => `"${String(s ?? "").replace(/"/g, '""')}"`;
    const rows = [["request_sent_ms", "client", "server", "connection", "api", "version", "correlation", "request_bytes", "response_bytes", "rtt_ms", "server_ms", "network_ms", "errors"].join(",")];
    for (const ex of exchanges) {
      rows.push([
        ex.req.at, q(`${nodeName(ex.client.node)}:${ex.client.port}`), q(`${nodeName(ex.server.node)}:${ex.server.port}`), ex.conn,
        q(apiName(ex.apiKey)), ex.version, ex.corr, ex.req.size, ex.resp?.size ?? "", ex.rtt ?? "", ex.serverMs ?? "", ex.rtt != null ? ex.rtt - ex.serverMs : "",
        q((ex.errors || []).join(" ")),
      ].join(","));
    }
    return rows.join("\n");
  }

  // pcapng with raw IPv4 packets (LINKTYPE_RAW) and a millisecond clock: the
  // nodes' lab addresses and ports, a TCP stream per connection with
  // consistent sequence numbers, payloads cut at the 64 KiB the capture kept.
  toPcapng(frames, comment) {
    const blocks = [];
    const te = new TextEncoder();
    const option = (code, bytes) => {
      const pad = (4 - (bytes.length % 4)) % 4;
      const out = new Uint8Array(4 + bytes.length + pad);
      const v = new DataView(out.buffer);
      v.setUint16(0, code, true);
      v.setUint16(2, bytes.length, true);
      out.set(bytes, 4);
      return out;
    };
    const block = (type, body) => {
      const len = 12 + body.length;
      const out = new Uint8Array(len);
      const v = new DataView(out.buffer);
      v.setUint32(0, type, true);
      v.setUint32(4, len, true);
      out.set(body, 8);
      v.setUint32(len - 4, len, true);
      blocks.push(out);
    };
    const concat = (parts) => {
      const n = parts.reduce((s, p) => s + p.length, 0);
      const out = new Uint8Array(n);
      let o = 0;
      for (const p of parts) {
        out.set(p, o);
        o += p.length;
      }
      return out;
    };
    // Section header: byte-order magic, version 1.0, unknown section length.
    const shb = new Uint8Array(16);
    const sv = new DataView(shb.buffer);
    sv.setUint32(0, 0x1a2b3c4d, true);
    sv.setUint16(4, 1, true);
    sv.setBigInt64(8, -1n, true);
    block(0x0a0d0d0a, concat([shb, option(4, te.encode("krabka Cluster Lab")), option(1, te.encode(comment)), new Uint8Array(4)]));
    // Interface: raw IP, no snap limit, timestamps in 10^-3 s.
    const idb = new Uint8Array(8);
    new DataView(idb.buffer).setUint16(0, 101, true);
    block(0x00000001, concat([idb, option(2, te.encode("lab")), option(9, Uint8Array.of(3)), new Uint8Array(4)]));

    const ip = (id) => nodeIp(id).split(".").map(Number);
    // A client end has port 0 in the lab; TCP needs a real one, so it gets an
    // ephemeral port from its connection id.
    const port = (ep, f) => (ep.port === 0 ? 32768 + (f.conn % 28232) : ep.port);
    const seqs = new Map();
    const seqOf = (f) => {
      const key = `${connKey(f)}>${f.src.node}:${f.src.port}`;
      if (!seqs.has(key)) seqs.set(key, 1000);
      return key;
    };
    let ipId = 0;
    const packet = (f, flags, payload, payloadLen, at) => {
      const key = seqOf(f);
      const rev = `${connKey(f)}>${f.dst.node}:${f.dst.port}`;
      const seq = seqs.get(key);
      const ack = seqs.get(rev) ?? 0;
      const captured = payload ? payload.length : 0;
      const head = new Uint8Array(40 + captured);
      const v = new DataView(head.buffer);
      const total = 40 + payloadLen;
      v.setUint8(0, 0x45);
      v.setUint16(2, total);
      v.setUint16(4, ipId++ & 0xffff);
      v.setUint16(6, 0x4000);
      v.setUint8(8, 64);
      v.setUint8(9, 6);
      head.set(ip(f.src.node), 12);
      head.set(ip(f.dst.node), 16);
      let sum = 0;
      for (let i = 0; i < 20; i += 2) sum += v.getUint16(i);
      while (sum > 0xffff) sum = (sum & 0xffff) + (sum >>> 16);
      v.setUint16(10, ~sum & 0xffff);
      v.setUint16(20, port(f.src, f));
      v.setUint16(22, port(f.dst, f));
      v.setUint32(24, seq >>> 0);
      v.setUint32(28, ack >>> 0);
      v.setUint8(32, 5 << 4);
      v.setUint8(33, flags);
      v.setUint16(34, 0xffff);
      if (payload) head.set(payload, 40);
      // The TCP checksum over the pseudo-header, header and payload. A
      // payload the capture cut short cannot be summed; it keeps 0, and the
      // packet's captured length tells tools it is truncated.
      if (captured === payloadLen) {
        let t = 6 + 20 + payloadLen;
        for (let i = 12; i < 20; i += 2) t += v.getUint16(i);
        for (let i = 20; i < head.length - 1; i += 2) t += v.getUint16(i);
        if (head.length % 2) t += head[head.length - 1] << 8;
        while (t > 0xffff) t = (t & 0xffff) + (t >>> 16);
        v.setUint16(36, ~t & 0xffff || 0xffff);
      }
      seqs.set(key, (seq + payloadLen + (flags & 0x03 ? 1 : 0)) >>> 0);
      const epb = new Uint8Array(20);
      const ev = new DataView(epb.buffer);
      ev.setUint32(4, Math.floor(at / 2 ** 32), true);
      ev.setUint32(8, at >>> 0, true);
      ev.setUint32(12, head.length, true);
      ev.setUint32(16, total, true);
      const pad = new Uint8Array((4 - (head.length % 4)) % 4);
      block(0x00000006, concat([epb, head, pad]));
    };
    const MSS = 65495;
    for (const f of frames) {
      if (f.kind === "open") packet(f, 0x02, null, 0, f.at);
      else if (f.kind === "close") packet(f, 0x11, null, 0, f.at);
      else {
        for (let off = 0; off < Math.max(1, f.size); off += MSS) {
          const len = Math.min(MSS, f.size - off);
          const part = f.bytes && off < f.bytes.length ? f.bytes.subarray(off, Math.min(f.bytes.length, off + len)) : null;
          packet(f, 0x18, part, len, f.at);
        }
      }
    }
    return concat(blocks);
  }
}
