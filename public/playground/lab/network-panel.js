// The Network tab: a protocol analyzer over the capture (capture.js).
//
// Three views of what crossed the virtual network since the scenario started:
//   Exchanges   every Kafka request with its response, timed in lab ms
//   Frames      every frame, including opens, closes and non-Kafka traffic
//   Statistics  per API (count, failures, bytes, RTT percentiles and a
//               histogram) and per link (bytes each way, throughput)
// Selecting a row decodes it field by field (kafka-decode.js) into the linked
// tree and hex view (bytes-view.js). The filter takes words and terms:
//   api:fetch  node:broker-1  port:9093  conn:3  corr:12  err  slow:50  pending
// and "Selected link" keeps the two nodes picked on the canvas.

import { el, button, fmtBytes, fmtNum, fmtMs, download } from "./dom.js";
import { BytesView } from "./bytes-view.js";
import { loadSchemas, decodeFrame } from "./kafka-decode.js";
import { percentile, connKey } from "./capture.js";

const ROW_H = 24;
const VIEWS = [
  { id: "exchanges", label: "Exchanges" },
  { id: "frames", label: "Frames" },
  { id: "stats", label: "Statistics" },
];

export class NetworkPanel {
  // hooks: capture, nodeName(id), selection() → [a, b], scenario() → { id, name, seed }, expand()
  constructor(container, hooks) {
    this.hooks = hooks;
    this.capture = hooks.capture;
    this.view = "exchanges";
    this.selectedId = null;
    this.follow = true;
    this.schemas = null;

    this.root = el("div", "lab-wire lab-net");
    this.root.open = true; // the old disclosure's contract: the panel is showing
    const bar = el("div", "lab-net-bar");
    this.toggleBtn = button("Pause capture", "lab-btn-sm", () => this.toggleCapture(), { title: "Stop or resume recording frames; the clock and the network keep running" });
    const clear = button("Clear", "lab-btn-sm", () => {
      this.capture.clear();
      this.selectedId = null;
      this.detail.clear("Nothing selected.");
    }, { title: "Forget every captured frame" });
    this.filter = el("input", "lab-input lab-net-filter");
    this.filter.type = "search";
    this.filter.placeholder = "Filter: api:fetch node:broker-1 port:9093 err slow:50 pending";
    this.filter.setAttribute("aria-label", "Filter the capture");
    this.filter.addEventListener("input", () => this.render());
    const linkRow = el("label", "lab-field-inline lab-net-link");
    this.linkOnly = el("input");
    this.linkOnly.type = "checkbox";
    this.linkOnly.addEventListener("change", () => this.render());
    this.linkLabel = el("span", null, "Selected link");
    linkRow.append(this.linkOnly, this.linkLabel);
    const exportBox = el("span", "lab-net-export");
    exportBox.append(
      el("span", "lab-muted lab-small", "Export"),
      button("pcapng", "lab-btn-sm", () => this.export("pcapng"), { title: "Download the filtered frames for Wireshark: raw IPv4/TCP, lab node addresses, lab milliseconds" }),
      button("JSON", "lab-btn-sm", () => this.export("json"), { title: "Download the filtered frames with their bytes, base64" }),
      button("CSV", "lab-btn-sm", () => this.export("csv"), { title: "Download one row per exchange: times, sizes, RTT, errors" }),
    );
    bar.append(this.toggleBtn, clear, this.filter, linkRow, exportBox);

    const tabs = el("div", "lab-net-views");
    this.viewBtns = new Map();
    const tablist = el("div", "lab-tabs");
    tablist.setAttribute("role", "tablist");
    tablist.setAttribute("aria-label", "Network views");
    for (const v of VIEWS) {
      const b = el("button", "lab-tab", v.label);
      b.type = "button";
      b.setAttribute("role", "tab");
      b.addEventListener("click", () => this.setView(v.id));
      tablist.appendChild(b);
      this.viewBtns.set(v.id, b);
    }
    this.status = el("span", "lab-net-status lab-muted lab-small");
    this.status.setAttribute("aria-live", "polite");
    tabs.append(tablist, this.status);

    this.main = el("div", "lab-net-main");
    this.listBox = el("div", "lab-net-listbox");
    this.head = el("div", "lab-net-row lab-net-head");
    this.list = el("div", "lab-net-list");
    this.list.tabIndex = 0;
    this.list.setAttribute("role", "listbox");
    this.list.setAttribute("aria-label", "Captured traffic");
    this.spacer = el("div", "lab-net-spacer");
    // The header rides in the scroller, so it scrolls sideways with the rows on a narrow screen.
    this.list.append(this.head, this.spacer);
    this.list.addEventListener("scroll", () => {
      this.follow = this.list.scrollTop + this.list.clientHeight >= this.list.scrollHeight - ROW_H;
      this.renderRows();
    });
    this.list.addEventListener("keydown", (e) => this.onListKey(e));
    this.listBox.appendChild(this.list);
    this.statsBox = el("div", "lab-net-stats");
    this.statsBox.hidden = true;

    this.detailBox = el("div", "lab-net-detail");
    this.detailHead = el("div", "lab-net-detail-head");
    this.sideTabs = el("div", "lab-tabs lab-net-sides");
    this.detailNote = el("div", "lab-net-problems");
    const viewHost = el("div", "lab-net-bv");
    this.detailBox.append(this.detailHead, this.sideTabs, this.detailNote, viewHost);
    this.detail = new BytesView(viewHost, { label: "Selected frame", empty: "Select an exchange or a frame to decode it field by field." });
    this.main.append(this.listBox, this.statsBox, this.detailBox);
    this.root.append(bar, tabs, this.main);
    container.appendChild(this.root);
    this.timer = null;
    this.scanning = false;
    this.setView("exchanges");
    loadSchemas().then((s) => {
      this.schemas = s;
      this.changed();
    }).catch((err) => {
      this.status.textContent = `Schemas did not load: ${err.message}`;
    });
  }

  // ---- data flow ----

  // The capture changed: re-render at most four times a second while showing.
  changed() {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      if (this.root.offsetParent !== null) this.render();
      this.scan();
    }, 250);
  }

  async scan() {
    if (this.scanning || !this.capture.scanQueue.length || !this.schemas) return;
    this.scanning = true;
    try {
      while ((await this.capture.scan(8)) > 0) await new Promise((r) => setTimeout(r, 30));
    } finally {
      this.scanning = false;
    }
  }

  // The app calls this on every snapshot; only a new selection does anything.
  update(selection) {
    const [a, b] = selection || [];
    const key = `${a}-${b}`;
    if (key === this.selKey) return;
    this.selKey = key;
    const two = a != null && b != null;
    this.linkOnly.disabled = !two;
    this.linkLabel.textContent = two ? `Only ${this.hooks.nodeName(a)} ↔ ${this.hooks.nodeName(b)}` : "Selected link only (pick two nodes)";
    if (!two) this.linkOnly.checked = false;
    this.render();
  }

  // From the fault bar's "Network bytes": this link only.
  open() {
    this.linkOnly.checked = !this.linkOnly.disabled;
    this.setView("exchanges");
  }

  refresh() {
    this.render();
  }

  toggleCapture() {
    this.capture.running = !this.capture.running;
    this.render();
  }

  setView(id) {
    this.view = id;
    for (const [k, b] of this.viewBtns) {
      b.classList.toggle("lab-tab-active", k === id);
      b.setAttribute("aria-selected", String(k === id));
    }
    this.listBox.hidden = id === "stats";
    this.statsBox.hidden = id !== "stats";
    this.detailBox.hidden = id === "stats";
    this.main.classList.toggle("lab-net-onecol", id === "stats");
    this.follow = true;
    this.render();
  }

  // ---- filtering ----

  apiName(key) {
    return this.schemas?.apiNames[key] ?? `API ${key}`;
  }

  matcher() {
    const terms = this.filter.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
    const [a, b] = this.hooks.selection() || [];
    const link = this.linkOnly.checked && a != null && b != null ? new Set([a, b]) : null;
    const name = (id) => this.hooks.nodeName(id).toLowerCase();
    // An item is an exchange or a frame; a frame answers for its exchange's API,
    // correlation, timing and errors, so a filter keeps both halves of an exchange.
    return (item) => {
      const f = item.req || item;
      const ex = item.req ? item : item.exchange;
      if (link && !(link.has(f.src.node) && link.has(f.dst.node))) return false;
      for (const t of terms) {
        const [k, v] = t.includes(":") ? t.split(/:(.*)/) : [null, t];
        const api = ex ? this.apiName(ex.apiKey).toLowerCase() : (f.label || "").toLowerCase();
        const nodes = `${name(f.src.node)} ${name(f.dst.node)}`;
        let ok;
        if (k === "api") ok = api.includes(v);
        else if (k === "node") ok = nodes.includes(v);
        else if (k === "port") ok = String(f.src.port) === v || String(f.dst.port) === v;
        else if (k === "conn") ok = String(f.conn) === v;
        else if (k === "corr") ok = ex != null && String(ex.corr) === v;
        else if (k === "slow") ok = ex?.rtt != null && ex.rtt >= Number(v);
        else if (t === "err") ok = Boolean(ex?.errors?.length || ex?.problems?.length);
        else if (t === "pending") ok = ex != null && !ex.resp;
        else ok = api.includes(t) || nodes.includes(t) || (f.label || "").toLowerCase().includes(t);
        if (!ok) return false;
      }
      return true;
    };
  }

  // ---- rendering ----

  render() {
    const c = this.capture;
    const span = c.span();
    this.toggleBtn.textContent = c.running ? "Pause capture" : "Resume capture";
    this.toggleBtn.classList.toggle("lab-net-paused", !c.running);
    const lost = [
      c.dropped && `${fmtNum(c.dropped)} dropped before the page read them`,
      c.evicted && `${fmtNum(c.evicted)} evicted to stay within ${fmtBytes(c.budget)}`,
      c.ignored && `${fmtNum(c.ignored)} not recorded while paused`,
    ].filter(Boolean);
    this.status.textContent = `${fmtNum(c.frames.length)} frames · ${fmtNum(c.exchanges.length)} exchanges · ${fmtBytes(c.bytes)}${span ? ` · ${fmtMs(span[0])}–${fmtMs(span[1])}` : ""}${lost.length ? ` · ${lost.join(" · ")}` : ""}`;
    const match = this.matcher();
    if (this.view === "stats") return this.renderStats(match);
    this.rows = (this.view === "exchanges" ? c.exchanges : c.frames).filter(match);
    this.renderHead();
    this.spacer.style.height = `${this.rows.length * ROW_H}px`;
    if (this.follow) this.list.scrollTop = this.list.scrollHeight;
    this.renderRows();
  }

  renderHead() {
    const cols = this.view === "exchanges"
      ? ["#", "sent", "client → server", "API", "corr", "req", "resp", "RTT", "status"]
      : ["#", "sent", "from → to", "conn", "kind", "bytes", "what"];
    this.head.className = `lab-net-row lab-net-head lab-net-${this.view}`;
    this.head.replaceChildren(...cols.map((c) => el("span", null, c)));
  }

  renderRows() {
    if (!this.rows) return;
    const first = Math.max(0, Math.floor(this.list.scrollTop / ROW_H) - 5);
    const last = Math.min(this.rows.length, first + Math.ceil((this.list.clientHeight || 200) / ROW_H) + 10);
    const frag = document.createDocumentFragment();
    const ep = (e) => `${this.hooks.nodeName(e.node)}${e.port ? `:${e.port}` : ""}`;
    for (let i = first; i < last; i++) {
      const it = this.rows[i];
      const row = el("div", `lab-net-row lab-net-${this.view}`);
      row.style.top = `${i * ROW_H}px`;
      row.setAttribute("role", "option");
      const id = this.view === "exchanges" ? `x${it.id}` : `f${it.seq}`;
      row.dataset.id = id;
      row.setAttribute("aria-selected", String(id === this.selectedId));
      if (this.view === "exchanges") {
        const failed = it.errors?.length;
        const status = !it.resp ? "pending" : failed ? it.errors.join(" ") : it.problems?.length ? "decode issue" : it.errors ? "ok" : "…";
        row.append(
          el("span", "lab-net-num", String(it.id)), el("span", null, fmtMs(it.req.at)),
          el("span", null, `${ep(it.client)} → ${ep(it.server)}`), el("span", "lab-net-api", `${this.apiName(it.apiKey)} v${it.version}`),
          el("span", "lab-net-num", String(it.corr)), el("span", "lab-net-num", fmtBytes(it.req.size)), el("span", "lab-net-num", it.resp ? fmtBytes(it.resp.size) : "–"),
          el("span", "lab-net-num", it.rtt != null ? `${it.rtt} ms` : "–"),
          el("span", `lab-net-st${failed || it.problems?.length ? " lab-net-bad" : !it.resp ? " lab-net-wait" : ""}`, status),
        );
      } else {
        row.append(
          el("span", "lab-net-num", String(it.seq)), el("span", null, fmtMs(it.at)), el("span", null, `${ep(it.src)} → ${ep(it.dst)}`),
          el("span", "lab-net-num", String(it.conn)), el("span", null, it.kind), el("span", "lab-net-num", it.kind === "data" ? fmtBytes(it.size) : "–"),
          el("span", null, it.role && it.exchange ? `${this.apiName(it.exchange.apiKey)} ${it.role}` : it.label),
        );
      }
      row.addEventListener("click", () => this.select(it));
      frag.appendChild(row);
    }
    this.spacer.replaceChildren(frag);
  }

  onListKey(e) {
    if (!this.rows?.length || !["ArrowDown", "ArrowUp", "Home", "End"].includes(e.key)) return;
    e.preventDefault();
    const idOf = (it) => (this.view === "exchanges" ? `x${it.id}` : `f${it.seq}`);
    let i = this.rows.findIndex((it) => idOf(it) === this.selectedId);
    if (e.key === "ArrowDown") i = Math.min(this.rows.length - 1, i + 1);
    else if (e.key === "ArrowUp") i = Math.max(0, i - 1);
    else if (e.key === "Home") i = 0;
    else i = this.rows.length - 1;
    const top = i * ROW_H;
    if (top < this.list.scrollTop || top > this.list.scrollTop + this.list.clientHeight - ROW_H) this.list.scrollTop = top - this.list.clientHeight / 2;
    this.select(this.rows[i]);
  }

  // ---- detail ----

  async select(item) {
    this.follow = false;
    this.decodeGen = (this.decodeGen || 0) + 1;
    const isEx = item.apiKey != null && item.req;
    this.selectedId = isEx ? `x${item.id}` : `f${item.seq}`;
    this.renderRows();
    this.hooks.expand?.();
    const ep = (e) => `${this.hooks.nodeName(e.node)}${e.port ? `:${e.port}` : ""}`;
    this.detailHead.replaceChildren();
    this.sideTabs.replaceChildren();
    this.detailNote.replaceChildren();
    if (isEx) {
      const ex = item;
      this.detailHead.append(
        el("strong", null, `${this.apiName(ex.apiKey)} v${ex.version}`),
        el("span", null, ` · ${ep(ex.client)} → ${ep(ex.server)} · connection ${connKey(ex.req)} · correlation ${ex.corr}`),
        this.timing(ex),
      );
      for (const [which, label] of [["req", `Request · ${fmtBytes(ex.req.size)}`], ["resp", ex.resp ? `Response · ${fmtBytes(ex.resp.size)}` : "Response · pending"]]) {
        const b = el("button", "lab-tab", label);
        b.type = "button";
        b.dataset.side = which;
        b.disabled = which === "resp" && !ex.resp;
        b.addEventListener("click", () => this.decodeInto(which === "req" ? ex.req : ex.resp, which === "req", ex));
        this.sideTabs.appendChild(b);
      }
      const side = ex.resp && this.lastSide === "resp" ? ex.resp : ex.req;
      await this.decodeInto(side, side === ex.req, ex);
    } else {
      const f = item;
      this.detailHead.append(el("strong", null, `Frame ${f.seq} · ${f.kind}`), el("span", null, ` · ${ep(f.src)} → ${ep(f.dst)} · sent ${fmtMs(f.at)}, delivered ${fmtMs(f.deliverAt)} · connection ${connKey(f)}`));
      if (f.exchange) this.detailHead.appendChild(this.timing(f.exchange));
      if (f.bytes && f.role && f.exchange) await this.decodeInto(f, f.role === "request", f.exchange);
      else if (f.bytes) this.detail.show({ buffers: { main: f.bytes }, root: { label: f.label, start: 0, end: f.bytes.length, kind: "bytes", value: `${f.size} bytes, not a Kafka frame` }, captured: f.bytes.length, size: f.size });
      else this.detail.clear(`A TCP ${f.kind}: no payload bytes.`);
    }
  }

  async decodeInto(f, request, ex) {
    this.lastSide = request ? "req" : "resp";
    for (const b of this.sideTabs.children) {
      b.classList.toggle("lab-tab-active", b.dataset.side === this.lastSide);
      b.setAttribute("aria-pressed", String(b.dataset.side === this.lastSide));
    }
    // A slow decode (a big compressed batch) must not overwrite a later selection.
    const gen = (this.decodeGen = (this.decodeGen || 0) + 1);
    const d = await decodeFrame(f.bytes, { size: f.size, request, answers: ex });
    if (gen !== this.decodeGen) return;
    const buffers = { main: f.bytes };
    for (const [k, v] of d.buffers || []) buffers[k] = v;
    this.detail.show({ buffers, root: d.root, captured: f.bytes.length, size: f.size });
    this.detailNote.replaceChildren();
    const notes = [...d.errors.map((e) => `error ${e.code} ${e.name}`), ...d.problems, ...(d.root.note ? [d.root.note] : [])];
    for (const n of notes) this.detailNote.appendChild(el("span", "lab-net-problem", n));
  }

  // A to-scale bar: the request on the link, the server, the response on the link.
  timing(ex) {
    const box = el("div", "lab-net-timing");
    if (!ex.resp) {
      box.textContent = `sent ${fmtMs(ex.req.at)}, delivered ${fmtMs(ex.req.deliverAt)}; no response captured`;
      return box;
    }
    // One side runs on another peer, whose clock is not this page's: only this side's times are known.
    if (ex.rtt == null || ex.serverMs == null) {
      box.textContent = ex.rtt != null
        ? `RTT ${ex.rtt} ms = ${ex.req.deliverAt - ex.req.at} ms request + ${ex.rtt - (ex.req.deliverAt - ex.req.at)} ms on the server's peer (server and response link, not split: another clock)`
        : `${ex.serverMs} ms server + ${ex.resp.deliverAt - ex.resp.at} ms response; the request came from another peer, so its link time and the RTT are unknown`;
      return box;
    }
    const up = ex.req.deliverAt - ex.req.at;
    const server = ex.serverMs;
    const down = ex.resp.deliverAt - ex.resp.at;
    const total = Math.max(1, up + server + down);
    const bar = el("div", "lab-net-tbar");
    bar.setAttribute("role", "img");
    bar.setAttribute("aria-label", `request ${up} ms, server ${server} ms, response ${down} ms`);
    for (const [cls, ms, label] of [["up", up, "request on the link"], ["srv", server, "server"], ["down", down, "response on the link"]]) {
      const seg = el("span", `lab-net-t-${cls}`);
      seg.style.flexGrow = String(Math.max(ms, total * 0.02));
      seg.title = `${label}: ${ms} ms`;
      bar.appendChild(seg);
    }
    box.append(bar, el("span", "lab-small", `RTT ${ex.rtt} ms = ${up} ms request + ${server} ms server + ${down} ms response`));
    return box;
  }

  // ---- statistics ----

  renderStats(match) {
    const c = this.capture;
    const exchanges = c.exchanges.filter(match);
    const frames = c.frames.filter(match);
    const { byApi, byLink } = c.stats(exchanges, frames, (k) => this.apiName(k));
    this.statsBox.replaceChildren();
    if (!byApi.length) this.statsBox.appendChild(el("p", "lab-muted", "No Kafka exchanges match. Run the scenario or loosen the filter."));
    const wrap = (table) => {
      const w = el("div", "lab-table-wrap");
      w.appendChild(table);
      return w;
    };
    const api = el("table", "lab-table lab-net-stat");
    api.appendChild(el("caption", null, "Per API: counts and bytes; round trip and server time in lab milliseconds. Select a row for its distribution."));
    const head = el("tr");
    for (const h of ["API", "requests", "answered", "failed", "req bytes", "resp bytes", "RTT p50", "p90", "p99", "max", "server p50"]) head.appendChild(el("th", null, h));
    api.appendChild(head);
    const hist = el("div", "lab-net-hist");
    for (const s of byApi) {
      const tr = el("tr", "lab-net-stat-row");
      const ms = (v) => (v == null ? "–" : String(v));
      for (const v of [s.name, fmtNum(s.count), fmtNum(s.answered), fmtNum(s.failed), fmtBytes(s.reqBytes), fmtBytes(s.respBytes), ms(percentile(s.rtts, 50)), ms(percentile(s.rtts, 90)), ms(percentile(s.rtts, 99)), ms(s.rtts[s.rtts.length - 1]), ms(percentile(s.servers, 50))]) tr.appendChild(el("td", null, v));
      tr.tabIndex = 0;
      const pick = () => {
        this.statApi = s.apiKey;
        this.histogram(s, hist);
      };
      tr.addEventListener("click", pick);
      tr.addEventListener("keydown", (e) => e.key === "Enter" && pick());
      api.appendChild(tr);
    }
    const chosen = byApi.find((s) => s.apiKey === this.statApi) || byApi[0];
    if (chosen) this.histogram(chosen, hist);
    const links = el("table", "lab-table lab-net-stat");
    links.appendChild(el("caption", null, "Per link: bytes each way, mean and peak throughput over lab seconds"));
    const lh = el("tr");
    for (const h of ["link (A ↔ B)", "frames", "A → B", "B → A", "active", "mean", "peak second", "bytes per second"]) lh.appendChild(el("th", null, h));
    links.appendChild(lh);
    for (const s of byLink) {
      const tr = el("tr");
      const secs = Math.max(1, (s.last - s.first) / 1000);
      const peak = Math.max(...s.buckets.values());
      tr.append(
        el("td", null, `${this.hooks.nodeName(s.a)} ↔ ${this.hooks.nodeName(s.b)}`), el("td", null, fmtNum(s.frames)),
        el("td", null, fmtBytes(s.ab)), el("td", null, fmtBytes(s.ba)), el("td", null, fmtMs(s.last - s.first)),
        el("td", null, `${fmtBytes(Math.round((s.ab + s.ba) / secs))}/s`), el("td", null, `${fmtBytes(peak)}/s`),
      );
      const td = el("td");
      td.appendChild(sparkline(s.buckets));
      tr.appendChild(td);
      links.appendChild(tr);
    }
    this.statsBox.append(wrap(api), hist, wrap(links));
  }

  histogram(s, box) {
    box.replaceChildren(el("strong", null, `${s.name}: round trip of ${fmtNum(s.rtts.length)} answered requests`));
    if (!s.rtts.length) return;
    const max = s.rtts[s.rtts.length - 1];
    const bins = Math.min(24, Math.max(4, Math.ceil(Math.sqrt(s.rtts.length))));
    const width = Math.max(1, Math.ceil((max + 1) / bins));
    const counts = new Array(bins).fill(0);
    for (const v of s.rtts) counts[Math.min(bins - 1, Math.floor(v / width))]++;
    const peak = Math.max(...counts);
    const chart = el("div", "lab-net-histbars");
    chart.setAttribute("role", "img");
    chart.setAttribute("aria-label", `RTT histogram: ${counts.map((n, i) => `${i * width}–${(i + 1) * width} ms ${n}`).join(", ")}`);
    counts.forEach((n, i) => {
      const bar = el("span", "lab-net-histbar");
      bar.style.height = `${(n / peak) * 100}%`;
      bar.title = `${i * width}–${(i + 1) * width} ms: ${n}`;
      chart.appendChild(bar);
    });
    box.append(chart, el("span", "lab-muted lab-small", `0 to ${bins * width} ms in ${bins} bins of ${width} ms · p50 ${percentile(s.rtts, 50)} ms · p99 ${percentile(s.rtts, 99)} ms · max ${max} ms`));
  }

  // ---- export ----

  export(kind) {
    const match = this.matcher();
    const frames = this.capture.frames.filter(match);
    const meta = this.hooks.scenario();
    const stamp = `${(meta.name || "scenario").replace(/[^\w-]+/g, "-")}-${Date.now()}`;
    if (kind === "pcapng") {
      const comment = `krabka Cluster Lab capture of "${meta.name}" (scenario ${meta.id}, seed ${meta.seed}). Timestamps are lab milliseconds since the scenario started; addresses are the lab's 10.0.x.y; payloads are cut at the 64 KiB the capture keeps.`;
      download(`${stamp}.pcapng`, this.capture.toPcapng(frames, comment), "application/vnd.tcpdump.pcap");
    } else if (kind === "json") download(`${stamp}.capture.json`, this.capture.toJson(frames, { scenario: meta }));
    else download(`${stamp}.exchanges.csv`, this.capture.toCsv(this.capture.exchanges.filter(match), (k) => this.apiName(k), (id) => this.hooks.nodeName(id)), "text/csv");
  }
}

function sparkline(buckets) {
  const svgNs = "http://www.w3.org/2000/svg";
  const s = document.createElementNS(svgNs, "svg");
  s.setAttribute("class", "lab-net-spark");
  s.setAttribute("viewBox", "0 0 100 20");
  s.setAttribute("preserveAspectRatio", "none");
  s.setAttribute("role", "img");
  const keys = [...buckets.keys()];
  if (!keys.length) return s;
  const lo = Math.min(...keys);
  const hi = Math.max(...keys);
  const peak = Math.max(...buckets.values());
  const pts = [];
  for (let k = lo; k <= hi; k++) pts.push(`${((k - lo) / Math.max(1, hi - lo)) * 100},${20 - ((buckets.get(k) || 0) / peak) * 18}`);
  const line = document.createElementNS(svgNs, "polyline");
  line.setAttribute("points", pts.join(" "));
  s.appendChild(line);
  s.setAttribute("aria-label", `bytes per second over ${hi - lo + 1} s, peak ${peak}`);
  return s;
}
