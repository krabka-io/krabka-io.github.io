// Checks the Cluster Lab's analyzers without a browser: the codecs, the Kafka
// decoder, the capture's pairing and exports, and the disk-format checks.
//
// The fixture (scripts/fixtures/lab-analyzer.json) holds real frames and log
// files from a krabka-broker run in the lab: one exchange of every API and
// version the three-broker preset uses, on both listeners, and the files of
// a few partitions and the KRaft metadata log.
//
// Usage: npm run check-lab-analyzer

import assert from 'node:assert/strict';
import fs from 'node:fs';
import zlib from 'node:zlib';
import { crc32c, decompress, snappy, lz4 } from '../public/playground/lab/codecs.js';
import { setSchemas, loadSchemas, decodeFrame, decodeBatches, Reader, inVersion } from '../public/playground/lab/kafka-decode.js';
import { Capture, connKey } from '../public/playground/lab/capture.js';
import { analyzeFile, analyzePartition, partitionsOf } from '../public/playground/lab/disk.js';

let passed = 0;
const check = (name, fn) => {
  try {
    fn();
    passed++;
  } catch (err) {
    console.error(`  FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
};
const checkAsync = async (name, fn) => {
  try {
    await fn();
    passed++;
  } catch (err) {
    console.error(`  FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
};
const fromB64 = (s) => Uint8Array.from(Buffer.from(s, 'base64'));
// node:zlib compresses zstd from Node 22.15; on older Nodes the zstd checks are skipped, not failed.
const checkZstd = typeof zlib.zstdCompressSync === 'function'
  ? checkAsync
  : async (name) => console.log(`  skip ${name}: Node ${process.version} has no zlib zstd (22.15+)`);
const walk = (n, f) => {
  f(n);
  for (const c of n.children || []) walk(c, f);
};

const schemas = JSON.parse(fs.readFileSync('public/playground/lab/kafka-schemas.json', 'utf8'));
setSchemas(schemas);
const fixture = JSON.parse(fs.readFileSync('scripts/fixtures/lab-analyzer.json', 'utf8'));

check('the schemas record the revisions they came from', () => {
  for (const repo of ['krabka-protocol', 'krabka-broker']) assert.match(schemas.sources[repo], /^[0-9a-f]{40}$/, repo);
  assert.equal(schemas.apiNames[0], 'Produce');
  assert.equal(schemas.metadata[2], 'TopicRecord');
  assert.equal(schemas.errors[35], 'UNSUPPORTED_VERSION');
});

// ---- codecs ----
check('CRC-32C matches the Castagnoli check value', () => assert.equal(crc32c(new TextEncoder().encode('123456789')), 0xe3069283));
check('snappy: a literal then an overlapping copy', () => {
  // 17 bytes: literal "hello " then copy 11 bytes from 6 back.
  const block = Uint8Array.of(17, 0x14, ...new TextEncoder().encode('hello '), 0x1d, 6);
  assert.equal(new TextDecoder().decode(snappy(block)), 'hello hello hello');
});
check('lz4: a frame with a compressed and a stored block', () => {
  const compressed = Uint8Array.of(0x35, 0x61, 0x62, 0x63, 3, 0); // "abc", then 9 bytes 3 back
  const stored = new TextEncoder().encode('xyz');
  const frame = Uint8Array.of(0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82, compressed.length, 0, 0, 0, ...compressed, stored.length, 0, 0, 0x80, ...stored, 0, 0, 0, 0);
  assert.equal(new TextDecoder().decode(lz4(frame)), 'abcabcabcabcxyz');
});
await checkAsync('gzip opens what zlib wrote', async () => {
  const out = await decompress(1, Uint8Array.from(zlib.gzipSync('records inside a batch')));
  assert.equal(new TextDecoder().decode(out), 'records inside a batch');
});
await checkZstd('zstd opens what libzstd wrote, at every strategy', async () => {
  const P = zlib.constants;
  let seed = 1;
  const rnd = () => (seed = (seed * 1103515245 + 12345) >>> 0) / 2 ** 32;
  const text = Buffer.from(Array.from({ length: 6000 }, (_, i) => `{"order":${i},"sku":"k${Math.floor(rnd() * 90)}","qty":${Math.floor(rnd() * 9)}}`).join('\n'));
  const noise = Buffer.from(Array.from({ length: 70000 }, () => Math.floor(rnd() * 256)));
  for (const input of [Buffer.alloc(0), Buffer.from('a'), Buffer.alloc(300000, 7), noise, text, Buffer.concat([text, noise, text])]) {
    for (const params of [{}, { [P.ZSTD_c_compressionLevel]: 1 }, { [P.ZSTD_c_compressionLevel]: 19 }, { [P.ZSTD_c_strategy]: 9, [P.ZSTD_c_windowLog]: 10 }, { [P.ZSTD_c_enableLongDistanceMatching]: 1 }]) {
      const out = await decompress(4, Uint8Array.from(zlib.zstdCompressSync(input, { params })));
      assert.ok(Buffer.from(out).equals(input), `${input.length} bytes, ${JSON.stringify(params)}`);
    }
  }
  // Two frames with a skippable frame between them read as one stream.
  const skippable = Uint8Array.of(0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3);
  const two = Buffer.concat([zlib.zstdCompressSync('first '), skippable, zlib.zstdCompressSync('second')]);
  assert.equal(new TextDecoder().decode(await decompress(4, Uint8Array.from(two))), 'first second');
  await assert.rejects(decompress(4, Uint8Array.from(zlib.zstdCompressSync(text)).subarray(0, 200)), /zstd/);
});
check('varints and version ranges', () => {
  assert.equal(new Reader(Uint8Array.of(0x80, 0x01)).uvarint(), 128);
  assert.equal(new Reader(Uint8Array.of(0x01)).varint(), -1);
  assert.equal(new Reader(Uint8Array.of(0x02)).varint(), 1);
  assert.equal(new Reader(Uint8Array.of(0xfe, 0xff, 0xff, 0xff, 0x1f)).varlong(), 4294967295n);
  assert.ok(inVersion('3+', 9) && !inVersion('3+', 2) && inVersion('3-7', 7) && !inVersion('3-7', 8) && inVersion('5', 5) && !inVersion('none', 0));
});

// ---- the capture ----
const capture = new Capture();
capture.add(fixture.frames);
check('SSPI records stay in the capture without pairing as Kafka', () => {
  const c = new Capture();
  const [frame] = fixture.frames;
  c.add([1, 2, 3].map((tag) => ({ ...frame, bytes: Buffer.from([83, 83, 80, 73, tag, ...Buffer.alloc(24)]).toString('base64') })));
  assert.equal(c.frames.length, 3);
  assert.equal(c.exchanges.length, 0);
});
check('every fixture request pairs with its response', () => {
  assert.equal(capture.exchanges.length, fixture.frames.length / 2);
  assert.ok(capture.exchanges.every((ex) => ex.resp && ex.rtt >= ex.serverMs && ex.serverMs >= 0), 'timing');
});
check('two clients may both have connection 7 to one broker', () => {
  const [ex] = capture.exchanges;
  const c = new Capture();
  const twin = (client, corr) => {
    const bytes = Buffer.from(ex.req.bytes);
    bytes.writeInt32BE(corr, 8);
    return { at: 10, deliver_at: 15, src: { node: client, port: 0 }, dst: ex.server, conn: 7, kind: 'data', size: bytes.length, label: 'request', bytes: bytes.toString('base64') };
  };
  const reply = (client, corr) => {
    const bytes = Buffer.from(ex.resp.bytes);
    bytes.writeInt32BE(corr, 4);
    return { at: 50, deliver_at: 55, src: ex.server, dst: { node: client, port: 0 }, conn: 7, kind: 'data', size: bytes.length, label: 'response', bytes: bytes.toString('base64') };
  };
  c.add([twin(40, 1), twin(41, 1), reply(41, 1), reply(40, 1)]);
  assert.equal(c.exchanges.length, 2);
  assert.ok(c.exchanges.every((x) => x.resp && x.resp.dst.node === x.client.node), 'each response went to its own client');
  assert.notEqual(connKey(c.exchanges[0].req), connKey(c.exchanges[1].req));
});
check('a peer on another clock leaves its side of the timing unknown', () => {
  const [ex] = capture.exchanges;
  const frame = (f, ingress) => ({ at: f.at, deliver_at: f.deliverAt, src: f.src, dst: f.dst, conn: f.conn, kind: 'data', size: f.size, label: f.label, bytes: Buffer.from(f.bytes).toString('base64'), ingress });
  const remoteServer = new Capture();
  remoteServer.add([frame(ex.req, false), frame(ex.resp, true)]);
  assert.deepEqual([remoteServer.exchanges[0].rtt, remoteServer.exchanges[0].serverMs], [ex.rtt, null]);
  const remoteClient = new Capture();
  remoteClient.add([frame(ex.req, true), frame(ex.resp, false)]);
  assert.deepEqual([remoteClient.exchanges[0].rtt, remoteClient.exchanges[0].serverMs], [null, ex.serverMs]);
  const { byApi } = remoteServer.stats(remoteServer.exchanges, remoteServer.frames, String);
  assert.deepEqual([byApi[0].rtts, byApi[0].servers], [[ex.rtt], []]);
});

// ---- the decoder, on every fixture exchange ----
await checkAsync('every fixture frame decodes to its last byte', async () => {
  const bad = [];
  for (const ex of capture.exchanges) {
    for (const [request, f] of [[true, ex.req], [false, ex.resp]]) {
      const d = await decodeFrame(f.bytes, { size: f.size, request, answers: ex });
      if (d.problems.length) bad.push(`${schemas.apiNames[ex.apiKey]} v${ex.version} ${request ? 'request' : 'response'}: ${d.problems.join('; ')}`);
      let covered = 0;
      walk(d.root, (n) => { if (!n.children?.length && n.buf === 'main') covered = Math.max(covered, n.end); });
      if (covered !== f.bytes.length) bad.push(`${schemas.apiNames[ex.apiKey]}: fields end at ${covered} of ${f.bytes.length}`);
    }
  }
  assert.deepEqual(bad, []);
});
await checkAsync('UNSUPPORTED_VERSION answers to ApiVersions decode at v0', async () => {
  const ex = capture.exchanges.find((x) => x.apiKey === 18 && x.version === 5);
  assert.ok(ex, 'the fixture has an ApiVersions v5 exchange');
  const d = await decodeFrame(ex.resp.bytes, { size: ex.resp.size, request: false, answers: ex });
  assert.deepEqual(d.errors.map((e) => e.name), ['UNSUPPORTED_VERSION']);
  assert.match(d.root.note, /at v0/);
});
await checkAsync('a produced batch decodes with a verified CRC and its records', async () => {
  const ex = capture.exchanges.find((x) => x.apiKey === 0);
  const d = await decodeFrame(ex.req.bytes, { size: ex.req.size, request: true, answers: ex });
  const batches = [];
  walk(d.root, (n) => n.batch && batches.push(n.batch));
  assert.ok(batches.length >= 1 && batches.every((b) => b.crcOk && b.records.length === b.count), JSON.stringify(batches.map((b) => [b.crcOk, b.count])));
});
await checkAsync('a fetched batch carries records', async () => {
  let found = 0;
  for (const ex of capture.exchanges.filter((x) => x.apiKey === 1)) {
    const d = await decodeFrame(ex.resp.bytes, { size: ex.resp.size, request: false, answers: ex });
    walk(d.root, (n) => { if (n.batch) found += n.batch.records.length; });
  }
  assert.ok(found > 0);
});

// ---- pcapng ----
check('the pcapng export is well-formed raw IPv4 with consistent TCP streams', () => {
  const frames = capture.frames;
  const out = capture.toPcapng(frames, 'check');
  const v = new DataView(out.buffer);
  let pos = 0;
  const types = [];
  const seqs = new Map();
  while (pos < out.length) {
    const type = v.getUint32(pos, true);
    const len = v.getUint32(pos + 4, true);
    assert.equal(v.getUint32(pos + len - 4, true), len, `trailing length of block at ${pos}`);
    assert.equal(len % 4, 0);
    types.push(type);
    if (type === 1) assert.equal(v.getUint16(pos + 8, true), 101, 'LINKTYPE_RAW');
    if (type === 6) {
      const cap = v.getUint32(pos + 20, true);
      const p = pos + 28;
      assert.equal(v.getUint8(p) >> 4, 4, 'IPv4');
      let sum = 0;
      for (let i = 0; i < 20; i += 2) sum += v.getUint16(p + i);
      while (sum > 0xffff) sum = (sum & 0xffff) + (sum >>> 16);
      assert.equal(sum, 0xffff, 'IPv4 header checksum');
      const payload = v.getUint16(p + 2) - 40;
      const key = `${v.getUint32(p + 12)}:${v.getUint16(p + 20)}>${v.getUint32(p + 16)}:${v.getUint16(p + 22)}`;
      const seq = v.getUint32(p + 24);
      if (seqs.has(key)) assert.equal(seq, seqs.get(key), `sequence continues on ${key}`);
      seqs.set(key, (seq + payload) >>> 0);
      assert.ok(cap >= 40 && cap <= 40 + payload);
      if (cap === 40 + payload) {
        // The TCP checksum over the pseudo-header, header and payload sums to all ones.
        let t = 6 + 20 + payload;
        for (let i = 12; i < 20; i += 2) t += v.getUint16(p + i);
        for (let i = 20; i < cap - 1; i += 2) t += v.getUint16(p + i);
        if (cap % 2) t += v.getUint8(p + cap - 1) << 8;
        while (t > 0xffff) t = (t & 0xffff) + (t >>> 16);
        assert.equal(t, 0xffff, `TCP checksum on ${key}`);
      }
    }
    pos += len;
  }
  assert.equal(types[0], 0x0a0d0d0a);
  assert.equal(types.filter((t) => t === 6).length, frames.length);
});

// ---- disk formats ----
const files = Object.fromEntries(Object.entries(fixture.files).map(([p, b]) => [p, fromB64(b)]));
const sibling = (path) => async (name) => files[path.slice(0, path.lastIndexOf('/') + 1) + name] ?? null;
await checkAsync('every fixture file passes its checks', async () => {
  const bad = [];
  for (const [path, bytes] of Object.entries(files)) {
    const a = await analyzeFile(path, bytes, { sibling: sibling(path) });
    for (const c of a.checks.filter((x) => !x.ok)) bad.push(`${path}: ${c.text}`);
  }
  assert.deepEqual(bad, []);
});
await checkAsync('an index entry is checked against the batch it points at', async () => {
  const path = Object.keys(files).find((p) => p.endsWith('.index') && files[p].length);
  const a = await analyzeFile(path, files[path], { sibling: sibling(path) });
  assert.ok(a.root.children.length && a.root.children.every((e) => e.status === 'ok'), JSON.stringify(a.checks));
});
await checkAsync('a flipped byte fails exactly one batch CRC', async () => {
  const path = Object.keys(files).find((p) => /orders-\d+\/\d+\.log$/.test(p) && files[p].length > 200);
  const bytes = files[path].slice();
  bytes[100] ^= 0x01;
  const a = await analyzeFile(path, bytes, { sibling: sibling(path) });
  assert.equal(a.batches.filter((b) => !b.crcOk).length, 1);
  assert.ok(a.checks.some((c) => !c.ok && /CRC/.test(c.text)));
});
await checkAsync('a cut segment reports the bytes after its last whole batch', async () => {
  const path = Object.keys(files).find((p) => /orders-\d+\/\d+\.log$/.test(p) && files[p].length > 200);
  const a = await analyzeFile(path, files[path].slice(0, files[path].length - 7), { sibling: sibling(path) });
  assert.ok(a.checks.some((c) => !c.ok && /after the last whole batch/.test(c.text)), JSON.stringify(a.checks));
});
await checkAsync('the KRaft metadata log decodes into metadata records', async () => {
  const path = Object.keys(files).find((p) => p.includes('@metadata-0') && p.endsWith('.log'));
  const { summaries } = await decodeBatches(files[path], 0, files[path].length, 'main', await loadSchemas(), { topic: '__cluster_metadata' });
  const kinds = new Set(summaries.flatMap((b) => b.records.map((r) => r.record)).filter(Boolean));
  for (const k of ['RegisterBrokerRecord', 'TopicRecord', 'PartitionRecord']) assert.ok(kinds.has(k), `${k} in ${[...kinds].join(', ')}`);
  assert.ok(summaries.some((b) => b.control), 'a control batch');
});
await checkZstd('a zstd batch decodes to the same records as its uncompressed twin', async () => {
  const path = Object.keys(files).find((p) => /orders-\d+\/\d+\.log$/.test(p) && files[p].length > 200);
  const plain = files[path];
  const { summaries: [batch] } = await decodeBatches(plain, 0, plain.length, 'main', await loadSchemas());
  assert.equal(batch.codec, 0);
  const records = plain.subarray(batch.pos + 61, batch.pos + batch.size);
  const header = Buffer.from(plain.subarray(batch.pos, batch.pos + 61));
  const zbatch = Buffer.concat([header, zlib.zstdCompressSync(records)]);
  zbatch.writeInt32BE(zbatch.length - 12, 8);
  zbatch.writeInt16BE(header.readInt16BE(21) | 4, 21);
  zbatch.writeUInt32BE(crc32c(zbatch.subarray(21)), 17);
  const { summaries: [z] } = await decodeBatches(Uint8Array.from(zbatch), 0, zbatch.length, 'main', await loadSchemas());
  assert.deepEqual([z.codec, z.crcOk, z.problems], [4, true, []]);
  assert.equal(JSON.stringify(z.records, (k, v) => (typeof v === 'bigint' ? `${v}` : v)), JSON.stringify(batch.records, (k, v) => (typeof v === 'bigint' ? `${v}` : v)));
  assert.ok(batch.records.length > 0);
});
await checkAsync('a partition agrees with its leader-epoch checkpoint', async () => {
  const parts = partitionsOf(Object.entries(files).map(([path, b]) => ({ path, size: b.length })));
  const part = parts.find((p) => p.topic === 'orders' && p.segments.some((s) => s.size));
  const r = await analyzePartition(part, async (p) => files[p]);
  assert.ok(r.checks.every((c) => c.ok), JSON.stringify(r.checks));
  assert.ok(r.epochs.length >= 1);
});
await checkAsync('a partition decodes only its newest segments within the budget', async () => {
  const parts = partitionsOf(Object.entries(files).map(([path, b]) => ({ path, size: b.length })));
  // The fixture keeps one segment per partition: put an older one in front of it.
  const real = parts.find((p) => p.topic === 'orders' && p.segments.some((s) => s.size));
  const newest = real.segments.find((s) => s.size);
  const part = { ...real, segments: [{ ...newest, path: 'older/00000000000000000000.log' }, newest] };
  const r = await analyzePartition(part, async (p) => files[p], { budget: newest.size });
  assert.deepEqual(r.segments.map((s) => s.path), [newest.path]);
  assert.equal(r.skipped.length, part.segments.length - 1);
  assert.ok(r.checks.some((c) => c.warn && /not decoded/.test(c.text)), JSON.stringify(r.checks));
  assert.ok(r.checks.every((c) => c.ok), JSON.stringify(r.checks));
});
await checkAsync('a partition reports a segment cut mid-batch', async () => {
  const parts = partitionsOf(Object.entries(files).map(([path, b]) => ({ path, size: b.length })));
  const part = parts.find((p) => p.topic === 'orders' && p.segments.some((s) => s.size > 200));
  const cut = part.segments.find((s) => s.size > 200).path;
  const r = await analyzePartition(part, async (p) => (p === cut ? files[p].slice(0, files[p].length - 7) : files[p]));
  assert.ok(r.checks.some((c) => !c.ok && /damaged or partial data/.test(c.text) && c.text.includes('Partial batch')), JSON.stringify(r.checks));
});
await checkAsync('each __consumer_offsets record is read by its own key', async () => {
  // An offset commit, then a group metadata record (key v2) in one batch.
  const zz = (n) => { const o = []; let v = (n << 1) ^ (n >> 31); while (v > 127) { o.push((v & 127) | 128); v >>>= 7; } o.push(v); return o; };
  const str = (s) => [...Buffer.from([0, s.length]), ...Buffer.from(s)];
  const i64 = (n) => [0, 0, 0, 0, 0, 0, 0, n];
  const record = (delta, key, value) => {
    const body = [0, ...zz(0), ...zz(delta), ...zz(key.length), ...key, ...zz(value.length), ...value, ...zz(0)];
    return [...zz(body.length), ...body];
  };
  const records = [
    ...record(0, [0, 1, ...str('g1'), ...str('orders'), 0, 0, 0, 0], [0, 3, ...i64(42), 0, 0, 0, 0, ...str(''), ...i64(9)]),
    ...record(1, [0, 2, ...str('g1')], [0, 3, ...str('consumer'), 0, 0, 0, 1]),
  ];
  const batch = Buffer.alloc(61 + records.length);
  batch.writeBigInt64BE(0n, 0);
  batch.writeInt32BE(batch.length - 12, 8);
  batch.writeInt8(2, 16);
  batch.writeInt32BE(1, 23);
  batch.writeBigInt64BE(-1n, 43);
  batch.writeInt16BE(-1, 51);
  batch.writeInt32BE(-1, 53);
  batch.writeInt32BE(2, 57);
  Buffer.from(records).copy(batch, 61);
  batch.writeUInt32BE(crc32c(batch.subarray(21)), 17);
  const { nodes, summaries: [b] } = await decodeBatches(Uint8Array.from(batch), 0, batch.length, 'main', await loadSchemas(), { topic: '__consumer_offsets' });
  assert.equal(b.records.length, 2, JSON.stringify(b.problems));
  const [first, second] = nodes[0].children.find((n) => n.label.startsWith('Records')).children;
  const value = second.children.find((n) => n.label === 'Value');
  assert.equal(value.children, undefined, `the group metadata value is not read as an offset commit: ${JSON.stringify(value.children?.map((n) => n.label))}`);
  assert.ok(first.children.find((n) => n.label === 'Value').children, 'the offset commit value is decoded');
});

console.log(`check-lab-analyzer: ${passed} checks passed${process.exitCode ? ', some failed' : ''}`);
