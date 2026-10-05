// A Zstandard decoder (RFC 8878) for record batches compressed with zstd,
// codec 4 of Kafka. Like inflate.js it is small rather than fast: it decodes
// one or more frames, skips skippable frames, and refuses frames that need a
// dictionary. The content checksum is not verified; the batch CRC-32C above it
// covers the compressed bytes.
//
// `unzstd(bytes: Uint8Array) -> Uint8Array`; throws on corrupt input.

const MAGIC = 0xfd2fb528;

// The default distributions (RFC 8878, 3.1.1.3.2.2).
const LL_DEFAULT = [4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, -1, -1, -1, -1];
const ML_DEFAULT = [1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1];
const OF_DEFAULT = [1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1];
// Literal-length and match-length codes: baseline and extra bits (3.1.1.3.2.1).
const LL_BASE = [16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
const LL_BITS = [1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const ML_BASE = [35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051, 4099, 8195, 16387, 32771, 65539];
const ML_BITS = [1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

const fail = (why) => {
  throw new Error(`zstd: ${why}`);
};
const highBit = (v) => 31 - Math.clz32(v);

// Bits read from the end of a byte range towards its start, as the entropy
// coded streams are written. Bits before the start read as zeros, which the
// FSE weight decoder relies on to find its end.
class BackBits {
  constructor(src, start, end) {
    if (end <= start || src[end - 1] === 0) fail("a bitstream has no end marker");
    this.src = src;
    this.start = start;
    this.pos = (end - 1 - start) * 8 + highBit(src[end - 1]);
  }
  bits(at, n) {
    const lo = Math.max(at, 0);
    const hi = at + n;
    if (hi <= lo) return 0;
    let acc = 0;
    let mul = 1;
    for (let i = lo >> 3; i <= (hi - 1) >> 3; i++, mul *= 256) acc += this.src[this.start + i] * mul;
    return (Math.floor(acc / 2 ** (lo & 7)) % 2 ** (hi - lo)) * 2 ** (lo - at);
  }
  read(n) {
    if (!n) return 0;
    this.pos -= n;
    return this.bits(this.pos, n);
  }
  peek(n) {
    return this.bits(this.pos - n, n);
  }
}

// An FSE table description (4.1.1), read forward: { norm, log, end }.
function readNormalized(src, at, end, maxSymbol, maxLog) {
  let bit = at * 8;
  const read = (n) => {
    let v = 0;
    for (let i = 0; i < n; i++, bit++) {
      if (bit >> 3 >= end) fail("FSE table description runs past its block");
      v += ((src[bit >> 3] >> (bit & 7)) & 1) * 2 ** i;
    }
    return v;
  };
  const peek = (n) => {
    const save = bit;
    const v = read(n);
    bit = save;
    return v;
  };
  const log = read(4) + 5;
  if (log > maxLog) fail(`FSE accuracy ${log} exceeds ${maxLog}`);
  let remaining = (1 << log) + 1;
  let threshold = 1 << log;
  let nbBits = log + 1;
  const norm = [];
  let previous0 = false;
  while (remaining > 1) {
    if (norm.length > maxSymbol) fail("FSE table has too many symbols");
    if (previous0) {
      let r;
      do {
        r = read(2);
        for (let k = 0; k < r; k++) norm.push(0);
      } while (r === 3);
      if (norm.length > maxSymbol) fail("FSE table has too many symbols");
    }
    const max = 2 * threshold - 1 - remaining;
    let count;
    if (peek(nbBits - 1) < max) count = read(nbBits - 1);
    else {
      count = read(nbBits);
      if (count >= threshold) count -= max;
    }
    count -= 1;
    remaining -= Math.abs(count);
    norm.push(count);
    previous0 = count === 0;
    while (remaining < threshold) {
      nbBits--;
      threshold >>= 1;
    }
  }
  if (remaining !== 1) fail("FSE probabilities do not add up");
  return { norm, log, end: Math.ceil(bit / 8) };
}

// The decoding table of a normalized distribution (4.1.1).
function fseTable(norm, log) {
  const size = 1 << log;
  const symbol = new Uint16Array(size);
  const bits = new Uint8Array(size);
  const base = new Uint32Array(size);
  const next = new Uint32Array(norm.length);
  let high = size - 1;
  norm.forEach((n, s) => {
    if (n === -1) {
      symbol[high--] = s;
      next[s] = 1;
    } else next[s] = n;
  });
  const step = (size >> 1) + (size >> 3) + 3;
  let pos = 0;
  norm.forEach((n, s) => {
    for (let i = 0; i < n; i++) {
      symbol[pos] = s;
      do pos = (pos + step) & (size - 1);
      while (pos > high);
    }
  });
  if (pos !== 0) fail("FSE table is not filled");
  for (let u = 0; u < size; u++) {
    const x = next[symbol[u]]++;
    bits[u] = log - highBit(x);
    base[u] = (x << bits[u]) - size;
  }
  return { symbol, bits, base, log };
}

const rleTable = (s) => ({ symbol: Uint16Array.of(s), bits: Uint8Array.of(0), base: Uint32Array.of(0), log: 0 });

// The Huffman tree description (4.2.1): a decoding table indexed by the next `max` bits.
function readHuffman(src, at, end) {
  const header = src[at];
  const weights = [];
  let pos;
  if (header < 128) {
    pos = at + 1 + header;
    if (pos > end) fail("Huffman weights run past the literals");
    const { norm, log, end: tableEnd } = readNormalized(src, at + 1, pos, 255, 6);
    const t = fseTable(norm, log);
    const bs = new BackBits(src, tableEnd, pos);
    let s1 = bs.read(log);
    let s2 = bs.read(log);
    for (;;) {
      weights.push(t.symbol[s1]);
      s1 = t.base[s1] + bs.read(t.bits[s1]);
      if (bs.pos < 0) {
        weights.push(t.symbol[s2]);
        break;
      }
      weights.push(t.symbol[s2]);
      s2 = t.base[s2] + bs.read(t.bits[s2]);
      if (bs.pos < 0) {
        weights.push(t.symbol[s1]);
        break;
      }
      if (weights.length > 255) fail("too many Huffman weights");
    }
  } else {
    const n = header - 127;
    pos = at + 1 + Math.ceil(n / 2);
    for (let i = 0; i < n; i++) weights.push(i % 2 ? src[at + 1 + (i >> 1)] & 15 : src[at + 1 + (i >> 1)] >> 4);
  }
  let total = 0;
  for (const w of weights) if (w) total += 1 << (w - 1);
  if (!total) fail("Huffman weights are all zero");
  const max = highBit(total) + 1;
  const rest = (1 << max) - total;
  if (rest & (rest - 1)) fail("Huffman weights do not complete a tree");
  weights.push(highBit(rest) + 1);
  if (max > 11) fail(`Huffman codes of ${max} bits`);
  const start = new Uint32Array(max + 2);
  const count = new Uint32Array(max + 2);
  for (const w of weights) count[w]++;
  for (let w = 1, next = 0; w <= max; w++) {
    start[w] = next;
    next += count[w] << (w - 1);
  }
  const size = 1 << max;
  const symbol = new Uint8Array(size);
  const bits = new Uint8Array(size);
  weights.forEach((w, s) => {
    if (!w) return;
    const len = (1 << w) >> 1;
    symbol.fill(s, start[w], start[w] + len);
    bits.fill(max + 1 - w, start[w], start[w] + len);
    start[w] += len;
  });
  return { table: { symbol, bits, max }, end: pos };
}

function huffmanStream(src, start, end, table, out, from, count) {
  const bs = new BackBits(src, start, end);
  for (let i = 0; i < count; i++) {
    const k = bs.peek(table.max);
    out[from + i] = table.symbol[k];
    bs.pos -= table.bits[k];
  }
  if (bs.pos !== 0) fail("a Huffman stream does not end on its last bit");
}

// The literals section (3.1.1.3.1): { literals, end }.
function readLiterals(src, at, end, frame) {
  const b0 = src[at];
  const type = b0 & 3;
  const format = (b0 >> 2) & 3;
  if (type < 2) {
    let size;
    let pos;
    if ((format & 1) === 0) [size, pos] = [b0 >> 3, at + 1];
    else if (format === 1) [size, pos] = [(b0 >> 4) + (src[at + 1] << 4), at + 2];
    else [size, pos] = [(b0 >> 4) + (src[at + 1] << 4) + (src[at + 2] << 12), at + 3];
    if (type === 0) {
      if (pos + size > end) fail("raw literals run past the block");
      return { literals: src.subarray(pos, pos + size), end: pos + size };
    }
    return { literals: new Uint8Array(size).fill(src[pos]), end: pos + 1 };
  }
  const width = [10, 10, 14, 18][format];
  const headerLen = [3, 3, 4, 5][format];
  let value = 0;
  for (let i = 0; i < headerLen; i++) value += src[at + i] * 2 ** (8 * i);
  const regen = Math.floor(value / 16) % 2 ** width;
  const compressed = Math.floor(value / 2 ** (4 + width)) % 2 ** width;
  let pos = at + headerLen;
  const stop = pos + compressed;
  if (stop > end) fail("compressed literals run past the block");
  if (type === 2) {
    const h = readHuffman(src, pos, stop);
    frame.huffman = h.table;
    pos = h.end;
  } else if (!frame.huffman) fail("treeless literals with no earlier Huffman table");
  const out = new Uint8Array(regen);
  if (format === 0) huffmanStream(src, pos, stop, frame.huffman, out, 0, regen);
  else {
    const s1 = src[pos] | (src[pos + 1] << 8);
    const s2 = src[pos + 2] | (src[pos + 3] << 8);
    const s3 = src[pos + 4] | (src[pos + 5] << 8);
    const seg = (regen + 3) >> 2;
    let p = pos + 6;
    const ends = [p + s1, p + s1 + s2, p + s1 + s2 + s3, stop];
    if (ends[2] > stop) fail("Huffman jump table runs past the literals");
    for (let i = 0; i < 4; i++) {
      huffmanStream(src, p, ends[i], frame.huffman, out, i * seg, i < 3 ? seg : regen - 3 * seg);
      p = ends[i];
    }
  }
  return { literals: out, end: stop };
}

// One of the three sequence tables, by its compression mode (3.1.1.3.2.2).
function sequenceTable(mode, src, pos, end, defaults, defaultLog, maxSymbol, maxLog, previous) {
  if (mode === 0) return { table: fseTable(defaults, defaultLog), end: pos };
  if (mode === 1) return { table: rleTable(src[pos]), end: pos + 1 };
  if (mode === 2) {
    const d = readNormalized(src, pos, end, maxSymbol, maxLog);
    return { table: fseTable(d.norm, d.log), end: d.end };
  }
  if (!previous) fail("a repeated sequence table with none before it");
  return { table: previous, end: pos };
}

function decodeBlock(src, at, end, out, frame) {
  const lit = readLiterals(src, at, end, frame);
  let pos = lit.end;
  let n = src[pos++];
  if (n >= 128) n = n === 255 ? src[pos++] + (src[pos++] << 8) + 0x7f00 : ((n - 128) << 8) + src[pos++];
  const literals = lit.literals;
  let litPos = 0;
  if (n > 0) {
    const modes = src[pos++];
    if (modes & 3) fail("reserved bits set in the sequence modes");
    const ll = sequenceTable(modes >> 6, src, pos, end, LL_DEFAULT, 6, 35, 9, frame.ll);
    const of = sequenceTable((modes >> 4) & 3, src, ll.end, end, OF_DEFAULT, 5, 31, 8, frame.of);
    const ml = sequenceTable((modes >> 2) & 3, src, of.end, end, ML_DEFAULT, 6, 52, 9, frame.ml);
    [frame.ll, frame.of, frame.ml] = [ll.table, of.table, ml.table];
    const bs = new BackBits(src, ml.end, end);
    let sLL = bs.read(ll.table.log);
    let sOF = bs.read(of.table.log);
    let sML = bs.read(ml.table.log);
    const rep = frame.rep;
    for (let i = 0; i < n; i++) {
      const ofCode = of.table.symbol[sOF];
      const mlCode = ml.table.symbol[sML];
      const llCode = ll.table.symbol[sLL];
      if (ofCode > 31) fail(`offset code ${ofCode}`);
      let offset = 2 ** ofCode + bs.read(ofCode);
      const matchLen = mlCode < 32 ? mlCode + 3 : ML_BASE[mlCode - 32] + bs.read(ML_BITS[mlCode - 32]);
      const litLen = llCode < 16 ? llCode : LL_BASE[llCode - 16] + bs.read(LL_BITS[llCode - 16]);
      if (offset > 3) {
        offset -= 3;
        rep.unshift(offset);
        rep.length = 3;
      } else {
        const idx = offset - (litLen === 0 ? 0 : 1);
        if (idx === 0) offset = rep[0];
        else {
          offset = idx === 3 ? rep[0] - 1 : rep[idx];
          if (!offset) fail("repeat offset of zero");
          if (idx === 1) rep[1] = rep[0];
          else {
            rep[2] = rep[1];
            rep[1] = rep[0];
          }
          rep[0] = offset;
        }
      }
      if (litPos + litLen > literals.length) fail("a sequence takes more literals than there are");
      out.push(literals.subarray(litPos, litPos + litLen));
      litPos += litLen;
      out.copyBack(offset, matchLen);
      if (i < n - 1) {
        sLL = ll.table.base[sLL] + bs.read(ll.table.bits[sLL]);
        sML = ml.table.base[sML] + bs.read(ml.table.bits[sML]);
        sOF = of.table.base[sOF] + bs.read(of.table.bits[sOF]);
      }
    }
    if (bs.pos !== 0) fail("the sequences do not end on their last bit");
  }
  out.push(literals.subarray(litPos));
}

class Output {
  constructor(size) {
    this.buf = new Uint8Array(Math.max(64, size));
    this.len = 0;
  }
  ensure(n) {
    if (this.len + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.len + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }
  push(bytes) {
    this.ensure(bytes.length);
    this.buf.set(bytes, this.len);
    this.len += bytes.length;
  }
  copyBack(offset, length) {
    if (offset > this.len) fail(`offset ${offset} reaches before the output`);
    this.ensure(length);
    for (let i = 0; i < length; i++, this.len++) this.buf[this.len] = this.buf[this.len - offset];
  }
}

export function unzstd(src) {
  const view = new DataView(src.buffer, src.byteOffset, src.byteLength);
  const out = new Output(src.length * 4);
  let pos = 0;
  while (pos < src.length) {
    if (pos + 4 > src.length) fail("truncated frame magic");
    const magic = view.getUint32(pos, true);
    if ((magic & 0xfffffff0) === 0x184d2a50) {
      pos += 8 + view.getUint32(pos + 4, true);
      continue;
    }
    if (magic !== MAGIC) fail(`no frame magic at byte ${pos}`);
    const fhd = src[pos + 4];
    pos += 5;
    const fcsFlag = fhd >> 6;
    const single = (fhd >> 5) & 1;
    if (fhd & 8) fail("reserved bit set in the frame header");
    const checksum = (fhd >> 2) & 1;
    const dictBytes = [0, 1, 2, 4][fhd & 3];
    if (!single) pos++; // window descriptor: the whole frame is kept, so its size does not matter
    let dict = 0;
    for (let i = 0; i < dictBytes; i++) dict += src[pos + i] * 2 ** (8 * i);
    if (dict) fail(`frame needs dictionary ${dict}`);
    pos += dictBytes + [single, 2, 4, 8][fcsFlag];
    const frame = { rep: [1, 4, 8], huffman: null, ll: null, of: null, ml: null };
    for (let last = 0; !last;) {
      if (pos + 3 > src.length) fail("truncated block header");
      const header = src[pos] | (src[pos + 1] << 8) | (src[pos + 2] << 16);
      pos += 3;
      last = header & 1;
      const type = (header >> 1) & 3;
      const size = header >>> 3;
      if (type === 0) {
        if (pos + size > src.length) fail("raw block runs past the input");
        out.push(src.subarray(pos, pos + size));
        pos += size;
      } else if (type === 1) {
        out.ensure(size);
        out.buf.fill(src[pos], out.len, out.len + size);
        out.len += size;
        pos += 1;
      } else if (type === 2) {
        if (pos + size > src.length) fail("compressed block runs past the input");
        decodeBlock(src, pos, pos + size, out, frame);
        pos += size;
      } else fail("reserved block type");
    }
    if (checksum) pos += 4;
  }
  return out.buf.slice(0, out.len);
}
