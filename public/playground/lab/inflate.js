// A raw DEFLATE decoder (RFC 1951), for browsers without
// `DecompressionStream`. It decodes what `CompressionStream("deflate-raw")`
// produces: stored, fixed-Huffman and dynamic-Huffman blocks, back-references
// up to 32 KiB, output of any size. It follows zlib's `puff.c`: canonical
// Huffman codes decoded a bit at a time, which is small and plenty fast for
// share and invite codes.
//
// `inflateRaw(bytes: Uint8Array) -> Uint8Array`; throws on corrupt input.

const MAX_BITS = 15;
const LEN_BASE = [3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258];
const LEN_EXTRA = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0];
const DIST_BASE = [1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577];
const DIST_EXTRA = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13];
// The order code-length code lengths arrive in (RFC 1951, 3.2.7).
const CL_ORDER = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];

// A canonical Huffman code: how many codes of each length, and the symbols
// in code order. Over-subscribed code lengths are an error; an incomplete
// code is allowed (a lone distance code is incomplete by nature).
function huffman(lengths) {
  const count = new Uint16Array(MAX_BITS + 1);
  for (const len of lengths) count[len] += 1;
  count[0] = 0;
  let left = 1;
  for (let len = 1; len <= MAX_BITS; len++) {
    left = (left << 1) - count[len];
    if (left < 0) throw new Error("inflate: over-subscribed Huffman code");
  }
  const offs = new Uint16Array(MAX_BITS + 2);
  for (let len = 1; len <= MAX_BITS; len++) offs[len + 1] = offs[len] + count[len];
  const symbol = new Uint16Array(lengths.length);
  for (let sym = 0; sym < lengths.length; sym++) if (lengths[sym]) symbol[offs[lengths[sym]]++] = sym;
  return { count, symbol };
}

const FIXED = (() => {
  const lit = new Uint8Array(288);
  lit.fill(8, 0, 144);
  lit.fill(9, 144, 256);
  lit.fill(7, 256, 280);
  lit.fill(8, 280, 288);
  return { lit: huffman(lit), dist: huffman(new Uint8Array(30).fill(5)) };
})();

class Reader {
  constructor(bytes) {
    this.bytes = bytes;
    this.pos = 0;
    this.bitBuf = 0;
    this.bitCount = 0;
  }

  // The next `n` bits (n <= 16), least significant first.
  bits(n) {
    let buf = this.bitBuf;
    while (this.bitCount < n) {
      if (this.pos >= this.bytes.length) throw new Error("inflate: unexpected end of data");
      buf |= this.bytes[this.pos++] << this.bitCount;
      this.bitCount += 8;
    }
    this.bitBuf = buf >>> n;
    this.bitCount -= n;
    return buf & ((1 << n) - 1);
  }

  // One symbol of `code`, read a bit at a time (codes are stored bit-reversed).
  decode(code) {
    let value = 0;
    let first = 0;
    let index = 0;
    for (let len = 1; len <= MAX_BITS; len++) {
      value |= this.bits(1);
      const count = code.count[len];
      if (value - count < first) return code.symbol[index + (value - first)];
      index += count;
      first = (first + count) << 1;
      value <<= 1;
    }
    throw new Error("inflate: invalid Huffman code");
  }
}

class Output {
  constructor(hint) {
    this.buf = new Uint8Array(Math.max(1024, hint));
    this.len = 0;
  }

  reserve(n) {
    if (this.len + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.len + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }

  result() {
    return this.buf.slice(0, this.len);
  }
}

function stored(r, out) {
  // Skip to a byte boundary; LEN and NLEN follow as whole bytes.
  r.bitBuf = 0;
  r.bitCount = 0;
  if (r.pos + 4 > r.bytes.length) throw new Error("inflate: unexpected end of data");
  const b = r.bytes;
  const len = b[r.pos] | (b[r.pos + 1] << 8);
  const nlen = b[r.pos + 2] | (b[r.pos + 3] << 8);
  r.pos += 4;
  if (len !== (~nlen & 0xffff)) throw new Error("inflate: stored block length mismatch");
  if (r.pos + len > b.length) throw new Error("inflate: unexpected end of data");
  out.reserve(len);
  out.buf.set(b.subarray(r.pos, r.pos + len), out.len);
  out.len += len;
  r.pos += len;
}

function codes(r, out, lit, dist) {
  for (;;) {
    const sym = r.decode(lit);
    if (sym < 256) {
      out.reserve(1);
      out.buf[out.len++] = sym;
    } else if (sym === 256) {
      return;
    } else {
      const li = sym - 257;
      if (li >= 29) throw new Error("inflate: invalid length symbol");
      const len = LEN_BASE[li] + r.bits(LEN_EXTRA[li]);
      const di = r.decode(dist);
      if (di >= 30) throw new Error("inflate: invalid distance symbol");
      const d = DIST_BASE[di] + r.bits(DIST_EXTRA[di]);
      if (d > out.len) throw new Error("inflate: distance too far back");
      out.reserve(len);
      const buf = out.buf;
      let at = out.len;
      // Byte by byte: the source may overlap what this copy writes.
      for (let i = 0; i < len; i++, at++) buf[at] = buf[at - d];
      out.len = at;
    }
  }
}

function dynamic(r, out) {
  const nlen = r.bits(5) + 257;
  const ndist = r.bits(5) + 1;
  const ncode = r.bits(4) + 4;
  if (nlen > 286 || ndist > 30) throw new Error("inflate: bad code counts");
  const clLengths = new Uint8Array(19);
  for (let i = 0; i < ncode; i++) clLengths[CL_ORDER[i]] = r.bits(3);
  const cl = huffman(clLengths);
  const lengths = new Uint8Array(nlen + ndist);
  for (let i = 0; i < nlen + ndist; ) {
    const sym = r.decode(cl);
    if (sym < 16) {
      lengths[i++] = sym;
      continue;
    }
    let value = 0;
    let repeat;
    if (sym === 16) {
      if (i === 0) throw new Error("inflate: repeat with no previous length");
      value = lengths[i - 1];
      repeat = 3 + r.bits(2);
    } else if (sym === 17) {
      repeat = 3 + r.bits(3);
    } else {
      repeat = 11 + r.bits(7);
    }
    if (i + repeat > nlen + ndist) throw new Error("inflate: too many code lengths");
    lengths.fill(value, i, i + repeat);
    i += repeat;
  }
  if (lengths[256] === 0) throw new Error("inflate: no end-of-block code");
  codes(r, out, huffman(lengths.subarray(0, nlen)), huffman(lengths.subarray(nlen)));
}

export function inflateRaw(bytes) {
  const r = new Reader(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes));
  const out = new Output(r.bytes.length * 4);
  let last;
  do {
    last = r.bits(1);
    const type = r.bits(2);
    if (type === 0) stored(r, out);
    else if (type === 1) codes(r, out, FIXED.lit, FIXED.dist);
    else if (type === 2) dynamic(r, out);
    else throw new Error("inflate: invalid block type");
  } while (!last);
  return out.result();
}
