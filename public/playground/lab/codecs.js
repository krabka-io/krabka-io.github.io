// Checksums and decompressors for Kafka record batches, in plain JavaScript,
// so the analyzer can verify and open what the brokers stored and sent.
//
// `crc32c(bytes)` is the Castagnoli CRC a RecordBatch v2 carries.
// `decompress(codec, bytes)` opens a batch's records: gzip (1), snappy (2),
// lz4 (3) and zstd (4).

import { inflateRaw } from "./inflate.js";
import { unzstd } from "./unzstd.js";

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32c(bytes) {
  let c = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

export const CODECS = ["none", "gzip", "snappy", "lz4", "zstd"];

class Growable {
  constructor(size = 1024) {
    this.buf = new Uint8Array(Math.max(16, size));
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
  // Copy `length` bytes from `offset` back; the ranges may overlap (a run).
  copyBack(offset, length) {
    if (offset <= 0 || offset > this.len) throw new Error(`back-reference ${offset} outside ${this.len} bytes`);
    this.ensure(length);
    for (let i = 0; i < length; i++, this.len++) this.buf[this.len] = this.buf[this.len - offset];
  }
  bytes() {
    return this.buf.slice(0, this.len);
  }
}

// One raw snappy block (the format, not the framing).
function snappyRaw(src) {
  let pos = 0;
  let expect = 0;
  for (let shift = 0; ; shift += 7) {
    const b = src[pos++];
    if (b === undefined) throw new Error("snappy: truncated length");
    expect |= (b & 0x7f) << shift;
    if (b < 0x80) break;
  }
  const out = new Growable(expect);
  while (pos < src.length) {
    const tag = src[pos++];
    const type = tag & 3;
    if (type === 0) {
      let len = tag >>> 2;
      if (len >= 60) {
        const n = len - 59;
        len = 0;
        for (let i = 0; i < n; i++) len |= src[pos++] << (8 * i);
      }
      len += 1;
      if (pos + len > src.length) throw new Error("snappy: literal runs past the input");
      out.push(src.subarray(pos, pos + len));
      pos += len;
    } else if (type === 1) {
      const len = ((tag >>> 2) & 7) + 4;
      const offset = ((tag >>> 5) << 8) | src[pos++];
      out.copyBack(offset, len);
    } else if (type === 2) {
      const len = (tag >>> 2) + 1;
      const offset = src[pos] | (src[pos + 1] << 8);
      pos += 2;
      out.copyBack(offset, len);
    } else {
      const len = (tag >>> 2) + 1;
      const offset = (src[pos] | (src[pos + 1] << 8) | (src[pos + 2] << 16) | (src[pos + 3] << 24)) >>> 0;
      pos += 4;
      out.copyBack(offset, len);
    }
  }
  if (out.len !== expect) throw new Error(`snappy: ${out.len} bytes, the header says ${expect}`);
  return out.bytes();
}

// Kafka's Java client writes snappy in xerial's stream framing; other clients
// write one raw block.
const XERIAL = [0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00];
export function snappy(src) {
  if (!XERIAL.every((b, i) => src[i] === b)) return snappyRaw(src);
  const view = new DataView(src.buffer, src.byteOffset, src.byteLength);
  const out = new Growable(src.length * 2);
  for (let pos = 16; pos < src.length;) {
    const len = view.getInt32(pos);
    pos += 4;
    if (len < 0 || pos + len > src.length) throw new Error("snappy: xerial block runs past the input");
    out.push(snappyRaw(src.subarray(pos, pos + len)));
    pos += len;
  }
  return out.bytes();
}

// The LZ4 frame format (lz4.org), which Kafka's lz4 codec writes.
export function lz4(src) {
  const view = new DataView(src.buffer, src.byteOffset, src.byteLength);
  if (src.length < 7 || view.getUint32(0, true) !== 0x184d2204) throw new Error("lz4: no frame magic");
  const flg = src[4];
  let pos = 6 + (flg & 0x08 ? 8 : 0) + (flg & 0x01 ? 4 : 0) + 1;
  const blockChecksum = Boolean(flg & 0x10);
  const out = new Growable(src.length * 3);
  for (;;) {
    if (pos + 4 > src.length) throw new Error("lz4: truncated block size");
    const word = view.getUint32(pos, true);
    pos += 4;
    if (word === 0) break;
    const size = word & 0x7fffffff;
    if (pos + size > src.length) throw new Error("lz4: block runs past the input");
    if (word & 0x80000000) out.push(src.subarray(pos, pos + size));
    else lz4Block(src.subarray(pos, pos + size), out);
    pos += size + (blockChecksum ? 4 : 0);
  }
  return out.bytes();
}

function lz4Block(src, out) {
  let pos = 0;
  while (pos < src.length) {
    const token = src[pos++];
    let lit = token >>> 4;
    if (lit === 15) for (let b = 255; b === 255;) lit += b = src[pos++];
    out.push(src.subarray(pos, pos + lit));
    pos += lit;
    if (pos >= src.length) break;
    const offset = src[pos] | (src[pos + 1] << 8);
    pos += 2;
    let len = (token & 15) + 4;
    if ((token & 15) === 15) for (let b = 255; b === 255;) len += b = src[pos++];
    out.copyBack(offset, len);
  }
}

async function gunzip(src) {
  if (typeof DecompressionStream === "function") {
    const stream = new Blob([src]).stream().pipeThrough(new DecompressionStream("gzip"));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  }
  // RFC 1952 header, then the raw DEFLATE stream.
  if (src[0] !== 0x1f || src[1] !== 0x8b) throw new Error("gzip: no magic");
  const flags = src[3];
  let pos = 10;
  if (flags & 4) pos += 2 + (src[pos] | (src[pos + 1] << 8));
  if (flags & 8) while (src[pos++] !== 0);
  if (flags & 16) while (src[pos++] !== 0);
  if (flags & 2) pos += 2;
  return inflateRaw(src.subarray(pos));
}

export async function decompress(codec, bytes) {
  switch (codec) {
    case 0: return bytes;
    case 1: return gunzip(bytes);
    case 2: return snappy(bytes);
    case 3: return lz4(bytes);
    case 4: return unzstd(bytes);
    default: throw new Error(`unknown compression codec ${codec}`);
  }
}
