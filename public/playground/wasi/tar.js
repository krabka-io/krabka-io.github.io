// A small POSIX tar (ustar) codec for exporting and importing volumes.
//
// `encodeTar` writes directories and regular files; a path longer than the
// 100-byte name field, or one that is not ASCII, gets a PAX extended header
// (`path=`), so topic directories with long names survive. `decodeTar` reads
// ustar, PAX (`x` and `g`) and GNU long-name (`L`) archives and skips entry
// types a volume cannot hold (links, devices), reporting them.

const BLOCK = 512;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function octal(value, width) {
  return value.toString(8).padStart(width - 1, "0") + "\0";
}

function writeString(block, offset, width, text) {
  const bytes = encoder.encode(text);
  block.set(bytes.subarray(0, width), offset);
}

function header({ name, type, size, mode, mtime }) {
  const block = new Uint8Array(BLOCK);
  writeString(block, 0, 100, name);
  writeString(block, 100, 8, octal(mode, 8));
  writeString(block, 108, 8, octal(0, 8));
  writeString(block, 116, 8, octal(0, 8));
  writeString(block, 124, 12, octal(size, 12));
  writeString(block, 136, 12, octal(Math.max(0, Math.floor(mtime)), 12));
  block.fill(0x20, 148, 156);
  block[156] = type.charCodeAt(0);
  writeString(block, 257, 6, "ustar\0");
  writeString(block, 263, 2, "00");
  writeString(block, 265, 32, "krabka");
  writeString(block, 297, 32, "krabka");
  let sum = 0;
  for (const byte of block) sum += byte;
  writeString(block, 148, 8, `${sum.toString(8).padStart(6, "0")}\0 `);
  return block;
}

/** One PAX record: "<len> key=value\n", where len counts the whole record. */
function paxRecord(key, value) {
  const body = ` ${key}=${value}\n`;
  const bodyLength = encoder.encode(body).length;
  let length = bodyLength + 1;
  while (String(length).length + bodyLength !== length) length = String(length).length + bodyLength;
  return `${length}${body}`;
}

function padded(bytes) {
  const size = Math.ceil(bytes.length / BLOCK) * BLOCK;
  if (size === bytes.length) return bytes;
  const out = new Uint8Array(size);
  out.set(bytes);
  return out;
}

/**
 * @param {Array<{path: string, type: "dir"|"file", bytes?: Uint8Array, mtimeMs: number}>} entries
 * @returns {Uint8Array} the archive
 */
export function encodeTar(entries) {
  const parts = [];
  for (const entry of entries) {
    const name = entry.type === "dir" ? `${entry.path}/` : entry.path;
    const data = entry.type === "dir" ? new Uint8Array(0) : entry.bytes;
    const seconds = entry.mtimeMs / 1000;
    const plain = /^[\x20-\x7e]*$/.test(name) && name.length <= 100;
    if (!plain) {
      const pax = encoder.encode(paxRecord("path", name) + paxRecord("mtime", seconds.toFixed(9)));
      parts.push(header({ name: `PaxHeader/${name.slice(-80).replace(/[^\x20-\x7e]/g, "_")}`, type: "x", size: pax.length, mode: 0o644, mtime: seconds }));
      parts.push(padded(pax));
    }
    const shortName = plain ? name : name.slice(0, 100).replace(/[^\x20-\x7e]/g, "_");
    parts.push(header({ name: shortName, type: entry.type === "dir" ? "5" : "0", size: data.length, mode: entry.type === "dir" ? 0o755 : 0o644, mtime: seconds }));
    if (data.length > 0) parts.push(padded(data));
  }
  parts.push(new Uint8Array(BLOCK * 2));
  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
}

function readString(block, offset, width) {
  let end = offset;
  while (end < offset + width && block[end] !== 0) end++;
  return decoder.decode(block.subarray(offset, end));
}

function readOctal(block, offset, width) {
  const text = readString(block, offset, width).trim();
  return text === "" ? 0 : parseInt(text, 8);
}

function parsePax(bytes) {
  const records = {};
  let at = 0;
  while (at < bytes.length) {
    let space = at;
    while (space < bytes.length && bytes[space] !== 0x20) space++;
    const length = Number(decoder.decode(bytes.subarray(at, space)));
    if (!Number.isInteger(length) || length <= 0 || at + length > bytes.length) break;
    const record = decoder.decode(bytes.subarray(space + 1, at + length)); // "key=value\n"
    const equals = record.indexOf("=");
    if (equals > 0) records[record.slice(0, equals)] = record.slice(equals + 1).replace(/\n$/, "");
    at += length;
  }
  return records;
}

/**
 * @param {Uint8Array} archive
 * @returns {{entries: Array<{path: string, type: "dir"|"file", bytes: Uint8Array, mtimeMs: number}>, skipped: string[]}}
 */
export function decodeTar(archive) {
  const entries = [];
  const skipped = [];
  let pax = {};
  let longName = null;
  let at = 0;
  while (at + BLOCK <= archive.length) {
    const block = archive.subarray(at, at + BLOCK);
    at += BLOCK;
    if (block.every((byte) => byte === 0)) break;
    let sum = 0;
    for (let i = 0; i < BLOCK; i++) sum += i >= 148 && i < 156 ? 0x20 : block[i];
    if (sum !== readOctal(block, 148, 8)) throw new Error(`tar: bad header checksum at byte ${at - BLOCK}`);
    const type = String.fromCharCode(block[156] || 0x30);
    const size = readOctal(block, 124, 12);
    const data = archive.subarray(at, at + size);
    at += Math.ceil(size / BLOCK) * BLOCK;
    if (type === "x") {
      pax = { ...pax, ...parsePax(data) };
      continue;
    }
    if (type === "g") continue;
    if (type === "L") {
      longName = readString(data, 0, data.length);
      continue;
    }
    const prefix = readString(block, 345, 155);
    let name = pax.path ?? longName ?? (prefix ? `${prefix}/${readString(block, 0, 100)}` : readString(block, 0, 100));
    const mtimeMs = (pax.mtime !== undefined ? Number(pax.mtime) : readOctal(block, 136, 12)) * 1000;
    pax = {};
    longName = null;
    name = name.replace(/^(\.\/)+/, "").replace(/^\/+/, "");
    const path = name.replace(/\/+$/, "");
    if (path === "" || path === ".") continue;
    if (path.split("/").some((part) => part === ".." || part === "")) {
      skipped.push(`${name} (unsafe path)`);
      continue;
    }
    if (type === "5") entries.push({ path, type: "dir", bytes: new Uint8Array(0), mtimeMs });
    else if (type === "0" || type === "7") entries.push({ path, type: "file", bytes: data.slice(), mtimeMs });
    else skipped.push(`${name} (entry type ${JSON.stringify(type)})`);
  }
  return { entries, skipped };
}
