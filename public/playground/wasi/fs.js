// The working file system of one volume, in the worker's memory.
//
// This is the hot path: every file call the guest makes is served from here
// synchronously. Files are sparse arrays of pages (PAGE_SIZE bytes at most;
// the last page of a file grows as it is appended to), so a preallocated
// index or a truncated segment costs nothing until it is written.
//
// When the volume is persistent, every mutation is journaled. Metadata
// operations (create, mkdir, unlink, rmdir, rename) are recorded in order as
// they happen; data is tracked as dirty pages per inode and emitted whole at
// flush time, together with the inode's size and times (nanoseconds as
// decimal strings, which every browser's structured clone carries). `take()` hands out
// one batch; the host applies it to IndexedDB in a single transaction, so the
// stored volume always equals the file system at one of the guest's flush
// points. Chunks are keyed by inode number, not by path, so a rename (the
// log's segment swap, a directory renamed on delete) rewrites no data.

import { ERRNO as E, FILETYPE } from "./abi.js";
import { PAGE_SIZE } from "./protocol.js";

const DIR = "dir";
const FILE = "file";
const NAME_MAX = 255;
const encoder = new TextEncoder();

export class Inode {
  constructor(ino, type, now) {
    this.ino = ino;
    this.type = type;
    this.nlink = 1;
    this.atime = now;
    this.mtime = now;
    this.ctime = now;
    this.parent = null;
    this.name = "";
    this.nameBytes = null;
    this.seq = 0; // position in the parent's listing; readdir cookies are built from it
    this.opens = 0;
    this.attrDirty = false;
    if (type === DIR) {
      this.entries = new Map(); // insertion order == seq order
      this.nextSeq = 2; // cookies 0 and 1 are "." and ".."
    } else {
      this.size = 0;
      this.pages = [];
      this.dirtyPages = null;
      this.truncatedTo = null;
    }
  }

  get isDir() {
    return this.type === DIR;
  }

  get filetype() {
    return this.type === DIR ? FILETYPE.DIRECTORY : FILETYPE.REGULAR_FILE;
  }

  get encodedName() {
    this.nameBytes ??= encoder.encode(this.name);
    return this.nameBytes;
  }
}

export class MemFs {
  /**
   * @param {object} options
   * @param {() => bigint} options.now The guest's REALTIME clock in ns (file times).
   * @param {boolean} options.journal Whether mutations are journaled for persistence.
   * @param {number} [options.maxBytes] Logical size limit for all files together (ENOSPC beyond it).
   */
  constructor({ now, journal, maxBytes = Infinity }) {
    this.now = now;
    this.journalOn = journal;
    this.maxBytes = maxBytes;
    this.nextIno = 2;
    this.root = new Inode(1, DIR, now());
    this.root.parent = this.root;
    this.files = 0;
    this.dirs = 0;
    this.bytes = 0; // sum of file sizes
    this.allocated = 0; // bytes held by pages
    this.meta = []; // ordered metadata journal ops
    this.dirty = new Set(); // inodes with dirty pages, size or times
    this.dirtyPageCount = 0;
  }

  /** Builds the tree from a stored image: `{ nextIno, nodes: [{path, type, ino, size, atime, mtime}], chunks: [{ino, index, bytes}] }`. */
  load(image, warn = () => {}) {
    const dirs = new Map([["", this.root]]);
    const byIno = new Map();
    let maxIno = 1;
    const nodes = [...image.nodes].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    for (const node of nodes) {
      const slash = node.path.lastIndexOf("/");
      const parent = dirs.get(slash < 0 ? "" : node.path.slice(0, slash));
      if (!parent) {
        warn(`volume image: ${node.path} has no parent directory; skipped`);
        continue;
      }
      const inode = new Inode(node.ino, node.type === DIR ? DIR : FILE, BigInt(node.mtime ?? 0));
      inode.atime = BigInt(node.atime ?? node.mtime ?? 0);
      this.#link(parent, node.path.slice(slash + 1), inode);
      byIno.set(node.ino, inode);
      maxIno = Math.max(maxIno, node.ino);
      if (inode.isDir) {
        dirs.set(node.path, inode);
        this.dirs++;
      } else {
        inode.size = node.size ?? 0;
        this.bytes += inode.size;
        this.files++;
      }
    }
    for (const chunk of image.chunks) {
      const inode = byIno.get(chunk.ino);
      if (!inode || inode.isDir) continue;
      const limit = Math.min(PAGE_SIZE, inode.size - chunk.index * PAGE_SIZE);
      if (limit <= 0 || chunk.bytes.length === 0) continue;
      const bytes = chunk.bytes.length > limit ? chunk.bytes.subarray(0, limit) : chunk.bytes;
      inode.pages[chunk.index] = bytes;
      this.allocated += bytes.length;
    }
    this.nextIno = Math.max(image.nextIno ?? 2, maxIno + 1);
  }

  // ---- paths --------------------------------------------------------------------------------

  /** The inode's path relative to the volume root ("" for the root). */
  pathOf(inode) {
    if (inode === this.root) return "";
    const parts = [];
    for (let node = inode; node !== this.root; node = node.parent) parts.push(node.name);
    return parts.reverse().join("/");
  }

  #childPath(dir, name) {
    return dir === this.root ? name : `${this.pathOf(dir)}/${name}`;
  }

  /**
   * Resolves `path` relative to the directory `start`. Returns an errno, or
   * `{ parent, name, node, self, trailingSlash }` where `node` is null when the
   * last component does not exist and `self` marks a path ending in "." or "..".
   */
  resolve(start, path) {
    if (start.nlink === 0) return E.NOENT;
    if (path.length === 0) return E.NOENT;
    if (path.charCodeAt(0) === 47) return E.NOTCAPABLE; // absolute: wasi-libc hands relative paths only
    if (path.includes("\0")) return E.INVAL;
    const parts = path.split("/");
    let end = parts.length - 1;
    while (end >= 0 && parts[end] === "") end--;
    const trailingSlash = end < parts.length - 1;
    let dir = start;
    for (let i = 0; i < end; i++) {
      const part = parts[i];
      if (part === "" || part === ".") continue;
      if (part === "..") {
        if (dir === this.root) return E.NOTCAPABLE;
        dir = dir.parent;
        continue;
      }
      const child = dir.entries.get(part);
      if (!child) return E.NOENT;
      if (!child.isDir) return E.NOTDIR;
      dir = child;
    }
    const name = parts[end];
    if (name === "." || name === "..") {
      if (name === ".." && dir === this.root) return E.NOTCAPABLE;
      const node = name === "." ? dir : dir.parent;
      return { parent: node === this.root ? null : node.parent, name: null, node, self: true, trailingSlash };
    }
    if (encoder.encode(name).length > NAME_MAX) return E.NAMETOOLONG;
    const node = dir.entries.get(name) ?? null;
    if (node && trailingSlash && !node.isDir) return E.NOTDIR;
    return { parent: dir, name, node, self: false, trailingSlash };
  }

  /** Looks up a volume-relative path ("a/b", "" or "/" for the root); null when missing. */
  lookup(relative) {
    const trimmed = relative.replace(/^\/+|\/+$/g, "");
    if (trimmed === "") return this.root;
    const r = this.resolve(this.root, trimmed);
    return typeof r === "number" ? null : r.node;
  }

  // ---- namespace operations -----------------------------------------------------------------

  /**
   * `path_open`'s file-system half: returns the inode, or an errno.
   * @param {{create: boolean, excl: boolean, trunc: boolean, directory: boolean, write: boolean}} how
   */
  open(start, path, how) {
    const r = this.resolve(start, path);
    if (typeof r === "number") return r;
    let node = r.node;
    if (!node) {
      if (!how.create) return E.NOENT;
      if (how.directory) return E.INVAL;
      if (r.trailingSlash) return E.ISDIR;
      if (r.parent.nlink === 0) return E.NOENT;
      node = new Inode(this.nextIno++, FILE, this.now());
      this.#link(r.parent, r.name, node);
      r.parent.mtime = node.mtime;
      this.files++;
      this.#record(["create", this.pathOf(node), node.ino, String(node.mtime)]);
      return node;
    }
    if (how.create && how.excl) return E.EXIST;
    if (how.directory && !node.isDir) return E.NOTDIR;
    if (node.isDir) return how.write || how.trunc ? E.ISDIR : node;
    if (how.trunc && node.size > 0) this.truncate(node, 0);
    return node;
  }

  mkdir(start, path) {
    const r = this.resolve(start, path);
    if (typeof r === "number") return r;
    if (r.self || r.node) return E.EXIST;
    if (r.parent.nlink === 0) return E.NOENT;
    const node = new Inode(this.nextIno++, DIR, this.now());
    this.#link(r.parent, r.name, node);
    r.parent.mtime = node.mtime;
    this.dirs++;
    this.#record(["mkdir", this.pathOf(node), node.ino, String(node.mtime)]);
    return E.SUCCESS;
  }

  rmdir(start, path) {
    const r = this.resolve(start, path);
    if (typeof r === "number") return r;
    if (r.self) return r.node === this.root ? E.BUSY : E.INVAL;
    if (!r.node) return E.NOENT;
    if (!r.node.isDir) return E.NOTDIR;
    if (r.node.entries.size > 0) return E.NOTEMPTY;
    const path_ = this.pathOf(r.node);
    r.parent.entries.delete(r.name);
    r.parent.mtime = this.now();
    r.node.nlink = 0;
    this.dirs--;
    this.#record(["rmdir", path_]);
    return E.SUCCESS;
  }

  unlink(start, path) {
    const r = this.resolve(start, path);
    if (typeof r === "number") return r;
    if (r.self || (r.node && r.node.isDir)) return E.ISDIR;
    if (!r.node) return E.NOENT;
    const path_ = this.pathOf(r.node);
    r.parent.entries.delete(r.name);
    r.parent.mtime = this.now();
    this.#drop(r.node);
    this.#record(["unlink", path_]);
    return E.SUCCESS;
  }

  rename(fromStart, fromPath, toStart, toPath) {
    const a = this.resolve(fromStart, fromPath);
    if (typeof a === "number") return a;
    const b = this.resolve(toStart, toPath);
    if (typeof b === "number") return b;
    if (a.self || b.self) return a.node === this.root || b.node === this.root ? E.BUSY : E.INVAL;
    const src = a.node;
    if (!src) return E.NOENT;
    if (b.parent.nlink === 0) return E.NOENT;
    const dst = b.node;
    if (src === dst) return E.SUCCESS;
    if (src.isDir) {
      for (let dir = b.parent; ; dir = dir.parent) {
        if (dir === src) return E.INVAL; // into itself or a descendant
        if (dir === this.root) break;
      }
      if (dst && !dst.isDir) return E.NOTDIR;
      if (dst && dst.entries.size > 0) return E.NOTEMPTY;
    } else {
      if (dst && dst.isDir) return E.ISDIR;
      if (a.trailingSlash || b.trailingSlash) return E.NOTDIR;
    }
    const from = this.pathOf(src);
    const to = this.#childPath(b.parent, b.name);
    const now = this.now();
    if (dst) {
      b.parent.entries.delete(b.name);
      if (dst.isDir) {
        dst.nlink = 0;
        this.dirs--;
      } else {
        this.#drop(dst);
      }
    }
    a.parent.entries.delete(a.name);
    this.#link(b.parent, b.name, src);
    src.ctime = now;
    a.parent.mtime = now;
    b.parent.mtime = now;
    this.#record(["rename", from, to]);
    return E.SUCCESS;
  }

  #link(dir, name, inode) {
    inode.parent = dir;
    inode.name = name;
    inode.nameBytes = null;
    inode.seq = dir.nextSeq++;
    dir.entries.set(name, inode);
  }

  /** A file lost its last name; its data lives until the last descriptor closes. */
  #drop(inode) {
    inode.nlink = 0;
    this.files--;
    this.bytes -= inode.size;
    if (this.dirty.delete(inode)) this.#clearDirty(inode);
    if (inode.opens === 0) this.#free(inode);
  }

  #free(inode) {
    for (const page of inode.pages) if (page) this.allocated -= page.length;
    inode.pages = [];
  }

  /** A descriptor on `inode` closed. */
  release(inode) {
    inode.opens--;
    if (inode.opens === 0 && inode.nlink === 0 && !inode.isDir) this.#free(inode);
  }

  // ---- data ---------------------------------------------------------------------------------

  /** Reads up to `len` bytes at `pos` into `dst[dstOff..]`; returns the count (holes read as zeros). */
  readAt(inode, pos, dst, dstOff, len) {
    if (pos >= inode.size) return 0;
    const n = Math.min(len, inode.size - pos);
    let done = 0;
    while (done < n) {
      const at = pos + done;
      const index = Math.floor(at / PAGE_SIZE);
      const inPage = at - index * PAGE_SIZE;
      const take = Math.min(n - done, PAGE_SIZE - inPage);
      const page = inode.pages[index];
      const have = page ? Math.max(0, Math.min(take, page.length - inPage)) : 0;
      if (have > 0) dst.set(page.subarray(inPage, inPage + have), dstOff + done);
      if (have < take) dst.fill(0, dstOff + done + have, dstOff + done + take);
      done += take;
    }
    return n;
  }

  /** Writes `src[srcOff..srcOff+len]` at `pos`; returns the count or a negated errno. */
  writeAt(inode, pos, src, srcOff, len) {
    if (len === 0) return 0;
    const end = pos + len;
    if (end > inode.size && inode.nlink > 0 && this.bytes + (end - inode.size) > this.maxBytes) return -E.NOSPC;
    let done = 0;
    while (done < len) {
      const at = pos + done;
      const index = Math.floor(at / PAGE_SIZE);
      const inPage = at - index * PAGE_SIZE;
      const take = Math.min(len - done, PAGE_SIZE - inPage);
      const page = this.#page(inode, index, inPage + take);
      page.set(src.subarray(srcOff + done, srcOff + done + take), inPage);
      this.#dirtyPage(inode, index);
      done += take;
    }
    if (end > inode.size) {
      if (inode.nlink > 0) this.bytes += end - inode.size;
      inode.size = end;
    }
    inode.mtime = this.now();
    inode.ctime = inode.mtime;
    this.#dirtyAttr(inode);
    return len;
  }

  /** The page `index` with room for at least `needed` bytes. */
  #page(inode, index, needed) {
    const page = inode.pages[index];
    if (page && page.length >= needed) return page;
    const current = page ? page.length : 0;
    const capacity = Math.min(PAGE_SIZE, Math.max(needed, current * 2, 256));
    const grown = new Uint8Array(capacity);
    if (page) grown.set(page);
    inode.pages[index] = grown;
    this.allocated += capacity - current;
    return grown;
  }

  /** Sets the size: shrinking drops whole pages and zeroes the tail of the last one; growing leaves a hole. */
  truncate(inode, size) {
    const old = inode.size;
    if (size > old && inode.nlink > 0 && this.bytes + (size - old) > this.maxBytes) return E.NOSPC;
    if (size < old) {
      const keep = Math.ceil(size / PAGE_SIZE);
      for (let i = keep; i < inode.pages.length; i++) {
        const page = inode.pages[i];
        if (page) this.allocated -= page.length;
      }
      inode.pages.length = Math.min(inode.pages.length, keep);
      const cut = size - (keep - 1) * PAGE_SIZE;
      const last = keep > 0 ? inode.pages[keep - 1] : undefined;
      if (last && cut < last.length) {
        last.fill(0, cut);
        this.#dirtyPage(inode, keep - 1);
      }
      if (this.journalOn && inode.nlink > 0) inode.truncatedTo = Math.min(inode.truncatedTo ?? size, size);
    }
    if (inode.nlink > 0) this.bytes += size - old;
    inode.size = size;
    inode.mtime = this.now();
    inode.ctime = inode.mtime;
    this.#dirtyAttr(inode);
    return E.SUCCESS;
  }

  /** Sets times (ns, or null to leave one alone). */
  setTimes(inode, atime, mtime) {
    if (atime !== null) inode.atime = atime;
    if (mtime !== null) inode.mtime = mtime;
    inode.ctime = this.now();
    this.#dirtyAttr(inode);
  }

  /** Whole file contents (for the host's live reads). */
  readAll(inode) {
    const out = new Uint8Array(inode.size);
    this.readAt(inode, 0, out, 0, inode.size);
    return out;
  }

  stat(inode) {
    return {
      dev: 1n,
      ino: BigInt(inode.ino),
      filetype: inode.filetype,
      nlink: BigInt(inode.nlink === 0 ? 0 : inode.isDir ? 2 : 1),
      size: BigInt(inode.isDir ? 0 : inode.size),
      atim: inode.atime,
      mtim: inode.mtime,
      ctim: inode.ctime,
    };
  }

  usage() {
    return { files: this.files, dirs: this.dirs, bytes: this.bytes, allocated: this.allocated };
  }

  // ---- journal ------------------------------------------------------------------------------

  /** Whether a flush would carry anything. */
  get journalPending() {
    return this.meta.length > 0 || this.dirty.size > 0;
  }

  /** Rough size of the pending journal, for the flush threshold. */
  get journalBytes() {
    return this.meta.length * 64 + this.dirtyPageCount * PAGE_SIZE;
  }

  #record(op) {
    if (this.journalOn) this.meta.push(op);
  }

  #dirtyPage(inode, index) {
    if (!this.journalOn || inode.nlink === 0) return;
    inode.dirtyPages ??= new Set();
    if (!inode.dirtyPages.has(index)) {
      inode.dirtyPages.add(index);
      this.dirtyPageCount++;
    }
    this.dirty.add(inode);
  }

  #dirtyAttr(inode) {
    if (!this.journalOn || inode.nlink === 0) return;
    inode.attrDirty = true;
    this.dirty.add(inode);
  }

  #clearDirty(inode) {
    if (inode.dirtyPages) this.dirtyPageCount -= inode.dirtyPages.size;
    inode.dirtyPages = null;
    inode.attrDirty = false;
    if (!inode.isDir) inode.truncatedTo = null;
  }

  /**
   * Takes the pending journal: metadata ops in order, then per dirty inode a
   * `size` op (after a shrink), its dirty pages, and an `attr` op. Returns
   * `{ ops, transfer, bytes }`, or null when nothing is pending.
   */
  take() {
    if (!this.journalPending) return null;
    const ops = this.meta;
    this.meta = [];
    const transfer = [];
    let bytes = ops.length * 64;
    for (const inode of this.dirty) {
      if (inode.nlink === 0) {
        this.#clearDirty(inode);
        continue;
      }
      if (!inode.isDir) {
        if (inode.truncatedTo !== null) ops.push(["size", inode.ino, inode.truncatedTo]);
        const pages = Math.ceil(inode.size / PAGE_SIZE);
        const dirty = inode.dirtyPages ? [...inode.dirtyPages].sort((x, y) => x - y) : [];
        for (const index of dirty) {
          if (index >= pages) continue; // truncated away: the size op dropped it
          const page = inode.pages[index];
          const valid = Math.min(PAGE_SIZE, inode.size - index * PAGE_SIZE);
          const out = new Uint8Array(page ? valid : 0);
          if (page) out.set(page.length > valid ? page.subarray(0, valid) : page);
          ops.push(["page", inode.ino, index, out]);
          transfer.push(out.buffer);
          bytes += out.length;
        }
      }
      ops.push(["attr", this.pathOf(inode), inode.type, inode.ino, inode.isDir ? 0 : inode.size, String(inode.atime), String(inode.mtime)]);
      bytes += 64;
      this.#clearDirty(inode);
    }
    this.dirty.clear();
    this.dirtyPageCount = 0;
    return { ops, transfer, bytes, nextIno: this.nextIno };
  }
}
