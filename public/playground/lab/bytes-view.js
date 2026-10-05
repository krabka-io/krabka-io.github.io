// A decoded-bytes viewer: the field tree beside a hex dump, linked both ways.
//
// Every leaf of the tree owns a byte range and a colour; adjacent leaves
// alternate shades, so field boundaries show at a glance. Pointing at a byte
// lights its whole field in both the hex and the ASCII column, on every row
// it spans, and names it in the status line; clicking it selects the field in
// the tree. Pointing at or selecting a tree row lights its range. The data
// inspector reads the bytes at the cursor as every integer width, a varint, a
// float and text, so an undecoded region can still be read by hand.
//
// Keys, with the hex focused: ←/→ the previous or next field, ↑ the
// enclosing field, Home/End the first or last field.
//
// show({ buffers: { main: Uint8Array, ... }, root, captured, size }): the
// tree's nodes carry { label, value, start, end, buf, kind, status, note,
// about, children } (see kafka-decode.js).

import { el, button, fmtNum } from "./dom.js";

const ROW_BYTES = 16;
const MAX_CHILDREN = 400;
const KINDS = ["int", "len", "str", "bytes", "crc", "err", "ts", "key", "value", "tags", "bool"];

let uid = 0;

function* walk(n, depth = 0, parent = null) {
  yield [n, depth, parent];
  for (const c of n.children || []) yield* walk(c, depth + 1, n);
}

function readAs(bytes, at) {
  const left = bytes.length - at;
  if (left <= 0) return [];
  const v = new DataView(bytes.buffer, bytes.byteOffset + at, left);
  const out = [["u8", bytes[at]], ["i8", v.getInt8(0)]];
  if (left >= 2) out.push(["i16 BE", v.getInt16(0)], ["u16 LE", v.getUint16(0, true)]);
  if (left >= 4) out.push(["i32 BE", v.getInt32(0)], ["u32 BE", v.getUint32(0)]);
  if (left >= 8) out.push(["i64 BE", v.getBigInt64(0).toString()], ["f64 BE", Number(v.getFloat64(0).toPrecision(6))]);
  let u = 0;
  let n = 0;
  for (; n < Math.min(5, left); n++) {
    u += (bytes[at + n] & 0x7f) * 2 ** (7 * n);
    if (bytes[at + n] < 0x80) break;
  }
  if (n < 5 && n < left) out.push(["uvarint", `${u} (${n + 1} B)`], ["varint", `${u % 2 ? -(u + 1) / 2 : u / 2}`]);
  const text = Array.from(bytes.subarray(at, at + 12), (b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : "·")).join("");
  out.push(["ASCII", JSON.stringify(text)]);
  return out;
}

export class BytesView {
  constructor(container, { label = "Decoded bytes", empty = "Nothing selected." } = {}) {
    this.id = ++uid;
    this.root = el("div", "lab-bv");
    this.root.setAttribute("role", "group");
    this.root.setAttribute("aria-label", label);
    this.tree = el("div", "lab-bv-tree");
    this.tree.setAttribute("role", "tree");
    this.tree.setAttribute("aria-label", `${label}: fields`);
    const hexCol = el("div", "lab-bv-hexcol");
    this.bufBar = el("div", "lab-bv-bufbar");
    this.hex = el("div", "lab-bv-hex");
    this.hex.tabIndex = 0;
    this.hex.setAttribute("role", "grid");
    this.hex.setAttribute("aria-label", `${label}: bytes`);
    this.spacer = el("div", "lab-bv-spacer");
    this.rows = el("div", "lab-bv-rows");
    this.spacer.appendChild(this.rows);
    this.hex.appendChild(this.spacer);
    this.status = el("div", "lab-bv-status");
    this.status.setAttribute("aria-live", "polite");
    this.inspector = el("dl", "lab-bv-inspect");
    hexCol.append(this.bufBar, this.hex, this.status, this.inspector);
    // The container query on the root stacks the two columns when the box is narrow.
    const inner = el("div", "lab-bv-in");
    inner.append(this.tree, hexCol);
    this.root.appendChild(inner);
    container.appendChild(this.root);
    this.emptyText = empty;
    this.expanded = new Set();
    this.hover = null;
    this.selected = null;
    this.cursor = null;
    this.rowH = 18;
    this.hex.addEventListener("scroll", () => this.schedule());
    this.hex.addEventListener("pointermove", (e) => this.onHexPointer(e));
    this.hex.addEventListener("pointerleave", () => this.setHover(null));
    this.hex.addEventListener("click", (e) => this.onHexClick(e));
    this.hex.addEventListener("keydown", (e) => this.onHexKey(e));
    new ResizeObserver(() => this.schedule()).observe(this.hex);
    this.clear();
  }

  clear(text = this.emptyText) {
    this.root.classList.add("lab-bv-empty");
    this.buffers = {};
    this.rootNode = null;
    this.tree.replaceChildren(el("p", "lab-muted lab-small", text));
    this.bufBar.replaceChildren();
    this.rows.replaceChildren();
    this.spacer.style.height = "0px";
    this.status.textContent = "";
    this.inspector.replaceChildren();
  }

  // buffers: { name: Uint8Array }; root: the decoded tree over them.
  show({ buffers, root, captured = null, size = null, expandDepth = 2, select = null }) {
    this.root.classList.remove("lab-bv-empty");
    this.buffers = buffers;
    this.rootNode = root;
    this.captured = captured;
    this.size = size;
    this.meta = new Map();
    this.leaves = new Map(); // buf -> sorted leaves
    let n = 0;
    for (const [node, depth, parent] of walk(root)) {
      node.buf ??= parent?.buf ?? "main";
      this.meta.set(node, { id: `bv${this.id}-${n++}`, depth, parent });
      if (!node.children?.length && node.end > node.start) {
        if (!this.leaves.has(node.buf)) this.leaves.set(node.buf, []);
        this.leaves.get(node.buf).push(node);
      }
    }
    this.leafAt = new Map();
    for (const [buf, list] of this.leaves) {
      list.sort((a, b) => a.start - b.start || b.end - a.end);
      const bytes = buffers[buf];
      if (!bytes) continue;
      const at = new Int32Array(bytes.length).fill(-1);
      list.forEach((leaf, i) => {
        leaf._shade = i % 2;
        for (let b = leaf.start; b < Math.min(leaf.end, bytes.length); b++) if (at[b] === -1) at[b] = i;
      });
      this.leafAt.set(buf, at);
    }
    this.expanded = new Set();
    for (const [node, depth] of walk(root)) if (depth < expandDepth && node.children?.length) this.expanded.add(node);
    this.hover = null;
    this.selected = null;
    this.view = "main" in buffers ? "main" : Object.keys(buffers)[0];
    this.renderTree();
    this.renderBufBar();
    if (select) this.select(select);
    else this.renderHex(true);
  }

  // ---- tree ----

  renderTree() {
    this.tree.replaceChildren();
    if (!this.rootNode) return;
    const add = (node) => {
      const m = this.meta.get(node);
      const row = el("div", "lab-bv-node");
      row.id = m.id;
      row.setAttribute("role", "treeitem");
      row.setAttribute("aria-level", String(m.depth + 1));
      row.style.paddingLeft = `${0.3 + m.depth * 0.9}rem`;
      row.tabIndex = node === this.selected ? 0 : -1;
      const kids = node.children?.length || 0;
      if (kids) row.setAttribute("aria-expanded", String(this.expanded.has(node)));
      if (node === this.selected) row.classList.add("lab-bv-sel");
      if (node.status) row.dataset.status = node.status;
      const twisty = el("span", "lab-bv-twisty", kids ? (this.expanded.has(node) ? "▾" : "▸") : "");
      const swatch = el("span", `lab-bv-swatch lab-k-${KINDS.includes(node.kind) ? node.kind : "none"}`);
      const label = el("span", "lab-bv-label", node.label);
      const value = el("span", "lab-bv-value", node.value === undefined ? "" : node.value === null ? "null" : String(node.value));
      const range = el("span", "lab-bv-range", node.end > node.start ? `${node.start}–${node.end} · ${fmtNum(node.end - node.start)} B` : "");
      row.append(twisty, swatch, label, value);
      if (node.status === "ok" || node.status === "bad" || node.status === "warn") row.appendChild(el("span", `lab-bv-mark lab-bv-${node.status}`, node.status === "ok" ? "✓" : node.status === "bad" ? "✗" : "!"));
      if (node.note) row.appendChild(el("span", "lab-bv-note", node.note));
      row.appendChild(range);
      if (node.about) row.title = node.about;
      row.addEventListener("click", (e) => {
        if (e.target === twisty && kids) this.toggle(node);
        else this.select(node, { fromTree: true });
      });
      row.addEventListener("dblclick", () => kids && this.toggle(node));
      row.addEventListener("pointerenter", () => this.setHover(node));
      row.addEventListener("pointerleave", () => this.setHover(null));
      row.addEventListener("keydown", (e) => this.onTreeKey(e, node));
      this.tree.appendChild(row);
      if (kids && this.expanded.has(node)) {
        const shown = node._showAll ? node.children : node.children.slice(0, MAX_CHILDREN);
        for (const c of shown) add(c);
        if (shown.length < kids) {
          const more = button(`Show ${fmtNum(kids - shown.length)} more`, "lab-btn-sm lab-bv-more", () => {
            node._showAll = true;
            this.renderTree();
          });
          more.style.marginLeft = `${1.2 + (m.depth + 1) * 0.9}rem`;
          this.tree.appendChild(more);
        }
      }
    };
    add(this.rootNode);
  }

  toggle(node) {
    if (this.expanded.has(node)) this.expanded.delete(node);
    else this.expanded.add(node);
    this.renderTree();
    document.getElementById(this.meta.get(node).id)?.focus();
  }

  onTreeKey(e, node) {
    const rows = [...this.tree.querySelectorAll(".lab-bv-node")];
    const i = rows.findIndex((r) => r.id === this.meta.get(node).id);
    const focusRow = (r) => r && r.focus();
    const nodeOf = (row) => [...this.meta].find(([, m]) => m.id === row.id)?.[0];
    if (e.key === "ArrowDown") focusRow(rows[i + 1]);
    else if (e.key === "ArrowUp") focusRow(rows[i - 1]);
    else if (e.key === "ArrowRight" && node.children?.length && !this.expanded.has(node)) this.toggle(node);
    else if (e.key === "ArrowLeft" && this.expanded.has(node)) this.toggle(node);
    else if (e.key === "ArrowLeft") {
      const p = this.meta.get(node).parent;
      if (p) document.getElementById(this.meta.get(p).id)?.focus();
    } else if (e.key === "Enter" || e.key === " ") this.select(node, { fromTree: true });
    else return;
    e.preventDefault();
    const focused = document.activeElement;
    if (focused?.classList.contains("lab-bv-node")) {
      const n = nodeOf(focused);
      if (n && e.key.startsWith("Arrow")) this.select(n, { fromTree: true, keepFocus: true });
    }
  }

  // Make `node` visible in the tree: expand its ancestors.
  reveal(node) {
    for (let p = this.meta.get(node)?.parent; p; p = this.meta.get(p)?.parent) this.expanded.add(p);
  }

  select(node, { fromTree = false, keepFocus = false } = {}) {
    if (!node || !this.meta.has(node)) return;
    this.selected = node;
    this.cursor = node.start;
    if (node.buf !== this.view && this.buffers[node.buf]) {
      this.view = node.buf;
      this.renderBufBar();
    }
    this.reveal(node);
    // Picked from outside the tree (a batch table, a byte map, a click in the hex): show its fields too.
    if (!fromTree && node.children?.length) this.expanded.add(node);
    const focusTree = fromTree && (keepFocus || this.tree.contains(document.activeElement));
    this.renderTree();
    const row = document.getElementById(this.meta.get(node).id);
    if (row) {
      row.scrollIntoView({ block: "nearest" });
      if (focusTree) row.focus({ preventScroll: true });
    }
    this.scrollToByte(node.start);
    this.renderHex();
    this.describe(node);
  }

  // ---- hex ----

  renderBufBar() {
    this.bufBar.replaceChildren();
    const names = Object.keys(this.buffers || {});
    for (const name of names) {
      const b = this.buffers[name];
      const label = name === "main" ? `bytes · ${fmtNum(b.length)}` : `${name.replace(/^main\+/, "").replace(/@(\d+)$/, " records of batch at byte $1")} · ${fmtNum(b.length)} B`;
      const tab = button(label, `lab-btn-sm${name === this.view ? " lab-bv-buf-on" : ""}`, () => {
        this.view = name;
        this.renderBufBar();
        this.renderHex(true);
      });
      tab.setAttribute("aria-pressed", String(name === this.view));
      if (names.length > 1 || name !== "main") this.bufBar.appendChild(tab);
    }
    if (this.size != null && this.captured != null && this.captured < this.size) {
      this.bufBar.appendChild(el("span", "lab-bv-trunc", `captured ${fmtNum(this.captured)} of ${fmtNum(this.size)} bytes`));
    }
  }

  scrollToByte(at) {
    const row = Math.floor(at / ROW_BYTES);
    const top = row * this.rowH;
    const h = this.hex.clientHeight || 200;
    if (top < this.hex.scrollTop || top > this.hex.scrollTop + h - this.rowH * 2) this.hex.scrollTop = Math.max(0, top - h / 3);
  }

  schedule() {
    if (this.frame) return;
    this.frame = requestAnimationFrame(() => {
      this.frame = 0;
      this.renderHex();
    });
  }

  renderHex(reset = false) {
    const bytes = this.buffers?.[this.view];
    if (!bytes) {
      this.rows.replaceChildren();
      this.spacer.style.height = "0px";
      return;
    }
    if (reset) this.hex.scrollTop = 0;
    const total = Math.ceil(bytes.length / ROW_BYTES);
    this.spacer.style.height = `${total * this.rowH}px`;
    const first = Math.max(0, Math.floor(this.hex.scrollTop / this.rowH) - 4);
    const last = Math.min(total, first + Math.ceil((this.hex.clientHeight || 300) / this.rowH) + 8);
    const leaves = this.leaves.get(this.view) || [];
    const at = this.leafAt.get(this.view);
    const hov = this.hover && this.hover.buf === this.view ? this.hover : null;
    const sel = this.selected && this.selected.buf === this.view ? this.selected : null;
    const frag = document.createDocumentFragment();
    for (let r = first; r < last; r++) {
      const row = el("div", "lab-bv-row");
      row.style.top = `${r * this.rowH}px`;
      row.setAttribute("role", "row");
      row.appendChild(el("span", "lab-bv-off", (r * ROW_BYTES).toString(16).padStart(8, "0")));
      const hexPart = el("span", "lab-bv-hx");
      const ascPart = el("span", "lab-bv-asc");
      for (let i = r * ROW_BYTES; i < Math.min(bytes.length, (r + 1) * ROW_BYTES); i++) {
        const leafIndex = at ? at[i] : -1;
        const leaf = leafIndex >= 0 ? leaves[leafIndex] : null;
        let cls = "lab-bv-b";
        if (leaf) cls += ` lab-k-${KINDS.includes(leaf.kind) ? leaf.kind : "none"} lab-s${leaf._shade}`;
        if (leaf?.status === "bad") cls += " lab-bv-badbyte";
        if (hov && i >= hov.start && i < hov.end) cls += " lab-bv-hov";
        if (sel && i >= sel.start && i < sel.end) cls += " lab-bv-on";
        if (i === this.cursor) cls += " lab-bv-cur";
        const b = bytes[i];
        const h = el("span", cls, b.toString(16).padStart(2, "0"));
        h.dataset.i = i;
        const a = el("span", cls, b >= 32 && b < 127 ? String.fromCharCode(b) : "·");
        a.dataset.i = i;
        hexPart.appendChild(h);
        ascPart.appendChild(a);
      }
      row.append(hexPart, ascPart);
      frag.appendChild(row);
    }
    this.rows.replaceChildren(frag);
  }

  byteFrom(e) {
    const t = e.target.closest?.("[data-i]");
    return t ? Number(t.dataset.i) : null;
  }

  leafFor(i) {
    const at = this.leafAt.get(this.view);
    const idx = at ? at[i] : -1;
    return idx >= 0 ? this.leaves.get(this.view)[idx] : null;
  }

  // The deepest node of the current buffer that holds byte `i`.
  deepest(i) {
    let best = null;
    let depth = -1;
    for (const [node, m] of this.meta) {
      if (node.buf === this.view && i >= node.start && i < node.end && m.depth > depth) {
        best = node;
        depth = m.depth;
      }
    }
    return best;
  }

  onHexPointer(e) {
    const i = this.byteFrom(e);
    if (i == null) return;
    const leaf = this.leafFor(i) || this.deepest(i);
    if (leaf !== this.hover) this.setHover(leaf, i);
    this.inspect(i);
  }

  onHexClick(e) {
    const i = this.byteFrom(e);
    if (i == null) return;
    const node = this.leafFor(i) || this.deepest(i);
    if (node) this.select(node);
    this.cursor = i;
    this.inspect(i);
    this.renderHex();
  }

  onHexKey(e) {
    const leaves = this.leaves.get(this.view) || [];
    if (!leaves.length) return;
    const cur = this.selected && this.selected.buf === this.view ? this.selected : null;
    const idx = cur ? leaves.indexOf(cur) : -1;
    let next = null;
    if (e.key === "ArrowRight") next = leaves[Math.min(leaves.length - 1, idx + 1)];
    else if (e.key === "ArrowLeft") next = leaves[Math.max(0, idx - 1)];
    else if (e.key === "Home") next = leaves[0];
    else if (e.key === "End") next = leaves[leaves.length - 1];
    else if (e.key === "ArrowUp" && cur) next = this.meta.get(cur).parent;
    else return;
    e.preventDefault();
    if (next) this.select(next);
  }

  setHover(node, i = null) {
    this.hover = node;
    if (node) this.describe(node, i);
    else if (this.selected) this.describe(this.selected);
    else this.status.textContent = "";
    this.schedule();
  }

  path(node) {
    const parts = [];
    for (let n = node; n && n !== this.rootNode; n = this.meta.get(n)?.parent) parts.unshift(n.label);
    return parts.join(" › ");
  }

  describe(node, i = null) {
    const v = node.value === undefined ? "" : ` = ${node.value === null ? "null" : node.value}`;
    const mark = node.status === "ok" ? " ✓" : node.status === "bad" ? " ✗" : node.status === "warn" ? " !" : "";
    const where = node.end > node.start ? ` · bytes ${node.start}–${node.end} (${fmtNum(node.end - node.start)} B)` : "";
    this.status.replaceChildren(
      el("strong", null, this.path(node) || node.label),
      `${v}${mark}${where}${i != null ? ` · byte ${i}` : ""}`,
      node.note ? el("span", "lab-bv-note", ` ${node.note}`) : "",
    );
    if (node.about) this.status.appendChild(el("span", "lab-bv-about", node.about));
    if (i == null) this.inspect(node.start);
  }

  inspect(i) {
    const bytes = this.buffers?.[this.view];
    this.inspector.replaceChildren();
    if (!bytes || i == null || i >= bytes.length) return;
    this.inspector.appendChild(el("dt", "lab-bv-inspect-head", `at byte ${i}`));
    for (const [k, v] of readAs(bytes, i)) this.inspector.append(el("dt", null, k), el("dd", null, String(v)));
  }
}
