// The Logs tab: a viewer for what the real brokers write, and the dialog that
// sets how much they write.
//
// `LogStore` (logstore.js) holds the lines; this panel filters them and draws
// the ones in view. Only the rows on screen are in the DOM, so 20,000 lines
// scroll as smoothly as twenty. A row is one line tall until it is expanded
// into its record (or, with Wrap on, until its message wraps): the heights of
// those rows are measured once they are drawn, and the rest are assumed to be
// one line. Focus stays on the list; the active row is named through
// `aria-activedescendant`, so a row that scrolls out of the DOM never takes
// the focus with it.

import { el, button, select, labelled, fmtNum, shortJson, copyToClipboard, download, debounce } from "./dom.js";
import { jsonTree } from "./json-tree.js";
import { openDialog } from "./forms.js";
import { LEVELS, filterEntries, parseDirective, parseQuery, toNdjson, DIRECTIVE_EXAMPLES } from "./logparse.js";

const RENDER_MS = 100; // the longest a new line waits before the list redraws
const ANNOUNCE_MS = 5000; // the fewest ms between two "new errors" announcements
const OVERSCAN_PX = 240;
const DETAIL_ESTIMATE_PX = 180;
const NARROW_PX = 640; // below this the filters and the less-used buttons fold away
const PRESETS = [
  { value: "warn", label: "Quiet", note: "warn" },
  { value: "", label: "Normal", note: "the default" },
  { value: "debug", label: "Verbose", note: "debug" },
  { value: "trace", label: "Trace", note: "trace" },
];

let counter = 0;

// Lab time of a line as seconds to the millisecond, the way the broker stamps it.
const fmtLab = (ms) => `${(ms / 1000).toFixed(3)}s`;

/** What a directive means, in words. */
export function describeDirective(directive) {
  const parsed = parseDirective(directive);
  if (!parsed.ok) return parsed.error;
  if (!parsed.entries.length) return "the broker's default: info, with its request log at warn";
  return parsed.entries.map((e) => (e.target === null ? `everything at ${e.level}` : `${e.target} at ${e.level}`)).join("; ");
}

export class LogsPanel {
  // hooks:
  //   store                a LogStore
  //   levels               a LogLevels
  //   dialogRoot           the element dialogs open in
  //   scenarioId()         the scenario whose level settings apply
  //   nodes()              [{ id, name, color }] the nodes whose lines can appear
  //   brokers()            [{ id, name, alive, level }] real brokers: `level` is the directive the running process
  //                        started with, or null when none has started
  //   apply({ scope, directive, restart })   saves the choice and restarts the brokers in `restart`
  //   toast(message)       a short notice
  //   onBadge({ count, errors, warns })      the tab's badge
  constructor(container, hooks) {
    this.hooks = hooks;
    this.store = hooks.store;
    this.uid = `lab-logs-${++counter}`;
    this.filter = { minLevel: "TRACE", nodes: new Set(), targets: new Set(), prefix: "", text: "" };
    this.query = parseQuery("");
    this.follow = true;
    this.wrap = false;
    this.narrow = null; // unknown until the panel has a width
    this.view = [];
    this.offsets = new Float64Array(1);
    this.layoutDirty = true;
    this.items = new Map(); // seq → { el, head, detail } of the rows in the DOM
    this.heights = new Map(); // seq → measured px, for rows that are expanded or wrapped
    this.expanded = new Set();
    this.active = null; // seq of the row the keyboard is on
    this.anchor = null; // { seq, delta }: where the top of the list sits, kept while lines come and go
    this.rowH = 0;
    this.width = 0;
    this.pauseSeq = 0;
    this.nodeKey = "";
    this.targetRows = new Map();
    this.folds = [];
    this.timer = 0;
    this.seen = this.store.seq;
    this.pendingErrors = 0;
    this.lastAnnounce = 0;
    this.announceTimer = 0;
    this.build(container);
    this.store.subscribe(() => this.onStore());
    new ResizeObserver(() => this.onResize()).observe(this.root);
    // "/" reaches the search from the tab strip or anywhere in the panel.
    document.addEventListener("keydown", (e) => this.onGlobalKey(e));
    this.refresh();
  }

  // ---- layout ---------------------------------------------------------------------------------------

  build(container) {
    this.root = el("section", "lab-logs");
    this.root.setAttribute("aria-label", "Broker logs");
    const bar = el("div", "lab-logs-bar");
    this.search = el("input", "lab-input lab-input-sm lab-logs-search");
    this.search.type = "search";
    this.search.placeholder = "Search or level:warn";
    this.search.setAttribute("aria-label", "Search log lines; field:value terms match a field, such as node:2 or level:warn");
    this.search.dataset.field = "log-search";
    const applySearch = debounce(() => this.setText(this.search.value), 120);
    this.search.addEventListener("input", applySearch);
    this.search.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        if (this.search.value) {
          this.search.value = "";
          this.setText("");
        } else this.scroll.focus();
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        this.setText(this.search.value);
        this.focusList();
      }
    });
    this.followBtn = button("Follow", "lab-btn-sm lab-logs-follow", () => this.setFollow(!this.follow, { jump: true }), { title: "Keep the newest line in view; scrolling up pauses it" });
    this.followBtn.dataset.field = "log-follow";
    this.levelsBtn = button("Log levels…", "lab-btn-sm", () => this.openLevels(), { title: "How much the brokers log, and a restart on their disks to apply it" });
    this.levelsBtn.dataset.field = "log-levels";
    const more = this.fold("More", "lab-logs-more");
    this.wrapBtn = button("Wrap", "lab-btn-sm", () => this.setWrap(!this.wrap), { title: "Wrap long messages instead of cutting them" });
    this.wrapBtn.dataset.field = "log-wrap";
    this.clearBtn = button("Clear", "lab-btn-sm", () => this.clear(), { title: "Forget every line held in this tab" });
    this.downloadBtn = button("Download NDJSON", "lab-btn-sm", () => this.downloadView(), { title: "The lines shown, as the broker wrote them, one per line" });
    this.downloadBtn.dataset.field = "log-download";
    this.copyBtn = button("Copy visible", "lab-btn-sm", () => this.copyView(), { title: "Copy the lines shown" });
    this.copyBtn.dataset.field = "log-copy-visible";
    more.body.append(this.wrapBtn, this.clearBtn, this.downloadBtn, this.copyBtn);

    const filters = this.fold("Filters", "lab-logs-filters");
    this.filtersToggle = filters.toggle;
    const levelSet = el("fieldset", "lab-logs-levelset");
    levelSet.appendChild(el("legend", "lab-sr", "Minimum level"));
    const seg = el("div", "lab-seg");
    this.levelCounts = {};
    for (const level of LEVELS) {
      const label = el("label", `lab-seg-opt lab-lv-${level.toLowerCase()}`);
      const radio = el("input");
      radio.type = "radio";
      radio.name = `${this.uid}-min`;
      radio.value = level;
      radio.checked = level === this.filter.minLevel;
      radio.addEventListener("change", () => {
        this.filter.minLevel = level;
        this.refresh();
      });
      const count = el("span", "lab-seg-n", "0");
      this.levelCounts[level] = count;
      label.append(radio, el("span", "lab-seg-name", level), count);
      seg.appendChild(label);
    }
    levelSet.appendChild(seg);
    this.chips = el("div", "lab-logs-chips");
    this.chips.setAttribute("role", "group");
    this.chips.setAttribute("aria-label", "Nodes to show; none chosen shows all");
    this.targets = el("details", "lab-logs-targets");
    this.targetSummary = el("summary", null, "Targets");
    const targetBox = el("div", "lab-logs-targetbox");
    this.prefix = el("input", "lab-input lab-input-sm");
    this.prefix.type = "search";
    this.prefix.placeholder = "target prefix, e.g. krabka_broker::raft";
    this.prefix.setAttribute("aria-label", "Show only targets that start with");
    this.prefix.addEventListener("input", debounce(() => {
      this.filter.prefix = this.prefix.value.trim();
      this.refresh();
    }, 120));
    this.targetList = el("div", "lab-logs-targetlist");
    targetBox.append(this.prefix, this.targetList);
    this.targets.append(this.targetSummary, targetBox);
    filters.body.append(levelSet, this.chips, this.targets);
    bar.append(this.search, this.followBtn, this.levelsBtn, more.box, filters.box);

    const status = el("div", "lab-logs-status");
    this.count = el("span", "lab-logs-count", "0 lines");
    this.pill = button("", "lab-btn-sm lab-logs-pill", () => this.setFollow(true, { jump: true }));
    this.pill.hidden = true;
    this.pill.dataset.field = "log-new";
    status.append(this.count, this.pill);

    this.scroll = el("div", "lab-logs-scroll");
    this.scroll.tabIndex = 0;
    this.scroll.setAttribute("role", "listbox");
    this.scroll.setAttribute("aria-label", "Log lines. Use the arrow keys or J and K to move, Enter to open a line");
    this.scroll.dataset.field = "log-list";
    this.body = el("div", "lab-logs-body");
    this.scroll.appendChild(this.body);
    this.scroll.addEventListener("scroll", () => this.onScroll());
    this.scroll.addEventListener("keydown", (e) => this.onListKey(e));
    this.empty = el("div", "lab-logs-empty");
    this.live = el("div", "lab-sr");
    this.live.setAttribute("role", "status");
    this.live.setAttribute("aria-live", "polite");
    this.root.append(bar, status, this.empty, this.scroll, this.live);
    container.appendChild(this.root);
    this.renderToggles();
  }

  // A block of controls that folds behind a button when the panel is narrow and is always open when it is wide.
  fold(label, cls) {
    const box = el("div", `lab-logs-fold ${cls}`);
    const body = el("div", "lab-logs-foldbody");
    body.id = `${this.uid}-fold-${this.folds.length}`;
    const toggle = button(label, "lab-btn-sm lab-logs-foldbtn", () => this.setFold(box, box.dataset.open !== "true"));
    toggle.setAttribute("aria-controls", body.id);
    box.append(toggle, body);
    box.toggle = toggle;
    this.folds.push(box);
    this.setFold(box, true);
    return { box, toggle, body };
  }

  setFold(box, open) {
    box.dataset.open = String(open);
    box.toggle.setAttribute("aria-expanded", String(open));
  }

  onResize() {
    const width = this.root.clientWidth;
    if (!width) return;
    const narrow = width < NARROW_PX;
    if (narrow !== this.narrow) {
      this.narrow = narrow;
      this.root.dataset.narrow = String(narrow);
      for (const box of this.folds) this.setFold(box, !narrow);
    }
    if (width !== this.width && this.wrap) this.heights.clear();
    this.width = width;
    this.rowH = 0;
    this.layoutDirty = true;
    if (this.stale) this.refresh();
    else this.render();
  }

  /** The tab was opened: the list could not be measured while it was hidden. */
  shown() {
    this.refresh();
  }

  focusSearch() {
    this.search.focus();
    this.search.select();
  }

  focusList() {
    this.scroll.focus();
    if (this.active == null || !this.view.some((e) => e.seq === this.active)) this.activate(this.view[this.indexAt(this.scroll.scrollTop)]?.seq ?? null);
  }

  // ---- state ----------------------------------------------------------------------------------------

  setText(text) {
    if (text === this.filter.text) return;
    this.filter.text = text;
    this.query = parseQuery(text);
    this.refresh();
  }

  setFollow(on, { jump = false } = {}) {
    if (on === this.follow && !jump) return;
    this.follow = on;
    if (!on) this.pauseSeq = this.store.seq;
    this.renderToggles();
    if (on && jump) this.render();
    this.updateStatus();
  }

  setWrap(on) {
    this.wrap = on;
    this.heights.clear();
    this.layoutDirty = true;
    this.renderToggles();
    this.render();
  }

  renderToggles() {
    this.followBtn.setAttribute("aria-pressed", String(this.follow));
    this.wrapBtn.setAttribute("aria-pressed", String(this.wrap));
    this.scroll.classList.toggle("lab-logs-wrap", this.wrap);
  }

  clear() {
    this.store.clear();
    this.expanded.clear();
    this.heights.clear();
    this.announce("Logs cleared");
  }

  clearFilters() {
    this.filter.minLevel = "TRACE";
    this.filter.nodes.clear();
    this.filter.targets.clear();
    this.filter.prefix = "";
    this.prefix.value = "";
    this.search.value = "";
    this.setText("");
    for (const radio of this.root.querySelectorAll(`input[name="${this.uid}-min"]`)) radio.checked = radio.value === "TRACE";
    for (const box of this.targetList.querySelectorAll("input")) box.checked = false;
    this.refresh();
  }

  nodeInfo(id) {
    return this.nodeMap?.get(id) ?? { id, name: `#${id}`, color: "" };
  }

  announce(text) {
    this.live.textContent = "";
    requestAnimationFrame(() => (this.live.textContent = text));
  }

  // New ERROR lines are announced in one sentence at most every few seconds,
  // so a burst of errors is one message, not a stream.
  noteErrors() {
    const fresh = this.store.since(this.seen, "ERROR");
    this.seen = this.store.seq;
    if (!fresh.length) return;
    this.pendingErrors += fresh.length;
    if (this.announceTimer) return;
    this.announceTimer = setTimeout(() => {
      this.announceTimer = 0;
      this.lastAnnounce = Date.now();
      const n = this.pendingErrors;
      this.pendingErrors = 0;
      this.announce(`${n} new error${n === 1 ? "" : "s"}`);
    }, Math.max(0, this.lastAnnounce + ANNOUNCE_MS - Date.now()));
  }

  onStore() {
    this.noteErrors();
    if (!this.timer) this.timer = setTimeout(() => this.refresh(), RENDER_MS);
  }

  // ---- filtering and the bar ---------------------------------------------------------------------------

  refresh() {
    clearTimeout(this.timer);
    this.timer = 0;
    this.updateBadge();
    // A hidden tab has nothing to draw: the next time it is shown it filters everything once.
    this.stale = !this.root.getClientRects().length;
    if (this.stale) return;
    const entries = this.store.entries();
    this.syncNodes();
    this.view = filterEntries(entries, { ...this.filter, query: this.query }, (id) => this.nodeInfo(id).name);
    this.layoutDirty = true;
    this.updateBar();
    this.render();
  }

  syncNodes() {
    const list = [...(this.hooks.nodes() || [])];
    for (const id of this.store.queues.keys()) if (!list.some((n) => n.id === id)) list.push({ id, name: `#${id}`, color: "" });
    const key = list.map((n) => `${n.id}:${n.name}:${n.color}`).join("|");
    if (key === this.nodeKey) return;
    this.nodeKey = key;
    this.nodeMap = new Map(list.map((n) => [n.id, n]));
    // Rows carry their node's name: they are drawn again under the new names.
    for (const item of this.items.values()) item.el.remove();
    this.items.clear();
    this.chips.replaceChildren();
    for (const n of list) {
      const chip = el("button", "lab-nchip");
      chip.type = "button";
      chip.setAttribute("aria-pressed", String(this.filter.nodes.has(n.id)));
      chip.dataset.node = String(n.id);
      if (n.color) chip.style.setProperty("--chip", n.color);
      chip.append(el("span", "lab-nchip-dot"), document.createTextNode(n.name));
      chip.addEventListener("click", () => {
        if (this.filter.nodes.has(n.id)) this.filter.nodes.delete(n.id);
        else this.filter.nodes.add(n.id);
        chip.setAttribute("aria-pressed", String(this.filter.nodes.has(n.id)));
        this.refresh();
      });
      this.chips.appendChild(chip);
    }
  }

  // Counts, the target list and the small texts. Targets are kept in place (added, removed, recounted) rather
  // than rebuilt, so a checkbox under the pointer is still there after the next line.
  updateBar() {
    const { levels, targets } = this.store;
    for (const level of LEVELS) this.levelCounts[level].textContent = fmtNum(levels[level]);
    for (const [target, n] of targets) {
      let row = this.targetRows.get(target);
      if (!row) {
        const label = el("label", "lab-logs-target");
        const box = el("input");
        box.type = "checkbox";
        box.checked = this.filter.targets.has(target);
        box.addEventListener("change", () => {
          if (box.checked) this.filter.targets.add(target);
          else this.filter.targets.delete(target);
          this.refresh();
        });
        const num = el("span", "lab-seg-n", "0");
        label.append(box, el("span", "lab-logs-tname", target), num);
        row = { label, num };
        this.targetRows.set(target, row);
        const next = [...this.targetList.children].find((c) => c.querySelector(".lab-logs-tname").textContent > target);
        this.targetList.insertBefore(label, next || null);
      }
      row.num.textContent = fmtNum(n);
      row.label.hidden = Boolean(this.filter.prefix) && !target.startsWith(this.filter.prefix);
    }
    for (const [target, row] of this.targetRows) {
      // A chosen target keeps its row at zero, so the reader can unchoose it.
      if (targets.has(target) || this.filter.targets.has(target)) continue;
      row.label.remove();
      this.targetRows.delete(target);
    }
    this.targetSummary.textContent = this.filter.targets.size ? `Targets · ${this.filter.targets.size} chosen` : `Targets · ${fmtNum(targets.size)}`;
    const active = (this.filter.minLevel !== "TRACE") + (this.filter.nodes.size > 0) + (this.filter.targets.size > 0 || Boolean(this.filter.prefix));
    this.filtersToggle.textContent = active ? `Filters · ${active} on` : "Filters";
    const hasLines = this.view.length > 0;
    this.downloadBtn.disabled = !hasLines;
    this.copyBtn.disabled = !hasLines;
    this.clearBtn.disabled = this.store.size === 0;
    this.updateStatus();
  }

  updateBadge() {
    const { warn, error } = this.store.attention;
    this.hooks.onBadge({ count: warn + error, errors: error, warns: warn });
  }

  updateStatus() {
    const total = this.store.size;
    const shown = this.view.length;
    this.count.textContent = shown === total ? `${fmtNum(total)} ${total === 1 ? "line" : "lines"}` : `${fmtNum(shown)} of ${fmtNum(total)} lines`;
    let fresh = 0;
    if (!this.follow) for (let i = shown - 1; i >= 0 && this.view[i].seq > this.pauseSeq; i--) fresh++;
    this.pill.hidden = fresh === 0;
    if (fresh) this.pill.textContent = `${fmtNum(fresh)} new ${fresh === 1 ? "line" : "lines"} ↓`;
    this.empty.hidden = shown > 0;
    if (shown > 0) return;
    this.empty.replaceChildren();
    if (total) {
      this.empty.append(`${fmtNum(total)} ${total === 1 ? "line is" : "lines are"} hidden by the filters. `, button("Clear filters", "lab-btn-sm", () => this.clearFilters()));
    } else if (!(this.hooks.brokers() || []).length) {
      this.empty.textContent = "This scenario has no real broker, so there is nothing to log. Load a preset from Scenarios.";
    } else {
      this.empty.textContent = "No log lines yet. They appear here as the brokers write them.";
    }
  }

  // ---- the virtual list --------------------------------------------------------------------------------

  // Height of a one-line row, measured from a probe: the CSS sets it, and it grows under a larger font or a touch screen.
  measureRow() {
    if (this.rowH) return;
    const probe = this.makeItem({ seq: 0, node: 0, marker: null, level: "INFO", at: 0, target: "t", message: "m", fields: {}, stream: "stderr" });
    this.body.appendChild(probe.el);
    this.rowH = probe.el.offsetHeight || 22;
    probe.el.remove();
  }

  heightOf(e) {
    if (!this.wrap && !this.expanded.has(e.seq)) return this.rowH;
    return this.heights.get(e.seq) ?? (this.expanded.has(e.seq) ? this.rowH + DETAIL_ESTIMATE_PX : this.rowH);
  }

  layout() {
    const n = this.view.length;
    const offsets = new Float64Array(n + 1);
    for (let i = 0; i < n; i++) offsets[i + 1] = offsets[i] + this.heightOf(this.view[i]);
    this.offsets = offsets;
    this.layoutDirty = false;
  }

  // The row the pixel `y` falls in.
  indexAt(y) {
    const { offsets } = this;
    let lo = 0;
    let hi = this.view.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (offsets[mid] <= y) lo = mid;
      else hi = mid - 1;
    }
    return Math.max(0, lo);
  }

  // The view is ordered by `seq`: the row for `seq`, or the first row after it.
  indexOfSeq(seq) {
    let lo = 0;
    let hi = this.view.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.view[mid].seq < seq) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  captureAnchor() {
    if (!this.view.length) {
      this.anchor = null;
      return;
    }
    const top = this.scroll.scrollTop;
    const i = this.indexAt(top);
    this.anchor = { seq: this.view[i].seq, delta: top - this.offsets[i] };
  }

  render() {
    const { scroll, view } = this;
    const viewport = scroll.clientHeight;
    if (!viewport) return;
    this.measureRow();
    let top = scroll.scrollTop;
    for (let pass = 0; pass < 3; pass++) {
      if (this.layoutDirty) this.layout();
      const n = view.length;
      const maxTop = Math.max(0, this.offsets[n] - viewport);
      if (this.follow) top = maxTop;
      else if (this.anchor && n) top = this.offsets[Math.min(this.indexOfSeq(this.anchor.seq), n - 1)] + this.anchor.delta;
      top = Math.min(Math.max(0, top), maxTop);
      const first = n ? this.indexAt(top - OVERSCAN_PX) : 0;
      const last = n ? this.indexAt(top + viewport + OVERSCAN_PX) : -1;
      this.reconcile(view.slice(first, last + 1));
      this.body.style.paddingTop = `${n ? this.offsets[first] : 0}px`;
      this.body.style.paddingBottom = `${n ? this.offsets[n] - this.offsets[last + 1] : 0}px`;
      if (!this.measure()) break;
      this.layoutDirty = true;
    }
    if (Math.abs(scroll.scrollTop - top) > 0.5) scroll.scrollTop = top;
    this.captureAnchor();
    this.syncActive();
    this.updateStatus();
  }

  // Reads back the height of the rows that are not one line tall; true when any differs from what the layout assumed.
  measure() {
    let changed = false;
    for (const [seq, item] of this.items) {
      if (!this.wrap && !this.expanded.has(seq)) continue;
      const h = item.el.offsetHeight;
      if (Math.abs(h - (this.heights.get(seq) ?? -1)) > 0.5) {
        this.heights.set(seq, h);
        changed = true;
      }
    }
    return changed;
  }

  // Makes the DOM rows the ones in `want`, in order, touching only what changed (a row the reader has open or selected text in stays).
  reconcile(want) {
    const keep = new Set(want.map((e) => e.seq));
    for (const [seq, item] of this.items) {
      if (keep.has(seq)) continue;
      if (item.el.contains(document.activeElement)) this.scroll.focus({ preventScroll: true });
      item.el.remove();
      this.items.delete(seq);
    }
    let ref = this.body.firstChild;
    for (const e of want) {
      let item = this.items.get(e.seq);
      if (!item) {
        item = this.makeItem(e);
        this.items.set(e.seq, item);
      }
      this.syncItem(item, e);
      if (item.el === ref) ref = ref.nextSibling;
      else this.body.insertBefore(item.el, ref);
    }
  }

  makeItem(e) {
    const item = el("div", `lab-lgrow lab-lv-${e.level.toLowerCase()}${e.marker ? " lab-log-marker" : ""}`);
    item.setAttribute("role", "none");
    item.dataset.seq = String(e.seq);
    const head = el("div", "lab-log-head");
    head.setAttribute("role", "option");
    head.id = `${this.uid}-${e.seq}`;
    const info = this.nodeInfo(e.node);
    const node = el("span", "lab-log-node");
    if (info.color) node.style.setProperty("--chip", info.color);
    node.append(el("span", "lab-nchip-dot"), document.createTextNode(info.name));
    const message = el("span", "lab-log-msg", e.message);
    const extra = e.marker ? "" : shortJson(e.fields, 240);
    if (extra) message.append(" ", el("span", "lab-log-kv", extra));
    const level = el("span", "lab-log-level", e.marker ? "PROC" : e.level);
    if (e.format === "raw") level.appendChild(el("small", "lab-log-raw", "raw"));
    head.append(el("span", "lab-log-time", fmtLab(e.at)), level, node, el("span", "lab-log-target", e.target || "–"), message);
    head.addEventListener("click", () => {
      // A drag across the text selects it; it should not also open the row.
      if (String(window.getSelection?.() ?? "")) return;
      this.activate(e.seq);
      this.toggle(e.seq);
    });
    item.appendChild(head);
    return { el: item, head, detail: null };
  }

  syncItem(item, e) {
    const open = this.expanded.has(e.seq);
    const active = this.active === e.seq;
    item.head.setAttribute("aria-expanded", String(open));
    item.head.setAttribute("aria-selected", String(active));
    item.el.classList.toggle("lab-log-open", open);
    item.el.classList.toggle("lab-log-active", active);
    if (open && !item.detail) {
      item.detail = this.makeDetail(e);
      item.el.appendChild(item.detail);
    } else if (!open && item.detail) {
      item.detail.remove();
      item.detail = null;
    }
  }

  // The whole record: the message in full (the tree cuts long strings), the fields as a tree, and Copy.
  makeDetail(e) {
    const box = el("div", "lab-log-detail");
    box.setAttribute("role", "group");
    box.setAttribute("aria-label", `Record of the line from ${this.nodeInfo(e.node).name}`);
    const bar = el("div", "lab-log-detail-bar");
    const kind = e.marker ? "process marker" : e.format === "json" ? "JSON line" : e.format === "text" ? "text line, read into fields" : "raw line: not JSON";
    const copy = (text, label) => button(label, "lab-btn-sm", async () => this.hooks.toast((await copyToClipboard(text)) ? "Copied" : "The browser refused to copy"));
    bar.append(el("span", "lab-muted", `${kind} · ${e.stream}`));
    if (e.record) bar.append(copy(JSON.stringify(e.record, null, 2), "Copy JSON"));
    bar.append(copy(e.raw, "Copy line"));
    box.appendChild(bar);
    if (e.message.length > 120 || e.message.includes("\n") || !e.record) box.appendChild(el("pre", "lab-log-full", e.message));
    if (e.record) box.appendChild(jsonTree(e.record, { openDepth: 1 }));
    return box;
  }

  syncActive() {
    const item = this.active == null ? null : this.items.get(this.active);
    if (item) this.scroll.setAttribute("aria-activedescendant", item.head.id);
    else this.scroll.removeAttribute("aria-activedescendant");
  }

  onScroll() {
    const atEnd = this.scroll.scrollHeight - this.scroll.scrollTop - this.scroll.clientHeight <= 4;
    if (atEnd !== this.follow) this.setFollow(atEnd);
    // The reader moved the list: that is the place to keep, not the one the last draw left.
    this.captureAnchor();
    this.render();
  }

  // ---- the keyboard ---------------------------------------------------------------------------------------

  activate(seq) {
    this.active = seq;
    const item = seq == null ? null : this.items.get(seq);
    for (const [s, it] of this.items) this.syncItem(it, this.viewEntry(s) ?? { seq: s });
    if (seq != null && !item) this.revealSeq(seq);
    this.syncActive();
  }

  viewEntry(seq) {
    const i = this.indexOfSeq(seq);
    return this.view[i]?.seq === seq ? this.view[i] : null;
  }

  // Scrolls just far enough to show the row.
  revealSeq(seq) {
    const i = this.indexOfSeq(seq);
    if (this.view[i]?.seq !== seq) return;
    const { scroll, offsets } = this;
    if (offsets[i] < scroll.scrollTop) scroll.scrollTop = offsets[i];
    else if (offsets[i + 1] > scroll.scrollTop + scroll.clientHeight) scroll.scrollTop = offsets[i + 1] - scroll.clientHeight;
    this.captureAnchor();
    this.render();
  }

  toggle(seq) {
    if (this.expanded.has(seq)) {
      this.expanded.delete(seq);
    } else {
      this.expanded.add(seq);
      // Reading a record is the opposite of following the newest line.
      this.setFollow(false);
    }
    this.layoutDirty = true;
    this.captureAnchor();
    this.render();
  }

  move(delta) {
    const n = this.view.length;
    if (!n) return;
    const from = this.active == null ? this.indexAt(this.scroll.scrollTop) - Math.sign(delta) : this.indexOfSeq(this.active);
    const to = Math.min(n - 1, Math.max(0, from + delta));
    this.activate(this.view[to].seq);
    this.revealSeq(this.view[to].seq);
  }

  onListKey(e) {
    if (e.target !== this.scroll) {
      // Inside an open record: Escape closes it and returns to the list.
      const open = e.target.closest?.(".lab-lgrow");
      if (e.key === "Escape" && open) {
        e.preventDefault();
        e.stopPropagation();
        this.expanded.delete(Number(open.dataset.seq));
        this.layoutDirty = true;
        this.scroll.focus();
        this.render();
      }
      return;
    }
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    const page = Math.max(1, Math.floor(this.scroll.clientHeight / (this.rowH || 22)) - 1);
    switch (e.key) {
      case "ArrowDown":
      case "j":
        this.move(1);
        break;
      case "ArrowUp":
      case "k":
        this.move(-1);
        break;
      case "PageDown":
        this.move(page);
        break;
      case "PageUp":
        this.move(-page);
        break;
      case "Home":
        this.move(-this.view.length);
        break;
      case "End":
        this.move(this.view.length);
        break;
      case "Enter":
      case " ":
        if (this.active != null) this.toggle(this.active);
        break;
      case "Escape":
        // The innermost thing first: the active record, then every open record, then the search text.
        if (this.active != null && this.expanded.has(this.active)) this.toggle(this.active);
        else if (this.expanded.size) {
          this.expanded.clear();
          this.layoutDirty = true;
          this.render();
        } else if (this.search.value) {
          this.search.value = "";
          this.setText("");
        } else return;
        break;
      case "/":
        this.focusSearch();
        break;
      default:
        // The lab's own shortcuts (K kills the selected node, R restarts it, F fits) are not for this list.
        if (e.key.length === 1) e.preventDefault();
        return;
    }
    e.preventDefault();
  }

  onGlobalKey(e) {
    if (e.key !== "/" || e.defaultPrevented || e.ctrlKey || e.metaKey || e.altKey) return;
    if (!this.root.getClientRects().length) return;
    const target = e.target instanceof Element ? e.target : null;
    if (!target || target.closest("input, textarea, select, [contenteditable], dialog")) return;
    if (!target.closest(".lab-dock")?.contains(this.root)) return;
    e.preventDefault();
    this.focusSearch();
  }

  // ---- actions --------------------------------------------------------------------------------------------

  downloadView() {
    download("krabka-lab-logs.ndjson", toNdjson(this.view), "application/x-ndjson");
  }

  async copyView() {
    const ok = await copyToClipboard(toNdjson(this.view));
    this.hooks.toast(ok ? `Copied ${fmtNum(this.view.length)} lines` : "The browser refused to copy");
  }

  // ---- the log levels dialog ------------------------------------------------------------------------------

  async openLevels() {
    const { levels, dialogRoot } = this.hooks;
    const scenario = this.hooks.scenarioId();
    const brokers = this.hooks.brokers() || [];
    const body = el("div", "lab-loglevels");
    body.dataset.field = "loglevels";
    const scope = select([{ value: "all", label: "All brokers" }, ...brokers.map((b) => ({ value: String(b.id), label: b.name }))], "all");
    scope.dataset.field = "loglevel-scope";
    body.appendChild(labelled("Applies to", scope));

    const set = el("fieldset", "lab-loglevels-presets");
    set.appendChild(el("legend", "lab-field-label", "Level"));
    const name = `${this.uid}-preset`;
    const radios = new Map();
    const choices = [...PRESETS, { value: "custom", label: "Custom", note: "a directive" }];
    for (const p of choices) {
      const label = el("label", "lab-loglevels-preset");
      const radio = el("input");
      radio.type = "radio";
      radio.name = name;
      radio.value = p.value;
      radio.dataset.preset = p.value || "normal";
      radios.set(p.value, radio);
      label.append(radio, el("span", null, p.label), el("small", "lab-muted", p.note));
      set.appendChild(label);
    }
    body.appendChild(set);

    const input = el("input", "lab-input lab-code");
    input.type = "text";
    input.spellcheck = false;
    input.autocomplete = "off";
    input.placeholder = "empty: the broker's default";
    input.dataset.field = "loglevel-directive";
    const check = el("p", "lab-small lab-loglevels-check");
    check.id = `${this.uid}-check`;
    check.setAttribute("role", "status");
    const field = labelled("Directive", input, "Comma-separated: a level for everything (warn) or target=level (krabka_broker=debug).");
    input.setAttribute("aria-describedby", `${check.id} ${input.getAttribute("aria-describedby")}`);
    body.append(field, check);
    const examples = el("div", "lab-loglevels-examples");
    examples.appendChild(el("span", "lab-muted lab-small", "Try"));
    for (const text of DIRECTIVE_EXAMPLES) examples.appendChild(button(text, "lab-btn-sm lab-code", () => show(text), { title: describeDirective(text) }));
    body.appendChild(examples);

    const running = el("ul", "lab-loglevels-running");
    running.dataset.field = "loglevel-running";
    body.append(el("p", "lab-field-label", "Running with"), running);
    if (!brokers.length) running.appendChild(el("li", "lab-muted", "No real broker in this scenario."));
    for (const b of brokers) {
      const li = el("li");
      const level = b.level == null ? "not started" : b.level === "" ? "the default" : b.level;
      li.append(el("strong", null, b.name), " started with ", el("code", null, level), b.alive ? "" : " (stopped)");
      running.appendChild(li);
    }

    const saved = () => (scope.value === "all" ? levels.get(scenario).default : levels.directive(scenario, scope.value));
    const validate = () => {
      const parsed = parseDirective(input.value);
      input.setAttribute("aria-invalid", String(!parsed.ok));
      check.textContent = parsed.ok ? `Valid: ${describeDirective(parsed.directive)}.` : parsed.error;
      check.classList.toggle("lab-loglevels-bad", !parsed.ok);
      const parsedText = parsed.ok ? parsed.directive : null;
      const preset = PRESETS.find((p) => p.value === parsedText);
      (radios.get(preset ? preset.value : "custom") ?? radios.get("custom")).checked = true;
      return parsed;
    };
    const show = (text) => {
      input.value = text;
      validate();
    };
    for (const [value, radio] of radios) {
      radio.addEventListener("change", () => {
        if (value === "custom") input.focus();
        else show(value);
      });
    }
    input.addEventListener("input", validate);
    scope.addEventListener("change", () => show(saved()));
    show(saved());

    const done = openDialog(dialogRoot, {
      title: "Log levels",
      body,
      submitLabel: "Apply and restart",
      focus: () => radios.get(PRESETS.find((p) => p.value === validate().directive)?.value ?? "custom").focus(),
      onSubmit: async () => {
        const parsed = validate();
        if (!parsed.ok) {
          input.focus();
          return false;
        }
        const chosen = scope.value === "all" ? brokers : brokers.filter((b) => String(b.id) === scope.value);
        const restart = chosen.filter((b) => b.alive && b.level != null && b.level !== parsed.directive);
        if (restart.length && !(await this.confirmRestart(restart, parsed.directive))) return false;
        await this.hooks.apply({ scope: scope.value === "all" ? "all" : Number(scope.value), directive: parsed.directive, restart: restart.map((b) => b.id) });
        return true;
      },
    });
    // The button says what it will do: nothing restarts when every chosen broker already runs at the level.
    const submit = body.closest("dialog")?.querySelector('button[type="submit"]');
    const relabel = () => {
      if (!submit) return;
      const parsed = parseDirective(input.value);
      const chosen = scope.value === "all" ? brokers : brokers.filter((b) => String(b.id) === scope.value);
      const n = parsed.ok ? chosen.filter((b) => b.alive && b.level != null && b.level !== parsed.directive).length : 0;
      submit.textContent = n ? `Apply and restart ${n === 1 ? "1 broker" : `${n} brokers`}` : "Apply";
    };
    input.addEventListener("input", relabel);
    scope.addEventListener("change", relabel);
    for (const radio of radios.values()) radio.addEventListener("change", relabel);
    relabel();
    await done;
  }

  // A restart is a kill and a boot on the same disk: its data stays, its connections drop and it rejoins the cluster.
  confirmRestart(restart, directive) {
    const names = restart.map((b) => b.name).join(", ");
    const body = el("div");
    body.dataset.field = "loglevel-confirm";
    body.append(
      el("p", null, `${restart.length === 1 ? "This broker is" : "These brokers are"} killed and started again on ${restart.length === 1 ? "its" : "their"} own disk: ${names}.`),
      el("p", "lab-muted lab-small", `The disk keeps its data. Connections drop and the broker rejoins the cluster, like Kill then Restart. It starts with ${directive ? `the level "${directive}"` : "the default level"}. A stopped broker takes the level when it is restarted.`),
    );
    return openDialog(this.hooks.dialogRoot, {
      title: `Restart ${restart.length === 1 ? "1 broker" : `${restart.length} brokers`} on ${restart.length === 1 ? "its" : "their"} disk?`,
      body,
      submitLabel: "Restart",
      // Cancel is the safe default for a step that drops connections.
      focus: () => [...this.hooks.dialogRoot.querySelectorAll("dialog.lab-dialog")].at(-1)?.querySelector(".lab-dialog-actions button:not([type=submit])")?.focus(),
    });
  }
}
