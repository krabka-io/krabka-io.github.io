// The event log: what the world recorded, newest at the bottom.
//
// Events arrive in batches from the world loop. The panel keeps the last few
// thousand in memory for re-filtering and at most 500 rows in the DOM. Rows
// are coloured by `detail.level`; the kinds that change who leads what carry
// a label and the colour of the node kind they concern; clicking a row
// selects its node.

import { el, button, select, fmtMs, shortJson } from "./dom.js";
import { KINDS } from "./kinds.js";

const MAX_ROWS = 500;
const MAX_KEPT = 4000;

// Event kinds shown with a label and a colour; every other kind shows its
// name as it is.
const KIND_STYLES = {
  election: { label: "registry election", color: KINDS["schema-registry"].color },
  elect: { label: "KRaft election", color: KINDS.broker.color },
  controller: { label: "active controller", color: KINDS.broker.color },
  quorum_observer: { label: "quorum observer", color: KINDS.broker.color },
  leader_change: { label: "leader change", color: KINDS.broker.color },
};

// The text a kind shows in the list and in the filter.
export function kindLabel(kind) {
  return KIND_STYLES[kind]?.label ?? kind;
}

export class Timeline {
  constructor(container, hooks) {
    this.hooks = hooks; // onSelect(id), nodeName(id)
    this.events = [];
    this.follow = true;
    this.nodeFilter = "";
    this.kindFilter = "";
    this.textFilter = "";
    this.kinds = new Set();
    this.nodeOptions = [];
    this.renderQueued = false;

    this.root = el("section", "lab-timeline");
    this.root.setAttribute("aria-label", "Event timeline");
    const bar = el("div", "lab-timeline-bar");
    bar.appendChild(el("span", "lab-panel-title", "Timeline"));
    this.nodeSel = select([{ value: "", label: "all nodes" }], "", (v) => {
      this.nodeFilter = v;
      this.rerender();
    });
    this.nodeSel.setAttribute("aria-label", "Filter by node");
    this.kindSel = select([{ value: "", label: "all kinds" }], "", (v) => {
      this.kindFilter = v;
      this.rerender();
    });
    this.kindSel.setAttribute("aria-label", "Filter by kind");
    this.text = el("input", "lab-input lab-input-sm");
    this.text.type = "search";
    this.text.placeholder = "filter text";
    this.text.setAttribute("aria-label", "Filter events by text");
    this.text.addEventListener("input", () => {
      this.textFilter = this.text.value.trim().toLowerCase();
      this.rerender();
    });
    const followLabel = el("label", "lab-field-inline lab-follow");
    this.followBox = el("input");
    this.followBox.type = "checkbox";
    this.followBox.checked = true;
    this.followBox.addEventListener("change", () => {
      this.follow = this.followBox.checked;
      if (this.follow) this.scrollToEnd();
    });
    followLabel.append(this.followBox, el("span", null, "follow"));
    this.count = el("span", "lab-timeline-count", "0 events");
    bar.append(this.nodeSel, this.kindSel, this.text, followLabel, this.count, button("Clear", "lab-btn-sm", () => this.clear()));
    this.list = el("ol", "lab-timeline-list");
    this.list.addEventListener("scroll", () => {
      // Scrolling up pauses auto-follow; scrolling back to the end resumes it.
      const atEnd = this.list.scrollTop + this.list.clientHeight >= this.list.scrollHeight - 4;
      if (this.follow !== atEnd) {
        this.follow = atEnd;
        this.followBox.checked = atEnd;
      }
    });
    this.root.append(bar, this.list);
    container.appendChild(this.root);
  }

  // The node names for the filter and the rows: `[{ id, name }]`.
  setNodes(nodes) {
    const key = nodes.map((n) => `${n.id}:${n.name}`).join("|");
    if (key === this.nodeKey) return;
    this.nodeKey = key;
    this.nodeOptions = nodes;
    const current = this.nodeSel.value;
    this.nodeSel.innerHTML = "";
    for (const o of [{ value: "", label: "all nodes" }, { value: "world", label: "world" }, ...nodes.map((n) => ({ value: String(n.id), label: n.name }))]) {
      const opt = document.createElement("option");
      opt.value = o.value;
      opt.textContent = o.label;
      this.nodeSel.appendChild(opt);
    }
    this.nodeSel.value = current;
    if (this.nodeSel.value !== current) {
      this.nodeSel.value = "";
      this.nodeFilter = "";
    }
  }

  append(events) {
    if (!events.length) return;
    let newKinds = false;
    for (const e of events) {
      this.events.push(e);
      if (!this.kinds.has(e.kind)) {
        this.kinds.add(e.kind);
        newKinds = true;
      }
    }
    if (this.events.length > MAX_KEPT) this.events.splice(0, this.events.length - MAX_KEPT);
    if (newKinds) this.refreshKinds();
    for (const e of events) {
      if (this.matches(e)) this.list.appendChild(this.row(e));
    }
    while (this.list.children.length > MAX_ROWS) this.list.firstChild.remove();
    this.count.textContent = `${this.events.length} events`;
    if (this.follow) this.scrollToEnd();
  }

  clear() {
    this.events = [];
    this.list.innerHTML = "";
    this.count.textContent = "0 events";
  }

  refreshKinds() {
    const current = this.kindSel.value;
    this.kindSel.innerHTML = "";
    for (const o of [{ value: "", label: "all kinds" }, ...[...this.kinds].sort().map((k) => ({ value: k, label: kindLabel(k) }))]) {
      const opt = document.createElement("option");
      opt.value = o.value;
      opt.textContent = o.label;
      this.kindSel.appendChild(opt);
    }
    this.kindSel.value = current;
  }

  matches(e) {
    if (this.nodeFilter === "world" && e.node != null) return false;
    if (this.nodeFilter && this.nodeFilter !== "world" && String(e.node) !== this.nodeFilter) return false;
    if (this.kindFilter && e.kind !== this.kindFilter) return false;
    if (this.textFilter) {
      const hay = `${e.kind} ${kindLabel(e.kind)} ${this.nodeName(e.node)} ${shortJson(e.detail, 400)}`.toLowerCase();
      if (!hay.includes(this.textFilter)) return false;
    }
    return true;
  }

  rerender() {
    this.list.innerHTML = "";
    const shown = this.events.filter((e) => this.matches(e)).slice(-MAX_ROWS);
    for (const e of shown) this.list.appendChild(this.row(e));
    if (this.follow) this.scrollToEnd();
  }

  nodeName(id) {
    if (id == null) return "world";
    return this.hooks.nodeName(id);
  }

  row(e) {
    const level = e.detail && typeof e.detail === "object" && e.detail.level ? String(e.detail.level) : "info";
    const li = el("li", `lab-ev lab-ev-${level}`);
    li.dataset.index = String(e.index);
    if (e.node != null) li.dataset.node = String(e.node);
    const time = el("span", "lab-ev-time", fmtMs(e.at));
    const node = el("button", "lab-ev-node");
    node.type = "button";
    node.textContent = this.nodeName(e.node);
    if (e.node != null) node.addEventListener("click", () => this.hooks.onSelect(e.node));
    else node.disabled = true;
    const style = KIND_STYLES[e.kind];
    const kind = el("span", "lab-ev-kind", style ? style.label : e.kind);
    kind.dataset.kind = e.kind;
    if (style) {
      kind.style.color = style.color;
      kind.title = e.kind;
    }
    const detail = el("span", "lab-ev-detail", shortJson(e.detail, 120));
    detail.title = typeof e.detail === "object" ? JSON.stringify(e.detail) : String(e.detail ?? "");
    li.append(time, node, kind, detail);
    return li;
  }

  scrollToEnd() {
    this.list.scrollTop = this.list.scrollHeight;
  }
}
