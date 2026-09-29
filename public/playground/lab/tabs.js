// A tab strip over a set of panels.
//
// The lab's side columns and its bottom dock each hold several panels that
// used to be stacked, collapsed disclosure boxes. A reader had to open each
// one to find out what it was. A tab set names every panel in one row and
// shows one at a time. Every panel stays mounted while hidden, so the module
// inside keeps its state, its scroll position and what the reader typed.

import { el } from "./dom.js";

let counter = 0;

export class TabSet {
  // `tabs`: [{ id, label, title }]. `hooks.onShow(id)` runs after a tab opens.
  constructor(container, { label, tabs, active, className = "", hooks = {} }) {
    this.hooks = hooks;
    this.uid = `lab-tabset-${++counter}`;
    this.root = el("div", `lab-tabset ${className}`.trim());
    this.bar = el("div", "lab-tabset-bar");
    this.bar.setAttribute("role", "tablist");
    this.bar.setAttribute("aria-label", label);
    this.body = el("div", "lab-tabset-body");
    this.root.append(this.bar, this.body);
    this.tabs = new Map();
    this.order = tabs.map((t) => t.id);
    for (const t of tabs) {
      const tab = el("button", "lab-dtab");
      tab.type = "button";
      tab.id = `${this.uid}-tab-${t.id}`;
      tab.dataset.tab = t.id;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-controls", `${this.uid}-panel-${t.id}`);
      if (t.title) tab.title = t.title;
      const text = el("span", "lab-dtab-label", t.label);
      const badge = el("span", "lab-dtab-badge");
      badge.hidden = true;
      tab.append(text, badge);
      tab.addEventListener("click", () => this.show(t.id));
      tab.addEventListener("keydown", (e) => this.onKey(e, t.id));
      const panel = el("div", "lab-dpanel");
      panel.id = `${this.uid}-panel-${t.id}`;
      panel.dataset.panel = t.id;
      panel.setAttribute("role", "tabpanel");
      panel.setAttribute("aria-labelledby", tab.id);
      this.bar.appendChild(tab);
      this.body.appendChild(panel);
      this.tabs.set(t.id, { tab, panel, badge });
    }
    container.appendChild(this.root);
    this.active = null;
    this.show(active ?? tabs[0].id, { silent: true });
  }

  panel(id) {
    return this.tabs.get(id).panel;
  }

  // Put a count or a dot on a tab: `text` empty clears it.
  setBadge(id, text) {
    const { badge } = this.tabs.get(id);
    badge.textContent = text || "";
    badge.hidden = !text;
  }

  show(id, { silent = false } = {}) {
    if (!this.tabs.has(id)) return;
    const changed = this.active !== id;
    this.active = id;
    for (const [k, t] of this.tabs) {
      const on = k === id;
      t.tab.classList.toggle("lab-dtab-active", on);
      t.tab.setAttribute("aria-selected", String(on));
      t.tab.tabIndex = on ? 0 : -1;
      t.panel.hidden = !on;
    }
    if (changed && !silent && this.hooks.onShow) this.hooks.onShow(id);
  }

  onKey(e, id) {
    const i = this.order.indexOf(id);
    let next = null;
    if (e.key === "ArrowRight") next = this.order[(i + 1) % this.order.length];
    else if (e.key === "ArrowLeft") next = this.order[(i + this.order.length - 1) % this.order.length];
    else if (e.key === "Home") next = this.order[0];
    else if (e.key === "End") next = this.order[this.order.length - 1];
    if (next == null) return;
    e.preventDefault();
    this.show(next);
    this.tabs.get(next).tab.focus();
  }
}
