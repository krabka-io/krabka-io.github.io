// The inspector: the side panel for the selected node.
//
// Header (kind, id, name, alive, hosted by), fault buttons, then three tabs:
// the kind-specific view of the snapshot's `state` (from `views.js`), the
// config form (the same form the palette uses to add a node), and the raw
// snapshot JSON. The state view re-renders at most four times a second and
// only when the state changed; the JSON tree keeps the branches the reader
// opened across renders.

import { el, button, select } from "./dom.js";
import { kindOf, renderState } from "./kinds.js";
import { jsonTree } from "./json-tree.js";
import { buildForm } from "./forms.js";
import { FAULT } from "./faults.js";

const STATE_INTERVAL_MS = 250;
const RAW_INTERVAL_MS = 500;

export class Inspector {
  // hooks: onFault(fault), onCommand(id, command), onHostChange(id, peerId),
  // onTakeOver(id), onUpdateNode(id, spec) → boolean, formCtx() → { nodes },
  // peerName(peerId), nodeName(id), nodeLabelForBroker(brokerId)
  constructor(container, hooks) {
    this.hooks = hooks;
    this.selected = null;
    this.tab = "state";
    this.treeState = new Map();
    this.lastStateKey = "";
    this.lastStateWall = 0;
    this.lastRawKey = "";
    this.lastRawWall = 0;
    this.form = null;
    this.formFor = null;

    this.root = el("section", "lab-inspector");
    this.root.setAttribute("aria-label", "Inspector");
    const titleRow = el("div", "lab-panel-title-row");
    titleRow.appendChild(el("span", "lab-panel-title", "Inspector"));
    this.root.appendChild(titleRow);
    this.emptyMsg = el("p", "lab-insp-empty lab-muted", "Select a node on the canvas to inspect it.");
    this.root.appendChild(this.emptyMsg);
    this.body = el("div", "lab-insp-body");
    this.body.hidden = true;
    this.root.appendChild(this.body);

    this.head = el("header", "lab-insp-head");
    this.glyph = el("span", "lab-insp-glyph");
    const titles = el("div", "lab-insp-titles");
    this.nameEl = el("div", "lab-insp-name");
    this.subEl = el("div", "lab-insp-sub");
    titles.append(this.nameEl, this.subEl);
    this.head.append(this.glyph, titles);
    this.body.appendChild(this.head);

    this.hostRow = el("div", "lab-insp-host");
    this.body.appendChild(this.hostRow);

    this.actions = el("div", "lab-insp-actions");
    this.body.appendChild(this.actions);

    this.tabs = el("div", "lab-tabs");
    this.tabs.setAttribute("role", "tablist");
    this.tabButtons = {};
    for (const [id, label] of [
      ["state", "State"],
      ["config", "Config"],
      ["raw", "Raw JSON"],
    ]) {
      const b = el("button", "lab-tab");
      b.type = "button";
      b.setAttribute("role", "tab");
      b.id = `lab-tab-${id}`;
      b.textContent = label;
      b.addEventListener("click", () => this.showTab(id));
      b.addEventListener("keydown", (e) => {
        const order = ["state", "config", "raw"];
        const i = order.indexOf(id);
        if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
          e.preventDefault();
          const next = order[(i + (e.key === "ArrowRight" ? 1 : order.length - 1)) % order.length];
          this.showTab(next);
          this.tabButtons[next].focus();
        }
      });
      this.tabButtons[id] = b;
      this.tabs.appendChild(b);
    }
    this.body.appendChild(this.tabs);
    this.panels = {};
    for (const id of ["state", "config", "raw"]) {
      const p = el("div", "lab-tabpanel");
      p.setAttribute("role", "tabpanel");
      p.setAttribute("aria-labelledby", `lab-tab-${id}`);
      p.dataset.tab = id;
      this.panels[id] = p;
      this.body.appendChild(p);
    }
    container.appendChild(this.root);
    this.showTab("state");
  }

  showTab(id) {
    this.tab = id;
    for (const [k, b] of Object.entries(this.tabButtons)) {
      const on = k === id;
      b.classList.toggle("lab-tab-active", on);
      b.setAttribute("aria-selected", String(on));
      b.tabIndex = on ? 0 : -1;
      this.panels[k].hidden = !on;
    }
    if (id === "config") this.renderConfig(true);
    this.renderCurrent(true);
  }

  setSelection(id) {
    if (this.selected === id) return;
    this.selected = id;
    this.lastStateKey = "";
    this.lastRawKey = "";
    this.formFor = null;
    if (this.tab === "config") this.renderConfig(true);
    this.renderCurrent(true);
  }

  // `data`: { snapshot, scenario, session }
  update(data) {
    this.data = data;
    this.renderCurrent(false);
  }

  node() {
    if (this.selected == null || !this.data?.snapshot) return null;
    return this.data.snapshot.nodes.find((n) => n.id === this.selected) || null;
  }

  renderCurrent(force) {
    const n = this.node();
    const has = n != null;
    this.body.hidden = !has;
    this.emptyMsg.hidden = has;
    if (!has) return;
    this.renderHeader(n);
    if (this.tab === "state") this.renderState(n, force);
    else if (this.tab === "raw") this.renderRaw(n, force);
    else if (this.tab === "config" && this.formFor !== n.id) this.renderConfig(true);
  }

  renderHeader(n) {
    const k = kindOf(n.kind);
    const session = this.data.session;
    const hostedBy = session?.hosting?.get(n.id) ?? null;
    const remote = !n.hosted;
    const key = [n.name, n.kind, n.alive, n.isolated, n.hosted, hostedBy, session?.role, session?.peersKey].join("|");
    if (key === this.headKey) return;
    this.headKey = key;
    this.glyph.textContent = k.glyph;
    this.glyph.style.background = k.color;
    this.nameEl.textContent = n.name;
    const bits = [k.label, `#${n.id}`, n.alive ? "up" : "down"];
    if (n.isolated) bits.push("isolated");
    if (session && session.role !== "solo") {
      const who = remote ? this.hooks.peerName(hostedBy) : "this tab";
      bits.push(`hosted by ${who}`);
    }
    this.subEl.textContent = bits.join(" · ");
    this.subEl.dataset.hosted = String(Boolean(n.hosted));

    // Hosting controls: the hub picks a host per node; a spoke can ask for it.
    this.hostRow.innerHTML = "";
    this.hostRow.hidden = true;
    if (session && session.role === "hub" && !k.hidden) {
      this.hostRow.hidden = false;
      const options = session.peers
        .filter((p) => p.state === "connected" || p.id === session.me)
        .map((p) => ({ value: p.id, label: p.id === session.me ? `${p.name} (this tab)` : p.name }));
      const current = hostedBy ?? session.me;
      if (!options.some((o) => o.value === current)) options.push({ value: current, label: `${this.hooks.peerName(current)} (offline)` });
      const sel = select(options, current, (v) => this.hooks.onHostChange(n.id, v));
      sel.dataset.hostSelect = String(n.id);
      sel.setAttribute("aria-label", `Host of ${n.name}`);
      const label = el("label", "lab-field-inline");
      label.append(el("span", "lab-muted", "hosted by"), sel);
      this.hostRow.appendChild(label);
    } else if (session && session.role === "spoke" && !k.hidden) {
      this.hostRow.hidden = false;
      this.hostRow.appendChild(el("span", "lab-muted", `hosted by ${remote ? this.hooks.peerName(hostedBy) : "this tab"}`));
      if (remote) this.hostRow.appendChild(button("Take over", "lab-btn-sm", () => this.hooks.onTakeOver(n.id), { title: "Ask the host to run this node in this tab" }));
    }

    this.actions.innerHTML = "";
    const fault = (f, label, title, disabled) => this.actions.appendChild(button(label, "lab-btn-sm", () => this.hooks.onFault(f), { title, disabled }));
    if (n.alive) fault(FAULT.kill(n.id), "Kill", "Halt the node; its disk survives");
    else fault(FAULT.restart(n.id), "Restart", "Boot again from the kept state");
    fault(FAULT.wipe(n.id), "Wipe", "Boot again from nothing");
    if (n.isolated) fault(FAULT.reconnect(n.id), "Reconnect", "Restore every link");
    else fault(FAULT.isolate(n.id), "Isolate", "Cut every link");
    if (!k.hidden) {
      this.actions.appendChild(button("Edit", "lab-btn-sm", () => this.showTab("config"), { title: "Change the configuration" }));
      this.actions.appendChild(button("Remove", "lab-btn-sm lab-danger", () => this.hooks.onCommand(n.id, "remove"), { title: "Remove the node from the scenario" }));
    }
  }

  renderState(n, force) {
    const now = performance.now();
    const key = JSON.stringify(n.state);
    if (!force && (key === this.lastStateKey || now - this.lastStateWall < STATE_INTERVAL_MS)) return;
    if (key === this.lastStateKey && !force) return;
    this.lastStateKey = key;
    this.lastStateWall = now;
    let ts = this.treeState.get(n.id);
    if (!ts) {
      ts = { expanded: new Set(), collapsed: new Set() };
      this.treeState.set(n.id, ts);
    }
    const ctx = {
      expanded: ts.expanded,
      collapsed: ts.collapsed,
      nodeName: this.hooks.nodeName,
      nodeLabelForBroker: this.hooks.nodeLabelForBroker,
    };
    const view = renderState(n, ctx);
    const panel = this.panels.state;
    // Keep the scroll position of a panel that only changed numbers.
    const scroll = panel.scrollTop;
    panel.innerHTML = "";
    panel.appendChild(view);
    panel.scrollTop = scroll;
  }

  renderRaw(n, force) {
    const now = performance.now();
    if (!force && now - this.lastRawWall < RAW_INTERVAL_MS) return;
    const text = JSON.stringify(n, null, 2);
    if (text === this.lastRawKey && !force) return;
    this.lastRawKey = text;
    this.lastRawWall = now;
    const panel = this.panels.raw;
    panel.innerHTML = "";
    const pre = el("pre", "lab-raw", text);
    panel.appendChild(pre);
  }

  renderConfig(force) {
    const n = this.node();
    const panel = this.panels.config;
    if (!n) return;
    if (!force && this.formFor === n.id) return;
    this.formFor = n.id;
    panel.innerHTML = "";
    const k = kindOf(n.kind);
    if (k.hidden) {
      panel.appendChild(el("p", "lab-muted", "The admin client has no editable configuration."));
      return;
    }
    const spec = this.data?.scenario?.nodes?.find((s) => s.id === n.id);
    if (!spec) {
      panel.appendChild(el("p", "lab-muted", "No configuration for this node."));
      return;
    }
    const nameInput = el("input", "lab-input");
    nameInput.type = "text";
    nameInput.value = spec.name || "";
    const nameField = el("label", "lab-field");
    nameField.append(el("span", "lab-field-label", "Name"), nameInput);
    panel.appendChild(nameField);
    const ctx = { ...this.hooks.formCtx(), self: n.id };
    this.form = buildForm(k.fields, spec.config || {}, ctx);
    panel.appendChild(this.form.root);
    if (!k.fields.length) panel.appendChild(el("p", "lab-muted", "This kind has no configuration."));
    const note = el("p", "lab-muted lab-small", "Applying restarts the node from nothing with the new configuration.");
    const row = el("div", "lab-form-actions");
    const apply = button("Apply", "lab-primary", () => {
      const r = this.form.read();
      if (r.errors.length) return;
      const next = { ...spec, name: nameInput.value.trim() || spec.name, config: r.value };
      if (this.hooks.onUpdateNode(n.id, next)) this.showTab("state");
    });
    const cancel = button("Cancel", "", () => this.showTab("state"));
    row.append(apply, cancel);
    panel.append(note, row);
  }
}
