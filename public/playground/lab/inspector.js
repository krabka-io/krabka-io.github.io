// The inspector: the side panel for the selected node.
//
// Header (kind, id, name, alive, hosted by), the node's
// control commands (send, rate, pause, query, ...; `commands` in `kinds.js`),
// then three tabs: the kind-specific view of the snapshot's `state` (from
// `views.js`), the config form (the same form the palette uses to add a
// node), and the raw snapshot JSON. The state view re-renders at most four
// times a second and only when the state changed; the JSON tree keeps the
// branches the reader opened across renders. The command bar is built once
// per node and only enabled or disabled after that, so what the reader types
// in it survives the snapshots.

import { el, button, select, plural } from "./dom.js";
import { kindOf, renderState, commandObject, statusLine } from "./kinds.js";
import { buildForm } from "./forms.js";
import { cutOff } from "./faults.js";

const STATE_INTERVAL_MS = 250;
const RAW_INTERVAL_MS = 500;

export class Inspector {
  // hooks: onCommand(id, command), onControl(id, command) →
  // { ok, answer | error }, onHostChange(id, peerId), onTakeOver(id),
  // onUpdateNode(id, spec) → boolean, onBrowseVolume(id), formCtx() → { nodes }, peerName(peerId),
  // nodeName(id), nodeLabelForBroker(brokerId)
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
    this.emptyMsg = el("div", "lab-overview");
    this.root.appendChild(this.emptyMsg);
    // The "Try this" list is built once and never replaced, so a button the
    // reader is pressing or has focused is not swapped out under them.
    this.tries = this.buildTries();
    this.emptyMsg.append(...this.tries);
    this.overviewSubs = new Map();
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

    this.commands = el("div", "lab-insp-commands");
    this.commands.setAttribute("aria-label", "Node commands");
    this.commands.hidden = true;
    this.body.appendChild(this.commands);
    this.commandKey = "";
    this.commandControls = [];

    this.tabs = el("div", "lab-tabs");
    this.tabs.setAttribute("role", "tablist");
    this.tabs.setAttribute("aria-label", "Node details");
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
    if (!has) {
      this.renderOverview();
      return;
    }
    this.renderHeader(n);
    this.renderCommands(n);
    if (this.tab === "state") this.renderState(n, force);
    else if (this.tab === "raw") this.renderRaw(n, force);
    else if (this.tab === "config" && this.formFor !== this.configKey(n)) this.renderConfig(true);
  }

  // What the inspector shows while nothing is selected: every node, its
  // health and its one-line status, each a button that selects it.
  renderOverview() {
    const snapshot = this.data?.snapshot;
    const nodes = (snapshot?.nodes || []).filter((n) => !kindOf(n.kind).hidden);
    // A node whose host tab has dropped off is as good as down, like on the canvas.
    const hostOffline = (n) => this.hostOffline(n);
    // The live one-line status is left out of the key: it changes on almost
    // every snapshot, and is patched into the rows below instead.
    const key = nodes.map((n) => [n.id, n.name, n.kind, n.alive, n.isolated, n.hosted, hostOffline(n)].join(":")).join("|");
    if (key !== this.overviewKey) {
      this.overviewKey = key;
      this.buildOverview(nodes);
    }
    for (const n of nodes) {
      const sub = this.overviewSubs.get(n.id);
      if (sub) sub.textContent = overviewSub(n);
    }
  }

  buildOverview(nodes) {
    const hostOffline = (n) => this.hostOffline(n);
    const box = this.emptyMsg;
    for (const child of [...box.children]) if (!this.tries.includes(child)) child.remove();
    this.overviewSubs.clear();
    for (const t of this.tries) t.hidden = !nodes.length;
    const top = [el("h2", "lab-rail-heading", "Cluster overview")];
    if (!nodes.length) {
      top.push(el("p", "lab-rail-help", "The canvas is empty. Add nodes from the Build tab, or load a preset from Scenarios."));
      const row = el("div", "lab-palette-actions");
      row.append(
        button("Load a preset", "lab-btn-sm lab-primary", () => this.hooks.onOpenTab?.("scenarios")),
        button("Add a node", "lab-btn-sm", () => this.hooks.onOpenTab?.("build")),
      );
      top.push(row);
      box.prepend(...top);
      return;
    }
    const up = nodes.filter((n) => n.alive && !hostOffline(n)).length;
    top.push(el("p", "lab-rail-help", `${up} of ${plural(nodes.length, "node")} up. Select a node to see its state, edit its configuration or send it commands.`));
    const list = el("ul", "lab-overview-list");
    for (const n of nodes) {
      const k = kindOf(n.kind);
      const li = el("li");
      const b = el("button", "lab-overview-node");
      b.type = "button";
      b.dataset.overviewNode = String(n.id);
      const glyph = el("span", "lab-kind-glyph", k.glyph);
      glyph.style.background = k.color;
      const text = el("span", "lab-overview-text");
      const chips = el("span", "lab-overview-chips");
      if (hostOffline(n)) chips.appendChild(el("span", "lab-chip lab-chip-err", "host offline"));
      else chips.appendChild(el("span", `lab-chip ${n.alive ? "lab-chip-ok" : "lab-chip-err"}`, n.alive ? "up" : "down"));
      if (n.isolated) chips.appendChild(el("span", "lab-chip lab-chip-warn", "isolated"));
      const head = el("span", "lab-overview-name");
      head.append(el("strong", null, n.name), chips);
      const sub = el("span", "lab-overview-sub");
      this.overviewSubs.set(n.id, sub);
      text.append(head, sub);
      b.append(glyph, text);
      b.addEventListener("click", () => this.hooks.onSelect?.(n.id));
      li.appendChild(b);
      list.appendChild(li);
    }
    top.push(list);
    box.prepend(...top);
  }

  // Things a reader can do to a cluster, each a button that does it.
  buildTries() {
    const heading = el("h2", "lab-rail-heading", "Try this");
    const tries = el("ul", "lab-try-list");
    for (const [key, label, what] of [
      ["kill", "Kill a broker", "Watch producers retry and the consumer group rebalance."],
      ["partition", "Cut a client off", "Partition a client from a broker and watch it retry."],
      ["latency", "Slow a link to 300 ms", "See round trips and consumer lag grow."],
      ["consumer", "Add a consumer", "Join the group and watch partitions rebalance."],
    ]) {
      const li = el("li");
      const b = el("button", "lab-try");
      b.type = "button";
      b.dataset.try = key;
      b.append(el("strong", null, label), el("span", null, what));
      b.addEventListener("click", () => this.hooks.onTry?.(key));
      li.appendChild(b);
      tries.appendChild(li);
    }
    return [heading, tries];
  }

  // A node whose host tab has dropped off is as good as down, like on the canvas.
  hostOffline(n) {
    const session = this.data?.session;
    const host = session?.hosting?.get(n.id);
    return !n.hosted && host != null && Boolean(session.offline?.has(host));
  }

  renderHeader(n) {
    const k = kindOf(n.kind);
    const session = this.data.session;
    const hostedBy = session?.hosting?.get(n.id) ?? null;
    const remote = !n.hosted;
    const offline = this.hostOffline(n);
    // The id is in the key: two nodes can share a name and kind, and the
    // buttons below close over the id.
    const cut = cutOff(this.data?.snapshot, n.id);
    const key = [n.id, n.name, n.kind, n.alive, n.isolated, cut, n.hosted, hostedBy, offline, session?.role, session?.peersKey].join("|");
    if (key === this.headKey) return;
    this.headKey = key;
    this.glyph.textContent = k.glyph;
    this.glyph.style.background = k.color;
    this.nameEl.textContent = n.name;
    const bits = [k.label, `#${n.id}`, !n.alive ? "down" : offline ? "host offline" : "up"];
    if (n.isolated) bits.push("isolated");
    if (session && session.role !== "solo") {
      const who = remote ? this.hooks.peerName(hostedBy) : "this tab";
      bits.push(`hosted by ${who}`);
    }
    this.subEl.textContent = bits.join(" · ");
    this.subEl.dataset.hosted = String(Boolean(n.hosted));

    // Hosting controls: the hub picks a host per node; a spoke can ask for it.
    // A pinned node (a real broker) stays where its process and volume are.
    this.hostRow.innerHTML = "";
    this.hostRow.hidden = true;
    if (session && session.role !== "solo" && k.pinned) {
      this.hostRow.hidden = false;
      const note = el(
        "span",
        "lab-muted lab-small",
        session.role === "hub"
          ? "Runs in this tab: a real broker's process and its volume live in this browser, so it cannot move to another tab."
          : "Runs in the host's tab: a real broker's process and its volume live in that browser, so it cannot move to this tab.",
      );
      note.dataset.field = "pinned-note";
      this.hostRow.appendChild(note);
    } else if (session && session.role === "hub" && !k.hidden) {
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
    if (!k.hidden) {
      const fault = (label, command, title, cls = "") => this.actions.appendChild(button(label, `lab-btn-sm ${cls}`.trim(), () => this.hooks.onCommand(n.id, command), { title, data: { fault: command } }));
      if (n.alive) fault("Kill", "kill", "Halt the node; its disk survives", "lab-danger");
      else fault("Restart", "restart", "Boot again from the kept state", "lab-primary");
      if (cut) fault("Reconnect", "reconnect", "Restore every link of the node");
      else fault("Isolate", "isolate", "Cut every link of the node");
    }
    if (n.kind === "krabka-broker" && n.hosted) {
      this.actions.appendChild(button("Browse disk", "lab-btn-sm", () => this.hooks.onBrowseVolume(n.id)));
    }
    if (!k.hidden && session?.role === "spoke") {
      this.actions.appendChild(el("span", "lab-muted lab-small", "The host edits and removes nodes."));
    } else if (!k.hidden) {
      this.actions.appendChild(button("Edit", "lab-btn-sm", () => this.showTab("config"), { title: "Change the configuration" }));
      this.actions.appendChild(button("Remove", "lab-btn-sm lab-danger", () => this.hooks.onCommand(n.id, "remove"), { title: "Remove the node from the scenario" }));
    }
  }

  // The command bar: one row per command of the kind that is not marked
  // `bar: false`. Built when the selection or its kind changes; after that
  // each snapshot only enables and disables the buttons, and refreshes the
  // inputs the reader has not touched.
  renderCommands(n) {
    const k = kindOf(n.kind);
    const key = `${n.id}:${n.kind}`;
    if (key !== this.commandKey) {
      this.commandKey = key;
      this.buildCommands(n, (k.commands || []).filter((c) => c.bar !== false));
    }
    if (!this.commandControls.length) return;
    const state = n.state && typeof n.state === "object" ? n.state : {};
    const blocked = !n.alive ? "the node is down" : !n.hosted ? "another tab runs this node" : "";
    this.commandNote.textContent = blocked ? `Commands wait: ${blocked}.` : "";
    this.commandNote.hidden = !blocked;
    for (const c of this.commandControls) {
      c.button.disabled = Boolean(blocked) || (c.spec.enabled ? !c.spec.enabled(state) : false);
      for (const input of c.inputs) {
        input.el.disabled = Boolean(blocked);
        input.refresh(state);
      }
    }
  }

  buildCommands(n, specs) {
    this.commands.innerHTML = "";
    this.commandControls = [];
    this.commands.hidden = specs.length === 0;
    if (!specs.length) return;
    const state = n.state && typeof n.state === "object" ? n.state : {};
    const rows = el("div", "lab-cmd-rows");
    for (const spec of specs) {
      const row = el("div", "lab-cmd");
      row.dataset.command = spec.cmd;
      const inputs = (spec.params || []).map((p) => commandParam(p, state, spec));
      for (const input of inputs) row.appendChild(input.wrap);
      const b = button(spec.label, "lab-btn-sm", () => this.runCommand(n.id, spec, inputs), { title: spec.title, data: { command: spec.cmd } });
      row.appendChild(b);
      rows.appendChild(row);
      this.commandControls.push({ spec, button: b, inputs });
    }
    this.commandNote = el("p", "lab-muted lab-small");
    this.commandNote.hidden = true;
    this.commandResult = el("p", "lab-cmd-result lab-small");
    this.commandResult.dataset.field = "command-result";
    this.commandResult.hidden = true;
    this.commands.append(rows, this.commandNote, this.commandResult);
  }

  runCommand(id, spec, inputs) {
    const values = {};
    for (const input of inputs) {
      const r = input.read();
      if (r.error) {
        this.showResult(spec, { ok: false, error: `${input.param.label}: ${r.error}` });
        return;
      }
      if (r.value !== undefined) values[input.param.key] = r.value;
    }
    const result = this.hooks.onControl(id, commandObject(spec, values));
    for (const input of inputs) input.touched = false;
    this.showResult(spec, result);
  }

  showResult(spec, r) {
    const el2 = this.commandResult;
    el2.hidden = false;
    el2.textContent = r.ok ? `${spec.label}: ${answerText(r.answer)}` : `${spec.label} failed: ${r.error}`;
    el2.dataset.ok = String(Boolean(r.ok));
    el2.dataset.command = spec.cmd;
    el2.classList.toggle("lab-cmd-error", !r.ok);
  }

  renderState(n, force) {
    const now = performance.now();
    const key = JSON.stringify(n.state);
    if (!force && (key === this.lastStateKey || now - this.lastStateWall < STATE_INTERVAL_MS)) return;
    if (key === this.lastStateKey && !force) return;
    const panel = this.panels.state;
    if (!force && inUse(panel)) return;
    this.lastStateKey = key;
    this.lastStateWall = now;
    let ts = this.treeState.get(n.id);
    if (!ts) {
      ts = { expanded: new Set(), collapsed: new Set(), sections: new Map() };
      this.treeState.set(n.id, ts);
    }
    const ctx = {
      expanded: ts.expanded,
      collapsed: ts.collapsed,
      nodeName: this.hooks.nodeName,
      nodeLabelForBroker: this.hooks.nodeLabelForBroker,
    };
    const view = renderState(n, ctx);
    keepSectionsOpen(view, ts.sections);
    // A section heading the reader just toggled has focus: find it again in the new DOM.
    const active = document.activeElement;
    const focusKey = active?.matches?.("details.lab-sec > summary") && panel.contains(active) ? sectionKey(active.parentElement, panel) : null;
    const old = panel.firstElementChild;
    if (old && !force) {
      // Patch the values in place: a heading or JSON branch the reader is
      // pressing or has focused stays the same element, so the press completes
      // and focus does not move.
      morph(old, view);
    } else {
      panel.innerHTML = "";
      panel.appendChild(view);
    }
    // Only a heading whose element was replaced needs its focus given back.
    if (focusKey != null && !panel.contains(document.activeElement)) {
      for (const d of panel.querySelectorAll("details.lab-sec")) {
        if (sectionKey(d, panel) === focusKey) d.querySelector(":scope > summary")?.focus({ preventScroll: true });
      }
    }
  }

  renderRaw(n, force) {
    const now = performance.now();
    if (!force && now - this.lastRawWall < RAW_INTERVAL_MS) return;
    const text = JSON.stringify(n, null, 2);
    if (text === this.lastRawKey && !force) return;
    const panel = this.panels.raw;
    if (!force && inUse(panel)) return;
    this.lastRawKey = text;
    this.lastRawWall = now;
    panel.innerHTML = "";
    const pre = el("pre", "lab-raw", text);
    panel.appendChild(pre);
  }

  // The config panel depends on the node and on whether this tab may edit.
  configKey(n) {
    return `${n.id}:${this.data?.session?.role ?? "solo"}`;
  }

  renderConfig(force) {
    const n = this.node();
    const panel = this.panels.config;
    if (!n) return;
    const key = this.configKey(n);
    if (!force && this.formFor === key) return;
    this.formFor = key;
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
    if (this.data?.session?.role === "spoke") {
      // A spoke runs the host's scenario; an edit here would diverge from it.
      const note = el("p", "lab-muted lab-small", "Read-only: the host owns the scenario, so node configuration is edited in the host's tab. This tab picks up every change the host makes.");
      note.dataset.field = "config-readonly";
      panel.append(note, el("pre", "lab-raw", JSON.stringify({ name: spec.name, config: spec.config }, null, 2)));
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
      if (r.errors.length) {
        this.form.focusInvalid();
        return;
      }
      const next ={ ...spec, name: nameInput.value.trim() || spec.name, config: r.value };
      if (this.hooks.onUpdateNode(n.id, next)) this.showTab("state");
    });
    const cancel = button("Cancel", "", () => this.showTab("state"));
    row.append(apply, cancel);
    panel.append(note, row);
  }
}

// One input of a command: a number, a text or a select. `fromState` fills a
// number from the node's state for as long as the reader has not typed in
// it; a select whose `options` is a function of the state follows it.
function commandParam(param, state, spec) {
  const wrap = el("label", "lab-cmd-param");
  const text = el("span", "lab-muted lab-small", param.label);
  let input;
  const out = { param, wrap, touched: false, el: null, read: null, refresh: () => {} };
  if (param.type === "select") {
    input = select([], null, null, "lab-input-sm");
    let current = "";
    out.refresh = (s) => {
      const options = typeof param.options === "function" ? param.options(s) || [] : param.options || [];
      const key = options.join("\u0000");
      if (key === current) return;
      current = key;
      const chosen = input.value;
      input.innerHTML = "";
      for (const o of options) {
        const opt = document.createElement("option");
        opt.value = String(o);
        opt.textContent = String(o);
        input.appendChild(opt);
      }
      if (options.map(String).includes(chosen)) input.value = chosen;
    };
    out.refresh(state);
    out.read = () => (input.value ? { value: input.value } : { error: "nothing to choose yet" });
  } else {
    input = el("input", "lab-input lab-input-sm");
    input.type = param.type === "number" ? "number" : "text";
    if (param.min != null) input.min = String(param.min);
    if (param.step != null) input.step = String(param.step);
    if (param.placeholder) input.placeholder = param.placeholder;
    const fromState = (s) => {
      const v = param.fromState ? param.fromState(s) : undefined;
      return v == null ? (param.default ?? "") : v;
    };
    input.value = String(fromState(state));
    input.addEventListener("input", () => {
      out.touched = true;
    });
    if (param.fromState) {
      out.refresh = (s) => {
        if (!out.touched && document.activeElement !== input) input.value = String(fromState(s));
      };
    }
    out.read = () => {
      const raw = input.value.trim();
      if (param.type !== "number") return raw ? { value: raw } : { error: "required" };
      if (!raw) return { error: "required" };
      const n = Number(raw);
      if (!Number.isFinite(n)) return { error: "not a number" };
      if (param.step === 1 && !Number.isInteger(n)) return { error: "a whole number" };
      if (param.min != null && n < param.min) return { error: `at least ${param.min}` };
      return { value: n };
    };
  }
  input.setAttribute("aria-label", `${spec.label}: ${param.label}`);
  input.dataset.param = param.key;
  out.el = input;
  wrap.append(text, input);
  return out;
}

// A panel the reader is using (typing in a field in it, or text in it
// selected) keeps its DOM: replacing it would drop the caret and clear the
// selection. It catches up on the first render after the reader lets go.
function inUse(panel) {
  if (panel.contains(document.activeElement) && document.activeElement.matches("input, textarea, select")) return true;
  const sel = window.getSelection();
  return Boolean(sel && !sel.isCollapsed && sel.containsNode(panel, true));
}

// One overview row's second line: the kind and its live status.
function overviewSub(n) {
  const status = statusLine(n);
  const label = kindOf(n.kind).label;
  return status ? `${label} · ${status}` : label;
}

// A command's answer as one line.
function answerText(answer) {
  if (answer == null) return "done";
  if (typeof answer !== "object") return String(answer);
  const text = JSON.stringify(answer);
  return text.length > 160 ? `${text.slice(0, 159)}…` : text;
}

// Every render of the State tab builds its sections afresh, each open or
// closed by its default, so a section the reader opened would close again at
// the next render. `sections` keeps what the reader chose, keyed by the
// titles from the outermost section down, and puts it back on the new view.
function keepSectionsOpen(view, sections) {
  for (const d of view.querySelectorAll("details.lab-sec")) {
    const key = sectionKey(d, view);
    if (sections.has(key)) d.open = sections.get(key);
    d.addEventListener("toggle", () => sections.set(key, d.open));
  }
}

// A heading without its live parts (a count in parentheses, the states after
// a " · ", a JSON branch's "{7}"), so it names the same section while those
// change.
function stableTitle(summary) {
  return (summary?.textContent ?? "").split(/ \(| · |:? ?[{[]\d+[}\]]$/)[0];
}

// A section's identity across renders: the headings from `root` down to it.
function sectionKey(d, root) {
  const titles = [];
  for (let e = d; e && e !== root; e = e.parentElement) {
    if (e.matches("details.lab-sec")) titles.unshift(stableTitle(e.querySelector(":scope > summary")));
  }
  return titles.join("\u0000");
}

// Patch `a` into the shape of `b`, reusing every node that still matches. A
// `<details>` keeps the open state the reader gave it, and is swapped for the
// new one when its heading names a different section.
function morph(a, b) {
  const sameKind = a.nodeName === b.nodeName && (a.nodeName !== "DETAILS" || stableTitle(a.firstElementChild) === stableTitle(b.firstElementChild));
  if (!sameKind) return a.replaceWith(b);
  if (a.nodeType !== 1) {
    if (a.data !== b.data) a.data = b.data;
    return undefined;
  }
  for (const { name } of [...a.attributes]) if (name !== "open" && !b.hasAttribute(name)) a.removeAttribute(name);
  for (const { name, value } of b.attributes) if (name !== "open" && a.getAttribute(name) !== value) a.setAttribute(name, value);
  const from = [...a.childNodes];
  const to = [...b.childNodes];
  to.forEach((node, i) => (from[i] ? morph(from[i], node) : a.appendChild(node)));
  for (const node of from.slice(to.length)) node.remove();
  return undefined;
}
