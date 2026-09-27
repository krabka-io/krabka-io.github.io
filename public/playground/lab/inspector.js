// The inspector: the side panel for the selected node.
//
// Header (kind, id, name, alive, hosted by), fault buttons, the node's
// control commands (send, rate, pause, query, ...; `commands` in `kinds.js`),
// then three tabs: the kind-specific view of the snapshot's `state` (from
// `views.js`), the config form (the same form the palette uses to add a
// node), and the raw snapshot JSON. The state view re-renders at most four
// times a second and only when the state changed; the JSON tree keeps the
// branches the reader opened across renders. The command bar is built once
// per node and only enabled or disabled after that, so what the reader types
// in it survives the snapshots.

import { el, button, select } from "./dom.js";
import { kindOf, renderState, commandObject } from "./kinds.js";
import { buildForm } from "./forms.js";
import { FAULT } from "./faults.js";

const STATE_INTERVAL_MS = 250;
const RAW_INTERVAL_MS = 500;

export class Inspector {
  // hooks: onFault(fault), onCommand(id, command), onControl(id, command) →
  // { ok, answer | error }, onHostChange(id, peerId), onTakeOver(id),
  // onUpdateNode(id, spec) → boolean, formCtx() → { nodes }, peerName(peerId),
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

    this.commands = el("div", "lab-insp-commands");
    this.commands.setAttribute("aria-label", "Node commands");
    this.commands.hidden = true;
    this.body.appendChild(this.commands);
    this.commandKey = "";
    this.commandControls = [];

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
    this.renderCommands(n);
    if (this.tab === "state") this.renderState(n, force);
    else if (this.tab === "raw") this.renderRaw(n, force);
    else if (this.tab === "config" && this.formFor !== this.configKey(n)) this.renderConfig(true);
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
    const fault = (f, label, title, disabled) => this.actions.appendChild(button(label, "lab-btn-sm", () => this.hooks.onFault(f), { title, disabled }));
    if (n.alive) fault(FAULT.kill(n.id), "Kill", "Halt the node; its disk survives");
    else fault(FAULT.restart(n.id), "Restart", "Boot again from the kept state");
    fault(FAULT.wipe(n.id), "Wipe", "Boot again from nothing");
    if (n.isolated) fault(FAULT.reconnect(n.id), "Reconnect", "Restore every link");
    else fault(FAULT.isolate(n.id), "Isolate", "Cut every link");
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
      spec: this.data?.scenario?.nodes?.find((s) => s.id === n.id) || null,
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
      if (r.errors.length) return;
      const next = { ...spec, name: nameInput.value.trim() || spec.name, config: r.value };
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

// A command's answer as one line.
function answerText(answer) {
  if (answer == null) return "done";
  if (typeof answer !== "object") return String(answer);
  const text = JSON.stringify(answer);
  return text.length > 160 ? `${text.slice(0, 159)}…` : text;
}
