// The Cluster Lab page: `/docs/lab`.
//
// Boots the WebAssembly module, builds the panels, and wires them to one
// `LabWorld`. The world runs on the animation frame; the panels read the
// snapshot back. Scenarios autosave to IndexedDB with their durable node
// state, and several tabs can share one cluster over WebRTC (`session.js`).
//
// Everything on screen came out of the module: no node logic lives here.

import init, { Lab } from "../krabka_playground.js";
import { el, button, select, labelled, fmtMs, fmtNum, copyToClipboard, debounce, Toasts } from "./dom.js";
import { LabWorld, SPEEDS } from "./world.js";
import { Canvas } from "./canvas.js";
import { Inspector } from "./inspector.js";
import { Timeline } from "./timeline.js";
import { FaultBar, FAULT, describeFault } from "./faults.js";
import { Palette } from "./palette.js";
import { StoragePanel } from "./storage-panel.js";
import { LabStorage } from "./storage.js";
import { Session, joinCodeFromUrl } from "./session.js";
import { KINDS, kindOf, defaultName, probeAvailability } from "./kinds.js";
import { PRESETS, presetById } from "./presets.js";
import { buildForm, openDialog } from "./forms.js";
import { validateScenario, saveLocal, loadLocal, exportScenario, importScenario, shareLink, scenarioFromHash } from "./scenarios.js";

const ROOT_ID = "krabka-lab";
const AUTOSAVE_MS = 800;
const BROADCAST_MS = 150;
const DEFAULT_PRESET = "network-probe";

function newScenarioId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
}

class LabApp {
  constructor(root) {
    this.root = root;
    root.innerHTML = "";
    this.selection = [];
    this.availability = {};
    this.saveState = "";
    this.remoteStates = new Map();
    this.toasts = new Toasts(root);

    this.storage = new LabStorage({ onError: (err, ctx) => this.toasts.error(err, ctx) });
    this.session = new Session({
      onPeers: () => this.onPeers(),
      onScenario: (doc, hosting) => this.onSessionScenario(doc, hosting),
      onIngress: (frames) => this.world.pushIngress(frames),
      onRemoteSnapshot: (id, state) => this.world.applyRemoteSnapshot(id, state),
      onFault: (fault) => {
        this.world.fault(fault);
        this.toasts.info(`peer: ${describeFault(fault, (id) => this.nodeName(id))}`);
      },
      onTakeover: (node, peer) => this.session.setHost(node, peer),
      onError: (err, ctx) => this.toasts.error(err, ctx),
      onLog: (text) => this.toasts.info(text),
      hostedSnapshots: () => (this.world.snapshot()?.nodes || []).filter((n) => n.hosted).map((n) => ({ id: n.id, state: n.state })),
      scenario: () => this.world.scenario(),
    });
    this.world = new LabWorld(Lab, {
      onError: (err, ctx) => this.toasts.error(err, ctx),
      onSnapshot: (snap) => this.onSnapshot(snap),
      onEvents: (events) => this.timeline.append(events),
      onEgress: (frames) => this.session.sendEgress(frames),
      onDurable: (ops) => this.storage.queueOps(this.world.id, ops),
      onLoad: (doc, images) => this.storage.resetMirror(images),
      onChange: (opts) => this.onChange(opts),
      onReset: () => this.onReset(),
      onClock: () => this.renderClock(),
    });
    this.availability = probeAvailability(Lab);

    this.buildLayout();
    this.autosave = debounce(() => this.saveNow(), AUTOSAVE_MS);
    this.broadcast = debounce(() => this.session.broadcastScenario(), BROADCAST_MS);
    window.addEventListener("beforeunload", () => {
      this.autosave.cancel();
      this.saveNow();
      this.session.leave();
    });
  }

  // ---- layout ---------------------------------------------------------------------------------------

  buildLayout() {
    const root = this.root;
    this.toolbar = el("div", "lab-toolbar");
    root.appendChild(this.toolbar);
    this.buildToolbar();

    const main = el("div", "lab-main");
    const left = el("div", "lab-col lab-col-left");
    const center = el("div", "lab-col lab-col-center");
    const right = el("div", "lab-col lab-col-right");
    main.append(left, center, right);
    root.appendChild(main);

    this.palette = new Palette(left, {
      presets: PRESETS,
      onAddNode: (kind) => this.addNodeDialog(kind),
      onAddTopic: () => this.topicDialog(null),
      onEditTopic: (name) => this.topicDialog(name),
      onRemoveTopic: (name) => this.world.setTopics(this.world.topics.filter((t) => t.name !== name)),
      onPreset: (id) => this.loadPreset(id),
      onOpenSaved: (id) => this.openSaved(id),
      onDeleteSaved: (id) => this.deleteSaved(id),
      onExport: () => exportScenario(this.world.scenario()),
      onImport: (file) => this.importFile(file),
      onShare: () => this.share(),
      onSave: () => this.saveNow(true),
      onSettings: () => this.settingsDialog(),
      onClear: () => this.newScenario(),
      listSaved: () => this.storage.listScenarios(),
    });

    this.canvas = new Canvas(center, {
      onSelect: (id, { additive }) => this.select(id, additive),
      onDeselect: () => this.select(null),
      onMove: (id, x, y) => this.world.setPosition(id, x, y),
      onCommand: (id, command) => this.command(id, command),
      menuItems: (id) => this.menuItems(id),
      nodeName: (id) => this.nodeName(id),
      peerName: (id) => this.session.peerName(id),
    });
    this.faultBar = new FaultBar(center, {
      onFault: (f) => this.fault(f),
      nodeName: (id) => this.nodeName(id),
    });

    this.inspector = new Inspector(right, {
      onFault: (f) => this.fault(f),
      onCommand: (id, command) => this.command(id, command),
      onHostChange: (id, peer) => this.session.setHost(id, peer),
      onTakeOver: (id) => this.session.requestTakeover(id),
      onUpdateNode: (id, spec) => this.updateNodeConfig(id, spec),
      formCtx: () => ({ nodes: this.nodeList() }),
      peerName: (id) => this.session.peerName(id),
      nodeName: (id) => this.nodeName(id),
      nodeLabelForBroker: (brokerId) => this.nodeLabelForBroker(brokerId),
    });
    this.storagePanel = new StoragePanel(right, {
      storage: this.storage,
      scenarioId: () => this.world.id,
      nodes: () => this.nodeList(),
      onForgetNode: (id) => this.forgetNode(id),
      onForgetScenario: () => this.forgetScenario(),
      onPersistChange: (on) => {
        // Turning it on stores the live state first, so nothing skipped while
        // it was off leaves a gap.
        this.storage.setPersist(on, this.world.id, this.session.myHostedIds());
        this.toasts.info(on ? "Persisting durable state to this browser" : "Persistence off: new changes are not stored");
      },
    });
    this.buildSessionPanel(right);

    this.timeline = new Timeline(root, {
      onSelect: (id) => this.select(id),
      nodeName: (id) => this.nodeName(id),
    });

    root.addEventListener("keydown", (e) => {
      if (e.target.closest("input, textarea, select, dialog")) return;
      if (e.key === " " && e.target.closest(".lab-canvas")) {
        e.preventDefault();
        this.togglePlay();
      } else if (e.key === "f" || e.key === "F") this.canvas.fit();
      else if (e.key === "Escape") this.select(null);
    });
  }

  buildToolbar() {
    const t = this.toolbar;
    t.appendChild(el("span", "lab-brand", "Cluster Lab"));
    this.playBtn = button("Pause", "lab-btn-sm lab-primary", () => this.togglePlay(), { title: "Run or pause the simulated clock (Space on the canvas)" });
    this.playBtn.dataset.action = "play";
    const step = (ms, label) => button(label, "lab-btn-sm", () => this.world.step(ms), { title: `Advance ${label} of simulated time`, data: { step: String(ms) } });
    this.speedSel = select(
      SPEEDS.map((s) => ({ value: s, label: `${s}×` })),
      1,
      (v) => this.world.setSpeed(Number(v)),
    );
    this.speedSel.setAttribute("aria-label", "Simulation speed");
    this.settleBtn = button("Settle", "lab-btn-sm", () => this.world.settle(), { title: "Run until nothing is due (at most 5 s of simulated time)" });
    this.timeEl = el("span", "lab-time", "0 ms");
    this.timeEl.dataset.field = "sim-time";
    this.statsEl = el("span", "lab-stats lab-muted");
    this.roleEl = el("span", "lab-role");
    t.append(this.playBtn, step(10, "+10 ms"), step(100, "+100 ms"), step(1000, "+1 s"), this.settleBtn, el("span", "lab-label", "speed"), this.speedSel, this.timeEl, this.statsEl, this.roleEl);
  }

  buildSessionPanel(container) {
    const d = el("details", "lab-session lab-side-section");
    d.open = false;
    d.appendChild(el("summary", "lab-panel-title", "Session"));
    const body = el("div", "lab-side-body");
    d.appendChild(body);
    this.sessionBody = body;
    container.appendChild(d);
    this.sessionDetails = d;
    this.renderSession();
  }

  renderSession() {
    const s = this.session;
    const body = this.sessionBody;
    body.innerHTML = "";
    if (!s.available) {
      body.appendChild(el("p", "lab-muted lab-small", "This browser has no WebRTC, so the cluster runs in this tab only."));
      return;
    }
    body.appendChild(
      el(
        "p",
        "lab-muted lab-small",
        "Several tabs, on one machine or across the internet, can each host a share of the nodes. Frames between tabs travel over WebRTC data channels; there is no server.",
      ),
    );
    const nameRow = el("label", "lab-field-inline");
    const nameInput = el("input", "lab-input lab-input-sm");
    nameInput.value = s.name;
    nameInput.setAttribute("aria-label", "Your peer name");
    nameInput.addEventListener("change", () => {
      s.name = nameInput.value.trim() || s.name;
      nameInput.value = s.name;
      this.renderSession();
      this.pushPanels();
    });
    nameRow.append(el("span", "lab-muted", "this tab"), nameInput, el("span", "lab-role-badge", s.role));
    body.appendChild(nameRow);

    const peers = el("ul", "lab-peer-list");
    for (const p of s.peerList()) {
      const li = el("li", `lab-peer lab-peer-${p.state}`);
      li.dataset.peer = p.id;
      const hosted = [...s.hosting].filter(([, peer]) => peer === p.id).map(([n]) => this.nodeName(n));
      li.append(el("span", "lab-peer-name", p.name), el("span", "lab-muted lab-small", `${p.state}${hosted.length ? ` · ${hosted.join(", ")}` : ""}`));
      peers.appendChild(li);
    }
    body.appendChild(peers);

    const actions = el("div", "lab-form-actions");
    if (s.role !== "spoke") {
      actions.appendChild(
        button("Invite a tab…", "lab-btn-sm", () => this.inviteDialog(), { title: "Make a link another tab or machine opens to join" }),
      );
    }
    if (s.role !== "solo") actions.appendChild(button("Leave session", "lab-btn-sm lab-danger", () => this.leaveSession()));
    body.appendChild(actions);
    if (this.answerCode) {
      body.appendChild(el("p", "lab-small", "Give this answer code to the host to finish joining:"));
      const ta = el("textarea", "lab-textarea lab-code");
      ta.readOnly = true;
      ta.rows = 3;
      ta.value = this.answerCode;
      ta.dataset.field = "answer-code";
      body.appendChild(ta);
      body.appendChild(button("Copy answer", "lab-btn-sm", async () => this.toasts.info((await copyToClipboard(this.answerCode)) ? "Answer copied" : "Copy failed; select the text")));
    }
  }

  async inviteDialog() {
    let link = "";
    const body = el("div", "lab-invite");
    body.appendChild(el("p", "lab-small", "1. Send this link to the other tab or machine. It carries the connection offer, so it works once."));
    const linkTa = el("textarea", "lab-textarea lab-code");
    linkTa.readOnly = true;
    linkTa.rows = 3;
    linkTa.value = "making the invite…";
    body.appendChild(linkTa);
    body.appendChild(button("Copy link", "lab-btn-sm", async () => this.toasts.info((await copyToClipboard(link)) ? "Link copied" : "Copy failed; select the text")));
    body.appendChild(el("p", "lab-small", "2. Paste the answer code that page shows:"));
    const answerTa = el("textarea", "lab-textarea lab-code");
    answerTa.rows = 3;
    answerTa.placeholder = "answer code";
    body.appendChild(answerTa);
    const status = el("p", "lab-muted lab-small");
    body.appendChild(status);
    const promise = openDialog(this.root, {
      title: "Invite a tab",
      body,
      submitLabel: "Accept answer",
      wide: true,
      onSubmit: async () => {
        try {
          await this.session.acceptAnswer(answerTa.value.trim());
          this.toasts.info("Peer accepted; connecting");
          return true;
        } catch (err) {
          status.textContent = err.message;
          return false;
        }
      },
    });
    try {
      link = await this.session.createInvite(window.location.href);
      linkTa.value = link;
      this.renderSession();
      this.pushPanels();
    } catch (err) {
      linkTa.value = `could not make an invite: ${err.message}`;
    }
    await promise;
  }

  leaveSession() {
    this.session.leave();
    this.answerCode = null;
    this.world.setHosted(null);
    this.renderSession();
    this.pushPanels();
  }

  // ---- state flow ------------------------------------------------------------------------------------------

  onSnapshot(snap) {
    this.pushPanels(snap);
  }

  pushPanels(snap = this.world.snapshot()) {
    const scenario = this.world.scenario();
    const session = {
      role: this.session.role,
      hosting: this.session.hosting,
      peers: this.session.peerList(),
      me: this.session.me,
      peersKey: this.session.peersKey,
    };
    this.canvas.setData({
      snapshot: snap,
      scenario,
      hosting: this.session.role === "solo" ? null : this.session.hosting,
      me: this.session.me,
      offline: this.session.offlinePeers(),
      selection: this.selection,
    });
    if (snap) this.canvas.animate(snap.now);
    this.inspector.update({ snapshot: snap, scenario, session });
    this.faultBar.update({ snapshot: snap, selection: this.selection });
    this.timeline.setNodes(this.nodeList());
    this.palette.update({ scenario, availability: this.availability, role: this.session.role, saveState: this.saveState });
    this.renderClock(snap);
  }

  renderClock(snap = this.world.snapshot()) {
    this.playBtn.textContent = this.world.paused ? "Play" : "Pause";
    this.playBtn.setAttribute("aria-pressed", String(!this.world.paused));
    this.timeEl.textContent = fmtMs(snap ? snap.now : this.world.now());
    if (snap) {
      const delivered = (snap.delivered || []).reduce((s, d) => s + (d[2] || 0), 0);
      this.statsEl.textContent = `${snap.nodes.length} nodes · ${snap.in_flight.length} in flight · ${fmtNum(delivered)} delivered · ${fmtNum(snap.event_count)} events`;
    }
    const r = this.session.role;
    this.roleEl.textContent = r === "solo" ? "" : `${r} · ${this.session.peerList().filter((p) => p.state === "connected").length} peers`;
  }

  onChange(opts = {}) {
    if (this.session.role === "hub") this.broadcast();
    if (this.session.role !== "spoke") this.autosave();
    if (!opts.positionOnly) this.pushPanels();
  }

  onReset() {
    this.timeline.clear();
    this.select(null);
    this.canvas.fitted = false;
  }

  onPeers() {
    this.renderSession();
    this.pushPanels();
  }

  // The hub sent the scenario and the hosting map (or, on the hub itself,
  // the hosting map changed).
  onSessionScenario(doc, hosting) {
    const mine = this.session.myHostedIds();
    const cur = this.world.scenario();
    if (this.session.role === "spoke" && cur.id !== doc.id) {
      this.world.load(doc, mine);
    } else {
      this.world.reconcile(doc, mine);
    }
    this.renderSession();
    this.pushPanels();
  }

  togglePlay() {
    this.world.setPaused(!this.world.paused);
    this.renderClock();
  }

  // ---- selection and node commands -----------------------------------------------------------------------------

  select(id, additive = false) {
    if (id == null) this.selection = [];
    else if (additive && this.selection.length && this.selection[0] !== id) this.selection = [this.selection[0], id];
    else this.selection = [id];
    this.inspector.setSelection(this.selection[0] ?? null);
    this.pushPanels();
  }

  nodeList() {
    return this.world.scenario().nodes.map((n) => ({ id: n.id, kind: n.kind, name: n.name }));
  }

  nodeName(id) {
    const n = this.world.scenario().nodes.find((s) => s.id === id);
    return n ? n.name || `#${id}` : `#${id}`;
  }

  // A broker id as the inspector shows it: the node that carries it.
  nodeLabelForBroker(brokerId) {
    const n = this.world.scenario().nodes.find((s) => s.kind === "broker" && Number(s.config?.broker_id) === Number(brokerId));
    return n ? `${n.name} (broker ${brokerId})` : `broker ${brokerId}`;
  }

  menuItems(id) {
    const snap = this.world.snapshot()?.nodes.find((n) => n.id === id);
    const k = kindOf(snap?.kind);
    const editable = this.session.role !== "spoke" && !k.hidden;
    return [
      { label: "Edit…", command: "edit", disabled: !editable },
      { label: "Send command…", command: "control" },
      { separator: true },
      { label: snap?.alive ? "Kill" : "Restart", command: snap?.alive ? "kill" : "restart" },
      { label: "Wipe (restart from nothing)", command: "wipe" },
      { label: snap?.isolated ? "Reconnect" : "Isolate", command: snap?.isolated ? "reconnect" : "isolate" },
      { separator: true },
      { label: "Remove", command: "remove", danger: true, disabled: !editable },
    ];
  }

  command(id, command) {
    switch (command) {
      case "edit":
        this.select(id);
        this.inspector.showTab("config");
        break;
      case "remove":
        if (this.session.role === "spoke") return;
        this.world.removeNode(id);
        if (this.selection.includes(id)) this.select(null);
        break;
      case "control":
        this.controlDialog(id);
        break;
      case "kill":
      case "restart":
      case "wipe":
      case "isolate":
      case "reconnect":
        this.fault(FAULT[command](id));
        break;
      default:
    }
  }

  // The inspector's Apply. Only the host edits the scenario: an edit in a
  // spoke would change that tab's world and nobody else's.
  updateNodeConfig(id, spec) {
    if (this.session.role === "spoke") {
      this.toasts.warn("Only the host edits node configuration");
      return false;
    }
    return this.world.updateNode(id, spec);
  }

  fault(f) {
    if (this.world.fault(f)) {
      this.session.broadcastFault(f);
      this.toasts.info(describeFault(f, (id) => this.nodeName(id)));
    }
  }

  async controlDialog(id) {
    const ta = el("textarea", "lab-textarea lab-code");
    ta.rows = 4;
    ta.value = JSON.stringify(kindOf(this.world.spec(id)?.kind).kind === "pinger" ? { cmd: "ping" } : { cmd: "" }, null, 2);
    const out = el("pre", "lab-raw");
    const body = el("div");
    body.append(labelled("Command JSON", ta, "Handed to the node's control handler; the answer appears below."), out);
    await openDialog(this.root, {
      title: `Command for ${this.nodeName(id)}`,
      body,
      submitLabel: "Send",
      cancelLabel: "Close",
      onSubmit: () => {
        let cmd;
        try {
          cmd = JSON.parse(ta.value);
        } catch (err) {
          out.textContent = `invalid JSON: ${err.message}`;
          return false;
        }
        const r = this.world.control(id, cmd);
        out.textContent = r.ok ? JSON.stringify(r.answer, null, 2) : `error: ${r.error}`;
        this.world.flush(performance.now(), true);
        return false;
      },
    });
  }

  // ---- dialogs -----------------------------------------------------------------------------------------------------

  async addNodeDialog(kind) {
    const k = KINDS[kind];
    if (!k) return;
    if (this.availability[kind] === false) this.toasts.warn(`${k.label}: not in the loaded module yet; the crate will reject it`);
    const scenario = this.world.scenario();
    const nextId = scenario.nodes.reduce((m, n) => Math.max(m, n.id), 0) + 1;
    const nameInput = el("input", "lab-input");
    nameInput.type = "text";
    nameInput.value = defaultName(kind, nextId);
    const defaults = {};
    if (kind === "broker") defaults.broker_id = nextId;
    const form = buildForm(k.fields, defaults, { nodes: this.nodeList() });
    const body = el("div");
    body.append(labelled("Name", nameInput), form.root);
    if (!k.fields.length) body.appendChild(el("p", "lab-muted lab-small", "This kind has no configuration."));
    const err = el("p", "lab-field-error");
    body.appendChild(err);
    await openDialog(this.root, {
      title: `Add ${k.label.toLowerCase()}`,
      body,
      submitLabel: "Add",
      onSubmit: () => {
        const r = form.read();
        if (r.errors.length) return false;
        const existing = scenario.nodes.map((n) => ({ x: n.x, y: n.y }));
        const pos = freePosition(existing);
        const spec = { id: 0, kind, name: nameInput.value.trim() || defaultName(kind, nextId), x: pos.x, y: pos.y, config: r.value };
        const id = this.world.addNode(spec);
        if (id == null) {
          err.textContent = "The module rejected this node; see the message above.";
          return false;
        }
        this.select(id);
        return true;
      },
    });
  }

  async topicDialog(name) {
    const existing = name ? this.world.topics.find((t) => t.name === name) : null;
    const fields = [
      { key: "name", label: "Name", type: "text", required: true, placeholder: "orders" },
      { key: "partitions", label: "Partitions", type: "number", default: 3, min: 1, step: 1, required: true },
      { key: "replication_factor", label: "Replication factor", type: "number", default: -1, min: -1, step: 1, required: true, help: "-1 uses the broker default." },
    ];
    const form = buildForm(fields, existing || {}, {});
    await openDialog(this.root, {
      title: existing ? `Edit topic ${name}` : "Add topic",
      body: form.root,
      onSubmit: () => {
        const r = form.read();
        if (r.errors.length) return false;
        const topics = this.world.topics.filter((t) => t.name !== name);
        if (topics.some((t) => t.name === r.value.name)) {
          form.root.appendChild(el("p", "lab-field-error", "a topic with that name exists"));
          return false;
        }
        topics.push({ name: r.value.name, partitions: r.value.partitions, replication_factor: r.value.replication_factor });
        this.world.setTopics(topics);
        return true;
      },
    });
  }

  async settingsDialog() {
    const cur = this.world.scenario();
    const fields = [
      { key: "name", label: "Name", type: "text", placeholder: "My cluster" },
      { key: "seed", label: "Seed", type: "number", required: true, min: 0, step: 1, help: "Changing the seed restarts the world." },
      { key: "latency", label: "Default link latency (ms)", type: "number", required: true, min: 0, step: 1, help: "Changing it restarts the world." },
    ];
    const form = buildForm(fields, { name: cur.name, seed: cur.seed, latency: cur.links.default_latency_ms }, {});
    await openDialog(this.root, {
      title: "Scenario settings",
      body: form.root,
      onSubmit: () => {
        const r = form.read();
        if (r.errors.length) return false;
        if (r.value.seed === cur.seed && r.value.latency === cur.links.default_latency_ms) {
          this.world.setName(r.value.name || "");
        } else {
          const doc = { ...cur, name: r.value.name || "", seed: r.value.seed, links: { default_latency_ms: r.value.latency } };
          this.openScenario(doc, { keepId: true });
        }
        return true;
      },
    });
  }

  // ---- scenarios ---------------------------------------------------------------------------------------------------

  // Replace the world. `keepId` keeps the identity (and reads its durable
  // state back); otherwise the scenario gets a fresh one.
  async openScenario(doc, { keepId = false, images = null } = {}) {
    if (this.session.role === "spoke") {
      this.toasts.warn("Only the host changes the scenario");
      return false;
    }
    const next = { ...doc };
    if (!keepId || !next.id) next.id = newScenarioId();
    let imgs = images;
    if (keepId && imgs == null) {
      if (this.world.ready && next.id === this.world.id) {
        // Restarting the running scenario (a new seed, a reopen): its nodes
        // restart from their live durable state, stored or not.
        this.world.drainDurable();
        imgs = this.storage.mirrorImages();
      } else if (this.storage.persist) {
        try {
          imgs = await this.storage.loadImages(next.id);
        } catch (err) {
          this.toasts.error(err, "read durable state");
        }
      }
    }
    const ok = this.world.load(next, this.session.myHostedIds(), imgs);
    if (ok) {
      this.session.syncHosting();
      this.session.broadcastScenario();
      this.autosave();
      this.storagePanel.refresh();
      const restored = imgs ? Object.keys(imgs).length : 0;
      if (restored) this.toasts.info(`Restored durable state for ${restored} node${restored === 1 ? "" : "s"}`);
    }
    return ok;
  }

  loadPreset(id) {
    const p = presetById(id);
    if (!p) return;
    const missing = [...new Set(p.scenario.nodes.map((n) => n.kind))].filter((k) => this.availability[k] === false);
    if (missing.length) this.toasts.warn(`This preset needs the full build (${missing.join(", ")} not in the loaded module); the crate will reject it`);
    this.openScenario(p.scenario);
  }

  newScenario() {
    this.openScenario({ version: 1, seed: 1, name: "", links: { default_latency_ms: 5 }, nodes: [], topics: [] });
  }

  async openSaved(id) {
    try {
      const doc = await this.storage.loadScenario(id);
      if (!doc) {
        this.toasts.warn("That scenario is gone");
        return;
      }
      await this.openScenario(doc, { keepId: true });
      this.toasts.info(`Opened ${doc.name || "saved scenario"}`);
    } catch (err) {
      this.toasts.error(err, "open saved scenario");
    }
  }

  async deleteSaved(id) {
    try {
      await this.storage.deleteScenario(id);
      if (this.world.id === id) this.world.setId("");
      this.palette.refreshSaved();
      this.toasts.info("Deleted");
    } catch (err) {
      this.toasts.error(err, "delete saved scenario");
    }
  }

  async importFile(file) {
    try {
      const doc = await importScenario(file);
      await this.openScenario(doc);
      this.toasts.info(`Imported ${doc.name || file.name}`);
    } catch (err) {
      this.toasts.error(err, "import");
    }
  }

  async share() {
    try {
      const url = await shareLink(this.world.scenario());
      const ok = await copyToClipboard(url);
      this.toasts.info(ok ? "Link copied" : "Copy failed; the link is in the address bar");
      if (!ok) window.location.hash = new URL(url).hash;
    } catch (err) {
      this.toasts.error(err, "share");
    }
  }

  // Save the running scenario: the document to IndexedDB (and as the last
  // scenario in localStorage). The first save assigns the identity.
  async saveNow(announce = false) {
    if (this.session.role === "spoke") return;
    const doc = this.world.scenario();
    if (!doc.nodes.length && !doc.name && !doc.topics.length) return;
    if (!doc.id) {
      const id = newScenarioId();
      this.world.setId(id);
      // Ops drained before the scenario had an identity were not stored.
      this.storage.syncFromMirror(id, this.session.myHostedIds());
      return; // setId triggers onChange, which schedules this again
    }
    try {
      const saved = await this.storage.saveScenario(doc);
      saveLocal(doc);
      this.saveState = saved ? `saved ${new Date().toLocaleTimeString()}` : "saved to this page only";
      if (announce) this.toasts.info(this.saveState);
      this.palette.update({ scenario: doc, availability: this.availability, role: this.session.role, saveState: this.saveState });
      if (this.palette.savedSection.details.open) this.palette.refreshSaved();
    } catch (err) {
      this.toasts.error(err, "save");
    }
  }

  async forgetNode(id) {
    try {
      await this.storage.forgetNode(this.world.id, id);
      this.storagePanel.refresh();
      this.toasts.info(`Forgot stored data of ${this.nodeName(id)}`);
    } catch (err) {
      this.toasts.error(err, "forget");
    }
  }

  async forgetScenario() {
    try {
      // In a session, only the nodes this tab hosts: another tab in this
      // browser may be storing the rest under the same scenario.
      const hosted = this.session.myHostedIds();
      const ids = hosted ?? this.nodeList().map((n) => n.id);
      await this.storage.forgetScenario(this.world.id, ids, hosted == null);
      this.storagePanel.refresh();
      this.toasts.info("Forgot the stored data of this scenario");
    } catch (err) {
      this.toasts.error(err, "forget");
    }
  }

  // ---- boot ------------------------------------------------------------------------------------------------------------

  async start() {
    const join = joinCodeFromUrl(window.location.search);
    if (join) {
      this.world.create(1);
      try {
        this.answerCode = await this.session.join(join);
        this.world.setHosted([]);
        this.sessionDetails.open = true;
        this.renderSession();
        this.toasts.info("Joined as a spoke; hand the answer code to the host");
      } catch (err) {
        this.toasts.error(err, "join");
      }
      // A join link works once; a reload should not try again.
      try {
        history.replaceState(null, "", window.location.pathname);
      } catch {
        // Nothing to clean up.
      }
    } else {
      let opened = false;
      try {
        const shared = await scenarioFromHash(window.location.hash);
        if (shared) {
          opened = await this.openScenario(shared);
          if (opened) this.toasts.info(`Opened shared scenario ${shared.name || ""}`.trim());
        }
      } catch (err) {
        this.toasts.error(err, "shared link");
      }
      if (!opened) {
        const last = loadLocal();
        if (last && last.id) opened = await this.openScenario(last, { keepId: true });
      }
      if (!opened) opened = await this.openScenario(presetById(DEFAULT_PRESET).scenario);
      if (!opened) this.world.create(1);
    }
    this.world.start();
    this.pushPanels();
  }
}

// A spot on the canvas that no card occupies: right of the rightmost card in
// the top row, or a new row when that runs long.
function freePosition(existing) {
  if (!existing.length) return { x: 140, y: 100 };
  const rows = new Map();
  for (const p of existing) {
    const row = Math.round(p.y / 130) * 130;
    rows.set(row, Math.max(rows.get(row) ?? -Infinity, p.x));
  }
  const rowKeys = [...rows.keys()].sort((a, b) => a - b);
  for (const y of rowKeys) if (rows.get(y) + 200 < 1000) return { x: rows.get(y) + 200, y: y || 100 };
  return { x: 140, y: rowKeys[rowKeys.length - 1] + 130 };
}

async function boot() {
  const root = document.getElementById(ROOT_ID);
  if (!root) return;
  try {
    await init();
    const app = new LabApp(root);
    window.krabkaLab = app; // for the end-to-end check and the curious
    await app.start();
    root.dataset.ready = "true";
  } catch (err) {
    root.innerHTML = "";
    const p = el("p", "lab-error", `The Cluster Lab failed to load: ${err instanceof Error ? err.message : String(err)}`);
    root.appendChild(p);
    // eslint-disable-next-line no-console
    console.error("krabka lab failed to initialise", err);
  }
}

boot();
