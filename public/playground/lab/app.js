// The Cluster Lab page: `/docs/lab`.
//
// Boots the WebAssembly module, builds the panels, and wires them to one
// `LabWorld`. The world runs on the animation frame; the panels read the
// snapshot back. Scenarios autosave to IndexedDB with their durable node
// state, and several tabs can share one cluster over WebRTC (`session.js`).
//
// Everything on screen came out of the module: no node logic lives here. A
// real broker is the exception that proves it: `external.js` runs its
// process in a Worker, and the page reloads once, cross-origin isolated, the
// first time a scenario with one runs here.

import init, { Lab } from "../krabka_playground.js";
import { el, button, select, labelled, fmtMs, fmtNum, plural, copyToClipboard, debounce, clamp, Toasts } from "./dom.js";
import { LabWorld, SPEEDS } from "./world.js";
import { Canvas } from "./canvas.js";
import { Inspector } from "./inspector.js";
import { Timeline } from "./timeline.js";
import { FaultBar, FAULT, describeFault, cutLinks, cutOff } from "./faults.js";
import { Palette } from "./palette.js";
import { TabSet } from "./tabs.js";
import { Tour } from "./tour.js";
import { StoragePanel } from "./storage-panel.js";
import { NetworkPanel } from "./network-panel.js";
import { Capture } from "./capture.js";
import { LabStorage } from "./storage.js";
import { Session, joinCodeFromUrl, NAME_MAX } from "./session.js";
import { KINDS, KIND_ORDER, kindOf, defaultName, probeAvailability, suggestedConfig, commandObject } from "./kinds.js";
import { PRESETS, presetById } from "./presets.js";
import { buildForm, openDialog } from "./forms.js";
import { validateScenario, saveLocal, loadLocal, clearLocal, exportScenario, importScenario, shareLink, scenarioFromHash } from "./scenarios.js";
import { ExternalHost, REAL_BROKER_KIND, hasRealBroker, volumeName } from "./external.js";
import { LogStore, LogLevels } from "./logstore.js";
import { LogsPanel } from "./logs.js";
import { KafkactlBridge, LOCAL_CLIENT_KIND, NO_CLIENT_NODE } from "./kafkactl.js";
// ---- J2: cluster state, charts, invariants, record trace ----
import { ClusterPanel, clusterHealth, decorateCanvas } from "./cluster-panel.js";
import { ChartsPanel, Sampler, clusterOf, faultMarkers } from "./charts.js";
import { InvariantChecker } from "./invariants.js";
import { TracePanel } from "./trace.js";
// ---- /J2 ----
// ---- J3: experiments, fork here, focus mode, process faults ----
import { ExperimentRunner, ExperimentPanel, validateExperiment } from "./experiment.js";
import { oneWayCut } from "./faults.js";
// Set before a run's isolation reload, so the reloaded page starts the experiment.
const RUN_EXPERIMENT_KEY = "krabka-lab.run-experiment";
// The scenario ids fresh runs created, so the next fresh run can drop them.
const FRESH_RUNS_KEY = "krabka-lab.fresh-runs";
// ---- /J3 ----

const ROOT_ID = "krabka-lab";
const AUTOSAVE_MS = 800;
const BROADCAST_MS = 150;
const DEFAULT_PRESET = "single-broker";
const COI_URL = new URL("../../docs/lab/coi.js", import.meta.url).href;
// Set just before the isolation reload, so the reloaded page can say why.
const RELOADED_KEY = "krabka-lab.isolation-reload";
// Why an invite or share link failed to open, kept across that reload.
const SHARE_ERROR_KEY = "krabka-lab.share-error";
// The work area's layout in this browser: folded columns and the dock's share
// of the stage height per tab.
const LAYOUT_KEY = "krabka-lab.layout";
const SPLIT_DEFAULTS = { events: 0.3, logs: 0.5, network: 0.62, storage: 0.6, cluster: 0.55, charts: 0.55, trace: 0.5 };
const MIN_CANVAS_PX = 120;

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
    this.pageEvents = 0;
    this.toasts = new Toasts(root);

    this.storage = new LabStorage({ onError: (err, ctx) => this.toasts.error(err, ctx) });
    this.capture = new Capture({ onChange: () => this.networkPanel?.changed() });
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
      // The hub's tab left or dropped: this tab keeps the last scenario but cannot edit it.
      onHubLost: () => this.toasts.show("The host is gone. This tab keeps the last scenario; leave the session to run it on your own.", { level: "warn", ttl: 0, action: { label: "Leave session", run: () => this.leaveSession() } }),
      hostedSnapshots: () => (this.world.snapshot()?.nodes || []).filter((n) => n.hosted).map((n) => ({ id: n.id, state: n.state })),
      scenario: () => this.world.scenario(),
    });
    // What those processes write, and the level they are asked to write at.
    this.logs = new LogStore();
    this.logLevels = new LogLevels();
    // The processes behind this tab's real brokers.
    this.external = new ExternalHost({
      log: (id, entry) => this.recordLog(id, entry),
      logLevel: (id) => this.logLevels.directive(this.world.id ?? "", id),
      route: (frames) => this.world.routeExternal(frames),
      publish: (id, state) => this.world.applyRemoteSnapshot(id, state),
      world: () => this.world.liveSnapshot(),
      now: () => this.world.now(),
      scenario: () => this.world.scenario(),
      event: (id, kind, detail) => this.processEvent(id, kind, detail),
      exited: (id, exit) => this.processExited(id, exit),
    });
    this.bridge = new KafkactlBridge({
      route: (frames) => this.world.routeExternal(frames),
      publish: (id, state) => this.world.applyRemoteSnapshot(id, state),
      world: () => this.world.liveSnapshot(),
      status: (state, reason) => this.renderBridgeStatus(state, reason),
    });
    this.world = new LabWorld(Lab, {
      onError: (err, ctx) => this.toasts.error(err, ctx),
      onSnapshot: (snap) => this.onSnapshot(snap),
      onEvents: (events) => {
        this.timeline.append(events);
        this.j2Events?.(events); // J2
        this.experiment?.onEvents(events); // J3
      },
      onEgress: (frames) => this.session.sendEgress(frames),
      onDurable: (ops) => this.storage.queueOps(this.world.id, ops),
      onWire: (frames, dropped) => this.capture.add(frames, dropped),
      onLoad: (doc, images) => {
        this.storage.resetMirror(images);
        // A capture belongs to one run of one scenario: its clock starts again.
        this.capture.clear();
        this.j2Reset?.(); // J2: the charts and the invariants start again with the run
        this.experiment?.stop("the scenario was replaced"); // J3
      },
      onChange: (opts) => this.onChange(opts),
      onReset: () => this.onReset(),
      onClock: () => this.renderClock(),
      onUpgrade: (ids) => {
        const one = ids.length === 1;
        this.toasts.show(`${one ? "A broker" : `${ids.length} brokers`} in this scenario used the lab's old simulated broker. ${one ? "It now runs" : "They now run"} the real krabka-broker, on a new empty disk, with only the settings the real broker supports.`, { ttl: 12_000 });
      },
    });
    this.world.attachExternal(this.external);
    this.world.attachBridge(this.bridge);
    this.availability = probeAvailability(Lab);

    this.buildLayout();
    this.autosave = debounce(() => this.saveNow(), AUTOSAVE_MS);
    this.broadcast = debounce(() => this.session.broadcastScenario(), BROADCAST_MS);
    window.addEventListener("beforeunload", () => {
      this.autosave.cancel();
      this.saveNow();
      this.session.leave();
      this.bridge.disconnect();
    });
  }

  // ---- layout ---------------------------------------------------------------------------------------

  buildLayout() {
    const root = this.root;
    root.classList.add("lab-app");
    // The lab ends at the bottom of the window: its height is the window's less what sits above it.
    const fitTop = () => root.style.setProperty("--lab-top", `${Math.round(root.getBoundingClientRect().top + window.scrollY)}px`);
    fitTop();
    window.addEventListener("resize", fitTop);
    document.fonts?.ready.then(fitTop);
    this.toolbar = el("div", "lab-toolbar");
    root.appendChild(this.toolbar);
    this.buildToolbar();

    const work = el("div", "lab-workspace");
    const rail = el("div", "lab-col lab-col-rail");
    const stage = el("div", "lab-col lab-col-stage");
    const side = el("div", "lab-col lab-col-side");
    work.append(rail, stage, side);
    root.appendChild(work);

    // Left rail: build, scenarios, connect.
    this.palette = new Palette(rail, {
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
    this.buildSessionPanel(this.palette.connectBody);
    this.buildBridgePanel(this.palette.connectBody);

    // Stage: the fault bar, the canvas, and the dock of logs under it.
    this.faultBar = new FaultBar(stage, {
      onFault: (f) => this.fault(f),
      onBrowseTraffic: () => this.showNetwork(),
      nodeName: (id) => this.nodeName(id),
    });
    this.canvas = new Canvas(stage, {
      onSelect: (id, { additive }) => this.select(id, additive, { scroll: false }),
      onDeselect: () => this.select(null),
      onMove: (id, x, y) => {
        // Only the host owns positions: a move here would be lost at the host's next change.
        if (this.session.role !== "spoke") return this.world.setPosition(id, x, y);
        this.toasts.warn("Only the host moves cards");
        this.pushPanels();
      },
      onCommand: (id, command) => this.command(id, command),
      menuItems: (id) => this.menuItems(id),
      nodeName: (id) => this.nodeName(id),
      peerName: (id) => this.session.peerName(id),
      onOpenTab: (tab) => this.palette.show(tab),
      onHelp: () => this.helpDialog(),
    });

    this.dock = new TabSet(stage, {
      label: "Cluster details",
      active: "events",
      className: "lab-dock",
      tools: true,
      tabs: [
        { id: "events", label: "Events", title: "Everything the cluster recorded, newest at the bottom" },
        { id: "logs", label: "Logs", title: "What the real brokers log, with filters and a setting for how much they log" },
        { id: "network", label: "Network", title: "Every frame on the virtual network: Kafka exchanges decoded field by field, timings and statistics" },
        { id: "storage", label: "Storage", title: "What each node keeps in this browser" },
        // J2
        { id: "cluster", label: "Cluster", title: "The KRaft quorum, the brokers and every partition's leader, ISR and high watermark, as the admin last saw them" },
        { id: "charts", label: "Charts", title: "Throughput, lag, round trips and replication over lab time, the faults on the same axis, and the live invariant checks" },
        { id: "trace", label: "Trace", title: "Follow one produced record through the network capture" },
      ],
      hooks: {
        onShow: (id) => {
          if (this.dock.root.classList.contains("lab-dock-collapsed")) this.setDockCollapsed(false);
          if (id === "logs") this.logsPanel.shown();
          if (id === "network") this.networkPanel.refresh();
          if (id === "storage") this.storagePanel.refresh();
          this.j2Shown?.(id); // J2
          this.applySplit();
        },
      },
    });
    this.dockToggle = button("Hide", "lab-btn-sm lab-dock-toggle", () => this.setDockCollapsed(!this.dock.root.classList.contains("lab-dock-collapsed")), {
      title: "Collapse or restore the details panel",
    });
    this.dock.tools.appendChild(this.dockToggle);
    // Choosing a tab, even the active one, opens a folded dock.
    this.dock.bar.addEventListener("click", (e) => {
      if (e.target.closest(".lab-dtab") && this.dock.root.classList.contains("lab-dock-collapsed")) this.setDockCollapsed(false);
    });
    this.timeline = new Timeline(this.dock.panel("events"), {
      onSelect: (id) => this.select(id),
      nodeName: (id) => this.nodeName(id),
    });
    this.logsPanel = new LogsPanel(this.dock.panel("logs"), {
      store: this.logs,
      levels: this.logLevels,
      dialogRoot: root,
      scenarioId: () => this.world.id ?? "",
      nodes: () => this.logNodes(),
      brokers: () => this.logBrokers(),
      apply: (change) => this.applyLogLevels(change),
      toast: (message) => this.toasts.info(message),
      onBadge: (badge) => this.renderLogBadge(badge),
    });
    this.networkPanel = new NetworkPanel(this.dock.panel("network"), {
      capture: this.capture,
      nodeName: (id) => this.nodeName(id),
      selection: () => this.selection,
      scenario: () => {
        const doc = this.world.scenario();
        return { id: this.world.id, name: doc?.name, seed: doc?.seed };
      },
      expand: () => this.expandDock(),
    });

    // Right column: the inspector.
    this.inspector = new Inspector(side, {
      onCommand: (id, command) => this.command(id, command),
      onControl: (id, command) => this.control(id, command),
      onHostChange: (id, peer) => this.session.setHost(id, peer),
      onTakeOver: (id) => {
        if (!this.session.requestTakeover(id)) this.toasts.warn("Cannot reach the host; leave the session to run this node here.");
      },
      onUpdateNode: (id, spec) => this.updateNodeConfig(id, spec),
      onSelect: (id) => this.select(id),
      onOpenTab: (tab) => this.palette.show(tab),
      onTry: (key) => this.tryIt(key),
      onBrowseVolume: async (id) => {
        // Showing the tab it is already on does not unfold a folded dock.
        this.setDockCollapsed(false);
        this.dock.show("storage");
        await this.storagePanel.refresh();
        await this.storagePanel.browseVolume(volumeName(this.world.id, id));
      },
      formCtx: () => ({ nodes: this.nodeList() }),
      peerName: (id) => this.session.peerName(id),
      nodeName: (id) => this.nodeName(id),
      nodeLabelForBroker: (brokerId) => this.nodeLabelForBroker(brokerId),
    });
    this.storagePanel = new StoragePanel(this.dock.panel("storage"), {
      storage: this.storage,
      scenarioId: () => this.world.id,
      nodes: () => this.nodeList(),
      volumes: () => (hasRealBroker(this.world.scenario()) ? this.external.volumes(this.world.id) : Promise.resolve([])),
      volumeFiles: (volume) => this.external.volumeFiles(volume),
      volumeFile: (volume, path) => this.external.volumeFile(volume, path),
      expand: () => this.expandDock(),
      onForgetVolume: (volume) => this.forgetVolume(volume),
      onForgetNode: (id) => this.forgetNode(id),
      onForgetScenario: () => this.forgetScenario(),
      onPersistChange: (on) => {
        // Turning it on stores the live state first, so nothing skipped while
        // it was off leaves a gap.
        this.storage.setPersist(on, this.world.id, this.session.myHostedIds());
        this.toasts.info(on ? "Persisting durable state to this browser" : "Persistence off: new changes are not stored");
      },
    });
    // The dock shows its panels as tabs, so the panels' own disclosure
    // headings stay open and out of sight.
    for (const panel of [this.timeline.root, this.storagePanel.root]) panel.open = true;

    this.tour = new Tour(stage, {
      steps: () => this.tourSteps(),
      onChange: () => this.renderTourButton(),
    });

    this.addExpandControls();
    this.j2Init(); // J2
    this.j3Init(); // J3

    // Stacked on a narrow screen, the details start folded so the inspector
    // and the rail sit one short scroll below the canvas.
    this.stacked = window.matchMedia("(max-width: 1024px)");
    if (this.stacked.matches) this.setDockCollapsed(true);
    this.buildWorkArea(rail, stage, side);

    // The shortcuts answer while focus is in the lab or the pointer is over it:
    // a click on a button that re-renders drops focus to the page.
    this.pointerInLab = false;
    root.addEventListener("pointerenter", () => (this.pointerInLab = true));
    root.addEventListener("pointerleave", () => (this.pointerInLab = false));
    document.addEventListener("keydown", (e) => this.onKey(e));
  }

  // The inspector's "Try this" buttons: break the running cluster in a way the
  // reader can watch.
  tryIt(key) {
    const nodes = this.world.scenario().nodes.filter((n) => !kindOf(n.kind).hidden);
    // A live node when there is one, so pressing the button twice breaks a second node.
    const up = (n) => this.world.snapshot()?.nodes?.find((s) => s.id === n.id)?.alive !== false;
    const pick = (list) => list.find(up) || list[0];
    const broker = pick(nodes.filter((n) => n.kind === REAL_BROKER_KIND));
    const client = pick(nodes.filter((n) => n.kind === "producer" || n.kind === "consumer" || n.kind === "streams"));
    if (key === "consumer") {
      // Only the host edits the scenario; a spoke's node would exist in this tab alone.
      if (this.session.role === "spoke") return this.toasts.warn("Only the host tab can add nodes; ask it to add the consumer.");
      return this.addNodeDialog("consumer");
    }
    if (!broker) return this.toasts.warn("This scenario has no broker to break; load a preset from Scenarios.");
    this.world.setPaused(false);
    if (key === "kill") {
      this.select(broker.id);
      this.fault(FAULT.kill(broker.id));
      return;
    }
    if (!client) return this.toasts.warn("This scenario has no client on the other end of a link.");
    this.select(broker.id);
    this.select(client.id, true);
    this.fault(key === "partition" ? FAULT.partition(broker.id, client.id) : FAULT.latency(broker.id, client.id, 300));
    return;
  }

  setDockCollapsed(collapsed) {
    this.dock.root.classList.toggle("lab-dock-collapsed", collapsed);
    // The label says what a press does, so it carries no expanded state too.
    this.dockToggle.textContent = collapsed ? "Show" : "Hide";
    this.applySplit?.();
  }

  // ---- work area: folding side columns and the canvas/dock splitter -----------------------------------

  buildWorkArea(rail, stage, side) {
    const root = this.root;
    this.layoutPrefs = readLayoutPrefs();
    // A phone gets the cluster to watch; building and breaking it need the room.
    const phone = el("p", "lab-phone-note", "View only on a phone: watch the cluster, play or pause it, and tap a card to inspect a node. Adding nodes and breaking links need a wider screen.");
    phone.setAttribute("role", "note");
    this.toolbar.after(phone);

    // Each side column folds to a strip that names it and opens it again.
    this.foldBtns = {};
    for (const [key, col, name, label] of [
      ["rail", rail, "the build and scenarios panel", "Build · Scenarios · Connect"],
      ["side", side, "the inspector", "Inspector"],
    ]) {
      col.id = `lab-col-${key}`;
      col.prepend(button(label, "lab-fold-strip", () => this.setFolded(key, false), { title: `Show ${name}` }));
      const toggle = button("", "lab-btn-sm lab-fold-btn", () => this.setFolded(key, !root.classList.contains(`lab-${key}-folded`)), {
        ariaLabel: `Show ${name}`,
        title: `Fold or unfold ${name}`,
      });
      toggle.setAttribute("aria-controls", col.id);
      this.foldBtns[key] = toggle;
    }
    this.toolbar.prepend(this.foldBtns.rail);
    this.toolbar.querySelector(".lab-tb-tools").append(this.foldBtns.side);
    this.setFolded("rail", Boolean(this.layoutPrefs.rail), false);
    this.setFolded("side", Boolean(this.layoutPrefs.side), false);
    // A button that opens a rail tab (Load a preset, Add a node) opens the rail too.
    const showTab = this.palette.show.bind(this.palette);
    this.palette.show = (tab) => {
      if (this.root.classList.contains("lab-rail-folded")) this.setFolded("rail", false);
      return showTab(tab);
    };

    // The splitter shares the stage's height between the canvas and the dock,
    // per dock tab: a log or the Network tab wants rows, Events wants canvas.
    const s = el("div", "lab-splitter");
    this.splitter = s;
    s.tabIndex = 0;
    s.setAttribute("role", "separator");
    s.setAttribute("aria-orientation", "horizontal");
    s.setAttribute("aria-label", "Canvas and details height");
    s.setAttribute("aria-valuemin", "0");
    s.setAttribute("aria-valuemax", "100");
    s.title = "Drag or use the arrow keys to share the height between the canvas and the details. Double-click resets it.";
    stage.insertBefore(s, this.dock.root);
    s.addEventListener("pointerdown", (e) => {
      if (e.button !== 0) return;
      e.preventDefault();
      s.setPointerCapture(e.pointerId);
      s.classList.add("lab-dragging");
      const bottom = this.dock.root.getBoundingClientRect().bottom;
      const move = (ev) => this.setDockHeight(bottom - ev.clientY - s.offsetHeight / 2, false);
      const end = () => {
        s.classList.remove("lab-dragging");
        s.removeEventListener("pointermove", move);
        writeLayoutPrefs(this.layoutPrefs);
      };
      s.addEventListener("pointermove", move);
      s.addEventListener("lostpointercapture", end, { once: true });
    });
    s.addEventListener("dblclick", () => {
      delete this.layoutPrefs.split?.[this.dock.active];
      writeLayoutPrefs(this.layoutPrefs);
      this.setDockCollapsed(false);
    });
    s.addEventListener("keydown", (e) => {
      const now = this.dock.root.offsetHeight;
      const step = e.shiftKey ? 96 : 24;
      const to = { ArrowUp: now + step, ArrowDown: now - step, Home: 0, End: Infinity }[e.key];
      if (to !== undefined) {
        e.preventDefault();
        this.setDockHeight(to);
      } else if (e.key === "Enter") {
        e.preventDefault();
        this.setDockCollapsed(!this.dock.root.classList.contains("lab-dock-collapsed"));
      }
    });
    let pending = 0;
    new ResizeObserver(() => {
      if (!pending) pending = requestAnimationFrame(() => ((pending = 0), this.applySplit()));
    }).observe(stage);
    this.stacked.addEventListener("change", () => this.applySplit());
    this.applySplit();

    // Toasts rise above the tour card while it is open.
    new ResizeObserver(() => root.style.setProperty("--lab-tour-h", `${this.tour.root.offsetHeight}px`)).observe(this.tour.root);
  }

  setFolded(key, folded, save = true) {
    this.root.classList.toggle(`lab-${key}-folded`, folded);
    const toggle = this.foldBtns[key];
    toggle.textContent = (key === "rail") === folded ? "›" : "‹";
    toggle.setAttribute("aria-expanded", String(!folded));
    toggle.setAttribute("aria-label", `${folded ? "Show" : "Hide"} ${key === "rail" ? "the build and scenarios panel" : "the inspector"}`);
    if (!save) return;
    this.layoutPrefs[key] = folded;
    writeLayoutPrefs(this.layoutPrefs);
  }

  // The height the canvas and the dock share, or 0 where the splitter is off:
  // stacked, or with either of them expanded over the window.
  splitRoom() {
    if (this.stacked.matches || this.expandedPanel === this.dock.root || this.expandedPanel === this.canvas.wrap) return 0;
    return this.canvas.wrap.offsetHeight + this.dock.root.offsetHeight;
  }

  // Drags and keys set the dock's share for the tab it shows.
  setDockHeight(px, save = true) {
    const room = this.splitRoom();
    if (!room) return;
    if (this.dock.root.classList.contains("lab-dock-collapsed")) this.setDockCollapsed(false);
    this.layoutPrefs.split ??= {};
    this.layoutPrefs.split[this.dock.active] = Math.round(clamp(px / room, 0, 1) * 1000) / 1000;
    if (save) writeLayoutPrefs(this.layoutPrefs);
    this.applySplit();
  }

  applySplit() {
    if (!this.splitter) return;
    const dock = this.dock.root;
    const room = this.splitRoom();
    this.splitter.hidden = this.stacked.matches;
    if (!room || dock.classList.contains("lab-dock-collapsed")) {
      dock.style.height = "";
      this.splitter.setAttribute("aria-valuenow", "0");
      return;
    }
    const tab = this.dock.active;
    const share = this.layoutPrefs.split?.[tab] ?? SPLIT_DEFAULTS[tab] ?? 0.5;
    // The dock keeps its tab strip and a few rows; the canvas keeps a band of cards.
    const px = Math.round(clamp(share * room, (this.dock.head?.offsetHeight || 40) + 64, room - MIN_CANVAS_PX));
    dock.style.height = `${px}px`;
    this.splitter.setAttribute("aria-valuenow", String(Math.round((px / room) * 100)));
    this.splitter.setAttribute("aria-valuetext", `details ${Math.round((px / room) * 100)}% of the height`);
  }

  // Drill-downs open the details full-window: the analyzers need the room.
  expandDock() {
    if (this.expandedPanel !== this.dock.root) this.dock.root._expandControl.click();
  }

  showNetwork() {
    this.setDockCollapsed(false);
    this.dock.show("network");
    this.networkPanel.open();
  }

  // Shortcuts that work anywhere in the lab, or with the pointer over it,
  // except while typing, while a dialog is open or while a control has focus
  // (Space would press it).
  onKey(e) {
    const target = e.target instanceof Element ? e.target : document.body;
    // A held key repeats: Space would flip the clock on every repeat.
    if (e.defaultPrevented || e.repeat || e.isComposing || e.ctrlKey || e.metaKey || e.altKey) return;
    const typing = target.closest("input, textarea, select, dialog, [contenteditable]") || document.querySelector("dialog[open]");
    // The tour opens by itself with focus still at the top of the page, so
    // Escape closes it from anywhere.
    if (e.key === "Escape" && this.tour.active && !typing) {
      this.tour.close();
      return;
    }
    if (!this.root.contains(target) && !this.pointerInLab) return;
    if (typing) return;
    const onControl = target.closest("button, summary, a, [role=tab]");
    const key = e.key;
    if (key === "?") {
      e.preventDefault();
      this.helpDialog();
    } else if (key === " " && !onControl) {
      e.preventDefault();
      this.togglePlay();
    } else if ((key === "f" || key === "F") && !onControl) this.canvas.fit({ whole: true });
    else if (key === "Escape") this.select(null);
    else if ((key === "k" || key === "K") && !onControl && this.selection.length === 1) this.command(this.selection[0], "kill");
    else if ((key === "r" || key === "R") && !onControl && this.selection.length === 1) this.command(this.selection[0], "restart");
  }

  async helpDialog() {
    const body = el("div", "lab-help");
    body.appendChild(el("p", "lab-small", "The lab runs real Krabka brokers in this tab. You control time and the network. Nothing goes to a server; the only traffic that leaves the tab goes to a tab you invite and to the kafkactl bridge."));
    const rows = [
      ["Space", "play or pause the clock"],
      ["Click a card", "inspect a node: its state, config and commands"],
      ["Shift+click a second card", "link controls: partition, latency, loss, and the bytes on the wire (on touch: long-press it, Pick as second node)"],
      ["Drag a card", "move it (positions are saved with the scenario)"],
      // Stacked, a plain wheel scrolls the page; Expand gives the canvas the whole window back.
      [this.stacked?.matches ? "Drag the background, Ctrl+wheel" : "Drag the background, wheel", "pan and zoom the canvas"],
      ["Right-click or long-press a card", "context menu: edit, send a command, fault, remove"],
      ["K / R", "kill or restart the selected node"],
      ["F", "fit every node in view"],
      ["Esc", "clear the selection"],
      ["?", "this list"],
    ];
    const table = el("dl", "lab-help-list");
    for (const [keys, what] of rows) {
      table.append(el("dt", null, keys), el("dd", null, what));
    }
    body.appendChild(table);
    body.appendChild(el("p", "lab-small lab-muted", "The keys answer while focus is in the lab or the pointer is over it."));

    // What the cards and lines on the canvas mean.
    body.appendChild(el("h4", "lab-rail-heading", "Reading the canvas"));
    const legend = el("ul", "lab-legend");
    for (const kind of KIND_ORDER) {
      const k = KINDS[kind];
      const item = el("li");
      const glyph = el("span", "lab-kind-glyph", k.glyph);
      glyph.style.background = k.color;
      item.append(glyph, el("span", null, k.label));
      legend.appendChild(item);
    }
    body.appendChild(legend);
    body.appendChild(
      el(
        "p",
        "lab-small lab-muted",
        "A card that says down was killed; dashed means its links are cut. Orange arrows are records moving between a client and a topic; faint dashed lines are connections. The brokers share a cluster frame: a client's connections to them are one line to the frame, and a line between two brokers shows while frames flow on it (All connections draws every connection). A dot sliding along a line is a Kafka frame in flight.",
      ),
    );
    body.appendChild(el("p", "lab-small lab-muted", "New here? The tour walks through the controls in about a minute."));
    let tour = false;
    await openDialog(this.root, {
      title: "Shortcuts and tips",
      body,
      submitLabel: "Take the tour",
      cancelLabel: "Close",
      onSubmit: () => {
        tour = true;
        return true;
      },
    });
    if (tour) this.tour.start();
  }

  // The tour, as functions so each step reads the live scenario.
  tourSteps() {
    const firstBroker = () => this.world.scenario().nodes.find((n) => n.kind === REAL_BROKER_KIND);
    return [
      {
        title: "A live Kafka cluster",
        text: "Cards are nodes. Lines show who talks to whom, and the dots are Kafka frames in flight. Everything here runs in your browser: real brokers, simulated network.",
        target: ".lab-canvas-wrap",
      },
      {
        title: "You control time",
        text: "Pause the clock, step forward in simulated milliseconds, or Settle until nothing is due. Raise the speed to watch slow things, like a broker session timeout, happen.",
        target: ".lab-toolbar",
      },
      {
        title: "Inspect a node",
        text: "Click a card to see its state, edit its configuration and send it commands. For a broker that includes its process, logs and network activity, and a Browse disk button for its files.",
        target: ".lab-col-side",
        action: {
          label: "Select a broker for me",
          run: () => {
            const n = firstBroker();
            if (n) this.select(n.id);
          },
        },
      },
      {
        title: "Break something",
        text: "Kill a broker and watch the producer retry and the consumers rebalance once its session runs out. The Break things bar also cuts links and adds latency or loss.",
        target: ".lab-faults",
        action: {
          label: "Kill the selected broker",
          run: () => {
            const n = this.selection.length ? this.world.scenario().nodes.find((x) => x.id === this.selection[0]) : firstBroker();
            if (!n) return;
            this.select(n.id);
            this.command(n.id, "kill");
            this.world.setPaused(false);
          },
        },
      },
      {
        title: "Heal it, then build your own",
        text: "Restart the node and it catches up and rejoins. When you are ready, Build adds nodes and topics, Scenarios loads presets and saved clusters, and Connect invites another tab or drives the cluster from kafkactl.",
        target: ".lab-col-rail",
        action: {
          label: "Restart the broker",
          run: () => {
            const dead = this.world.snapshot()?.nodes.find((n) => !n.alive && !kindOf(n.kind).hidden);
            if (dead) this.command(dead.id, "restart");
          },
        },
      },
    ];
  }

  renderTourButton() {
    if (this.tourBtn) this.tourBtn.setAttribute("aria-pressed", String(this.tour?.active || false));
  }

  addExpandControls() {
    this.inerted = [];
    const panels = [this.canvas.wrap, this.inspector.root, this.dock.root];
    for (const panel of panels) {
      const name = panel.getAttribute("aria-label") || (panel === this.dock.root ? "details" : "canvas");
      const control = button("Expand", "lab-btn-sm lab-expand", (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (this.expandedPanel === panel) {
          this.collapsePanel();
          return;
        }
        this.collapsePanel();
        this.expandedPanel = panel;
        panel.classList.add("lab-expanded");
        // The old view was laid out for the old box: refit to the new one.
        if (panel === this.canvas.wrap) this.canvas.userMovedView = false;
        if (panel === this.dock.root) this.setDockCollapsed(false);
        // The panel covers the window: it is a modal for the keyboard and for
        // screen readers too, and the page behind it cannot be reached.
        panel.setAttribute("role", "dialog");
        panel.setAttribute("aria-modal", "true");
        if (panel._expandLabelled) panel.setAttribute("aria-label", name);
        this.inertAround(panel);
        this.applySplit();
        // Folding the details away would leave the window empty.
        this.dockToggle.hidden = panel === this.dock.root;
        // Short, so the dock's tab strip keeps room on a phone.
        control.textContent = "Close";
        control.setAttribute("aria-label", `Close expanded ${name}`);
        control.focus();
      }, { ariaLabel: `Expand ${name}` });
      if (panel === this.canvas.wrap) this.canvas.tools.appendChild(control);
      else if (panel === this.dock.root) this.dock.tools.insertBefore(control, this.dockToggle);
      else panel.appendChild(control);
      panel._expandControl = control;
      panel._expandName = name;
      // A label added for the dialog comes off again with it.
      panel._expandLabelled = !panel.hasAttribute("aria-label");
    }
    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape" || !this.expandedPanel || document.querySelector("dialog[open]")) return;
      event.preventDefault();
      event.stopPropagation();
      this.collapsePanel();
    }, true);
  }

  collapsePanel() {
    const panel = this.expandedPanel;
    if (!panel) return;
    panel.classList.remove("lab-expanded");
    if (panel === this.canvas.wrap) this.canvas.userMovedView = false;
    panel.removeAttribute("role");
    panel.removeAttribute("aria-modal");
    if (panel._expandLabelled) panel.removeAttribute("aria-label");
    for (const e of this.inerted) e.inert = false;
    this.inerted = [];
    this.dockToggle.hidden = false;
    const control = panel._expandControl;
    control.textContent = "Expand";
    control.setAttribute("aria-label", `Expand ${panel._expandName}`);
    this.expandedPanel = null;
    this.applySplit();
    control.focus();
  }

  // Everything around an expanded panel is covered by it: inert keeps Tab and
  // screen readers out. The toasts stay live, and a dialog opened from the
  // panel is added after this and so is not inert.
  inertAround(panel) {
    for (let node = panel; node !== document.body; node = node.parentElement) {
      for (const sibling of node.parentElement.children) {
        if (sibling === node || sibling.inert || sibling.matches(".lab-toasts")) continue;
        sibling.inert = true;
        this.inerted.push(sibling);
      }
    }
  }

  buildBridgePanel(container) {
    const executable = /Win/i.test(navigator.userAgentData?.platform || navigator.platform) ? ".\\kafkactl.exe" : "./kafkactl";
    const panel = el("details", "lab-side-section lab-bridge");
    panel.appendChild(el("summary", "lab-panel-title", "Connect with kafkactl"));
    const body = el("div", "lab-side-body");
    const downloads = el("p", "lab-small");
    downloads.append("1. Download the krabka build of kafkactl: ");
    for (const [label, asset] of [
      ["Windows", "kafkactl-lab-windows-amd64.zip"],
      ["macOS Intel", "kafkactl-lab-darwin-amd64.tar.gz"],
      ["macOS Apple silicon", "kafkactl-lab-darwin-arm64.tar.gz"],
      ["Linux x64", "kafkactl-lab-linux-amd64.tar.gz"],
      ["Linux ARM64", "kafkactl-lab-linux-arm64.tar.gz"],
    ]) {
      const link = el("a", "", label);
      link.href = `https://github.com/krabka-io/krabka-io.github.io/releases/download/kafkactl-lab-v0.1.0/${asset}`;
      link.rel = "noopener noreferrer";
      downloads.append(link, " · ");
    }
    const checksums = el("a", "", "SHA-256 checksums");
    checksums.href = "https://github.com/krabka-io/krabka-io.github.io/releases/download/kafkactl-lab-v0.1.0/checksums.txt";
    downloads.append(checksums);
    body.appendChild(downloads);
    const command = (label, value) => {
      const row = el("div", "lab-bridge-command");
      const code = el("code", "lab-code");
      code.append(...commandTokens(value));
      row.append(code, button("Copy", "lab-btn-sm", async () => this.toasts.info((await copyToClipboard(value)) ? `${label} copied` : "Copy failed; select the command"), { ariaLabel: `Copy ${label.toLowerCase()}` }));
      body.appendChild(row);
    };
    body.appendChild(el("p", "lab-small", "2. Extract it and open a terminal in the extracted folder. Start the bridge there:"));
    command("Bridge command", `${executable} lab bridge`);
    body.appendChild(el("p", "lab-small", "3. Enter the token the bridge prints. Allow this site to access the local network if your browser asks."));
    const token = el("input", "lab-input");
    token.type = "password";
    token.placeholder = "Pairing token";
    token.autocomplete = "off";
    const connect = button("Connect", "lab-btn-sm lab-primary", async () => {
      if (this.session.role === "spoke") return this.toasts.warn("Connect from the host tab that runs the real brokers.");
      if (!this.addLocalClient()) return;
      this.bridge.sync();
      const value = token.value.trim();
      // A click with no token must not drop the live bridge: it only restores the client node above.
      if (!value && this.bridge.socket) return;
      try {
        await this.bridge.connect(value);
        token.value = "";
      } catch (err) {
        this.renderBridgeStatus("error", err.message);
      }
    });
    body.append(token, connect);
    this.bridgeStatus = el("p", "lab-muted lab-small", "Bridge disconnected");
    this.bridgeStatus.setAttribute("role", "status");
    body.appendChild(this.bridgeStatus);
    this.bridgeAddNode = button("Add client node", "lab-btn-sm", () => {
      if (this.addLocalClient()) this.bridge.sync();
    }, { title: "Add the local kafkactl client node this scenario needs" });
    this.bridgeAddNode.hidden = true;
    body.appendChild(this.bridgeAddNode);
    body.appendChild(el("p", "lab-small", "4. Open a second terminal. Add a context, then inspect, write, and read the orders topic:"));
    command("Context command", `${executable} config add krabka-lab --broker 127.0.0.1:9092`);
    command("Broker command", `${executable} --context krabka-lab get brokers`);
    command("Topic command", `${executable} --context krabka-lab get topics`);
    command("Produce command", `echo hello-from-kafkactl | ${executable} --context krabka-lab produce orders`);
    command("Consume command", `${executable} --context krabka-lab consume orders --offset=oldest --output=raw`);
    body.appendChild(el("p", "lab-muted lab-small", "Press Ctrl+C to stop consuming."));
    body.appendChild(el("p", "lab-muted lab-small", "Keep this tab and the bridge running. If a port is in use, stop the other local Kafka service; if a command waits, check that the lab clock is playing and the bridge is connected."));
    panel.appendChild(body);
    container.appendChild(panel);
    this.bridgePanel = panel;
  }

  renderBridgeStatus(state, reason = "") {
    if (!this.bridgeStatus) return;
    this.bridgeStatus.textContent = `${state === "connected" ? "Connected" : state === "connecting" ? "Connecting" : state === "error" ? "Bridge error" : "Bridge disconnected"}${reason ? `: ${reason}` : ""}`;
    this.bridgeStatus.dataset.state = state;
    if (this.bridgeAddNode) this.bridgeAddNode.hidden = !(state === "error" && reason === NO_CLIENT_NODE);
  }

  // The scenario's local kafkactl client node, added in a free spot when it is
  // missing. False when the world refused it.
  addLocalClient() {
    const scenario = this.world.scenario();
    if (scenario.nodes.some((n) => n.kind === LOCAL_CLIENT_KIND)) return true;
    const ids = this.world.liveSnapshot()?.nodes.map((n) => n.id) || scenario.nodes.map((n) => n.id);
    const pos = freePosition(scenario.nodes);
    return this.world.addNode({ id: Math.max(0, ...ids) + 1, kind: LOCAL_CLIENT_KIND, name: "local kafkactl", x: pos.x, y: pos.y, config: {} }) != null;
  }

  buildToolbar() {
    const t = this.toolbar;
    const brand = el("div", "lab-tb-brand");
    this.nameEl = el("span", "lab-tb-scenario");
    brand.append(el("span", "lab-brand", "Cluster Lab"), this.nameEl);

    this.playBtn = el("button", "lab-btn lab-primary lab-play");
    this.playBtn.type = "button";
    this.playBtn.title = "Run or pause the simulated clock (Space)";
    this.playBtn.dataset.action = "play";
    this.playBtn.addEventListener("click", () => this.togglePlay());
    this.playGlyph = el("span", "lab-play-glyph");
    this.playGlyph.setAttribute("aria-hidden", "true");
    this.playText = el("span", "lab-play-text", "Pause");
    this.playBtn.append(this.playGlyph, this.playText);

    const step = (ms, label) => button(label, "lab-btn-sm", () => this.world.step(ms), { title: `Advance ${label} of simulated time`, data: { step: String(ms) } });
    const stepGroup = el("div", "lab-tb-group");
    stepGroup.setAttribute("role", "group");
    stepGroup.setAttribute("aria-label", "Step the clock");
    this.settleBtn = button("Settle", "lab-btn-sm", () => this.world.settle(), { title: "Run until nothing is due (at most 5 s of simulated time)" });
    stepGroup.append(el("span", "lab-tb-cap", "Step"), step(10, "+10 ms"), step(100, "+100 ms"), step(1000, "+1 s"), this.settleBtn);

    this.speedSel = select(
      SPEEDS.map((s) => ({ value: s, label: `${s}×` })),
      1,
      (v) => this.world.setSpeed(Number(v)),
    );
    this.speedSel.setAttribute("aria-label", "Simulation speed");
    const speedGroup = el("label", "lab-tb-group");
    speedGroup.append(el("span", "lab-tb-cap", "Speed"), this.speedSel);

    this.timeEl = el("span", "lab-time", "0 ms");
    this.timeEl.dataset.field = "sim-time";
    this.statsEl = el("span", "lab-stats lab-muted");
    const clock = el("div", "lab-tb-clock");
    clock.append(this.timeEl, this.statsEl);
    this.roleEl = el("span", "lab-role");

    this.tourBtn = button("Tour", "lab-btn-sm", () => this.tour.start(), { title: "A one-minute walk through the controls" });
    this.tourBtn.setAttribute("aria-pressed", "false");
    const shareBtn = button("Share", "lab-btn-sm", () => this.share(), { title: "Copy a link that carries this scenario" });
    const helpBtn = button("?", "lab-btn-sm", () => this.helpDialog(), { title: "Shortcuts and tips (?)", ariaLabel: "Shortcuts and tips" });
    const tools = el("div", "lab-tb-group lab-tb-tools");
    tools.append(this.roleEl, this.tourBtn, shareBtn, helpBtn);

    t.append(brand, this.playBtn, stepGroup, speedGroup, clock, tools);
  }

  buildSessionPanel(container) {
    const d = el("details", "lab-session lab-side-section");
    d.open = false;
    d.appendChild(el("summary", "lab-panel-title", "Session: share this cluster across tabs"));
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
        "Several tabs, on one machine or across the internet, can each host a share of the nodes. Frames between tabs travel over WebRTC data channels; there is no server. After a tab connects, select a node and pick its host in the inspector. Each tab keeps its own clock, speed and event counts.",
      ),
    );
    const nameRow = el("label", "lab-field-inline lab-name-row");
    const nameInput = el("input", "lab-input lab-input-sm");
    nameInput.value = s.name;
    nameInput.setAttribute("aria-label", "Your peer name");
    nameInput.maxLength = NAME_MAX;
    nameInput.addEventListener("change", () => s.setName(nameInput.value));
    nameRow.append(el("span", "lab-muted", "this tab"), nameInput, el("span", "lab-role-badge", s.role));
    body.appendChild(nameRow);

    const peers = el("ul", "lab-peer-list");
    for (const p of s.peerList()) {
      const li = el("li", `lab-peer lab-peer-${p.state}`);
      li.dataset.peer = p.id;
      const hosted = [...s.hosting].filter(([, peer]) => peer === p.id).map(([n]) => this.nodeName(n));
      li.append(el("span", "lab-peer-name", p.name), el("span", "lab-muted lab-small", `${p.state}${hosted.length ? ` · ${hosted.join(", ")}` : ""}`));
      if (s.role === "hub" && !p.self && p.state === "closed") {
        li.appendChild(button("Forget", "lab-btn-sm", () => s.forgetPeer(p.id), { title: `Remove ${p.name} and run its nodes here again` }));
      }
      peers.appendChild(li);
    }
    body.appendChild(peers);
    // Once the host accepted the answer it is spent.
    if (s.peerList().some((p) => !p.self && p.state === "connected")) this.answerCode = null;

    const actions = el("div", "lab-form-actions");
    // Kept so focus can go back to it: creating the invite rebuilds this panel.
    this.inviteBtn = null;
    if (s.role !== "spoke") {
      this.inviteBtn = button("Invite a tab…", "lab-btn-sm", () => this.inviteDialog(), { title: "Make a link another tab or machine opens to join" });
      actions.appendChild(this.inviteBtn);
    }
    if (s.role !== "solo") actions.appendChild(button("Leave session", "lab-btn-sm lab-danger", () => this.leaveSession()));
    body.appendChild(actions);
    if (this.answerCode) {
      body.appendChild(el("p", "lab-small", "Waiting for the host. Give them this answer code to paste in their Invite dialog; the scenario appears once they accept it:"));
      const ta = el("textarea", "lab-textarea lab-code");
      ta.readOnly = true;
      ta.rows = 3;
      ta.value = this.answerCode;
      ta.dataset.field = "answer-code";
      ta.setAttribute("aria-label", "Answer code");
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
    linkTa.setAttribute("aria-label", "Invite link");
    linkTa.value = "making the invite…";
    body.appendChild(linkTa);
    body.appendChild(button("Copy link", "lab-btn-sm", async () => this.toasts.info((await copyToClipboard(link)) ? "Link copied" : "Copy failed; select the text")));
    body.appendChild(el("p", "lab-small", "2. Paste the answer code that page shows:"));
    const answerTa = el("textarea", "lab-textarea lab-code");
    answerTa.rows = 3;
    answerTa.placeholder = "answer code";
    answerTa.setAttribute("aria-label", "Answer code");
    body.appendChild(answerTa);
    const status = el("p", "lab-muted lab-small");
    status.setAttribute("role", "status");
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
    this.refocus(this.inviteBtn);
  }

  // A dialog's opener can be rebuilt or gone by the time it closes: focus goes
  // to its stand-in instead of dropping to the page.
  refocus(target) {
    if (document.activeElement === document.body) target?.focus();
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
    this.experiment?.tick(snap); // J3
    this.pushPanels(snap);
  }

  pushPanels(snap = this.world.snapshot()) {
    const scenario = this.world.scenario();
    // The hub can remove a node this tab has selected; a selection of nothing
    // would leave live Kill and Isolate buttons for a node that is gone.
    const gone = (id) => !scenario.nodes.some((n) => n.id === id) && !snap?.nodes?.some((n) => n.id === id);
    if (this.selection.some(gone)) {
      this.selection = this.selection.filter((id) => !gone(id));
      this.inspector.setSelection(this.selection[0] ?? null);
    }
    const session = {
      role: this.session.role,
      hosting: this.session.hosting,
      peers: this.session.peerList(),
      me: this.session.me,
      peersKey: this.session.peersKey,
      offline: this.session.offlinePeers(),
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
    this.networkPanel.update(this.selection);
    this.timeline.setNodes(this.nodeList());
    this.palette.update({ scenario, availability: this.availability, role: this.session.role, saveState: this.saveState });
    this.nameEl.textContent = scenario?.name || "Untitled scenario";
    this.dock.setBadge("events", this.timeline.events.length ? fmtNum(this.timeline.events.length) : "");
    this.renderClock(snap);
    this.j2Update(snap); // J2
    this.experimentPanel?.update(scenario); // J3
  }

  renderClock(snap = this.world.snapshot()) {
    this.playGlyph.textContent = this.world.paused ? "▶" : "❚❚";
    this.playText.textContent = this.world.paused ? "Play" : "Pause";
    this.playBtn.classList.toggle("lab-paused", this.world.paused);
    this.timeEl.textContent = fmtMs(snap ? snap.now : this.world.now());
    if (snap) {
      const delivered = (snap.delivered || []).reduce((s, d) => s + (d[2] || 0), 0);
      // The hidden admin client is not a node the reader sees.
      const shown = snap.nodes.filter((n) => !kindOf(n.kind).hidden).length;
      this.statsEl.textContent = `${plural(shown, "node")} · ${snap.in_flight.length} in flight · ${fmtNum(delivered)} delivered · ${plural(this.timeline.events.length, "event")}`;
    }
    const r = this.session.role;
    // Other tabs only: the list also carries this one.
    this.roleEl.textContent = r === "solo" ? "" : `${r} · ${plural(this.session.peerList().filter((p) => p.state === "connected" && !p.self).length, "peer")}`;
    this.roleEl.title = r === "solo" ? "" : "In a session each tab keeps its own clock, speed and counters";
  }

  onChange(opts = {}) {
    this.bridge.sync();
    if (this.session.role === "hub") this.broadcast();
    if (this.session.role !== "spoke") this.autosave();
    if (!opts.positionOnly) this.pushPanels();
  }

  onReset() {
    this.timeline.clear();
    this.logs.clear();
    this.select(null);
    // An Undo made for the old scenario would put its node or its contents into this one.
    this.toasts.dropActions();
    // A new scenario starts from a fit, whatever zoom the last one was left at.
    this.canvas.fitted = false;
    this.canvas.userMovedView = false;
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

  // `scroll: false` for a click on the canvas: the reader is working there,
  // and the fault bar already names the node.
  select(id, additive = false, { scroll = true } = {}) {
    if (id == null) this.selection = [];
    else if (additive && this.selection.length && this.selection[0] !== id) this.selection = [this.selection[0], id];
    else this.selection = [id];
    this.inspector.setSelection(this.selection[0] ?? null);
    this.pushPanels();
    // Stacked, the inspector is below the canvas: when a pick from a list
    // leaves it out of sight altogether, bring it into view. The tour's own
    // card would scroll out of sight, so leave the page alone then.
    if (id != null && scroll && this.stacked?.matches && !this.tour?.active) {
      const box = this.inspector.root.getBoundingClientRect();
      if (box.bottom < 0 || box.top > window.innerHeight) this.inspector.root.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }

  nodeList() {
    return this.world.scenario().nodes.map((n) => ({ id: n.id, kind: n.kind, name: n.name }));
  }

  // A node's name: from the scenario, or for a node the world adds itself
  // (the hidden admin), from the snapshot.
  nodeName(id) {
    const n = this.world.scenario().nodes.find((s) => s.id === id) || this.world.snapshot()?.nodes?.find((s) => s.id === id);
    return n ? n.name || `#${id}` : `#${id}`;
  }

  // A broker id as the inspector shows it: the node that carries it.
  nodeLabelForBroker(brokerId) {
    const n = this.world.scenario().nodes.find((s) => s.kind === REAL_BROKER_KIND && Number(s.id) === Number(brokerId));
    return n ? `${n.name} (broker ${brokerId})` : `broker ${brokerId}`;
  }

  menuItems(id) {
    const snap = this.world.snapshot()?.nodes.find((n) => n.id === id);
    const k = kindOf(snap?.kind);
    const editable = this.session.role !== "spoke" && !k.hidden;
    return [
      { label: "Edit…", command: "edit", disabled: !editable },
      // A node another tab runs answers only there.
      { label: "Send command…", command: "control", disabled: Boolean(k.real) || (this.session.role !== "solo" && this.session.hostOf(id) !== this.session.me) },
      // The touch route to a link: Shift+click has no finger equivalent.
      { label: "Pick as second node", command: "pick-second", disabled: !this.selection.length || this.selection[0] === id },
      { separator: true },
      { label: snap?.alive ? "Kill" : "Restart", command: snap?.alive ? "kill" : "restart" },
      { label: "Wipe (restart from nothing)", command: "wipe" },
      cutOff(this.world.snapshot(), id) ? { label: "Reconnect", command: "reconnect" } : { label: "Isolate", command: "isolate" },
      ...this.j3MenuItems(id, snap), // J3
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
      case "pick-second":
        this.select(id, true, { scroll: false });
        break;
      case "remove":
        // The hidden admin is the lab's own: not removable (the menu disables it too).
        if (this.session.role === "spoke" || kindOf(this.world.snapshot()?.nodes.find((n) => n.id === id)?.kind).hidden) return;
        this.removeNode(id);
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
        this.j3Command(id, command); // J3
    }
  }

  // Remove acts at once; the toast offers to put the node back. What comes
  // back is its configuration, not the state it had stored.
  removeNode(id) {
    const spec = this.world.spec(id);
    // A keyboard user's focus is on the node or button that is about to go.
    const byKeyboard = this.root.contains(document.activeElement) && document.activeElement.matches(":focus-visible");
    this.world.removeNode(id);
    if (this.selection.includes(id)) this.select(null);
    if (!spec) return;
    const toast = this.toasts.show(`Removed ${spec.name}`, {
      action: {
        label: "Undo",
        run: () => {
          if (this.session.role !== "spoke" && this.world.addNode(spec) != null) {
            this.select(spec.id);
            if (byKeyboard) {
              // The card is drawn from the next snapshot: take it now.
              this.world.flush(performance.now(), true);
              this.canvas.focusNode(spec.id);
            }
          }
        },
      },
    });
    if (byKeyboard) toast.querySelector(".lab-toast-action").focus();
  }

  // A control command for a node, from the inspector's command bar or the
  // Send command dialog. The answer comes back at once; what the command
  // changed shows in the next snapshot, taken right away.
  control(id, command) {
    const r = this.world.control(id, command);
    this.world.flush(performance.now(), true);
    return r;
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

  fault(f, { quiet = false } = {}) {
    // Reconnect restores every link of the node: its cut links heal too.
    if (f.kind === "reconnect") for (const l of cutLinks(this.world.snapshot(), f.node)) this.fault(FAULT.heal(l.a, l.b), { quiet: true });
    if (this.world.fault(f)) {
      this.session.broadcastFault(f);
      if (!quiet) this.toasts.info(describeFault(f, (id) => this.nodeName(id)));
      return true; // J3: the experiment runner records refusals
    }
    return false;
  }

  // ---- J2: cluster state, charts, invariants, record trace ---------------------------------------------------

  j2Init() {
    this.sampler = new Sampler();
    this.checker = new InvariantChecker();
    this.j2Markers = [];
    this.j2Pending = [];
    this.clusterPanel = new ClusterPanel(this.dock.panel("cluster"), { nodeLabelForBroker: (id) => this.nodeLabelForBroker(id) });
    this.chartsPanel = new ChartsPanel(this.dock.panel("charts"), {
      sampler: this.sampler,
      hooks: { onRerun: () => this.rerun(), onSelectNode: (id) => this.select(id), scenarioName: () => this.world.scenario()?.name },
    });
    this.tracePanel = new TracePanel(this.dock.panel("trace"), {
      capture: this.capture,
      nodeName: (id) => this.nodeName(id),
      context: () => this.traceContext(),
      showExchange: (ex) => this.showExchange(ex),
    });
    this.inspector.hooks.onTrace = (id, rec) => this.traceRecord(id, rec);
    this.inspector.hooks.clusterHealth = () => clusterHealth(clusterOf(this.world.snapshot()));
    this.canvas.hooks.decorate = (canvas) => decorateCanvas(canvas, clusterOf(canvas.snapshot));
    this.invPill = button("Invariants hold", "lab-btn-sm lab-inv-pill", () => {
      this.setDockCollapsed(false);
      this.dock.show("charts");
    }, { title: "The live invariant checks: no acked record lost, offsets forward, one leader per epoch. Opens the Charts tab." });
    this.invPill.dataset.field = "invariants";
    this.toolbar.querySelector(".lab-tb-tools")?.prepend(this.invPill);
  }

  // A new run of a scenario: its series and its checks start from nothing.
  j2Reset() {
    if (!this.sampler) return;
    this.sampler.reset();
    this.checker.reset();
    this.j2Markers = [];
    this.j2Pending = [];
    this.chartsPanel.setMarkers([]);
    this.chartsPanel.setViolations([]);
    this.chartsPanel.render();
    this.renderInvPill();
  }

  j2Events(events) {
    if (!this.sampler) return;
    this.j2Pending.push(...events);
    this.j2Markers.push(...faultMarkers(events, (id) => this.nodeName(id)));
  }

  j2Update(snap) {
    if (!snap || !this.sampler) return;
    const fresh = this.checker.observe(snap, this.j2Pending.splice(0), this.world.scenario());
    if (fresh.length) {
      this.chartsPanel.setViolations(this.checker.violations);
      this.renderInvPill();
      for (const v of fresh) this.toasts.warn(`Invariant violated at ${fmtMs(v.at)}: ${v.text}`);
    }
    const row = this.sampler.sample(snap, this.capture);
    if (row || fresh.length) {
      this.chartsPanel.setMarkers(this.j2Markers);
      if (this.chartsPanel.shown()) this.chartsPanel.render();
    }
    this.clusterPanel.update(snap);
  }

  j2Shown(id) {
    if (id === "cluster") this.clusterPanel.render(true);
    if (id === "charts") this.chartsPanel.render();
  }

  renderInvPill() {
    const n = this.checker.violations.length;
    this.invPill.textContent = n ? `${plural(n, "invariant violation")}` : "Invariants hold";
    this.invPill.classList.toggle("lab-inv-bad", n > 0);
  }

  // The Charts tab's Rerun: the scenario as it is configured now, from
  // nothing (a new identity, so every broker gets a new empty disk).
  rerun() {
    this.openFresh(this.world.scenario()).then((ok) => ok && this.toasts.info("Rerunning from nothing with the current configuration"));
  }

  traceContext() {
    const c = clusterOf(this.world.snapshot());
    const brokers = new Set(this.world.scenario().nodes.filter((n) => n.kind === REAL_BROKER_KIND).map((n) => n.id));
    const topicIds = new Map((c?.topics || []).filter((t) => t.id).map((t) => [t.id, t.name]));
    const replicas = (topic, partition) => c?.topics?.find((t) => t.name === topic)?.partitions?.find((p) => p.partition === partition)?.replicas || null;
    return { brokers, topicIds, replicas };
  }

  // A producer's Last records row, from its Trace button.
  traceRecord(id, rec) {
    const topic = this.world.snapshot()?.nodes?.find((n) => n.id === id)?.state?.topic;
    this.setDockCollapsed(false);
    this.dock.show("trace");
    this.tracePanel.trace({ ...rec, producer: id, topic });
  }

  showExchange(ex) {
    this.setDockCollapsed(false);
    this.dock.show("network");
    const net = this.networkPanel;
    net.setView("exchanges");
    net.select(ex);
    const i = net.rows?.indexOf(ex) ?? -1;
    // 24: the Network list's row height.
    if (i >= 0) net.list.scrollTop = Math.max(0, i * 24 - net.list.clientHeight / 2);
  }

  // ---- /J2 -----------------------------------------------------------------------------------------------------

  // ---- J3: experiments, fork here, focus mode, process faults ----------------------------------------------------

  j3Init() {
    this.experiment = new ExperimentRunner({
      fault: (f) => this.fault(f, { quiet: true }),
      control: (id, cmd) => this.control(id, cmd),
      pause: () => {
        if (!this.world.paused) this.togglePlay();
      },
      scenario: () => this.world.scenario(),
      capture: this.capture,
      nodeName: (id) => this.nodeName(id),
      describeFault: (f) => describeFault(f, (id) => this.nodeName(id)),
      onChange: () => this.experimentPanel?.renderResults(),
      onEnd: (r) => {
        const j = r.toJSON();
        this.toasts.show(`Experiment ${j.state}: ${j.passed} of ${j.checks.length} checks passed. The clock is paused.`, { level: j.state === "passed" ? "info" : "warn", ttl: 10_000 });
      },
    });
    this.experimentPanel = new ExperimentPanel(this.palette.tabs.panel("scenarios"), {
      scenario: () => this.world.scenario(),
      apply: (exp) => {
        if (this.session.role === "spoke") return false;
        this.world.setExperiment(exp);
        return true;
      },
      run: () => this.runExperiment(),
      stop: () => this.experiment.stop(),
      fork: () => this.forkHere(),
      runner: this.experiment,
      nodeName: (id) => this.nodeName(id),
      role: () => this.session.role,
    });
  }

  // `?focus=1` (an embed's "Open in the lab") folds both side columns; `?run=1`
  // runs the scenario's experiment; a run that reloaded for isolation goes on.
  j3Boot() {
    const params = new URLSearchParams(window.location.search);
    if (!this.reloading) {
      const run = sessionFlag(RUN_EXPERIMENT_KEY, false) || params.get("run") === "1";
      if (params.has("run")) {
        params.delete("run");
        const query = params.toString();
        history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`);
      }
      if (run && this.world.scenario().experiment) this.j3Start();
    }
    // After the run started: showing the Scenarios tab unfolds the rail.
    if (params.get("focus") === "1") {
      this.setFolded("rail", true, false);
      this.setFolded("side", true, false);
    }
  }

  j3Start() {
    if (this.world.paused) this.togglePlay();
    this.palette.show("scenarios");
    const exp = this.world.scenario().experiment;
    this.experiment.start(exp, this.world.now());
    this.toasts.info(`Running the experiment "${exp.name || "Experiment"}": steps and checks are in the Scenarios tab`);
  }

  // Restart the scenario fresh (a new identity: new broker disks, fresh
  // clients) and run its experiment from lab time 0.
  async runExperiment() {
    const doc = this.world.scenario();
    const errors = doc.experiment ? validateExperiment(doc.experiment, doc) : ["this scenario has no experiment"];
    if (errors.length) {
      this.toasts.warn(`Cannot run the experiment: ${errors[0]}`);
      return;
    }
    sessionFlag(RUN_EXPERIMENT_KEY, true);
    const ok = await this.openFresh(doc);
    if (!ok || this.reloading) return; // the reloaded page starts it
    sessionFlag(RUN_EXPERIMENT_KEY, false);
    this.j3Start();
  }

  // Snapshot and fork: the brokers' disks as they are now, under a new
  // scenario that boots from them. Clients and the admin start fresh.
  async forkHere() {
    if (this.session.role === "spoke") return;
    const doc = this.world.scenario();
    if (!doc.id) {
      this.toasts.warn("The scenario has no identity yet: save it first");
      return;
    }
    if (!this.world.paused) this.togglePlay();
    this.experiment.stop("forked");
    const at = this.world.now();
    const id = newScenarioId();
    try {
      const wasi = await import("../wasi/host.js");
      const brokers = doc.nodes.filter((n) => n.kind === REAL_BROKER_KIND);
      // Whatever the processes wrote reaches IndexedDB first.
      await Promise.all(brokers.map((n) => this.external.process(n.id)?.flush().catch(() => {})));
      const stored = new Set((await wasi.listVolumes()).map((v) => v.id));
      let copied = 0;
      for (const n of brokers) {
        const from = volumeName(doc.id, n.id);
        if (!stored.has(from)) continue;
        await wasi.importVolume(volumeName(id, n.id), await wasi.exportVolume(from));
        copied += 1;
      }
      // The copied disks carry the original cluster id (`clusterIdFor`), which the fork keeps.
      const fork = { ...doc, id, forked_from: doc.forked_from || doc.id, name: `${doc.name || "Scenario"} (fork at ${fmtMs(at)})` };
      await this.storage.saveScenario(fork);
      if (await this.openScenario(fork, { keepId: true })) {
        if (this.world.paused) this.togglePlay();
        this.toasts.info(`Forked at ${fmtMs(at)}: ${plural(copied, "broker disk")} copied. The brokers boot from them; clients start fresh.`);
      }
    } catch (err) {
      this.toasts.error(err, "fork");
    }
  }

  // The canvas menu's process faults, and a one-way cut toward the selected node.
  j3MenuItems(id, snap) {
    const items = [{ separator: true }];
    items.push(snap?.paused ? { label: "Resume", command: "pause-toggle" } : { label: "Pause (like SIGSTOP)", command: "pause-toggle", disabled: !snap?.alive });
    items.push({ label: "Clock skew and disk…", command: "process-faults", disabled: snap?.kind !== REAL_BROKER_KIND });
    const other = this.selection[0];
    if (other != null && other !== id) {
      const world = this.world.snapshot();
      for (const [from, to] of [[other, id], [id, other]]) {
        items.push({ label: `${oneWayCut(world, from, to) ? "Heal" : "Cut"} ${this.nodeName(from)} → ${this.nodeName(to)}`, command: `one-way:${from}:${to}` });
      }
    }
    return items;
  }

  j3Command(id, command) {
    if (command === "pause-toggle") {
      const paused = this.world.snapshot()?.nodes.find((n) => n.id === id)?.paused;
      this.fault(paused ? FAULT.resume(id) : FAULT.pause(id));
    } else if (command === "process-faults") {
      this.select(id);
      this.faultBar.openMore();
    } else {
      const m = /^one-way:(\d+):(\d+)$/.exec(command);
      if (!m) return;
      const [from, to] = [Number(m[1]), Number(m[2])];
      this.fault(oneWayCut(this.world.snapshot(), from, to) ? FAULT.heal_one_way(from, to) : FAULT.cut_one_way(from, to));
    }
  }
  // ---- /J3 -----------------------------------------------------------------------------------------------------

  // ---- real brokers ------------------------------------------------------------------------------------------

  // A timeline entry about a real broker's process. The world does not run
  // the process, so the page records these itself.
  processEvent(id, kind, detail) {
    this.pageEvents += 1;
    this.timeline.append([{ index: `page-${this.pageEvents}`, at: this.world.now(), node: id, kind, detail }]);
  }

  // A real broker's process ended by itself: the node goes down in the world.
  processExited(id, exit) {
    this.toasts.warn(`${this.nodeName(id)}: the process ${exit.message}; the node is down`);
    this.fault(FAULT.kill(id), { quiet: true });
  }

  // A line a real broker wrote, or a row about its process, for the Logs tab.
  recordLog(id, entry) {
    const now = this.world.now();
    if (entry.marker) this.logs.mark(id, entry.marker, entry.message, { level: entry.level, now, detail: entry.detail });
    else this.logs.add(id, entry.stream, entry.text, now, entry.base);
  }

  // The real brokers, as the Logs tab's node chips.
  logNodes() {
    return this.world
      .scenario()
      .nodes.filter((n) => n.kind === REAL_BROKER_KIND)
      .map((n) => ({ id: n.id, name: n.name || `#${n.id}`, color: kindOf(n.kind).color }));
  }

  // Each real broker with the level its process started with (null: none has started).
  logBrokers() {
    const up = this.world.snapshot()?.nodes || [];
    return this.logNodes().map((n) => {
      const env = this.external.state(n.id)?.env;
      return { ...n, alive: up.find((s) => s.id === n.id)?.alive !== false, level: env ? (env.KRABKA_LOG ?? "") : null };
    });
  }

  renderLogBadge({ count, errors, warns }) {
    this.dock.setBadge("logs", count ? fmtNum(count) : "");
    const { badge } = this.dock.tabs.get("logs");
    badge.dataset.severity = errors ? "error" : "warn";
    badge.title = `${plural(errors, "error")} and ${plural(warns, "warning")} logged`;
  }

  // Saves the level, then restarts the brokers that run at another one: a kill
  // and a boot on the same disk, the path of the Kill and Restart buttons.
  applyLogLevels({ scope, directive, restart }) {
    // The brokers run on the hub, so a spoke's setting would change nothing.
    if (this.session.role === "spoke") return this.toasts.warn("Only the host tab runs the brokers; change their log level there.");
    this.logLevels.set(this.world.id ?? "", scope, directive);
    for (const id of restart) {
      this.fault(FAULT.kill(id), { quiet: true });
      this.fault(FAULT.restart(id), { quiet: true });
    }
    const level = directive ? `log level ${directive}` : "the default log level";
    this.toasts.info(restart.length ? `Restarted ${plural(restart.length, "broker")} on ${restart.length === 1 ? "its" : "their"} disk with ${level}` : `Saved ${level}; a broker takes it when it starts`);
  }

  // Whether this tab runs the scenario's real brokers: alone or as the hub
  // (they never move to a spoke).
  hostsRealBroker(doc) {
    return this.session.role !== "spoke" && hasRealBroker(doc);
  }

  // A real broker runs in a Worker that needs a cross-origin isolated page.
  // When this tab would run one and the page is not isolated, the scenario is
  // saved and the page reloads once, isolated, and reopens it. Nothing
  // happens when the broker build is not on the site: the nodes say so.
  // Resolves true when the page is about to reload.
  async ensureIsolation(doc = this.world.scenario()) {
    if (!this.hostsRealBroker(doc)) return false;
    let ensureCrossOriginIsolation;
    try {
      ({ ensureCrossOriginIsolation } = await import(COI_URL));
    } catch (err) {
      // The scenario still opens; its real brokers say why they cannot run.
      const reason = `the isolation helper did not load (${err.message})`;
      this.external.setIsolation({ isolated: false, reason });
      this.toasts.warn(`Real brokers cannot run in this browser: ${reason}`);
      return false;
    }
    if (globalThis.crossOriginIsolated) {
      this.external.setIsolation(await ensureCrossOriginIsolation());
      // Kept, so the tour's notice can say both in one toast.
      if (sessionFlag(RELOADED_KEY, false)) this.reloadedToast = this.toasts.show("Reloaded once so the real broker can run in this tab.", { ttl: 3000 });
      return false;
    }
    if (!(await this.external.moduleAvailable())) return false;
    const notice = this.toasts.warn("A real Krabka broker needs one page reload before it can run in the browser. The scenario is kept.");
    await this.keepScenarioForReload();
    sessionFlag(RELOADED_KEY, true);
    const coi = await ensureCrossOriginIsolation();
    if (!coi.isolated && !coi.reloading) {
      // No reload is coming: retract the promise and the flag that explains one.
      notice.remove();
      sessionFlag(RELOADED_KEY, false);
      this.external.setIsolation(coi);
      this.toasts.warn(`Real brokers cannot run in this browser: ${coi.reason}`);
    }
    this.reloading = coi.reloading;
    return coi.reloading;
  }

  // Saves the scenario, with an identity, as the last one, so the reload
  // reopens it with its stored state.
  async keepScenarioForReload() {
    this.autosave.cancel();
    if (!this.world.id) {
      const id = newScenarioId();
      this.world.setId(id);
      this.storage.syncFromMirror(id, this.session.myHostedIds());
    }
    this.world.drainDurable();
    const doc = this.world.scenario();
    saveLocal(doc);
    try {
      await this.storage.saveScenario(doc);
      await this.storage.flush();
    } catch (err) {
      this.toasts.error(err, "save before the reload");
    }
    // A shared link would open as a new scenario after the reload.
    if (/[#&]s=/.test(window.location.hash)) history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
  }

  async forgetVolume(volume) {
    try {
      await this.external.forgetVolume(volume);
      this.storagePanel.refresh();
      this.toasts.info(`Forgot the volume ${volume}`);
    } catch (err) {
      this.toasts.error(err, "forget volume");
    }
  }

  // The command palette of a node: its kind's commands as templates, and a
  // JSON box for anything else.
  async controlDialog(id) {
    const k = kindOf(this.world.spec(id)?.kind);
    const commands = k.commands || [];
    const ta = el("textarea", "lab-textarea lab-code");
    ta.rows = 4;
    ta.value = JSON.stringify(commands.length ? commandExample(commands[0]) : { cmd: "" }, null, 2);
    const out = el("pre", "lab-raw");
    out.dataset.field = "command-answer";
    // The answer appears after Send, with focus still on the button.
    out.setAttribute("aria-live", "polite");
    const body = el("div");
    if (commands.length) {
      const list = el("div", "lab-cmd-palette");
      list.appendChild(el("span", "lab-field-label", "Commands"));
      const row = el("div", "lab-cmd-palette-row");
      for (const c of commands) {
        row.appendChild(button(c.label, "lab-btn-sm", () => (ta.value = JSON.stringify(commandExample(c), null, 2)), { title: c.title, data: { template: c.cmd } }));
      }
      list.appendChild(row);
      body.appendChild(list);
    } else {
      const note = el("p", "lab-muted lab-small", k.noCommands || "This kind documents no control commands.");
      note.dataset.field = "no-commands";
      body.appendChild(note);
    }
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
        const r = this.control(id, cmd);
        out.textContent = r.ok ? JSON.stringify(r.answer, null, 2) : `error: ${r.error}`;
        return false;
      },
    });
    // Its opener, the context menu, is gone: the card it was for is the next stop.
    this.refocus(this.canvas.nodeEls.get(id)?.g);
  }

  // ---- dialogs -----------------------------------------------------------------------------------------------------

  async addNodeDialog(kind) {
    const k = KINDS[kind];
    if (!k) return;
    if (this.availability[kind] === false) this.toasts.warn(`${k.label}: not in the loaded module yet; the crate will reject it`);
    // A real broker may need the one isolation reload first; say so up front.
    const reloads = kind === REAL_BROKER_KIND && !globalThis.crossOriginIsolated && this.session.role !== "spoke" && (await this.external.moduleAvailable());
    const scenario = this.world.scenario();
    // The id the node gets, given explicitly: a broker's id is its node id,
    // and the world's own nodes (the hidden admin) take ids too.
    const taken = [...scenario.nodes, ...(this.world.snapshot()?.nodes || [])];
    const nextId = taken.reduce((m, n) => Math.max(m, n.id), 0) + 1;
    const nameInput = el("input", "lab-input");
    nameInput.type = "text";
    nameInput.value = defaultName(kind, nextId);
    const defaults = suggestedConfig(kind, { nextId, nodes: this.nodeList() });
    const form = buildForm(k.fields, defaults, { nodes: this.nodeList() });
    const body = el("div");
    body.append(labelled("Name", nameInput), form.root);
    if (!k.fields.length) body.appendChild(el("p", "lab-muted lab-small", "This kind has no configuration."));
    if (reloads) {
      const note = el(
        "p",
        "lab-small lab-reload-note",
        "A real broker runs in a Web Worker on the browser WASI runtime, which needs a cross-origin isolated page. Adding it reloads this page once to turn isolation on; the scenario and every node's stored state are kept.",
      );
      note.dataset.field = "isolation-reload";
      body.appendChild(note);
    }
    const err = el("p", "lab-field-error");
    body.appendChild(err);
    await openDialog(this.root, {
      title: `Add ${k.label.toLowerCase()}`,
      body,
      submitLabel: reloads ? "Add and reload" : "Add",
      onSubmit: () => {
        const r = form.read();
        if (r.errors.length) return false;
        const existing = scenario.nodes.map((n) => ({ x: n.x, y: n.y }));
        const pos = freePosition(existing);
        const spec = { id: nextId, kind, name: nameInput.value.trim() || defaultName(kind, nextId), x: pos.x, y: pos.y, config: r.value };
        const id = this.world.addNode(spec);
        if (id == null) {
          err.textContent = "The module rejected this node; see the message above.";
          return false;
        }
        this.select(id);
        if (kind === REAL_BROKER_KIND) this.ensureIsolation();
        return true;
      },
    });
  }

  async topicDialog(name) {
    const existing = name ? this.world.topics.find((t) => t.name === name) : null;
    // What Kafka accepts in a topic name; a name it rejects would fail later, silently.
    const nameProblem = (v) => {
      if (!/^[A-Za-z0-9._-]+$/.test(v)) return "use letters, digits, . _ and - only";
      if (v.length > 249) return "at most 249 characters";
      if (v === "." || v === "..") return "cannot be . or ..";
      return this.world.topics.some((t) => t.name === v && t.name !== name) ? "a topic with that name exists" : null;
    };
    const fields = [
      { key: "name", label: "Name", type: "text", required: true, placeholder: "orders", validate: nameProblem },
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
      await this.ensureIsolation(this.world.scenario());
    }
    return ok;
  }

  loadPreset(id) {
    const p = presetById(id);
    if (!p) return;
    const missing = [...new Set(p.scenario.nodes.map((n) => n.kind))].filter((k) => this.availability[k] === false);
    if (missing.length) this.toasts.warn(`This preset needs the full build (${missing.join(", ")} not in the loaded module); the crate will reject it`);
    this.openScenario(p.scenario).then((ok) => ok && this.toasts.info(`Opened ${p.name}`));
  }

  // Clears at once; the toast offers to bring the scenario back.
  async newScenario() {
    const prev = this.world.scenario();
    const cleared = await this.openScenario({ version: 1, seed: 1, name: "", links: { default_latency_ms: 5 }, nodes: [], topics: [] });
    if (cleared && (prev.nodes.length || prev.topics.length)) {
      this.toasts.show(`Cleared ${prev.name || "the scenario"}`, { ttl: 8000, action: { label: "Undo", run: () => this.openScenario(prev, { keepId: true }) } });
    }
  }

  async openSaved(id) {
    try {
      const doc = await this.storage.loadScenario(id);
      if (!doc) {
        this.toasts.warn("That scenario is gone");
        return;
      }
      if (await this.openScenario(doc, { keepId: true })) this.toasts.info(`Opened ${doc.name || "saved scenario"}`);
    } catch (err) {
      this.toasts.error(err, "open saved scenario");
    }
  }

  // A fresh run opens the scenario under a new identity, the only way to
  // give every broker an empty disk. The copy an earlier fresh run made is
  // dropped, disks and all, so reruns do not pile up in Saved; a scenario the
  // reader saved or opened themselves is never dropped.
  async openFresh(doc) {
    const previous = this.world.id;
    const ok = await this.openScenario(doc);
    if (!ok || this.reloading) return ok;
    const runs = new Set(readJson(FRESH_RUNS_KEY, []));
    if (previous && previous !== this.world.id && runs.delete(previous)) {
      try {
        await this.storage.deleteScenario(previous);
        await this.external.forgetScenarioVolumes(previous);
        this.palette.refreshSaved();
      } catch {
        // A copy left behind costs a row in Saved, nothing more.
      }
    }
    if (this.world.id) runs.add(this.world.id);
    writeJson(FRESH_RUNS_KEY, [...runs].slice(-20));
    return ok;
  }

  async deleteSaved(id) {
    if (!(await this.confirm("Delete saved scenario", "Delete this scenario and everything stored for it in this browser? This cannot be undone.", "Delete"))) return;
    try {
      await this.storage.deleteScenario(id);
      await this.external.forgetScenarioVolumes(id);
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
      if (await this.openScenario(doc)) this.toasts.info(`Imported ${doc.name || file.name}`);
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
      const local = saveLocal(doc);
      // A copy that did not fit must not leave the last smaller one to reopen on reload.
      if (!local) clearLocal();
      const failed = !saved && !local;
      // Autosave says so once, not on every tick.
      const firstFailure = failed && !this.saveFailed;
      this.saveFailed = failed;
      const time = new Date().toLocaleTimeString();
      this.saveState = saved
        ? local ? `saved ${time}` : `saved ${time}, but too large to reopen on reload: open it from Saved in this browser`
        : local ? "saved to this page only" : "not saved: this browser's storage is full or blocked";
      if (announce || firstFailure) this.toasts[failed ? "warn" : "info"](this.saveState);
      this.palette.update({ scenario: doc, availability: this.availability, role: this.session.role, saveState: this.saveState });
      if (this.palette.savedVisible()) this.palette.refreshSaved();
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

  // A question for an action that cannot be undone: resolves true on the yes.
  confirm(title, text, submitLabel) {
    return openDialog(this.root, { title, body: el("p", null, text), submitLabel, onSubmit: () => true });
  }

  async forgetScenario() {
    if (!(await this.confirm("Forget stored data", "Drop every stored log and key of this scenario? This cannot be undone.", "Forget"))) return;
    try {
      // In a session, only the nodes this tab hosts: another tab in this
      // browser may be storing the rest under the same scenario.
      const hosted = this.session.myHostedIds();
      const ids = hosted ?? this.nodeList().map((n) => n.id);
      await this.storage.forgetScenario(this.world.id, ids, hosted == null);
      const running = await this.external.forgetScenarioVolumes(this.world.id);
      this.storagePanel.refresh();
      this.toasts.info(running ? `Forgot the stored data of this scenario, except the volumes of ${running} running real broker${running === 1 ? "" : "s"}` : "Forgot the stored data of this scenario");
    } catch (err) {
      this.toasts.error(err, "forget");
    }
  }

  // ---- boot ------------------------------------------------------------------------------------------------------------

  // A link that would not open: say so, and keep the message for the page that
  // follows the isolation reload, which would wipe this one.
  startFailed(err, context) {
    this.toasts.error(err, context);
    try {
      sessionStorage.setItem(SHARE_ERROR_KEY, `${context}: ${err instanceof Error ? err.message : err}`);
    } catch {
      // The message is not carried over a reload.
    }
  }

  async start() {
    // A link error shown before the isolation reload would vanish with the
    // page: it waits here, and every page shows it until one finishes loading.
    try {
      const carried = sessionStorage.getItem(SHARE_ERROR_KEY);
      if (carried) this.toasts.warn(carried);
    } catch {
      // No sessionStorage: nothing was carried.
    }
    let opened = false;
    const join = joinCodeFromUrl(window.location.search);
    if (join) {
      this.world.create(1);
      try {
        this.answerCode = await this.session.join(join);
        this.world.setHosted([]);
        this.palette.show("connect");
        this.sessionDetails.open = true;
        this.renderSession();
        this.sessionBody.querySelector("[data-field=answer-code]")?.scrollIntoView({ block: "nearest" });
        this.toasts.info("Joined as a spoke; hand the answer code to the host");
        opened = true;
      } catch (err) {
        // A broken invite leaves an ordinary lab behind, not an empty one.
        this.session.leave();
        this.startFailed(err, "join");
      }
      // A join link works once; a reload should not try again.
      try {
        history.replaceState(null, "", window.location.pathname);
      } catch {
        // Nothing to clean up.
      }
    } else {
      try {
        const shared = await scenarioFromHash(window.location.hash);
        if (shared) {
          opened = await this.openScenario(shared);
          if (opened) {
            this.toasts.info(`Opened shared scenario ${shared.name || ""}`.trim());
            // The copy is this reader's now: a reload reopens the saved, edited one, not the link.
            history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
          }
        }
      } catch (err) {
        this.startFailed(err, "shared link");
      }
    }
    if (!opened) {
      const last = loadLocal();
      if (last && last.id) opened = await this.openScenario(last, { keepId: true });
    }
    if (!opened) opened = await this.openScenario(presetById(DEFAULT_PRESET).scenario);
    if (!opened) this.world.create(1);
    // Not while a reload is coming: the next page still has to show it.
    if (!this.reloading) {
      try {
        sessionStorage.removeItem(SHARE_ERROR_KEY);
      } catch {
        // Nothing was stored.
      }
    }
    this.world.start();
    this.pushPanels();
    this.j3Boot(); // J3
    // It opens without taking focus, so say so for those who cannot see it.
    if (this.tour.maybeStart()) {
      const reloaded = Boolean(this.reloadedToast?.isConnected);
      this.reloadedToast?.remove();
      this.toasts.info(`${reloaded ? "Reloaded once so the real broker can run in this tab. " : ""}A short tour of the lab is open. Press Escape to close it.`);
    }
  }
}

// Sets (`value` true) or takes and clears (`value` false) a flag in
// sessionStorage; returns whether it was set. Without sessionStorage there is
// no flag.
function readLayoutPrefs() {
  try {
    const prefs = JSON.parse(localStorage.getItem(LAYOUT_KEY) || "{}");
    return prefs && typeof prefs === "object" ? prefs : {};
  } catch {
    return {};
  }
}

function writeLayoutPrefs(prefs) {
  try {
    localStorage.setItem(LAYOUT_KEY, JSON.stringify(prefs));
  } catch {
    // Not remembered: the next visit starts from the defaults.
  }
}

function readJson(key, fallback) {
  try {
    return JSON.parse(localStorage.getItem(key)) ?? fallback;
  } catch {
    return fallback;
  }
}

function writeJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // No storage: reruns keep their copies.
  }
}

function sessionFlag(key, value) {
  try {
    const was = sessionStorage.getItem(key) === "1";
    if (value) sessionStorage.setItem(key, "1");
    else sessionStorage.removeItem(key);
    return was;
  } catch {
    return false;
  }
}

// A shell command line as text and colour spans: the program, its flags, the
// values that follow them and the pipe. The text itself is unchanged, so what
// the Copy button copies is the plain command.
function commandTokens(value) {
  const tok = (cls, text) => el("span", `lab-tok-${cls}`, text);
  const out = [];
  let program = true; // the next word is a program
  let operand = false; // the next word is the value of a flag or of echo
  for (const part of value.split(/(\s+)/)) {
    const flag = /^(-[^=]+)(=)(.*)$/.exec(part);
    if (!part.trim()) out.push(part);
    else if (part === "|") {
      out.push(tok("op", part));
      program = true;
    } else if (program) {
      out.push(tok("cmd", part));
      program = false;
      operand = part === "echo";
    } else if (flag) out.push(tok("flag", flag[1]), tok("op", flag[2]), tok("str", flag[3]));
    else if (part.startsWith("-")) {
      out.push(tok("flag", part));
      operand = true;
    } else if (part.startsWith("$")) out.push(tok("var", part));
    else if (operand) {
      out.push(tok("str", part));
      operand = false;
    } else out.push(part);
  }
  return out;
}

// A command of `kinds.js` as the JSON the Send command dialog starts from.
function commandExample(spec) {
  if (spec.example) return spec.example;
  const values = {};
  for (const p of spec.params || []) values[p.key] = p.default ?? (p.type === "number" ? 0 : "");
  return commandObject(spec, values);
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
    // The build's content hash rides on the URL: the isolation service worker
    // keeps each version once (`/docs/lab/coi-sw.js`), and a new build misses.
    const wasm = new URL("../krabka_playground_bg.wasm", import.meta.url);
    if (root.dataset.labWasm) wasm.searchParams.set("v", root.dataset.labWasm);
    await init({ module_or_path: wasm });
    const app = new LabApp(root);
    window.krabkaLab = app; // for the end-to-end check and the curious
    await app.start();
    root.dataset.ready = "true";
  } catch (err) {
    root.innerHTML = "";
    const p = el("p", "lab-error", `The Cluster Lab failed to load. Check your connection and reload. Details: ${err instanceof Error ? err.message : String(err)}`);
    root.append(p, button("Reload the lab", "", () => window.location.reload()));
    // eslint-disable-next-line no-console
    console.error("krabka lab failed to initialise", err);
  }
}

boot();
