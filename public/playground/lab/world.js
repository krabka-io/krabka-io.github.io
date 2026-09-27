// The world wrapper: owns the `Lab` instance and the clock.
//
// The simulation only moves when the page tells it to. Every animation frame
// the wrapper turns the wall-clock delta into simulated milliseconds (delta ×
// speed), calls `stepUntil`, hands the frames for nodes hosted elsewhere to
// the session layer once this world's clock reaches their `deliver_at` (see
// `EgressScheduler`), hands the durable-state ops to storage, and, at most
// every 50 ms of wall time, reads a snapshot and the new events back.
//
// Every `Lab` call goes through `guard`, so an error inside the module (a
// rejected config, a bad fault) becomes a toast, never a broken page.
//
// Seeds and times cross the boundary as plain numbers (milliseconds).

export const SPEEDS = [0.1, 0.5, 1, 2, 5, 20];
const SNAPSHOT_INTERVAL_MS = 50;
// A background tab stops its animation frames; when it comes back, do not
// fast-forward the simulation by minutes at once.
const MAX_WALL_DELTA_MS = 250;
const SETTLE_WINDOW_MS = 5000;
const SETTLE_STEP_MS = 10;
// Node ids start at 1, so a hosted list of `[0]` runs nothing. The crate reads
// an empty list as "run everything".
const NONE_HOSTED = [0];

const u64 = (n) => Math.max(0, Math.floor(Number(n) || 0));

// ---- the egress scheduler -----------------------------------------------------------

// Frames for nodes another tab hosts, held at the sender until this world's
// clock reaches their `deliver_at`.
//
// The world computes `deliver_at` from the link model (latency, the
// per-connection delivery floor) and the receiving tab delivers a frame the
// moment it arrives, because tabs share no clock. Holding each frame here
// until the sending clock gets there is what makes a link's latency real
// across tabs. Frames leave in `deliver_at` order; the sort is stable, so
// frames due at the same time keep the order the world queued them in, which
// keeps every connection's frames in order.
//
// A held frame is still on the wire: a fault that drops frames in the world's
// queues drops it too (`faultPurge`, `nodePurge`). Anything that hosts nodes
// elsewhere, a peer tab or a Worker, can drive one.
export class EgressScheduler {
  constructor() {
    this.held = [];
  }

  get size() {
    return this.held.length;
  }

  // When the earliest held frame is due; Infinity when nothing is held.
  nextAt() {
    return this.held.length ? this.held[0].deliver_at : Infinity;
  }

  // Hold `timedFrames` (`{ deliver_at, frame }`, in the order the world
  // queued them). Each goes after every frame due at or before its time.
  hold(timedFrames) {
    for (const t of timedFrames) {
      const at = Number(t.deliver_at) || 0;
      let lo = 0;
      let hi = this.held.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (this.held[mid].deliver_at <= at) lo = mid + 1;
        else hi = mid;
      }
      this.held.splice(lo, 0, { deliver_at: at, frame: t.frame });
    }
  }

  // Take every frame due at or before `now`, in `deliver_at` order.
  release(now) {
    let n = 0;
    while (n < this.held.length && this.held[n].deliver_at <= now) n += 1;
    return n ? this.held.splice(0, n) : [];
  }

  // Drop every held frame `pred(frame)` selects.
  purge(pred) {
    if (pred && this.held.length) this.held = this.held.filter((t) => !pred(t.frame));
  }

  clear() {
    this.held = [];
  }
}

const endNode = (end) => Number(end?.node);

// The frames a node-level change drops: every frame from or to the node, as
// the world does on a kill, a wipe, a config change and a removal.
export function nodePurge(id) {
  const n = Number(id);
  return (f) => endNode(f.src) === n || endNode(f.dst) === n;
}

// The frames a fault drops, by the rules `World::fault` applies to its own
// queues; null when the fault drops nothing (restart, heal, reconnect,
// latency, loss).
export function faultPurge(fault) {
  switch (fault && fault.kind) {
    case "kill":
    case "wipe":
      return nodePurge(fault.node);
    case "partition": {
      const a = Number(fault.a);
      const b = Number(fault.b);
      return (f) => {
        const src = endNode(f.src);
        const dst = endNode(f.dst);
        return (src === a && dst === b) || (src === b && dst === a);
      };
    }
    case "isolate": {
      const n = Number(fault.node);
      return (f) => (endNode(f.src) === n) !== (endNode(f.dst) === n);
    }
    default:
      return null;
  }
}

// ---- the world ------------------------------------------------------------------------

export class LabWorld {
  // `Lab` is the wasm-bindgen class; `hooks` are `onError(err, context)`,
  // `onSnapshot(snapshot)`, `onEvents(events)`, `onEgress(timedFrames)` (due
  // frames for nodes hosted elsewhere), `onDurable(ops)`, `onLoad(doc,
  // images)` (a new world was built, from these durable images) and
  // `onChange()` (the scenario document changed).
  constructor(Lab, hooks) {
    this.Lab = Lab;
    this.hooks = hooks;
    this.lab = null;
    this.egress = new EgressScheduler();
    this.speed = 1;
    this.paused = false;
    this.target = 0;
    this.wall = 0;
    this.raf = 0;
    this.lastSnapshotWall = -Infinity;
    this.eventIndex = 0;
    this.snapshotCache = null;
    this.scenarioCache = null;
    // Scenario fields the crate cannot change on a running world; the page
    // keeps them and merges them into the document it saves and shares.
    this.topics = [];
    this.name = "";
    this.hostedIds = null; // null: every node runs here
    this.id = "";
  }

  // Run `fn` against the module and report, not throw, when it fails.
  guard(context, fn) {
    try {
      return fn();
    } catch (err) {
      this.hooks.onError(err, context);
      return undefined;
    }
  }

  // ---- lifecycle ------------------------------------------------------------

  // An empty world. The canvas stays empty until a node is added.
  create(seed = 1) {
    // The old world's last durable ops belong to its scenario; keep them.
    this.drainDurable();
    this.dispose();
    this.lab = this.guard("create world", () => new this.Lab(u64(seed)));
    this.topics = [];
    this.name = "";
    this.id = "";
    this.hostedIds = null;
    this.hooks.onLoad?.(null, {});
    this.afterReset();
    return this.lab != null;
  }

  // Replace the world with one built from a scenario document. `hostedIds` is
  // `null` to run everything here, an empty list to run nothing, or the ids
  // this peer hosts. `images` is the durable state per node id, as
  // `storage.js` folded it, handed to the nodes before they start.
  load(scenario, hostedIds = null, images = null) {
    const doc = normalizeScenario(scenario);
    const json = JSON.stringify(doc);
    if (!this.lab) this.lab = this.guard("create world", () => new this.Lab(u64(doc.seed)));
    if (!this.lab) return false;
    const ids = hostedIds == null ? [] : hostedIds.length ? hostedIds : NONE_HOSTED;
    // The old world's last durable ops belong to its scenario; keep them.
    this.drainDurable();
    const ok = this.guard("load scenario", () => {
      if (images && Object.keys(images).length) this.lab.loadScenarioWithState(json, JSON.stringify(ids), JSON.stringify(images));
      else if (hostedIds == null) this.lab.loadScenario(json);
      else this.lab.loadScenarioHosted(json, JSON.stringify(ids));
      return true;
    });
    if (!ok) return false;
    this.hostedIds = hostedIds;
    this.topics = (doc.topics || []).map((t) => ({ ...t }));
    this.name = doc.name || "";
    this.id = doc.id || "";
    // Frames held for the old world's peers went with it.
    this.egress.clear();
    this.hooks.onLoad?.(doc, images || {});
    this.afterReset();
    return true;
  }

  // Bring a running world to a new scenario document with as few restarts as
  // possible: unchanged nodes keep their state, changed ones are rebuilt,
  // positions are copied. Anything the crate cannot change in place (seed,
  // link defaults, topics) forces a full reload.
  reconcile(scenario, hostedIds) {
    const next = normalizeScenario(scenario);
    const cur = this.scenario();
    if (
      !this.lab ||
      cur.id !== next.id ||
      cur.seed !== next.seed ||
      cur.links.default_latency_ms !== next.links.default_latency_ms ||
      JSON.stringify(cur.topics) !== JSON.stringify(next.topics)
    ) {
      return this.load(next, hostedIds);
    }
    const curById = new Map(cur.nodes.map((n) => [n.id, n]));
    const nextById = new Map(next.nodes.map((n) => [n.id, n]));
    for (const id of curById.keys()) {
      if (!nextById.has(id)) {
        this.guard("remove node", () => this.lab.removeNode(id));
        this.egress.purge(nodePurge(id));
      }
    }
    for (const spec of next.nodes) {
      const old = curById.get(spec.id);
      if (!old) {
        this.guard("add node", () => this.lab.addNode(JSON.stringify(spec)));
      } else if (
        old.kind !== spec.kind ||
        old.name !== spec.name ||
        JSON.stringify(old.config) !== JSON.stringify(spec.config)
      ) {
        this.guard("update node", () => this.lab.updateNode(spec.id, JSON.stringify(spec)));
        this.egress.purge(nodePurge(spec.id));
      } else if (old.x !== spec.x || old.y !== spec.y) {
        this.guard("move node", () => this.lab.setPosition(spec.id, spec.x, spec.y));
      }
    }
    this.name = next.name || "";
    this.scenarioCache = null;
    if (hostedIds !== undefined) this.setHosted(hostedIds);
    this.hooks.onChange();
    return true;
  }

  afterReset() {
    this.eventIndex = 0;
    this.snapshotCache = null;
    this.scenarioCache = null;
    this.target = this.now();
    this.lastSnapshotWall = -Infinity;
    this.hooks.onReset?.();
    this.hooks.onChange();
    // A snapshot right away, so the panels never see a world without one.
    if (this.lab) this.flush(performance.now(), true);
  }

  dispose() {
    this.egress.clear();
    if (this.lab && typeof this.lab.free === "function") {
      try {
        this.lab.free();
      } catch {
        // Already freed; nothing to do.
      }
    }
    this.lab = null;
  }

  get ready() {
    return this.lab != null;
  }

  // ---- the scenario document ----------------------------------------------------

  // The current document, positions included, with the page-side topics and
  // name merged in. Cached until the next change.
  scenario() {
    if (this.scenarioCache) return this.scenarioCache;
    let doc = null;
    if (this.lab) doc = this.guard("read scenario", () => JSON.parse(this.lab.scenario()));
    if (!doc) doc = { version: 1, seed: 1, name: "", links: { default_latency_ms: 5 }, nodes: [], topics: [] };
    doc.topics = this.topics.map((t) => ({ ...t }));
    doc.name = this.name;
    doc.id = this.id;
    this.scenarioCache = normalizeScenario(doc);
    return this.scenarioCache;
  }

  spec(id) {
    return this.scenario().nodes.find((n) => n.id === id) || null;
  }

  addNode(spec) {
    if (!this.lab) return null;
    const id = this.guard("add node", () => this.lab.addNode(JSON.stringify(spec)));
    if (id == null) return null;
    this.scenarioCache = null;
    this.hooks.onChange();
    return id;
  }

  removeNode(id) {
    if (!this.lab) return;
    this.guard("remove node", () => this.lab.removeNode(id));
    this.egress.purge(nodePurge(id));
    this.scenarioCache = null;
    this.hooks.onChange();
  }

  updateNode(id, spec) {
    if (!this.lab) return false;
    const ok = this.guard("update node", () => {
      this.lab.updateNode(id, JSON.stringify({ ...spec, id }));
      return true;
    });
    if (ok) this.egress.purge(nodePurge(id));
    this.scenarioCache = null;
    this.hooks.onChange();
    return ok === true;
  }

  setPosition(id, x, y) {
    if (!this.lab) return;
    this.guard("move node", () => this.lab.setPosition(id, x, y));
    if (this.scenarioCache) {
      const n = this.scenarioCache.nodes.find((s) => s.id === id);
      if (n) {
        n.x = x;
        n.y = y;
      }
    }
    this.hooks.onChange({ positionOnly: true });
  }

  setTopics(topics) {
    this.topics = topics.map((t) => ({ ...t }));
    this.scenarioCache = null;
    this.hooks.onChange();
  }

  setName(name) {
    this.name = name;
    this.scenarioCache = null;
    this.hooks.onChange();
  }

  // The identity that keys the durable state. The crate keeps it on the
  // running world too, so a reload of the running scenario finds its data.
  setId(id) {
    this.id = id || "";
    this.scenarioCache = null;
    this.hooks.onChange();
  }

  // ---- the clock ------------------------------------------------------------------

  now() {
    if (!this.lab) return 0;
    const n = this.guard("read clock", () => this.lab.now());
    return n == null ? 0 : Number(n);
  }

  start() {
    if (this.raf) return;
    this.wall = performance.now();
    const frame = (t) => {
      this.raf = requestAnimationFrame(frame);
      this.tick(t);
    };
    this.raf = requestAnimationFrame(frame);
  }

  stop() {
    if (this.raf) cancelAnimationFrame(this.raf);
    this.raf = 0;
  }

  tick(t) {
    const delta = Math.min(MAX_WALL_DELTA_MS, Math.max(0, t - this.wall));
    this.wall = t;
    if (this.lab && !this.paused) {
      this.target = Math.max(this.target, this.now()) + delta * this.speed;
      this.stepUntil(this.target);
    }
    this.flush(t);
  }

  stepUntil(ms) {
    if (!this.lab) return 0;
    const steps = this.guard("step", () => this.lab.stepUntil(u64(ms)));
    return steps == null ? 0 : Number(steps);
  }

  // Advance by `ms` of simulated time, running everything due.
  step(ms) {
    if (!this.lab) return;
    const target = this.now() + ms;
    this.stepUntil(target);
    this.target = target;
    this.flush(performance.now(), true);
  }

  // Run until nothing is due any more, or 5 s of simulated time passed. A
  // frame held for another tab is due work too: it leaves when the clock
  // reaches it.
  settle() {
    if (!this.lab) return;
    const start = this.now();
    const limit = start + SETTLE_WINDOW_MS;
    let t = start;
    while (t < limit) {
      const busy = this.guard("settle", () => this.lab.hasWorkBy(u64(limit))) || this.egress.nextAt() <= limit;
      if (!busy) break;
      t = Math.min(limit, t + SETTLE_STEP_MS);
      this.stepUntil(t);
      this.pumpEgress();
    }
    this.target = this.now();
    this.flush(performance.now(), true);
  }

  setSpeed(speed) {
    this.speed = Number(speed) || 1;
    this.hooks.onClock?.();
  }

  setPaused(paused) {
    this.paused = Boolean(paused);
    if (!this.paused) this.target = this.now();
    this.hooks.onClock?.();
  }

  // ---- observation ------------------------------------------------------------------

  // Move the world's new egress into the hold, then hand every frame the
  // clock has reached to the session layer.
  pumpEgress() {
    if (!this.lab) return;
    const raw = this.guard("drain egress", () => this.lab.drainEgress());
    if (raw && raw.length > 2) {
      const frames = this.guard("parse egress", () => JSON.parse(raw));
      if (frames && frames.length) this.egress.hold(frames);
    }
    if (!this.egress.size) return;
    const due = this.egress.release(this.now());
    if (due.length) this.hooks.onEgress(due);
  }

  // Hand the durable-state ops recorded since the last drain to storage.
  drainDurable() {
    if (!this.lab) return;
    const raw = this.guard("drain durable", () => this.lab.drainDurable());
    if (raw && raw.length > 2) {
      const ops = this.guard("parse durable", () => JSON.parse(raw));
      if (ops && ops.length) this.hooks.onDurable(ops);
    }
  }

  // Release due egress, drain durable ops and, when due, read a snapshot and
  // the new events.
  flush(wallNow, force = false) {
    if (!this.lab) return;
    this.pumpEgress();
    this.drainDurable();
    if (!force && wallNow - this.lastSnapshotWall < SNAPSHOT_INTERVAL_MS) return;
    this.lastSnapshotWall = wallNow;
    const snap = this.guard("snapshot", () => JSON.parse(this.lab.snapshot()));
    if (snap) {
      this.snapshotCache = snap;
      this.hooks.onSnapshot(snap);
    }
    const count = this.guard("count events", () => this.lab.eventCount());
    if (count != null && Number(count) > this.eventIndex) {
      const events = this.guard("read events", () => JSON.parse(this.lab.eventsSince(this.eventIndex)));
      if (events && events.length) {
        this.eventIndex = events[events.length - 1].index + 1;
        this.hooks.onEvents(events);
      } else {
        this.eventIndex = Number(count);
      }
    }
  }

  snapshot() {
    return this.snapshotCache;
  }

  // ---- faults and commands ------------------------------------------------------------

  fault(fault) {
    if (!this.lab) return false;
    const ok = this.guard("fault", () => {
      this.lab.fault(JSON.stringify(fault));
      return true;
    });
    if (ok) this.egress.purge(faultPurge(fault));
    this.scenarioCache = null;
    this.hooks.onChange();
    return ok === true;
  }

  // Returns `{ ok, answer }` or `{ ok: false, error }`; never throws.
  control(id, command) {
    if (!this.lab) return { ok: false, error: "no world" };
    try {
      const answer = this.lab.control(id, JSON.stringify(command));
      return { ok: true, answer: JSON.parse(answer) };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }

  // ---- distributed hosting --------------------------------------------------------------

  // `ids`: null runs everything here, an empty list runs nothing.
  setHosted(ids) {
    if (!this.lab) return;
    this.hostedIds = ids;
    const list = ids == null ? [] : ids.length ? ids : NONE_HOSTED;
    this.guard("set hosted", () => this.lab.setHosted(JSON.stringify(list)));
  }

  pushIngress(frames) {
    if (!this.lab || !frames.length) return;
    this.guard("push ingress", () => this.lab.pushIngress(JSON.stringify(frames)));
  }

  applyRemoteSnapshot(id, state) {
    if (!this.lab) return;
    this.guard("remote snapshot", () => this.lab.applyRemoteSnapshot(id, JSON.stringify(state ?? null)));
  }
}

// Fill the defaults of a scenario document so the crate, which rejects unknown
// keys and needs `version`, accepts what the page hands it.
export function normalizeScenario(input) {
  const s = input && typeof input === "object" ? input : {};
  const out = {
    version: 1,
    seed: Number.isFinite(Number(s.seed)) ? Number(s.seed) : 1,
    name: typeof s.name === "string" ? s.name : "",
    links: { default_latency_ms: Number(s.links?.default_latency_ms ?? 5) },
    link_overrides: Array.isArray(s.link_overrides) ? s.link_overrides : [],
    nodes: (Array.isArray(s.nodes) ? s.nodes : []).map((n) => ({
      id: Number(n.id) || 0,
      kind: String(n.kind || ""),
      name: typeof n.name === "string" ? n.name : "",
      x: Number(n.x) || 0,
      y: Number(n.y) || 0,
      config: n.config && typeof n.config === "object" ? n.config : {},
    })),
    topics: (Array.isArray(s.topics) ? s.topics : []).map((t) => {
      const topic = {
        name: String(t.name || ""),
        partitions: Number(t.partitions) || 1,
        replication_factor: Number.isFinite(Number(t.replication_factor)) ? Number(t.replication_factor) : -1,
      };
      if (t.configs && typeof t.configs === "object" && Object.keys(t.configs).length) topic.configs = t.configs;
      return topic;
    }),
  };
  if (typeof s.id === "string" && s.id) out.id = s.id;
  return out;
}
