// Scripted experiments: a scenario's `experiment` runs against the lab clock.
//
//   "experiment": {
//     "name": "Lose a replica under acks=all",
//     "steps":  [{ "at": 15000, "fault": {"kind":"kill","node":3} },
//                { "at": 30000, "command": {"node":4, "cmd":"rate", "rate_per_sec": 20} }],
//     "expect": [{ "by": 60000, "check": "invariants_hold" },
//                { "at": 20000, "check": "isr_size", "topic":"orders", "partition":0, "op":"==", "value":2 }],
//     "end": 60000
//   }
//
// Times are lab milliseconds from the start of the run. A command's `node`
// may be "admin", the scenario's hidden admin node (alter_config, reassign…). A step applies its
// fault through `world.fault` or its command through `world.control` at the
// first snapshot at or after its time (the result records when). A check
// with `at` is decided once, at that time; a check with `by` passes as soon
// as it holds and fails if it has not by then; `after` (default 0) opens its
// window, and `since` takes a baseline for a counter (the value at that time
// is subtracted). `invariants_hold` instead must hold for the whole window:
// it fails at the first violation the invariant checker reports and passes
// at `by` (or at `end`). At `end` the run stops, the clock pauses, and every
// open check is decided.
//
// Checks: invariants_hold; isr_size {topic, partition}; consumer_lag {node};
// event {kind, node?, match?}; producer_acked {node, since?}; leader_changed
// {topic, partition, since?}; snapshot_path {node, path: "a.b.0.c"};
// kafka_error {error: "NOT_ENOUGH_REPLICAS", node?}, an error code in a Kafka
// response the network capture decoded. Comparisons take `op` (==, !=, <,
// <=, >, >=, contains) and `value`.
//
// `ExperimentRunner` is the logic (the page exposes it as
// `window.krabkaLab.experiment`); `ExperimentPanel` is the Scenarios tab's
// editor and results list.

import { el, button, fmtMs } from "./dom.js";

export const CHECKS = {
  invariants_hold: { label: "invariants hold", needs: [] },
  isr_size: { label: "ISR size", needs: ["topic", "partition"], op: true },
  consumer_lag: { label: "consumer lag", needs: ["node"], op: true },
  event: { label: "event", needs: ["kind"] },
  producer_acked: { label: "records acknowledged", needs: ["node"], op: true },
  leader_changed: { label: "leader changed", needs: ["topic", "partition"] },
  snapshot_path: { label: "state", needs: ["node", "path"], op: true },
  kafka_error: { label: "Kafka error", needs: ["error"] },
};

const OPS = {
  "==": (a, b) => (typeof a === "object" || typeof b === "object" ? JSON.stringify(a) === JSON.stringify(b) : a == b), // eslint-disable-line eqeqeq
  "!=": (a, b) => !OPS["=="](a, b),
  "<": (a, b) => a < b,
  "<=": (a, b) => a <= b,
  ">": (a, b) => a > b,
  ">=": (a, b) => a >= b,
  contains: (a, b) => (Array.isArray(a) ? a.some((x) => OPS["=="](x, b)) : String(a ?? "").includes(String(b))),
};

const isTime = (v) => Number.isFinite(v) && v >= 0;

// The node ids a fault names.
function faultNodes(f) {
  return ["node", "a", "b", "from", "to"].filter((k) => f[k] != null).map((k) => Number(f[k]));
}

/**
 * Problems with an experiment, as readable lines; an empty list means it can
 * run. With `scenario`, node ids must name its nodes.
 */
export function validateExperiment(exp, scenario = null) {
  const errors = [];
  if (!exp || typeof exp !== "object" || Array.isArray(exp)) return ["an experiment is a JSON object"];
  const ids = scenario ? new Set(scenario.nodes.map((n) => Number(n.id))) : null;
  const known = (id, where) => {
    if (ids && id !== "admin" && !ids.has(Number(id))) errors.push(`${where}: no node ${id} in this scenario`);
  };
  if (exp.name != null && typeof exp.name !== "string") errors.push("`name` is text");
  if (!isTime(exp.end) || exp.end <= 0) errors.push("`end` is the lab time the run stops at, in ms (a positive number)");
  const steps = exp.steps ?? [];
  const expect = exp.expect ?? [];
  if (!Array.isArray(steps)) errors.push("`steps` is a list");
  if (!Array.isArray(expect)) errors.push("`expect` is a list");
  if (Array.isArray(steps)) {
    steps.forEach((s, i) => {
      const where = `step ${i + 1}`;
      if (!s || typeof s !== "object") return errors.push(`${where} is an object`);
      if (!isTime(s.at)) errors.push(`${where}: \`at\` is a lab time in ms`);
      else if (isTime(exp.end) && s.at > exp.end) errors.push(`${where}: at ${s.at} is after the end (${exp.end})`);
      if (Boolean(s.fault) === Boolean(s.command)) return errors.push(`${where}: give either a \`fault\` or a \`command\``);
      if (s.fault) {
        if (typeof s.fault.kind !== "string") errors.push(`${where}: the fault needs a \`kind\``);
        else for (const id of faultNodes(s.fault)) known(id, where);
      } else {
        if (s.command.node == null || typeof s.command.cmd !== "string") errors.push(`${where}: a command is { "node": id, "cmd": "…", … }`);
        else known(s.command.node, where);
      }
    });
  }
  if (Array.isArray(expect)) {
    expect.forEach((c, i) => {
      const where = `check ${i + 1}`;
      if (!c || typeof c !== "object") return errors.push(`${where} is an object`);
      const spec = CHECKS[c.check];
      if (!spec) return errors.push(`${where}: unknown check \`${c.check}\` (one of ${Object.keys(CHECKS).join(", ")})`);
      const timed = (c.at != null) + (c.by != null);
      if (timed > 1) errors.push(`${where}: give \`at\` or \`by\`, not both`);
      if (timed === 0 && c.check !== "invariants_hold") errors.push(`${where}: give \`at\` (decided then) or \`by\` (must hold by then)`);
      for (const k of ["at", "by", "after", "since"]) if (c[k] != null && !isTime(c[k])) errors.push(`${where}: \`${k}\` is a lab time in ms`);
      const t = c.at ?? c.by;
      if (isTime(t) && isTime(exp.end) && t > exp.end) errors.push(`${where}: ${c.at != null ? "at" : "by"} ${t} is after the end (${exp.end})`);
      if (isTime(c.after) && isTime(t ?? exp.end) && c.after > (t ?? exp.end)) errors.push(`${where}: after ${c.after} is later than ${t != null ? (c.at != null ? "at" : "by") : "the end"} (${t ?? exp.end}), so it could never be decided`);
      for (const k of spec.needs) if (c[k] == null || c[k] === "") errors.push(`${where}: \`${c.check}\` needs \`${k}\``);
      if (spec.op) {
        if (!OPS[c.op]) errors.push(`${where}: \`op\` is one of ${Object.keys(OPS).join(" ")}`);
        if (c.value === undefined) errors.push(`${where}: \`${c.check}\` needs a \`value\``);
      }
      if (c.node != null) known(c.node, where);
    });
  }
  return errors;
}

// A dotted path into an object: "client.metadata.brokers.0.id".
export function pathValue(obj, path) {
  let at = obj;
  for (const step of String(path).split(".")) {
    if (at == null) return undefined;
    at = at[Array.isArray(at) && /^\d+$/.test(step) ? Number(step) : step];
  }
  return at;
}

function partitionOf(snap, topic, partition) {
  const cluster = snap.nodes.find((n) => n.kind === "admin" && n.state?.cluster)?.state.cluster;
  return cluster?.topics?.find((t) => t.name === topic)?.partitions?.find((p) => p.partition === Number(partition)) ?? null;
}

// A node's state; "admin" is the scenario's hidden admin node (its `cluster` observer).
const stateOf = (snap, id) => snap.nodes.find((n) => (id === "admin" ? n.kind === "admin" : n.id === Number(id)))?.state ?? null;

// The words a step or check shows in the results list.
function describeCheck(c, name) {
  const cmp = c.op ? ` ${c.op} ${JSON.stringify(c.value)}` : "";
  switch (c.check) {
    case "invariants_hold":
      return "invariants hold";
    case "isr_size":
      return `ISR of ${c.topic}-${c.partition}${cmp}`;
    case "consumer_lag":
      return `lag of ${name(c.node)}${cmp}`;
    case "event":
      return `event ${c.kind}${c.node != null ? ` from ${name(c.node)}` : ""}`;
    case "producer_acked":
      return `${name(c.node)} acknowledged${c.since != null ? ` since ${fmtMs(c.since)}` : ""}${cmp}`;
    case "leader_changed":
      return `leader of ${c.topic}-${c.partition} changed`;
    case "snapshot_path":
      return `${name(c.node)}.${c.path}${c.since != null ? ` (since ${fmtMs(c.since)})` : ""}${cmp}`;
    case "kafka_error":
      return `${c.error} in a Kafka response${c.node != null ? ` to or from ${name(c.node)}` : ""}`;
    default:
      return c.check;
  }
}

export class ExperimentRunner {
  // hooks: fault(f), control(id, cmd) → { ok, error }, pause(), scenario(),
  // capture (the network capture, for kafka_error), nodeName(id), onChange(), onEnd(runner)
  constructor(hooks) {
    this.hooks = hooks;
    this.reset();
  }

  reset() {
    this.state = "idle"; // idle | starting | running | passed | failed | stopped
    this.name = "";
    this.t0 = 0;
    this.end = 0;
    this.now = 0; // lab ms from the start of the run, at the last tick
    this.steps = [];
    this.checks = [];
    this.events = [];
    this.fresh = [];
    this.invariants = null;
    this.scanning = false;
  }

  get running() {
    return this.state === "running" || this.state === "starting";
  }

  /** Starts `exp` at the lab time `t0` (the world's clock now). */
  async start(exp, t0 = 0) {
    this.reset();
    this.state = "starting";
    this.name = exp.name || "Experiment";
    this.t0 = t0;
    this.end = exp.end;
    const name = (id) => (id === "admin" ? "scenario-admin" : this.hooks.nodeName(id));
    this.steps = (exp.steps || [])
      .map((s, i) => ({ i, at: s.at, fault: s.fault ?? null, command: s.command ?? null, status: "pending", t: null, error: null }))
      .sort((a, b) => a.at - b.at || a.i - b.i);
    for (const s of this.steps) s.label = s.fault ? this.describeFault(s.fault) : `${name(s.command.node)}: ${s.command.cmd}${s.command.set ? ` ${Object.entries(s.command.set).map(([k, v]) => `${k}=${v}`).join(" ")}` : ""}`;
    this.checks = (exp.expect || []).map((c, i) => ({ i, spec: c, label: describeCheck(c, name), status: "pending", t: null, observed: null, baseline: undefined }));
    // The invariant checker (invariants.js) is loaded on first use; without it the check says so.
    if (this.checks.some((c) => c.spec.check === "invariants_hold")) {
      try {
        const { InvariantChecker } = await import("./invariants.js");
        this.invariants = new InvariantChecker();
      } catch {
        this.invariants = null;
      }
    }
    if (this.state === "starting") this.state = "running";
    this.hooks.onChange?.();
  }

  describeFault(f) {
    return this.hooks.describeFault ? this.hooks.describeFault(f) : `${f.kind} ${faultNodes(f).join(" ")}`;
  }

  stop(reason = "stopped") {
    if (!this.running) return;
    this.state = "stopped";
    for (const c of this.checks) if (c.status === "pending") c.observed = c.observed ?? reason;
    this.hooks.onChange?.();
  }

  /** World events, as the timeline gets them. */
  onEvents(events) {
    if (this.state !== "running") return;
    for (const e of events) {
      this.events.push(e);
      this.fresh.push(e);
    }
  }

  /** One snapshot of the world: run due steps, decide checks, stop at the end. */
  tick(snap) {
    // A command flushes a snapshot at once, which comes back here: one tick at a time.
    if (this.state !== "running" || !snap || this.ticking) return;
    this.ticking = true;
    try {
      this.step(snap);
    } finally {
      this.ticking = false;
    }
  }

  step(snap) {
    const rel = snap.now - this.t0;
    this.now = rel;
    let changed = false;
    for (const s of this.steps) {
      if (s.status !== "pending" || s.at > rel) continue;
      s.t = rel;
      if (s.fault) {
        const ok = this.hooks.fault(s.fault);
        s.status = ok === false ? "error" : "done";
        if (ok === false) s.error = "the world refused the fault";
      } else {
        const { node, ...cmd } = s.command;
        // "admin" names the scenario's hidden admin node, whatever id the world gave it.
        const id = node === "admin" ? snap.nodes.find((n) => n.kind === "admin")?.id : Number(node);
        const r = id == null ? { ok: false, error: "no admin node" } : this.hooks.control(id, cmd);
        s.status = r.ok ? "done" : "error";
        s.error = r.ok ? null : r.error;
      }
      changed = true;
    }
    if (this.invariants) {
      try {
        this.invariants.observe(snap, this.fresh, this.hooks.scenario?.());
      } catch {
        // A checker bug must not stop the run; its check decides on what it has.
      }
    }
    this.fresh = [];
    if (this.checks.some((c) => c.status === "pending" && c.spec.check === "kafka_error")) this.scan();
    const final = rel >= this.end;
    // Responses sent by `until` that wait for decoding: later traffic must
    // not hold a decision back.
    const decoding = (until) => this.scanning || (this.hooks.capture?.scanQueue || []).some((ex) => ex.req.at <= until);
    let deferred = false;
    for (const c of this.checks) {
      if (c.status !== "pending") continue;
      // A due kafka_error waits for the responses still being decoded, then
      // the same snapshot is judged again.
      const due = c.spec.at ?? c.spec.by ?? this.end;
      if (c.spec.check === "kafka_error" && (final || rel >= due) && decoding(this.t0 + due)) {
        deferred = true;
        continue;
      }
      if (this.decide(c, snap, rel, final)) changed = true;
    }
    if (deferred) {
      this.drain(snap.now).then(() => this.tick(snap));
    } else if (final) {
      // A check that could not be decided by the end did not hold.
      for (const c of this.checks) if (c.status === "pending") Object.assign(c, { status: "fail", t: rel, observed: c.observed ?? "not decided by the end" });
      this.state = this.checks.some((c) => c.status === "fail") || this.steps.some((s) => s.status === "error") ? "failed" : "passed";
      for (const s of this.steps) if (s.status === "pending") s.status = "skipped";
      changed = true;
      this.hooks.pause?.();
      this.hooks.onEnd?.(this);
    }
    if (changed) this.hooks.onChange?.();
  }

  // Decodes the queued responses sent by `until`, after any scan in progress.
  async drain(until) {
    while (this.scanning) await new Promise((resolve) => setTimeout(resolve, 5));
    while ((this.hooks.capture?.scanQueue || []).some((ex) => ex.req.at <= until)) {
      await this.scan();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  // Decodes the capture's queued responses a few ms at a time, for kafka_error.
  async scan() {
    if (this.scanning || !this.hooks.capture) return;
    this.scanning = true;
    try {
      await this.hooks.capture.scan(6);
    } catch {
      // Decoding is best effort.
    } finally {
      this.scanning = false;
    }
  }

  // Decides one pending check if it can; returns whether it changed.
  decide(c, snap, rel, final) {
    const spec = c.spec;
    const after = spec.after ?? 0;
    if (rel < after) return false;
    const pass = (observed) => Boolean(Object.assign(c, { status: "pass", t: rel, observed }));
    const fail = (observed) => Boolean(Object.assign(c, { status: "fail", t: rel, observed }));
    if (spec.check === "invariants_hold") {
      if (!this.invariants) {
        if (!final && rel < (spec.at ?? spec.by ?? this.end)) return false;
        c.status = "skipped";
        c.t = rel;
        c.observed = "invariants.js is not loaded";
        return true;
      }
      const v = this.invariants.violations.find((x) => x.at == null || x.at - this.t0 >= after);
      if (v) return fail(v.text || v.check);
      if (rel >= (spec.at ?? spec.by ?? this.end) || final) return pass("no violation");
      return false;
    }
    const m = this.measure(c, snap, rel);
    if (m === null) {
      if (final || (spec.at != null && rel >= spec.at) || (spec.by != null && rel >= spec.by)) return fail(c.observed ?? "nothing to measure yet");
      return false;
    }
    c.observed = m.observed;
    if (spec.at != null) {
      if (rel < spec.at) return false;
      return (m.ok ? pass : fail)(m.observed);
    }
    if (m.ok) return pass(m.observed);
    if (rel >= spec.by || final) return fail(m.observed);
    return false;
  }

  // `{ ok, observed }` for a check now, or null when there is nothing to measure.
  measure(c, snap, rel) {
    const spec = c.spec;
    const cmp = (v) => ({ ok: OPS[spec.op](v, spec.value), observed: v });
    // A counter measured from `since`: the value then is the baseline.
    const counted = (v) => {
      if (spec.since == null) return cmp(v);
      if (rel < spec.since) return null;
      if (c.baseline === undefined) c.baseline = v;
      return { ...cmp(typeof v === "number" && typeof c.baseline === "number" ? v - c.baseline : v), observed: typeof v === "number" ? `${v - c.baseline} (${c.baseline} → ${v})` : v };
    };
    switch (spec.check) {
      case "isr_size": {
        const p = partitionOf(snap, spec.topic, spec.partition);
        if (!p) return null;
        return { ok: OPS[spec.op](p.isr.length, spec.value), observed: `${p.isr.length} [${p.isr.join(", ")}]` };
      }
      case "consumer_lag": {
        const lag = stateOf(snap, spec.node)?.lag;
        return lag == null ? null : cmp(lag);
      }
      case "producer_acked": {
        const acked = stateOf(snap, spec.node)?.acked;
        return acked == null ? null : counted(acked);
      }
      case "snapshot_path": {
        const v = pathValue(stateOf(snap, spec.node), spec.path);
        return v === undefined ? null : counted(v);
      }
      case "leader_changed": {
        const p = partitionOf(snap, spec.topic, spec.partition);
        if (!p) return null;
        if (rel < (spec.since ?? 0)) return null;
        if (c.baseline === undefined) c.baseline = p.leader;
        return { ok: p.leader != null && p.leader >= 0 && p.leader !== c.baseline, observed: `leader ${c.baseline} → ${p.leader}` };
      }
      case "event": {
        const from = this.t0 + (spec.after ?? 0);
        const until = spec.at != null ? this.t0 + spec.at : Infinity;
        const hit = this.events.find(
          (e) =>
            e.kind === spec.kind &&
            e.at >= from &&
            e.at <= until &&
            (spec.node == null || e.node === Number(spec.node)) &&
            (!spec.match || Object.entries(spec.match).every(([k, v]) => OPS["=="](e.detail?.[k], v))),
        );
        return hit ? { ok: true, observed: `at ${fmtMs(hit.at - this.t0)}` } : { ok: false, observed: "not seen" };
      }
      case "kafka_error": {
        const exchanges = this.hooks.capture?.exchanges || [];
        const from = this.t0 + (spec.after ?? 0);
        // A decision that waited for decoding still counts only its window.
        const until = this.t0 + (spec.at ?? spec.by ?? this.end);
        for (let i = exchanges.length - 1; i >= 0; i--) {
          const ex = exchanges[i];
          if (ex.req.at < from) break;
          if (ex.req.at > until || !ex.errors?.includes(spec.error)) continue;
          if (spec.node != null && ex.client.node !== Number(spec.node) && ex.server.node !== Number(spec.node)) continue;
          return { ok: true, observed: `at ${fmtMs(ex.req.at - this.t0)}, ${this.hooks.nodeName(ex.client.node)} → ${this.hooks.nodeName(ex.server.node)}` };
        }
        return { ok: false, observed: "not seen" };
      }
      default:
        return null;
    }
  }

  /** Plain results, for tests: `{ state, name, now, end, steps, checks, passed, failed, pending }`. */
  toJSON() {
    const count = (s) => this.checks.filter((c) => c.status === s).length;
    return {
      state: this.state,
      name: this.name,
      now: this.now,
      end: this.end,
      steps: this.steps.map(({ at, label, status, t, error }) => ({ at, label, status, t, error })),
      checks: this.checks.map(({ spec, label, status, t, observed }) => ({ check: spec.check, at: spec.at ?? null, by: spec.by ?? null, label, status, t, observed: observed == null ? null : String(observed) })),
      passed: count("pass"),
      failed: count("fail"),
      pending: count("pending"),
    };
  }
}

// The example the editor offers for a scenario without an experiment.
function example(scenario) {
  const brokers = scenario.nodes.filter((n) => n.kind === "krabka-broker").map((n) => n.id);
  const victim = brokers[brokers.length - 1] ?? 1;
  return {
    name: "Kill a broker and bring it back",
    steps: [
      { at: 15000, fault: { kind: "kill", node: victim } },
      { at: 45000, fault: { kind: "restart", node: victim } },
    ],
    expect: [{ by: 90000, check: "invariants_hold" }],
    end: 90000,
  };
}

const STATUS_TEXT = { pending: "pending", pass: "pass", fail: "fail", done: "done", error: "error", skipped: "skipped" };

export class ExperimentPanel {
  // hooks: scenario(), apply(exp | null) → bool, run(), stop(), fork(), runner, nodeName(id), role()
  constructor(container, hooks) {
    this.hooks = hooks;
    this.dirty = false;
    this.lastJson = null;
    const section = el("section", "lab-pal-section lab-exp");
    section.dataset.section = "experiment";
    section.appendChild(el("h2", "lab-rail-heading", "Experiment"));
    section.appendChild(
      el("p", "lab-rail-help", "A script of faults and commands on the lab clock, with checks. Run experiment restarts this scenario fresh (new broker disks, fresh clients) and runs it. The experiment travels with Copy link and Export."),
    );
    this.editor = el("textarea", "lab-textarea lab-exp-editor");
    this.editor.rows = 10;
    this.editor.spellcheck = false;
    this.editor.setAttribute("aria-label", "Experiment JSON");
    this.editor.addEventListener("input", () => {
      this.dirty = true;
      this.validate();
    });
    this.problems = el("ul", "lab-exp-problems");
    this.problems.setAttribute("aria-live", "polite");
    const actions = el("div", "lab-palette-actions");
    this.applyBtn = button("Apply", "lab-btn-sm", () => this.apply(), { title: "Keep this experiment with the scenario (no restart)" });
    this.exampleBtn = button("Example", "lab-btn-sm", () => this.fillExample(), { title: "Fill in an example experiment for this scenario" });
    this.runBtn = button("Run experiment", "lab-btn-sm lab-primary", () => this.run(), { title: "Restart the scenario fresh and run the experiment" });
    this.stopBtn = button("Stop", "lab-btn-sm", () => this.hooks.stop(), { title: "Stop the run; the cluster keeps going" });
    actions.append(this.runBtn, this.stopBtn, this.applyBtn, this.exampleBtn);
    this.results = el("div", "lab-exp-results");
    this.results.setAttribute("aria-live", "polite");
    const fork = el("div", "lab-exp-fork");
    this.forkBtn = button("Fork here", "lab-btn-sm", () => this.hooks.fork(), { title: "Copy the brokers' disks into a new scenario and open it" });
    fork.append(
      this.forkBtn,
      el("p", "lab-rail-help", "Pauses the clock, flushes the brokers' disks, copies them to a new saved scenario (same nodes, links and experiment) and opens it. The brokers boot from the copied disks; clients start fresh."),
    );
    section.append(this.editor, this.problems, actions, this.results, fork);
    // After "This scenario", before the presets.
    const first = container.querySelector(".lab-pal-section");
    container.insertBefore(section, first ? first.nextSibling : null);
    this.root = section;
  }

  // The scenario changed: show its experiment unless the reader is editing.
  update(scenario) {
    const json = scenario.experiment ? JSON.stringify(scenario.experiment, null, 2) : "";
    if (json !== this.lastJson && (!this.dirty || document.activeElement !== this.editor)) {
      this.lastJson = json;
      this.editor.value = json;
      this.dirty = false;
      this.validate();
    }
    const spoke = this.hooks.role() === "spoke";
    this.forkBtn.disabled = spoke || !scenario.nodes.some((n) => n.kind === "krabka-broker");
    this.renderResults();
  }

  // The editor's text as an experiment: `{ exp, errors }`; empty text is no experiment.
  read() {
    const text = this.editor.value.trim();
    if (!text) return { exp: null, errors: [] };
    let exp;
    try {
      exp = JSON.parse(text);
    } catch (err) {
      return { exp: null, errors: [`not JSON: ${err.message}`] };
    }
    return { exp, errors: validateExperiment(exp, this.hooks.scenario()) };
  }

  validate() {
    const { exp, errors } = this.read();
    this.problems.innerHTML = "";
    for (const e of errors) this.problems.appendChild(el("li", null, e));
    this.root.classList.toggle("lab-invalid", errors.length > 0);
    const spoke = this.hooks.role() === "spoke";
    this.applyBtn.disabled = spoke || errors.length > 0 || !this.dirty;
    this.runBtn.disabled = spoke || errors.length > 0 || !exp;
    this.exampleBtn.disabled = spoke;
    return { exp, errors };
  }

  apply() {
    const { exp, errors } = this.validate();
    if (errors.length) return false;
    const ok = this.hooks.apply(exp);
    if (ok !== false) {
      this.dirty = false;
      this.lastJson = exp ? JSON.stringify(exp, null, 2) : "";
      this.validate();
    }
    return ok;
  }

  fillExample() {
    this.editor.value = JSON.stringify(example(this.hooks.scenario()), null, 2);
    this.dirty = true;
    this.validate();
  }

  run() {
    if (this.dirty && this.apply() === false) return;
    this.hooks.run();
  }

  renderResults() {
    const r = this.hooks.runner;
    this.stopBtn.disabled = !r.running;
    const key = JSON.stringify([r.state, r.steps.map((s) => s.status), r.checks.map((c) => [c.status, c.observed]), Math.floor(r.now / 1000)]);
    if (key === this.resultsKey) return;
    this.resultsKey = key;
    this.results.innerHTML = "";
    if (r.state === "idle") return;
    const head = el("p", "lab-exp-head");
    const counts = r.toJSON();
    const verdict = {
      starting: "starting…",
      running: `running · ${fmtMs(r.now)} of ${fmtMs(r.end)}`,
      passed: `passed · ${counts.passed} of ${r.checks.length} checks`,
      failed: `failed · ${counts.failed} of ${r.checks.length} checks failed`,
      stopped: "stopped",
    }[r.state];
    head.append(el("strong", null, r.name), el("span", `lab-exp-state lab-exp-${r.state}`, verdict));
    this.results.appendChild(head);
    const list = (title, rows) => {
      if (!rows.length) return;
      this.results.appendChild(el("h3", "lab-exp-sub", title));
      const ul = el("ul", "lab-exp-list");
      for (const row of rows) ul.appendChild(row);
      this.results.appendChild(ul);
    };
    list(
      "Steps",
      r.steps.map((s) => {
        const li = el("li", `lab-exp-row lab-exp-${s.status}`);
        li.append(el("span", "lab-exp-time", fmtMs(s.at)), el("span", "lab-exp-label", s.label), el("span", "lab-exp-status", s.t == null ? STATUS_TEXT[s.status] : `${STATUS_TEXT[s.status]} at ${fmtMs(s.t)}`));
        if (s.error) li.title = s.error;
        return li;
      }),
    );
    list(
      "Checks",
      r.checks.map((c) => {
        const li = el("li", `lab-exp-row lab-exp-${c.status}`);
        const when = c.spec.at != null ? `at ${fmtMs(c.spec.at)}` : `by ${fmtMs(c.spec.by ?? r.end)}`;
        li.append(el("span", "lab-exp-time", when), el("span", "lab-exp-label", c.label), el("span", "lab-exp-status", c.t == null ? STATUS_TEXT[c.status] : `${STATUS_TEXT[c.status]} at ${fmtMs(c.t)}`));
        if (c.observed != null) li.appendChild(el("span", "lab-exp-observed", String(c.observed)));
        return li;
      }),
    );
  }
}
