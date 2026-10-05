// The fault toolbar above the canvas. It is always on screen, so a reader
// sees at once that the cluster can be broken; its buttons wake up as nodes
// are selected.
//
// One selected node offers kill / restart / wipe / isolate / reconnect /
// pause, and behind "More…" a clock skew and a disk mode; two selected nodes
// (Shift+click the second) offer partition / heal, a one-way cut either way
// and the link's latency and loss. Every button builds the `Fault` JSON the
// crate's serde derive reads: `{"kind":"kill","node":1}`,
// `{"kind":"partition","a":1,"b":2}`, `{"kind":"cut_one_way","from":1,"to":2}`,
// `{"kind":"latency","a":1,"b":2,"ms":50}`, `{"kind":"loss","a":1,"b":2,"permille":100}`,
// `{"kind":"clock_skew","node":1,"ms":-5000}`, `{"kind":"disk","node":1,"mode":"slow","ms":200}`.

import { el, button, select } from "./dom.js";

const DISK_MODES = [
  { value: "ok", label: "ok" },
  { value: "slow", label: "slow syncs" },
  { value: "full", label: "full (ENOSPC)" },
  { value: "eio", label: "failing (EIO)" },
];

// Constructors for every fault shape, shared with the context menu.
export const FAULT = {
  kill: (node) => ({ kind: "kill", node }),
  restart: (node) => ({ kind: "restart", node }),
  wipe: (node) => ({ kind: "wipe", node }),
  isolate: (node) => ({ kind: "isolate", node }),
  reconnect: (node) => ({ kind: "reconnect", node }),
  partition: (a, b) => ({ kind: "partition", a, b }),
  heal: (a, b) => ({ kind: "heal", a, b }),
  latency: (a, b, ms) => ({ kind: "latency", a, b, ms }),
  loss: (a, b, permille) => ({ kind: "loss", a, b, permille }),
  cut_one_way: (from, to) => ({ kind: "cut_one_way", from, to }),
  heal_one_way: (from, to) => ({ kind: "heal_one_way", from, to }),
  pause: (node) => ({ kind: "pause", node }),
  resume: (node) => ({ kind: "resume", node }),
  clock_skew: (node, ms) => ({ kind: "clock_skew", node, ms }),
  disk: (node, mode, ms) => (mode === "slow" ? { kind: "disk", node, mode, ms } : { kind: "disk", node, mode }),
};

// Whether frames from `from` to `to` are dropped by a one-way cut.
export function oneWayCut(snapshot, from, to) {
  return (snapshot?.one_way_cuts || []).some((c) => c.from === from && c.to === to);
}

// The links of node `id` cut one by one (a partition, or a scenario's cut
// link), apart from the node's own isolation.
export function cutLinks(snapshot, id) {
  return (snapshot?.links || []).filter((l) => l.cut && (l.a === id || l.b === id));
}

// Whether Reconnect has anything to restore: Reconnect heals the node's cut
// links as well as its isolation, as "restore every link" says.
export function cutOff(snapshot, id) {
  return Boolean(snapshot?.nodes?.find((n) => n.id === id)?.isolated) || cutLinks(snapshot, id).length > 0;
}

// A one-line description for toasts and the timeline.
export function describeFault(f, nodeName) {
  const n = (id) => (nodeName ? nodeName(id) : `#${id}`);
  switch (f.kind) {
    case "partition":
      return `cut ${n(f.a)} ↔ ${n(f.b)}`;
    case "heal":
      return `healed ${n(f.a)} ↔ ${n(f.b)}`;
    case "latency":
      return `latency ${n(f.a)} ↔ ${n(f.b)} = ${f.ms} ms`;
    case "loss":
      return `loss ${n(f.a)} ↔ ${n(f.b)} = ${(f.permille / 10).toFixed(1)}%`;
    case "cut_one_way":
      return `cut ${n(f.from)} → ${n(f.to)} (one way)`;
    case "heal_one_way":
      return `healed ${n(f.from)} → ${n(f.to)}`;
    case "clock_skew":
      return f.ms ? `clock of ${n(f.node)} skewed by ${f.ms > 0 ? "+" : ""}${f.ms} ms` : `clock skew of ${n(f.node)} removed`;
    case "disk":
      return f.mode === "ok" ? `disk of ${n(f.node)} ok` : `disk of ${n(f.node)}: ${f.mode}${f.mode === "slow" ? ` (${f.ms} ms per sync)` : ""}`;
    default:
      return `${f.kind} ${n(f.node)}`;
  }
}

export class FaultBar {
  constructor(container, hooks) {
    this.hooks = hooks; // onFault(fault), nodeName(id), onClear()
    this.selection = [];
    this.snapshot = null;
    this.root = el("div", "lab-faults");
    this.root.setAttribute("role", "group");
    this.root.setAttribute("aria-label", "Faults");
    this.nodeGroup = el("span", "lab-faults-group lab-faults-node");
    this.linkGroup = el("span", "lab-faults-group lab-faults-link");
    this.nodeCap = el("span", "lab-faults-cap", "Node");
    this.linkCap = el("span", "lab-faults-cap", "Link");
    this.nodeGroup.appendChild(this.nodeCap);
    this.linkGroup.appendChild(this.linkCap);
    this.hint = el("span", "lab-faults-hint", "Shift+click a second card for link faults");
    this.root.append(el("strong", "lab-faults-title", "Break things"), this.nodeGroup, this.hint, this.linkGroup);
    container.appendChild(this.root);
    this.buildNodeGroup();
    this.buildLinkGroup();
    this.update({ snapshot: null, selection: [] });
  }

  buildNodeGroup() {
    const g = this.nodeGroup;
    this.killBtn = button("Kill", "lab-btn-sm", () => this.node("kill"), { title: "Halt the node; its disk survives" });
    this.restartBtn = button("Restart", "lab-btn-sm", () => this.node("restart"), { title: "Boot again from the kept state" });
    this.wipeBtn = button("Wipe", "lab-btn-sm", () => this.node("wipe"), { title: "Boot again from nothing" });
    this.isolateBtn = button("Isolate", "lab-btn-sm", () => this.node("isolate"), { title: "Cut every link of the node" });
    this.reconnectBtn = button("Reconnect", "lab-btn-sm", () => this.node("reconnect"), { title: "Restore every link of the node" });
    this.pauseBtn = button("Pause", "lab-btn-sm", () => this.node(this.pauseBtn.dataset.fault), { data: { fault: "pause" } });
    g.append(this.killBtn, this.restartBtn, this.wipeBtn, this.isolateBtn, this.reconnectBtn, this.pauseBtn, this.buildMore());
  }

  // A "More…" disclosure (native <details>) whose panel floats under the bar,
  // closed by Escape or a click elsewhere; it keeps each group to one row.
  disclosure(title, children) {
    const details = el("details", "lab-faults-more");
    const summary = el("summary", "lab-btn lab-btn-sm", "More…");
    summary.title = title;
    const panel = el("div", "lab-faults-more-panel");
    panel.append(...children);
    details.append(summary, panel);
    details.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && details.open) {
        details.open = false;
        summary.focus();
      }
    });
    document.addEventListener("pointerdown", (e) => {
      if (details.open && !details.contains(e.target)) details.open = false;
    });
    return details;
  }

  // Clock skew and disk faults.
  buildMore() {
    this.skewInput = el("input", "lab-input lab-input-xs");
    Object.assign(this.skewInput, { type: "number", step: "100", value: "-5000" });
    this.skewInput.setAttribute("aria-label", "Clock skew in milliseconds");
    this.skewBtn = button("Set", "lab-btn-sm", () => this.skew(), { title: "Offset the node's wall clock (REALTIME); its monotonic clock is untouched. 0 removes the skew" });
    const skew = el("span", "lab-fault-num");
    skew.append("Clock skew", this.skewInput, "ms", this.skewBtn);
    this.diskSelect = select(DISK_MODES, "slow");
    this.diskSelect.setAttribute("aria-label", "Disk mode");
    this.diskMsInput = el("input", "lab-input lab-input-xs");
    Object.assign(this.diskMsInput, { type: "number", min: "0", step: "50", value: "200" });
    this.diskMsInput.setAttribute("aria-label", "Milliseconds per sync on a slow disk");
    this.diskSelect.addEventListener("change", () => (this.diskMsInput.disabled = this.diskSelect.value !== "slow" || this.diskSelect.disabled));
    this.diskBtn = button("Set", "lab-btn-sm", () => this.diskFault(), { title: "slow: every fsync takes this long; full: growing writes fail with ENOSPC; failing: writes and syncs fail with EIO" });
    const disk = el("span", "lab-fault-num");
    disk.append("Disk", this.diskSelect, this.diskMsInput, "ms", this.diskBtn);
    this.moreNote = el("p", "lab-faults-more-note", "Real brokers only: the page applies these to the broker's process and volume.");
    this.more = this.disclosure("Clock skew and disk faults", [skew, disk, this.moreNote]);
    return this.more;
  }

  /** Opens "More…" for the selected node (the canvas menu's route to skew and disk). */
  openMore() {
    this.more.open = true;
    this.skewInput.focus();
  }

  skew() {
    const [a] = this.selection;
    if (a == null) return;
    this.hooks.onFault(FAULT.clock_skew(a, Math.round(Number(this.skewInput.value) || 0)));
  }

  diskFault() {
    const [a] = this.selection;
    if (a == null) return;
    const ms = Math.max(0, Math.round(Number(this.diskMsInput.value) || 0));
    this.hooks.onFault(FAULT.disk(a, this.diskSelect.value, ms));
  }

  buildLinkGroup() {
    const g = this.linkGroup;
    this.partitionBtn = button("Partition", "lab-btn-sm", () => this.link("partition"), { title: "Cut the link both ways" });
    this.healBtn = button("Heal", "lab-btn-sm", () => this.link("heal"), { title: "Restore the link" });
    // One-way cuts: the arrow reads with the caption "A ↔ B". A cut direction turns into its heal.
    this.cutAB = button("Cut →", "lab-btn-sm", () => this.oneWay(false));
    this.cutBA = button("Cut ←", "lab-btn-sm", () => this.oneWay(true));
    this.latencyInput = el("input", "lab-input lab-input-xs");
    this.latencyInput.type = "number";
    this.latencyInput.min = "0";
    this.latencyInput.step = "1";
    this.latencyInput.value = "50";
    this.latencyInput.setAttribute("aria-label", "Link latency in milliseconds");
    this.latencyBtn = button("Set", "lab-btn-sm", () => this.link("latency"), { title: "Set the one-way latency of the link" });
    this.lossInput = el("input", "lab-input lab-input-xs");
    this.lossInput.type = "number";
    this.lossInput.min = "0";
    this.lossInput.max = "100";
    this.lossInput.step = "1";
    this.lossInput.value = "10";
    this.lossInput.setAttribute("aria-label", "Link loss in percent");
    this.lossBtn = button("Set", "lab-btn-sm", () => this.link("loss"), { title: "Drop this share of the data frames on the link" });
    this.resetBtn = button("Reset", "lab-btn-sm", () => this.resetLink(), { title: "Heal, no loss, default latency" });
    this.bytesBtn = button("Network bytes", "lab-btn-sm", () => this.hooks.onBrowseTraffic(), { title: "Inspect recent frames and payload bytes between these nodes" });
    const latency = el("span", "lab-fault-num");
    latency.append("Latency", this.latencyInput, "ms", this.latencyBtn);
    const loss = el("span", "lab-fault-num");
    loss.append("Loss", this.lossInput, "%", this.lossBtn);
    const reset = el("span", "lab-fault-num");
    reset.append(this.resetBtn, "heal both ways, no loss, default latency");
    this.linkMore = this.disclosure("Latency, loss and reset", [latency, loss, reset]);
    g.append(this.partitionBtn, this.healBtn, this.cutAB, this.cutBA, this.linkMore, this.bytesBtn);
  }

  update({ snapshot, selection }) {
    this.snapshot = snapshot;
    this.selection = selection || [];
    const [a, b] = this.selection;
    const name = (id) => this.hooks.nodeName(id);
    const nodeSnap = (id) => snapshot?.nodes?.find((n) => n.id === id);
    const one = a != null;
    const two = a != null && b != null;
    this.root.dataset.selected = String(this.selection.length);
    this.nodeGroup.dataset.active = String(one && !two);
    this.linkGroup.dataset.active = String(two);
    // Two nodes swap the node buttons for the link controls in the same row.
    this.nodeGroup.hidden = two;
    this.linkGroup.hidden = !two;
    this.hint.hidden = two;
    const nodeControls = [this.killBtn, this.restartBtn, this.wipeBtn, this.isolateBtn, this.reconnectBtn, this.pauseBtn, this.skewInput, this.skewBtn, this.diskSelect, this.diskMsInput, this.diskBtn];
    for (const btn of nodeControls) btn.disabled = !one || two;
    this.more.classList.toggle("lab-disabled", !one || two);
    if (!one || two) this.more.open = false;
    const paused = one && !two && Boolean(nodeSnap(a)?.paused);
    this.pauseBtn.dataset.fault = paused ? "resume" : "pause";
    this.pauseBtn.textContent = paused ? "Resume" : "Pause";
    this.pauseBtn.title = paused ? "Let the node run again: the frames that waited go in first" : "Stop the node like SIGSTOP or a long GC pause: no timers, frames for it wait";
    if (one && !two) {
      const s = nodeSnap(a);
      if (s) {
        this.killBtn.disabled = !s.alive;
        this.restartBtn.disabled = s.alive;
        this.isolateBtn.disabled = s.isolated;
        this.reconnectBtn.disabled = !cutOff(snapshot, a);
        this.pauseBtn.disabled = !s.alive;
        // Skew and disk act on a real broker's process; a simulated node only records them.
        const real = s.kind === "krabka-broker";
        for (const c of [this.skewInput, this.skewBtn, this.diskSelect, this.diskMsInput, this.diskBtn]) c.disabled = !real;
        this.moreNote.textContent = real
          ? `Now: clock ${s.skew_ms ? `${s.skew_ms > 0 ? "+" : ""}${s.skew_ms} ms` : "not skewed"}, disk ${s.disk || "ok"}. The page applies these to the broker's process and volume.`
          : "Real brokers only: the page applies these to the broker's process and volume.";
      }
      this.diskMsInput.disabled ||= this.diskSelect.value !== "slow";
    }
    for (const c of [this.partitionBtn, this.healBtn, this.cutAB, this.cutBA, this.latencyInput, this.latencyBtn, this.lossInput, this.lossBtn, this.resetBtn, this.bytesBtn]) c.disabled = !two;
    this.linkMore.classList.toggle("lab-disabled", !two);
    if (!two) this.linkMore.open = false;
    if (two) {
      const link = snapshot?.links?.find((l) => (l.a === a && l.b === b) || (l.a === b && l.b === a));
      const cut = Boolean(link?.cut);
      this.partitionBtn.disabled = cut;
      this.healBtn.disabled = !cut;
      const ab = oneWayCut(snapshot, a, b);
      const ba = oneWayCut(snapshot, b, a);
      this.cutAB.textContent = ab ? "Heal →" : "Cut →";
      this.cutBA.textContent = ba ? "Heal ←" : "Cut ←";
      this.cutAB.title = ab ? `Let frames from ${name(a)} to ${name(b)} through again` : `Drop frames from ${name(a)} to ${name(b)}; ${name(b)} to ${name(a)} still flows`;
      this.cutBA.title = ba ? `Let frames from ${name(b)} to ${name(a)} through again` : `Drop frames from ${name(b)} to ${name(a)}; ${name(a)} to ${name(b)} still flows`;
      this.cutAB.setAttribute("aria-label", `${ab ? "Heal" : "Cut"} ${name(a)} to ${name(b)}`);
      this.cutBA.setAttribute("aria-label", `${ba ? "Heal" : "Cut"} ${name(b)} to ${name(a)}`);
      this.cutAB.disabled = cut;
      this.cutBA.disabled = cut;
      const bits = [`${name(a)} ↔ ${name(b)}`];
      if (ab) bits.push(`→ cut`);
      if (ba) bits.push(`← cut`);
      if (link) {
        if (link.cut) bits.push("cut");
        bits.push(`${link.latency_ms} ms`);
        if (link.loss_permille) bits.push(`${(link.loss_permille / 10).toFixed(1)}% loss`);
      } else if (snapshot) bits.push(`${snapshot.default_latency_ms} ms`);
      this.linkCap.textContent = `Link · ${bits.join(" · ")}`;
      this.nodeCap.textContent = "Node";
    } else if (one) {
      const s = nodeSnap(a);
      const state = [name(a)];
      if (s?.paused) state.push("paused");
      if (s?.skew_ms) state.push(`skew ${s.skew_ms > 0 ? "+" : ""}${s.skew_ms} ms`);
      if (s?.disk && s.disk !== "ok") state.push(`disk ${s.disk}`);
      this.nodeCap.textContent = `Node · ${state.join(" · ")}`;
      this.linkCap.textContent = "Link";
    } else {
      this.nodeCap.textContent = "Node · click a card";
      this.linkCap.textContent = "Link";
    }
  }

  node(kind) {
    const [a] = this.selection;
    if (a == null) return;
    this.hooks.onFault(FAULT[kind](a));
  }

  link(kind) {
    const [a, b] = this.selection;
    if (a == null || b == null) return;
    if (kind === "latency") {
      const ms = Math.max(0, Math.round(Number(this.latencyInput.value) || 0));
      this.hooks.onFault(FAULT.latency(a, b, ms));
    } else if (kind === "loss") {
      const pct = Math.min(100, Math.max(0, Number(this.lossInput.value) || 0));
      this.hooks.onFault(FAULT.loss(a, b, Math.round(pct * 10)));
    } else this.hooks.onFault(FAULT[kind](a, b));
  }

  oneWay(reverse) {
    const [a, b] = this.selection;
    if (a == null || b == null) return;
    const [from, to] = reverse ? [b, a] : [a, b];
    this.hooks.onFault(oneWayCut(this.snapshot, from, to) ? FAULT.heal_one_way(from, to) : FAULT.cut_one_way(from, to));
  }

  resetLink() {
    const [a, b] = this.selection;
    if (a == null || b == null) return;
    const def = this.snapshot?.default_latency_ms ?? 5;
    this.hooks.onFault(FAULT.heal(a, b));
    if (oneWayCut(this.snapshot, a, b)) this.hooks.onFault(FAULT.heal_one_way(a, b));
    if (oneWayCut(this.snapshot, b, a)) this.hooks.onFault(FAULT.heal_one_way(b, a));
    this.hooks.onFault(FAULT.loss(a, b, 0));
    this.hooks.onFault(FAULT.latency(a, b, def));
  }
}
