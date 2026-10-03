// The fault toolbar above the canvas. It is always on screen, so a reader
// sees at once that the cluster can be broken; its buttons wake up as nodes
// are selected.
//
// One selected node offers kill / restart / wipe / isolate / reconnect; two
// selected nodes (Shift+click the second) offer partition / heal and the
// link's latency and loss. Every button builds the `Fault` JSON the crate's
// serde derive reads: `{"kind":"kill","node":1}`,
// `{"kind":"partition","a":1,"b":2}`, `{"kind":"latency","a":1,"b":2,"ms":50}`,
// `{"kind":"loss","a":1,"b":2,"permille":100}`.

import { el, button } from "./dom.js";

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
};

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
    this.hint = el("span", "lab-faults-hint", "Pick a second node: link controls");
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
    g.append(this.killBtn, this.restartBtn, this.wipeBtn, this.isolateBtn, this.reconnectBtn);
  }

  buildLinkGroup() {
    const g = this.linkGroup;
    this.partitionBtn = button("Partition", "lab-btn-sm", () => this.link("partition"), { title: "Cut the link both ways" });
    this.healBtn = button("Heal", "lab-btn-sm", () => this.link("heal"), { title: "Restore the link" });
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
    g.append(this.partitionBtn, this.healBtn, latency, loss, this.resetBtn, this.bytesBtn);
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
    for (const btn of [this.killBtn, this.restartBtn, this.wipeBtn, this.isolateBtn, this.reconnectBtn]) btn.disabled = !one || two;
    if (one && !two) {
      const s = nodeSnap(a);
      if (s) {
        this.killBtn.disabled = !s.alive;
        this.restartBtn.disabled = s.alive;
        this.isolateBtn.disabled = s.isolated;
        this.reconnectBtn.disabled = !s.isolated;
      }
    }
    for (const c of [this.partitionBtn, this.healBtn, this.latencyInput, this.latencyBtn, this.lossInput, this.lossBtn, this.resetBtn, this.bytesBtn]) c.disabled = !two;
    if (two) {
      const link = snapshot?.links?.find((l) => (l.a === a && l.b === b) || (l.a === b && l.b === a));
      const cut = Boolean(link?.cut);
      this.partitionBtn.disabled = cut;
      this.healBtn.disabled = !cut;
      const bits = [`${name(a)} ↔ ${name(b)}`];
      if (link) {
        if (link.cut) bits.push("cut");
        bits.push(`${link.latency_ms} ms`);
        if (link.loss_permille) bits.push(`${(link.loss_permille / 10).toFixed(1)}% loss`);
      } else if (snapshot) bits.push(`${snapshot.default_latency_ms} ms`);
      this.linkCap.textContent = `Link · ${bits.join(" · ")}`;
      this.nodeCap.textContent = "Node";
    } else if (one) {
      this.nodeCap.textContent = `Node · ${name(a)}`;
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

  resetLink() {
    const [a, b] = this.selection;
    if (a == null || b == null) return;
    const def = this.snapshot?.default_latency_ms ?? 5;
    this.hooks.onFault(FAULT.heal(a, b));
    this.hooks.onFault(FAULT.loss(a, b, 0));
    this.hooks.onFault(FAULT.latency(a, b, def));
  }
}
