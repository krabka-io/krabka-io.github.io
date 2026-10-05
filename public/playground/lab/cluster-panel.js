// The Cluster tab: what the admin's cluster observer last saw.
//
// The scenario's admin node polls the brokers every `observe_ms` (Metadata,
// DescribeQuorum, ListOffsets, the groups' offsets) and reports `cluster` in
// its snapshot (contract in `playground/src/lab/apps/admin.rs`). This tab
// draws it: the KRaft quorum with each voter's lag, the brokers with how many
// partitions each leads and hosts, and every partition with its leader, leader
// epoch, replicas, ISR (replicas out of sync struck through), high watermark,
// log start and each group's lag. Offline and under-replicated partitions sort
// first and are marked; internal topics hide behind a toggle.
//
// It also decorates the canvas (`decorateCanvas`): a "controller" badge on the
// quorum leader's card, a leads/hosts count on each broker card, and one chip
// per partition under a topic's pill, coloured by health, with its leader id.

import { el, svg, fmtMs, fmtNum, plural } from "./dom.js";
import { section, kv, table } from "./views.js";
import { clusterOf } from "./charts.js";

const isOffline = (p) => p.leader == null || p.leader < 0;
const isUnder = (p) => (p.isr || []).length < (p.replicas || []).length;

// Per broker: partitions it leads and partitions it holds a replica of.
export function brokerCounts(cluster) {
  const out = new Map();
  const get = (id) => out.get(id) || out.set(id, { leaders: 0, replicas: 0 }).get(id);
  for (const b of cluster?.brokers || []) get(b.id);
  for (const t of cluster?.topics || []) {
    for (const p of t.partitions || []) {
      if (!isOffline(p)) get(p.leader).leaders++;
      for (const r of p.replicas || []) get(r).replicas++;
    }
  }
  return out;
}

// One line on the cluster's health, for the inspector's overview. `level`:
// "ok" | "warn" | "err".
export function clusterHealth(cluster) {
  if (!cluster) return null;
  const parts = (cluster.topics || []).flatMap((t) => t.partitions || []);
  const offline = parts.filter(isOffline).length;
  const under = parts.filter((p) => !isOffline(p) && isUnder(p)).length;
  const fenced = (cluster.brokers || []).filter((b) => b.fenced).length;
  const bits = [];
  bits.push(cluster.controller != null ? `controller ${cluster.controller}` : "no controller");
  bits.push(`${plural((cluster.brokers || []).length, "broker")}${fenced ? `, ${fenced} fenced` : ""}`);
  bits.push(`${plural(parts.length, "partition")}${under ? `, ${under} under-replicated` : ""}${offline ? `, ${offline} offline` : ""}`);
  return { text: bits.join(" · "), level: offline || cluster.controller == null ? "err" : under || fenced ? "warn" : "ok" };
}

export class ClusterPanel {
  // hooks: nodeLabelForBroker(id), onSelectNode(id)
  constructor(container, hooks) {
    this.hooks = hooks;
    this.showInternal = false;
    this.key = "";
    this.root = el("div", "lab-cluster");
    const bar = el("div", "lab-charts-bar");
    const toggle = el("label", "lab-field-inline");
    this.internalBox = el("input");
    this.internalBox.type = "checkbox";
    this.internalBox.addEventListener("change", () => {
      this.showInternal = this.internalBox.checked;
      this.render(true);
    });
    toggle.append(this.internalBox, el("span", null, "Show internal topics"));
    this.status = el("span", "lab-muted lab-small");
    this.status.setAttribute("aria-live", "polite");
    bar.append(toggle, this.status);
    this.body = el("div", "lab-cluster-body");
    this.root.append(bar, this.body);
    container.appendChild(this.root);
    this.render(true);
  }

  update(snapshot) {
    this.cluster = clusterOf(snapshot);
    this.render(false);
  }

  render(force) {
    const c = this.cluster;
    const key = c ? `${c.at}|${this.showInternal}` : "none";
    if (!force && (key === this.key || this.root.offsetParent === null)) return;
    this.key = key;
    const body = el("div", "lab-cluster-body");
    if (!c) {
      this.status.textContent = "";
      body.appendChild(el("p", "lab-muted lab-small", "No cluster report yet. The scenario's admin node polls the brokers once a lab second and reports here; a lab module without the cluster observer reports nothing."));
      this.body.replaceWith(body);
      this.body = body;
      return;
    }
    const label = (id) => (id == null || id < 0 ? "none" : this.hooks.nodeLabelForBroker(id));
    this.status.textContent = `polled at ${fmtMs(c.at)}${c.cluster_id ? ` · cluster ${c.cluster_id}` : ""}${c.errors?.length ? ` · ${c.errors.join("; ")}` : ""}`;

    const q = c.quorum;
    if (q) {
      const box = el("div");
      box.appendChild(kv([
        { label: "leader", value: label(q.leader), key: "quorum_leader" },
        { label: "epoch", value: String(q.epoch ?? "–"), key: "quorum_epoch" },
        { label: "high watermark", value: q.high_watermark == null ? "–" : fmtNum(q.high_watermark), key: "quorum_hwm" },
      ]));
      const voters = [...(q.voters || []).map((v) => ({ ...v, role: v.id === q.leader ? "leader" : "voter" })), ...(q.observers || []).map((v) => ({ ...v, role: "observer" }))];
      box.appendChild(table([
        { key: "id", label: "broker", render: (v) => label(v) },
        { key: "role", label: "role" },
        { key: "log_end_offset", label: "log end", render: (v) => (v == null ? "–" : fmtNum(v)) },
        { key: "lag", label: "lag", render: (v) => (v == null ? "–" : fmtNum(v)) },
      ], voters, { rowKey: (v) => `q${v.id}` }));
      body.appendChild(section("KRaft quorum", box));
    } else body.appendChild(el("p", "lab-muted lab-small", "The quorum was not described in the last poll."));

    const counts = brokerCounts(c);
    const brokers = (c.brokers || []).map((b) => ({ ...b, ...counts.get(b.id) }));
    const bt = table([
      { key: "id", label: "broker", render: (v) => label(v) },
      { key: "rack", label: "rack", render: (v) => v ?? "–" },
      { key: "fenced", label: "fenced", render: (v) => (v ? "yes" : "no") },
      { key: "leaders", label: "leads" },
      { key: "replicas", label: "hosts" },
      { key: "controller", label: "", get: (b) => (b.id === c.controller ? "controller" : "") },
    ], brokers, { rowKey: (b) => `b${b.id}` });
    brokers.forEach((b, i) => bt.querySelectorAll("tbody tr")[i]?.classList.toggle("lab-cl-warn", Boolean(b.fenced)));
    body.appendChild(section(`Brokers (${brokers.length})`, bt));

    // Each group's lag on each partition.
    const lagOf = new Map();
    for (const g of c.groups || []) for (const o of g.offsets || []) {
      const k = `${o.topic}-${o.partition}`;
      lagOf.set(k, [...(lagOf.get(k) || []), `${g.id} ${o.lag ?? "–"}`]);
    }
    const all = (c.topics || []).flatMap((t) => (t.partitions || []).map((p) => ({ ...p, topic: t.name, internal: Boolean(t.internal) })));
    const shown = all.filter((p) => this.showInternal || !p.internal);
    const rank = (p) => (isOffline(p) ? 0 : isUnder(p) ? 1 : 2);
    shown.sort((a, b) => rank(a) - rank(b) || a.topic.localeCompare(b.topic) || a.partition - b.partition);
    const isrCell = (p) => {
      const span = el("span", "lab-cl-isr");
      for (const r of p.replicas || []) {
        const inSync = (p.isr || []).includes(r);
        const s = el("span", inSync ? "lab-cl-in" : "lab-cl-out", String(r));
        s.title = inSync ? `broker ${r} is in sync` : `broker ${r} is out of sync`;
        span.appendChild(s);
      }
      return span;
    };
    const pt = table([
      { key: "topic", label: "topic" },
      { key: "partition", label: "p" },
      { key: "leader", label: "leader", render: (v) => (v == null || v < 0 ? "none" : String(v)) },
      { key: "leader_epoch", label: "epoch", render: (v) => (v == null ? "–" : String(v)) },
      { key: "replicas", label: "replicas", render: (v) => (v || []).join(", ") },
      { key: "isr", label: "ISR", render: (_, p) => isrCell(p) },
      { key: "offline", label: "offline", render: (v) => (v?.length ? v.join(", ") : "–") },
      { key: "hwm", label: "HWM", render: (v) => (v == null ? "–" : fmtNum(v)) },
      { key: "log_start", label: "log start", render: (v) => (v == null ? "–" : fmtNum(v)) },
      { key: "lag", label: "lag", get: (p) => (lagOf.get(`${p.topic}-${p.partition}`) || []).join(", ") || "–" },
    ], shown, { rowKey: (p) => `${p.topic}-${p.partition}` });
    shown.forEach((p, i) => {
      const tr = pt.querySelectorAll("tbody tr")[i];
      tr?.classList.toggle("lab-cl-err", isOffline(p));
      tr?.classList.toggle("lab-cl-warn", !isOffline(p) && isUnder(p));
    });
    const hidden = all.length - shown.length;
    const under = all.filter((p) => !isOffline(p) && isUnder(p)).length;
    const offline = all.filter(isOffline).length;
    body.appendChild(section(`Partitions (${shown.length}${hidden ? `, ${hidden} internal hidden` : ""}${under ? ` · ${under} under-replicated` : ""}${offline ? ` · ${offline} offline` : ""})`, pt));
    // Keep the sections the reader folded.
    const was = new Map([...this.body.querySelectorAll("details.lab-sec")].map((d) => [d.firstChild.textContent.split(" (")[0], d.open]));
    for (const d of body.querySelectorAll("details.lab-sec")) {
      const k = d.firstChild.textContent.split(" (")[0];
      if (was.has(k)) d.open = was.get(k);
    }
    this.body.replaceWith(body);
    this.body = body;
  }
}

// ---- the canvas ----

const CHIP_W = 15;
const CHIP_MAX = 8;

// Adds the cluster's facts to the canvas's broker cards and topic pills, in a
// group of its own on each, redrawn only when they change.
export function decorateCanvas(canvas, cluster) {
  const counts = cluster ? brokerCounts(cluster) : new Map();
  for (const [id, entry] of canvas.nodeEls) {
    const g = entry.g;
    const isBroker = g.dataset.kind === "krabka-broker";
    const n = counts.get(id);
    const ctl = cluster && cluster.controller === id;
    const key = isBroker && cluster ? `${ctl}|${n?.leaders}|${n?.replicas}|${entry.w}|${entry.h}` : "";
    if (g.dataset.clusterKey === key) continue;
    g.dataset.clusterKey = key;
    g.querySelector(":scope > .lab-cl-deco")?.remove();
    if (!key) continue;
    const deco = svg("g", { class: "lab-cl-deco" });
    if (ctl) {
      const b = svg("g", { class: "lab-cl-ctl", transform: `translate(${-entry.w / 2 + 8}, ${-entry.h / 2 - 7})` });
      b.append(svg("rect", { class: "lab-badge lab-cl-ctl-badge", x: 0, y: 0, width: 62, height: 15, rx: 7 }), Object.assign(svg("text", { class: "lab-badge-text", x: 31, y: 11, "text-anchor": "middle" }), { textContent: "controller" }));
      deco.appendChild(b);
    }
    if (n) {
      const text = `leads ${n.leaders} of ${n.replicas}`;
      const w = text.length * 5.6 + 10;
      const c = svg("g", { class: "lab-cl-count", transform: `translate(${entry.w / 2 - 8 - w}, ${entry.h / 2 - 8})` });
      const title = svg("title");
      title.textContent = `Leader of ${plural(n.leaders, "partition")}; holds a replica of ${plural(n.replicas, "partition")}`;
      c.append(title, svg("rect", { class: "lab-badge lab-cl-count-badge", x: 0, y: 0, width: w, height: 15, rx: 7 }), Object.assign(svg("text", { class: "lab-badge-text", x: w / 2, y: 11, "text-anchor": "middle" }), { textContent: text }));
      deco.appendChild(c);
    }
    g.appendChild(deco);
  }
  const topics = new Map((cluster?.topics || []).map((t) => [t.name, t]));
  for (const [name, g] of canvas.topicEls) {
    const parts = (topics.get(name)?.partitions || []).slice().sort((a, b) => a.partition - b.partition);
    const key = parts.map((p) => `${p.partition}:${p.leader}:${isOffline(p) ? "x" : isUnder(p) ? "u" : "k"}`).join(",");
    if (g.dataset.clusterKey === key) continue;
    g.dataset.clusterKey = key;
    g.querySelector(":scope > .lab-cl-deco")?.remove();
    if (!parts.length) continue;
    const bg = g.querySelector(".lab-topic-bg");
    const h = Number(bg?.getAttribute("height")) || 36;
    const shown = parts.slice(0, CHIP_MAX);
    const total = shown.length * (CHIP_W + 2) - 2 + (parts.length > CHIP_MAX ? 26 : 0);
    const deco = svg("g", { class: "lab-cl-deco", transform: `translate(${-total / 2}, ${h / 2 + 3})` });
    shown.forEach((p, i) => {
      const state = isOffline(p) ? "err" : isUnder(p) ? "warn" : "ok";
      const chip = svg("g", { class: `lab-cl-chip lab-cl-chip-${state}`, transform: `translate(${i * (CHIP_W + 2)}, 0)` });
      const title = svg("title");
      title.textContent = `${name}-${p.partition}: ${isOffline(p) ? "offline, no leader" : `leader ${p.leader}, ISR ${(p.isr || []).join(",")} of ${(p.replicas || []).join(",")}`}`;
      chip.append(title, svg("rect", { x: 0, y: 0, width: CHIP_W, height: 13, rx: 3 }), Object.assign(svg("text", { x: CHIP_W / 2, y: 10, "text-anchor": "middle" }), { textContent: isOffline(p) ? "–" : String(p.leader) }));
      deco.appendChild(chip);
    });
    if (parts.length > CHIP_MAX) deco.appendChild(Object.assign(svg("text", { class: "lab-cl-more", x: shown.length * (CHIP_W + 2) + 2, y: 10 }), { textContent: `+${parts.length - CHIP_MAX}` }));
    g.appendChild(deco);
  }
}
