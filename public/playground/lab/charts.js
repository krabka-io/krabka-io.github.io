// The Charts tab: a time-series strip over the run, on the lab clock.
//
// `Sampler` takes one row per lab second from the world snapshot and the
// network capture (pure, no DOM, so the plain-node check runs it):
//
//   produce, consume   records/s: acked records summed over the producers,
//                      processed records summed over the consumers
//   lag                total consumer lag, summed over the consumers
//   produce_p50/p99    RTT of the Produce exchanges answered in that second
//   fetch_p50/p99      the same for Fetch (consumers and replicas alike)
//   min_isr, urp       smallest ISR and the count of under-replicated
//                      partitions, from the admin's cluster observer
//
// `ChartsPanel` draws the rows as small SVG charts on one lab-time axis, the
// fault markers (world `fault` events: kill, restart, partition, …) and the
// invariant violations as labelled vertical lines on every chart, and a
// crosshair that reads every chart at the hovered instant. "Pin as baseline"
// keeps the rows (in memory and in localStorage); a later run draws them
// dashed, aligned on lab time. "Rerun" opens the scenario again from nothing
// with its current configuration, so one knob can be changed and compared.

import { el, button, fmtMs, svg } from "./dom.js";
import { percentile } from "./capture.js";
import { CHECKS } from "./invariants.js";

export const CHARTS = [
  { id: "throughput", title: "Throughput", unit: "records/s", lines: [{ key: "produce", label: "produced (acked)" }, { key: "consume", label: "consumed" }] },
  { id: "lag", title: "Consumer lag", unit: "records", lines: [{ key: "lag", label: "total lag" }] },
  { id: "produce_rtt", title: "Produce RTT", unit: "ms", lines: [{ key: "produce_p50", label: "p50" }, { key: "produce_p99", label: "p99" }] },
  { id: "fetch_rtt", title: "Fetch RTT", unit: "ms", lines: [{ key: "fetch_p50", label: "p50" }, { key: "fetch_p99", label: "p99" }] },
  { id: "isr", title: "Smallest ISR", unit: "", lines: [{ key: "min_isr", label: "min ISR size" }] },
  { id: "urp", title: "Under-replicated partitions", unit: "", lines: [{ key: "urp", label: "partitions" }] },
];
// The site's chart colours (src/utils/benchmark-chart.mjs).
const COLORS = ["#ff8063", "#38bdf8", "#c4a0ff"];
const MAX_ROWS = 3600;
// Exchanges answered this long after their request are not looked for.
const RTT_WINDOW_MS = 60_000;
const BASELINE_KEY = "krabka-lab.chart-baseline";

const sum = (list) => list.reduce((s, v) => s + v, 0);

// The cluster the admin's observer last reported, or null.
export function clusterOf(snapshot) {
  for (const n of snapshot?.nodes || []) if (n.state?.cluster) return n.state.cluster;
  return null;
}

export class Sampler {
  constructor({ maxRows = MAX_ROWS } = {}) {
    this.maxRows = maxRows;
    this.reset();
  }

  reset() {
    this.rows = [];
    this.last = null; // { t, acked, processed }
  }

  // Takes a row when the snapshot is in a later lab second than the last row.
  // Returns the row, or null.
  sample(snapshot, capture) {
    if (!snapshot?.nodes) return null;
    const t = snapshot.now ?? 0;
    if (this.last && Math.floor(t / 1000) <= Math.floor(this.last.t / 1000)) return null;
    const of = (kind, field) => sum(snapshot.nodes.filter((n) => n.kind === kind && typeof n.state?.[field] === "number").map((n) => n.state[field]));
    const acked = of("producer", "acked");
    const processed = of("consumer", "processed");
    const consumers = snapshot.nodes.filter((n) => n.kind === "consumer" && typeof n.state?.lag === "number");
    const row = { t, produce: null, consume: null, lag: consumers.length ? sum(consumers.map((n) => n.state.lag)) : null };
    if (this.last) {
      const dt = (t - this.last.t) / 1000;
      // A restarted node counts from zero again: no negative rates.
      row.produce = Math.max(0, acked - this.last.acked) / dt;
      row.consume = Math.max(0, processed - this.last.processed) / dt;
    }
    Object.assign(row, rtts(capture, this.last ? this.last.t : -Infinity, t));
    const c = clusterOf(snapshot);
    const parts = (c?.topics || []).flatMap((tp) => tp.partitions || []);
    row.min_isr = parts.length ? Math.min(...parts.map((p) => (p.isr || []).length)) : null;
    row.urp = parts.length ? parts.filter((p) => (p.isr || []).length < (p.replicas || []).length).length : null;
    this.last = { t, acked, processed };
    this.rows.push(row);
    if (this.rows.length > this.maxRows) this.rows.shift();
    return row;
  }
}

// RTT percentiles of the Produce (0) and Fetch (1) exchanges whose response
// reached the client in (from, to].
export function rtts(capture, from, to) {
  const out = { produce_p50: null, produce_p99: null, fetch_p50: null, fetch_p99: null };
  const xs = capture?.exchanges;
  if (!xs?.length) return out;
  const got = { 0: [], 1: [] };
  for (let i = xs.length - 1; i >= 0; i--) {
    const ex = xs[i];
    if (ex.req.at < from - RTT_WINDOW_MS) break;
    if (!ex.resp || !(ex.apiKey in got) || ex.resp.deliverAt <= from || ex.resp.deliverAt > to) continue;
    got[ex.apiKey].push(ex.rtt);
  }
  for (const [key, name] of [[0, "produce"], [1, "fetch"]]) {
    const s = got[key].sort((a, b) => a - b);
    out[`${name}_p50`] = percentile(s, 50);
    out[`${name}_p99`] = percentile(s, 99);
  }
  return out;
}

// Markers for the charts from world events: every fault, kill and restart included.
export function faultMarkers(events, nodeName = (id) => `#${id}`) {
  const out = [];
  for (const e of events || []) {
    if (e.kind !== "fault" || !e.detail) continue;
    const f = e.detail;
    const who = [f.node, f.a, f.from].filter((v) => v != null).map(nodeName);
    const other = [f.b, f.to].filter((v) => v != null).map(nodeName);
    out.push({ t: e.at, kind: "fault", label: `${String(f.kind).replace(/_/g, " ")} ${who.join("")}${other.length ? `–${other.join("")}` : ""}`.trim() });
  }
  return out;
}

// A round axis maximum: 1, 2 or 5 times a power of ten.
function niceMax(v) {
  if (!(v > 0)) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 5, 10].map((m) => m * p).find((m) => m >= v);
}

function fmtValue(v, unit) {
  if (v == null || !Number.isFinite(v)) return "–";
  const text = Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(v >= 10_000 ? 0 : 1)}k` : Number.isInteger(v) ? String(v) : v.toFixed(v < 10 ? 1 : 0);
  return unit === "ms" ? `${text} ms` : text;
}

function loadBaseline() {
  try {
    const raw = localStorage.getItem(BASELINE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

export class ChartsPanel {
  // hooks: onRerun(), onSelectNode(id), scenarioName() → text
  constructor(container, { sampler, hooks = {} }) {
    this.sampler = sampler;
    this.hooks = hooks;
    this.markers = [];
    this.violations = [];
    this.baseline = loadBaseline();
    this.hoverT = null;
    this.compact = false;

    this.root = el("div", "lab-charts");
    const bar = el("div", "lab-charts-bar");
    this.pinBtn = button("Pin as baseline", "lab-btn-sm", () => this.pin(), { title: "Keep this run's series to compare the next run against" });
    this.clearBtn = button("Clear baseline", "lab-btn-sm", () => this.setBaseline(null));
    const rerun = button("Rerun", "lab-btn-sm lab-primary", () => hooks.onRerun?.(), { title: "Open the scenario again from nothing, with its current configuration" });
    const compact = el("label", "lab-field-inline");
    this.compactBox = el("input");
    this.compactBox.type = "checkbox";
    this.compactBox.addEventListener("change", () => {
      this.compact = this.compactBox.checked;
      this.root.classList.toggle("lab-charts-compact", this.compact);
      this.render();
    });
    compact.append(this.compactBox, el("span", null, "Compact strip"));
    this.baseNote = el("span", "lab-muted lab-small");
    bar.append(this.pinBtn, this.clearBtn, rerun, compact, this.baseNote);

    this.inv = el("details", "lab-sec lab-inv");
    this.inv.open = true;
    this.invTitle = el("summary", "lab-sec-title", "Invariants");
    this.invTitle.title = "Checked on every snapshot. A violation is listed here with its lab time and drawn as a red line on the charts.";
    this.invBody = el("div", "lab-inv-body");
    this.inv.append(this.invTitle, this.invBody);

    this.grid = el("div", "lab-charts-grid");
    this.charts = CHARTS.map((def) => {
      const box = el("figure", "lab-chart");
      box.dataset.chart = def.id;
      const head = el("figcaption", "lab-chart-head");
      const title = el("strong", null, def.title);
      const legend = el("span", "lab-chart-legend");
      def.lines.forEach((l, i) => {
        const sw = el("span", "lab-chart-swatch");
        sw.style.background = COLORS[i];
        legend.append(sw, el("span", null, l.label));
      });
      const read = el("span", "lab-chart-read");
      head.append(title, legend, read);
      const plot = svg("svg", { class: "lab-chart-svg", role: "img", "aria-label": `${def.title} over lab time` });
      plot.addEventListener("pointermove", (e) => this.hover(e, plot));
      plot.addEventListener("pointerleave", () => this.setHover(null));
      box.append(head, plot);
      this.grid.appendChild(box);
      return { def, box, read, plot, geom: null };
    });
    this.root.append(bar, this.grid, this.inv);
    container.appendChild(this.root);
    this.renderInvariants();
    this.render();
  }

  pin() {
    if (!this.sampler.rows.length) return;
    this.setBaseline({ name: this.hooks.scenarioName?.() || "", pinned: new Date().toISOString(), rows: this.sampler.rows.slice(), markers: this.markers.slice() });
  }

  setBaseline(b) {
    this.baseline = b;
    try {
      if (b) localStorage.setItem(BASELINE_KEY, JSON.stringify(b));
      else localStorage.removeItem(BASELINE_KEY);
    } catch {
      // Kept for this page only.
    }
    this.render();
  }

  setMarkers(markers) {
    this.markers = markers;
  }

  setViolations(list) {
    this.violations = list;
    this.renderInvariants();
  }

  shown() {
    return this.root.offsetParent !== null;
  }

  renderInvariants() {
    const v = this.violations;
    this.invTitle.textContent = v.length ? `Invariants · ${v.length} violated` : "Invariants · holding";
    this.inv.dataset.ok = String(!v.length);
    const body = el("div", "lab-inv-body");
    const list = el("ul", "lab-inv-checks");
    for (const [key, label] of Object.entries(CHECKS)) {
      const n = v.filter((x) => x.check === key).length;
      const li = el("li", n ? "lab-inv-bad" : "lab-inv-good");
      li.dataset.check = key;
      li.append(el("span", "lab-inv-mark", n ? "✗" : "✓"), el("span", null, n ? `${label} (${n})` : label));
      list.appendChild(li);
    }
    body.appendChild(list);
    if (v.length) {
      const rows = el("ol", "lab-inv-list");
      for (const x of v.slice(-50)) {
        const li = el("li");
        li.dataset.violation = x.check;
        li.append(el("span", "lab-inv-at", fmtMs(x.at)), el("span", null, x.text));
        if (x.node != null && this.hooks.onSelectNode) li.appendChild(button("Select", "lab-btn-sm", () => this.hooks.onSelectNode(x.node)));
        rows.appendChild(li);
      }
      body.appendChild(rows);
    }
    this.invBody.replaceWith(body);
    this.invBody = body;
  }

  // ---- drawing ----

  render() {
    const rows = this.sampler.rows;
    const base = this.baseline?.rows || [];
    const last = (r) => (r.length ? r[r.length - 1].t : 0);
    this.domain = [0, Math.max(10_000, last(rows), last(base))];
    this.pinBtn.disabled = !rows.length;
    this.clearBtn.disabled = !this.baseline;
    this.baseNote.textContent = this.baseline ? `Baseline: ${this.baseline.name || "a run"}, ${fmtMs(last(base))}, dashed` : "No baseline pinned";
    const marks = [
      ...(this.baseline?.markers || []).map((mk) => ({ ...mk, kind: "base", label: `base: ${mk.label}` })),
      ...this.markers,
      ...this.violations.map((x) => ({ t: x.at, kind: "violation", label: CHECKS[x.check] || x.check })),
    ];
    for (const c of this.charts) this.drawChart(c, rows, base, marks);
    this.renderReadout();
  }

  drawChart(c, rows, base, marks) {
    const W = 360;
    const H = this.compact ? 64 : 150;
    const m = this.compact ? { l: 6, r: 6, t: 4, b: 4 } : { l: 40, r: 8, t: 14, b: 18 };
    const keys = c.def.lines.map((l) => l.key);
    const values = [...rows, ...base].flatMap((r) => keys.map((k) => r[k])).filter((v) => v != null && Number.isFinite(v));
    const yMax = niceMax(Math.max(0, ...values));
    const [t0, t1] = this.domain;
    const x = (t) => m.l + ((t - t0) / (t1 - t0)) * (W - m.l - m.r);
    const y = (v) => H - m.b - (v / yMax) * (H - m.t - m.b);
    c.geom = { x, m, W, H, t0, t1 };
    const parts = [];
    const plot = c.plot;
    plot.setAttribute("viewBox", `0 0 ${W} ${H}`);
    plot.replaceChildren();
    if (!this.compact) {
      for (const f of [0, 0.5, 1]) {
        const v = yMax * f;
        parts.push(svg("line", { class: "lab-chart-grid", x1: m.l, x2: W - m.r, y1: y(v), y2: y(v) }));
        const lab = svg("text", { class: "lab-chart-tick", x: m.l - 4, y: y(v) + 3, "text-anchor": "end" });
        lab.textContent = fmtValue(v, "");
        parts.push(lab);
      }
      const step = niceMax((t1 - t0) / 4);
      for (let t = 0; t <= t1; t += step) {
        const lab = svg("text", { class: "lab-chart-tick", x: x(t), y: H - 4, "text-anchor": "middle" });
        lab.textContent = `${Math.round(t / 1000)} s`;
        parts.push(lab);
      }
    }
    // A label goes on the first row its left end clears; close markers stack.
    const rowEnds = [];
    for (const mk of marks.slice().sort((p, q) => p.t - q.t)) {
      if (mk.t < t0 || mk.t > t1) continue;
      const g = svg("g", { class: `lab-chart-marker lab-chart-marker-${mk.kind}` });
      g.appendChild(svg("line", { x1: x(mk.t), x2: x(mk.t), y1: m.t - (this.compact ? 0 : 10), y2: H - m.b }));
      if (!this.compact) {
        const text = mk.label.length > 24 ? `${mk.label.slice(0, 23)}…` : mk.label;
        let row = rowEnds.findIndex((end) => x(mk.t) > end);
        if (row < 0) row = rowEnds.length;
        rowEnds[row] = x(mk.t) + 6 + text.length * 4.4;
        const lab = svg("text", { x: x(mk.t) + 3, y: m.t - 3 + row * 10 });
        lab.textContent = text;
        g.appendChild(lab);
      }
      const tip = svg("title");
      tip.textContent = `${fmtMs(mk.t)}: ${mk.label}`;
      g.appendChild(tip);
      parts.push(g);
    }
    const path = (data, key) => {
      let d = "";
      let pen = false;
      for (const r of data) {
        const v = r[key];
        if (v == null || !Number.isFinite(v)) {
          pen = false;
          continue;
        }
        d += `${pen ? "L" : "M"}${x(r.t).toFixed(1)},${y(v).toFixed(1)}`;
        pen = true;
      }
      return d;
    };
    c.def.lines.forEach((l, i) => {
      if (base.length) parts.push(svg("path", { class: "lab-chart-line lab-chart-base", d: path(base, l.key), stroke: COLORS[i] }));
      parts.push(svg("path", { class: "lab-chart-line", d: path(rows, l.key), stroke: COLORS[i] }));
    });
    c.cross = svg("line", { class: "lab-chart-cross", y1: m.t, y2: H - m.b, x1: 0, x2: 0, visibility: "hidden" });
    parts.push(c.cross);
    plot.append(...parts);
  }

  hover(e, plot) {
    const c = this.charts.find((ch) => ch.plot === plot);
    if (!c?.geom) return;
    const box = plot.getBoundingClientRect();
    const { m, W, t0, t1 } = c.geom;
    const px = ((e.clientX - box.left) / box.width) * W;
    const t = t0 + ((px - m.l) / (W - m.l - m.r)) * (t1 - t0);
    this.setHover(t >= t0 && t <= t1 ? t : null);
  }

  setHover(t) {
    this.hoverT = t;
    for (const c of this.charts) {
      if (!c.cross || !c.geom) continue;
      c.cross.setAttribute("visibility", t == null ? "hidden" : "visible");
      if (t != null) {
        const px = c.geom.x(t);
        c.cross.setAttribute("x1", px);
        c.cross.setAttribute("x2", px);
      }
    }
    this.renderReadout();
  }

  // Each chart's values at the hovered instant, or the latest ones.
  renderReadout() {
    const t = this.hoverT;
    const near = (rows) => {
      if (!rows?.length) return null;
      if (t == null) return rows[rows.length - 1];
      let best = rows[0];
      for (const r of rows) if (Math.abs(r.t - t) < Math.abs(best.t - t)) best = r;
      return Math.abs(best.t - t) <= 1500 ? best : null;
    };
    const row = near(this.sampler.rows);
    const base = near(this.baseline?.rows);
    for (const c of this.charts) {
      const at = t ?? row?.t;
      const vals = c.def.lines.map((l) => `${l.label} ${fmtValue(row?.[l.key], c.def.unit)}${base ? ` (base ${fmtValue(base[l.key], c.def.unit)})` : ""}`);
      c.read.textContent = at == null ? "no samples yet" : `${fmtMs(at)}: ${vals.join(" · ")}`;
      c.read.title = c.read.textContent;
    }
  }
}
