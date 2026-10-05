// The SVG canvas: nodes as draggable cards, the edges the configs imply,
// topics as pills between producers and consumers (a streams app's changelog
// and repartition topics as dashed pills beside it), frames in flight as
// dots that slide along the wire, and the overlays of cut, slow and lossy
// links.
//
// Two or more brokers sit in a cluster frame, and a client's connections to
// them are one edge to the frame. A line between two brokers shows only while
// frames flow on it. "All connections" draws every client-to-broker edge.
//
// Coordinates: every node has a scenario position `(x, y)` in canvas units;
// the viewport transform (pan `x`, `y` and zoom `k`) maps them to pixels.
// Drag on a card moves it and reports the new position once, on release;
// drag on empty space pans; the wheel zooms around the pointer.
//
// The canvas keeps one `<g>` per node, topic and edge and updates it in
// place, so the reader's focus and the hover state survive snapshots.

import { svg, el, button, setAttrs, clamp, plural } from "./dom.js";
import { kindOf, derivedEdges, topicNames, internalTopics, statusLine } from "./kinds.js";

const CARD_W = 184;
const CARD_H = 64;
const GHOST_W = 132;
const GHOST_H = 36;
const TOPIC_W = 148;
const TOPIC_H = 36;
const MAX_DOTS = 200;
const MAX_LABELLED_DOTS = 40;
const DRAG_THRESHOLD = 4;
const LONG_PRESS_MS = 500;
const MIN_ZOOM = 0.25;
const MAX_ZOOM = 2.5;
// Fit never zooms out below this: card text stays near 10px or more, and a
// graph wider than the box is panned instead (the Fit button still shows all).
const MIN_FIT_ZOOM = 0.8;
// Edges drawn faint: connections and a streams app's internal topics, not
// the data flow of the scenario's own topics.
const FAINT_EDGES = new Set(["bootstrap", "ping", "registry", "changelog", "repartition"]);
const BROKER_KIND = "krabka-broker";
// Room around the brokers inside the cluster frame; the top holds its label
// above the cards' badges.
const FRAME_PAD = 18;
const FRAME_PAD_TOP = 38;
// A broker-to-broker line stays this many lab milliseconds after its last frame.
const ACTIVE_MS = 1200;

export class Canvas {
  // hooks: onSelect(id, { additive }), onDeselect(), onMove(id, x, y),
  // onCommand(id, command), menuItems(id) → [{ label, command, disabled }],
  // nodeName(id), peerName(peerId), onViewChange()
  constructor(container, hooks) {
    this.hooks = hooks;
    this.view = { x: 40, y: 40, k: 1 };
    this.positions = new Map();
    this.topicPositions = new Map();
    this.nodeEls = new Map();
    this.topicEls = new Map();
    this.edgeEls = new Map();
    this.linkEls = new Map();
    // Broker pairs → lab time of their last frame, and the frames delivered per
    // direction at the last snapshot.
    this.activity = new Map();
    this.delivered = null;
    this.activeEls = new Map();
    this.allEdges = false;
    this.frame = null;
    this.dotPool = [];
    this.inFlight = [];
    this.latencies = new Map();
    this.defaultLatency = 5;
    this.snapshot = null;
    this.scenario = null;
    this.selection = [];
    this.userMovedView = false;
    this.drag = null;
    this.longPress = null;
    // Fingers on the canvas, and the two-finger gesture they make.
    this.touches = new Map();
    this.pinch = null;

    this.wrap = el("div", "lab-canvas-wrap");
    this.svg = svg("svg", { class: "lab-canvas", tabindex: "0", role: "application", "aria-label": "Cluster canvas" });
    this.svg.appendChild(this.defs());
    this.viewport = svg("g", { class: "lab-viewport" });
    this.layerCluster = svg("g", { class: "lab-cluster", "aria-hidden": "true" });
    this.clusterRect = svg("rect", { class: "lab-cluster-frame", rx: 16 });
    this.clusterLabel = svg("text", { class: "lab-cluster-label" });
    this.layerCluster.append(this.clusterRect, this.clusterLabel);
    this.layerEdges = svg("g", { class: "lab-layer-edges" });
    this.layerLinks = svg("g", { class: "lab-layer-links" });
    this.layerTopics = svg("g", { class: "lab-layer-topics" });
    this.layerFrames = svg("g", { class: "lab-layer-frames" });
    this.layerNodes = svg("g", { class: "lab-layer-nodes" });
    this.viewport.append(this.layerCluster, this.layerEdges, this.layerLinks, this.layerTopics, this.layerFrames, this.layerNodes);
    this.svg.appendChild(this.viewport);
    this.wrap.appendChild(this.svg);

    this.tools = el("div", "lab-canvas-tools");
    this.tools.append(
      button("Fit", "lab-btn-sm", () => this.fit({ whole: true }), { title: "Fit every node in view (F)" }),
      button("−", "lab-btn-sm", () => this.zoomBy(1 / 1.25), { ariaLabel: "Zoom out" }),
      button("+", "lab-btn-sm", () => this.zoomBy(1.25), { ariaLabel: "Zoom in" }),
      button("?", "lab-btn-sm lab-help-btn", () => hooks.onHelp?.(), { title: "Shortcuts and tips (?)", ariaLabel: "Shortcuts and tips" }),
    );
    this.allBtn = button("All connections", "lab-btn-sm lab-all-edges", () => this.setAllEdges(!this.allEdges), {
      title: "Draw every client-to-broker connection instead of one edge to the cluster",
    });
    this.allBtn.setAttribute("aria-pressed", "false");
    this.tools.prepend(this.allBtn);
    this.wrap.appendChild(this.tools);

    this.empty = el("div", "lab-empty");
    const card = el("div", "lab-empty-card");
    card.append(
      el("strong", null, "The canvas is empty"),
      el("p", null, "Load a ready-made cluster, or build one node by node."),
    );
    const emptyActions = el("div", "lab-palette-actions");
    emptyActions.append(
      button("Load a preset", "lab-btn-sm lab-primary", () => hooks.onOpenTab?.("scenarios")),
      button("Add a node", "lab-btn-sm", () => hooks.onOpenTab?.("build")),
    );
    card.appendChild(emptyActions);
    this.empty.appendChild(card);
    this.wrap.appendChild(this.empty);

    // The gestures a reader would not guess, always on screen. A touch screen
    // has no Shift or wheel: its second node comes from the long-press menu.
    // Stacked, the canvas sits in a page that scrolls: the wheel scrolls it and
    // only Ctrl+wheel (or a trackpad pinch) zooms.
    this.hint = el("ul", "lab-canvas-hint");
    this.hint.setAttribute("aria-label", "Canvas gestures");
    this.stacked = window.matchMedia("(max-width: 1024px)");
    this.coarse = window.matchMedia("(pointer: coarse)");
    const labelHint = () => {
      const gestures = this.coarse.matches
        ? // Most useful first: the strip drops its later items as it narrows.
          [["Long-press", "a card: second node, more"], ["Pinch", "zoom"], ["Drag", "move or pan"], ["Tap", "inspect a node"]]
        : [
            ["Click", "inspect a node"],
            ["Shift+click", "a second node for a link"],
            ["Drag", "move or pan"],
            ["Right-click", "more actions"],
            [this.stacked.matches ? "Ctrl+wheel" : "Wheel", "zoom"],
          ];
      this.hint.replaceChildren(
        ...gestures.map(([keys, what]) => {
          const li = el("li");
          li.append(el("kbd", null, keys), ` ${what}`);
          return li;
        }),
      );
    };
    labelHint();
    this.stacked.addEventListener("change", labelHint);
    this.coarse.addEventListener("change", labelHint);
    this.wrap.appendChild(this.hint);

    // Said while the view is zoomed in past what fits, until the reader moves it.
    this.note = el("div", "lab-canvas-note", "Zoomed in to stay readable: drag to pan, Fit shows all.");
    this.note.hidden = true;
    this.wrap.appendChild(this.note);

    this.menu = el("div", "lab-menu");
    this.menu.setAttribute("role", "menu");
    this.menu.hidden = true;
    this.wrap.appendChild(this.menu);

    container.appendChild(this.wrap);
    this.bindPointer();
    this.bindKeys();
    this.applyView();
    // The box changes with the window, Expand and Close: refit, unless the
    // reader has moved the view. The fault bar's second row only nudges the
    // height (selecting a link must not move the cards), so a small height
    // change alone is ignored, measured from the last fit.
    let refit = 0;
    new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      if (!this.fitted || this.userMovedView || width <= 0) return;
      const last = this.fitBox;
      if (last && Math.abs(width - last.w) < 1 && Math.abs(height - last.h) < 60) return;
      // Fitting can show or hide the note strip, which resizes this box again:
      // do it after the observer's own delivery, once per frame.
      if (refit) return;
      refit = requestAnimationFrame(() => {
        refit = 0;
        if (!this.userMovedView) this.fit({ whole: this.whole });
      });
    }).observe(this.svg);
  }

  defs() {
    const defs = svg("defs");
    for (const [id, cls] of [
      ["lab-arrow", "lab-arrow-tip"],
      ["lab-arrow-faint", "lab-arrow-tip-faint"],
    ]) {
      const marker = svg("marker", { id, viewBox: "0 0 10 10", refX: "9", refY: "5", markerWidth: "7", markerHeight: "7", orient: "auto-start-reverse" });
      marker.appendChild(svg("path", { d: "M 0 0 L 10 5 L 0 10 z", class: cls }));
      defs.appendChild(marker);
    }
    return defs;
  }

  // ---- data ----------------------------------------------------------------------------

  // `data`: { snapshot, scenario, hosting (Map nodeId → peerId | null),
  // me (peer id), offline (Set of peer ids), selection [ids] }
  setData(data) {
    this.snapshot = data.snapshot;
    this.scenario = data.scenario;
    this.selection = data.selection || [];
    this.hosting = data.hosting || null;
    this.me = data.me;
    this.offline = data.offline || new Set();
    const snap = data.snapshot;
    this.defaultLatency = snap?.default_latency_ms ?? 5;
    this.latencies = new Map();
    for (const l of snap?.links || []) this.latencies.set(pairKey(l.a, l.b), l);
    const nodes = snap?.nodes || [];
    for (const n of nodes) {
      if (this.drag && this.drag.id === n.id) continue;
      const cur = this.positions.get(n.id);
      if (!cur || cur.x !== n.x || cur.y !== n.y) this.positions.set(n.id, { x: n.x, y: n.y });
    }
    for (const id of [...this.positions.keys()]) if (!nodes.some((n) => n.id === id)) this.positions.delete(id);
    this.trackActivity(snap, nodes);
    this.autoPlace(nodes);
    this.syncNodes(nodes);
    this.syncTopics();
    this.syncEdges();
    this.syncLinks();
    this.inFlight = (snap?.in_flight || []).slice(0, MAX_DOTS);
    this.layout();
    this.hooks.decorate?.(this); // J2: cluster badges and partition chips (cluster-panel.js)
    const visible = nodes.filter((n) => !kindOf(n.kind).hidden).length;
    this.empty.hidden = visible > 0;
    this.hint.hidden = visible === 0;
    // A node added or removed moves the graph's edges (and its topic pills):
    // keep everything in view until the reader takes the view over.
    const ids = nodes.map((n) => n.id).sort().join();
    const changed = ids !== this.nodeIds;
    this.nodeIds = ids;
    if (visible > 0 && !this.userMovedView && (!this.fitted || changed)) {
      this.fit({ whole: this.fitted && this.whole });
      this.fitted = true;
    }
    if (visible === 0) this.fitted = false;
  }

  // A hidden node (the scenario's admin) has no position of its own; put it
  // under the broker it bootstraps.
  autoPlace(nodes) {
    for (const n of nodes) {
      const k = kindOf(n.kind);
      if (!k.hidden) continue;
      const p = this.positions.get(n.id);
      if (p && (p.x !== 0 || p.y !== 0)) continue;
      const spec = this.scenario?.nodes?.find((s) => s.id === n.id);
      const target = spec?.config?.bootstrap?.[0];
      const anchor = target != null ? this.positions.get(Number(target)) : null;
      const brokers = nodes.filter((b) => b.kind === "krabka-broker");
      const base = anchor || (brokers.length ? this.positions.get(brokers[0].id) : null) || { x: 60, y: 60 };
      // The nearest row under the broker that no card is on: where cards sit
      // close together the gap is narrower than the usual 96.
      const x = base.x - 40;
      const free = (y) => !nodes.some((o) => {
        const p = o.id !== n.id && this.positions.get(o.id);
        return p && Math.abs(p.x - x) < (CARD_W + GHOST_W) / 2 + 8 && Math.abs(p.y - y) < (CARD_H + GHOST_H) / 2 + 8;
      });
      const dy = [96, 88, 104, 80, 112, 72, 120, 64].find((d) => free(base.y + d)) ?? 96;
      this.positions.set(n.id, { x, y: base.y + dy });
    }
  }

  // ---- nodes --------------------------------------------------------------------------------

  syncNodes(nodes) {
    const seen = new Set();
    for (const n of nodes) {
      seen.add(n.id);
      let entry = this.nodeEls.get(n.id);
      if (!entry) {
        entry = this.makeNode(n);
        this.nodeEls.set(n.id, entry);
        this.layerNodes.appendChild(entry.g);
      }
      this.updateNode(entry, n);
    }
    for (const [id, entry] of this.nodeEls) {
      if (!seen.has(id)) {
        entry.g.remove();
        this.nodeEls.delete(id);
      }
    }
  }

  makeNode(n) {
    const k = kindOf(n.kind);
    const ghost = Boolean(k.hidden);
    const w = ghost ? GHOST_W : CARD_W;
    const h = ghost ? GHOST_H : CARD_H;
    const g = svg("g", { class: `lab-node${ghost ? " lab-node-ghost" : ""}`, tabindex: "0", role: "button" });
    g.dataset.nodeId = String(n.id);
    const rect = svg("rect", { class: "lab-card", x: -w / 2, y: -h / 2, width: w, height: h, rx: ghost ? 8 : 12 });
    rect.style.stroke = k.color;
    g.appendChild(rect);
    const glyphBg = svg("circle", { class: "lab-glyph-bg", cx: -w / 2 + (ghost ? 14 : 22), cy: 0, r: ghost ? 9 : 14 });
    glyphBg.style.fill = k.color;
    const glyph = svg("text", { class: "lab-glyph", x: -w / 2 + (ghost ? 14 : 22), y: ghost ? 4 : 5, "text-anchor": "middle" });
    glyph.textContent = k.glyph;
    g.append(glyphBg, glyph);
    const tx = -w / 2 + (ghost ? 30 : 44);
    const name = svg("text", { class: "lab-card-name", x: tx, y: ghost ? 4 : -9 });
    g.appendChild(name);
    let kind = null;
    let status = null;
    if (!ghost) {
      kind = svg("text", { class: "lab-card-kind", x: tx, y: 6 });
      status = svg("text", { class: "lab-card-status", x: tx, y: 22 });
      g.append(kind, status);
    }
    // Badges straddle the top edge, above the title, so a long name is never covered.
    const badges = svg("g", { class: "lab-badges", transform: `translate(${w / 2 - 6}, ${-h / 2 - 7})` });
    g.appendChild(badges);
    const halo = svg("rect", { class: "lab-card-halo", x: -w / 2 - 4, y: -h / 2 - 4, width: w + 8, height: h + 8, rx: ghost ? 11 : 15 });
    g.insertBefore(halo, rect);
    return { g, rect, name, kind, status, badges, w, h, ghost, lastKey: "" };
  }

  updateNode(entry, n) {
    const k = kindOf(n.kind);
    const st = statusLine(n);
    const hostedBy = this.hosting ? this.hosting.get(n.id) : null;
    const remote = !n.hosted;
    const peerLabel = remote ? this.hooks.peerName(hostedBy) : "";
    const offline = remote && hostedBy != null && this.offline.has(hostedBy);
    const sel = this.selection.indexOf(n.id);
    const key = [n.name, st, n.alive, n.isolated, n.hosted, peerLabel, offline, sel, n.paused, n.skew_ms, n.disk].join("|"); // J3: paused, skew, disk
    if (key === entry.lastKey) return;
    entry.lastKey = key;
    // Text is cut to the card's measured room; the status drops whole fields
    // from the end first. The full text stays in the title and aria-label.
    const room = entry.w - (entry.ghost ? 38 : 52);
    fitText(entry.name, [n.name || `${n.kind}-${n.id}`], "", room, entry.ghost ? 14 : 16);
    if (entry.kind) fitText(entry.kind, [k.label.toLowerCase(), `#${n.id}`], " · ", room, 22);
    if (entry.status) fitText(entry.status, st.split(" · "), " · ", room, 18);
    const g = entry.g;
    g.classList.toggle("lab-down", !n.alive);
    g.classList.toggle("lab-isolated", n.isolated);
    g.classList.toggle("lab-remote", remote);
    g.classList.toggle("lab-offline", offline);
    g.setAttribute("aria-pressed", String(sel >= 0));
    g.classList.toggle("lab-selected", sel === 0);
    g.classList.toggle("lab-pick-b", sel === 1);
    g.classList.toggle("lab-real", Boolean(k.real));
    g.dataset.kind = n.kind;
    g.dataset.hosted = String(Boolean(n.hosted));
    g.dataset.alive = String(Boolean(n.alive));
    g.dataset.isolated = String(Boolean(n.isolated));
    g.dataset.status = st;
    // A node whose host tab dropped off is not "up", whatever its last snapshot says.
    const bits = [n.name, k.label];
    if (!n.alive) bits.push("down");
    else if (!offline) bits.push("up");
    if (k.real) bits.push("runs the real code");
    if (n.isolated) bits.push("isolated");
    // J3: the process faults (pause, clock skew, disk mode).
    if (n.paused) bits.push("paused");
    if (n.skew_ms) bits.push(`clock skewed ${n.skew_ms > 0 ? "+" : ""}${n.skew_ms} ms`);
    if (n.disk && n.disk !== "ok") bits.push(`disk ${n.disk}`);
    if (remote && hostedBy != null) bits.push(offline ? "host offline" : `hosted by ${peerLabel}`);
    if (st) bits.push(st);
    g.setAttribute("aria-label", bits.join(", "));
    let title = g.querySelector(":scope > title");
    if (!title) {
      title = svg("title");
      g.prepend(title);
    }
    title.textContent = bits.join(", ");
    // Badges, right-aligned from the card's top-right corner (see makeNode).
    entry.badges.innerHTML = "";
    const badges = [];
    // A node that runs the real code, not the lab's model of it.
    if (k.real) badges.push(["real", "lab-badge-real"]);
    if (!n.alive) badges.push(["down", "lab-badge-down"]);
    if (n.isolated) badges.push(["isolated", "lab-badge-isolated"]);
    // J3: the process faults.
    if (n.paused && n.alive) badges.push(["paused", "lab-badge-isolated"]);
    if (n.skew_ms) badges.push([`skew ${n.skew_ms > 0 ? "+" : ""}${Math.abs(n.skew_ms) >= 1000 ? `${n.skew_ms / 1000}s` : `${n.skew_ms}ms`}`, "lab-badge-offline"]);
    if (n.disk && n.disk !== "ok") badges.push([`disk ${n.disk}`, "lab-badge-down"]);
    if (offline) badges.push(["host offline", "lab-badge-offline"]);
    // A node with no known host (the admin before hosting is settled) has no one to name.
    else if (remote && hostedBy != null) badges.push([`@${peerLabel}`, "lab-badge-remote"]);
    let x = 0;
    for (const [text, cls] of badges) {
      const t = svg("text", { class: "lab-badge-text", y: 11, "text-anchor": "middle" });
      entry.badges.appendChild(t);
      fitText(t, [text], "", 84, 14);
      const w = (t.getComputedTextLength() || text.length * 6.2) + 10;
      x -= w;
      t.setAttribute("x", x + w / 2);
      entry.badges.insertBefore(svg("rect", { class: `lab-badge ${cls}`, x, y: 0, width: w, height: 15, rx: 7 }), t);
      x -= 4;
    }
  }

  // ---- topics ------------------------------------------------------------------------------------

  syncTopics() {
    const names = this.scenario ? topicNames(this.scenario) : [];
    const specs = new Map((this.scenario?.topics || []).map((t) => [t.name, t]));
    const internal = this.scenario ? internalTopics(this.scenario) : new Map();
    const seen = new Set();
    for (const name of names) {
      seen.add(name);
      let g = this.topicEls.get(name);
      if (!g) {
        g = svg("g", { class: "lab-topic" });
        g.dataset.topic = name;
        g.appendChild(svg("title"));
        g.appendChild(svg("rect", { class: "lab-topic-bg", x: -TOPIC_W / 2, y: -TOPIC_H / 2, width: TOPIC_W, height: TOPIC_H, rx: TOPIC_H / 2 }));
        const t = svg("text", { class: "lab-topic-name", x: 0, y: -1, "text-anchor": "middle" });
        const sub = svg("text", { class: "lab-topic-sub", x: 0, y: 11, "text-anchor": "middle" });
        g.append(t, sub);
        this.layerTopics.appendChild(g);
        this.topicEls.set(name, g);
      }
      const spec = specs.get(name);
      const role = internal.get(name);
      g.classList.toggle("lab-topic-internal", Boolean(role) && !spec);
      const sub = spec
        ? [plural(spec.partitions, "partition"), `rf ${spec.replication_factor === -1 ? "default" : spec.replication_factor}`]
        : role
          ? [role, "internal"]
          : ["topic"];
      // Measuring forces layout: only when the words changed.
      const text = [name, ...sub].join("|");
      if (g.dataset.text !== text) {
        g.dataset.text = text;
        fitText(g.querySelector(".lab-topic-name"), [name], "", TOPIC_W - 20, 17);
        fitText(g.querySelector(".lab-topic-sub"), sub, " · ", TOPIC_W - 20, 24);
      }
      g.querySelector("title").textContent = role && !spec ? `${name}: the streams app's ${role} topic, which the streams group creates` : spec ? `${name}: ${sub.join(" · ")}` : name;
    }
    for (const [name, g] of this.topicEls) {
      if (!seen.has(name)) {
        g.remove();
        this.topicEls.delete(name);
        this.topicPositions.delete(name);
      }
    }
  }

  // Topics sit at the centroid of the nodes they connect, pushed down when
  // that would land on a card, and spread when two topics would overlap.
  placeTopics(edges) {
    const placed = [];
    for (const name of this.topicEls.keys()) {
      const peers = [];
      for (const e of edges) {
        if (e.from.topic === name && e.to.node != null) peers.push(this.positions.get(e.to.node));
        if (e.to.topic === name && e.from.node != null) peers.push(this.positions.get(e.from.node));
      }
      const pts = peers.filter(Boolean);
      let x;
      let y;
      if (pts.length) {
        x = pts.reduce((s, p) => s + p.x, 0) / pts.length;
        y = pts.reduce((s, p) => s + p.y, 0) / pts.length;
        const producers = pts.length === 1 || pts.every((p) => Math.abs(p.y - pts[0].y) < 1 && Math.abs(p.x - pts[0].x) < 1);
        if (producers) y += 100;
      } else {
        const all = [...this.positions.values()];
        const maxY = all.length ? Math.max(...all.map((p) => p.y)) : 0;
        const minX = all.length ? Math.min(...all.map((p) => p.x)) : 0;
        x = minX + placed.length * (TOPIC_W + 20);
        y = maxY + 120;
      }
      // Step down past whatever the pill sits on, cards and pills placed before
      // it alike, until nothing is left under it. Each step clears one obstacle
      // for good, so this ends.
      const blocks = [...this.positions.values()].map((p) => ({ ...p, w: CARD_W, h: CARD_H }));
      for (const q of placed) blocks.push({ ...q, w: TOPIC_W, h: TOPIC_H });
      for (let hit; (hit = blocks.find((b) => Math.abs(b.x - x) < (b.w + TOPIC_W) / 2 + 4 && Math.abs(b.y - y) < (b.h + TOPIC_H) / 2 + 4)); ) {
        y = hit.y + (hit.h + TOPIC_H) / 2 + 16;
      }
      const pos = { x, y };
      placed.push(pos);
      this.topicPositions.set(name, pos);
    }
  }

  // ---- edges and link overlays -------------------------------------------------------------------

  setAllEdges(on) {
    this.allEdges = on;
    this.allBtn.setAttribute("aria-pressed", String(on));
    this.syncEdges();
    this.layout();
  }

  // The brokers, and their frame: two or more brokers with no other card inside
  // their box (one edge per client would then point at the wrong card).
  clusterMode() {
    const ids = (this.snapshot?.nodes || []).filter((n) => n.kind === BROKER_KIND).map((n) => n.id);
    if (this.allEdges || ids.length < 2) return { ids, box: null };
    const box = this.boxOf(ids, FRAME_PAD, FRAME_PAD_TOP);
    if (!box) return { ids, box: null };
    for (const [id, p] of this.positions) {
      if (ids.includes(id)) continue;
      if (p.x > box.x && p.x < box.x + box.w && p.y > box.y && p.y < box.y + box.h) return { ids, box: null };
    }
    return { ids, box };
  }

  // The box around the cards of `ids`, padded.
  boxOf(ids, pad, padTop) {
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const id of ids) {
      const p = this.positions.get(id);
      if (!p) continue;
      const e = this.nodeEls.get(id);
      const w = e ? e.w : CARD_W;
      const h = e ? e.h : CARD_H;
      minX = Math.min(minX, p.x - w / 2);
      maxX = Math.max(maxX, p.x + w / 2);
      minY = Math.min(minY, p.y - h / 2);
      maxY = Math.max(maxY, p.y + h / 2);
    }
    if (minX === Infinity) return null;
    return { x: minX - pad, y: minY - padTop, w: maxX - minX + 2 * pad, h: maxY - minY + pad + padTop };
  }

  // Lab time of the last frame on each broker pair: from the frames in flight
  // and from the per-direction delivery counts that grew since the last snapshot.
  trackActivity(snap, nodes) {
    if (!snap) return;
    const brokers = new Set(nodes.filter((n) => n.kind === BROKER_KIND).map((n) => n.id));
    const mark = (a, b) => {
      if (a !== b && brokers.has(a) && brokers.has(b)) this.activity.set(pairKey(a, b), snap.now);
    };
    const counts = new Map();
    for (const [a, b, n] of snap.delivered || []) {
      const key = `${a}>${b}`;
      counts.set(key, n);
      if (this.delivered && n > (this.delivered.get(key) ?? 0)) mark(a, b);
    }
    this.delivered = counts;
    for (const f of snap.in_flight || []) mark(f.src, f.dst);
    // A reset world starts its clock again: what flowed in the old one is gone.
    for (const [key, at] of this.activity) if (at > snap.now || snap.now - at > ACTIVE_MS) this.activity.delete(key);
  }

  syncEdges() {
    const mode = this.clusterMode();
    this.frame = mode.box;
    // With a frame, an edge to any broker becomes one edge to the frame.
    const brokers = new Set(mode.box ? mode.ids : []);
    const toFrame = (end) => (end.node != null && brokers.has(end.node) ? { cluster: true } : end);
    const unique = new Map();
    for (const e of this.scenario ? derivedEdges(this.scenario) : []) {
      const d = { ...e, from: toFrame(e.from), to: toFrame(e.to) };
      if (d.from.cluster && d.to.cluster) continue;
      unique.set(`${endKey(d.from)}>${endKey(d.to)}:${d.type}`, d);
    }
    const edges = [...unique.values()];
    this.edges = edges;
    const seen = new Set();
    for (const e of edges) {
      const key = `${endKey(e.from)}>${endKey(e.to)}:${e.type}`;
      seen.add(key);
      let line = this.edgeEls.get(key);
      if (!line) {
        line = svg("line", { class: `lab-edge lab-edge-${e.type}` });
        line.setAttribute("marker-end", FAINT_EDGES.has(e.type) ? "url(#lab-arrow-faint)" : "url(#lab-arrow)");
        this.layerEdges.appendChild(line);
        this.edgeEls.set(key, line);
      }
    }
    for (const [key, line] of this.edgeEls) {
      if (!seen.has(key)) {
        line.remove();
        this.edgeEls.delete(key);
      }
    }
  }

  syncLinks() {
    const links = this.snapshot?.links || [];
    const seen = new Set();
    for (const l of links) {
      const key = pairKey(l.a, l.b);
      seen.add(key);
      let g = this.linkEls.get(key);
      if (!g) {
        g = svg("g", { class: "lab-link" });
        g.appendChild(svg("line", { class: "lab-link-line" }));
        const label = svg("text", { class: "lab-link-label", "text-anchor": "middle" });
        g.appendChild(label);
        this.layerLinks.appendChild(g);
        this.linkEls.set(key, g);
      }
      g.classList.toggle("lab-link-cut", Boolean(l.cut));
      const bits = [];
      if (l.cut) bits.push("✂ cut");
      if (l.latency_ms !== this.defaultLatency) bits.push(`${l.latency_ms} ms`);
      if (l.loss_permille) bits.push(`${(l.loss_permille / 10).toFixed(l.loss_permille % 10 ? 1 : 0)}% loss`);
      g.querySelector(".lab-link-label").textContent = bits.join(" · ");
      g.dataset.a = String(l.a);
      g.dataset.b = String(l.b);
    }
    for (const [key, g] of this.linkEls) {
      if (!seen.has(key)) {
        g.remove();
        this.linkEls.delete(key);
      }
    }
  }

  // ---- layout ---------------------------------------------------------------------------------------

  layout() {
    for (const [id, entry] of this.nodeEls) {
      const p = this.positions.get(id);
      if (p) entry.g.setAttribute("transform", `translate(${p.x}, ${p.y})`);
    }
    // A dragged card can enter or leave the brokers' box: the edges follow.
    const mode = this.clusterMode();
    if (Boolean(mode.box) !== Boolean(this.frame)) this.syncEdges();
    this.frame = mode.box;
    this.layerCluster.style.display = this.frame ? "" : "none";
    if (this.frame) {
      const f = this.frame;
      setAttrs(this.clusterRect, { x: f.x, y: f.y, width: f.w, height: f.h });
      setAttrs(this.clusterLabel, { x: f.x + 14, y: f.y + 17 });
      this.clusterLabel.textContent = `cluster · ${plural(mode.ids.length, "broker")}`;
    }
    this.layoutActive();
    this.placeTopics(this.edges || []);
    for (const [name, g] of this.topicEls) {
      const p = this.topicPositions.get(name);
      if (p) g.setAttribute("transform", `translate(${p.x}, ${p.y})`);
    }
    for (const e of this.edges || []) {
      const key = `${endKey(e.from)}>${endKey(e.to)}:${e.type}`;
      const line = this.edgeEls.get(key);
      const a = this.endPoint(e.from);
      const b = this.endPoint(e.to);
      if (!line || !a || !b) {
        if (line) line.setAttribute("visibility", "hidden");
        continue;
      }
      const start = trim(a, b, this.endSize(e.from));
      const end = trim(b, a, this.endSize(e.to));
      setAttrs(line, { x1: start.x, y1: start.y, x2: end.x, y2: end.y, visibility: null });
    }
    for (const [key, g] of this.linkEls) {
      const [a, b] = key.split("-").map(Number);
      const pa = this.positions.get(a);
      const pb = this.positions.get(b);
      if (!pa || !pb) {
        g.setAttribute("visibility", "hidden");
        continue;
      }
      g.removeAttribute("visibility");
      setAttrs(g.querySelector("line"), { x1: pa.x, y1: pa.y, x2: pb.x, y2: pb.y });
      setAttrs(g.querySelector("text"), { x: (pa.x + pb.x) / 2, y: (pa.y + pb.y) / 2 - 6 });
    }
  }

  // Broker-to-broker lines, while frames flow on them.
  layoutActive() {
    const now = this.snapshot?.now ?? 0;
    for (const [key, line] of this.activeEls) {
      if (this.activity.has(key)) continue;
      line.remove();
      this.activeEls.delete(key);
    }
    for (const [key, at] of this.activity) {
      const [a, b] = key.split("-").map(Number);
      const pa = this.positions.get(a);
      const pb = this.positions.get(b);
      let line = this.activeEls.get(key);
      if (!pa || !pb) {
        line?.remove();
        this.activeEls.delete(key);
        continue;
      }
      if (!line) {
        line = svg("line", { class: "lab-edge lab-edge-active" });
        this.layerEdges.appendChild(line);
        this.activeEls.set(key, line);
      }
      const start = trim(pa, pb, this.endSize({ node: a }));
      const end = trim(pb, pa, this.endSize({ node: b }));
      setAttrs(line, { x1: start.x, y1: start.y, x2: end.x, y2: end.y });
      // Full strength while frames flow, fading after the last one.
      line.style.opacity = String(clamp(1 - (now - at) / ACTIVE_MS, 0.15, 1));
    }
  }

  endPoint(end) {
    if (end.cluster) return this.frame ? { x: this.frame.x + this.frame.w / 2, y: this.frame.y + this.frame.h / 2 } : null;
    if (end.node != null) return this.positions.get(end.node) || null;
    if (end.topic != null) return this.topicPositions.get(end.topic) || null;
    return null;
  }

  endSize(end) {
    if (end.cluster) return this.frame ? { w: this.frame.w, h: this.frame.h } : { w: CARD_W, h: CARD_H };
    if (end.topic != null) return { w: TOPIC_W, h: TOPIC_H };
    const entry = end.node != null ? this.nodeEls.get(end.node) : null;
    return entry ? { w: entry.w, h: entry.h } : { w: CARD_W, h: CARD_H };
  }

  // ---- frames in flight ------------------------------------------------------------------------------

  // Move every dot to where its frame is at simulated time `now`.
  animate(now) {
    const frames = this.inFlight;
    const labelled = frames.length <= MAX_LABELLED_DOTS && this.view.k >= 0.6;
    let i = 0;
    for (const f of frames) {
      const a = this.positions.get(f.src);
      const b = this.positions.get(f.dst);
      if (!a || !b) continue;
      const link = this.latencies.get(pairKey(f.src, f.dst));
      const latency = link ? link.latency_ms : this.defaultLatency;
      const remaining = Math.max(0, f.at - now);
      const p = latency > 0 ? clamp(1 - remaining / latency, 0, 1) : 1;
      // Frames in opposite directions ride on opposite sides of the wire.
      const dx = b.x - a.x;
      const dy = b.y - a.y;
      const len = Math.hypot(dx, dy) || 1;
      const off = f.src < f.dst ? 7 : -7;
      const ox = (-dy / len) * off;
      const oy = (dx / len) * off;
      const x = a.x + dx * p + ox;
      const y = a.y + dy * p + oy;
      let dot = this.dotPool[i];
      if (!dot) {
        dot = svg("g", { class: "lab-frame" });
        dot.appendChild(svg("circle", { r: 4 }));
        dot.appendChild(svg("text", { class: "lab-frame-label", x: 6, y: -6 }));
        this.layerFrames.appendChild(dot);
        this.dotPool[i] = dot;
      }
      dot.setAttribute("transform", `translate(${x}, ${y})`);
      dot.style.display = "";
      const srcKind = this.snapshot?.nodes?.find((n) => n.id === f.src)?.kind;
      dot.querySelector("circle").style.fill = kindOf(srcKind).color;
      const label = dot.querySelector("text");
      const text = labelled ? f.label : "";
      if (label.textContent !== text) label.textContent = text;
      dot.classList.toggle("lab-frame-close", f.label === "close");
      i += 1;
    }
    for (; i < this.dotPool.length; i++) this.dotPool[i].style.display = "none";
  }

  // ---- view ----------------------------------------------------------------------------------------------

  applyView() {
    const { x, y, k } = this.view;
    this.viewport.setAttribute("transform", `translate(${x}, ${y}) scale(${k})`);
  }

  size() {
    const r = this.svg.getBoundingClientRect();
    return { w: r.width || 800, h: r.height || 480 };
  }

  toWorld(clientX, clientY) {
    const r = this.svg.getBoundingClientRect();
    return { x: (clientX - r.left - this.view.x) / this.view.k, y: (clientY - r.top - this.view.y) / this.view.k };
  }

  zoomAt(clientX, clientY, factor) {
    const r = this.svg.getBoundingClientRect();
    const px = clientX - r.left;
    const py = clientY - r.top;
    const k = clamp(this.view.k * factor, MIN_ZOOM, MAX_ZOOM);
    const ratio = k / this.view.k;
    this.view = { x: px - (px - this.view.x) * ratio, y: py - (py - this.view.y) * ratio, k };
    this.userMovedView = true;
    this.note.hidden = true;
    this.applyView();
  }

  zoomBy(factor) {
    const { w, h } = this.size();
    const r = this.svg.getBoundingClientRect();
    this.zoomAt(r.left + w / 2, r.top + h / 2, factor);
  }

  // Every node in view, but never smaller than MIN_FIT_ZOOM unless `whole`
  // (the Fit button): a graph that does not fit then starts at the top left.
  fit({ whole = false } = {}) {
    this.whole = whole;
    const pts = [];
    for (const [id, p] of this.positions) {
      const entry = this.nodeEls.get(id);
      const w = entry ? entry.w : CARD_W;
      const h = entry ? entry.h : CARD_H;
      pts.push({ x: p.x - w / 2, y: p.y - h / 2 }, { x: p.x + w / 2, y: p.y + h / 2 });
    }
    for (const p of this.topicPositions.values()) pts.push({ x: p.x - TOPIC_W / 2, y: p.y - TOPIC_H / 2 }, { x: p.x + TOPIC_W / 2, y: p.y + TOPIC_H / 2 });
    if (this.frame) pts.push({ x: this.frame.x, y: this.frame.y }, { x: this.frame.x + this.frame.w, y: this.frame.y + this.frame.h });
    if (!pts.length) {
      this.view = { x: 40, y: 40, k: 1 };
      this.note.hidden = true;
      this.applyView();
      return;
    }
    const minX = Math.min(...pts.map((p) => p.x));
    const maxX = Math.max(...pts.map((p) => p.x));
    const minY = Math.min(...pts.map((p) => p.y));
    const maxY = Math.max(...pts.map((p) => p.y));
    const { w, h } = this.size();
    // More room above the cards: the canvas tools float there, and the status badges rise above a card.
    const padX = 36;
    const padTop = 46;
    const padBottom = 30;
    const bw = Math.max(1, maxX - minX);
    const bh = Math.max(1, maxY - minY);
    const roomX = w - 2 * padX;
    const roomY = h - padTop - padBottom;
    const snug = Math.min(roomX / bw, roomY / bh);
    const k = clamp(whole ? snug : Math.max(snug, MIN_FIT_ZOOM), MIN_ZOOM, 1.4);
    const cropped = bw * k > roomX + 0.5 || bh * k > roomY + 0.5;
    this.view = {
      x: padX + Math.max(0, roomX - bw * k) / 2 - minX * k,
      y: padTop + Math.max(0, roomY - bh * k) / 2 - minY * k,
      k,
    };
    this.fitBox = { w, h };
    this.note.hidden = !cropped;
    this.userMovedView = false;
    this.applyView();
  }

  focusNode(id) {
    const entry = this.nodeEls.get(id);
    if (entry) entry.g.focus();
  }

  // ---- pointer input ---------------------------------------------------------------------------------------

  bindPointer() {
    const s = this.svg;
    s.addEventListener("pointerdown", (e) => {
      this.closeMenu();
      if (e.button === 2) return;
      if (e.pointerType === "touch") {
        this.touches.set(e.pointerId, { x: e.clientX, y: e.clientY });
        // A second finger turns whatever the first was doing into a pinch.
        if (this.touches.size > 1) {
          if (!this.pinch) this.startPinch();
          return;
        }
      }
      const nodeG = e.target.closest?.(".lab-node");
      const start = { x: e.clientX, y: e.clientY };
      if (nodeG) {
        const id = Number(nodeG.dataset.nodeId);
        const p = this.positions.get(id) || { x: 0, y: 0 };
        this.drag = { id, start, origin: { ...p }, moved: false, pointerId: e.pointerId, shift: e.shiftKey };
        nodeG.classList.add("lab-dragging");
        try {
          s.setPointerCapture(e.pointerId);
        } catch {
          // Capture is a convenience; dragging works without it.
        }
        if (e.pointerType === "touch") {
          this.longPress = setTimeout(() => {
            this.longPress = null;
            if (this.drag && !this.drag.moved) {
              this.cancelDrag();
              this.openMenu(id, e.clientX, e.clientY);
            }
          }, LONG_PRESS_MS);
        }
        e.preventDefault();
      } else if (e.button === 0) {
        this.drag = { pan: true, start, origin: { x: this.view.x, y: this.view.y }, moved: false, pointerId: e.pointerId };
        try {
          s.setPointerCapture(e.pointerId);
        } catch {
          // See above.
        }
        e.preventDefault();
      }
    });
    s.addEventListener("pointermove", (e) => {
      if (this.touches.has(e.pointerId)) this.touches.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (this.pinch) {
        this.movePinch();
        return;
      }
      const d = this.drag;
      if (!d || d.pointerId !== e.pointerId) return;
      const dx = e.clientX - d.start.x;
      const dy = e.clientY - d.start.y;
      if (!d.moved && Math.hypot(dx, dy) < DRAG_THRESHOLD) return;
      d.moved = true;
      if (this.longPress) {
        clearTimeout(this.longPress);
        this.longPress = null;
      }
      if (d.pan) {
        this.view.x = d.origin.x + dx;
        this.view.y = d.origin.y + dy;
        this.userMovedView = true;
        this.note.hidden = true;
        this.applyView();
      } else {
        const p = { x: d.origin.x + dx / this.view.k, y: d.origin.y + dy / this.view.k };
        this.positions.set(d.id, p);
        this.layout();
      }
    });
    const release = (e) => {
      this.touches.delete(e.pointerId);
      if (this.pinch && this.touches.size < 2) this.pinch = null;
    };
    const finish = (e) => {
      release(e);
      const d = this.drag;
      if (!d || d.pointerId !== e.pointerId) return;
      if (this.longPress) {
        clearTimeout(this.longPress);
        this.longPress = null;
      }
      this.drag = null;
      if (!d.pan) {
        const entry = this.nodeEls.get(d.id);
        if (entry) entry.g.classList.remove("lab-dragging");
        if (d.moved) {
          const p = this.positions.get(d.id);
          if (p) this.hooks.onMove(d.id, Math.round(p.x), Math.round(p.y));
        } else {
          this.hooks.onSelect(d.id, { additive: d.shift });
          if (entry) entry.g.focus();
        }
      } else if (!d.moved) {
        this.hooks.onDeselect();
      }
    };
    s.addEventListener("pointerup", finish);
    s.addEventListener("pointercancel", (e) => {
      release(e);
      this.cancelDrag();
    });
    s.addEventListener("contextmenu", (e) => {
      const nodeG = e.target.closest?.(".lab-node");
      e.preventDefault();
      if (nodeG) this.openMenu(Number(nodeG.dataset.nodeId), e.clientX, e.clientY);
      else this.closeMenu();
    });
    s.addEventListener(
      "wheel",
      (e) => {
        if (this.stacked.matches && !e.ctrlKey && !e.metaKey && !this.wrap.classList.contains("lab-expanded")) return;
        e.preventDefault();
        const factor = Math.exp(-e.deltaY * (e.deltaMode === 1 ? 0.05 : 0.0015));
        this.zoomAt(e.clientX, e.clientY, factor);
      },
      { passive: false },
    );
    s.addEventListener("dblclick", (e) => {
      const nodeG = e.target.closest?.(".lab-node");
      if (nodeG) this.hooks.onCommand(Number(nodeG.dataset.nodeId), "edit");
    });
  }

  startPinch() {
    if (this.longPress) {
      clearTimeout(this.longPress);
      this.longPress = null;
    }
    this.cancelDrag();
    this.pinch = this.pinchState();
  }

  // Distance and midpoint of the first two fingers.
  pinchState() {
    const [a, b] = [...this.touches.values()];
    return { dist: Math.hypot(a.x - b.x, a.y - b.y) || 1, cx: (a.x + b.x) / 2, cy: (a.y + b.y) / 2 };
  }

  // Zoom by the change in finger distance around the midpoint, and follow the midpoint.
  movePinch() {
    const now = this.pinchState();
    this.zoomAt(now.cx, now.cy, now.dist / this.pinch.dist);
    this.view.x += now.cx - this.pinch.cx;
    this.view.y += now.cy - this.pinch.cy;
    this.applyView();
    this.pinch = now;
  }

  cancelDrag() {
    const d = this.drag;
    this.drag = null;
    if (d?.pan && d.moved) {
      // The browser took the gesture to scroll the page, or a second finger made it a pinch.
      this.view.x = d.origin.x;
      this.view.y = d.origin.y;
      this.applyView();
    }
    if (d && !d.pan) {
      const entry = this.nodeEls.get(d.id);
      if (entry) entry.g.classList.remove("lab-dragging");
      this.positions.set(d.id, d.origin);
      this.layout();
    }
  }

  bindKeys() {
    this.svg.addEventListener("keydown", (e) => {
      const nodeG = e.target.closest?.(".lab-node");
      if (!nodeG) return;
      const id = Number(nodeG.dataset.nodeId);
      const step = e.shiftKey ? 1 : 10;
      const move = (dx, dy) => {
        const p = this.positions.get(id) || { x: 0, y: 0 };
        const next = { x: p.x + dx, y: p.y + dy };
        this.positions.set(id, next);
        this.layout();
        this.hooks.onMove(id, next.x, next.y);
      };
      switch (e.key) {
        case "Enter":
        case " ":
          e.preventDefault();
          this.hooks.onSelect(id, { additive: e.shiftKey });
          break;
        case "Delete":
        case "Backspace":
          e.preventDefault();
          this.hooks.onCommand(id, "remove");
          break;
        case "ArrowLeft":
          e.preventDefault();
          move(-step, 0);
          break;
        case "ArrowRight":
          e.preventDefault();
          move(step, 0);
          break;
        case "ArrowUp":
          e.preventDefault();
          move(0, -step);
          break;
        case "ArrowDown":
          e.preventDefault();
          move(0, step);
          break;
        case "ContextMenu":
        case "F10":
          if (e.key === "F10" && !e.shiftKey) return;
          e.preventDefault();
          {
            const r = nodeG.getBoundingClientRect();
            this.openMenu(id, r.left + r.width / 2, r.top + r.height / 2);
          }
          break;
        case "e":
          this.hooks.onCommand(id, "edit");
          break;
        default:
      }
    });
  }

  // ---- the context menu ------------------------------------------------------------------------------------------

  openMenu(id, clientX, clientY) {
    this.closeMenu();
    const items = this.hooks.menuItems(id) || [];
    if (!items.length) return;
    this.menu.innerHTML = "";
    this.menu.dataset.nodeId = String(id);
    const buttons = [];
    for (const it of items) {
      if (it.separator) {
        this.menu.appendChild(el("div", "lab-menu-sep"));
        continue;
      }
      const b = el("button", `lab-menu-item${it.danger ? " lab-menu-danger" : ""}`);
      b.type = "button";
      b.setAttribute("role", "menuitem");
      b.textContent = it.label;
      b.disabled = Boolean(it.disabled);
      b.addEventListener("click", () => {
        this.closeMenu();
        this.hooks.onCommand(id, it.command);
      });
      this.menu.appendChild(b);
      buttons.push(b);
    }
    const wr = this.wrap.getBoundingClientRect();
    this.menu.hidden = false;
    const mw = this.menu.offsetWidth || 180;
    const mh = this.menu.offsetHeight || 200;
    const left = clamp(clientX - wr.left, 4, Math.max(4, wr.width - mw - 4));
    const top = clamp(clientY - wr.top, 4, Math.max(4, wr.height - mh - 4));
    this.menu.style.left = `${left}px`;
    this.menu.style.top = `${top}px`;
    this.menuFocusReturn = this.nodeEls.get(id)?.g || this.svg;
    const enabled = buttons.filter((b) => !b.disabled);
    if (enabled.length) enabled[0].focus();
    this.menu.onkeydown = (e) => {
      const list = buttons.filter((b) => !b.disabled);
      const i = list.indexOf(document.activeElement);
      if (e.key === "ArrowDown") {
        e.preventDefault();
        list[(i + 1) % list.length]?.focus();
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        list[(i - 1 + list.length) % list.length]?.focus();
      } else if (e.key === "Escape" || e.key === "Tab") {
        e.preventDefault();
        this.closeMenu(true);
      }
    };
    this.menuOutside = (e) => {
      if (!this.menu.contains(e.target)) this.closeMenu();
    };
    setTimeout(() => document.addEventListener("pointerdown", this.menuOutside, true), 0);
  }

  closeMenu(refocus = false) {
    if (this.menu.hidden) return;
    this.menu.hidden = true;
    this.menu.innerHTML = "";
    if (this.menuOutside) document.removeEventListener("pointerdown", this.menuOutside, true);
    this.menuOutside = null;
    if (refocus && this.menuFocusReturn) this.menuFocusReturn.focus();
  }
}

// ---- geometry ------------------------------------------------------------------------------------

function pairKey(a, b) {
  return a <= b ? `${a}-${b}` : `${b}-${a}`;
}

function endKey(end) {
  if (end.cluster) return "c";
  return end.node != null ? `n${end.node}` : `t${end.topic}`;
}

// The point where the segment from the centre `a` of a `w`×`h` box toward
// `b` leaves the box.
function trim(a, b, size) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  if (dx === 0 && dy === 0) return { ...a };
  const tx = dx !== 0 ? Math.abs(size.w / 2 / dx) : Infinity;
  const ty = dy !== 0 ? Math.abs(size.h / 2 / dy) : Infinity;
  const t = Math.min(tx, ty) + (Math.hypot(dx, dy) > 0 ? 6 / Math.hypot(dx, dy) : 0);
  return { x: a.x + dx * t, y: a.y + dy * t };
}

// By code point, so an emoji is never split in two.
function truncate(s, n) {
  const chars = Array.from(String(s ?? ""));
  return chars.length > n ? `${chars.slice(0, n - 1).join("")}…` : chars.join("");
}

// Puts as many of `parts` (joined by `sep`) into the SVG text `node` as fit in
// `maxW` canvas units, dropping whole parts from the end; when the first part
// alone is too long it is cut, starting from `n` characters. Where nothing can
// be measured (the canvas is not laid out) it falls back to `n` characters.
function fitText(node, parts, sep, maxW, n) {
  for (let i = parts.length; i > 0; i--) {
    node.textContent = parts.slice(0, i).join(sep);
    const w = node.getComputedTextLength();
    if (!w) {
      node.textContent = truncate(parts.join(sep), n);
      return;
    }
    if (w <= maxW) return;
  }
  const chars = Array.from(parts[0]);
  for (let m = Math.min(chars.length, n) - 1; m > 0; m--) {
    node.textContent = `${chars.slice(0, m).join("").trimEnd()}…`;
    if (node.getComputedTextLength() <= maxW) return;
  }
}
