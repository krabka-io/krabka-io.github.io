// Why3 proof explorer.
//
// Renders the proof sessions why3find recorded for Krabka's Creusot-verified
// kernels. The page ships the sessions as JSON in `#proof-sessions`; this
// module turns them into a filterable list grouped by module, and for the
// selected session shows the proof tree (which tactic split each goal, which
// prover discharged each leaf and how long it took), the `[@expl]` obligations
// with their source spans, the generated Coma on demand, and, when the
// why3-web bundle is part of the build, a browser re-check that replays the
// recorded tree through Why3 and Alt-Ergo compiled to JavaScript.
//
// No bundler: this is a plain ES module loaded by
// src/pages/docs/proof-explorer.astro. Styles are in src/styles/proofs.css.

// ---- paths -------------------------------------------------------------------
//
// Site-relative. The page passes the deployment's base path in `data-base` on
// the root element, and `sitePath` prefixes it.

const COMA_DIR = "/proofs/coma/";
const WHY3_WEB_DIR = "/why3-web/";
const MANIFEST_PATH = WHY3_WEB_DIR + "manifest.json";
const WHY3_WORKER_PATH = WHY3_WEB_DIR + "proof_worker.js";
const ALT_ERGO_WORKER_PATH = WHY3_WEB_DIR + "alt-ergo-worker.js";
const VERIFICATION_PAGE = "/verification";

const SESSIONS_ID = "proof-sessions";
const ROOT_ID = "krabka-proofs";
const HASH_PREFIX = "#session=";
const GITHUB = "https://github.com";

// Alt-Ergo in the browser.
const ALT_ERGO_STEPS_BOUND = 5_000_000;
const ALT_ERGO_TIMEOUT_MS = 60_000;

const KINDS = [
  { key: "kernel", label: "kernels", on: true },
  { key: "helper", label: "helpers", on: true },
  { key: "lemma", label: "lemmas", on: true },
  { key: "derived", label: "derived impls", on: false },
];

const PROVERS = ["alt-ergo", "z3", "cvc5", "cvc4"];

// ---- state -------------------------------------------------------------------

let base = "";
let source = null;
let sessions = [];
let byId = new Map();
let selectedId = null;
let filterText = "";
const kindsOn = new Set(KINDS.filter((k) => k.on).map((k) => k.key));

// The DOM the module owns.
let root = null;
let listEl = null;
let countEl = null;
let detailEl = null;
let chipEls = new Map();

// Set while a session is rendered: leaf key -> status cell, so the live check
// can fill in results for the tree on screen.
let leafCells = new Map();

// What the manifest probe found: null while probing, false when the bundle is
// missing, or the manifest object.
let bundle = null;

// ---- helpers -----------------------------------------------------------------

function sitePath(path) {
  return base + path;
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

function link(href, text, cls, external) {
  const a = el("a", cls, text);
  a.href = href;
  if (external) {
    a.target = "_blank";
    a.rel = "noopener noreferrer";
  }
  return a;
}

function fmtCount(n) {
  return Number(n).toLocaleString("en-US");
}

function fmtTime(seconds) {
  if (!Number.isFinite(seconds)) return "?";
  if (seconds < 0.01) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 10) return `${seconds.toFixed(2)} s`;
  return `${seconds.toFixed(1)} s`;
}

function fmtMs(ms) {
  return fmtTime(ms / 1000);
}

function plural(n, one, many) {
  return `${fmtCount(n)} ${n === 1 ? one : many}`;
}

function githubBlob(path, line) {
  const url = `${GITHUB}/${source.repo}/blob/${source.commit}/${path}`;
  return line ? `${url}#L${line}` : url;
}

function proofJsonUrl(id) {
  return githubBlob(`verif/krabka_verified_rlib/${id}/proof.json`);
}

function comaGithubUrl(id) {
  return githubBlob(`verif/krabka_verified_rlib/${id}.coma`);
}

function comaSitePath(id) {
  return sitePath(`${COMA_DIR}${id}.coma`);
}

function displayName(session) {
  return session.impl ? `${session.impl}/${session.name}` : session.name;
}

function kindLabel(kind) {
  const k = KINDS.find((x) => x.key === kind);
  return kind === "derived" ? "derived impl" : k ? k.label.replace(/s$/, "") : kind;
}

function proverClass(prover) {
  return PROVERS.includes(prover) ? `px-prover-${prover}` : "px-prover-other";
}

// Walk a recorded tree; `visit(node, key, depth)` is called for every node.
// Keys are the path of child indices from the goal, so they are stable across
// renders and the live check can address a leaf without holding DOM nodes.
function walkTree(node, key, depth, visit) {
  visit(node, key, depth);
  if (node.tactic && Array.isArray(node.children)) {
    node.children.forEach((child, i) => walkTree(child, `${key}/${i}`, depth + 1, visit));
  }
}

function countLeaves(node) {
  if (!node.tactic) return 1;
  return (node.children || []).reduce((n, c) => n + countLeaves(c), 0);
}

function sumTime(node) {
  if (!node.tactic) return Number(node.time) || 0;
  return (node.children || []).reduce((n, c) => n + sumTime(c), 0);
}

// ---- hash --------------------------------------------------------------------

function idFromHash() {
  const hash = location.hash || "";
  if (!hash.startsWith(HASH_PREFIX)) return null;
  try {
    return decodeURIComponent(hash.slice(HASH_PREFIX.length));
  } catch {
    return null;
  }
}

function writeHash(id) {
  const hash = HASH_PREFIX + encodeURIComponent(id);
  if (location.hash !== hash) {
    // A hash change would scroll to a matching element; there is none, so the
    // page stays put and the listener below picks the selection up.
    history.pushState(null, "", hash);
  }
}

// ---- sidebar -----------------------------------------------------------------

function visibleSessions() {
  const needle = filterText.trim().toLowerCase();
  return sessions.filter((s) => {
    if (!kindsOn.has(s.kind)) return false;
    if (!needle) return true;
    return s.search.includes(needle);
  });
}

function renderList() {
  const visible = visibleSessions();
  listEl.replaceChildren();
  countEl.textContent = `${fmtCount(visible.length)} of ${fmtCount(sessions.length)} sessions`;
  for (const [key, chip] of chipEls) {
    chip.setAttribute("aria-pressed", String(kindsOn.has(key)));
  }
  if (visible.length === 0) {
    listEl.appendChild(el("p", "px-empty", "No session matches the filter."));
    return;
  }
  const groups = new Map();
  for (const s of visible) {
    if (!groups.has(s.module)) groups.set(s.module, []);
    groups.get(s.module).push(s);
  }
  for (const [module, rows] of groups) {
    const group = el("section", "px-group");
    const title = el("h3", "px-group-title");
    title.append(el("span", "", module), el("span", "px-group-n", fmtCount(rows.length)));
    group.appendChild(title);
    for (const s of rows) {
      const item = el("button", "px-item");
      item.type = "button";
      item.dataset.id = s.id;
      if (s.id === selectedId) item.classList.add("px-active");
      const name = el("span", "px-item-name", s.name);
      if (s.impl) name.prepend(el("span", "px-item-impl", s.impl));
      const meta = el("span", "px-item-meta", `${s.stats.leaves} · ${fmtTime(s.stats.time)}`);
      meta.title = `${plural(s.stats.leaves, "leaf", "leaves")}, ${fmtTime(s.stats.time)} prover time`;
      item.append(name, meta);
      item.addEventListener("click", () => {
        select(s.id, true);
      });
      group.appendChild(item);
    }
    listEl.appendChild(group);
  }
}

function markActive() {
  for (const item of listEl.querySelectorAll(".px-item")) {
    item.classList.toggle("px-active", item.dataset.id === selectedId);
  }
  const active = listEl.querySelector(".px-item.px-active");
  if (active && typeof active.scrollIntoView === "function") {
    active.scrollIntoView({ block: "nearest" });
  }
}

// ---- detail: head ------------------------------------------------------------

function renderHead(session) {
  const head = el("header", "px-head");
  const row = el("div", "px-title-row");
  const title = el("h2", "px-title");
  if (session.impl) {
    title.append(el("span", "px-title-impl", `${session.impl}/`), document.createTextNode(session.name));
  } else {
    title.textContent = session.name;
  }
  row.append(title, el("span", "px-module", session.module), el("span", `px-kind px-kind-${session.kind}`, kindLabel(session.kind)));
  head.appendChild(row);

  const links = el("div", "px-links");
  if (session.source) {
    links.appendChild(link(githubBlob(session.source.file, session.source.line), `Source ${session.source.file.split("/").pop()}:${session.source.line}`, "px-link", true));
  }
  links.appendChild(link(proofJsonUrl(session.id), "proof.json", "px-link", true));
  links.appendChild(link(comaGithubUrl(session.id), "Coma on GitHub", "px-link", true));
  if (session.ledger) {
    links.appendChild(link(sitePath(`${VERIFICATION_PAGE}#kernel-${session.ledger.row}`), "Ledger row", "px-link px-link-primary", false));
    if (Array.isArray(session.ledger.kernels) && session.ledger.kernels.length > 0) {
      links.appendChild(el("span", "px-ledger-kernels", session.ledger.kernels.join(", ")));
    }
  }
  head.appendChild(links);

  const st = session.stats;
  const provers = Object.entries(st.provers || {})
    .sort((a, b) => b[1] - a[1])
    .map(([p, n]) => `${p} ${n}`)
    .join(", ");
  const stats = el("p", "px-stats");
  const add = (label, value) => {
    const span = el("span");
    span.append(el("b", "", value), document.createTextNode(` ${label}`));
    stats.appendChild(span);
  };
  add(session.goals.length === 1 ? "goal" : "goals", fmtCount(session.goals.length));
  add(st.leaves === 1 ? "leaf" : "leaves", fmtCount(st.leaves));
  stats.appendChild(el("span", "", `provers: ${provers || "none"}`));
  add("total", fmtTime(st.time));
  add("max leaf", fmtTime(st.maxTime));
  add("depth", String(st.depth));
  if (st.stuck) add("stuck", fmtCount(st.stuck));
  head.appendChild(stats);
  return head;
}

// ---- detail: proof tree ------------------------------------------------------

function renderTree(session) {
  const section = el("section", "px-section");
  const shead = el("div", "px-section-head");
  shead.append(el("h3", "px-section-title", "Proof tree"), el("span", "px-section-hint", "goal → tactic → prover leaf; bar is the leaf's time against the session's slowest leaf"));
  section.appendChild(shead);

  const tree = el("div", "px-tree");
  const maxTime = Math.max(Number(session.stats.maxTime) || 0, 1e-6);
  leafCells = new Map();

  session.goals.forEach((goal, gi) => {
    const details = el("details", "px-goal");
    details.open = true;
    const summary = el("summary");
    const leaves = countLeaves(goal.tree);
    summary.append(el("span", "px-goal-name", goal.name), el("span", "px-goal-meta", `${plural(leaves, "leaf", "leaves")} · ${fmtTime(sumTime(goal.tree))}`));
    details.appendChild(summary);
    const holder = el("div", "px-children");
    let leafNo = 0;
    const render = (node, key, depth) => {
      if (node.tactic) {
        const tnode = el("details", "px-node");
        tnode.open = true;
        const ts = el("summary");
        ts.append(el("span", "px-node-name", node.tactic), el("span", "px-node-meta", plural((node.children || []).length, "child", "children")));
        tnode.appendChild(ts);
        const kids = el("div", "px-children");
        (node.children || []).forEach((child, i) => kids.appendChild(render(child, `${key}/${i}`, depth + 1)));
        tnode.appendChild(kids);
        return tnode;
      }
      leafNo += 1;
      const leaf = el("div", `px-leaf px-leaf-${node.prover}`);
      leaf.dataset.key = key;
      const time = Number(node.time) || 0;
      const bar = el("span", "px-bar");
      const fill = el("span");
      fill.style.width = `${Math.max(1, Math.min(100, (time / maxTime) * 100)).toFixed(1)}%`;
      bar.appendChild(fill);
      bar.title = `${fmtTime(time)} of ${fmtTime(maxTime)} max`;
      const live = el("span", "px-live");
      live.setAttribute("aria-live", "polite");
      leaf.append(el("span", "px-leaf-idx", String(leafNo)), el("span", `px-prover ${proverClass(node.prover)}`, node.prover), el("span", "px-leaf-time", fmtTime(time)), bar, live);
      leafCells.set(key, { cell: live, recorded: time, prover: node.prover });
      return leaf;
    };
    holder.appendChild(render(goal.tree, String(gi), 0));
    details.appendChild(holder);
    tree.appendChild(details);
  });
  section.appendChild(tree);
  return section;
}

// ---- detail: obligations -----------------------------------------------------

function spanText(span) {
  if (!span) return "";
  const file = span.file.split("/").pop();
  return `${file}:${span.line}`;
}

function spanIsInRepo(span) {
  return Boolean(span && /^crates\//.test(span.file));
}

function groupObligations(obligations) {
  const groups = [];
  for (const ob of obligations) {
    const last = groups[groups.length - 1];
    const same = last && last.expl === ob.expl && last.formula === ob.formula && JSON.stringify(last.span) === JSON.stringify(ob.span);
    if (same) last.count += 1;
    else groups.push({ ...ob, count: 1 });
  }
  return groups;
}

function renderObligations(session) {
  const section = el("section", "px-section");
  const shead = el("div", "px-section-head");
  shead.appendChild(el("h3", "px-section-title", "Obligations"));
  const obligations = Array.isArray(session.obligations) ? session.obligations : [];
  if (session.kind === "derived") {
    section.append(shead, el("p", "px-note", "The page does not list obligations for derived Clone implementations; open the Coma below to read the generated conditions."));
    return section;
  }
  if (obligations.length === 0) {
    section.append(shead, el("p", "px-note", "The Coma file for this session carries no [@expl] labels."));
    return section;
  }
  const groups = groupObligations(obligations);
  const hint = el("span", "px-section-hint", `${plural(obligations.length, "condition", "conditions")}, ${plural(groups.length, "distinct row", "distinct rows")}`);
  const filter = el("input", "px-input px-obfilter");
  filter.type = "search";
  filter.placeholder = "Filter by label, file or formula";
  filter.setAttribute("aria-label", "Filter obligations");
  filter.autocomplete = "off";
  filter.spellcheck = false;
  shead.append(hint, filter);
  section.appendChild(shead);

  const list = el("div", "px-oblist");
  const rows = groups.map((g) => {
    const row = el("div", "px-ob");
    const h = el("div", "px-ob-head");
    h.appendChild(el("span", "px-ob-expl", g.expl || "(unlabelled)"));
    if (g.count > 1) h.appendChild(el("span", "px-ob-count", `×${g.count}`));
    if (g.span) {
      const text = spanText(g.span);
      if (spanIsInRepo(g.span)) {
        const a = link(githubBlob(g.span.file, g.span.line), text, "px-ob-span", true);
        a.title = `${g.span.file}:${g.span.line}:${g.span.col}`;
        h.appendChild(a);
      } else {
        const s = el("span", "px-ob-span", text);
        s.title = `${g.span.file}:${g.span.line}:${g.span.col} (outside the repository)`;
        h.appendChild(s);
      }
    }
    row.appendChild(h);
    if (g.formula) row.appendChild(el("pre", "px-ob-formula", g.formula));
    row.dataset.search = `${g.expl || ""} ${g.span ? g.span.file : ""} ${g.formula || ""}`.toLowerCase();
    list.appendChild(row);
    return row;
  });
  const empty = el("p", "px-empty", "No obligation matches the filter.");
  empty.hidden = true;
  section.append(list, empty);
  filter.addEventListener("input", () => {
    const needle = filter.value.trim().toLowerCase();
    let shown = 0;
    for (const row of rows) {
      const hit = !needle || row.dataset.search.includes(needle);
      row.hidden = !hit;
      if (hit) shown += 1;
    }
    empty.hidden = shown > 0;
  });
  return section;
}

// ---- detail: Coma ------------------------------------------------------------

function renderComa(session) {
  const details = el("details", "px-section px-coma");
  const summary = el("summary");
  const size = session.coma && session.coma.bytes ? `${fmtCount(session.coma.bytes)} bytes` : "";
  summary.append(el("h3", "px-section-title", "Generated Coma"), el("span", "px-section-hint", `${session.id}.coma${size ? ", " + size : ""}, fetched when opened`));
  details.appendChild(summary);
  const body = el("div", "px-coma-body");
  const note = el("p", "px-note");
  note.append(
    document.createTextNode("Creusot translates the Rust function into this Coma program; Why3 derives the verification conditions from it. The "),
    el("code", "", "[%#span]"),
    document.createTextNode(" markers on each condition refer to the "),
    el("code", "", "let%span"),
    document.createTextNode(" declarations at the top, which name the source file, line and column the condition came from."),
  );
  body.appendChild(note);
  const pre = el("pre", "px-pre", "");
  pre.hidden = true;
  const status = el("p", "px-note", "");
  body.append(status, pre);
  details.appendChild(body);
  let fetched = false;
  details.addEventListener("toggle", async () => {
    if (!details.open || fetched) return;
    fetched = true;
    status.textContent = "Fetching the Coma file...";
    try {
      const text = await fetchComa(session.id);
      pre.textContent = text;
      pre.hidden = false;
      status.textContent = `${fmtCount(text.split("\n").length)} lines.`;
    } catch (err) {
      fetched = false;
      status.textContent = `Could not fetch ${comaSitePath(session.id)}: ${err && err.message ? err.message : err}`;
      status.classList.add("px-error");
    }
  });
  return details;
}

const comaCache = new Map();

async function fetchComa(id) {
  if (comaCache.has(id)) return comaCache.get(id);
  const res = await fetch(comaSitePath(id));
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const text = await res.text();
  comaCache.set(id, text);
  return text;
}

// ---- detail: assembly ----------------------------------------------------------

function renderDetail(session) {
  detailEl.replaceChildren();
  if (!session) {
    detailEl.appendChild(el("p", "px-empty", "Pick a session from the list."));
    return;
  }
  detailEl.append(renderHead(session), renderTree(session), renderObligations(session), renderComa(session), renderCheckPanel(session));
}

function select(id, pushHash) {
  const session = byId.get(id);
  if (!session) return false;
  selectedId = id;
  if (!kindsOn.has(session.kind)) {
    kindsOn.add(session.kind);
    renderList();
  } else {
    markActive();
  }
  renderDetail(session);
  if (pushHash) writeHash(id);
  return true;
}

// ---- boot ----------------------------------------------------------------------

function readSessions() {
  const node = document.getElementById(SESSIONS_ID);
  if (!node) throw new Error(`missing #${SESSIONS_ID}`);
  const data = JSON.parse(node.textContent);
  if (!data || !Array.isArray(data.sessions)) throw new Error("proof sessions JSON has no sessions array");
  return data;
}

function buildShell() {
  root.replaceChildren();
  const layout = el("div", "px-layout");

  const sidebar = el("aside", "px-sidebar");
  const filter = el("input", "px-input px-filter");
  filter.type = "search";
  filter.placeholder = "Filter by name, module or kernel";
  filter.setAttribute("aria-label", "Filter sessions");
  filter.autocomplete = "off";
  filter.spellcheck = false;
  filter.addEventListener("input", () => {
    filterText = filter.value;
    renderList();
  });
  const chips = el("div", "px-chips");
  chips.setAttribute("role", "group");
  chips.setAttribute("aria-label", "Session kinds");
  chipEls = new Map();
  for (const kind of KINDS) {
    const n = sessions.filter((s) => s.kind === kind.key).length;
    const chip = el("button", "px-chip");
    chip.type = "button";
    chip.setAttribute("aria-pressed", String(kindsOn.has(kind.key)));
    chip.append(document.createTextNode(kind.label), el("span", "px-chip-n", fmtCount(n)));
    chip.addEventListener("click", () => {
      if (kindsOn.has(kind.key)) kindsOn.delete(kind.key);
      else kindsOn.add(kind.key);
      renderList();
    });
    chipEls.set(kind.key, chip);
    chips.appendChild(chip);
  }
  countEl = el("p", "px-count");
  countEl.setAttribute("role", "status");
  listEl = el("nav", "px-list");
  listEl.setAttribute("aria-label", "Proof sessions");
  sidebar.append(filter, chips, countEl, listEl);

  detailEl = el("section", "px-detail");
  detailEl.setAttribute("aria-live", "polite");
  layout.append(sidebar, detailEl);
  root.appendChild(layout);
}

function main() {
  root = document.getElementById(ROOT_ID);
  if (!root) return;
  try {
    base = (root.dataset.base || "").replace(/\/$/, "");
    const data = readSessions();
    source = data.source;
    sessions = data.sessions.map((s) => ({
      ...s,
      search: [s.id, s.name, s.impl || "", s.module, s.kind, ...(s.ledger && s.ledger.kernels ? s.ledger.kernels : [])].join(" ").toLowerCase(),
    }));
    byId = new Map(sessions.map((s) => [s.id, s]));
    buildShell();
    root.classList.add("px-ready");

    const wanted = idFromHash();
    renderList();
    if (!(wanted && select(wanted, false))) {
      const first = visibleSessions()[0];
      if (first) select(first.id, false);
    }
    window.addEventListener("hashchange", () => {
      const id = idFromHash();
      if (id && id !== selectedId) select(id, false);
    });
    window.addEventListener("popstate", () => {
      const id = idFromHash();
      if (id && id !== selectedId) select(id, false);
    });
    probeBundle();
  } catch (err) {
    const p = el("p", "px-error", `The proof explorer could not start: ${err && err.message ? err.message : err}`);
    root.appendChild(p);
  }
}

// =============================================================================
// Browser re-check
// =============================================================================
//
// Two web workers from the why3-web bundle, both optional in a build:
//
//   WHY3_WORKER_PATH      Why3 as a worker. Requests and replies are JSON
//                         strings; strictly one reply per request, in order.
//     {"cmd":"ping"}                        -> {"kind":"pong","why3":..,"prover":..}
//     {"cmd":"load","name":..,"content":..} -> {"kind":"loaded","name":..,"theories":[{"name":..,"goals":[{"id","name","expl"}]}]}
//     {"cmd":"transform","id":..,"name":..} -> {"kind":"children","id":..,"name":..,"children":[{"id","expl"}]}
//     {"cmd":"task","id":..}                -> {"kind":"task","id":..,"name":..,"expl":..,"text":..,"pretty":..}
//     any failure                           -> {"kind":"error","cmd":..,"id":..,"message":..}
//
//   ALT_ERGO_WORKER_PATH  Alt-Ergo 2.6.2 over its Dolmen solving loop, in a
//                         worker of our own. JSON strings both ways, one task
//                         at a time per worker:
//     {"id":n,"filename":"task.smt2","content":<task.text>,"steps":5000000}
//       -> {"id":n,"status":"unsat"|"sat"|"unknown"|"timeout"|"error","output":..,"diagnostic":..,"exception":..,"ms":n}
//                         where "unsat" means proved.
//
// Replay: for each recorded goal, find the loaded goal by name and walk the
// recorded tree. A tactic node sends `transform` on the current Why3 task and
// pairs the returned children with the recorded children in order; a count
// mismatch marks the subtree diverged. A leaf recorded with alt-ergo fetches
// its `task` text (SMT-LIB, from Why3's driver for Alt-Ergo's Dolmen front
// end) and hands it to the Alt-Ergo pool; a leaf recorded with another prover
// is shown as recorded only.

let why3 = null;
let altErgoPool = null;
let checkRunning = false;

async function probeBundle() {
  try {
    const res = await fetch(sitePath(MANIFEST_PATH), { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const manifest = await res.json();
    bundle = manifest && typeof manifest === "object" ? manifest : false;
  } catch {
    bundle = false;
  }
  const session = byId.get(selectedId);
  const panel = detailEl && detailEl.querySelector(".px-check");
  if (session && panel) panel.replaceWith(renderCheckPanel(session));
}

function renderCheckPanel(session) {
  const panel = el("section", "px-section px-check");
  const shead = el("div", "px-section-head");
  shead.appendChild(el("h3", "px-section-title", "Re-check in your browser"));
  panel.appendChild(shead);

  if (bundle === null) {
    panel.appendChild(el("p", "px-note", "Looking for the why3-web bundle..."));
    return panel;
  }
  if (bundle === false) {
    panel.appendChild(el("p", "px-note", "The browser re-check is not part of this build: the why3-web bundle (Why3 and Alt-Ergo compiled to JavaScript) was not published with the site."));
    panel.appendChild(
      el(
        "p",
        "px-note",
        "With the bundle present, this panel loads the session's Coma file into Why3 running in a web worker, applies the recorded split_vc and compute_specified tactics to reproduce the same leaves, and hands each leaf as SMT-LIB to Alt-Ergo in a second worker. Each leaf then shows whether the browser reproduced the recorded proof and how long it took beside the recorded time.",
      ),
    );
    return panel;
  }

  const versions = [];
  if (bundle.why3) versions.push(`Why3 ${bundle.why3}`);
  if (bundle.alt_ergo) versions.push(`Alt-Ergo ${bundle.alt_ergo}`);
  if (bundle.creusot) versions.push(`Creusot prelude ${bundle.creusot}`);
  shead.appendChild(el("span", "px-section-hint", versions.join(", ") + (bundle.built ? `, built ${String(bundle.built).slice(0, 10)}` : "")));

  const altLeaves = [];
  const otherLeaves = [];
  session.goals.forEach((goal, gi) => {
    walkTree(goal.tree, String(gi), 0, (node) => {
      if (node.tactic) return;
      if (node.prover === "alt-ergo") altLeaves.push(node);
      else otherLeaves.push(node);
    });
  });

  const note = el("p", "px-note");
  note.textContent =
    `Replays the recorded tree: Why3 loads the Coma file and splits it with the recorded tactics, then Alt-Ergo compiled to JavaScript tries each of the ${plural(altLeaves.length, "leaf", "leaves")} recorded with alt-ergo` +
    (otherLeaves.length > 0 ? `; the ${plural(otherLeaves.length, "leaf", "leaves")} recorded with ${[...new Set(otherLeaves.map((l) => l.prover))].join(", ")} stay as recorded.` : ".") +
    ` Up to ${Math.max(1, navigator.hardwareConcurrency || 2)} Alt-Ergo workers run in parallel with a ${ALT_ERGO_TIMEOUT_MS / 1000} s limit per leaf.`;
  panel.appendChild(note);

  const actions = el("div", "px-check-actions");
  const button = el("button", "px-btn px-primary", "Re-check this session");
  button.type = "button";
  const status = el("p", "px-check-status", "");
  actions.append(button, status);
  panel.appendChild(actions);
  const log = el("ul", "px-check-log");
  log.hidden = true;
  panel.appendChild(log);

  button.addEventListener("click", async () => {
    if (checkRunning) return;
    checkRunning = true;
    button.disabled = true;
    button.textContent = "Re-checking...";
    log.replaceChildren();
    log.hidden = false;
    panel.classList.remove("px-check-ok", "px-check-partial", "px-check-failed");
    try {
      const summary = await recheckSession(session, {
        status: (text) => {
          status.textContent = text;
        },
        log: (text, isError) => {
          const li = el("li", isError ? "px-log-error" : "", text);
          log.appendChild(li);
          log.scrollTop = log.scrollHeight;
        },
      });
      status.textContent = summary.text;
      panel.classList.add(summary.proved === summary.attempted && summary.attempted > 0 ? "px-check-ok" : summary.proved > 0 ? "px-check-partial" : "px-check-failed");
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      status.textContent = `The re-check stopped: ${message}`;
      const li = el("li", "px-log-error", message);
      log.appendChild(li);
      panel.classList.add("px-check-failed");
    } finally {
      checkRunning = false;
      button.disabled = false;
      button.textContent = "Re-check again";
    }
  });
  return panel;
}

// ---- Why3 worker: one request in flight at a time -----------------------------

class Why3Worker {
  constructor(url) {
    this.worker = new Worker(url);
    this.pending = [];
    this.worker.onmessage = (event) => {
      const next = this.pending.shift();
      if (!next) return;
      let reply;
      try {
        reply = typeof event.data === "string" ? JSON.parse(event.data) : event.data;
      } catch (err) {
        next.reject(new Error(`Why3 worker sent unparsable data: ${err.message}`));
        return;
      }
      if (reply && reply.kind === "error") {
        next.reject(new Error(`Why3 ${reply.cmd || "request"}${reply.id ? ` #${reply.id}` : ""}: ${reply.message}`));
      } else {
        next.resolve(reply);
      }
    };
    this.worker.onerror = (event) => {
      const err = new Error(`Why3 worker failed: ${event.message || "unknown error"}`);
      const waiting = this.pending.splice(0);
      for (const p of waiting) p.reject(err);
    };
  }

  // Requests are serialised: the worker answers strictly one reply per request
  // in order, so the queue below is resolved in the same order. The chain
  // waits for the previous request before posting the next one.
  request(msg) {
    const run = () =>
      new Promise((resolve, reject) => {
        this.pending.push({ resolve, reject });
        try {
          this.worker.postMessage(JSON.stringify(msg));
        } catch (err) {
          this.pending.pop();
          reject(err);
        }
      });
    const p = (this.chain || Promise.resolve()).then(run, run);
    this.chain = p.catch(() => {});
    return p;
  }

  terminate() {
    this.worker.terminate();
  }
}

async function getWhy3() {
  if (why3) return why3;
  const w = new Why3Worker(sitePath(WHY3_WORKER_PATH));
  const pong = await w.request({ cmd: "ping" });
  if (!pong || pong.kind !== "pong") throw new Error("Why3 worker did not answer the ping");
  w.info = pong;
  why3 = w;
  return w;
}

// ---- Alt-Ergo pool ----------------------------------------------------------------

class AltErgoPool {
  constructor(url, size) {
    this.url = url;
    this.size = Math.max(1, size);
    this.slots = [];
    this.queue = [];
    this.nextId = 1;
    for (let i = 0; i < this.size; i += 1) this.slots.push({ worker: null, job: null, timer: null });
  }

  spawn(slot) {
    const worker = new Worker(this.url);
    worker.onmessage = (event) => this.onReply(slot, event.data);
    worker.onerror = (event) => this.finish(slot, { kind: "error", message: `Alt-Ergo worker failed: ${event.message || "unknown error"}` });
    slot.worker = worker;
  }

  // Proves `text` (SMT-LIB from Why3's driver). Resolves to
  // {kind: "proved"|"unproved"|"timeout"|"error", message, ms}, where `ms` is
  // the worker's own solving time when it reports one, else wall-clock.
  prove(text) {
    return new Promise((resolve) => {
      this.queue.push({ text, resolve });
      this.pump();
    });
  }

  pump() {
    for (const slot of this.slots) {
      if (slot.job || this.queue.length === 0) continue;
      const job = this.queue.shift();
      slot.job = job;
      job.id = this.nextId;
      this.nextId += 1;
      job.started = performance.now();
      if (!slot.worker) this.spawn(slot);
      const request = JSON.stringify({ id: job.id, filename: "task.smt2", content: job.text, steps: ALT_ERGO_STEPS_BOUND });
      slot.timer = setTimeout(() => {
        // A task past the limit is abandoned with its worker, since the worker
        // cannot be interrupted; the slot gets a fresh one for the next task.
        slot.worker.terminate();
        slot.worker = null;
        this.finish(slot, { kind: "timeout", message: `no answer within ${ALT_ERGO_TIMEOUT_MS / 1000} s` });
      }, ALT_ERGO_TIMEOUT_MS);
      try {
        slot.worker.postMessage(request);
      } catch (err) {
        this.finish(slot, { kind: "error", message: `could not post to the Alt-Ergo worker: ${err.message}` });
      }
    }
  }

  onReply(slot, data) {
    let reply;
    try {
      reply = typeof data === "string" ? JSON.parse(data) : data;
    } catch (err) {
      this.finish(slot, { kind: "error", message: `Alt-Ergo worker sent unparsable data: ${err.message}` });
      return;
    }
    if (!slot.job) return;
    if (reply && typeof reply.id === "number" && reply.id !== slot.job.id) return;
    const status = reply && typeof reply.status === "string" ? reply.status : "";
    const diagnostic = String((reply && reply.diagnostic) || "").trim();
    const exception = String((reply && reply.exception) || "").trim();
    const detail = [diagnostic, exception].filter(Boolean).join(" ");
    const ms = reply && Number.isFinite(reply.ms) ? Number(reply.ms) : undefined;
    if (status === "unsat") {
      this.finish(slot, { kind: "proved", message: diagnostic || "unsat" }, ms);
    } else if (status === "sat" || status === "unknown") {
      this.finish(slot, { kind: "unproved", message: `${status}${detail ? " " + detail : ""}` }, ms);
    } else if (status === "timeout") {
      this.finish(slot, { kind: "timeout", message: `step budget exhausted${detail ? ": " + detail : ""}` }, ms);
    } else if (status === "error") {
      this.finish(slot, { kind: "error", message: detail || String((reply && reply.output) || "").trim() || "Alt-Ergo reported an error" }, ms);
    } else {
      this.finish(slot, { kind: "error", message: `unexpected reply: ${JSON.stringify(reply).slice(0, 200)}` }, ms);
    }
  }

  finish(slot, outcome, ms) {
    const job = slot.job;
    if (!job) return;
    clearTimeout(slot.timer);
    slot.timer = null;
    slot.job = null;
    job.resolve({ ...outcome, ms: ms === undefined ? performance.now() - job.started : ms });
    this.pump();
  }

  terminate() {
    for (const slot of this.slots) {
      clearTimeout(slot.timer);
      if (slot.worker) slot.worker.terminate();
      slot.worker = null;
      slot.job = null;
    }
    for (const job of this.queue.splice(0)) job.resolve({ kind: "error", message: "pool shut down", ms: 0 });
  }
}

function getAltErgoPool() {
  if (!altErgoPool) altErgoPool = new AltErgoPool(sitePath(ALT_ERGO_WORKER_PATH), navigator.hardwareConcurrency || 2);
  return altErgoPool;
}

// ---- replay ---------------------------------------------------------------------

function setLeaf(key, state, text) {
  const entry = leafCells.get(key);
  if (!entry) return;
  entry.cell.className = `px-live px-live-${state}`;
  entry.cell.textContent = text;
}

async function recheckSession(session, ui) {
  const started = performance.now();
  const totals = { attempted: 0, proved: 0, unproved: 0, timeout: 0, error: 0, skipped: 0, diverged: 0, missing: 0 };

  ui.status("Starting Why3...");
  const w = await getWhy3();
  ui.log(`Why3 ${w.info.why3}, driver for ${w.info.prover}`);
  const pool = getAltErgoPool();

  ui.status("Fetching the Coma file...");
  const coma = await fetchComa(session.id);
  ui.status("Loading the Coma file into Why3...");
  const loaded = await w.request({ cmd: "load", name: session.name, content: coma });
  const loadedGoals = new Map();
  for (const theory of loaded.theories || []) {
    for (const goal of theory.goals || []) loadedGoals.set(goal.name, goal.id);
  }
  ui.log(`Loaded ${plural(loadedGoals.size, "goal", "goals")} from ${session.id}.coma`);

  // Reset the status cells.
  for (const key of leafCells.keys()) setLeaf(key, "running", "queued");

  const proofs = [];
  let leafNo = 0;

  const markSubtree = (node, key, state, text, counter) => {
    walkTree(node, key, 0, (n, k) => {
      if (n.tactic) return;
      setLeaf(k, state, text);
      totals[counter] += 1;
    });
  };

  const walk = async (node, key, taskId) => {
    if (node.tactic) {
      let reply;
      try {
        reply = await w.request({ cmd: "transform", id: taskId, name: node.tactic });
      } catch (err) {
        ui.log(`${node.tactic} on task ${taskId}: ${err.message}`, true);
        markSubtree(node, key, "error", "transform failed", "error");
        return;
      }
      const children = Array.isArray(reply.children) ? reply.children : [];
      const recorded = node.children || [];
      if (children.length !== recorded.length) {
        ui.log(`${node.tactic} on task ${taskId} gave ${children.length} children, the session recorded ${recorded.length}: subtree diverged`, true);
        markSubtree(node, key, "diverged", `diverged (${children.length} vs ${recorded.length})`, "diverged");
        return;
      }
      for (let i = 0; i < recorded.length; i += 1) {
        await walk(recorded[i], `${key}/${i}`, children[i].id);
      }
      return;
    }
    leafNo += 1;
    if (node.prover !== "alt-ergo") {
      setLeaf(key, "skipped", `recorded only (${node.prover})`);
      totals.skipped += 1;
      return;
    }
    let task;
    try {
      setLeaf(key, "running", "printing task");
      task = await w.request({ cmd: "task", id: taskId });
    } catch (err) {
      ui.log(`task ${taskId}: ${err.message}`, true);
      setLeaf(key, "error", "no task text");
      totals.error += 1;
      return;
    }
    totals.attempted += 1;
    setLeaf(key, "running", "alt-ergo running");
    const n = leafNo;
    const p = pool.prove(task.text).then((outcome) => {
      const time = fmtMs(outcome.ms);
      const recorded = fmtTime(node.time);
      if (outcome.kind === "proved") {
        totals.proved += 1;
        setLeaf(key, "proved", `proved ${time} (rec. ${recorded})`);
      } else if (outcome.kind === "timeout") {
        totals.timeout += 1;
        setLeaf(key, "timeout", `timed out in the browser after ${time}`);
        ui.log(`leaf ${n} (${task.expl || task.name || taskId}): ${outcome.message}`, true);
      } else if (outcome.kind === "unproved") {
        totals.unproved += 1;
        setLeaf(key, "unproved", `not proved (${outcome.message}) ${time}`);
        ui.log(`leaf ${n} (${task.expl || task.name || taskId}): ${outcome.message}`, true);
      } else {
        totals.error += 1;
        setLeaf(key, "error", `error ${time}`);
        ui.log(`leaf ${n} (${task.expl || task.name || taskId}): ${outcome.message}`, true);
      }
      ui.status(`${fmtCount(totals.proved)} of ${fmtCount(totals.attempted)} leaves re-proved so far, ${fmtMs(performance.now() - started)} elapsed`);
    });
    proofs.push(p);
  };

  for (let gi = 0; gi < session.goals.length; gi += 1) {
    const goal = session.goals[gi];
    const id = loadedGoals.get(goal.name);
    if (id === undefined) {
      ui.log(`goal ${goal.name} is not in the loaded file`, true);
      markSubtree(goal.tree, String(gi), "error", "goal not found", "missing");
      continue;
    }
    ui.status(`Walking ${goal.name}...`);
    await walk(goal.tree, String(gi), id);
  }

  ui.status(`Waiting for Alt-Ergo on ${plural(totals.attempted, "leaf", "leaves")}...`);
  await Promise.all(proofs);

  const elapsed = performance.now() - started;
  const parts = [];
  if (totals.skipped) parts.push(`${fmtCount(totals.skipped)} recorded with another prover`);
  if (totals.diverged) parts.push(`${fmtCount(totals.diverged)} diverged`);
  if (totals.missing) parts.push(`${fmtCount(totals.missing)} not found`);
  if (totals.timeout) parts.push(`${fmtCount(totals.timeout)} timed out`);
  if (totals.unproved) parts.push(`${fmtCount(totals.unproved)} not proved`);
  if (totals.error) parts.push(`${fmtCount(totals.error)} errors`);
  const text = `${fmtCount(totals.proved)} of ${fmtCount(totals.attempted)} leaves re-proved in the browser in ${fmtMs(elapsed)}` + (parts.length ? ` (${parts.join(", ")})` : "") + ".";
  ui.log(text);
  return { ...totals, text };
}

// ---- go -----------------------------------------------------------------------------

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", main);
} else {
  main();
}
