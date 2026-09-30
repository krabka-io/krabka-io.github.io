// The palette: the left rail of the lab.
//
// Three tabs. "Build" adds nodes and edits the scenario's topics. "Scenarios"
// starts from a canned preset, reopens a saved scenario, and holds the file,
// share and settings actions. "Connect" hosts what reaches outside the tab:
// the multi-tab session and the kafkactl bridge, which the app builds and
// mounts through `connectBody`. The forms themselves come from `forms.js`; the
// palette only asks the app to open them.

import { el, button } from "./dom.js";
import { KINDS, kindOf } from "./kinds.js";
import { TabSet } from "./tabs.js";

const FULL_BUILD_NOTE = "needs the full build: this kind is not in the loaded module yet";

// The kinds the Build tab offers, grouped by what a reader is adding.
const GROUPS = [
  { title: "The cluster", help: "The servers that store and replicate records.", kinds: ["krabka-broker", "schema-registry"] },
  { title: "Clients and apps", help: "What writes to and reads from the cluster.", kinds: ["producer", "consumer", "streams"] },
  { title: "Network probes", help: "Cheap nodes for watching latency and cuts.", kinds: ["echo", "pinger"] },
];

// The first sentence of a kind's description: the one-line subtitle of its card.
function firstSentence(text) {
  const m = /^.*?[.!?](?=\s|$)/.exec(text || "");
  return m ? m[0] : text || "";
}

// "3 brokers · 1 producer · 2 consumers" for a preset's document.
function summarize(scenario) {
  const counts = new Map();
  for (const n of scenario.nodes) {
    const k = kindOf(n.kind);
    if (k.hidden) continue;
    const label = k.label.replace(/ \(real\)$/, "").replace(/^Krabka /, "").toLowerCase();
    counts.set(label, (counts.get(label) || 0) + 1);
  }
  const plural = (label) => (/y$/.test(label) ? `${label.slice(0, -1)}ies` : `${label}s`);
  return [...counts].map(([label, n]) => `${n} ${n > 1 ? plural(label) : label}`).join(" · ");
}

export class Palette {
  // hooks: onAddNode(kind), onAddTopic(), onEditTopic(name), onRemoveTopic(name),
  // onPreset(id), onOpenSaved(id), onDeleteSaved(id), onExport(), onImport(file),
  // onShare(), onSave(), onSettings(), onClear(), presets, listSaved() → Promise
  constructor(container, hooks) {
    this.hooks = hooks;
    this.availability = {};
    this.role = "solo";
    this.root = el("aside", "lab-palette");
    this.root.setAttribute("aria-label", "Palette");
    container.appendChild(this.root);

    this.tabs = new TabSet(this.root, {
      label: "Cluster tools",
      active: "build",
      className: "lab-rail-tabs",
      tabs: [
        { id: "build", label: "Build", title: "Add nodes and topics to the cluster" },
        { id: "scenarios", label: "Scenarios", title: "Load a preset, reopen a saved scenario, share or export" },
        { id: "connect", label: "Connect", title: "Host nodes in other tabs, or drive the cluster from kafkactl" },
      ],
      hooks: { onShow: (id) => id === "scenarios" && this.refreshSaved() },
    });
    this.buildTab();
    this.scenariosTab();
    this.connectBody = this.tabs.panel("connect");
  }

  show(id) {
    this.tabs.show(id);
  }

  // A titled block inside a tab. `.lab-pal-section` marks it for the checks.
  block(panel, title, help) {
    const section = el("section", "lab-pal-section");
    section.dataset.section = title.toLowerCase().replace(/[^a-z]+/g, "-");
    section.appendChild(el("h2", "lab-rail-heading", title));
    if (help) section.appendChild(el("p", "lab-rail-help", help));
    const body = el("div", "lab-pal-body");
    section.appendChild(body);
    panel.appendChild(section);
    return body;
  }

  buildTab() {
    const panel = this.tabs.panel("build");
    this.kindButtons = {};
    this.roleNote = el("p", "lab-note lab-small", "You joined a session: the host edits the scenario.");
    this.roleNote.hidden = true;
    panel.appendChild(this.roleNote);
    for (const group of GROUPS) {
      const body = this.block(panel, group.title, group.help);
      const grid = el("div", "lab-kind-grid");
      for (const kind of group.kinds) {
        const k = KINDS[kind];
        const b = el("button", "lab-kind-btn");
        b.type = "button";
        b.dataset.kind = kind;
        b.title = k.description;
        const glyph = el("span", "lab-kind-glyph", k.glyph);
        glyph.style.background = k.color;
        const text = el("span", "lab-kind-text");
        const head = el("span", "lab-kind-label", k.label);
        const badge = el("span", "lab-kind-badge", "full build");
        badge.hidden = true;
        head.appendChild(badge);
        text.append(head, el("span", "lab-kind-sub", firstSentence(k.description)));
        b.append(glyph, text, el("span", "lab-kind-add", "+"));
        b.addEventListener("click", () => this.hooks.onAddNode(kind));
        this.kindButtons[kind] = { button: b, badge };
        grid.appendChild(b);
      }
      body.appendChild(grid);
    }

    const topics = this.block(panel, "Topics", "Producers write to a topic and consumers read from it. Brokers need at least one.");
    this.topicList = el("ul", "lab-topic-list");
    topics.appendChild(this.topicList);
    this.topicAdd = button("+ Add topic", "lab-btn-sm", () => this.hooks.onAddTopic());
    topics.appendChild(this.topicAdd);
  }

  scenariosTab() {
    const panel = this.tabs.panel("scenarios");
    this.spokeNote = el("p", "lab-note lab-small", "You joined a session: the host picks the scenario, so presets, saves and imports are off here.");
    this.spokeNote.hidden = true;
    panel.appendChild(this.spokeNote);

    const current = this.block(panel, "This scenario");
    this.scenarioName = el("div", "lab-scenario-name");
    this.scenarioMeta = el("div", "lab-muted lab-small");
    current.append(this.scenarioName, this.scenarioMeta);
    const actions = el("div", "lab-palette-actions");
    this.settingsBtn = button("Settings…", "lab-btn-sm", () => this.hooks.onSettings(), { title: "Name, seed and link latency (restarts the world)" });
    this.saveBtn = button("Save", "lab-btn-sm", () => this.hooks.onSave(), { title: "Save to this browser now" });
    this.clearBtn = button("New (empty)", "lab-btn-sm", () => this.hooks.onClear(), { title: "Start an empty scenario" });
    actions.append(this.settingsBtn, this.saveBtn, this.clearBtn);
    current.appendChild(actions);
    this.saveState = el("p", "lab-muted lab-small");
    current.appendChild(this.saveState);

    const presets = this.block(panel, "Start from a preset", "Every preset runs real brokers. Pick one, press Play, then break something.");
    this.presetButtons = [];
    for (const p of this.hooks.presets) {
      const b = el("button", "lab-preset-btn");
      b.type = "button";
      b.dataset.preset = p.id;
      b.title = p.description;
      const head = el("span", "lab-preset-head");
      head.appendChild(el("span", "lab-preset-name", p.name));
      const badge = el("span", "lab-kind-badge", "full build");
      badge.hidden = true;
      head.appendChild(badge);
      b.append(head, el("span", "lab-preset-summary", summarize(p.scenario)), el("span", "lab-preset-desc", p.description));
      b.addEventListener("click", () => this.hooks.onPreset(p.id));
      this.presetButtons.push({ button: b, badge, preset: p });
      presets.appendChild(b);
    }

    const saved = this.block(panel, "Saved in this browser", "Autosaved scenarios reopen with their stored broker data.");
    this.savedList = el("ul", "lab-saved-list");
    saved.appendChild(this.savedList);

    const files = this.block(panel, "Share and files");
    const fileActions = el("div", "lab-palette-actions");
    this.shareBtn = button("Copy link", "lab-btn-sm", () => this.hooks.onShare(), { title: "Copy a URL that carries this scenario" });
    this.exportBtn = button("Export JSON", "lab-btn-sm", () => this.hooks.onExport());
    this.importInput = el("input");
    this.importInput.type = "file";
    this.importInput.accept = "application/json,.json";
    this.importInput.hidden = true;
    this.importInput.addEventListener("change", () => {
      const file = this.importInput.files && this.importInput.files[0];
      if (file) this.hooks.onImport(file);
      this.importInput.value = "";
    });
    this.importBtn = button("Import JSON", "lab-btn-sm", () => this.importInput.click());
    fileActions.append(this.shareBtn, this.exportBtn, this.importBtn, this.importInput);
    files.appendChild(fileActions);
  }

  // `data`: { scenario, availability, role, saveState }
  update(data) {
    const { scenario, availability, role, saveState } = data;
    this.availability = availability || {};
    this.role = role || "solo";
    const editable = this.role !== "spoke";
    for (const [kind, { button: b, badge }] of Object.entries(this.kindButtons)) {
      const ok = this.availability[kind] !== false;
      badge.hidden = ok;
      b.classList.toggle("lab-unavailable", !ok);
      b.title = ok ? KINDS[kind].description : `${KINDS[kind].description} (${FULL_BUILD_NOTE})`;
      b.disabled = !editable;
    }
    this.roleNote.hidden = editable;
    for (const { button: b, badge, preset } of this.presetButtons) {
      const kinds = new Set(preset.scenario.nodes.map((n) => n.kind));
      const ok = [...kinds].every((k) => this.availability[k] !== false);
      badge.hidden = ok;
      b.classList.toggle("lab-unavailable", !ok);
      b.title = ok ? preset.description : `${preset.description} (${FULL_BUILD_NOTE})`;
      b.disabled = !editable;
      const current = scenario?.name === preset.scenario.name;
      b.classList.toggle("lab-preset-current", current);
      if (current) b.setAttribute("aria-current", "true");
      else b.removeAttribute("aria-current");
    }
    this.topicAdd.disabled = !editable;
    this.renderTopics(scenario, editable);
    this.scenarioName.textContent = scenario?.name || "Untitled scenario";
    const n = scenario?.nodes?.length || 0;
    const t = scenario?.topics?.length || 0;
    const idText = scenario?.id ? ` · id ${scenario.id.slice(0, 8)}` : "";
    this.scenarioMeta.textContent = `${n} node${n === 1 ? "" : "s"} · ${t} topic${t === 1 ? "" : "s"} · seed ${scenario?.seed ?? "?"} · ${scenario?.links?.default_latency_ms ?? "?"} ms links${idText}`;
    for (const b of [this.settingsBtn, this.saveBtn, this.clearBtn, this.importBtn]) b.disabled = !editable;
    this.spokeNote.hidden = editable;
    for (const b of this.savedList.querySelectorAll(".lab-saved-open")) b.disabled = !editable;
    this.saveState.textContent = saveState || "";
  }

  renderTopics(scenario, editable) {
    const topics = scenario?.topics || [];
    const key = JSON.stringify(topics) + editable;
    if (key === this.topicKey) return;
    this.topicKey = key;
    this.topicList.innerHTML = "";
    if (!topics.length) this.topicList.appendChild(el("li", "lab-muted lab-small", "No topics yet."));
    topics.forEach((t, i) => {
      const li = el("li", "lab-topic-row");
      const name = el("span", "lab-topic-row-name", t.name);
      const meta = el("span", "lab-muted lab-small", `${t.partitions}p · rf ${t.replication_factor === -1 ? "default" : t.replication_factor}`);
      li.append(name, meta);
      if (editable) {
        li.appendChild(button("Edit", "lab-btn-sm", () => this.hooks.onEditTopic(t.name), { ariaLabel: `Edit topic ${t.name}` }));
        li.appendChild(button("×", "lab-btn-sm", () => this.removeTopic(t.name, i), { ariaLabel: `Remove topic ${t.name}` }));
      }
      this.topicList.appendChild(li);
    });
  }

  // The list is rebuilt without the row, taking the focused button with it:
  // focus goes to the remove button that took its place, or to Add topic.
  removeTopic(name, index) {
    this.hooks.onRemoveTopic(name);
    if (this.topicList.contains(document.activeElement)) return;
    const removes = this.topicList.querySelectorAll(".lab-topic-row button:last-child");
    (removes[Math.min(index, removes.length - 1)] || this.topicAdd).focus();
  }

  // Whether the saved list is on screen, so a save can refresh it.
  savedVisible() {
    return this.tabs.active === "scenarios";
  }

  async refreshSaved() {
    let rows = [];
    try {
      rows = await this.hooks.listSaved();
    } catch {
      rows = [];
    }
    this.savedList.innerHTML = "";
    if (!rows.length) {
      this.savedList.appendChild(el("li", "lab-muted lab-small", "Nothing saved in this browser yet."));
      return;
    }
    for (const r of rows) {
      const li = el("li", "lab-saved-row");
      li.dataset.savedId = r.id;
      const open = el("button", "lab-saved-open");
      open.type = "button";
      open.dataset.savedOpen = r.id;
      open.textContent = r.name || "Untitled scenario";
      open.title = `Open (saved ${new Date(r.updated).toLocaleString()})`;
      open.disabled = this.role === "spoke";
      open.addEventListener("click", () => this.hooks.onOpenSaved(r.id));
      const when = el("span", "lab-muted lab-small", timeAgo(r.updated));
      const del = button("×", "lab-btn-sm", () => this.hooks.onDeleteSaved(r.id), { ariaLabel: `Delete saved scenario ${r.name}` });
      li.append(open, when, del);
      this.savedList.appendChild(li);
    }
  }
}

function timeAgo(t) {
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} d ago`;
}

export { kindOf };
