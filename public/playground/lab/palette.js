// The palette: the left column of the lab.
//
// "Add node" offers one button per kind; "Topics" edits the scenario's
// topics; "Presets" loads a canned scenario; "Saved" lists the scenarios
// this browser keeps (with their durable state); "Scenario" holds the
// settings and the export / import / share / save buttons. The forms
// themselves come from `forms.js`; the palette only asks the app to open
// them.

import { el, button } from "./dom.js";
import { KINDS, KIND_ORDER, kindOf } from "./kinds.js";

const FULL_BUILD_NOTE = "needs the full build: this kind is not in the loaded module yet";

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

    // Add node.
    this.addSection = this.section("Add node", true);
    this.kindButtons = {};
    const grid = el("div", "lab-kind-grid");
    for (const kind of KIND_ORDER) {
      const k = KINDS[kind];
      const b = el("button", "lab-kind-btn");
      b.type = "button";
      b.dataset.kind = kind;
      b.title = k.description;
      const glyph = el("span", "lab-kind-glyph", k.glyph);
      glyph.style.background = k.color;
      const label = el("span", "lab-kind-label", k.label);
      const badge = el("span", "lab-kind-badge", "full build");
      badge.hidden = true;
      b.append(glyph, label, badge);
      b.addEventListener("click", () => hooks.onAddNode(kind));
      this.kindButtons[kind] = { button: b, badge };
      grid.appendChild(b);
    }
    this.addSection.body.appendChild(grid);
    this.roleNote = el("p", "lab-muted lab-small");
    this.roleNote.hidden = true;
    this.addSection.body.appendChild(this.roleNote);

    // Topics.
    this.topicSection = this.section("Topics", true);
    this.topicList = el("ul", "lab-topic-list");
    this.topicSection.body.appendChild(this.topicList);
    this.topicAdd = button("+ Add topic", "lab-btn-sm", () => hooks.onAddTopic());
    this.topicSection.body.appendChild(this.topicAdd);

    // Presets.
    this.presetSection = this.section("Presets", true);
    this.presetButtons = [];
    for (const p of hooks.presets) {
      const b = el("button", "lab-preset-btn");
      b.type = "button";
      b.dataset.preset = p.id;
      b.title = p.description;
      const name = el("span", "lab-preset-name", p.name);
      const badge = el("span", "lab-kind-badge", "full build");
      badge.hidden = true;
      b.append(name, badge);
      b.addEventListener("click", () => hooks.onPreset(p.id));
      this.presetButtons.push({ button: b, badge, preset: p });
      this.presetSection.body.appendChild(b);
    }

    // Saved scenarios.
    this.savedSection = this.section("Saved", false);
    this.savedList = el("ul", "lab-saved-list");
    this.savedSection.body.appendChild(this.savedList);
    this.savedSection.details.addEventListener("toggle", () => {
      if (this.savedSection.details.open) this.refreshSaved();
    });

    // Scenario.
    this.scenarioSection = this.section("Scenario", true);
    this.scenarioName = el("div", "lab-scenario-name");
    this.scenarioMeta = el("div", "lab-muted lab-small");
    this.scenarioSection.body.append(this.scenarioName, this.scenarioMeta);
    const actions = el("div", "lab-palette-actions");
    this.settingsBtn = button("Settings…", "lab-btn-sm", () => hooks.onSettings(), { title: "Name, seed and link latency (restarts the world)" });
    this.saveBtn = button("Save", "lab-btn-sm", () => hooks.onSave(), { title: "Save to this browser now" });
    this.exportBtn = button("Export JSON", "lab-btn-sm", () => hooks.onExport());
    this.importInput = el("input");
    this.importInput.type = "file";
    this.importInput.accept = "application/json,.json";
    this.importInput.hidden = true;
    this.importInput.addEventListener("change", () => {
      const file = this.importInput.files && this.importInput.files[0];
      if (file) hooks.onImport(file);
      this.importInput.value = "";
    });
    this.importBtn = button("Import JSON", "lab-btn-sm", () => this.importInput.click());
    this.shareBtn = button("Copy link", "lab-btn-sm", () => hooks.onShare(), { title: "Copy a URL that carries this scenario" });
    this.clearBtn = button("New (empty)", "lab-btn-sm", () => hooks.onClear(), { title: "Start an empty scenario" });
    actions.append(this.settingsBtn, this.saveBtn, this.exportBtn, this.importBtn, this.importInput, this.shareBtn, this.clearBtn);
    this.scenarioSection.body.appendChild(actions);
    this.saveState = el("p", "lab-muted lab-small");
    this.scenarioSection.body.appendChild(this.saveState);
  }

  section(title, open) {
    const details = el("details", "lab-pal-section");
    details.open = open;
    const summary = el("summary", "lab-panel-title", title);
    const body = el("div", "lab-pal-body");
    details.append(summary, body);
    this.root.appendChild(details);
    return { details, body };
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
    this.roleNote.textContent = "You joined a session: the host edits the scenario.";
    for (const { button: b, badge, preset } of this.presetButtons) {
      const kinds = new Set(preset.scenario.nodes.map((n) => n.kind));
      const ok = [...kinds].every((k) => this.availability[k] !== false);
      badge.hidden = ok;
      b.classList.toggle("lab-unavailable", !ok);
      b.title = ok ? preset.description : `${preset.description} (${FULL_BUILD_NOTE})`;
      b.disabled = !editable;
    }
    this.topicAdd.disabled = !editable;
    this.renderTopics(scenario, editable);
    this.scenarioName.textContent = scenario?.name || "Untitled scenario";
    const n = scenario?.nodes?.length || 0;
    const t = scenario?.topics?.length || 0;
    const idText = scenario?.id ? ` · id ${scenario.id.slice(0, 8)}` : "";
    this.scenarioMeta.textContent = `${n} node${n === 1 ? "" : "s"} · ${t} topic${t === 1 ? "" : "s"} · seed ${scenario?.seed ?? "?"} · ${scenario?.links?.default_latency_ms ?? "?"} ms links${idText}`;
    for (const b of [this.settingsBtn, this.clearBtn, this.importBtn]) b.disabled = !editable;
    this.saveState.textContent = saveState || "";
  }

  renderTopics(scenario, editable) {
    const topics = scenario?.topics || [];
    const key = JSON.stringify(topics) + editable;
    if (key === this.topicKey) return;
    this.topicKey = key;
    this.topicList.innerHTML = "";
    if (!topics.length) this.topicList.appendChild(el("li", "lab-muted lab-small", "No topics. Brokers need at least one for producers to write to."));
    for (const t of topics) {
      const li = el("li", "lab-topic-row");
      const name = el("span", "lab-topic-row-name", t.name);
      const meta = el("span", "lab-muted lab-small", `${t.partitions}p · rf ${t.replication_factor === -1 ? "default" : t.replication_factor}`);
      li.append(name, meta);
      if (editable) {
        li.appendChild(button("Edit", "lab-btn-sm", () => this.hooks.onEditTopic(t.name), { ariaLabel: `Edit topic ${t.name}` }));
        li.appendChild(button("×", "lab-btn-sm", () => this.hooks.onRemoveTopic(t.name), { ariaLabel: `Remove topic ${t.name}` }));
      }
      this.topicList.appendChild(li);
    }
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
