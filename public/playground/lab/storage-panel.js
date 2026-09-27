// The Storage panel: what this browser keeps for the current scenario.
//
// Per node the bytes and entries stored, the total, a "forget" button per
// node and for the whole scenario, and the "Persist to this browser" toggle.
// The numbers come from a cursor walk over IndexedDB every few seconds while
// the panel is open.

import { el, button, fmtBytes, fmtNum } from "./dom.js";

const REFRESH_MS = 2500;

export class StoragePanel {
  // hooks: storage (LabStorage), scenarioId() → string, nodes() → [{ id, name }],
  // onForgetNode(id), onForgetScenario(), onPersistChange(on), onToast(msg)
  constructor(container, hooks) {
    this.hooks = hooks;
    this.root = el("details", "lab-storage lab-side-section");
    this.root.open = false;
    const summary = el("summary", "lab-panel-title", "Storage");
    this.root.appendChild(summary);
    const body = el("div", "lab-side-body");
    this.root.appendChild(body);

    const toggleRow = el("label", "lab-field-inline");
    this.toggle = el("input");
    this.toggle.type = "checkbox";
    this.toggle.checked = hooks.storage.persist;
    this.toggle.disabled = !hooks.storage.available;
    this.toggle.addEventListener("change", () => {
      hooks.onPersistChange(this.toggle.checked);
      this.refresh();
    });
    toggleRow.append(this.toggle, el("span", null, "Persist to this browser"));
    body.appendChild(toggleRow);
    body.appendChild(
      el(
        "p",
        "lab-muted lab-small",
        hooks.storage.available
          ? "Durable node state (logs, metadata, schemas) lives only in this browser's IndexedDB. It never leaves this machine."
          : "This browser has no IndexedDB, so nothing is kept across reloads.",
      ),
    );
    this.summaryLine = el("p", "lab-storage-total");
    body.appendChild(this.summaryLine);
    this.table = el("table", "lab-table lab-storage-table");
    body.appendChild(this.table);
    this.actions = el("div", "lab-form-actions");
    this.actions.appendChild(
      button("Forget stored data", "lab-btn-sm lab-danger", () => hooks.onForgetScenario(), { title: "Drop every stored log and key of this scenario" }),
    );
    body.appendChild(this.actions);
    container.appendChild(this.root);

    this.root.addEventListener("toggle", () => {
      if (this.root.open) this.refresh();
    });
    this.timer = setInterval(() => {
      if (this.root.open) this.refresh();
    }, REFRESH_MS);
    this.refresh();
  }

  async refresh() {
    this.toggle.checked = this.hooks.storage.persist;
    const id = this.hooks.scenarioId();
    const nodes = this.hooks.nodes();
    if (!id) {
      this.summaryLine.textContent = "Nothing stored yet: this scenario has no saved identity.";
      this.table.innerHTML = "";
      return;
    }
    let usage;
    try {
      usage = await this.hooks.storage.usage(id);
    } catch (err) {
      this.summaryLine.textContent = `Storage unavailable: ${err.message}`;
      return;
    }
    const t = usage.total;
    this.summaryLine.textContent = `Scenario ${id.slice(0, 8)}: ${fmtBytes(t.bytes)} in ${fmtNum(t.logEntries)} log entries and ${fmtNum(t.kvEntries)} keys${
      this.hooks.storage.persist ? "" : " · persistence off, new changes are dropped"
    }`;
    this.table.innerHTML = "";
    const thead = el("thead");
    const hr = el("tr");
    for (const h of ["node", "bytes", "log entries", "keys", ""]) hr.appendChild(el("th", null, h));
    thead.appendChild(hr);
    const tbody = el("tbody");
    const ids = new Set([...nodes.map((n) => String(n.id)), ...Object.keys(usage.nodes)]);
    for (const key of [...ids].sort((a, b) => Number(a) - Number(b))) {
      const u = usage.nodes[key] || { bytes: 0, logEntries: 0, kvEntries: 0 };
      const node = nodes.find((n) => String(n.id) === key);
      const tr = el("tr");
      tr.dataset.storageNode = key;
      tr.appendChild(el("td", null, node ? node.name : `#${key} (removed)`));
      const bytes = el("td", null, fmtBytes(u.bytes));
      bytes.dataset.field = "bytes";
      tr.appendChild(bytes);
      tr.appendChild(el("td", null, fmtNum(u.logEntries)));
      tr.appendChild(el("td", null, fmtNum(u.kvEntries)));
      const td = el("td");
      td.appendChild(button("Forget", "lab-btn-sm", () => this.hooks.onForgetNode(Number(key)), { title: "Drop this node's stored data", disabled: u.bytes === 0 && u.logEntries === 0 && u.kvEntries === 0 }));
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    if (!ids.size) {
      const tr = el("tr");
      const td = el("td", "lab-muted", "no nodes");
      td.colSpan = 5;
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    this.table.append(thead, tbody);
  }
}
