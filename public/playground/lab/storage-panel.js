// The Storage panel: what this browser keeps for the current scenario.
//
// Per node the bytes and entries stored, the total, a "forget" button per
// node and for the whole scenario, and the "Persist to this browser" toggle.
// The numbers come from a cursor walk over IndexedDB every few seconds while
// the panel is open. Below them, the volumes of the scenario's real brokers:
// each is the disk of one process, kept by the WASI runtime in its own
// database whatever the toggle says.

import { el, button, fmtBytes, fmtNum } from "./dom.js";

const REFRESH_MS = 2500;

export class StoragePanel {
  // hooks: storage (LabStorage), scenarioId() → string, nodes() → [{ id, name }],
  // volumes() → Promise<[{ node, volume, bytes, files, inUse }]>,
  // onForgetVolume(volume), onForgetNode(id), onForgetScenario(),
  // onPersistChange(on), onToast(msg)
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
    this.volumeBox = el("div", "lab-storage-volumes");
    body.appendChild(this.volumeBox);
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
      const nameCell = el("td", null, node ? node.name : `#${key} (removed)`);
      if (this.hooks.storage.forgotten.has(Number(key))) {
        nameCell.appendChild(el("span", "lab-muted lab-small", " · not stored"));
        nameCell.title = "Forgotten: not stored again until it restarts from nothing (Wipe) or persistence is turned back on";
      }
      tr.appendChild(nameCell);
      const bytes = el("td", null, fmtBytes(u.bytes));
      bytes.dataset.field = "bytes";
      tr.appendChild(bytes);
      tr.appendChild(el("td", null, fmtNum(u.logEntries)));
      tr.appendChild(el("td", null, fmtNum(u.kvEntries)));
      const td = el("td");
      td.appendChild(button("Forget", "lab-btn-sm", () => this.hooks.onForgetNode(Number(key)), { title: "Drop this node's stored data; the running node is not stored again until it restarts from nothing", disabled: u.bytes === 0 && u.logEntries === 0 && u.kvEntries === 0 }));
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
    await this.refreshVolumes(nodes);
  }

  // The real brokers' volumes: the disk each process runs on.
  async refreshVolumes(nodes) {
    let volumes = [];
    try {
      volumes = this.hooks.volumes ? await this.hooks.volumes() : [];
    } catch (err) {
      this.volumeBox.innerHTML = "";
      this.volumeBox.appendChild(el("p", "lab-muted lab-small", `Volumes unavailable: ${err.message}`));
      return;
    }
    this.volumeBox.innerHTML = "";
    if (!volumes.length) return;
    this.volumeBox.appendChild(
      el("p", "lab-storage-total", "Real broker volumes: each process's disk, kept by the WASI runtime in this browser whatever the toggle says. A wipe or removing the node forgets it."),
    );
    const table = el("table", "lab-table lab-storage-table");
    const head = el("tr");
    for (const h of ["node", "bytes", "files", ""]) head.appendChild(el("th", null, h));
    const thead = el("thead");
    thead.appendChild(head);
    const tbody = el("tbody");
    for (const v of volumes.sort((a, b) => a.node - b.node)) {
      const node = nodes.find((n) => n.id === v.node);
      const tr = el("tr");
      tr.dataset.storageVolume = v.volume;
      const name = el("td", null, node ? node.name : `#${v.node} (removed)`);
      name.title = v.volume;
      tr.appendChild(name);
      const bytes = el("td", null, fmtBytes(v.bytes));
      bytes.dataset.field = "volume-bytes";
      tr.appendChild(bytes);
      tr.appendChild(el("td", null, fmtNum(v.files)));
      const td = el("td");
      td.appendChild(
        button("Forget", "lab-btn-sm", () => this.hooks.onForgetVolume(v.volume), {
          title: v.inUse ? "Its process runs on it: kill or wipe the node first" : "Delete this volume",
          disabled: v.inUse,
        }),
      );
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    table.append(thead, tbody);
    this.volumeBox.appendChild(table);
  }
}
