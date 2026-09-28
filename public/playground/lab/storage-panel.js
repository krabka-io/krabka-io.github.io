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
const HEX_PAGE_BYTES = 256;

export function hexDump(bytes, offset) {
  const lines = [];
  for (let i = 0; i < bytes.length; i += 16) {
    const row = bytes.subarray(i, i + 16);
    const hex = Array.from(row, (b) => b.toString(16).padStart(2, "0")).join(" ").padEnd(47);
    const ascii = Array.from(row, (b) => (b >= 32 && b <= 126 ? String.fromCharCode(b) : ".")).join("");
    lines.push(`${(offset + i).toString(16).padStart(8, "0")}  ${hex}  |${ascii}|`);
  }
  return lines.join("\n");
}

export function recordBatchHeader(bytes, fileSize) {
  if (bytes.length < 61 || bytes[16] !== 2) return null;
  const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = data.getInt32(8);
  if (length < 49 || length + 12 > fileSize) return null;
  const baseOffset = data.getBigInt64(0);
  return {
    baseOffset: String(baseOffset),
    lastOffset: String(baseOffset + BigInt(data.getInt32(23))),
    bytes: length + 12,
    leaderEpoch: data.getInt32(12),
    records: data.getInt32(57),
    crc: data.getUint32(17).toString(16).padStart(8, "0"),
    firstTimestamp: String(data.getBigInt64(27)),
    maxTimestamp: String(data.getBigInt64(35)),
  };
}

export class StoragePanel {
  // hooks: storage (LabStorage), scenarioId() → string, nodes() → [{ id, name }],
  // volumes() → Promise<[{ node, volume, bytes, files, inUse }]>,
  // volumeFiles(volume), volumeFileRange(volume, path, offset, length),
  // onForgetVolume(volume), onForgetNode(id), onForgetScenario(),
  // onPersistChange(on), onToast(msg)
  constructor(container, hooks) {
    this.hooks = hooks;
    this.browseVersion = 0;
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
          ? "Durable node state (the brokers' logs and metadata) lives only in this browser's IndexedDB. It never leaves this machine."
          : "This browser has no IndexedDB, so nothing is kept across reloads.",
      ),
    );
    this.summaryLine = el("p", "lab-storage-total");
    body.appendChild(this.summaryLine);
    this.table = el("table", "lab-table lab-storage-table");
    body.appendChild(this.table);
    this.volumeBox = el("div", "lab-storage-volumes");
    body.appendChild(this.volumeBox);
    this.volumeExplorer = el("div", "lab-volume-explorer");
    body.appendChild(this.volumeExplorer);
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
      this.closeVolumeExplorer();
      return;
    }
    if (this.activeVolume && !this.activeVolume.startsWith(`${id}/`)) this.closeVolumeExplorer();
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
    if (this.activeVolume && !volumes.some((v) => v.volume === this.activeVolume)) this.closeVolumeExplorer();
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
      td.appendChild(button("Browse", "lab-btn-sm", () => this.browseVolume(v.volume), { title: "List the broker's stored files" }));
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

  closeVolumeExplorer() {
    this.browseVersion++;
    this.activeVolume = null;
    this.volumeExplorer.innerHTML = "";
  }

  async browseVolume(volume) {
    const version = ++this.browseVersion;
    this.activeVolume = volume;
    this.volumeExplorer.innerHTML = "";
    const head = el("div", "lab-file-head");
    head.append(el("strong", null, `Broker disk · ${volume}`));
    head.appendChild(button("Refresh files", "lab-btn-sm", () => this.browseVolume(volume)));
    head.appendChild(button("Close", "lab-btn-sm", () => this.closeVolumeExplorer()));
    this.volumeExplorer.appendChild(head);
    this.volumeExplorer.appendChild(el("p", "lab-muted lab-small", "Files and bytes are the last committed IndexedDB state. Refresh to see newer writes."));
    const filter = el("input", "lab-input lab-file-filter");
    filter.type = "search";
    filter.placeholder = "Filter file paths";
    filter.setAttribute("aria-label", "Filter broker files");
    this.volumeExplorer.appendChild(filter);
    const count = el("span", "lab-muted lab-small");
    this.volumeExplorer.appendChild(count);
    const list = el("div", "lab-file-list", "Loading files…");
    this.volumeExplorer.appendChild(list);
    try {
      const files = await this.hooks.volumeFiles(volume);
      if (version !== this.browseVersion) return;
      list.innerHTML = "";
      if (!files.length) {
        list.textContent = "No files stored yet.";
        return;
      }
      const entries = [];
      for (const file of files.sort((a, b) => a.path.localeCompare(b.path))) {
        const detail = el("details", "lab-file");
        detail.appendChild(el("summary", null, `${file.path} · ${fmtBytes(file.size)}`));
        const contents = el("div", "lab-file-contents");
        detail.appendChild(contents);
        detail.addEventListener("toggle", () => {
          if (detail.open) this.showFile(volume, file.path, detail, contents);
        });
        list.appendChild(detail);
        entries.push([file.path.toLowerCase(), detail]);
      }
      const applyFilter = () => {
        const query = filter.value.trim().toLowerCase();
        let shown = 0;
        for (const [path, detail] of entries) {
          detail.hidden = !path.includes(query);
          if (!detail.hidden) shown++;
        }
        count.textContent = `${shown} of ${entries.length} files`;
      };
      filter.addEventListener("input", applyFilter);
      applyFilter();
    } catch (err) {
      if (version === this.browseVersion) list.textContent = `Files unavailable: ${err.message}`;
    }
  }

  showFile(volume, path, detail, contents) {
    contents.innerHTML = "";
    const nav = el("div", "lab-file-nav");
    const info = el("span", "lab-muted lab-small");
    const input = el("input", "lab-input lab-input-sm");
    input.type = "number";
    input.min = "0";
    input.step = String(HEX_PAGE_BYTES);
    input.value = "0";
    input.setAttribute("aria-label", `Byte offset in ${path}`);
    const pre = el("pre", "lab-hex-view", "Loading bytes…");
    const header = el("div", "lab-file-header");
    let offset = 0;
    let request = 0;
    const read = async (at) => {
      const mine = ++request;
      pre.textContent = "Loading bytes…";
      try {
        const file = await this.hooks.volumeFileRange(volume, path, at, HEX_PAGE_BYTES);
        if (mine !== request || !detail.open || this.activeVolume !== volume) return;
        if (!file) {
          pre.textContent = "File no longer exists. Refresh files.";
          return;
        }
        offset = at;
        input.value = String(offset);
        input.max = String(Math.max(0, file.size - 1));
        info.textContent = `${fmtNum(offset)}–${fmtNum(offset + file.bytes.length)} of ${fmtNum(file.size)} bytes`;
        previous.disabled = offset === 0;
        next.disabled = offset + file.bytes.length >= file.size;
        pre.textContent = file.bytes.length ? hexDump(file.bytes, offset) : "End of file";
        if (at === 0 && path.endsWith(".log")) {
          header.innerHTML = "";
          const batch = recordBatchHeader(file.bytes, file.size);
          if (batch) {
            header.appendChild(el("strong", null, "First Kafka RecordBatch v2"));
            const fields = el("dl", "lab-kv");
            for (const [label, value] of [
              ["base offset", batch.baseOffset], ["last offset", batch.lastOffset],
              ["batch bytes", batch.bytes], ["records", batch.records],
              ["leader epoch", batch.leaderEpoch], ["CRC-32C", batch.crc],
              ["first time (ms)", batch.firstTimestamp], ["max time (ms)", batch.maxTimestamp],
            ]) fields.append(el("dt", null, label), el("dd", null, String(value)));
            header.appendChild(fields);
          }
        }
      } catch (err) {
        if (mine === request) pre.textContent = `Bytes unavailable: ${err.message}`;
      }
    };
    const previous = button("Previous", "lab-btn-sm", () => read(Math.max(0, offset - HEX_PAGE_BYTES)));
    const next = button("Next", "lab-btn-sm", () => read(offset + HEX_PAGE_BYTES));
    const go = button("Go", "lab-btn-sm", () => {
      const at = Number(input.value);
      if (Number.isSafeInteger(at) && at >= 0) read(Math.floor(at / HEX_PAGE_BYTES) * HEX_PAGE_BYTES);
    });
    const offsetLabel = el("label", "lab-muted lab-small", "Byte offset");
    offsetLabel.appendChild(input);
    nav.append(previous, next, offsetLabel, go, info);
    contents.append(nav, header, pre);
    read(0);
  }
}
