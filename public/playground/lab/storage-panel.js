// The Storage panel: what this browser keeps for the current scenario.
//
// Per node the bytes and entries stored, the total, a "forget" button per
// node and for the whole scenario, and the "Persist to this browser" toggle.
// The numbers come from a cursor walk over IndexedDB every few seconds while
// the panel is open. Below them, the volumes of the scenario's real brokers:
// each is the disk of one process, kept by the WASI runtime in its own
// database whatever the toggle says.

import { el, button, select, fmtBytes, fmtNum } from "./dom.js";

const REFRESH_MS = 2500;
const HEX_PAGE_BYTES = 256;
const MAX_ANNOTATED_BYTES = 65536;

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

export function recordBatchFields(bytes, fileSize) {
  const fields = [];
  const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let start = 0; start + 61 <= bytes.length;) {
    const length = data.getInt32(start + 8);
    const end = start + 12 + length;
    if (bytes[start + 16] !== 2 || length < 49 || end > fileSize) break;
    const base = data.getBigInt64(start);
    const crc = data.getUint32(start + 17).toString(16).padStart(8, "0");
    const header = [
      [0, 8, "Base offset", String(base)], [8, 12, "Batch length", String(length)],
      [12, 16, "Leader epoch", String(data.getInt32(start + 12))], [16, 17, "Format version", "2"],
      [17, 21, "CRC-32C", `0x${crc}`], [21, 23, "Attributes", `0x${data.getUint16(start + 21).toString(16)}`],
      [23, 27, "Last offset delta", String(data.getInt32(start + 23))],
      [27, 35, "Base timestamp (ms)", String(data.getBigInt64(start + 27))],
      [35, 43, "Max timestamp (ms)", String(data.getBigInt64(start + 35))],
      [43, 51, "Producer ID", String(data.getBigInt64(start + 43))],
      [51, 53, "Producer epoch", String(data.getInt16(start + 51))],
      [53, 57, "Base sequence", String(data.getInt32(start + 53))],
      [57, 61, "Record count", String(data.getInt32(start + 57))],
    ];
    for (const [from, to, name, value] of header) fields.push({ start: start + from, end: start + to, label: `${name}: ${value}` });
    if (end > start + 61) fields.push({ start: start + 61, end, label: `Encoded records in batch at offset ${base}` });
    start = end;
  }
  return fields;
}

function annotatedHexDump(pre, bytes, offset, fields) {
  pre.replaceChildren();
  const appendBytes = (row, rowOffset, ascii) => {
    for (let i = 0; i < row.length;) {
      const field = fields.find((f) => rowOffset + i >= f.start && rowOffset + i < f.end);
      let j = i + 1;
      while (j < row.length && fields.find((f) => rowOffset + j >= f.start && rowOffset + j < f.end) === field) j++;
      const value = Array.from(row.subarray(i, j), (b, k) =>
        ascii ? (b >= 32 && b <= 126 ? String.fromCharCode(b) : ".") : `${b.toString(16).padStart(2, "0")}${i + k < row.length - 1 ? " " : ""}`,
      ).join("");
      if (field) {
        const span = el("span", "lab-hex-field", value);
        span.title = field.label;
        if (!ascii) { span.tabIndex = 0; span.setAttribute("aria-label", field.label); }
        pre.appendChild(span);
      } else pre.append(value);
      i = j;
    }
  };
  for (let i = 0; i < bytes.length; i += 16) {
    const row = bytes.subarray(i, i + 16);
    const rowOffset = offset + i;
    pre.append(`${(offset + i).toString(16).padStart(8, "0")}  `);
    appendBytes(row, rowOffset, false);
    pre.append(`${" ".repeat(47 - (row.length * 3 - 1))}  |`);
    appendBytes(row, rowOffset, true);
    pre.append(i + 16 < bytes.length ? "|\n" : "|");
  }
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
    // The dock keeps the disclosure open, so poll only while its tab is showing.
    this.timer = setInterval(() => {
      if (this.root.open && this.root.offsetParent !== null) this.refresh();
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
    const value = (text) => el("strong", "lab-storage-value", text);
    this.summaryLine.replaceChildren(
      "Scenario ", value(id.slice(0, 8)), ": ", value(fmtBytes(t.bytes)),
      " in ", value(fmtNum(t.logEntries)), " log entries and ", value(fmtNum(t.kvEntries)), " keys",
      this.hooks.storage.persist ? "" : " · persistence off, new changes are dropped",
    );
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
    const sort = select([
      { value: "name-asc", label: "Name A–Z" }, { value: "name-desc", label: "Name Z–A" },
      { value: "size-desc", label: "Largest first" }, { value: "size-asc", label: "Smallest first" },
    ], "name-asc");
    sort.setAttribute("aria-label", "Sort broker files");
    sort.classList.add("lab-file-sort");
    this.volumeExplorer.appendChild(sort);
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
      for (const file of files) {
        const detail = el("details", "lab-file");
        detail.appendChild(el("summary", null, `${file.path} · ${fmtBytes(file.size)}`));
        const contents = el("div", "lab-file-contents");
        detail.appendChild(contents);
        detail.addEventListener("toggle", () => {
          if (detail.open) this.showFile(volume, file.path, detail, contents);
        });
        list.appendChild(detail);
        entries.push({ path: file.path.toLowerCase(), size: file.size, detail });
      }
      const applyFilter = () => {
        const query = filter.value.trim().toLowerCase();
        let shown = 0;
        const direction = sort.value.endsWith("desc") ? -1 : 1;
        entries.sort((a, b) => direction * (sort.value.startsWith("size") ? a.size - b.size || a.path.localeCompare(b.path) : a.path.localeCompare(b.path)));
        for (const entry of entries) {
          entry.detail.hidden = !entry.path.includes(query);
          if (!entry.detail.hidden) shown++;
          list.appendChild(entry.detail);
        }
        count.textContent = `${shown} of ${entries.length} files`;
      };
      filter.addEventListener("input", applyFilter);
      sort.addEventListener("change", applyFilter);
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
    const hint = el("p", "lab-muted lab-small lab-byte-hint", "Hover or focus highlighted bytes for RecordBatch fields. Record contents remain encoded.");
    pre.addEventListener("pointerover", (event) => {
      hint.textContent = event.target.closest(".lab-hex-field")?.title || "Hover or focus highlighted bytes for RecordBatch fields. Record contents remain encoded.";
    });
    pre.addEventListener("pointerleave", () => { hint.textContent = "Hover or focus highlighted bytes for RecordBatch fields. Record contents remain encoded."; });
    pre.addEventListener("focusin", (event) => { hint.textContent = event.target.title || hint.textContent; });
    let offset = 0;
    let request = 0;
    let annotationBytes = new Uint8Array(0);
    const fieldsThrough = async (end, fileSize) => {
      // ponytail: cap header scanning at 64 KiB; index batches on disk if deep-file inspection becomes useful.
      const target = Math.min(fileSize, MAX_ANNOTATED_BYTES, end + 61);
      while (annotationBytes.length < target) {
        const chunk = await this.hooks.volumeFileRange(volume, path, annotationBytes.length, Math.min(4096, target - annotationBytes.length));
        if (!chunk?.bytes.length) break;
        const joined = new Uint8Array(annotationBytes.length + chunk.bytes.length);
        joined.set(annotationBytes);
        joined.set(chunk.bytes, annotationBytes.length);
        annotationBytes = joined;
      }
      return recordBatchFields(annotationBytes, fileSize);
    };
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
        if (path.endsWith(".log") && file.bytes.length && at < MAX_ANNOTATED_BYTES) {
          try {
            const fields = await fieldsThrough(at + file.bytes.length, file.size);
            if (mine !== request || !detail.open || this.activeVolume !== volume) return;
            if (fields.length) annotatedHexDump(pre, file.bytes, offset, fields);
          } catch { /* Keep the plain hex dump when labels cannot be loaded. */ }
        }
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
    if (path.endsWith(".log")) contents.appendChild(hint);
    read(0);
  }
}
