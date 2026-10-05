// The Storage panel: what this browser keeps for the current scenario.
//
// Per node the bytes and entries stored, the total, a "forget" button per
// node and for the whole scenario, and the "Persist to this browser" toggle.
// The numbers come from a cursor walk over IndexedDB every few seconds while
// the panel is open. Below them, the volumes of the scenario's real brokers:
// each is the disk of one process, kept by the WASI runtime in its own
// database whatever the toggle says.
//
// Browse opens a broker's disk explorer: its partitions with their segments,
// and every other file. A partition shows its segments and the checks that
// span them (CRCs, offsets, leader epochs); a file is decoded by its format
// (disk.js) with its own checks, a batch table and byte map for a segment,
// and the linked field tree and hex view (bytes-view.js).

import { el, button, fmtBytes, fmtNum } from "./dom.js";
import { BytesView } from "./bytes-view.js";
import { analyzeFile, analyzePartition, partitionsOf, baseName } from "./disk.js";
import { CODECS } from "./codecs.js";

// The header of a column of buttons: empty to the eye, named for a screen reader.
function actionsHeader() {
  const th = el("th");
  th.appendChild(el("span", "lab-sr", "actions"));
  return th;
}

const REFRESH_MS = 2500;
// ponytail: whole-file reads; a segment larger than this needs range reads and a batch index.
const MAX_FILE_BYTES = 32 * 1024 * 1024;

export class StoragePanel {
  // hooks: storage (LabStorage), scenarioId() → string, nodes() → [{ id, name }],
  // volumes() → Promise<[{ node, volume, bytes, files, inUse }]>,
  // volumeFiles(volume), volumeFile(volume, path), onForgetVolume(volume),
  // onForgetNode(id), onForgetScenario(), onPersistChange(on), expand()
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
          ? "What the lab's own nodes keep (an echo node's counter) lives only in this browser's IndexedDB. A broker's disk is listed below and is kept whatever this setting says. Nothing leaves this machine."
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
    for (const h of ["node", "bytes", "log entries", "keys", ""]) hr.appendChild(h ? el("th", null, h) : actionsHeader());
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
    for (const h of ["node", "bytes", "files", ""]) head.appendChild(h ? el("th", null, h) : actionsHeader());
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
      td.appendChild(button("Browse", "lab-btn-sm", () => this.browseVolume(v.volume), { title: "Open the broker's disk: partitions, segments and every stored file, decoded" }));
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

  // ---- the disk explorer ----

  async browseVolume(volume) {
    const version = ++this.browseVersion;
    const reopening = this.activeVolume === volume;
    this.activeVolume = volume;
    this.hooks.expand?.();
    const nodeId = Number(volume.split("/").pop());
    const nodeName = this.hooks.nodes().find((n) => n.id === nodeId)?.name ?? `#${nodeId}`;
    this.volumeExplorer.innerHTML = "";
    const head = el("div", "lab-file-head");
    head.append(el("strong", null, `Disk of ${nodeName}`), el("span", "lab-muted lab-small", volume));
    head.appendChild(button("Refresh", "lab-btn-sm", () => this.browseVolume(volume), { title: "Read the files again: newer writes appear" }));
    head.appendChild(button("Close", "lab-btn-sm", () => this.closeVolumeExplorer()));
    const note = el("p", "lab-muted lab-small", "The last committed IndexedDB state of the volume. Decoders and checks run here, on the bytes as stored.");
    const grid = el("div", "lab-vx");
    const side = el("div", "lab-vx-files");
    const filter = el("input", "lab-input lab-file-filter");
    filter.type = "search";
    filter.placeholder = "Filter paths";
    filter.setAttribute("aria-label", "Filter broker files");
    const emptyRow = el("label", "lab-field-inline lab-small");
    const hideEmpty = el("input");
    hideEmpty.type = "checkbox";
    hideEmpty.checked = this.hideEmpty ?? true;
    emptyRow.append(hideEmpty, el("span", null, "Hide empty partitions"));
    const list = el("div", "lab-vx-list", "Loading files…");
    list.setAttribute("role", "tree");
    list.setAttribute("aria-label", "Broker files");
    side.append(filter, emptyRow, list);
    this.view = el("div", "lab-vx-view");
    this.view.appendChild(el("p", "lab-muted", "Pick a partition or a file."));
    grid.append(side, this.view);
    this.volumeExplorer.append(head, note, grid);

    let files;
    try {
      files = await this.hooks.volumeFiles(volume);
    } catch (err) {
      if (version === this.browseVersion) list.textContent = `Files unavailable: ${err.message}`;
      return;
    }
    if (version !== this.browseVersion) return;
    const parts = partitionsOf(files);
    const inParts = new Set(parts.flatMap((p) => p.files.map((f) => f.path)));
    const others = files.filter((f) => !inParts.has(f.path)).sort((a, b) => a.path.localeCompare(b.path));
    const read = (path) => this.hooks.volumeFile(volume, path);
    const render = () => {
      this.hideEmpty = hideEmpty.checked;
      const q = filter.value.trim().toLowerCase();
      list.replaceChildren();
      const fileRow = (f, depth) => {
        const b = el("button", "lab-vx-item", "");
        b.type = "button";
        b.style.paddingLeft = `${0.4 + depth * 0.9}rem`;
        b.append(el("span", "lab-vx-name", depth ? baseName(f.path) : f.path.replace(/^log\//, "")), el("span", "lab-vx-size", fmtBytes(f.size)));
        b.dataset.path = f.path;
        b.setAttribute("aria-current", String(this.current === f.path));
        b.addEventListener("click", () => this.openFile(volume, f, read));
        return b;
      };
      let shown = 0;
      for (const p of parts) {
        const logBytes = p.segments.reduce((n, s) => n + s.size, 0);
        if (hideEmpty.checked && logBytes === 0) continue;
        if (q && !p.dir.toLowerCase().includes(q) && !p.files.some((f) => f.path.toLowerCase().includes(q))) continue;
        shown++;
        const box = el("details", "lab-vx-part");
        box.open = this.openParts?.has(p.dir) || Boolean(q);
        box.addEventListener("toggle", () => {
          this.openParts ??= new Set();
          if (box.open) this.openParts.add(p.dir);
          else this.openParts.delete(p.dir);
        });
        const sum = el("summary", "lab-vx-item lab-vx-partrow");
        sum.append(el("span", "lab-vx-name", p.dir.replace(/^log\//, "")), el("span", "lab-vx-size", `${p.segments.length} seg · ${fmtBytes(logBytes)}`));
        sum.setAttribute("aria-current", String(this.current === p.dir));
        sum.addEventListener("click", (e) => {
          if (e.target.closest(".lab-vx-name")) {
            e.preventDefault();
            // Recorded before the list re-renders, so the partition stays open.
            this.openParts ??= new Set();
            this.openParts.add(p.dir);
            this.openPartition(p, read);
          }
        });
        box.appendChild(sum);
        for (const f of p.files.slice().sort((a, b) => a.path.localeCompare(b.path, undefined, { numeric: true }))) {
          if (!q || f.path.toLowerCase().includes(q) || p.dir.toLowerCase().includes(q)) box.appendChild(fileRow(f, 1));
        }
        list.appendChild(box);
      }
      const rest = others.filter((f) => !q || f.path.toLowerCase().includes(q));
      if (rest.length) {
        list.appendChild(el("p", "lab-rail-heading", "Other files"));
        for (const f of rest) list.appendChild(fileRow(f, 0));
      }
      const hidden = parts.length - shown;
      if (hidden && hideEmpty.checked && !q) list.appendChild(el("p", "lab-muted lab-small", `${hidden} empty partition${hidden === 1 ? "" : "s"} hidden`));
    };
    this.renderList = render;
    filter.addEventListener("input", render);
    hideEmpty.addEventListener("change", render);
    render();
    // A refresh reopens what was open; a first look opens the busiest partition.
    if (reopening && this.current) {
      const p = parts.find((x) => x.dir === this.current);
      const f = files.find((x) => x.path === this.current);
      if (p) this.openPartition(p, read);
      else if (f) this.openFile(volume, f, read);
    } else {
      const busiest = parts.filter((p) => !p.topic.startsWith("__")).sort((a, b) => b.bytes - a.bytes)[0] || parts[0];
      if (busiest) this.openPartition(busiest, read);
    }
  }

  setCurrent(key) {
    this.current = key;
    this.renderList?.();
  }

  async openPartition(p, read) {
    this.setCurrent(p.dir);
    const view = this.view;
    view.replaceChildren(el("p", "lab-muted", `Reading ${p.segments.length} segments…`));
    const result = await analyzePartition(p, read);
    if (this.current !== p.dir) return;
    view.replaceChildren();
    view.appendChild(el("h4", "lab-vx-title", `${p.dir.replace(/^log\//, "")} · topic ${p.topic}`));
    view.appendChild(checkList(result.checks));
    const table = el("table", "lab-table lab-vx-table");
    const head = el("tr");
    for (const h of ["segment", "bytes", "batches", "records", "offsets", "leader epochs", "control", "CRC"]) head.appendChild(el("th", null, h));
    table.appendChild(head);
    for (const s of result.segments) {
      const tr = el("tr", "lab-vx-rowlink");
      tr.tabIndex = 0;
      const open = () => this.openFile(this.activeVolume, p.files.find((f) => f.path === s.path), read);
      tr.addEventListener("click", open);
      tr.addEventListener("keydown", (e) => e.key === "Enter" && open());
      tr.append(
        el("td", null, baseName(s.path)), el("td", null, fmtBytes(s.size)), el("td", null, fmtNum(s.batches)), el("td", null, fmtNum(s.records)),
        el("td", null, s.first == null ? "–" : `${s.first}–${s.last}`), el("td", null, s.epochs.join(", ") || "–"), el("td", null, String(s.control)),
        el("td", s.crcBad ? "lab-bv-bad" : "lab-bv-ok", s.crcBad ? `${s.crcBad} bad` : s.batches ? "✓" : "–"),
      );
      table.appendChild(tr);
    }
    const wrap = el("div", "lab-table-wrap");
    wrap.appendChild(table);
    view.appendChild(wrap);
    if (result.epochs.length) {
      const dl = el("dl", "lab-kv");
      for (const e of result.epochs) dl.append(el("dt", null, `epoch ${e.epoch}`), el("dd", null, `starts at offset ${e.start}${e.firstSeen != null ? ` · first batch at ${e.firstSeen}` : " · no batch of it left"}`));
      view.append(el("p", "lab-rail-heading", "leader-epoch-checkpoint"), dl);
    }
    view.appendChild(el("p", "lab-muted lab-small", "Select a segment to decode its batches and records."));
  }

  async openFile(volume, f, read) {
    if (!f) return;
    this.setCurrent(f.path);
    const view = this.view;
    view.replaceChildren(el("p", "lab-muted", `Reading ${f.path}…`));
    if (f.size > MAX_FILE_BYTES) {
      view.replaceChildren(el("p", "lab-muted", `${f.path} is ${fmtBytes(f.size)}; this explorer decodes files up to ${fmtBytes(MAX_FILE_BYTES)}.`));
      return;
    }
    let bytes;
    try {
      bytes = await read(f.path);
    } catch (err) {
      view.replaceChildren(el("p", "lab-error", `Bytes unavailable: ${err.message}`));
      return;
    }
    if (this.current !== f.path || this.activeVolume !== volume) return;
    if (!bytes) {
      view.replaceChildren(el("p", "lab-muted", "The file is gone. Refresh the file list."));
      return;
    }
    const dir = f.path.slice(0, f.path.lastIndexOf("/") + 1);
    const a = await analyzeFile(f.path, bytes, { sibling: (name) => read(dir + name) });
    if (this.current !== f.path) return;
    view.replaceChildren();
    view.appendChild(el("h4", "lab-vx-title", `${f.path} · ${fmtBytes(bytes.length)} · ${a.kind}`));
    if (a.checks.length) view.appendChild(checkList(a.checks));
    if (a.summary.length) {
      const dl = el("dl", "lab-kv lab-vx-summary");
      for (const [k, v] of a.summary) dl.append(el("dt", null, k), el("dd", null, String(v)));
      view.appendChild(dl);
    }
    const host = el("div", "lab-vx-bytes");
    const bv = new BytesView(host, { label: f.path });
    if (a.batches?.length) view.append(byteMap(a.batches, bytes.length, a.root, bv), batchTable(a.batches, a.root, bv));
    if (a.text != null) {
      const pre = el("pre", "lab-vx-text", a.text);
      view.appendChild(pre);
    }
    view.appendChild(host);
    const buffers = { main: bytes };
    for (const [k, v] of a.buffers || []) buffers[k] = v;
    bv.show({ buffers, root: a.root, expandDepth: a.kind === "segment" ? 1 : 2 });
  }
}

function checkList(checks) {
  const ul = el("ul", "lab-vx-checks");
  for (const c of checks) {
    const state = !c.ok ? "bad" : c.warn ? "warn" : "ok";
    const li = el("li", `lab-vx-check lab-bv-${state}`);
    li.append(el("span", "lab-vx-mark", state === "ok" ? "✓" : state === "bad" ? "✗" : "!"), el("span", null, c.text));
    ul.appendChild(li);
  }
  return ul;
}

// The segment as a strip: one block per batch, as wide as its bytes.
function byteMap(batches, size, root, bv) {
  const strip = el("div", "lab-vx-map");
  strip.setAttribute("role", "group");
  strip.setAttribute("aria-label", "Batches by position in the file");
  batches.forEach((b, i) => {
    const seg = el("button", `lab-vx-mapseg${b.control ? " lab-vx-ctl" : ""}${b.codec ? " lab-vx-zip" : ""}${b.crcOk ? "" : " lab-vx-crcbad"}`);
    seg.type = "button";
    seg.style.flexGrow = String(b.size);
    seg.title = `offsets ${b.baseOffset}–${b.lastOffset} · ${b.size} B at byte ${b.pos} · ${CODECS[b.codec]}${b.control ? " · control" : ""}${b.crcOk ? "" : " · CRC mismatch"}`;
    seg.setAttribute("aria-label", seg.title);
    seg.addEventListener("click", () => bv.select(root.children[i]));
    strip.appendChild(seg);
  });
  const legend = el("p", "lab-muted lab-small lab-vx-legend", `${batches.length} batches in ${size} bytes · blue data · purple control · orange compressed · red CRC mismatch`);
  const box = el("div");
  box.append(strip, legend);
  return box;
}

function batchTable(batches, root, bv) {
  const table = el("table", "lab-table lab-vx-table");
  const head = el("tr");
  for (const h of ["byte", "offsets", "records", "bytes", "codec", "epoch", "producer", "timestamps", "CRC"]) head.appendChild(el("th", null, h));
  table.appendChild(head);
  batches.slice(0, 2000).forEach((b, i) => {
    const tr = el("tr", "lab-vx-rowlink");
    tr.tabIndex = 0;
    const pick = () => bv.select(root.children[i]);
    tr.addEventListener("click", pick);
    tr.addEventListener("keydown", (e) => e.key === "Enter" && pick());
    tr.append(
      el("td", null, String(b.pos)), el("td", null, `${b.baseOffset}–${b.lastOffset}`), el("td", null, String(b.count)), el("td", null, String(b.size)),
      el("td", null, `${CODECS[b.codec]}${b.control ? " · control" : ""}${b.transactional ? " · txn" : ""}`), el("td", null, String(b.epoch)),
      el("td", null, b.producerId < 0n ? "–" : `${b.producerId}/${b.producerEpoch} seq ${b.baseSequence}`),
      el("td", null, b.baseTimestamp === b.maxTimestamp ? String(b.baseTimestamp) : `${b.baseTimestamp}–${b.maxTimestamp}`),
      el("td", b.crcOk ? "lab-bv-ok" : "lab-bv-bad", b.crcOk ? "✓" : `✗ ${b.crcComputed.toString(16)}`),
    );
    table.appendChild(tr);
  });
  const wrap = el("div", "lab-table-wrap lab-vx-batches");
  wrap.appendChild(table);
  return wrap;
}
