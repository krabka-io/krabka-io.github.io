// The kind-specific inspector views.
//
// Every renderer reads the snapshot `state` a node kind reports and draws the
// fields it knows: tables for partitions and groups, bars for counters and
// histograms, key/value rows for the rest. A field that is missing skips its
// section; a field the renderer does not know lands in the "Other fields"
// JSON tree at the end. That is what keeps the inspector working before the
// real node kinds land, and afterwards when a snapshot grows a key.
//
// Snapshot shapes are read loosely: a list may be an array or a map keyed by
// name; a replica may be an id or an object with `id`, `leo` and `hwm`.

import { el, fmtNum, fmtBytes, shortJson } from "./dom.js";
import { jsonTree } from "./json-tree.js";

// Render the view for `kind`; `ctx` gives `nodeName(id)` and the tree state.
export function renderView(kind, state, ctx) {
  const root = el("div", "lab-view");
  if (state == null || typeof state !== "object") {
    root.appendChild(el("p", "lab-muted", state == null ? "No state reported yet." : String(state)));
    return root;
  }
  const used = new Set();
  const renderer = VIEWS[kind] || renderGeneric;
  try {
    renderer(root, state, used, ctx);
  } catch (err) {
    root.appendChild(el("p", "lab-muted", `View error: ${err.message}`));
  }
  const rest = omit(state, used);
  if (Object.keys(rest).length) {
    root.appendChild(section("Other fields", jsonTree(rest, ctx), { open: renderer === renderGeneric }));
  }
  return root;
}

const VIEWS = {
  broker: renderBroker,
  "schema-registry": renderRegistry,
  producer: renderProducer,
  consumer: renderConsumer,
  streams: renderStreams,
  echo: renderCounters,
  pinger: renderCounters,
  admin: renderCounters,
};

// ---- broker -----------------------------------------------------------------------

function renderBroker(root, s, used, ctx) {
  const q = pickObj(s, used, "quorum", "kraft", "controller_state") || {};
  const rows = [];
  addRow(rows, "role", q.role ?? q.state ?? take(s, used, "role"));
  addRow(rows, "epoch", q.epoch ?? take(s, used, "epoch"));
  const ctrl = q.controller_id ?? q.controller ?? q.leader ?? take(s, used, "controller_id", "controller");
  addRow(rows, "controller", ctrl != null ? ctx.nodeLabelForBroker(ctrl) : null);
  addRow(rows, "broker id", take(s, used, "broker_id"));
  addRow(rows, "rack", take(s, used, "rack"));
  addRow(rows, "fenced", take(s, used, "fenced"));
  addRow(rows, "hwm", q.hwm ?? q.high_watermark);
  addRow(rows, "log end", q.leo ?? q.log_end_offset ?? q.log_len);
  addRow(rows, "connections", countValue(take(s, used, "connections", "connection_count")));
  if (rows.length) root.appendChild(section("Quorum", kv(rows)));
  for (const k of ["role", "state", "epoch", "controller_id", "controller", "leader", "hwm", "high_watermark", "leo", "log_end_offset", "log_len"]) delete q[k];
  if (Object.keys(q).length) root.appendChild(section("Quorum details", jsonTree(q, ctx), { open: false }));

  const brokers = asList(take(s, used, "brokers", "registered_brokers"), "id");
  if (brokers) {
    root.appendChild(
      section(
        `Brokers (${brokers.length})`,
        table(
          [
            { key: "id", label: "id" },
            { key: "epoch", label: "epoch" },
            { key: "fenced", label: "fenced", render: bool },
            { key: "rack", label: "rack" },
            { key: "state", label: "state" },
          ],
          brokers,
        ),
      ),
    );
  }

  const topics = asList(take(s, used, "topics"), "name");
  if (topics) {
    const body = el("div");
    for (const t of topics) {
      const parts = asList(t.partitions, "partition");
      const title = `${t.name}${parts ? ` · ${parts.length} partitions` : ""}${t.id ? ` · ${String(t.id).slice(0, 8)}` : ""}`;
      const inner = parts
        ? table(
            [
              { key: "partition", label: "p" },
              { key: "leader", label: "leader" },
              { key: "leader_epoch", label: "epoch", get: (p) => p.leader_epoch ?? p.epoch },
              { key: "isr", label: "ISR", render: idList },
              { key: "replicas", label: "replicas · LEO", render: replicaList },
              { key: "hwm", label: "HWM", get: (p) => p.hwm ?? p.high_watermark },
              { key: "leo", label: "LEO", get: (p) => p.leo ?? p.log_end_offset },
            ],
            parts,
          )
        : jsonTree(t, ctx);
      body.appendChild(section(title, inner, { open: topics.length <= 3, nested: true }));
    }
    root.appendChild(section(`Topics (${topics.length})`, body));
  }

  const groups = asList(take(s, used, "groups", "consumer_groups"), "id");
  if (groups) {
    const body = el("div");
    for (const g of groups) {
      const members = asList(g.members, "id");
      const rows = [];
      addRow(rows, "state", g.state);
      addRow(rows, "protocol", g.protocol ?? g.type);
      addRow(rows, "generation", g.generation ?? g.epoch ?? g.group_epoch);
      addRow(rows, "members", members ? members.length : g.member_count);
      addRow(rows, "lag", g.lag);
      const inner = el("div");
      inner.appendChild(kv(rows));
      if (members) {
        inner.appendChild(
          table(
            [
              { key: "id", label: "member", render: (v) => shortText(v, 18) },
              { key: "epoch", label: "epoch", get: (m) => m.epoch ?? m.member_epoch },
              { key: "assignment", label: "assignment", render: assignmentList, get: (m) => m.assignment ?? m.assigned ?? m.partitions },
              { key: "lag", label: "lag" },
            ],
            members,
          ),
        );
      }
      const offsets = g.offsets ?? g.committed;
      if (offsets && typeof offsets === "object") inner.appendChild(section("Committed offsets", jsonTree(offsets, ctx), { open: false, nested: true }));
      body.appendChild(section(`${g.id ?? g.name ?? "group"}`, inner, { open: groups.length <= 2, nested: true }));
    }
    root.appendChild(section(`Groups (${groups.length})`, body));
  }

  const requests = take(s, used, "requests", "request_counts", "api_counts");
  if (requests && typeof requests === "object") {
    root.appendChild(section("Requests", bars(Object.entries(requests).map(([label, value]) => ({ label, value: Number(value) || 0 })))));
  }
}

// ---- schema registry --------------------------------------------------------------

function renderRegistry(root, s, used, ctx) {
  const rows = [];
  const cfg = take(s, used, "config");
  const compat = take(s, used, "compatibility") ?? (typeof cfg === "object" && cfg ? (cfg.compatibility ?? cfg.compatibilityLevel ?? shortJson(cfg)) : cfg);
  addRow(rows, "compatibility", compat);
  addRow(rows, "mode", take(s, used, "mode"));
  addRow(rows, "schemas", countValue(take(s, used, "schemas", "schema_count")));
  addRow(rows, "_schemas records", take(s, used, "records"));
  addRow(rows, "applied", take(s, used, "applied"));
  addRow(rows, "requests", countValue(take(s, used, "requests", "request_count")));
  addRow(rows, "errors", take(s, used, "errors"));
  addRow(rows, "connections", take(s, used, "connections"));
  addRow(rows, "_schemas offset", take(s, used, "offset", "schemas_offset", "next_offset"));
  if (rows.length) root.appendChild(section("Registry", kv(rows)));

  const subjects = take(s, used, "subjects");
  if (subjects && typeof subjects === "object") {
    const list = Array.isArray(subjects) ? subjects : Object.entries(subjects).map(([subject, versions]) => ({ subject, versions }));
    const body = el("div");
    for (const sub of list) {
      const name = sub.subject ?? sub.name ?? "?";
      const versions = asList(sub.versions ?? sub, "version");
      const inner = versions
        ? table(
            [
              { key: "version", label: "version" },
              { key: "id", label: "id" },
              { key: "schema_type", label: "type", get: (v) => v.schema_type ?? v.schemaType ?? v.type },
              { key: "deleted", label: "deleted", render: bool },
            ],
            versions,
          )
        : jsonTree(sub, ctx);
      body.appendChild(section(name, inner, { open: list.length <= 3, nested: true }));
    }
    root.appendChild(section(`Subjects (${list.length})`, body));
  }
}

// ---- producer -----------------------------------------------------------------------

function renderProducer(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "topic", take(s, used, "topic"));
  addRow(rows, "rate", withUnit(take(s, used, "rate_per_sec", "rate"), "/s"));
  addRow(rows, "sent", take(s, used, "sent", "records_sent"));
  addRow(rows, "acked", take(s, used, "acked", "records_acked"));
  addRow(rows, "failed", take(s, used, "failed", "records_failed"));
  addRow(rows, "retried", take(s, used, "retried", "retries"));
  addRow(rows, "in flight", take(s, used, "in_flight", "inflight"));
  const bytes = take(s, used, "bytes", "bytes_sent");
  addRow(rows, "bytes", bytes != null ? fmtBytes(bytes) : null);
  addRow(rows, "producer id", take(s, used, "producer_id"));
  addRow(rows, "epoch", take(s, used, "producer_epoch", "epoch"));
  addRow(rows, "schema id", take(s, used, "schema_id"));
  if (rows.length) root.appendChild(section("Producer", kv(rows)));

  const hist = take(s, used, "latency_histogram", "latency_ms", "latency", "histogram");
  const items = histogramItems(hist);
  if (items) root.appendChild(section("Ack latency", bars(items)));

  const parts = take(s, used, "partitions", "last_offsets", "offsets");
  if (parts && typeof parts === "object") {
    const list = Array.isArray(parts) ? parts : Object.entries(parts).map(([partition, v]) => (typeof v === "object" && v ? { partition, ...v } : { partition, last_offset: v }));
    root.appendChild(
      section(
        "Partitions",
        table(
          [
            { key: "partition", label: "p" },
            { key: "last_offset", label: "last offset", get: (p) => p.last_offset ?? p.offset ?? p.last },
            { key: "sent", label: "sent" },
            { key: "acked", label: "acked" },
          ],
          list,
        ),
      ),
    );
  }
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
}

// ---- consumer -----------------------------------------------------------------------

function renderConsumer(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "group", take(s, used, "group", "group_id"));
  addRow(rows, "member", shortText(take(s, used, "member_id"), 24));
  addRow(rows, "protocol", take(s, used, "protocol"));
  addRow(rows, "state", take(s, used, "state"));
  addRow(rows, "generation", take(s, used, "generation", "generation_id", "member_epoch", "epoch"));
  addRow(rows, "coordinator", take(s, used, "coordinator"));
  addRow(rows, "consumed", take(s, used, "consumed", "records", "records_consumed"));
  addRow(rows, "committed", typeof s.committed === "number" ? take(s, used, "committed") : null);
  addRow(rows, "lag", typeof s.lag === "number" ? take(s, used, "lag") : null);
  if (rows.length) root.appendChild(section("Consumer", kv(rows)));

  const assignment = take(s, used, "assignment", "assigned");
  const positions = take(s, used, "positions", "partitions");
  const posList = positions && typeof positions === "object" ? asList(positions, "partition") : null;
  if (posList) {
    root.appendChild(
      section(
        "Partitions",
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "position", label: "position", get: (p) => p.position ?? p.offset },
            { key: "committed", label: "committed" },
            { key: "lag", label: "lag" },
          ],
          posList,
        ),
      ),
    );
  } else if (assignment != null) {
    const list = Array.isArray(assignment) ? assignment : [assignment];
    root.appendChild(section("Assignment", el("p", "lab-mono", list.map(assignmentText).join(", ") || "none")));
  }
  if (posList && assignment != null && Array.isArray(assignment) && assignment.length && !posList.length) {
    root.appendChild(section("Assignment", el("p", "lab-mono", assignment.map(assignmentText).join(", "))));
  }

  const records = asList(take(s, used, "last_records", "recent", "records_tail"), "offset");
  if (records) {
    root.appendChild(
      section(
        `Last records (${records.length})`,
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "offset", label: "offset" },
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 24) },
            { key: "value", label: "value", render: (v) => shortText(valueText(v), 60) },
          ],
          records,
        ),
      ),
    );
  }
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
}

// ---- streams --------------------------------------------------------------------------

function renderStreams(root, s, used, ctx) {
  const rows = [];
  const m = pickObj(s, used, "member", "group") || {};
  addRow(rows, "application", take(s, used, "application_id"));
  addRow(rows, "state", m.state ?? take(s, used, "state"));
  addRow(rows, "member", shortText(m.member_id ?? m.id ?? take(s, used, "member_id"), 24));
  addRow(rows, "epoch", m.epoch ?? m.member_epoch ?? take(s, used, "epoch", "member_epoch"));
  addRow(rows, "topology epoch", m.topology_epoch ?? take(s, used, "topology_epoch"));
  addRow(rows, "records in", take(s, used, "records_in", "in"));
  addRow(rows, "records out", take(s, used, "records_out", "out"));
  const changelog = take(s, used, "changelog");
  if (changelog && typeof changelog === "object") {
    addRow(rows, "changelog written", changelog.written ?? changelog.produced);
    addRow(rows, "changelog restored", changelog.restored);
  } else addRow(rows, "changelog", changelog);
  addRow(rows, "status", m.status ?? take(s, used, "status"));
  if (rows.length) root.appendChild(section("Streams", kv(rows)));

  const tasks = asList(take(s, used, "tasks", "active_tasks"), "id");
  if (tasks) {
    root.appendChild(
      section(
        `Active tasks (${tasks.length})`,
        table(
          [
            { key: "id", label: "task", get: (t) => t.id ?? t.task ?? `${t.subtopology ?? "?"}_${t.partition ?? "?"}` },
            { key: "subtopology", label: "subtopology" },
            { key: "partitions", label: "partitions", render: assignmentList, get: (t) => t.partitions ?? t.partition },
            { key: "state", label: "state" },
            { key: "processed", label: "processed", get: (t) => t.processed ?? t.records },
          ],
          tasks,
        ),
      ),
    );
  }
  const standby = asList(take(s, used, "standby_tasks"), "id");
  if (standby && standby.length) root.appendChild(section(`Standby tasks (${standby.length})`, jsonTree(standby, ctx), { open: false }));

  const stores = take(s, used, "stores", "state_stores");
  if (stores && typeof stores === "object") {
    const list = Array.isArray(stores) ? stores : Object.entries(stores).map(([name, entries]) => ({ name, entries }));
    const body = el("div");
    for (const st of list) {
      const entries = st.entries ?? st.data ?? st.values ?? st;
      let inner;
      if (Array.isArray(entries)) {
        inner = table(
          [
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 30) },
            { key: "value", label: "value", render: (v) => shortText(valueText(v), 50) },
          ],
          entries.slice(0, 50),
        );
      } else if (entries && typeof entries === "object") {
        inner = table(
          [
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 30) },
            { key: "value", label: "value", render: (v) => shortText(valueText(v), 50) },
          ],
          Object.entries(entries)
            .slice(0, 50)
            .map(([key, value]) => ({ key, value })),
        );
      } else inner = el("p", "lab-muted", String(entries));
      body.appendChild(section(`${st.name ?? "store"}${st.size != null ? ` · ${st.size}` : ""}`, inner, { open: list.length <= 2, nested: true }));
    }
    root.appendChild(section(`State stores (${list.length})`, body));
  }
  const topology = take(s, used, "topology");
  if (topology && typeof topology === "object") root.appendChild(section("Topology", jsonTree(topology, ctx), { open: false }));
}

// ---- echo, pinger, admin, unknown kinds ---------------------------------------------------

function renderCounters(root, s, used, ctx) {
  const rows = [];
  for (const [k, v] of Object.entries(s)) {
    if (v == null || typeof v !== "object") {
      used.add(k);
      let value = v;
      if (k === "target" && typeof v === "number") value = ctx.nodeName(v);
      addRow(rows, k.replace(/_/g, " "), value, k);
    }
  }
  if (rows.length) root.appendChild(section("State", kv(rows)));
}

function renderGeneric(root, s, used, ctx) {
  renderCounters(root, s, used, ctx);
}

// ---- building blocks ----------------------------------------------------------------------

export function section(title, body, { open = true, nested = false } = {}) {
  const d = el("details", nested ? "lab-sec lab-sec-nested" : "lab-sec");
  d.open = open;
  d.appendChild(el("summary", "lab-sec-title", title));
  d.appendChild(body);
  return d;
}

// Key/value rows. Each `dd` carries `data-field` for the end-to-end check.
export function kv(rows) {
  const dl = el("dl", "lab-kv");
  for (const r of rows) {
    const dt = el("dt", null, r.label);
    const dd = el("dd", null, r.value);
    dd.dataset.field = r.key || r.label.replace(/\s+/g, "_");
    dl.append(dt, dd);
  }
  return dl;
}

export function table(columns, rows) {
  const t = el("table", "lab-table");
  const thead = el("thead");
  const hr = el("tr");
  for (const c of columns) hr.appendChild(el("th", null, c.label));
  thead.appendChild(hr);
  const tbody = el("tbody");
  for (const row of rows) {
    const tr = el("tr");
    for (const c of columns) {
      const raw = c.get ? c.get(row) : row[c.key];
      const text = c.render ? c.render(raw, row) : cell(raw);
      const td = el("td");
      if (text instanceof Node) td.appendChild(text);
      else td.textContent = text;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  if (!rows.length) {
    const tr = el("tr");
    const td = el("td", "lab-muted", "none");
    td.colSpan = columns.length;
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
  t.append(thead, tbody);
  return t;
}

// Horizontal bars, scaled to the largest value.
export function bars(items) {
  const max = Math.max(1, ...items.map((i) => Number(i.value) || 0));
  const wrap = el("div", "lab-bars");
  for (const it of items) {
    const row = el("div", "lab-bar-row");
    row.appendChild(el("span", "lab-bar-label", String(it.label)));
    const track = el("span", "lab-bar-track");
    const fill = el("span", "lab-bar-fill");
    fill.style.width = `${Math.max(1, (100 * (Number(it.value) || 0)) / max)}%`;
    track.appendChild(fill);
    row.appendChild(track);
    row.appendChild(el("span", "lab-bar-value", fmtNum(it.value)));
    wrap.appendChild(row);
  }
  if (!items.length) wrap.appendChild(el("p", "lab-muted", "none"));
  return wrap;
}

function addRow(rows, label, value, key) {
  if (value == null || value === "") return;
  rows.push({ label, value: cell(value), key: key || label.replace(/\s+/g, "_") });
}

function cell(v) {
  if (v == null) return "–";
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (typeof v === "number") return Number.isInteger(v) ? fmtNum(v) : String(Math.round(v * 100) / 100);
  if (typeof v === "object") return shortJson(v, 60);
  return String(v);
}

function bool(v) {
  return v == null ? "–" : v ? "yes" : "no";
}

function withUnit(v, unit) {
  return v == null ? null : `${v}${unit}`;
}

function countValue(v) {
  if (v == null) return null;
  if (typeof v === "number") return v;
  if (Array.isArray(v)) return v.length;
  if (typeof v === "object") return Object.keys(v).length;
  return v;
}

// Take the first present key out of `state`, marking every alias used.
function take(s, used, ...keys) {
  let found;
  for (const k of keys) {
    if (k in s) {
      used.add(k);
      if (found === undefined) found = s[k];
    }
  }
  return found;
}

function pickObj(s, used, ...keys) {
  const v = take(s, used, ...keys);
  return v && typeof v === "object" && !Array.isArray(v) ? { ...v } : null;
}

function omit(s, used) {
  const out = {};
  for (const [k, v] of Object.entries(s)) if (!used.has(k)) out[k] = v;
  return out;
}

// A list from an array or from a map keyed by `keyName`.
function asList(v, keyName) {
  if (v == null) return null;
  if (Array.isArray(v)) return v.map((x) => (x && typeof x === "object" ? x : { [keyName]: x }));
  if (typeof v === "object") {
    return Object.entries(v).map(([k, x]) => (x && typeof x === "object" && !Array.isArray(x) ? { [keyName]: keyOrNumber(k), ...x } : { [keyName]: keyOrNumber(k), value: x }));
  }
  return null;
}

function keyOrNumber(k) {
  return /^\d+$/.test(k) ? Number(k) : k;
}

function idList(v) {
  if (v == null) return "–";
  const list = Array.isArray(v) ? v : [v];
  return list.map((r) => (r && typeof r === "object" ? (r.id ?? r.broker ?? "?") : String(r))).join(" ");
}

// `replicas` as ids or as objects with `leo`/`hwm`; a sibling `leo` map is
// merged when present.
function replicaList(v, row) {
  if (v == null) return "–";
  const list = Array.isArray(v) ? v : [v];
  const leoMap = row && row.leo && typeof row.leo === "object" ? row.leo : null;
  return list
    .map((r) => {
      if (r && typeof r === "object") {
        const id = r.id ?? r.broker ?? "?";
        const leo = r.leo ?? r.log_end_offset;
        return leo != null ? `${id}:${leo}` : String(id);
      }
      const leo = leoMap ? leoMap[String(r)] : null;
      return leo != null ? `${r}:${leo}` : String(r);
    })
    .join(" ");
}

function assignmentText(a) {
  if (a == null) return "";
  if (typeof a === "string" || typeof a === "number") return String(a);
  if (typeof a === "object") {
    if (a.topic != null && a.partition != null) return `${a.topic}-${a.partition}`;
    if (a.topic != null && Array.isArray(a.partitions)) return `${a.topic}[${a.partitions.join(",")}]`;
    return shortJson(a, 40);
  }
  return String(a);
}

function assignmentList(v) {
  if (v == null) return "–";
  if (!Array.isArray(v)) return assignmentText(v);
  if (!v.length) return "none";
  return v.map(assignmentText).join(" ");
}

function valueText(v) {
  if (v == null) return "null";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function shortText(v, max) {
  if (v == null) return null;
  const s = String(v);
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

// A histogram from `[{le, count}]`, `[{bucket, count}]`, `{ "10": 3 }` or a
// plain array of counts.
function histogramItems(h) {
  if (h == null) return null;
  if (Array.isArray(h)) {
    if (!h.length) return [];
    if (typeof h[0] === "number") return h.map((count, i) => ({ label: `#${i}`, value: count }));
    return h.map((b) => ({
      label: b.le != null ? `≤${b.le} ms` : b.bucket != null ? String(b.bucket) : b.label ?? "?",
      value: b.count ?? b.value ?? 0,
    }));
  }
  if (typeof h === "object") {
    const entries = Object.entries(h).filter(([, v]) => typeof v === "number");
    if (!entries.length) return null;
    return entries.map(([k, v]) => ({ label: /^\d+(\.\d+)?$/.test(k) ? `≤${k} ms` : k, value: v }));
  }
  return null;
}
