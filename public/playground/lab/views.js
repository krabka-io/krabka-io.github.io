// The kind-specific inspector views.
//
// Every renderer reads the snapshot `state` a node kind reports (the shapes
// are in each node module's documentation under `playground/src/lab/`) and
// draws the fields it knows: tables for partitions, groups, assignments,
// tasks and stores, bars for counters, key/value rows for the rest. A field
// that is missing skips its section; a field the renderer does not know lands
// in the "Other fields" JSON tree at the end, so a snapshot that grows a key
// still shows it.
//
// Tables carry `data-row` and `data-col` and key/value rows `data-field`, the
// hooks `scripts/check-lab.mjs` reads the page by.

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
  "krabka-broker": renderRealBroker,
  "schema-registry": renderRegistry,
  producer: renderProducer,
  consumer: renderConsumer,
  streams: renderStreams,
  echo: renderCounters,
  pinger: renderCounters,
  admin: renderAdmin,
};

// ---- broker -----------------------------------------------------------------------

// The simulated broker (`lab::broker`): its lifecycle, its share of the
// KRaft quorum, the channels to the active controller, its pending ISR
// changes and producer-id blocks, the groups it coordinates, and the
// partitions it hosts.
function renderBroker(root, s, used, ctx) {
  const q = pickObj(s, used, "quorum") || {};
  const life = pickObj(s, used, "lifecycle") || {};
  const me = take(s, used, "broker_id");
  const brokers = asList(take(s, used, "brokers"), "id") || [];
  const self = brokers.find((b) => b.id === me);
  const controller = take(s, used, "controller_id");
  const brokerLabel = (id) => (id == null ? null : ctx.nodeLabelForBroker(id));

  const rows = [];
  addRow(rows, "broker id", me, "broker_id");
  addRow(rows, "state", take(s, used, "state") ?? life.state, "state");
  addRow(rows, "fenced", life.fenced, "fenced");
  addRow(rows, "registered", life.registered, "registered");
  addRow(rows, "broker epoch", life.broker_epoch, "broker_epoch");
  addRow(rows, "incarnation", shortText(life.incarnation_id, 13), "incarnation");
  addRow(rows, "rack", self?.rack, "rack");
  addRow(rows, "cluster id", take(s, used, "cluster_id"), "cluster_id");
  addRow(rows, "client connections", take(s, used, "connections"), "connections");
  addRow(rows, "controller connections", take(s, used, "controller_connections"), "controller_connections");
  addRow(rows, "held requests", take(s, used, "held_requests"), "held_requests");
  root.appendChild(section("Broker", kv(rows)));

  const qr = [];
  addRow(qr, "role", q.role, "quorum_role");
  addRow(qr, "votes", q.voter === false ? "no: an observer" : q.voter === true ? "yes: a voter" : null, "quorum_votes");
  addRow(qr, "epoch", q.epoch, "quorum_epoch");
  addRow(qr, "leader", q.leader != null ? brokerLabel(q.leader) : "none", "quorum_leader");
  addRow(qr, "voters", Array.isArray(q.voters) ? q.voters.join(", ") : null, "quorum_voters");
  if (Array.isArray(q.observers) && q.observers.length) addRow(qr, "observers", q.observers.join(", "), "quorum_observers");
  addRow(qr, "high watermark", q.hwm, "quorum_hwm");
  addRow(qr, "log end", q.leo, "quorum_leo");
  addRow(qr, "applied up to", q.metadata_offset, "quorum_applied");
  addRow(qr, "active controller", q.active ? "this broker" : controller != null ? brokerLabel(controller) : "none known", "controller");
  const quorumBody = el("div");
  quorumBody.appendChild(kv(qr));
  const why = observerNote(q, ctx.spec);
  if (why) quorumBody.appendChild(note(why, "observer-note"));
  root.appendChild(section("KRaft quorum", quorumBody));

  const channels = take(s, used, "channels");
  if (channels && typeof channels === "object") {
    const list = Object.entries(channels).map(([name, c]) => ({ name, ...(c || {}) }));
    root.appendChild(
      section(
        "Controller channels",
        table(
          [
            { key: "name", label: "channel" },
            { key: "controller", label: "controller", render: (v) => (v == null ? "–" : String(v)) },
            { key: "connected", label: "connected", render: bool },
            { key: "queued", label: "queued" },
            { key: "in_flight", label: "in flight", render: (v) => v ?? "–" },
          ],
          list,
          { rowKey: (r) => r.name },
        ),
        { open: false },
      ),
    );
  }

  const topics = asList(take(s, used, "topics"), "name") || [];
  const isr = take(s, used, "isr_changes");
  const pending = [];
  for (const t of topics) for (const p of t.partitions || []) if (Array.isArray(p.pending_isr)) pending.push({ partition: `${t.name}-${p.index}`, isr: p.isr, proposed: p.pending_isr });
  if (isr && typeof isr === "object") {
    const body = el("div");
    const ir = [];
    addRow(ir, "queued", Array.isArray(isr.queued) && isr.queued.length ? isr.queued.join(", ") : "none", "isr_queued");
    addRow(ir, "AlterPartition in flight", Boolean(isr.in_flight), "isr_in_flight");
    body.appendChild(kv(ir));
    if (pending.length) {
      body.appendChild(
        table(
          [
            { key: "partition", label: "partition" },
            { key: "isr", label: "ISR", render: idList },
            { key: "proposed", label: "proposed", render: idList },
          ],
          pending,
          { rowKey: (r) => r.partition },
        ),
      );
    }
    root.appendChild(section(`Pending ISR changes (${pending.length})`, body, { open: pending.length > 0 }));
  }

  const pids = take(s, used, "producer_ids");
  if (pids && typeof pids === "object") {
    const pr = [];
    addRow(pr, "next id", pids.next_id ?? "none: no block yet", "pid_next");
    addRow(pr, "block ends at", pids.block_end, "pid_block_end");
    addRow(pr, "next block", pids.next_block, "pid_next_block");
    addRow(pr, "asking the controller", Boolean(pids.requesting), "pid_requesting");
    root.appendChild(section("Producer-id blocks", kv(pr), { open: false }));
  }

  const groups = take(s, used, "groups");
  if (groups && typeof groups === "object") root.appendChild(renderCoordinator(groups, ctx));

  if (brokers.length) {
    root.appendChild(
      section(
        `Registered brokers (${brokers.length})`,
        table(
          [
            { key: "id", label: "broker", render: (v) => brokerLabel(v) },
            { key: "rack", label: "rack", render: (v) => v ?? "–" },
            { key: "fenced", label: "fenced", render: bool },
          ],
          brokers,
          { rowKey: (r) => String(r.id) },
        ),
        { open: false },
      ),
    );
  }

  const user = topics.filter((t) => !t.internal);
  const internal = topics.filter((t) => t.internal);
  // Kafka's `kafka-topics --describe` columns, with this broker's copy of
  // the log: its high watermark and log end, and, where it leads, each
  // follower's log end and lag.
  const partitionTable = (t) => {
    const parts = t.partitions || [];
    const columns = [
      { key: "index", label: "p" },
      { key: "leader", label: "leader", render: (v) => (v == null ? "none" : String(v)) },
      { key: "leader_epoch", label: "ep" },
      { key: "replicas", label: "replicas", render: idList },
      { key: "isr", label: "ISR", render: (v, p) => (Array.isArray(p.pending_isr) ? `${idList(v)} → ${idList(p.pending_isr)}` : idList(v)) },
      { key: "hwm", label: "HWM", render: (v) => (v == null ? "–" : fmtNum(v)) },
      { key: "log_end", label: "LEO", render: (v, p) => logEnd(v, p) },
    ];
    if (parts.some((p) => Array.isArray(p.followers) && p.followers.length)) columns.push({ key: "followers", label: "followers", render: followerList });
    return table(columns, parts, { rowKey: (p) => `${t.name}-${p.index}` });
  };
  if (user.length) {
    const body = el("div");
    for (const t of user) {
      const parts = t.partitions || [];
      const led = parts.filter((p) => p.leader === me).length;
      const title = `${t.name} · ${parts.length} partition${parts.length === 1 ? "" : "s"} · leads ${led}`;
      body.appendChild(section(title, partitionTable(t), { open: user.length <= 3, nested: true }));
    }
    root.appendChild(section(`Topics (${user.length})`, body));
  }
  if (internal.length) {
    const body = el("div");
    for (const t of internal) {
      const parts = t.partitions || [];
      const led = parts.filter((p) => p.leader === me).length;
      body.appendChild(section(`${t.name} · ${parts.length} partitions · leads ${led}`, partitionTable(t), { open: false, nested: true }));
    }
    root.appendChild(section(`Internal topics (${internal.length})`, body, { open: false }));
  }

  const requests = take(s, used, "requests");
  if (requests && typeof requests === "object") {
    root.appendChild(section("Requests served", bars(Object.entries(requests).map(([label, value]) => ({ label, value: Number(value) || 0 }))), { open: false }));
  }
}

// Why a broker observes the quorum instead of voting, or null when it votes.
function observerNote(q, spec) {
  if (!q || q.voter !== false) return null;
  const voters = Array.isArray(q.voters) && q.voters.length ? q.voters.join(", ") : "none";
  if (spec && spec.config && spec.config.voter === false) {
    return "An observer by configuration (voter unchecked): it replicates the metadata log and never votes.";
  }
  return `An observer because it joined a running scenario. A KRaft quorum without KIP-853 is static: its voters (${voters}) were fixed when the scenario loaded. This broker replicates the metadata log without a vote, and becomes a voter the next time the scenario loads (a page reload, or reopening it from Saved).`;
}

// The group coordinator's part of a broker snapshot: the groups it
// coordinates, their members and their committed offsets.
function renderCoordinator(g, ctx) {
  const groups = g.groups && typeof g.groups === "object" ? Object.entries(g.groups) : [];
  const offsets = g.offsets && typeof g.offsets === "object" ? g.offsets : {};
  const body = el("div");
  const loaded = Array.isArray(g.loaded_partitions) ? g.loaded_partitions : [];
  const gr = [];
  addRow(gr, "__consumer_offsets partitions it leads", loaded.length ? `${loaded.length} of 50` : "none", "coordinator_partitions");
  addRow(gr, "held requests", g.held_requests, "coordinator_held");
  body.appendChild(kv(gr));
  if (!groups.length) body.appendChild(el("p", "lab-muted lab-small", "No group lives on the partitions this broker leads."));
  for (const [id, group] of groups) {
    const inner = el("div");
    const rows = [];
    addRow(rows, "type", group.type, "group_type");
    addRow(rows, "state", group.state, "group_state");
    addRow(rows, group.type === "classic" ? "generation" : "group epoch", group.generation ?? group.group_epoch, "group_epoch");
    addRow(rows, "assignment epoch", group.assignment_epoch, "group_assignment_epoch");
    addRow(rows, "topology epoch", group.topology_epoch, "group_topology_epoch");
    if (group.status && typeof group.status === "object") addRow(rows, "status", group.status.detail ?? group.status.code, "group_status");
    if (group.protocol_name) addRow(rows, "protocol", group.protocol_name, "group_protocol");
    inner.appendChild(kv(rows));
    const members = Array.isArray(group.members) ? group.members : [];
    inner.appendChild(
      table(
        [
          { key: "client_id", label: "member", render: (v, m) => titled(v ?? shortText(m.member_id, 10), `member id ${m.member_id}`) },
          { key: "epoch", label: "epoch", get: (m) => m.member_epoch ?? (group.type === "classic" ? group.generation : null) },
          { key: "state", label: "state", get: (m) => m.state ?? (m.awaiting_join ? "awaiting join" : m.awaiting_sync ? "awaiting sync" : "stable") },
          {
            key: "assigned",
            label: "assigned",
            get: (m) => m.assigned ?? m.tasks,
            render: (v, m) => {
              const revoking = assignedText(m.pending_revocation);
              return revoking && revoking !== "none" && revoking !== "–" ? `${assignedText(v)}, revoking ${revoking}` : assignedText(v);
            },
          },
        ],
        members,
        { rowKey: (m) => String(m.member_id) },
      ),
    );
    const committed = Array.isArray(offsets[id]) ? offsets[id] : [];
    if (committed.length) {
      inner.appendChild(
        section(
          `Committed offsets (${committed.length})`,
          table(
            [
              { key: "topic", label: "topic" },
              { key: "partition", label: "p" },
              { key: "offset", label: "offset" },
              { key: "leader_epoch", label: "epoch" },
              { key: "commit_timestamp", label: "at", render: (v) => (v == null ? "–" : `${fmtNum(v)} ms`) },
            ],
            committed,
            { rowKey: (o) => `${o.topic}-${o.partition}` },
          ),
          { open: false, nested: true },
        ),
      );
    }
    body.appendChild(section(`${id} · ${group.type ?? "group"} · ${group.state ?? "?"} · ${members.length} member${members.length === 1 ? "" : "s"}`, inner, { open: groups.length <= 2, nested: true }));
  }
  return section(`Groups it coordinates (${groups.length})`, body);
}

// `{topic: [partitions]}` or a streams member's `{active, standby, warmup}`
// task maps, as one line.
function assignedText(v) {
  if (v == null) return "–";
  if (typeof v !== "object") return String(v);
  if ("active" in v || "standby" in v || "warmup" in v) {
    const parts = [];
    for (const role of ["active", "standby", "warmup"]) {
      const tasks = v[role];
      if (!tasks || typeof tasks !== "object") continue;
      const ids = Object.entries(tasks).flatMap(([sub, ps]) => (Array.isArray(ps) ? ps.map((p) => `${sub}_${p}`) : []));
      if (ids.length) parts.push(`${role} ${ids.join(" ")}`);
    }
    return parts.length ? parts.join("; ") : "none";
  }
  const parts = Object.entries(v).map(([t, ps]) => `${t}[${Array.isArray(ps) ? ps.join(",") : ps}]`);
  return parts.length ? parts.join(" ") : "none";
}

// The followers a leader tracks: `id:leo` with the lag when there is one.
function followerList(v) {
  if (!Array.isArray(v) || !v.length) return "–";
  return v.map((f) => `${f.id}:${f.leo}${f.lag_ms ? ` (${fmtNum(f.lag_ms)} ms)` : ""}`).join(" ");
}

// This broker's log end, with the log start in the tooltip; a partition the
// broker holds no replica of has none.
function logEnd(v, p) {
  if (v == null) return titled("–", "no replica on this broker");
  return titled(fmtNum(v), `log ${fmtNum(p.log_start)}–${fmtNum(v)} · ${fmtNum(p.batches)} batches · ${fmtBytes(p.size_bytes)} · fetch ${p.fetch_state}`);
}

// Text with a tooltip.
function titled(text, title) {
  const span = el("span", null, String(text));
  span.title = title;
  return span;
}

// A sentence under a section, with a hook for the end-to-end check.
function note(text, field) {
  const p = el("p", "lab-note lab-small", text);
  if (field) p.dataset.field = field;
  return p;
}

// ---- real broker ------------------------------------------------------------------

// What `external.js` reports for a real broker: its process, connections,
// the tails of its stdout and stderr, the runtime's counters and the
// environment it was started with.
function renderRealBroker(root, s, used, ctx) {
  take(s, used, "external");
  const p = pickObj(s, used, "process");
  if (!p) {
    root.appendChild(el("p", "lab-muted", "This tab does not run the process: no report from it yet."));
    return;
  }
  const rows = [];
  addRow(rows, "state", p.state, "process_state");
  addRow(rows, "why", p.reason, "process_reason");
  if (p.lagging) addRow(rows, "lagging", "it missed one instant of the lab's clock and runs free until it waits again", "process_lagging");
  if (p.exit) addRow(rows, "exit", p.exit.message ?? p.exit.reason, "process_exit");
  addRow(rows, "address", p.address, "process_address");
  addRow(rows, "volume", p.volume, "process_volume");
  addRow(rows, "module", p.module, "process_module");
  addRow(rows, "incarnation", p.incarnation, "process_incarnation");
  addRow(rows, "started at", p.started_at_ms == null ? null : `${p.started_at_ms} ms`, "process_started");
  root.appendChild(section("Process", kv(rows)));
  if (Array.isArray(p.notes) && p.notes.length) {
    root.appendChild(section("Runtime notes", logBlock(p.notes.map((n) => `${n.level}: ${n.text}`), "process_notes"), { open: false }));
  }

  const c = take(s, used, "connections");
  if (c && typeof c === "object") {
    const conns = [];
    addRow(conns, "accepted", c.inbound, "conns_inbound");
    addRow(conns, "dialed", c.outbound, "conns_outbound");
    addRow(conns, "dials waiting for a link", c.waiting_dials, "conns_waiting");
    addRow(conns, "bytes held for the process", c.held_bytes, "conns_held");
    root.appendChild(section("Connections", kv(conns)));
  }
  for (const stream of ["stdout", "stderr"]) {
    const lines = take(s, used, stream);
    if (Array.isArray(lines)) root.appendChild(section(`${stream} (last ${lines.length} lines)`, logBlock(lines, stream)));
  }
  const runtime = take(s, used, "runtime");
  if (runtime && typeof runtime === "object") {
    const r = [];
    addRow(r, "guest clock", withUnit(runtime.clock_ms, " ms"), "runtime_clock_ms");
    addRow(r, "uptime (wall)", withUnit(runtime.uptime_ms, " ms"), "runtime_uptime_ms");
    addRow(r, "busy / blocked (wall)", runtime.busy_ms == null ? null : `${runtime.busy_ms} / ${runtime.blocked_ms} ms`, "runtime_busy");
    addRow(r, "polls", runtime.polls, "runtime_polls");
    addRow(r, "bytes in / out", runtime.bytes_in == null ? null : `${fmtBytes(runtime.bytes_in)} / ${fmtBytes(runtime.bytes_out)}`, "runtime_bytes");
    addRow(r, "sockets", runtime.sockets, "runtime_sockets");
    addRow(r, "accepted / dialed", runtime.accepted == null ? null : `${runtime.accepted} / ${runtime.dials}`, "runtime_conns");
    addRow(r, "files", runtime.files == null ? null : `${runtime.files} · ${fmtBytes(runtime.file_bytes)}`, "runtime_files");
    addRow(r, "fsyncs", runtime.syncs, "runtime_syncs");
    addRow(r, "journal", runtime.journal_flushes == null ? null : `${runtime.journal_flushes} flushes · ${fmtBytes(runtime.journal_bytes)}`, "runtime_journal");
    root.appendChild(section("Runtime", kv(r), { open: false }));
  }
  const env = take(s, used, "env");
  if (env && typeof env === "object") {
    const e = [];
    for (const [key, value] of Object.entries(env)) addRow(e, key, value, key);
    root.appendChild(section("Environment", kv(e), { open: false }));
  }
}

function logBlock(lines, field) {
  const pre = el("pre", "lab-raw lab-log", lines.length ? lines.join("\n") : "(nothing yet)");
  pre.dataset.field = field;
  return pre;
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

// The producer node (`lab::apps::producer`): the rate and the counters, the
// schema registration, the last records it generated, and the client
// producer's partitions and ack latency.
function renderProducer(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "topic", take(s, used, "topic"), "topic");
  addRow(rows, "rate", withUnit(take(s, used, "rate"), "/s"), "rate");
  addRow(rows, "paused", take(s, used, "paused"), "paused");
  addRow(rows, "generated", take(s, used, "generated"), "generated");
  addRow(rows, "sent", take(s, used, "sent"), "sent");
  addRow(rows, "acked", take(s, used, "acked"), "acked");
  addRow(rows, "failed", take(s, used, "failed"), "failed");
  addRow(rows, "retried", take(s, used, "retried"), "retried");
  addRow(rows, "waiting to send", take(s, used, "pending_records"), "pending_records");
  addRow(rows, "batches in flight", take(s, used, "in_flight_batches"), "in_flight_batches");
  take(s, used, "in_flight_requests", "batches_sent");
  const bytes = take(s, used, "bytes");
  addRow(rows, "bytes", bytes != null ? fmtBytes(bytes) : null, "bytes");
  addRow(rows, "acks", take(s, used, "acks"), "acks");
  addRow(rows, "idempotent", take(s, used, "idempotent"), "idempotent");
  addRow(rows, "producer id", take(s, used, "producer_id") ?? "none yet", "producer_id");
  addRow(rows, "producer epoch", take(s, used, "producer_epoch"), "producer_epoch");
  addRow(rows, "compression", take(s, used, "compression"), "compression");
  const deferred = take(s, used, "deferred_topics");
  if (Array.isArray(deferred) && deferred.length) addRow(rows, "waiting for metadata of", deferred.join(", "), "deferred_topics");
  root.appendChild(section("Producer", kv(rows)));

  const ser = take(s, used, "serialization");
  if (ser && typeof ser === "object") root.appendChild(serializationSection(ser, ctx));

  const records = take(s, used, "last_records");
  if (Array.isArray(records)) {
    root.appendChild(
      section(
        `Last records (${records.length})`,
        table(
          [
            { key: "seq", label: "seq" },
            { key: "partition", label: "p", render: (v) => (v == null ? "…" : String(v)) },
            { key: "offset", label: "offset", render: (v) => (v == null ? "unacked" : fmtNum(v)) },
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 22) },
            { key: "value_preview", label: "value", wrap: true, render: (v) => shortText(valueText(v), 60) },
          ],
          records.slice().reverse(),
          { rowKey: (r) => String(r.seq) },
        ),
      ),
    );
  }

  const parts = take(s, used, "partitions");
  if (Array.isArray(parts)) {
    root.appendChild(
      section(
        "Partitions",
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "last_offset", label: "last offset", render: (v) => (v == null ? "–" : fmtNum(v)) },
            { key: "next_sequence", label: "next seq" },
            { key: "records", label: "queued" },
            { key: "in_flight", label: "in flight" },
          ],
          parts,
          { rowKey: (p) => `${p.topic}-${p.partition}` },
        ),
        { open: false },
      ),
    );
  }

  const rtt = take(s, used, "rtt");
  if (rtt && typeof rtt === "object" && Array.isArray(rtt.buckets)) {
    const body = el("div");
    const rr = [];
    addRow(rr, "acknowledged batches", rtt.count, "rtt_count");
    addRow(rr, "mean", withUnit(rtt.mean_ms, " ms"), "rtt_mean");
    addRow(rr, "max", withUnit(rtt.max_ms, " ms"), "rtt_max");
    body.appendChild(kv(rr));
    const buckets = rtt.buckets.filter((b) => b.count > 0);
    if (buckets.length) body.appendChild(bars(buckets.map((b) => ({ label: b.le == null ? "more" : `≤${b.le} ms`, value: b.count }))));
    root.appendChild(section("Ack latency", body, { open: false }));
  }
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
}

// A producer's or streams app's schema registration.
function serializationSection(ser, ctx) {
  const rows = [];
  addRow(rows, "registry", ser.registry != null ? ctx.nodeName(ser.registry) : null, "ser_registry");
  addRow(rows, "subject", ser.subject, "ser_subject");
  addRow(rows, "format", ser.format, "ser_format");
  addRow(rows, "state", ser.state, "ser_state");
  addRow(rows, "schema id", ser.schema_id ?? "not yet", "ser_schema_id");
  addRow(rows, "not serialized", ser.failed, "ser_failed");
  addRow(rows, "last error", ser.error, "ser_error");
  const body = el("div");
  body.appendChild(kv(rows));
  if (ser.client && typeof ser.client === "object") body.appendChild(section("Registry client", jsonTree(ser.client, ctx), { open: false, nested: true }));
  return section("Serialization", body);
}

// ---- consumer -----------------------------------------------------------------------

// The consumer node (`lab::apps::consumer`): its membership, its assignment
// with positions, commits and lag, the processing backlog, and the last
// records it processed, decoded.
function renderConsumer(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "group", take(s, used, "group"), "group");
  addRow(rows, "protocol", take(s, used, "protocol"), "protocol");
  addRow(rows, "state", take(s, used, "state"), "state");
  addRow(rows, "member", shortText(take(s, used, "member_id"), 24), "member_id");
  addRow(rows, "epoch", take(s, used, "epoch"), "epoch");
  const coordinator = take(s, used, "coordinator");
  addRow(rows, "coordinator", coordinator != null ? ctx.nodeLabelForBroker(coordinator) : "none yet", "coordinator");
  const sub = take(s, used, "subscription");
  addRow(rows, "subscription", Array.isArray(sub) ? sub.join(", ") : sub, "subscription");
  addRow(rows, "paused", take(s, used, "paused"), "paused");
  addRow(rows, "processing per record", withUnit(take(s, used, "process_ms"), " ms"), "process_ms");
  addRow(rows, "processed", take(s, used, "processed"), "processed");
  addRow(rows, "polled, not processed", take(s, used, "processing_backlog"), "processing_backlog");
  addRow(rows, "lag", take(s, used, "lag"), "lag");
  addRow(rows, "polled", take(s, used, "polled"), "polled");
  addRow(rows, "records fetched", take(s, used, "records"), "records");
  addRow(rows, "fetches", take(s, used, "fetches"), "fetches");
  addRow(rows, "commits", take(s, used, "commits"), "commits");
  addRow(rows, "rebalances", take(s, used, "rebalances"), "rebalances");
  addRow(rows, "max.poll.records", take(s, used, "max_poll_records"), "max_poll_records");
  const des = take(s, used, "deserialize");
  if (des && typeof des === "object") addRow(rows, "decodes through", des.registry != null ? ctx.nodeName(des.registry) : shortJson(des, 40), "deserialize");
  root.appendChild(section("Consumer", kv(rows)));

  const assignment = take(s, used, "assignment");
  if (Array.isArray(assignment)) {
    root.appendChild(
      section(
        `Assignment (${assignment.length})`,
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "position", label: "position", render: (v) => (v == null ? "–" : fmtNum(v)) },
            { key: "committed", label: "committed", render: (v) => (v == null ? "–" : fmtNum(v)) },
            { key: "hwm", label: "HWM", render: (v) => (v == null ? "–" : fmtNum(v)) },
            { key: "lag", label: "lag", render: (v) => (v == null ? "–" : fmtNum(v)) },
          ],
          assignment,
          { rowKey: (a) => `${a.topic}-${a.partition}` },
        ),
      ),
    );
  }

  const records = take(s, used, "last_records");
  if (Array.isArray(records)) {
    root.appendChild(
      section(
        `Last records (${records.length})`,
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "offset", label: "offset" },
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 22) },
            { key: "value_preview", label: "value", wrap: true, render: (v) => shortText(valueText(v), 60) },
            { key: "schema_id", label: "schema", render: (v) => (v == null ? "–" : `id ${v}`) },
          ],
          records.slice().reverse(),
          { rowKey: (r) => `${r.topic}-${r.partition}-${r.offset}` },
        ),
      ),
    );
  }
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
}

// ---- streams --------------------------------------------------------------------------

// The streams node (`lab::apps::streams`): its KIP-1071 membership, the
// compiled topology with its internal topics, the tasks with their phase and
// lag, the contents of the local stores, and the last sink records.
function renderStreams(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "application", take(s, used, "application_id"), "application_id");
  addRow(rows, "state", take(s, used, "state"), "state");
  addRow(rows, "member", shortText(take(s, used, "member_id"), 24), "member_id");
  addRow(rows, "member epoch", take(s, used, "member_epoch"), "member_epoch");
  addRow(rows, "paused", take(s, used, "paused"), "paused");
  addRow(rows, "records in", take(s, used, "records_in"), "records_in");
  addRow(rows, "records out", take(s, used, "records_out"), "records_out");
  addRow(rows, "commits", take(s, used, "commits"), "commits");
  addRow(rows, "commit interval", withUnit(take(s, used, "commit_interval_ms"), " ms"), "commit_interval_ms");
  const error = take(s, used, "error");
  addRow(rows, "error", error, "error");
  const des = take(s, used, "deserialize");
  if (des && typeof des === "object") addRow(rows, "decodes through", des.registry != null ? ctx.nodeName(des.registry) : shortJson(des, 40), "deserialize");
  root.appendChild(section("Streams", kv(rows)));

  const m = take(s, used, "membership");
  if (m && typeof m === "object") {
    const mr = [];
    addRow(mr, "group state", m.state, "membership_state");
    addRow(mr, "heartbeats", m.heartbeats, "membership_heartbeats");
    addRow(mr, "heartbeat interval", withUnit(m.heartbeat_interval_ms, " ms"), "membership_interval");
    addRow(mr, "process id", shortText(m.process_id, 13), "membership_process");
    addRow(mr, "owned active", Array.isArray(m.owned_active) ? m.owned_active.join(" ") || "none" : null, "owned_active");
    addRow(mr, "owned standby", Array.isArray(m.owned_standby) && m.owned_standby.length ? m.owned_standby.join(" ") : null, "owned_standby");
    const body = el("div");
    body.appendChild(kv(mr));
    const status = Array.isArray(m.status) ? m.status : [];
    if (status.length) body.appendChild(table([{ key: "name", label: "status" }, { key: "detail", label: "detail", wrap: true }], status, { rowKey: (x) => String(x.name ?? x.code) }));
    root.appendChild(section("Streams group membership", body, { open: status.length > 0 }));
  }

  const topology = take(s, used, "topology");
  if (topology && typeof topology === "object") {
    const body = el("div");
    const tr = [];
    addRow(tr, "source", topology.source, "topology_source");
    addRow(tr, "sink", topology.sink, "topology_sink");
    addRow(tr, "repartition topics", Array.isArray(topology.repartition_topics) ? topology.repartition_topics.join(", ") || "none" : null, "topology_repartition");
    body.appendChild(kv(tr));
    const subs = Array.isArray(topology.subtopologies) ? topology.subtopologies : [];
    if (subs.length) {
      body.appendChild(
        table(
          [
            { key: "id", label: "sub" },
            { key: "source_topics", label: "reads", get: (x) => [...(x.source_topics || []), ...(x.repartition_source_topics || [])], render: listText },
            { key: "repartition_sink_topics", label: "repartitions to", render: listText },
            { key: "changelog_topics", label: "changelogs", render: listText },
          ],
          subs,
          { rowKey: (x) => String(x.id) },
        ),
      );
    }
    const stores = Array.isArray(topology.stores) ? topology.stores : [];
    if (stores.length) {
      body.appendChild(
        table(
          [
            { key: "name", label: "store" },
            { key: "kind", label: "kind" },
            { key: "changelog", label: "changelog" },
          ],
          stores,
          { rowKey: (x) => x.name },
        ),
      );
    }
    root.appendChild(section("Topology", body));
  }

  const tasks = take(s, used, "tasks");
  if (Array.isArray(tasks)) {
    root.appendChild(
      section(
        `Tasks (${tasks.length})`,
        table(
          [
            { key: "id", label: "task" },
            { key: "role", label: "role" },
            { key: "phase", label: "phase" },
            { key: "partitions", label: "partitions", wrap: true, render: listText },
            { key: "records_in", label: "in" },
            { key: "records_out", label: "out" },
            { key: "changelog_out", label: "logged" },
            { key: "restored", label: "restored" },
            { key: "lag", label: "lag" },
            { key: "skipped", label: "skipped" },
          ],
          tasks,
          { rowKey: (t) => String(t.id) },
        ),
      ),
    );
  }

  const stores = take(s, used, "stores");
  if (Array.isArray(stores)) {
    const body = el("div");
    for (const st of stores) {
      const entries = Array.isArray(st.entries) ? st.entries : [];
      const inner = table(
        [
          { key: "key", label: "key", render: (v) => shortText(valueText(v), 30) },
          { key: "value", label: "value", render: storeValue },
        ],
        entries.map(([key, value]) => ({ key, value })),
        { rowKey: (e) => valueText(e.key) },
      );
      const count = entries.length >= STORE_ROWS ? `the first ${entries.length} entries` : `${entries.length} entr${entries.length === 1 ? "y" : "ies"}`;
      const box = section(`${st.name} · task ${st.task} · ${count}`, inner, { open: stores.length <= 3, nested: true });
      box.dataset.store = `${st.name}/${st.task}`;
      body.appendChild(box);
    }
    if (!stores.length) body.appendChild(el("p", "lab-muted lab-small", "No active task holds a store yet."));
    root.appendChild(section(`Store contents (${stores.length})`, body));
  }

  const outputs = take(s, used, "last_outputs");
  if (Array.isArray(outputs)) {
    root.appendChild(
      section(
        `Last outputs (${outputs.length})`,
        table(
          [
            { key: "topic", label: "topic" },
            { key: "key", label: "key", render: (v) => shortText(valueText(v), 22) },
            { key: "value", label: "value", wrap: true, render: (v) => shortText(valueText(v), 60) },
            { key: "timestamp", label: "at", render: (v) => (v == null ? "–" : `${fmtNum(v)} ms`) },
          ],
          outputs,
        ),
        { open: false },
      ),
    );
  }

  const producer = take(s, used, "producer");
  if (producer && typeof producer === "object") {
    const pr = [];
    addRow(pr, "acked", producer.acked, "producer_acked");
    addRow(pr, "failed", producer.failed, "producer_failed");
    addRow(pr, "waiting to send", producer.pending, "producer_pending");
    addRow(pr, "requests in flight", producer.in_flight_requests, "producer_in_flight");
    root.appendChild(section("Record collector", kv(pr), { open: false }));
  }
  const ser = take(s, used, "serialize");
  if (ser && typeof ser === "object") root.appendChild(serializationSection(ser, ctx));
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
}

// How many entries of a store a streams snapshot lists per task.
const STORE_ROWS = 20;

// A store value: a count or a sum, or a window `{window_start, window_end, count}`.
function storeValue(v) {
  if (v && typeof v === "object" && "window_start" in v) return `[${fmtNum(v.window_start)}, ${fmtNum(v.window_end)}) → ${v.count}`;
  return shortText(valueText(v), 50);
}

function listText(v) {
  if (!Array.isArray(v) || !v.length) return "–";
  return v.join(", ");
}

// ---- the scenario's admin ------------------------------------------------------------------

// The hidden admin node (`lab::apps::admin`): the scenario's topics and how
// far their `CreateTopics` got.
function renderAdmin(root, s, used, ctx) {
  const topics = take(s, used, "topics");
  if (Array.isArray(topics)) {
    root.appendChild(
      section(
        `Topics (${topics.length})`,
        table(
          [
            { key: "name", label: "topic" },
            { key: "partitions", label: "p" },
            { key: "replication_factor", label: "rf" },
            { key: "status", label: "status" },
            { key: "error", label: "error", render: (v) => (v == null ? "–" : String(v)) },
          ],
          topics,
          { rowKey: (t) => t.name },
        ),
      ),
    );
  }
  const client = take(s, used, "client");
  if (client && typeof client === "object") root.appendChild(section("Client", jsonTree(client, ctx), { open: false }));
  renderCounters(root, s, used, ctx);
}

// ---- echo, pinger, unknown kinds ------------------------------------------------------------

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

// A table. Each cell carries `data-col` (the column key) and, with
// `rowKey`, each row `data-row`, so the end-to-end check can read one cell.
export function table(columns, rows, { rowKey } = {}) {
  const wrap = el("div", "lab-table-wrap");
  const t = el("table", "lab-table");
  const thead = el("thead");
  const hr = el("tr");
  for (const c of columns) hr.appendChild(el("th", null, c.label));
  thead.appendChild(hr);
  const tbody = el("tbody");
  for (const row of rows) {
    const tr = el("tr");
    if (rowKey) tr.dataset.row = String(rowKey(row));
    for (const c of columns) {
      const raw = c.get ? c.get(row) : row[c.key];
      const text = c.render ? c.render(raw, row) : cell(raw);
      const td = el("td", c.wrap ? "lab-td-wrap" : null);
      td.dataset.col = c.key;
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
  wrap.appendChild(t);
  return wrap;
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

