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

import { el, fmtNum, fmtBytes, plural, shortJson } from "./dom.js";
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
  "krabka-broker": renderRealBroker,
  "schema-registry": renderRegistry,
  producer: renderProducer,
  consumer: renderConsumer,
  streams: renderStreams,
  echo: renderCounters,
  pinger: renderCounters,
  admin: renderAdmin,
};

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
    if (Array.isArray(lines)) root.appendChild(section(`${stream} (last ${plural(lines.length, "line")})`,logBlock(lines, stream), { open: false }));
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

// The schema registry (`lab::registry`): its startup and serving state, the
// write queue, the `_schemas` store on the brokers (its setup step, topic,
// reader and producer), and the subjects it replayed from that topic.
function renderRegistry(root, s, used, ctx) {
  const rows = [];
  addRow(rows, "state", take(s, used, "state"), "state");
  addRow(rows, "startup error", take(s, used, "error"), "error");
  addRow(rows, "starts", take(s, used, "started"), "started");
  const bootstrap = take(s, used, "bootstrap");
  addRow(rows, "bootstrap", Array.isArray(bootstrap) ? bootstrap.map((b) => ctx.nodeName(b)).join(", ") : null, "bootstrap");
  addRow(rows, "compatibility", take(s, used, "compatibility"), "compatibility");
  addRow(rows, "mode", take(s, used, "mode"), "mode");
  addRow(rows, "schemas", take(s, used, "schemas"), "schemas");
  addRow(rows, "_schemas records read", take(s, used, "records"), "records");
  addRow(rows, "applied up to", take(s, used, "applied"), "applied");
  const unknown = take(s, used, "unknown_records");
  const undecodable = take(s, used, "undecodable_records");
  if (unknown || undecodable) addRow(rows, "records skipped", `${unknown ?? 0} unknown · ${undecodable ?? 0} undecodable`, "skipped_records");
  addRow(rows, "requests", take(s, used, "requests"), "requests");
  addRow(rows, "error answers", take(s, used, "errors"), "errors");
  addRow(rows, "connections", take(s, used, "connections"), "connections");
  addRow(rows, "connections refused", take(s, used, "refused"), "refused");
  const writes = take(s, used, "writes");
  if (writes && typeof writes === "object") {
    addRow(rows, "writes queued", writes.queued, "writes_queued");
    const a = writes.active;
    addRow(rows, "write in progress", a ? `${a.op} ${a.path} · ${String(a.stage).replace("_", " ")}` : "none", "writes_active");
  }
  root.appendChild(section("Registry", kv(rows)));

  const election = take(s, used, "election");
  const forwarder = take(s, used, "forwarder");
  if ((election && typeof election === "object") || (forwarder && typeof forwarder === "object")) root.appendChild(electionSection(election || {}, forwarder, ctx));

  const store = take(s, used, "store");
  if (store && typeof store === "object") root.appendChild(storeSection(store, ctx));
  else if (store === null && used.has("store")) root.appendChild(el("p", "lab-muted lab-small", "The node is down: its store is closed."));

  const cfg = take(s, used, "config");
  if (cfg && typeof cfg === "object") {
    const cr = [];
    for (const [key, value] of Object.entries(cfg)) addRow(cr, key, value, key);
    root.appendChild(section("kafkastore config", kv(cr), { open: false }));
  }

  const subjects = take(s, used, "subjects");
  if (Array.isArray(subjects)) {
    const body = el("div");
    for (const sub of subjects) {
      const versions = Array.isArray(sub.versions) ? sub.versions : [];
      const inner = el("div");
      const sr = [];
      addRow(sr, "compatibility", sub.compatibility, "subject_compatibility");
      addRow(sr, "mode", sub.mode, "subject_mode");
      if (sr.length) inner.appendChild(kv(sr));
      inner.appendChild(
        table(
          [
            { key: "version", label: "version" },
            { key: "id", label: "id" },
            { key: "deleted", label: "deleted", render: bool },
          ],
          versions,
          { rowKey: (v) => `${sub.subject}/${v.version}` },
        ),
      );
      body.appendChild(section(sub.subject, inner, { open: subjects.length <= 3, nested: true }));
    }
    if (!subjects.length) body.appendChild(el("p", "lab-muted lab-small", "No subject yet."));
    root.appendChild(section(`Subjects (${subjects.length})`, body));
  }
}

// The registry's part in its group: whether it is the primary, which
// instance is, and the writes it forwarded to it.
function electionSection(e, forwarder, ctx) {
  const rows = [];
  addRow(rows, "role", e.is_leader ? "primary" : e.leader ? "secondary: forwards writes to the primary" : e.joined ? "no primary known" : "joining the group", "election_role");
  addRow(rows, "primary", e.leader ?? "none known", "election_leader");
  addRow(rows, "this instance", e.url, "election_url");
  addRow(rows, "may lead", e.eligible, "election_eligible");
  const m = e.member;
  if (m && typeof m === "object") {
    addRow(rows, "group", m.group, "election_group");
    addRow(rows, "member state", m.state, "election_member_state");
    addRow(rows, "generation", m.generation, "election_generation");
    addRow(rows, "joins", m.joins, "election_joins");
    addRow(rows, "last error", m.last_error, "election_error");
  }
  if (forwarder && typeof forwarder === "object") {
    addRow(rows, "writes forwarded", forwarder.forwarded, "forwarded");
    addRow(rows, "forwards failed", forwarder.failed, "forward_failed");
    const a = forwarder.active;
    if (a && typeof a === "object") addRow(rows, "forwarding", `${a.method} ${a.path} → ${a.to}`, "forward_active");
  }
  const body = el("div");
  body.appendChild(kv(rows));
  if (m && m.client && typeof m.client === "object") body.appendChild(section("Group client", jsonTree(m.client, ctx), { open: false, nested: true }));
  return section("Primary election", body);
}

// The registry's `_schemas` store: Confluent's startup steps, the topic, the
// reader that replays it, and the producer, consumer and admin client
// underneath.
function storeSection(store, ctx) {
  const rows = [];
  addRow(rows, "state", store.state, "store_state");
  addRow(rows, "startup step", store.step ? String(store.step).replace(/_/g, " ") : "done", "store_step");
  addRow(rows, "error", store.error, "store_error");
  const t = store.topic;
  addRow(
    rows,
    "topic",
    t ? `${t.name} · ${t.partitions} partition${t.partitions === 1 ? "" : "s"} · rf ${t.replication_factor} · ${t.cleanup_policy}${t.created ? " · created by this registry" : ""}` : "not set up yet",
    "store_topic",
  );
  const r = store.reader;
  if (r && typeof r === "object") {
    addRow(rows, "reader offset", r.offset, "reader_offset");
    addRow(rows, "reader end offset", r.end_offset ?? "not known yet", "reader_end_offset");
    addRow(rows, "reader", `${r.phase}${r.leader != null ? ` · from ${ctx.nodeLabelForBroker(r.leader)}` : ""}`, "reader_phase");
    if (r.last_error != null) addRow(rows, "reader error code", r.last_error, "reader_error");
  }
  addRow(rows, "last written offset", store.last_written_offset ?? "unknown", "last_written_offset");
  const task = store.task;
  if (task && typeof task === "object") addRow(rows, "store task", `${task.kind} · ${task.stage}${task.records_left != null ? ` · ${task.records_left} left` : ""}`, "store_task");
  addRow(rows, "records put", store.puts, "store_puts");
  addRow(rows, "NOOP records", store.noops, "store_noops");
  const body = el("div");
  body.appendChild(kv(rows));
  for (const [name, part] of [["Producer", store.producer], ["Reader consumer", r?.consumer], ["Admin client", store.admin]]) {
    if (part && typeof part === "object") body.appendChild(section(name, jsonTree(part, ctx), { open: false, nested: true }));
  }
  return section("_schemas store", body);
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
  addRow(rows, "closed", take(s, used, "closed"), "closed");
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
            { key: "schema_id", label: "schema", render: (v) => (v == null ? "–" : `id ${v}`) },
            { key: "value_preview", label: "value", wrap: true, render: (v) => shortText(valueText(v), 60) },
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

  // The client producer a stream thread writes its sink, repartition and
  // changelog records through.
  const producer = take(s, used, "producer");
  if (producer && typeof producer === "object") {
    const pr = [];
    addRow(pr, "sent", producer.sent, "producer_sent");
    addRow(pr, "acked", producer.acked, "producer_acked");
    addRow(pr, "failed", producer.failed, "producer_failed");
    addRow(pr, "retried", producer.retried, "producer_retried");
    addRow(pr, "waiting to send", producer.pending_records, "producer_pending");
    addRow(pr, "batches in flight", producer.in_flight_batches, "producer_in_flight");
    addRow(pr, "producer id", producer.producer_id ?? "none yet", "producer_id");
    const body = el("div");
    body.appendChild(kv(pr));
    if (Array.isArray(producer.partitions) && producer.partitions.length) {
      body.appendChild(
        table(
          [
            { key: "topic", label: "topic" },
            { key: "partition", label: "p" },
            { key: "last_offset", label: "last offset", render: (v) => (v == null ? "–" : fmtNum(v)) },
            { key: "records", label: "queued" },
          ],
          producer.partitions,
          { rowKey: (p) => `${p.topic}-${p.partition}` },
        ),
      );
    }
    root.appendChild(section("Producer", body, { open: false }));
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

