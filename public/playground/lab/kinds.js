// The node-kind catalogue.
//
// One entry per kind the crate builds: the label and glyph the canvas draws,
// the form fields the palette turns into a `config` object (keys follow
// `playground/docs/lab-design.md`), the edges the canvas derives from a
// config, the one-line status the card shows, and a probe config the boot
// sequence uses to find out which kinds the loaded module accepts.
//
// Field spec keys:
//   key        the config key (nested through `group` fields)
//   type       text | number | boolean | select | list | noderef | noderefs |
//              json | group | ops
//   default    the initial value; a `boolean`/`select`/`number` with
//              `emitDefault: false` is left out of the config while it still
//              holds the default, so a kind that does not know the key never
//              sees it
//   required   the form refuses an empty value
//   of         for node references: the kinds the picker offers
//   fields     for a group: the nested specs; `optional: true` adds an
//              enable checkbox and leaves the whole group out when unchecked
//   stringify  for json: store the document as a JSON string, not an object

import { renderView } from "./views.js";

export const KAFKA_PORT = 9092;
export const HTTP_PORT = 8081;

const BOOTSTRAP = {
  key: "bootstrap",
  label: "Bootstrap brokers",
  type: "noderefs",
  of: ["broker"],
  required: true,
  help: "The brokers the client connects to first. Metadata leads it to the rest.",
};

const OPS = ["filter", "map", "select_key", "count_by_key", "sum_by_key", "window_count"];

export const KINDS = {
  broker: {
    kind: "broker",
    label: "Broker",
    glyph: "▣",
    color: "#f7b73a",
    description: "A Krabka broker: KRaft voter, partition leader or follower, group coordinator.",
    listens: KAFKA_PORT,
    probe: { broker_id: 99 },
    fields: [
      { key: "broker_id", label: "Broker id", type: "number", required: true, min: 0, step: 1, help: "Unique across the cluster. Defaults to the node id." },
      { key: "rack", label: "Rack", type: "text", placeholder: "a", help: "Optional rack label for replica placement." },
      { key: "voter", label: "KRaft voter", type: "boolean", default: true, emitDefault: false, help: "Unchecked, the broker joins the quorum as an observer." },
    ],
    edges: () => [],
    status: (s) => {
      const q = s.quorum && typeof s.quorum === "object" ? s.quorum : s;
      const role = q.role ?? q.state;
      const epoch = q.epoch ?? q.leader_epoch;
      const brokers = countOf(s.brokers);
      const parts = [];
      if (role != null) parts.push(brokers != null ? `${String(role).toLowerCase()} of ${brokers}` : String(role).toLowerCase());
      if (epoch != null) parts.push(`e${epoch}`);
      const ctrl = q.controller_id ?? q.controller ?? s.controller_id ?? s.controller;
      const me = s.broker_id ?? s.id;
      if (ctrl != null && me != null && Number(ctrl) === Number(me)) parts.push("ctrl");
      else if (q.is_controller || s.is_controller) parts.push("ctrl");
      const topics = countOf(s.topics);
      if (topics != null) parts.push(`${topics} topics`);
      return parts.join(" · ");
    },
  },
  "schema-registry": {
    kind: "schema-registry",
    label: "Schema registry",
    glyph: "◈",
    color: "#5aa0e0",
    description: "A Confluent-compatible schema registry: the REST API over the _schemas log, kept in this browser.",
    listens: HTTP_PORT,
    probe: {},
    fields: [
      { ...BOOTSTRAP, required: false, help: "The brokers the Kafka-backed store will use; optional until that store lands." },
      { key: "compatibility", label: "Default compatibility", type: "select", default: "BACKWARD", emitDefault: false, options: ["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"] },
      { key: "mode", label: "Mode", type: "select", default: "READWRITE", emitDefault: false, options: ["READWRITE", "READONLY", "IMPORT"] },
    ],
    edges: (spec) => (spec.config?.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap")),
    status: (s) => {
      const subjects = countOf(s.subjects);
      const parts = [];
      if (subjects != null) parts.push(`${subjects} subjects`);
      const schemas = s.schemas != null ? countOf(s.schemas) : null;
      if (schemas != null) parts.push(`${schemas} schemas`);
      if (s.requests != null && typeof s.requests !== "object") parts.push(`${s.requests} req`);
      return parts.join(" · ");
    },
  },
  producer: {
    kind: "producer",
    label: "Producer",
    glyph: "▶",
    color: "#45c178",
    description: "A Kafka producer with key and value templates, optionally serialized through the registry.",
    probe: { bootstrap: [1], topic: "t" },
    fields: [
      BOOTSTRAP,
      { key: "topic", label: "Topic", type: "text", required: true, placeholder: "orders" },
      { key: "rate_per_sec", label: "Records per second", type: "number", default: 5, min: 0, step: 1 },
      { key: "acks", label: "acks", type: "select", default: -1, options: [{ value: -1, label: "all (-1)" }, { value: 1, label: "leader (1)" }, { value: 0, label: "none (0)" }] },
      { key: "key", label: "Key", type: "group", fields: [
        { key: "pattern", label: "Pattern", type: "text", default: "customer-{seq % 10}", help: "Placeholders: {seq}, {rand a b}, {now}, {pick a|b|c}." },
      ] },
      { key: "value", label: "Value", type: "group", fields: [
        { key: "format", label: "Format", type: "select", default: "json", options: ["json"] },
        { key: "template", label: "Template", type: "json", default: { id: "{seq}", total: "{rand 1 500}" }, help: "A JSON document; string values take the same placeholders." },
      ] },
      { key: "serialization", label: "Schema registry serialization", type: "group", optional: true, fields: [
        { key: "registry", label: "Registry", type: "noderef", of: ["schema-registry"], required: true },
        { key: "format", label: "Format", type: "select", default: "avro", options: ["avro", "json"] },
        { key: "schema", label: "Schema", type: "json", stringify: true, default: { type: "record", name: "Order", fields: [{ name: "id", type: "long" }, { name: "total", type: "double" }] }, help: "Registered under <topic>-value before the first record." },
      ] },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      if (c.topic) out.push(edge(spec.id, topic(c.topic), "produce"));
      if (c.serialization?.registry) out.push(edge(spec.id, node(c.serialization.registry), "registry"));
      return out;
    },
    status: (s) => {
      const parts = [];
      const acked = s.acked ?? s.records_acked;
      if (acked != null) parts.push(`${acked} acked`);
      if (s.failed) parts.push(`${s.failed} failed`);
      const rate = s.rate_per_sec ?? s.rate;
      if (rate != null && acked == null) parts.push(`${rate}/s`);
      return parts.join(" · ");
    },
  },
  consumer: {
    kind: "consumer",
    label: "Consumer",
    glyph: "◀",
    color: "#c084fc",
    description: "A consumer group member, classic or KIP-848, that commits offsets as it reads.",
    probe: { bootstrap: [1], group: "g", topics: ["t"] },
    fields: [
      BOOTSTRAP,
      { key: "group", label: "Group", type: "text", required: true, placeholder: "billing" },
      { key: "topics", label: "Topics", type: "list", required: true, placeholder: "orders, payments", help: "Comma-separated." },
      { key: "protocol", label: "Protocol", type: "select", default: "consumer", options: [{ value: "consumer", label: "consumer (KIP-848)" }, { value: "classic", label: "classic (JoinGroup/SyncGroup)" }] },
      { key: "auto_offset_reset", label: "auto.offset.reset", type: "select", default: "earliest", options: ["earliest", "latest"] },
      { key: "process_ms", label: "Processing time per record (ms)", type: "number", default: 2, min: 0, step: 1 },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      for (const t of c.topics || []) out.push(edge(topic(t), spec.id, "consume"));
      return out;
    },
    status: (s) => {
      const lag = totalLag(s);
      const parts = [];
      if (lag != null) parts.push(`lag ${lag}`);
      const n = countOf(s.assignment ?? s.assigned);
      if (n != null) parts.push(`${n} partitions`);
      if (s.state) parts.push(String(s.state).toLowerCase());
      return parts.join(" · ");
    },
  },
  streams: {
    kind: "streams",
    label: "Streams app",
    glyph: "⋈",
    color: "#ff8466",
    description: "A krabka-client-streams application: a topology over a source topic into a sink, joined through the KIP-1071 streams group.",
    probe: { bootstrap: [1], application_id: "a", topology: { source: "t", ops: [], sink: "u" } },
    fields: [
      BOOTSTRAP,
      { key: "application_id", label: "Application id", type: "text", required: true, placeholder: "order-stats" },
      { key: "topology", label: "Topology", type: "group", fields: [
        { key: "source", label: "Source topic", type: "text", required: true, placeholder: "orders" },
        { key: "ops", label: "Operations", type: "ops", default: [] },
        { key: "sink", label: "Sink topic", type: "text", required: true, placeholder: "order-counts" },
      ] },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      if (c.topology?.source) out.push(edge(topic(c.topology.source), spec.id, "consume"));
      if (c.topology?.sink) out.push(edge(spec.id, topic(c.topology.sink), "produce"));
      return out;
    },
    status: (s) => {
      const parts = [];
      const m = s.member && typeof s.member === "object" ? s.member : s;
      if (m.state) parts.push(String(m.state).toLowerCase());
      const tasks = countOf(s.tasks ?? s.active_tasks);
      if (tasks != null) parts.push(`${tasks} tasks`);
      const out = s.records_out ?? s.out;
      if (out != null) parts.push(`${out} out`);
      return parts.join(" · ");
    },
  },
  echo: {
    kind: "echo",
    label: "Echo",
    glyph: "◎",
    color: "#94a3b8",
    description: "Answers every data frame with the same bytes. A network probe target.",
    listens: KAFKA_PORT,
    probe: {},
    fields: [],
    edges: () => [],
    status: (s) => (s.frames != null ? `${s.frames} frames` : ""),
  },
  pinger: {
    kind: "pinger",
    label: "Pinger",
    glyph: "◉",
    color: "#38bdf8",
    description: "Opens a connection to a target and pings it on a period; reports the mean round trip.",
    probe: { target: 1 },
    fields: [
      { key: "target", label: "Target", type: "noderef", of: ["echo", "broker", "schema-registry", "pinger"], required: true },
      { key: "period_ms", label: "Period (ms)", type: "number", default: 100, min: 1, step: 1 },
      { key: "port", label: "Port", type: "number", default: KAFKA_PORT, emitDefault: false, min: 0, max: 65535, step: 1, help: "9092 for a broker or echo, 8081 for a registry." },
    ],
    edges: (spec) => (spec.config?.target != null ? [edge(spec.id, node(spec.config.target), "ping")] : []),
    status: (s) => {
      if (s.echoes == null) return "";
      const parts = [`${s.echoes}/${s.sent ?? "?"} echoed`];
      if (s.mean_rtt_ms != null) parts.push(`${s.mean_rtt_ms} ms rtt`);
      return parts.join(" · ");
    },
  },
  admin: {
    kind: "admin",
    label: "Admin",
    glyph: "⚙",
    color: "#6b7280",
    description: "The scenario's hidden admin client: it creates the topics through CreateTopics once the cluster has a controller.",
    hidden: true,
    fields: [],
    edges: (spec) => (spec.config?.bootstrap || []).slice(0, 1).map((b) => edge(spec.id, node(b), "bootstrap")),
    status: (s) => (s && typeof s === "object" && s.state ? String(s.state) : ""),
  },
};

// The kinds the palette offers, in order.
export const KIND_ORDER = ["broker", "schema-registry", "producer", "consumer", "streams", "echo", "pinger"];

const UNKNOWN = {
  kind: "?",
  label: "Unknown",
  glyph: "?",
  color: "#6b7280",
  description: "",
  fields: [],
  edges: () => [],
  status: () => "",
};

export function kindOf(kind) {
  return KINDS[kind] || { ...UNKNOWN, kind: String(kind), label: String(kind) };
}

// The default name for a fresh node of `kind`.
export function defaultName(kind, id) {
  return `${kind}-${id}`;
}

// Endpoint constructors for edges: a node id, or a topic by name.
export const node = (id) => ({ node: Number(id) });
export const topic = (name) => ({ topic: String(name) });
export const edge = (from, to, type) => ({
  from: typeof from === "object" ? from : node(from),
  to: typeof to === "object" ? to : node(to),
  type,
});

// Every edge the canvas draws for a scenario: a client to each bootstrap
// broker, producers to their topic, topics to their consumers, streams from
// their source and to their sink.
export function derivedEdges(scenario) {
  const out = [];
  for (const spec of scenario.nodes || []) {
    const k = kindOf(spec.kind);
    try {
      for (const e of k.edges(spec, scenario)) out.push({ ...e, owner: spec.id });
    } catch {
      // A malformed config draws no edges.
    }
  }
  return out;
}

// The topic names on the canvas: the scenario's topics plus every topic a
// config refers to.
export function topicNames(scenario) {
  const names = new Set((scenario.topics || []).map((t) => t.name).filter(Boolean));
  for (const e of derivedEdges(scenario)) {
    if (e.from.topic) names.add(e.from.topic);
    if (e.to.topic) names.add(e.to.topic);
  }
  return [...names];
}

// The card's status line for a node snapshot; never throws.
export function statusLine(nodeSnap) {
  const k = kindOf(nodeSnap.kind);
  const state = nodeSnap.state && typeof nodeSnap.state === "object" ? nodeSnap.state : {};
  try {
    return k.status(state, nodeSnap) || "";
  } catch {
    return "";
  }
}

// The inspector's kind-specific view.
export function renderState(nodeSnap, ctx) {
  return renderView(nodeSnap.kind, nodeSnap.state, ctx);
}

// Probe the loaded module: which kinds does it accept? A kind whose
// constructor answers "not implemented" is marked unavailable, so the
// palette can say so and the presets that need it carry a badge.
export function probeAvailability(Lab) {
  const available = {};
  let lab = null;
  try {
    lab = new Lab(1);
  } catch {
    lab = null;
  }
  if (!lab) {
    for (const kind of KIND_ORDER) available[kind] = true;
    return available;
  }
  for (const kind of KIND_ORDER) {
    const spec = { id: 0, kind, name: `probe-${kind}`, x: 0, y: 0, config: KINDS[kind].probe || {} };
    try {
      lab.addNode(JSON.stringify(spec));
      available[kind] = true;
    } catch (err) {
      const text = String(err && err.message ? err.message : err);
      available[kind] = !/not implemented/i.test(text);
    }
  }
  try {
    lab.free();
  } catch {
    // Nothing to release.
  }
  return available;
}

// ---- helpers over loosely-shaped snapshots ------------------------------------------

function countOf(v) {
  if (v == null) return null;
  if (Array.isArray(v)) return v.length;
  if (typeof v === "object") return Object.keys(v).length;
  if (typeof v === "number") return v;
  return null;
}

// The total consumer lag from any of the shapes a consumer may report.
export function totalLag(s) {
  if (typeof s.lag === "number") return s.lag;
  const rows = s.positions ?? s.partitions ?? s.assignment;
  if (Array.isArray(rows)) {
    let sum = 0;
    let seen = false;
    for (const r of rows) {
      if (r && typeof r === "object" && typeof r.lag === "number") {
        sum += r.lag;
        seen = true;
      }
    }
    return seen ? sum : null;
  }
  if (rows && typeof rows === "object") {
    let sum = 0;
    let seen = false;
    for (const r of Object.values(rows)) {
      if (r && typeof r === "object" && typeof r.lag === "number") {
        sum += r.lag;
        seen = true;
      }
    }
    return seen ? sum : null;
  }
  return null;
}
