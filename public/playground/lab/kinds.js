// The node-kind catalogue.
//
// One entry per kind the crate builds: the label and glyph the canvas draws,
// the form fields the palette turns into a `config` object (keys follow the
// module documentation of each node kind in `playground/src/lab/`), the
// values a new node of the kind starts with, the control commands it takes,
// the edges the canvas derives from a config, the one-line status the card
// shows, and a probe config the boot sequence uses to find out which kinds
// the loaded module accepts. A kind marked `real` runs the real code in a
// process of this tab (`external.js`); one marked `pinned` never moves to
// another tab of a session.
//
// Field spec keys:
//   key        the config key (nested through `group` fields)
//   type       text | number | boolean | select | list | noderef | noderefs |
//              json | group | advanced | record-value | ops
//   default    what the node does when the key is missing; with
//              `emitDefault: false` a field that holds it is left out of the
//              config, so a spec carries only what differs from the node
//   required   the form refuses an empty value
//   of         for node references: the kinds the picker offers
//   fields     for a group: the nested specs; `optional: true` adds an
//              enable checkbox and leaves the whole group out when unchecked.
//              For `advanced`: fields folded behind a disclosure, whose keys
//              sit beside the others in the same config object
//   stringify  for json: store the document as a JSON string, not an object
//   validate   a function that returns an error message or null
//
// `suggest(ctx)` gives the config a node added from the palette starts with:
// the page's demo values, written into the spec explicitly wherever they
// differ from the node's own defaults. `commands` lists the node's control
// commands (see `inspector.js` and the Send command dialog): `cmd`, a label,
// `fixed` values and `params` (number, text or select inputs) that join the
// command object, and `enabled(state)`; `bar: false` keeps a command out of
// the inspector's command bar, for the dialog's `example` only.

import { plural } from "./dom.js";
import { renderView } from "./views.js";
import { MISSING_BUILD, REAL_BROKER_KIND } from "./external.js";
import { TEMPLATE_HELP } from "./forms.js";

export const KAFKA_PORT = 9092;
export const HTTP_PORT = 8081;

const BOOTSTRAP = {
  key: "bootstrap",
  label: "Bootstrap brokers",
  type: "noderefs",
  of: [REAL_BROKER_KIND],
  required: true,
  help: "The brokers the client connects to first. Metadata leads it to the rest.",
};

const REGISTRY = { key: "registry", label: "Registry", type: "noderef", of: ["schema-registry"], required: true };

// A number field that holds the node's default unless changed, and is left
// out of the config while it does.
const num = (key, label, def, help, extra = {}) => ({ key, label, type: "number", default: def, emitDefault: false, min: 0, step: 1, help, ...extra });

// The value a producer writes when its config names none.
const DEFAULT_VALUE = { format: "json", template: { id: "{seq}", total: "{rand 1 500}" } };

const ORDER_SCHEMA = { type: "record", name: "Order", fields: [{ name: "id", type: "long" }, { name: "total", type: "double" }] };

export const KINDS = {
  "local-client": {
    kind: "local-client",
    label: "Local kafkactl",
    glyph: "⌁",
    color: "#8ed9e8",
    pinned: true,
    description: "The kafkactl bridge on this computer. Its traffic crosses the lab network like another client node.",
    probe: {},
    fields: [],
    commands: [],
    edges: () => [],
    status: (s) => s?.connected ? "connected" : "waiting for kafkactl",
  },
  [REAL_BROKER_KIND]: {
    kind: REAL_BROKER_KIND,
    label: "Krabka broker",
    glyph: "▣",
    color: "#f7b73a",
    real: true,
    pinned: true,
    description: "The real krabka-broker, compiled to wasm32-wasip1, in a Web Worker on the browser WASI runtime. Its disk is a volume in this browser, so it runs in this tab.",
    listens: KAFKA_PORT,
    probe: {},
    // Every key but `voter` lands in KRABKA_CONFIG, the JSON form of the
    // broker's `broker.toml` (see CONFIG_KEYS in external.js); empty keeps the
    // broker's default.
    fields: [
      { key: "voter", label: "KRaft voter", type: "boolean", default: true, emitDefault: false, help: "Listed in KRABKA_VOTERS: a controller and a broker. Unchecked, it runs the broker role only." },
      { key: "rack", label: "Rack", type: "text", placeholder: "a", help: "The broker's rack (KIP-392). Sent as rack." },
      { key: "num_partitions", label: "Default partitions", type: "number", min: 1, max: 2147483647, step: 1, placeholder: "broker default", help: "Kafka's num.partitions. Blank keeps the broker's default. Sent as runtime.num_partitions." },
      { key: "default_replication_factor", label: "Default replication factor", type: "number", min: 1, max: 32767, step: 1, placeholder: "broker default", help: "Kafka's default.replication.factor. Blank keeps the broker's default. Sent as runtime.default_replication_factor." },
      { key: "min_insync_replicas", label: "min.insync.replicas", type: "number", min: 1, max: 2147483647, step: 1, placeholder: "broker default", help: "The broker's default min.insync.replicas. Blank keeps the broker's default. Sent as runtime.default_min_insync_replicas." },
      { key: "replica_lag_time_max_ms", label: "Replica lag time max (ms)", type: "number", min: 1, max: 2147483647, step: 1, placeholder: "broker default", help: "Kafka's replica.lag.time.max.ms. Blank keeps the broker's default. Sent as replica_lag_time_max." },
    ],
    commands: [],
    noCommands: "A real broker runs in a process; the lab sends it no control commands.",
    edges: () => [],
    status: (s) => realBrokerStatus(s),
  },
  "schema-registry": {
    kind: "schema-registry",
    label: "Schema registry",
    glyph: "◈",
    color: "#5aa0e0",
    description: "A Confluent-compatible schema registry: the REST API over the _schemas topic it keeps on the brokers. Instances of one group elect a primary; the others forward writes to it.",
    listens: HTTP_PORT,
    probe: { bootstrap: [1] },
    fields: [
      { ...BOOTSTRAP, help: "The brokers that hold the _schemas topic (kafkastore.bootstrap.servers)." },
      { key: "compatibility", label: "Default compatibility", type: "select", default: "BACKWARD", emitDefault: false, options: ["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"] },
      { key: "mode", label: "Mode", type: "select", default: "READWRITE", emitDefault: false, options: ["READWRITE", "READONLY", "IMPORT"] },
      {
        key: "leader.eligibility",
        label: "May lead",
        type: "boolean",
        default: true,
        emitDefault: false,
        help: "leader.eligibility. The instances of one group elect a primary, the eligible one with the smallest URL; a secondary forwards writes to it.",
      },
      {
        type: "advanced",
        label: "Advanced: the _schemas store",
        fields: [
          { key: "kafkastore.topic", label: "Store topic", type: "text", default: "_schemas", emitDefault: false, help: "kafkastore.topic: one partition, compacted, created on first start." },
          num("kafkastore.timeout.ms", "Store timeout (ms)", 500, "kafkastore.timeout.ms: how long a write waits for its acknowledgement, and then for the reader to read it back."),
          num("kafkastore.init.timeout.ms", "Store init timeout (ms)", 60000, "kafkastore.init.timeout.ms: how long each startup step may take."),
          num("kafkastore.topic.replication.factor", "Store replication factor", 3, "kafkastore.topic.replication.factor, lowered to the live brokers with Confluent's warning.", { min: 1 }),
        ],
      },
      {
        type: "advanced",
        label: "Advanced: the primary election",
        fields: [
          { key: "schema.registry.group.id", label: "Group", type: "text", default: "schema-registry", emitDefault: false, help: "schema.registry.group.id: the classic group whose members elect the primary." },
          num("kafkagroup.session.timeout.ms", "Session timeout (ms)", 10000, "kafkagroup.session.timeout.ms: how long the group keeps a member that stopped heartbeating."),
          num("kafkagroup.heartbeat.interval.ms", "Heartbeat interval (ms)", 3000, "kafkagroup.heartbeat.interval.ms"),
          num("kafkagroup.rebalance.timeout.ms", "Rebalance timeout (ms)", 300000, "kafkagroup.rebalance.timeout.ms"),
          num("leader.read.timeout.ms", "Forward timeout (ms)", 60000, "leader.read.timeout.ms: how long a secondary waits for the primary to answer a forwarded write (then 50003)."),
        ],
      },
    ],
    commands: [
      { cmd: "http", label: "GET", title: "Read a REST resource", fixed: { method: "GET" }, params: [{ key: "path", label: "path", type: "text", default: "/subjects", placeholder: "/subjects" }] },
      { cmd: "http", label: "POST a schema", bar: false, example: { cmd: "http", method: "POST", path: "/subjects/orders-value/versions", body: { schema: JSON.stringify(ORDER_SCHEMA) } } },
      { cmd: "http", label: "Set compatibility", bar: false, example: { cmd: "http", method: "PUT", path: "/config", body: { compatibility: "FULL" } } },
    ],
    edges: (spec) => (spec.config?.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap")),
    status: (s) => {
      const parts = [];
      if (s.state && s.state !== "ready") parts.push(String(s.state));
      else if (s.election?.is_leader) parts.push("primary");
      else if (s.election?.leader) parts.push("secondary");
      const subjects = countOf(s.subjects);
      if (subjects != null) parts.push(`${subjects} subject${subjects === 1 ? "" : "s"}`);
      if (typeof s.schemas === "number") parts.push(`${s.schemas} schema${s.schemas === 1 ? "" : "s"}`);
      if (s.writes?.queued) parts.push(`${s.writes.queued} queued`);
      return parts.join(" · ");
    },
  },
  producer: {
    kind: "producer",
    label: "Producer",
    glyph: "▶",
    color: "#45c178",
    description: "A Kafka producer that writes templated records at a rate, optionally serialized through the schema registry.",
    probe: { bootstrap: [1], topic: "t" },
    fields: [
      BOOTSTRAP,
      { key: "topic", label: "Topic", type: "text", required: true, placeholder: "orders" },
      num("rate_per_sec", "Records per second", 5, "Exact over time, fractions allowed; 0 sends only on the Send command.", { step: "any" }),
      { key: "acks", label: "acks", type: "select", default: -1, emitDefault: false, options: [{ value: -1, label: "all (-1)" }, { value: 1, label: "leader (1)" }, { value: 0, label: "none (0)" }] },
      {
        key: "key",
        label: "Key",
        type: "group",
        optional: true,
        help: "Unchecked, every key is null and the sticky partitioner spreads the records.",
        fields: [{ key: "pattern", label: "Pattern", type: "text", required: true, placeholder: "customer-{seq % 10}", help: `A text template; murmur2 on the key picks the partition. ${TEMPLATE_HELP}` }],
      },
      { key: "value", label: "Value", type: "record-value", default: DEFAULT_VALUE, emitDefault: false, help: `A JSON string that is exactly one numeric placeholder renders as a number. ${TEMPLATE_HELP}` },
      {
        key: "serialization",
        label: "Schema registry serialization",
        type: "group",
        optional: true,
        help: "Registers the schema before the first record and frames every value as 0x00, the schema id, then the Avro datum or the JSON text.",
        fields: [
          REGISTRY,
          { key: "format", label: "Format", type: "select", default: "avro", options: ["avro", "json"] },
          { key: "schema", label: "Schema", type: "json", stringify: true, required: true, default: ORDER_SCHEMA, help: "An Avro schema or a JSON Schema." },
          { key: "subject", label: "Subject", type: "text", placeholder: "<topic>-value", help: "Empty: <topic>-value." },
        ],
      },
      {
        type: "advanced",
        label: "Advanced: batching, idempotence, compression, headers",
        fields: [
          num("linger_ms", "linger.ms", 5, "How long a batch waits for more records."),
          num("batch_size", "batch.size (bytes)", 16384, "The most bytes a batch holds.", { min: 1 }),
          { key: "enable_idempotence", label: "enable.idempotence", type: "boolean", default: true, emitDefault: false, help: "A producer id and sequence numbers, so a retry never duplicates a record." },
          { key: "compression", label: "compression.type", type: "select", default: "none", emitDefault: false, options: ["none", "gzip", "snappy", "lz4", "zstd"] },
          {
            key: "headers",
            label: "Headers",
            type: "json",
            placeholder: '{"source": "lab-{seq}"}',
            validate: (v) => (v && typeof v === "object" && !Array.isArray(v) && Object.values(v).every((t) => typeof t === "string") ? null : "an object of header name → text template"),
            help: "Header name → text template.",
          },
        ],
      },
      {
        type: "advanced",
        label: "Advanced: transactions",
        fields: [
          { key: "transactional_id", label: "transactional.id", type: "text", placeholder: "orders-tx", help: "Set, the records go out in transactions, each record marked with the header lab-txn = \"<n>:commit\" or \"<n>:abort\". Needs acks all and enable.idempotence." },
          num("transaction_records", "Records per transaction", 10, "The node commits (or aborts) a transaction once it holds this many records, all acknowledged.", { min: 1 }),
          num("abort_every", "Abort every Nth transaction", 0, "0 never aborts; 3 aborts transactions 3, 6, 9 and so on."),
        ],
      },
    ],
    suggest: () => ({ key: { pattern: "customer-{seq % 10}" } }),
    commands: [
      { cmd: "send", label: "Send", title: "Generate records now, paused or not", params: [{ key: "count", label: "records", type: "number", default: 10, min: 1, step: 1 }] },
      { cmd: "rate", label: "Set rate", title: "Records per second; 0 sends only on Send", params: [{ key: "rate_per_sec", label: "per second", type: "number", default: 5, min: 0, step: "any", fromState: (s) => s.rate }] },
      { cmd: "pause", label: "Pause", title: "Stop generating records", enabled: (s) => !s.paused },
      { cmd: "resume", label: "Resume", title: "Generate at the rate again", enabled: (s) => Boolean(s.paused) },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      if (c.topic) out.push(edge(spec.id, topic(c.topic), "produce"));
      if (c.serialization?.registry != null) out.push(edge(spec.id, node(c.serialization.registry), "registry"));
      return out;
    },
    status: (s) => {
      const parts = [];
      if (s.serialization && s.serialization.state !== "ready") parts.push(String(s.serialization.state || "registering"));
      if (s.paused) parts.push("paused");
      parts.push(`${s.acked ?? 0} acked`);
      if (s.transactions) parts.push(`${s.transactions.committed ?? 0} txn, ${s.transactions.aborted ?? 0} aborted`);
      if (s.failed) parts.push(`${s.failed} failed`);
      else if (s.rate != null && !s.paused) parts.push(`${s.rate}/s`);
      return parts.join(" · ");
    },
  },
  consumer: {
    kind: "consumer",
    label: "Consumer",
    glyph: "◀",
    color: "#c084fc",
    description: "A consumer group member, classic or KIP-848, that processes what it polls one record at a time and commits as it goes.",
    probe: { bootstrap: [1], group: "g", topics: ["t"] },
    fields: [
      BOOTSTRAP,
      { key: "group", label: "Group", type: "text", required: true, placeholder: "billing" },
      { key: "topics", label: "Topics", type: "list", required: true, placeholder: "orders, payments", help: "Comma-separated." },
      {
        key: "protocol",
        label: "Protocol",
        type: "select",
        default: "classic",
        emitDefault: false,
        options: [
          { value: "classic", label: "classic (JoinGroup/SyncGroup)" },
          { value: "consumer", label: "consumer (KIP-848)" },
        ],
        help: "group.protocol: classic is Kafka's default.",
      },
      { key: "auto_offset_reset", label: "auto.offset.reset", type: "select", default: "latest", emitDefault: false, options: ["latest", "earliest"], help: "Where a partition with no committed offset starts." },
      {
        key: "isolation_level",
        label: "isolation.level",
        type: "select",
        default: "read_uncommitted",
        emitDefault: false,
        options: ["read_uncommitted", "read_committed"],
        help: "read_committed reads up to the last stable offset and drops the records of aborted transactions.",
      },
      num("process_ms", "Processing time per record (ms)", 0, "The logical time one record takes; a slow consumer shows its lag."),
      {
        key: "deserialize",
        label: "Decode through the schema registry",
        type: "group",
        optional: true,
        help: "Decodes values in the Confluent wire format, fetching each schema by id (GET /schemas/ids/{id}).",
        fields: [REGISTRY],
      },
      {
        type: "advanced",
        label: "Advanced: polling, commits, classic sessions",
        fields: [
          num("max_poll_records", "max.poll.records", 500, "The most records one poll returns.", { min: 1 }),
          { key: "enable_auto_commit", label: "enable.auto.commit", type: "boolean", default: true, emitDefault: false, help: "Commit what was processed in the poll after each interval." },
          num("auto_commit_interval_ms", "auto.commit.interval.ms", 5000, null),
          num("session_timeout_ms", "session.timeout.ms", 45000, "Classic protocol only; a KIP-848 member takes it from the broker.", { min: 1 }),
          num("heartbeat_interval_ms", "heartbeat.interval.ms", 3000, "Classic protocol only; a KIP-848 member takes it from the broker.", { min: 1 }),
          {
            key: "instance_id",
            label: "group.instance.id",
            type: "text",
            placeholder: "billing-1",
            validate: (v) => (/^[A-Za-z0-9._-]{1,249}$/.test(v) ? null : "1 to 249 ASCII letters, digits, '.', '_' or '-'"),
            help: "A static member (KIP-345) gets its partitions back after a restart within the session timeout, without a rebalance. A KIP-848 static member must Close before it restarts; a crashed one keeps its instance id until its session expires.",
          },
        ],
      },
    ],
    suggest: () => ({ protocol: "consumer", auto_offset_reset: "earliest", process_ms: 2 }),
    commands: [
      { cmd: "pause", label: "Pause", title: "Stop taking records; heartbeats go on", enabled: (s) => !s.paused },
      { cmd: "resume", label: "Resume", title: "Take records again", enabled: (s) => Boolean(s.paused) },
      { cmd: "process_ms", label: "Set processing", title: "The logical time one record takes", params: [{ key: "ms", label: "ms per record", type: "number", default: 2, min: 0, step: 1, fromState: (s) => s.process_ms }] },
      { cmd: "commit", label: "Commit now", title: "Commit the positions of what was processed" },
      {
        cmd: "seek",
        label: "Seek",
        title: "The next poll reads the partition from this offset (Kafka's seek)",
        params: [
          { key: "topic", label: "topic", type: "select", options: (s) => [...new Set((s.assignment || []).map((a) => a.topic))] },
          { key: "partition", label: "partition", type: "number", default: 0, min: 0, step: 1 },
          { key: "offset", label: "offset", type: "number", default: 0, min: 0, step: 1 },
        ],
      },
      { cmd: "close", label: "Close", title: "Commit, then leave the group as a JVM application's shutdown does; the node takes no records until it starts again", enabled: (s) => !s.closed },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      for (const t of c.topics || []) out.push(edge(topic(t), spec.id, "consume"));
      if (c.deserialize?.registry != null) out.push(edge(spec.id, node(c.deserialize.registry), "registry"));
      return out;
    },
    status: (s) => {
      const parts = [];
      if (s.closed) parts.push("closed");
      else if (s.paused) parts.push("paused");
      else if (s.state && s.state !== "stable") parts.push(String(s.state).toLowerCase());
      if (s.processed != null) parts.push(`${s.processed} read`);
      if (s.aborted_seen) parts.push(`${s.aborted_seen} aborted seen`);
      const lag = totalLag(s);
      if (lag != null) parts.push(`lag ${lag}`);
      const n = countOf(s.assignment ?? s.assigned);
      if (n != null) parts.push(`${n}p`);
      return parts.join(" · ");
    },
  },
  streams: {
    kind: "streams",
    label: "Streams app",
    glyph: "⋈",
    color: "#ff8466",
    description: "A krabka-client-streams application: a topology from a source topic into a sink, run as tasks of a KIP-1071 streams group.",
    probe: { bootstrap: [1], application_id: "a", topology: { source: "t", ops: [], sink: "u" } },
    fields: [
      BOOTSTRAP,
      { key: "application_id", label: "Application id", type: "text", required: true, placeholder: "order-stats", help: "The streams group, and the prefix of its internal topics." },
      {
        key: "topology",
        label: "Topology",
        type: "group",
        fields: [
          { key: "source", label: "Source topic", type: "text", required: true, placeholder: "orders" },
          { key: "ops", label: "Operations", type: "ops", default: [] },
          { key: "sink", label: "Sink topic", type: "text", required: true, placeholder: "order-counts" },
        ],
      },
      num("commit_interval_ms", "commit.interval.ms", 100, "How often the app flushes, waits for its acks and commits.", { min: 1 }),
      {
        key: "processing_guarantee",
        label: "processing.guarantee",
        type: "select",
        default: "at_least_once",
        emitDefault: false,
        options: ["at_least_once", "exactly_once_v2"],
        help: "exactly_once_v2 writes the outputs, the changelogs and the consumed offsets in one transaction per commit, and reads read_committed.",
      },
      {
        key: "deserialize",
        label: "Decode the source through the schema registry",
        type: "group",
        optional: true,
        fields: [REGISTRY],
      },
      {
        key: "serialize",
        label: "Serialize the sink through the schema registry",
        type: "group",
        optional: true,
        help: "Repartition and changelog records stay JSON.",
        fields: [
          REGISTRY,
          { key: "format", label: "Format", type: "select", default: "avro", options: ["avro", "json"] },
          { key: "schema", label: "Schema", type: "json", stringify: true, required: true, help: "An Avro schema or a JSON Schema for the sink values." },
          { key: "subject", label: "Subject", type: "text", placeholder: "<sink>-value", help: "Empty: <sink>-value." },
        ],
      },
      {
        type: "advanced",
        label: "Advanced",
        fields: [
          num("num_standby_replicas", "num.standby.replicas", 0, "Accepted and not used: with KIP-1071 the broker's group.streams.num.standby.replicas decides; a value other than 0 is reported as a warning."),
        ],
      },
    ],
    commands: [
      { cmd: "pause", label: "Pause", title: "Stop fetching and processing (KIP-834); heartbeats go on", enabled: (s) => !s.paused },
      { cmd: "resume", label: "Resume", title: "Fetch and process again", enabled: (s) => Boolean(s.paused) },
      {
        cmd: "query",
        label: "Query",
        title: "Read a key from a local state store (interactive query)",
        example: { cmd: "query", store: "counts", key: "kafka" },
        params: [
          { key: "store", label: "store", type: "select", options: (s) => (s.topology?.stores || []).map((x) => x.name) },
          { key: "key", label: "key", type: "text", placeholder: "key" },
        ],
      },
    ],
    edges: (spec) => {
      const c = spec.config || {};
      const out = (c.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap"));
      if (c.topology?.source) out.push(edge(topic(c.topology.source), spec.id, "consume"));
      if (c.topology?.sink) out.push(edge(spec.id, topic(c.topology.sink), "produce"));
      for (const t of streamsInternalTopics(c)) out.push(edge(spec.id, topic(t.name), t.role));
      for (const r of [c.deserialize?.registry, c.serialize?.registry]) if (r != null) out.push(edge(spec.id, node(r), "registry"));
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
    commands: [],
    noCommands: "An echo takes no control commands.",
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
      { key: "target", label: "Target", type: "noderef", of: ["echo", REAL_BROKER_KIND, "schema-registry", "pinger"], required: true },
      { key: "period_ms", label: "Period (ms)", type: "number", default: 100, min: 1, step: 1 },
      { key: "port", label: "Port", type: "number", default: KAFKA_PORT, emitDefault: false, min: 0, max: 65535, step: 1, help: "9092 for a broker or echo, 8081 for a registry." },
    ],
    commands: [{ cmd: "ping", label: "Ping now", title: "Send one ping at once" }],
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
    description: "The scenario's hidden admin client: it creates the topics through CreateTopics once the cluster has a controller, and polls the cluster's metadata, quorum, offsets and groups.",
    hidden: true,
    fields: [],
    commands: [
      { cmd: "create_topic", label: "Create a topic", bar: false, example: { cmd: "create_topic", name: "payments", partitions: 3, replication_factor: 3 } },
      { cmd: "delete_topic", label: "Delete a topic", bar: false, example: { cmd: "delete_topic", name: "payments" } },
      // ---- operator commands (the admin's module documentation)
      {
        cmd: "alter_config",
        label: "Set config",
        title: "IncrementalAlterConfigs: set one topic config",
        fixed: { resource: "topic" },
        params: [
          { key: "name", label: "topic", type: "select", options: (s) => adminTopics(s) },
          { key: "config", label: "config", type: "text", default: "retention.ms" },
          { key: "value", label: "value", type: "text", default: "60000" },
        ],
        example: { cmd: "alter_config", resource: "topic", name: "orders", set: { "retention.ms": "60000" }, delete: ["cleanup.policy"] },
      },
      {
        cmd: "describe_config",
        label: "Describe config",
        title: "DescribeConfigs: the topic's configs land in the state's configs",
        fixed: { resource: "topic" },
        params: [{ key: "name", label: "topic", type: "select", options: (s) => adminTopics(s) }],
      },
      {
        cmd: "reassign",
        label: "Reassign",
        title: "AlterPartitionReassignments: move a partition to these brokers, the first preferred as leader",
        params: [
          { key: "topic", label: "topic", type: "select", options: (s) => adminTopics(s) },
          { key: "partition", label: "partition", type: "number", default: 0, min: 0, step: 1 },
          { key: "replicas", label: "replicas", type: "text", placeholder: "3, 1, 2" },
        ],
        example: { cmd: "reassign", topic: "orders", partition: 0, replicas: [3, 1, 2] },
      },
      {
        cmd: "cancel_reassign",
        label: "Cancel reassignment",
        title: "AlterPartitionReassignments with no replicas: stop a reassignment in progress",
        enabled: (s) => (s.reassignments || []).length > 0,
        params: [
          { key: "topic", label: "topic", type: "select", options: (s) => [...new Set((s.reassignments || []).map((r) => r.topic))] },
          { key: "partition", label: "partition", type: "number", default: 0, min: 0, step: 1 },
        ],
      },
      {
        cmd: "elect_leaders",
        label: "Elect leader",
        title: "ElectLeaders: preferred moves leadership to the first replica; unclean lets an out-of-sync replica lead a leaderless partition",
        params: [
          { key: "type", label: "type", type: "select", options: ["preferred", "unclean"] },
          { key: "topic", label: "topic", type: "select", options: (s) => adminTopics(s) },
          { key: "partition", label: "partition", type: "number", default: 0, min: 0, step: 1 },
        ],
        example: { cmd: "elect_leaders", type: "preferred", topic: "orders" },
      },
      {
        cmd: "reset_offsets",
        label: "Reset offsets",
        title: "OffsetCommit for a group without members, as kafka-consumer-groups --reset-offsets",
        params: [
          { key: "group", label: "group", type: "select", options: (s) => (s.cluster?.groups || []).map((g) => g.id) },
          { key: "topic", label: "topic", type: "select", options: (s) => adminTopics(s) },
          { key: "to", label: "to", type: "select", options: ["earliest", "latest"] },
        ],
        example: { cmd: "reset_offsets", group: "billing", topic: "orders", to: 0 },
      },
    ],
    edges: (spec) => (spec.config?.bootstrap || []).slice(0, 1).map((b) => edge(spec.id, node(b), "bootstrap")),
    status: (s) => (s && typeof s === "object" && s.state ? String(s.state) : ""),
  },
  "share-consumer": {
    kind: "share-consumer", label: "Share consumer", glyph: "⇤", color: "#e879f9",
    description: "Shares records with other members of a share group, including on the same partition. The broker tracks acquisition locks, delivery counts and Accept, Release or Reject acknowledgements.",
    probe: { bootstrap: [1], group: "g", topics: ["t"] },
    fields: [BOOTSTRAP,
      { key: "group", label: "Share group", type: "text", required: true, placeholder: "workers" },
      { key: "topics", label: "Topics", type: "list", required: true, placeholder: "jobs" },
      num("process_ms", "Processing time per record (ms)", 0, "Acknowledgements are sent after processing finishes."),
      num("max_records", "Maximum records per fetch", 10, "One batch is processed before fetching again.", { min: 1, max: 1000 }),
      { key: "acknowledgement", label: "After processing", type: "select", default: "accept", emitDefault: false, options: ["accept", "release", "reject"], help: "Accept completes the record; Release makes it available for redelivery; Reject archives it. These are broker acknowledgements, not offset commits." },
    ],
    suggest: () => ({ process_ms: 100, max_records: 1 }),
    commands: [
      { cmd: "pause", label: "Pause", title: "Stop taking work; heartbeats continue", enabled: (s) => !s.paused && !s.closed },
      { cmd: "resume", label: "Resume", title: "Take work again", enabled: (s) => s.paused && !s.closed },
      { cmd: "process_ms", label: "Set processing", params: [{ key: "ms", label: "ms per record", type: "number", default: 100, min: 0, step: 1, fromState: (s) => s.process_ms }] },
      { cmd: "acknowledgement", label: "Set acknowledgement", params: [{ key: "type", label: "after processing", type: "select", options: ["accept", "release", "reject"], fromState: (s) => s.acknowledgement }] },
      { cmd: "close", label: "Close", title: "Release unfinished work, close share sessions and leave the group", enabled: (s) => !s.closed && s.state !== "closing" },
    ],
    edges: (spec) => KINDS.consumer.edges(spec),
    status: (s) => [s.state, s.paused ? "paused" : null, `${s.accepted || 0} accepted`, s.redelivered ? `${s.redelivered} redelivered` : null].filter(Boolean).join(" · "),
  },
  rebalancer: {
    kind: "rebalancer",
    label: "Rebalancer",
    glyph: "⇄",
    color: "#f59e0b",
    description: "Reads the partition layout through Metadata and moves replicas and preferred leaders so every broker carries about the same share, with AlterPartitionReassignments and ElectLeaders.",
    probe: { bootstrap: [1] },
    fields: [
      BOOTSTRAP,
      { key: "goals", label: "Goals", type: "list", default: ["replica_count", "leader_count"], emitDefault: false, placeholder: "replica_count, leader_count", help: "replica_count evens the replicas per broker; leader_count evens the preferred leaders." },
      num("interval_ms", "Interval (ms)", 10000, "The logical time between two runs.", { min: 1 }),
      { key: "execute", label: "Execute", type: "boolean", default: true, emitDefault: false, help: "Carry the proposals out. Unchecked, the node only plans." },
    ],
    commands: [
      { cmd: "plan", label: "Plan now", title: "Read the cluster and propose moves without carrying them out" },
      { cmd: "execute", label: "Execute now", title: "Read the cluster, propose moves and carry them out" },
      { cmd: "pause", label: "Pause", title: "Stop the periodic runs", enabled: (s) => !s.paused },
      { cmd: "resume", label: "Resume", title: "Run every interval again", enabled: (s) => Boolean(s.paused) },
    ],
    edges: (spec) => (spec.config?.bootstrap || []).map((b) => edge(spec.id, node(b), "bootstrap")),
    status: (s) => {
      const parts = [];
      if (s.paused) parts.push("paused");
      else if (s.state) parts.push(String(s.state));
      if (Array.isArray(s.proposals) && s.proposals.length) parts.push(plural(s.proposals.length, "move"));
      if (s.executed) parts.push(`${s.executed} executed`);
      return parts.join(" · ");
    },
  },
};

// The topics the admin's cluster observer saw, internal ones left out.
function adminTopics(s) {
  return (s?.cluster?.topics || []).filter((t) => !t.internal).map((t) => t.name);
}

// The kinds the palette offers, in order.
export const KIND_ORDER = [REAL_BROKER_KIND, "schema-registry", "producer", "consumer", "share-consumer", "streams", "rebalancer", "echo", "pinger"];

const UNKNOWN = {
  kind: "?",
  label: "Unknown",
  glyph: "?",
  color: "#6b7280",
  description: "",
  fields: [],
  commands: [],
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

// The config a node added from the palette starts with.
export function suggestedConfig(kind, ctx) {
  const k = KINDS[kind];
  return k && k.suggest ? k.suggest(ctx) : {};
}

// The command object a command spec and its parameter values make: its
// `fixed` values and then the parameters.
export function commandObject(spec, values = {}) {
  const out = { cmd: spec.cmd, ...(spec.fixed || {}) };
  for (const p of spec.params || []) if (values[p.key] !== undefined) out[p.key] = values[p.key];
  return out;
}

// Endpoint constructors for edges: a node id, or a topic by name.
export const node = (id) => ({ node: Number(id) });
export const topic = (name) => ({ topic: String(name) });
export const edge = (from, to, type) => ({
  from: typeof from === "object" ? from : node(from),
  to: typeof to === "object" ? to : node(to),
  type,
});

// The internal topics a streams app's topology makes, named as
// `apps::topology` names them: a changelog `<app>-<store>-changelog` per
// stateful op, whose store defaults to `<op>-<index>` with dashes, and a
// repartition topic `<app>-<store>-repartition` in front of the first
// aggregation after a `select_key`. `[{ name, role }]`, role `changelog` or
// `repartition`.
export function streamsInternalTopics(config) {
  const app = config?.application_id;
  const ops = config?.topology?.ops;
  if (!app || !Array.isArray(ops)) return [];
  const out = [];
  let rekeyed = false;
  ops.forEach((op, i) => {
    const name = String(op?.op || "");
    if (["count_by_key", "sum_by_key", "window_count"].includes(name)) {
      const store = op.store || `${name.replace(/_/g, "-")}-${i}`;
      if (rekeyed) {
        out.push({ name: `${app}-${store}-repartition`, role: "repartition" });
        rekeyed = false;
      }
      out.push({ name: `${app}-${store}-changelog`, role: "changelog" });
    }
    if (name === "select_key") rekeyed = true;
  });
  return out;
}

// Every edge the canvas draws for a scenario: a client to each bootstrap
// broker, producers to their topic, topics to their consumers, streams from
// their source, to their sink and to their internal topics, and clients to
// the registry they serialize through.
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

// The internal topics of the scenario's streams apps: name → role.
export function internalTopics(scenario) {
  const out = new Map();
  for (const spec of scenario.nodes || []) {
    if (spec.kind !== "streams") continue;
    for (const t of streamsInternalTopics(spec.config)) out.set(t.name, t.role);
  }
  return out;
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

// ---- the broker's card line ----------------------------------------------------------

// The card's line for a real broker: what its process is doing.
function realBrokerStatus(s) {
  const p = s.process;
  if (!p) return "";
  switch (p.state) {
    case "running": {
      const c = s.connections || {};
      return `real · ${plural((c.inbound ?? 0) + (c.outbound ?? 0), "conn")}${p.lagging ? " · lagging" : ""}${p.paused ? " · paused" : ""}${p.disk && p.disk !== "ok" ? ` · disk ${p.disk}` : ""}`; // J3: paused, disk
    }
    case "unavailable":
      return p.reason === MISSING_BUILD ? "no build on this site" : "unavailable";
    case "exited":
      return p.exit && p.exit.code != null ? `exited ${p.exit.code}` : "exited";
    default:
      return String(p.state);
  }
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
