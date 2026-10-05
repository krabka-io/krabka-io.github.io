// The preset scenarios of the palette. Every document follows the scenario
// format in `playground/docs/lab-design.md`; positions are canvas pixels.
//
// Each preset runs the real krabka-broker process. The scenario's admin node
// creates topics through the active controller. The page's demo values,
// where they differ from the nodes' own defaults (consumers that read from
// the earliest offset and take a little time per record, keyed
// records), are written into each config.

const ORDER_SCHEMA = {
  type: "record",
  name: "Order",
  namespace: "io.krabka.lab",
  fields: [
    { name: "id", type: "long" },
    { name: "customer", type: "string" },
    { name: "total", type: "double" },
  ],
};

const broker = (id, x, y, rack) => ({
  id,
  kind: "krabka-broker",
  name: `broker-${id}`,
  x,
  y,
  config: { rack },
});

// A KIP-848 member that reads from the earliest offset and spends `process_ms`
// on each record.
const consumer = (id, name, x, y, bootstrap, group, topics, extra = {}) => ({
  id,
  kind: "consumer",
  name,
  x,
  y,
  config: { bootstrap, group, topics, protocol: "consumer", auto_offset_reset: "earliest", process_ms: 2, ...extra },
});

const ORDERS = { format: "json", template: { id: "{seq}", total: "{rand 1 500}" } };

export const PRESETS = [
  {
    id: "single-broker",
    name: "Single broker quickstart",
    description: "One real broker, one topic, a producer and a consumer. Start here to watch a record move from write to read.",
    scenario: {
      version: 1,
      seed: 7,
      name: "Single broker quickstart",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 400, 80, "a"),
        { id: 2, kind: "producer", name: "orders-producer", x: 160, y: 310, config: { bootstrap: [1], topic: "orders", rate_per_sec: 2, value: ORDERS } },
        consumer(3, "billing", 640, 310, [1], "billing", ["orders"]),
      ],
      topics: [{ name: "orders", partitions: 1, replication_factor: 1 }],
    },
  },
  {
    id: "three-brokers",
    name: "Three brokers, a producer and a consumer group",
    description:
      "Three real brokers form the KRaft quorum and serve a three-partition topic with three replicas. A producer writes five records a second, and two consumers share the partitions. Kill a partition leader and watch leadership move.",
    scenario: {
      version: 1,
      seed: 42,
      name: "Three brokers, a producer and a consumer group",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"),
        broker(2, 400, 80, "b"),
        broker(3, 680, 80, "c"),
        {
          id: 4,
          kind: "producer",
          name: "orders-producer",
          x: 120,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            topic: "orders",
            rate_per_sec: 5,
            key: { pattern: "customer-{seq % 10}" },
            value: ORDERS,
          },
        },
        consumer(5, "billing-1", 560, 340, [1, 2, 3], "billing", ["orders"]),
        consumer(6, "billing-2", 800, 340, [1, 2, 3], "billing", ["orders"]),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "schema-registry",
    name: "Schema registry with an Avro producer and a decoding consumer",
    description:
      "Three real brokers and a schema registry that keeps its schemas in the _schemas topic. The producer registers an Avro schema before its first record; the consumer fetches and decodes it.",
    scenario: {
      version: 1,
      seed: 11,
      name: "Schema registry with an Avro producer and a decoding consumer",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"),
        broker(2, 400, 80, "b"),
        broker(3, 680, 80, "c"),
        { id: 4, kind: "schema-registry", name: "registry", x: 400, y: 260, config: { bootstrap: [1, 2, 3] } },
        {
          id: 5,
          kind: "producer",
          name: "orders-producer",
          x: 120,
          y: 420,
          config: {
            bootstrap: [1, 2, 3],
            topic: "orders",
            rate_per_sec: 3,
            key: { pattern: "customer-{seq % 10}" },
            value: {
              format: "json",
              template: { id: "{seq}", customer: "{pick alice|bob|carol}", total: "{rand 1 500}" },
            },
            serialization: { registry: 4, format: "avro", schema: JSON.stringify(ORDER_SCHEMA) },
          },
        },
        consumer(6, "billing", 680, 420, [1, 2, 3], "billing", ["orders"], { deserialize: { registry: 4 } }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "streams-word-count",
    name: "Kafka Streams word count",
    description:
      "Three real brokers serve a word stream. A streams app counts English words in a state store and writes updates to word-counts for a consumer to read.",
    scenario: {
      version: 1,
      seed: 23,
      name: "Kafka Streams word count",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"),
        broker(2, 400, 80, "b"),
        broker(3, 680, 80, "c"),
        {
          id: 4,
          kind: "producer",
          name: "words",
          x: 60,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            topic: "words",
            rate_per_sec: 5,
            key: { pattern: "{pick kafka|krabka|stream|table|topic|broker|log|raft}" },
            value: { format: "json", template: { n: "{seq}", lang: "{pick en|en|en|de}" } },
          },
        },
        {
          id: 5,
          kind: "streams",
          name: "word-count",
          x: 400,
          y: 440,
          config: {
            bootstrap: [1, 2, 3],
            application_id: "word-count",
            topology: {
              source: "words",
              ops: [
                { op: "filter", field: "lang", eq: "en" },
                { op: "count_by_key", store: "counts" },
              ],
              sink: "word-counts",
            },
          },
        },
        consumer(6, "dashboard", 760, 340, [1, 2, 3], "dashboard", ["word-counts"], { process_ms: 1 }),
      ],
      topics: [
        { name: "words", partitions: 3, replication_factor: 3 },
        { name: "word-counts", partitions: 3, replication_factor: 3 },
      ],
    },
  },
  {
    id: "two-consumer-groups",
    name: "Two independent consumer groups",
    description: "Two groups read the same orders topic from three real brokers. Compare their assignments and offsets while the producer runs.",
    scenario: {
      version: 1, seed: 31, name: "Two independent consumer groups",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"), broker(2, 400, 80, "b"), broker(3, 680, 80, "c"),
        { id: 4, kind: "producer", name: "orders-producer", x: 120, y: 330, config: { bootstrap: [1, 2, 3], topic: "orders", rate_per_sec: 4, value: ORDERS } },
        consumer(5, "billing", 420, 330, [1, 2, 3], "billing", ["orders"], { protocol: "classic" }),
        consumer(6, "analytics", 700, 330, [1, 2, 3], "analytics", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "transactions",
    name: "Transactions and read_committed",
    description:
      "A transactional producer writes ten records per transaction and aborts every third one. Two groups read the topic: the read_committed consumer never gets an aborted record, the read_uncommitted one counts them as aborted records seen.",
    scenario: {
      version: 1, seed: 37, name: "Transactions and read_committed",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"), broker(2, 400, 80, "b"), broker(3, 680, 80, "c"),
        {
          id: 4,
          kind: "producer",
          name: "orders-tx",
          x: 120,
          y: 330,
          config: {
            bootstrap: [1, 2, 3],
            topic: "orders",
            rate_per_sec: 10,
            key: { pattern: "customer-{seq % 10}" },
            value: ORDERS,
            transactional_id: "orders-tx",
            transaction_records: 10,
            abort_every: 3,
          },
        },
        consumer(5, "committed", 420, 330, [1, 2, 3], "committed", ["orders"], { isolation_level: "read_committed" }),
        consumer(6, "uncommitted", 700, 330, [1, 2, 3], "uncommitted", ["orders"]),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "observer-broker",
    name: "Two voters and a broker observer",
    description: "Brokers 1 and 2 vote in the controller quorum. Broker 3 serves data without a controller vote. Compare their process state and topic replicas.",
    scenario: {
      version: 1, seed: 32, name: "Two voters and a broker observer",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"), broker(2, 400, 80, "b"),
        { ...broker(3, 680, 80, "c"), config: { voter: false, rack: "c" } },
        { id: 4, kind: "producer", name: "orders-producer", x: 180, y: 330, config: { bootstrap: [1, 2, 3], topic: "orders", rate_per_sec: 3, value: ORDERS } },
        consumer(5, "billing", 620, 330, [1, 2, 3], "billing", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "min-isr",
    name: "Minimum in-sync replicas",
    description: "Three real brokers require two in-sync replicas for writes. Kill replicas one at a time and inspect the producer's acknowledgements.",
    scenario: {
      version: 1, seed: 33, name: "Minimum in-sync replicas",
      links: { default_latency_ms: 5 },
      nodes: [
        ...[broker(1, 120, 80, "a"), broker(2, 400, 80, "b"), broker(3, 680, 80, "c")].map((n) => ({ ...n, config: { ...n.config, min_insync_replicas: 2 } })),
        { id: 4, kind: "producer", name: "orders-producer", x: 180, y: 330, config: { bootstrap: [1, 2, 3], topic: "orders", rate_per_sec: 3, acks: -1, value: ORDERS } },
        consumer(5, "billing", 620, 330, [1, 2, 3], "billing", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "slow-replica",
    name: "Slow broker link",
    description: "Broker 3 has slower links to the other two real brokers. Compare replication and client activity, then change the link latency.",
    scenario: {
      version: 1, seed: 34, name: "Slow broker link",
      links: { default_latency_ms: 5 },
      link_overrides: [{ a: 1, b: 3, latency_ms: 200 }, { a: 2, b: 3, latency_ms: 200 }],
      nodes: [
        // Broker 2 sits below the row: on it, the 1-3 link would run behind its card.
        broker(1, 120, 80, "a"), broker(2, 400, 170, "b"), broker(3, 680, 80, "c"),
        { id: 4, kind: "producer", name: "orders-producer", x: 180, y: 330, config: { bootstrap: [1, 2], topic: "orders", rate_per_sec: 3, value: ORDERS } },
        consumer(5, "billing", 620, 330, [1, 2], "billing", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "rack-split",
    name: "Two racks, one broker cut off",
    description: "Three real brokers span two racks, and broker 3 starts cut off, so the three-replica topic and the consumer group wait for it. Select broker 3 and press Reconnect, then inspect the quorum and replica state.",
    scenario: {
      version: 1, seed: 35, name: "Two racks, one broker cut off",
      links: { default_latency_ms: 5 },
      link_overrides: [{ a: 1, b: 3, cut: true }, { a: 2, b: 3, cut: true }],
      nodes: [
        // Broker 2 sits below the row: on it, the cut 1-3 link would run behind its card.
        broker(1, 120, 80, "a"), broker(2, 400, 170, "a"), broker(3, 680, 80, "b"),
        { id: 4, kind: "producer", name: "orders-producer", x: 180, y: 330, config: { bootstrap: [1, 2], topic: "orders", rate_per_sec: 3, value: ORDERS } },
        consumer(5, "billing", 620, 330, [1, 2], "billing", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "five-brokers-partition",
    name: "Five brokers under partition",
    description:
      "Five real voters, with brokers 4 and 5 cut off from 1, 2 and 3. The majority can elect a controller; the minority cannot. Heal the links and watch the brokers rejoin.",
    scenario: {
      version: 1,
      seed: 5,
      name: "Five brokers under partition",
      links: { default_latency_ms: 5 },
      link_overrides: [
        { a: 4, b: 1, cut: true },
        { a: 4, b: 2, cut: true },
        { a: 4, b: 3, cut: true },
        { a: 5, b: 1, cut: true },
        { a: 5, b: 2, cut: true },
        { a: 5, b: 3, cut: true },
      ],
      nodes: [
        broker(1, 520, 60, "b"),
        broker(2, 720, 160, "b"),
        broker(3, 520, 260, "b"),
        broker(4, 80, 80, "a"),
        broker(5, 80, 240, "a"),
        {
          id: 6,
          kind: "producer",
          name: "orders-producer",
          x: 300,
          y: 440,
          config: {
            bootstrap: [1, 2, 3, 4, 5],
            topic: "orders",
            rate_per_sec: 5,
            key: { pattern: "customer-{seq % 10}" },
            value: ORDERS,
          },
        },
        consumer(7, "billing", 620, 440, [1, 2, 3, 4, 5], "billing", ["orders"], { protocol: "classic" }),
      ],
      topics: [{ name: "orders", partitions: 5, replication_factor: 3 }],
    },
  },
];

// ---- J3: scripted experiments (experiment.js) ----
// Each was run on the real brokers (scripts/check-lab-experiments.mjs runs
// them in CI). Times are lab ms from the start of the run.
const healMinority = [4, 5].flatMap((a) => [1, 2, 3].map((b) => ({ at: 60000, fault: { kind: "heal", a, b } })));
const EXPERIMENTS = {
  // A second kill would take the three-voter KRaft quorum with it (produce
  // requests then time out rather than fail), so the ISR is pushed under the
  // minimum the other way: one replica down, then the topic's minimum raised.
  "min-isr": {
    name: "Raise min.insync.replicas above a shrunken ISR",
    steps: [
      { at: 15000, fault: { kind: "kill", node: 3 } },
      { at: 45000, command: { node: "admin", cmd: "alter_config", resource: "topic", name: "orders", set: { "min.insync.replicas": "3" } } },
      { at: 75000, fault: { kind: "restart", node: 3 } },
    ],
    expect: [
      { by: 45000, after: 15000, check: "isr_size", topic: "orders", partition: 0, op: "==", value: 2 },
      { by: 45000, check: "producer_acked", node: 4, since: 30000, op: ">", value: 0 },
      { by: 75000, after: 45000, check: "kafka_error", error: "NOT_ENOUGH_REPLICAS" },
      { at: 75000, check: "producer_acked", node: 4, since: 60000, op: "==", value: 0 },
      { by: 120000, after: 75000, check: "isr_size", topic: "orders", partition: 0, op: "==", value: 3 },
      { by: 120000, check: "producer_acked", node: 4, since: 75000, op: ">", value: 0 },
      { by: 120000, check: "invariants_hold" },
    ],
    end: 120000,
  },
  "five-brokers-partition": {
    name: "The majority serves through a partition; the minority rejoins on heal",
    steps: healMinority,
    expect: [
      { at: 55000, check: "snapshot_path", node: "admin", path: "cluster.brokers.length", op: "==", value: 3 },
      { by: 55000, check: "producer_acked", node: 6, since: 20000, op: ">", value: 20 },
      { by: 100000, after: 60000, check: "snapshot_path", node: "admin", path: "cluster.brokers.length", op: "==", value: 5 },
      { by: 120000, check: "producer_acked", node: 6, since: 60000, op: ">", value: 20 },
      { by: 120000, check: "invariants_hold" },
    ],
    end: 120000,
  },
  // Broker 3's links slow to 15 s each way: its fetches miss the replica lag
  // limit, it leaves the ISR and the group's lag jumps; back at 200 ms it
  // rejoins and the lag comes down.
  "slow-replica": {
    name: "Slow broker 3 until it leaves the ISR, then let it catch up",
    steps: [
      { at: 20000, fault: { kind: "latency", a: 1, b: 3, ms: 15000 } },
      { at: 20000, fault: { kind: "latency", a: 2, b: 3, ms: 15000 } },
      { at: 80000, fault: { kind: "latency", a: 1, b: 3, ms: 200 } },
      { at: 80000, fault: { kind: "latency", a: 2, b: 3, ms: 200 } },
    ],
    expect: [
      { by: 60000, after: 20000, check: "isr_size", topic: "orders", partition: 0, op: "==", value: 2 },
      { by: 75000, after: 20000, check: "snapshot_path", node: "admin", path: "cluster.groups.0.lag", op: ">=", value: 20 },
      { by: 130000, after: 95000, check: "isr_size", topic: "orders", partition: 0, op: "==", value: 3 },
      { by: 140000, after: 100000, check: "snapshot_path", node: "admin", path: "cluster.groups.0.lag", op: "<=", value: 5 },
      { by: 140000, check: "producer_acked", node: 4, since: 85000, op: ">", value: 30 },
      { by: 140000, check: "invariants_hold" },
    ],
    end: 140000,
  },
};
for (const p of PRESETS) if (EXPERIMENTS[p.id]) p.scenario.experiment = EXPERIMENTS[p.id];
// ---- /J3 ----

export function presetById(id) {
  return PRESETS.find((p) => p.id === id) || null;
}
