// The preset scenarios of the palette. Every document follows the scenario
// format in `playground/docs/lab-design.md`; positions are canvas pixels.
//
// `Network probe` runs on the diagnostic `echo` and `pinger` kinds. The
// others are KRaft clusters in combined mode: the world gives a scenario's
// brokers one static controller quorum (every broker whose `voter` is not
// false votes), the scenario's admin node creates its topics through the
// active controller, and the apps are wired to them. The page's demo values,
// where they differ from the nodes' own defaults (KIP-848 consumers that
// read from the earliest offset and take a little time per record, keyed
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
  kind: "broker",
  name: `broker-${id}`,
  x,
  y,
  config: { broker_id: id, rack },
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
    id: "network-probe",
    name: "Network probe",
    description:
      "Two echo nodes and a pinger. The pinger opens a connection and pings every 100 ms; watch the round trip on the canvas and the RTT in the inspector.",
    scenario: {
      version: 1,
      seed: 7,
      name: "Network probe",
      links: { default_latency_ms: 10 },
      nodes: [
        { id: 1, kind: "echo", name: "echo-a", x: 140, y: 120, config: {} },
        { id: 2, kind: "echo", name: "echo-b", x: 140, y: 300, config: {} },
        { id: 3, kind: "pinger", name: "pinger", x: 460, y: 210, config: { target: 1, period_ms: 100 } },
      ],
      topics: [],
    },
  },
  {
    id: "three-brokers",
    name: "Three brokers, a producer and a consumer group",
    description:
      "Three brokers form the KRaft quorum; the controller creates orders with three partitions replicated three times. A producer writes five records a second, and two KIP-848 consumers of one group share the partitions. Kill the leader of a partition and watch the leadership move while the group keeps consuming.",
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
      "Three brokers and a schema registry that keeps its schemas in the _schemas topic on them. The producer registers an Avro schema before its first record and frames every value with the schema id; the consumer fetches the schema by id and decodes each value.",
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
      "A producer writes one record per word, keyed by the word and tagged with a language. A krabka-client-streams app keeps the English ones and counts each word in its counts store, whose changelog topic the streams group creates; every new count goes to word-counts, where a consumer reads it.",
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
    id: "five-brokers-partition",
    name: "Five brokers under partition",
    description:
      "Five voters, with brokers 4 and 5 cut off from 1, 2 and 3 from the start. The majority elects the controller, registers and serves the topic; the minority cannot register and stays fenced. Heal the links and watch 4 and 5 catch up with the metadata log and join.",
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
        consumer(7, "billing", 620, 440, [1, 2, 3, 4, 5], "billing", ["orders"]),
      ],
      topics: [{ name: "orders", partitions: 5, replication_factor: 3 }],
    },
  },
];

export function presetById(id) {
  return PRESETS.find((p) => p.id === id) || null;
}
