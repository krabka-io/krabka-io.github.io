// The preset scenarios of the palette. Every document follows the scenario
// format in `playground/docs/lab-design.md`; positions are canvas pixels.
//
// `Network probe` runs on the diagnostic `echo` and `pinger` kinds. The other
// presets use the broker, registry, producer, consumer and streams kinds; the
// palette marks them as needing the full build while the crate still rejects
// those kinds, and drops the mark by itself once they land.

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
    name: "Three brokers, a producer, a consumer group",
    description:
      "A three-broker KRaft cluster, one topic with three partitions replicated three times, a producer at five records a second, and a two-member consumer group.",
    scenario: {
      version: 1,
      seed: 42,
      name: "Three brokers, a producer, a consumer group",
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
            acks: -1,
            key: { pattern: "customer-{seq % 10}" },
            value: { format: "json", template: { id: "{seq}", total: "{rand 1 500}" } },
          },
        },
        {
          id: 5,
          kind: "consumer",
          name: "billing-1",
          x: 560,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            group: "billing",
            topics: ["orders"],
            protocol: "consumer",
            auto_offset_reset: "earliest",
            process_ms: 2,
          },
        },
        {
          id: 6,
          kind: "consumer",
          name: "billing-2",
          x: 800,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            group: "billing",
            topics: ["orders"],
            protocol: "consumer",
            auto_offset_reset: "earliest",
            process_ms: 2,
          },
        },
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "schema-registry",
    name: "Schema registry with an Avro producer",
    description:
      "Three brokers, a schema registry backed by the _schemas topic, a producer that registers an Avro schema before its first record, and a consumer that fetches the schema by id.",
    scenario: {
      version: 1,
      seed: 11,
      name: "Schema registry with an Avro producer",
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
            acks: -1,
            key: { pattern: "customer-{seq % 10}" },
            value: {
              format: "json",
              template: { id: "{seq}", customer: "{pick alice|bob|carol}", total: "{rand 1 500}" },
            },
            serialization: { registry: 4, format: "avro", schema: JSON.stringify(ORDER_SCHEMA) },
          },
        },
        {
          id: 6,
          kind: "consumer",
          name: "billing",
          x: 680,
          y: 420,
          config: {
            bootstrap: [1, 2, 3],
            group: "billing",
            topics: ["orders"],
            protocol: "consumer",
            auto_offset_reset: "earliest",
            process_ms: 2,
          },
        },
      ],
      topics: [{ name: "orders", partitions: 3, replication_factor: 3 }],
    },
  },
  {
    id: "streams-orders",
    name: "Streams app counting orders",
    description:
      "A producer writes orders, a krabka-client-streams application keeps the orders above 100 and counts them per customer into order-counts, and a consumer reads the counts.",
    scenario: {
      version: 1,
      seed: 23,
      name: "Streams app counting orders",
      links: { default_latency_ms: 5 },
      nodes: [
        broker(1, 120, 80, "a"),
        broker(2, 400, 80, "b"),
        broker(3, 680, 80, "c"),
        {
          id: 4,
          kind: "producer",
          name: "orders-producer",
          x: 60,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            topic: "orders",
            rate_per_sec: 5,
            acks: -1,
            key: { pattern: "customer-{seq % 10}" },
            value: { format: "json", template: { id: "{seq}", total: "{rand 1 500}" } },
          },
        },
        {
          id: 5,
          kind: "streams",
          name: "order-stats",
          x: 400,
          y: 440,
          config: {
            bootstrap: [1, 2, 3],
            application_id: "order-stats",
            topology: {
              source: "orders",
              ops: [{ op: "filter", field: "total", gt: 100 }, { op: "count_by_key" }],
              sink: "order-counts",
            },
          },
        },
        {
          id: 6,
          kind: "consumer",
          name: "dashboard",
          x: 760,
          y: 340,
          config: {
            bootstrap: [1, 2, 3],
            group: "dashboard",
            topics: ["order-counts"],
            protocol: "consumer",
            auto_offset_reset: "earliest",
            process_ms: 1,
          },
        },
      ],
      topics: [
        { name: "orders", partitions: 3, replication_factor: 3 },
        { name: "order-counts", partitions: 3, replication_factor: 3 },
      ],
    },
  },
  {
    id: "five-brokers-partition",
    name: "Five brokers under partition",
    description:
      "Five brokers, with brokers 1 and 2 cut off from 3, 4 and 5 from the start. The majority side elects the controller and keeps the topic available; heal the links and watch the minority catch up.",
    scenario: {
      version: 1,
      seed: 5,
      name: "Five brokers under partition",
      links: { default_latency_ms: 5 },
      link_overrides: [
        { a: 1, b: 3, cut: true },
        { a: 1, b: 4, cut: true },
        { a: 1, b: 5, cut: true },
        { a: 2, b: 3, cut: true },
        { a: 2, b: 4, cut: true },
        { a: 2, b: 5, cut: true },
      ],
      nodes: [
        broker(1, 80, 80, "a"),
        broker(2, 80, 240, "a"),
        broker(3, 520, 60, "b"),
        broker(4, 720, 160, "b"),
        broker(5, 520, 260, "b"),
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
            acks: -1,
            key: { pattern: "customer-{seq % 10}" },
            value: { format: "json", template: { id: "{seq}", total: "{rand 1 500}" } },
          },
        },
        {
          id: 7,
          kind: "consumer",
          name: "billing",
          x: 620,
          y: 440,
          config: {
            bootstrap: [1, 2, 3, 4, 5],
            group: "billing",
            topics: ["orders"],
            protocol: "consumer",
            auto_offset_reset: "earliest",
            process_ms: 2,
          },
        },
      ],
      topics: [{ name: "orders", partitions: 5, replication_factor: 3 }],
    },
  },
];

export function presetById(id) {
  return PRESETS.find((p) => p.id === id) || null;
}
