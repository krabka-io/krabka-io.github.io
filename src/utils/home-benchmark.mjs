// The home page's one benchmark claim, computed from the published
// OpenMessaging run so the sentence cannot outlive the numbers behind it.
//
// Memory is shown for krabka alone: Kafka's JVM heap and Redpanda's
// preallocation are configured sizes, so a ratio against them would compare
// usage with reservations (see benchmarks/README.md).
export const HOME_WORKLOAD = 'max-rate-1-topic-16-partitions-1kb';
// "Kafka-class" is only claimed while krabka is at most this far behind Kafka.
export const KAFKA_CLASS_GAP = 0.10;

export function homeBenchmark(openmessaging) {
  if (!openmessaging) return null;
  const group = openmessaging.rows.find((row) => row.rf === 1);
  const workload = group?.cases.find((c) => c.id === HOME_WORKLOAD);
  const krabka = workload?.vendors.find((v) => v.id === 'krabka');
  const kafka = workload?.vendors.find((v) => v.id === 'kafka');
  if (!krabka || !kafka || !krabka.rss_peak_bytes) return null;

  const gap = 1 - krabka.publish_rate / kafka.publish_rate;
  // Round the gap up, so "within N%" is never smaller than the measured gap.
  const comparison = gap > 0
    ? `within ${Math.ceil(gap * 100)}% of Apache Kafka on the same workload`
    : `at or above Apache Kafka on the same workload`;
  const rounds = openmessaging.provenance.repetitions;
  const top = Math.max(krabka.publish_rate, kafka.publish_rate);
  return {
    kafkaClass: gap <= KAFKA_CLASS_GAP,
    bars: [krabka, kafka].map((v) => ({
      id: v.id,
      name: v.name,
      value: `${Math.round(v.publish_rate / 1000)}k msg/s`,
      width: (v.publish_rate / top) * 100,
    })),
    rate: `${Math.round(krabka.publish_rate / 1000)}k msg/s`,
    memory: `${Math.round(krabka.rss_peak_bytes / 1024 ** 2)} MiB`,
    comparison,
    rounds: rounds === 1 ? 'one measured round' : `median of ${rounds} measured rounds`,
  };
}
