// The home page's benchmark summary, computed from the published OpenMessaging
// run so its sentences cannot outlive the numbers behind them.
//
// It summarizes every max-rate run (OMB's producerRate of 0, or a cap no broker
// reaches), on every replication factor, rather than picking one workload:
//   - CPU: messages published per broker CPU-second, krabka against Apache Kafka.
//     CPU is not reserved up front, so unlike memory it compares fairly.
//   - Throughput: publish rate as a share of Kafka's, as a median per record
//     size and broker count, so a reader can find a workload like their own.
// Each claim in the copy is checked against the data and falls back to neutral
// wording when the data stops supporting it.

export const MAX_RATE_FLOOR = 1_000_000;

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};
const percent = (ratio) => `${Math.round(ratio * 100)}%`;
const words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];
const count = (n) => words[n] ?? String(n);

export function isMaxRate(config) {
  return config.producerRate === 0 || config.producerRate >= MAX_RATE_FLOOR;
}

export function homeBenchmark(openmessaging) {
  if (!openmessaging) return null;
  const runs = [];
  for (const group of openmessaging.rows) {
    for (const workload of group.cases) {
      if (!isMaxRate(workload.config)) continue;
      const krabka = workload.vendors.find((v) => v.id === 'krabka');
      const kafka = workload.vendors.find((v) => v.id === 'kafka');
      if (!krabka || !kafka || !krabka.cpu_seconds || !kafka.cpu_seconds) continue;
      runs.push({
        rf: group.rf,
        size: workload.config.messageSize,
        throughput: krabka.publish_rate / kafka.publish_rate,
        efficiency: (krabka.publish_rate / krabka.cpu_seconds) / (kafka.publish_rate / kafka.cpu_seconds),
      });
    }
  }
  if (runs.length === 0) return null;

  // CPU: the headline claim, only while krabka wins on most runs.
  const wins = runs.filter((r) => r.efficiency > 1).length;
  const cpuLeads = wins > runs.length / 2;
  const cpuMedian = median(runs.map((r) => r.efficiency));
  const heading = cpuLeads ? 'Less CPU per message than Apache Kafka' : 'How krabka compares with Apache Kafka';
  // The CPU sentence is returned in parts so the page can bold the run count.
  // "CPU‑second" uses a non-breaking hyphen so the unit never splits across lines.
  const cpu = cpuLeads
    ? {
        figure: `+${Math.round((cpuMedian - 1) * 100)}%`,
        figureLabel: `messages per CPU‑second vs. Kafka, median of ${runs.length} runs`,
        lead: 'In ',
        strong: `${wins} of ${runs.length}`,
        rest: ` max-rate OpenMessaging runs, krabka published more messages per CPU‑second than Apache Kafka, a median of ${Math.round((cpuMedian - 1) * 100)}% more.`,
      }
    : {
        figure: null,
        lead: `Across ${runs.length} max-rate OpenMessaging runs, krabka published a median ${percent(cpuMedian)} of Apache Kafka's messages per CPU‑second.`,
        strong: '',
        rest: '',
      };

  // Throughput: a median per record size (rows) and broker count (columns).
  const sizes = [...new Set(runs.map((r) => r.size))].sort((a, b) => a - b);
  const rfs = [...new Set(runs.map((r) => r.rf))].sort((a, b) => a - b);
  const cell = (size, rf) => {
    const values = runs.filter((r) => r.size === size && r.rf === rf).map((r) => r.throughput);
    return values.length ? median(values) : null;
  };
  const grid = {
    columns: rfs.map((rf) => (rf === 1 ? '1 broker' : `${rf} brokers, RF${rf}`)),
    rows: sizes.map((size) => ({
      label: size >= 1024 ? `${size / 1024} KiB records` : `${size} B records`,
      cells: rfs.map((rf) => {
        const value = cell(size, rf);
        return value === null ? null : { value: percent(value) };
      }),
    })),
  };

  // The prose about throughput is only used while it describes the grid: small
  // records within 5% of Kafka (or ahead), and the weakest group being the
  // largest records on the most brokers.
  const smallest = sizes[0];
  const largest = sizes.at(-1);
  const mostBrokers = rfs.at(-1);
  const cells = sizes.flatMap((size) => rfs.map((rf) => ({ size, rf, value: cell(size, rf) }))).filter((c) => c.value !== null);
  const weakest = cells.reduce((low, c) => (c.value < low.value ? c : low));
  const smallKeepsPace = cells.filter((c) => c.size === smallest).every((c) => c.value >= 0.95);
  const throughputSentence = smallKeepsPace && weakest.size === largest && weakest.rf === mostBrokers && sizes.length > 1
    ? `Its throughput keeps pace with Kafka on small records and trails most with ${grid.rows.at(-1).label.replace(' records', '')} records on ${count(mostBrokers)} replicated brokers.`
    : 'Its throughput against Kafka, by record size and broker count, is below.';

  return {
    heading,
    cpu,
    throughput: throughputSentence,
    grid,
    rounds: openmessaging.provenance.repetitions === 1 ? 'One measured round per run' : `The median of ${openmessaging.provenance.repetitions} measured rounds per run`,
  };
}
