import { median, isMaxRate } from './home-benchmark.mjs';

export const releaseVersion = (run) =>
  run.provenance.images.krabka.requested.split(':').at(-1).replace(/^v/, '');

// The rate a workload sustained end to end: the lower of its publish and
// consume rates, so a consumer that falls behind does not count as faster.
export const throughput = (vendor) =>
  vendor.consume_rate > 0
    ? Math.min(vendor.publish_rate, vendor.consume_rate)
    : vendor.publish_rate;

export function comparison(run, peer = 'kafka', size, rf) {
  const pairs = run.rows
    .flatMap((group) =>
      group.cases
        .filter(
          (workload) =>
            isMaxRate(workload.config) &&
            (size === undefined || workload.config.messageSize === size) &&
            (rf === undefined || group.rf === rf),
        )
        .map((workload) => [
          workload.vendors.find((v) => v.id === 'krabka'),
          workload.vendors.find((v) => v.id === peer),
        ]),
    )
    .filter(([a, b]) => a?.publish_rate > 0 && b?.publish_rate > 0)
    .map(([a, b]) => [a, b, throughput(a) / throughput(b)]);
  if (!pairs.length) return null;
  const efficiencies = pairs
    .filter(([a, b]) => a.cpu_seconds > 0 && b.cpu_seconds > 0)
    .map(([a, b]) => throughput(a) / a.cpu_seconds / (throughput(b) / b.cpu_seconds));
  return {
    count: pairs.length,
    throughput: median(pairs.map(([, , ratio]) => ratio)),
    min: Math.min(...pairs.map(([, , ratio]) => ratio)),
    max: Math.max(...pairs.map(([, , ratio]) => ratio)),
    efficiency: efficiencies.length ? median(efficiencies) : null,
    efficientCount: efficiencies.filter((value) => value > 1).length,
  };
}

export function directComparison(experiment) {
  if (!experiment.rounds.length) return null;
  const ratios = experiment.rounds.map((round) => round.krabka_rate / round.redpanda_rate);
  if (!ratios.every((value) => Number.isFinite(value) && value > 0)) return null;
  return {
    count: ratios.length,
    gap: Math.ceil(Math.max(...ratios.map((value) => Math.abs(value - 1))) * 100),
    nearParity: ratios.every((value) => Math.abs(value - 1) <= 0.03),
  };
}

export function matrixValue(vendors, peer, metric) {
  const krabka = vendors.find((v) => v.id === 'krabka');
  const baseline = vendors.find((v) => v.id === peer);
  if (!krabka || !baseline || krabka.omitted || baseline.omitted) return null;
  const a = krabka[metric],
    b = baseline[metric];
  if (!Number.isFinite(a) || !Number.isFinite(b) || a < 0 || b <= 0) return null;
  return { ratio: a / b, krabka: a, baseline: b };
}
