// Shared summaries use every max-rate workload, with equal weight per workload/RF pair.
export const MAX_RATE_FLOOR = 1_000_000;

export const median = values => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

export function isMaxRate(config) {
  return config.producerRate === 0 || config.producerRate >= MAX_RATE_FLOOR;
}
