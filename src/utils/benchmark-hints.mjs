import { isMaxRate } from './home-benchmark.mjs';

export function benchmarkHint(workload, rf) {
  const config = workload.config;
  const size =
    config.messageSize >= 1024 ? `${config.messageSize / 1024} KiB` : `${config.messageSize} bytes`;
  const streams =
    config.partitionsPerTopic === 1
      ? 'one message stream'
      : `${config.partitionsPerTopic} separate message streams`;
  let what, why;
  if (config.consumerBacklogSizeGB > 0) {
    what = `Messages pile up until there are ${config.consumerBacklogSizeGB} GB waiting. Then readers start to catch up across ${streams}.`;
    why =
      'It shows what happens when readers fall behind and must read old messages. The delay includes time spent waiting in the pile.';
  } else if (isMaxRate(config)) {
    what =
      config.producerRate === 0
        ? `The test finds a fast send rate, then sends messages across ${streams}.`
        : `The test offers up to ${config.producerRate.toLocaleString('en-US')} messages each second to ${streams}.`;
    if (workload.id.endsWith('-3producers'))
      what = `Three senders work at once across ${streams}. The test finds a fast send rate.`;
    why =
      config.partitionsPerTopic === 1
        ? 'It checks how fast one busy stream can go. Extra streams cannot hide a slow path.'
        : 'It checks how much work the broker can do when many streams are busy at once.';
  } else {
    what = `The test sends ${config.producerRate.toLocaleString('en-US')} messages each second across ${streams}. Readers receive them as they arrive.`;
    why =
      'It checks whether messages arrive quickly under a steady load. Equal send rates here do not prove equal top speed.';
  }
  const payload = `Each message is ${size}. ${config.messageSize < 1024 ? 'Small messages help expose the work needed for each message.' : 'Larger messages put more bytes through the network and storage.'}`;
  const copies =
    rf === 1
      ? 'RF1 keeps one copy of each message.'
      : `RF${rf} keeps ${rf} copies, so brokers also send messages to each other.`;
  return `What happens\n${what}\n\nWhy this test is useful\n${why}\n\n${payload} ${copies}`;
}
