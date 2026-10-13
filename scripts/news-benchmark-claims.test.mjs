// Guards for the benchmark claims in src/pages/news/krabka-1-0-3.md. A
// Markdown page cannot compute its wording, so these tests fail when the
// published OpenMessaging data stops supporting a sentence in it. Change the
// post, then the test, if a new run is published.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const load = run => new Map(JSON.parse(fs.readFileSync(new URL(`../benchmarks/openmessaging/${run}/trials.json`, import.meta.url), 'utf8'))
  .map(t => [`${t.rf}/${t.vendor}/${t.case.id}`, t]));
const current = load('omb-local-2026-10-13');
const previous = load('omb-local-2026-10-11');
const ids = [...new Set([...current.values()].map(t => t.case.id))];
const config = id => [...current.values()].find(t => t.case.id === id).case.config;
const backlog = id => config(id).consumerBacklogSizeGB > 0;
const maxRate = id => !backlog(id) && (id.startsWith('max-rate') || config(id).producerRate === 0);
const fixedRate = id => !backlog(id) && !maxRate(id);
const rate = (run, rf, vendor, id) => run.get(`${rf}/${vendor}/${id}`).publish_rate;
const p99 = (run, rf, vendor, id) => run.get(`${rf}/${vendor}/${id}`).end_to_end_latency_ms.p99;
const pairs = filter => [1, 3].flatMap(rf => ids.filter(filter).map(id => [rf, id]));

test('1.0.3 published faster than 1.0.1 in most maximum-rate workloads, most of all in the 1 KB ones', () => {
  const gains = pairs(maxRate).map(([rf, id]) => ({ id, gain: rate(current, rf, 'krabka', id) / rate(previous, rf, 'krabka', id) }));
  assert.ok(gains.filter(g => g.gain > 1.01).length > gains.length / 2);
  const mean = list => list.reduce((a, g) => a + g.gain, 0) / list.length;
  assert.ok(mean(gains.filter(g => g.id.endsWith('1kb'))) > mean(gains.filter(g => !g.id.endsWith('1kb'))));
});

test('1.0.3 end-to-end p99 was lower or the same as 1.0.1 in every fixed-rate workload', () => {
  for (const [rf, id] of pairs(fixedRate)) assert.ok(p99(current, rf, 'krabka', id) <= p99(previous, rf, 'krabka', id), `RF${rf} ${id}`);
});

test('at RF1 krabka led both single-partition maximum-rate workloads and matched Redpanda at fixed rates', () => {
  for (const id of ids.filter(i => maxRate(i) && i.includes('1-partition'))) {
    for (const other of ['kafka', 'redpanda']) assert.ok(rate(current, 1, 'krabka', id) > rate(current, 1, other, id), `${id} ${other}`);
  }
  for (const id of ids.filter(fixedRate)) assert.equal(p99(current, 1, 'krabka', id), p99(current, 1, 'redpanda', id), id);
});

test('Redpanda had the highest rate in most multi-partition maximum-rate workloads', () => {
  const multi = pairs(id => maxRate(id) && !id.includes('1-partition'));
  const led = multi.filter(([rf, id]) => ['krabka', 'kafka'].every(v => rate(current, rf, 'redpanda', id) > rate(current, rf, v, id)));
  assert.ok(led.length > multi.length / 2);
});

test('at RF3 krabka p99 in the two 1 KB fixed-rate workloads is far above Redpanda, above Kafka in one and below in the other', () => {
  const oneKb = ids.filter(id => fixedRate(id) && id.endsWith('1kb'));
  assert.equal(oneKb.length, 2);
  for (const id of oneKb) assert.ok(p99(current, 3, 'krabka', id) > 10 * p99(current, 3, 'redpanda', id), id);
  const aboveKafka = oneKb.filter(id => p99(current, 3, 'krabka', id) > p99(current, 3, 'kafka', id));
  assert.equal(aboveKafka.length, 1);
});
