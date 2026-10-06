// End-to-end check of the Cluster Lab's cluster presets on real brokers, in
// headless Chromium.
//
// Serves the built site from `dist/` (with the real `krabka-broker` module,
// `npm run build:broker`), opens the presets as the site ships them, and runs
// each at 5x in a browser context of its own (the real broker takes real
// CPU, so 20x starves it). The hidden admin node creates each scenario's
// topics on the brokers.
//
// - three-brokers: two consumers share the three partitions and both consume;
//   a section the reader opens stays open as the State tab renders again; the
//   producer's command bar pauses, sends, resumes and sets the rate; the
//   consumer's pauses, resumes (and catches up), sets the processing time,
//   commits and seeks; Close hands its partitions to the other member.
// - schema-registry: the producer registers its schema, the registry answers
//   GET /subjects and the consumer decodes every value; a schema registered
//   through REST is answered; a second registry joins, the two elect a
//   primary, and a write through the secondary is forwarded; the inspector
//   and the timeline say so; after a reload the registry replays `_schemas`
//   from the brokers' disks.
// - streams-word-count: the streams app counts words in its store, the group
//   creates the changelog topic, a query reads the store, Pause and Resume.
// - five-brokers-partition: the majority serves while brokers 4 and 5 are cut
//   off (they log no "serving" line); once the links heal both serve and a
//   restarted producer lists five brokers.
// - sspi-encrypted: clients and brokers mutually authenticate; only tokens
//   and ciphertext are captured; reload retains the mode; settings switch
//   encryption off and on using the same scenario and broker disks.
//
// Usage:  npm run build && npm run build:broker && npm run check-lab-clusters [-- --headed]
// A single flow: npm run check-lab-clusters -- --preset=sspi-encrypted
// Needs Playwright and a Chromium (see lab-check-lib.mjs). Exits 2 when one
// is missing, or when dist/ has no broker build, 1 when a check fails.

import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { DIST_DIR, ROOT, checker, command, fit, inspect, launchOrExit, newLabContext, nodeStateOf, openLab, openScenario, serve, setSpeed, until, waitFor, watchErrors } from './lab-check-lib.mjs';

const SPEED = 5;
const t = checker();
const { check } = t;

const avro = (name, fields) => JSON.stringify({ type: 'record', name, namespace: 'io.krabka.lab', fields });
const PAYMENT_V1 = avro('Payment', [{ name: 'id', type: 'long' }, { name: 'amount', type: 'double' }]);
const SHIPMENT_V1 = avro('Shipment', [{ name: 'order', type: 'long' }, { name: 'carrier', type: 'string' }]);

// A REST write through the registry's `http` command: it joins the write
// queue and is answered by the `registry` event that carries its number.
async function registryWrite(page, id, method, path, body) {
  const r = await page.evaluate(([id, method, path, body]) => window.krabkaLab.world.control(id, { cmd: 'http', method, path, body }), [id, method, path, body]);
  if (!r.ok || r.answer.queued == null) return r;
  const detail = JSON.parse(
    await waitFor(
      page,
      `(() => { const e = window.krabkaLab.timeline.events.find((x) => x.node === ${id} && x.kind === 'registry' && x.detail && x.detail.request === ${r.answer.queued}); return e ? JSON.stringify(e.detail) : null; })()`,
      `the answer to request ${r.answer.queued}`,
      60_000,
    ),
  );
  return { ok: true, answer: { status: detail.status, body: detail.result ?? { message: detail.message } } };
}

// Reload the page and wait for the isolated lab to hold its scenario again.
async function reload(page, name) {
  await page.reload({ waitUntil: 'load' });
  await waitFor(page, `self.crossOriginIsolated && document.querySelector('#krabka-lab[data-ready="true"]') !== null && window.krabkaLab.world.scenario().name === ${JSON.stringify(name)}`, 'the reloaded lab');
  await setSpeed(page, SPEED);
}

async function threeBrokers(page, preset) {
  // Brokers 1 to 3, the producer 4, the consumers 5 and 6.
  const group = await until(page, 'both consumers to consume', `(n) => {
    const c = [5, 6].map((i) => n[i].state);
    if (!c.every((s) => s && s.state === 'stable' && s.processed > 0 && s.assignment.length)) return null;
    return c.map((s) => ({ parts: s.assignment.map((a) => a.topic + '-' + a.partition), processed: s.processed }));
  }`, 120_000);
  const shared = [...group[0].parts, ...group[1].parts].sort();
  check('the two consumers of the group share the three partitions, and both consume', JSON.stringify(shared) === '["orders-0","orders-1","orders-2"]' && group.every((c) => c.processed > 0), JSON.stringify(group));

  // A section the reader opens stays open through the State tab's renders,
  // each of which builds every section afresh.
  await inspect(page, 4, 'orders-producer');
  const clientSection = `[...document.querySelectorAll('#krabka-lab .lab-inspector details.lab-sec')].find((d) => d.querySelector(':scope > summary')?.textContent === 'Client')`;
  await page.locator('#krabka-lab .lab-inspector details.lab-sec > summary', { hasText: /^Client$/ }).first().click();
  const sectionOpened = await page.evaluate(`(() => { const d = ${clientSection}; if (d) d.dataset.seen = 'yes'; return d?.open === true; })()`);
  await waitFor(page, `(() => { const d = ${clientSection}; return Boolean(d) && d.dataset.seen !== 'yes'; })()`, 'the State tab to render again');
  const sectionKept = await page.evaluate(`${clientSection}?.open === true`);
  check('a section the reader opens stays open when the State tab renders again', sectionOpened && sectionKept, `opened ${sectionOpened}, after a render ${sectionKept}`);

  // The producer's command bar.
  const paused = await command(page, 'pause');
  const atPause = await until(page, 'the producer to pause', `(n) => n[4].state.paused === true && { generated: n[4].state.generated, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${atPause.now} + 3000`, 'three seconds to pass');
  const stillPaused = (await nodeStateOf(page, 4)).generated;
  check('Pause stops the producer', paused.ok && stillPaused === atPause.generated, `${JSON.stringify(paused)}; ${atPause.generated} -> ${stillPaused}`);
  const sent = await command(page, 'send', { count: 7 });
  const afterSend = await until(page, 'Send to generate seven records', `(n) => n[4].state.generated === ${atPause.generated + 7} && n[4].state`);
  check('Send generates records at once, paused or not', sent.ok && /"generated":7/.test(sent.text) && afterSend.paused === true, sent.text);
  await command(page, 'resume');
  const rate = await command(page, 'rate', { rate_per_sec: 10 });
  const resumed = await until(page, 'the producer to resume at ten a second', `(n) => n[4].state.paused === false && n[4].state.rate === 10 && n[4].state`);
  check('Resume and Set rate take effect', rate.ok && resumed.rate === 10, rate.text);
  await command(page, 'rate', { rate_per_sec: 5 });

  // The consumer's command bar.
  await inspect(page, 5, 'billing-1');
  // Paused, the consumer takes no records; like Kafka's fetcher, its client
  // does not fetch a partition while records wait in its buffer.
  await command(page, 'pause');
  const held = await until(page, 'the consumer to pause', `(n) => n[5].state.paused === true && { processed: n[5].state.processed, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${held.now} + 3000`, 'three seconds to pass');
  const stillHeld = (await nodeStateOf(page, 5)).processed;
  check('Pause stops the consumer taking records', stillHeld === held.processed, `${held.processed} -> ${stillHeld}`);
  await command(page, 'resume');
  const caught = await until(page, 'the resumed consumer to catch up', `(n) => n[5].state.paused === false && n[5].state.lag === 0 && n[5].state.processed > ${stillHeld} && n[5].state`, 120_000);
  check('Resume lets it catch up', caught.lag === 0, `processed ${stillHeld} -> ${caught.processed}`);
  const slow = await command(page, 'process_ms', { ms: 5 });
  await until(page, 'the new processing time', `(n) => n[5].state.process_ms === 5`);
  check('Set processing changes the time per record', slow.ok, slow.text);
  await command(page, 'process_ms', { ms: 2 });
  const commits = (await nodeStateOf(page, 5)).commits;
  const commit = await command(page, 'commit');
  await until(page, 'the commit', `(n) => n[5].state.commits > ${commits}`);
  check('Commit now commits', commit.ok, commit.text);
  // Seek, with the producer held so what the consumer reads next is the
  // records it read before.
  await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'pause' }));
  await until(page, 'billing-1 to catch up', `(n) => n[4].state.paused && n[5].state.lag === 0 && n[5].state.processing_backlog === 0`, 120_000);
  const [row] = (await nodeStateOf(page, 5)).assignment.filter((a) => a.position >= 3);
  const back = row.position - 3;
  const seek = await command(page, 'seek', { topic: row.topic, partition: row.partition, offset: back });
  const reread = await until(page, 'the consumer to read again from the offset', `(n) => {
    const r = n[5].state.last_records.filter((x) => x.topic === '${row.topic}' && x.partition === ${row.partition}).map((x) => x.offset);
    return r.includes(${back}) && r.includes(${row.position - 1}) && r.slice(-3);
  }`);
  check(`Seek reads ${row.topic}-${row.partition} again from offset ${back}`, seek.ok && JSON.stringify(reread) === JSON.stringify([back, back + 1, back + 2]), `${seek.text} ${JSON.stringify(reread)}`);
  await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'resume' }));

  // Close: billing-2 commits and leaves, and billing-1 takes its partitions.
  await inspect(page, 6, 'billing-2');
  const closed = await command(page, 'close');
  const alone = await until(page, 'billing-1 to take every partition', `(n) => n[6].state.closed === true && n[6].state.assignment.length === 0 && n[5].state.assignment.length === 3 && n[5].state.assignment.map((a) => a.partition)`, 120_000);
  check('Close commits and leaves the group, and the other member takes its partitions over', closed.ok && alone.length === 3, closed.text);

  // A real broker's inspector shows its process.
  await inspect(page, 2, 'broker-2');
  const process_ = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="process_state"]')?.textContent || null`, 'the process in the broker inspector');
  check('a broker inspector shows its running process', process_ === 'running', process_);
}

async function sspiEncrypted(page, preset) {
  const consumed = `(n) => n[4].state.acked > 0 && [5, 6].every((id) => n[id].state.processed > 0)`;
  await until(page, 'both consumers to read over encrypted links', consumed, 120_000);
  const wire = await page.evaluate(() => {
    const lab = window.krabkaLab;
    const data = lab.capture.frames.filter((f) => f.kind === 'data');
    return {
      labels: [...new Set(data.map((f) => f.label))],
      protected: data.length > 0 && data.every((f) => f.bytes && f.bytes[0] === 83 && f.bytes[1] === 83 && f.bytes[2] === 80 && f.bytes[3] === 73),
      exchanges: lab.capture.exchanges.length,
      authenticated: [...new Set(lab.timeline.events.filter((e) => e.kind === 'sspi_authenticated').map((e) => e.node))],
      failures: lab.timeline.events.filter((e) => e.kind === 'sspi_failed'),
      id: lab.world.scenario().id,
    };
  });
  check('encrypted clients and all three real brokers authenticate', [1, 2, 3, 4, 5, 6].every((id) => wire.authenticated.includes(id)) && wire.failures.length === 0, JSON.stringify(wire));
  check('the capture holds Kerberos tokens and ciphertext without bogus Kafka exchanges', wire.protected && wire.labels.includes('Kerberos token') && wire.labels.includes('SSPI encrypted') && wire.exchanges === 0, JSON.stringify(wire.labels));

  await page.evaluate(() => window.krabkaLab.saveNow());
  await page.evaluate(() => window.krabkaLab.storage.flush());
  await reload(page, preset.name);
  await until(page, 'the reloaded encrypted cluster to consume', consumed, 120_000);
  const saved = await page.evaluate(() => window.krabkaLab.world.scenario());
  check('a reload preserves the encryption mode and scenario identity', saved.security === 'kerberos-encrypted' && saved.id === wire.id, `${saved.security}, ${saved.id}`);

  // Change the real settings form, restarting on the same broker disks.
  await page.evaluate(() => { window.krabkaLab.settingsDialog(); });
  await page.getByLabel('SSPI / Kerberos transport', { exact: true }).selectOption('plaintext');
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  await waitFor(page, `!window.krabkaLab.world.scenario().security && document.querySelector('#krabka-lab[data-ready="true"]') !== null`, 'the plaintext scenario');
  await setSpeed(page, SPEED);
  await until(page, 'the plaintext cluster to consume', consumed, 120_000);
  const plain = await page.evaluate(() => ({
    id: window.krabkaLab.world.scenario().id,
    exchanges: window.krabkaLab.capture.exchanges.length,
    failures: window.krabkaLab.timeline.events.filter((e) => e.kind === 'sspi_failed'),
  }));
  check('settings turn encryption off on the existing scenario', plain.id === wire.id && plain.exchanges > 0 && plain.failures.length === 0, JSON.stringify(plain));
  await page.evaluate(() => { window.krabkaLab.settingsDialog(); });
  await page.getByLabel('SSPI / Kerberos transport', { exact: true }).selectOption('kerberos-encrypted');
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  await waitFor(page, `window.krabkaLab.world.scenario().security === 'kerberos-encrypted' && document.querySelector('#krabka-lab[data-ready="true"]') !== null`, 'the encrypted scenario');
  await setSpeed(page, SPEED);
  await until(page, 'the re-enabled encrypted cluster to consume', consumed, 120_000);
  check('settings enable encryption again on the existing scenario', await page.evaluate((id) => window.krabkaLab.world.scenario().id === id && window.krabkaLab.capture.frames.some((f) => f.label === 'SSPI encrypted') && !window.krabkaLab.timeline.events.some((e) => e.kind === 'sspi_failed'), wire.id));
}

async function brokerAcls(page, preset) {
  await until(page, 'the authorized producer and consumer to exchange records', `(n) => n[4].state.acked > 0 && n[5].state.processed > 0`, 120_000);
  const evidence = await waitFor(page, `(() => {
    const lab = window.krabkaLab;
    const admin = lab.world.liveSnapshot().nodes.find((n) => n.kind === 'admin');
    const denied = lab.timeline.events.find((e) => e.node === 6 && e.detail?.code === 30);
    if (!admin?.state.authorization?.ready || !denied) return null;
    return { authorization: admin.state.authorization, denied,
      principals: lab.timeline.events.filter((e) => e.kind === 'broker_authenticated').map((e) => e.detail.principal),
      failures: lab.timeline.events.filter((e) => e.kind === 'broker_auth_failed') };
  })()`, 'replicated ACLs and the broker group denial', 120_000);
  check('every real broker confirms the exact ACL set', evidence.authorization.verified_brokers === 3, JSON.stringify(evidence.authorization));
  check('verified Kerberos identities bind to distinct broker principals', [4,5,6].every((id) => evidence.principals.includes(`node-${id}@LAB.KRABKA`)) && evidence.failures.length === 0, JSON.stringify(evidence.principals));
  const blocked = await nodeStateOf(page, 6);
  check('a topic grant does not grant another consumer group', evidence.denied.detail.code === 30 && blocked.processed === 0, JSON.stringify(blocked));
  if (preset.id === 'broker-acls-deny') {
    const denied = await waitFor(page, `window.krabkaLab.timeline.events.find((e) => e.node === 7 && e.detail?.code === 29)`, 'the broker topic denial', 120_000);
    const producer = await nodeStateOf(page,7);
    check('literal deny overrides a prefixed write allow', denied.detail.code === 29 && producer.acked === 0, JSON.stringify(producer));
  }
  await page.evaluate(() => window.krabkaLab.saveNow());
  await page.evaluate(() => window.krabkaLab.storage.flush());
  await reload(page,preset.name);
  await until(page, 'the reloaded ACL cluster to exchange authorized records', `(n) => n[4].state.acked > 0 && n[5].state.processed > 0`, 120_000);
  const saved = await page.evaluate(() => window.krabkaLab.world.scenario());
  check('reload retains the ACL policy and encrypted transport', saved.security === 'kerberos-encrypted' && JSON.stringify(saved.authorization) === JSON.stringify(preset.scenario.authorization));
  if (preset.id === 'broker-acls') {
    const policy = { acls: saved.authorization.acls.filter((r) => !(r.principal === 'User:node-5@LAB.KRABKA' && r.resource_type === 'group')) };
    await page.evaluate(() => { window.krabkaLab.settingsDialog(); });
    await page.getByLabel('Broker ACL policy (JSON)', { exact: true }).fill(JSON.stringify(policy));
    await page.locator('#krabka-lab dialog button[type="submit"]').click();
    await waitFor(page, `window.krabkaLab.world.scenario().authorization.acls.length === 4`, 'the revised ACL policy');
    await setSpeed(page,SPEED);
    await waitFor(page, `window.krabkaLab.world.liveSnapshot().nodes.some((n) => n.kind === 'admin' && n.state.authorization?.ready) && window.krabkaLab.timeline.events.some((e) => e.node === 5 && e.detail?.code === 30)`, 'the revoked group grant', 120_000);
    const revoked = await nodeStateOf(page,5);
    check('editing the policy revokes a stored grant on the existing broker disks', revoked.processed === 0 && await page.evaluate((id) => window.krabkaLab.world.scenario().id === id,saved.id));
    await page.evaluate(() => { window.krabkaLab.settingsDialog(); });
    await page.getByLabel('Broker ACL policy (JSON)', { exact: true }).fill('');
    await page.locator('#krabka-lab dialog button[type="submit"]').click();
    await waitFor(page, `!window.krabkaLab.world.scenario().authorization`, 'ACL enforcement to be disabled');
    await setSpeed(page,SPEED);
    await until(page, 'both consumers to read with ACL enforcement disabled', `(n) => n[5].state.processed > 0 && n[6].state.processed > 0`,120_000);
    check('clearing the ACL policy restores the default unrestricted broker',true);
  }
}

async function schemaRegistry(page, preset) {
  // Brokers 1 to 3, the registry 4, the producer 5, the consumer 6.
  const registered = await until(page, 'the producer to register its schema', `(n) => n[5].state.serialization && n[5].state.serialization.state === 'ready' && n[5].state.serialization`, 120_000);
  const subjects = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects' }));
  check('the registry answers GET /subjects with the subject the producer registered', subjects.ok && subjects.answer.status === 200 && JSON.stringify(subjects.answer.body) === '["orders-value"]', JSON.stringify(subjects));
  const decoded = await until(page, 'the consumer to decode', `(n) => {
    const r = n[6].state.last_records;
    return n[5].state.acked > 0 && r.length && r.every((x) => x.schema_id === ${registered.schema_id} && x.value_preview && typeof x.value_preview.customer === 'string') && r;
  }`, 120_000);
  check('the consumer decodes every value with the schema it fetched by id', decoded.length > 0, JSON.stringify(decoded[0]));
  await inspect(page, 6, 'billing');
  const shown = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector td[data-col="schema_id"]')?.textContent || null`, 'the schema column');
  check('the consumer inspector shows the schema id of each record', shown === `id ${registered.schema_id}`, shown);

  const payments = await registryWrite(page, 4, 'POST', '/subjects/payments-value/versions', { schema: PAYMENT_V1 });
  check('a schema registered through REST is answered once its record is back from the brokers', payments.ok && payments.answer.status === 200 && Number.isInteger(payments.answer.body.id), JSON.stringify(payments));

  // A second registry joins the group. The eligible instance with the
  // smallest URL, node 4, stays the primary; the new one is a secondary that
  // forwards the writes it takes to the primary.
  await page.locator('#krabka-lab .lab-dtab[data-tab="build"]').click();
  await page.locator('#krabka-lab .lab-kind-btn[data-kind="schema-registry"]').click();
  await page.waitForSelector('#krabka-lab dialog[open]');
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  const second = await waitFor(page, `(() => { const n = window.krabkaLab.world.scenario().nodes.find((x) => x.kind === 'schema-registry' && x.id !== 4); return n ? n.id : null; })()`, 'the second registry');
  const roles = await until(page, 'the two registries to elect a primary', `(n) => {
    const e = [4, ${second}].map((i) => n[i].state.state === 'ready' && n[i].state.election);
    return e[0] && e[1] && e[0].leader && e[0].leader === e[1].leader && { leader: e[0].leader, primary: e.map((x) => x.is_leader) };
  }`, 120_000);
  check('two registries of one group elect one primary, the smaller URL', roles.leader === 'http://node-4:8081' && JSON.stringify(roles.primary) === '[true,false]', JSON.stringify(roles));
  const via = await registryWrite(page, second, 'POST', '/subjects/shipments-value/versions', { schema: SHIPMENT_V1 });
  const onPrimary = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/shipments-value/versions/1' }));
  const forwarded = await until(page, 'the secondary to count the forward', `(n) => n[${second}].state.forwarder && n[${second}].state.forwarder.forwarded > 0 && n[${second}].state.forwarder`);
  check(
    'a write through the secondary is forwarded to the primary and served',
    via.ok && via.answer.status === 200 && onPrimary.ok && onPrimary.answer.status === 200 && onPrimary.answer.body.id === via.answer.body.id && forwarded.forwarded > 0,
    JSON.stringify({ via, onPrimary: onPrimary.answer, forwarded }),
  );
  await fit(page);
  await inspect(page, second, `schema-registry-${second}`);
  const role = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="election_role"]')?.textContent || null`, 'the election in the inspector');
  const labelled = await page.evaluate(() => [...document.querySelectorAll('#krabka-lab .lab-ev-kind[data-kind="election"]')].map((e) => e.textContent));
  check('the inspector names the secondary, and the timeline labels the election', /^secondary/.test(role) && labelled.length > 0 && labelled.every((l) => l === 'registry election'), `${role}; ${labelled.length} election rows`);
  const listed = await command(page, 'http', { path: '/subjects' });
  check('the registry command bar reads a REST resource', listed.ok && /"status":200/.test(listed.text) && /shipments-value/.test(listed.text), listed.text);

  // The registry keeps nothing of its own: a schema's record lives in
  // `_schemas` on the brokers' disks, which a reload keeps.
  const live = await until(page, 'the registry to show every subject', `(n) => n[4].state.subjects.length === 3 && n[4].state.subjects`);
  const versions = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/orders-value/versions/latest' }));
  await page.evaluate(() => window.krabkaLab.saveNow());
  await page.evaluate(() => window.krabkaLab.storage.flush());
  await page.waitForTimeout(1500);
  await reload(page, preset.name);
  const replayed = await until(page, 'the registry to replay _schemas', `(n) => n[4].state.state === 'ready' && n[4].state.subjects`, 120_000);
  const after = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/orders-value/versions/latest' }));
  check(
    'after a reload the registry replays every schema from the brokers\' _schemas log, with the same ids and versions',
    JSON.stringify(replayed) === JSON.stringify(live) && after.ok && JSON.stringify(after.answer.body) === JSON.stringify(versions.answer.body),
    `${JSON.stringify(replayed)} vs ${JSON.stringify(live)}`,
  );
}

async function wordCount(page, preset) {
  // Brokers 1 to 3, the producer 4, the streams app 5, the consumer 6.
  const counting = await until(page, 'the streams app to count', `(n) => {
    const s = n[5].state;
    if (s.state !== 'running' || s.tasks.length !== 3 || !s.tasks.every((task) => task.phase === 'running')) return null;
    const entries = s.stores.flatMap((st) => st.entries);
    return entries.length && n[6].state.processed > 0 && { entries, tasks: s.tasks.map((task) => task.id), sink: n[6].state.processed };
  }`, 120_000);
  check('the streams app runs three tasks and counts the words in its store', counting.tasks.length === 3 && counting.entries.every(([, count]) => count > 0), JSON.stringify(counting.entries.slice(0, 4)));
  check('a consumer reads the counts from word-counts', counting.sink > 0, `${counting.sink} records`);
  // The changelog topic is read from the streams node's own client metadata,
  // which the group's admin calls filled in.
  const changelog = await until(page, 'the changelog topic', `(n) => {
    const t = n[5].state.client?.metadata?.topics?.['word-count-counts-changelog'];
    return t && { partitions: t.partitions.length };
  }`, 90_000);
  const pill = await page.locator('#krabka-lab .lab-topic[data-topic="word-count-counts-changelog"]').count();
  check('the group creates the store\'s changelog topic, and the canvas draws it', changelog.partitions === 3 && pill === 1, `${JSON.stringify(changelog)}, ${pill} pill`);

  await inspect(page, 5, 'word-count');
  const [word] = counting.entries[0];
  const query = await command(page, 'query', { store: 'counts', key: word });
  const answer = JSON.parse(query.text.slice(query.text.indexOf('{')));
  check(`the query box reads ${word} from the counts store`, query.ok && answer.key === word && answer.value >= counting.entries[0][1], query.text);
  await command(page, 'pause');
  const held = await until(page, 'the streams app to pause', `(n) => n[5].state.paused === true && { in: n[5].state.records_in, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${held.now} + 3000`, 'three seconds to pass');
  const stillIn = (await nodeStateOf(page, 5)).records_in;
  await command(page, 'resume');
  const moving = await until(page, 'the streams app to resume', `(n) => n[5].state.paused === false && n[5].state.records_in > ${stillIn} && n[5].state.records_in`);
  check('Pause holds the streams app and Resume starts it again', stillIn === held.in && moving > stillIn, `${held.in} -> ${stillIn} -> ${moving}`);
}

// Brokers 1 to 5, with 4 and 5 cut off from the rest; the producer 6, the
// consumer 7. A minority cut off for longer than about two minutes of lab
// time gives up and exits, so the heal comes soon after the majority serves.
async function partition(page, preset) {
  const serving = (id) => page.evaluate((id) => { const p = window.krabkaLab.external.process(id); return p ? p.tail('stderr').some((l) => l.includes('krabka-broker serving on')) : null; }, id);
  await until(page, 'the majority to serve', `(n) => n[6].state.acked > 0 && n[7].state.processed > 0`, 120_000);
  await page.waitForTimeout(3000);
  const before = await Promise.all([1, 2, 3, 4, 5].map(serving));
  check('with brokers 4 and 5 cut off, brokers 1 to 3 serve the group and 4 and 5 do not serve', before.slice(0, 3).every(Boolean) && !before[3] && !before[4], JSON.stringify(before));
  for (const a of [4, 5]) for (const b of [1, 2, 3]) await page.evaluate(([a, b]) => window.krabkaLab.fault({ kind: 'heal', a, b }), [a, b]);
  await waitFor(page, `[4, 5].every((id) => window.krabkaLab.external.process(id)?.tail('stderr').some((l) => l.includes('krabka-broker serving on')))`, 'brokers 4 and 5 to serve after the heal', 90_000);
  check('healed, brokers 4 and 5 register and serve', true);
  // A restarted client refetches its metadata and sees five brokers.
  let five = null;
  for (const start = Date.now(); !five && Date.now() - start < 90_000; ) {
    await page.evaluate(() => window.krabkaLab.fault({ kind: 'restart', node: 6 }));
    await page.waitForTimeout(3000);
    const ids = await page.evaluate(() => {
      const s = window.krabkaLab.world.snapshot().nodes.find((n) => n.id === 6).state;
      return s.client?.metadata ? (s.client.metadata.brokers || []).map((b) => b.id).sort() : [];
    });
    if (ids.length === 5) five = ids;
  }
  check('a restarted producer sees five brokers in its metadata', Boolean(five), JSON.stringify(five));
}

const FLOWS = [
  ['sspi-encrypted', sspiEncrypted],
  ['broker-acls', brokerAcls],
  ['broker-acls-deny', brokerAcls],
  ['three-brokers', threeBrokers],
  ['schema-registry', schemaRegistry],
  ['streams-word-count', wordCount],
  ['five-brokers-partition', partition],
];

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  if (!fs.existsSync(path.join(DIST_DIR, 'playground', 'broker', 'krabka-broker.wasm'))) {
    console.error('dist/playground/broker/krabka-broker.wasm is missing: run `npm run build:broker` and `npm run build` first.');
    process.exit(2);
  }
  const { PRESETS } = await import(pathToFileURL(path.join(ROOT, 'public', 'playground', 'lab', 'presets.js')).href);
  const browser = await launchOrExit();
  const { server, port } = await serve(DIST_DIR);
  const base = `http://127.0.0.1:${port}`;
  const errors = [];
  const started = Date.now();
  try {
    const selected = process.argv.find((arg) => arg.startsWith('--preset='))?.slice('--preset='.length);
    if (selected && !FLOWS.some(([id]) => id === selected)) throw new Error(`Unknown cluster check: ${selected}`);
    for (const [id, flow] of FLOWS.filter(([id]) => !selected || id === selected)) {
      const preset = PRESETS.find((p) => p.id === id);
      console.log(`Cluster Lab: ${preset.name}`);
      const t0 = Date.now();
      // A context of its own: its IndexedDB and broker disks are the flow's.
      const context = await newLabContext(browser, { width: 1400, height: 1000 });
      const page = await context.newPage();
      errors.push(watchErrors(page, id, base));
      await t.flow(id, async () => {
        await openLab(page, base);
        await openScenario(page, preset.scenario, 120_000);
        await setSpeed(page, SPEED);
        try { await flow(page, preset); }
        catch (err) {
          const evidence = await page.evaluate(() => ({
            world: window.krabkaLab.world.liveSnapshot(),
            events: window.krabkaLab.timeline.events.slice(-100),
          })).catch(() => null);
          const directory = path.join(ROOT, 'artifacts', 'lab-clusters');
          fs.mkdirSync(directory, { recursive: true });
          fs.writeFileSync(path.join(directory, `${id}-failure.json`), JSON.stringify(evidence, null, 2));
          throw err;
        }
      });
      console.log(`  (${((Date.now() - t0) / 1000).toFixed(1)} s)`);
      await context.close();
    }
  } finally {
    await browser.close();
    server.close();
  }
  check('no page errors or console errors', errors.flat().length === 0, errors.flat().slice(0, 3).join(' | '));
  t.finish('✅ PASS: the cluster presets run on real brokers.', started);
}

main();
