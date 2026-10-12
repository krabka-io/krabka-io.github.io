// Renders the per-section social cards in public/og/ (1200x630 PNG) with
// Playwright's Chromium. Run after changing a card: node scripts/og-images.mjs
import fs from 'node:fs';
import path from 'node:path';
import { chromium } from 'playwright';

const root = path.resolve(import.meta.dirname, '..');
const logo = fs.readFileSync(path.join(root, 'public', 'logo.svg'), 'utf8');
const font = fs.readFileSync(path.join(root, 'public', 'fonts', 'inter-latin-wght-normal.woff2')).toString('base64');

const cards = [
  { file: 'home', kicker: 'Kafka-compatible broker', title: 'Keep your Kafka clients and swap the broker.', subtitle: 'Safe Rust, with its own KRaft quorum. Key consensus and replication decisions are proved with Creusot and model-checked with Stateright.' },
  { file: 'docs', kicker: 'Documentation', title: 'Guides for running and operating krabka', subtitle: 'Quickstart, broker configuration, KIP matrix, streams libraries, operator and CLI guides.' },
  { file: 'benchmarks', kicker: 'Benchmarks', title: 'krabka, Apache Kafka and Redpanda', subtitle: 'Publish rates and latency from the OpenMessaging Benchmark, with the full results and method.' },
  { file: 'verification', kicker: 'Verification', title: 'How krabka is verified', subtitle: 'Selected decision kernels are proved with Creusot. Stateright checks failure sequences within each model’s bounds. The code is safe Rust.' },
  { file: 'lab', kicker: 'Cluster Lab', title: 'Real brokers in your browser', subtitle: 'Run krabka brokers, clients and streams apps in WebAssembly. Break the network and read the logs.' },
  { file: 'features', kicker: 'Features', title: 'A Kafka-compatible platform in Rust', subtitle: 'Native KRaft quorum, tiered storage, Postgres CDC, polyglot streams and a Kubernetes operator.' },
];

const html = (c) => `<!doctype html><html><head><style>
@font-face { font-family: Inter; src: url(data:font/woff2;base64,${font}) format('woff2'); font-weight: 100 900; }
* { margin: 0; box-sizing: border-box; }
body { width: 1200px; height: 630px; font-family: Inter, sans-serif; color: #fff; padding: 72px 80px;
  background: radial-gradient(ellipse 90% 70% at 85% 10%, #14223d, #0c1322 55%, #080d1a); display: flex; flex-direction: column; }
.brand { display: flex; align-items: center; gap: 16px; font-size: 38px; font-weight: 800; letter-spacing: -0.02em; }
.brand svg { width: 60px; height: 60px; }
.kicker { margin-top: 92px; color: #ff8466; font-size: 26px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; }
h1 { margin-top: 18px; font-size: 64px; font-weight: 800; letter-spacing: -0.03em; line-height: 1.05; text-wrap: balance; }
p { margin-top: 26px; max-width: 960px; color: #94a3b8; font-size: 27px; line-height: 1.4; }
.url { margin-top: auto; align-self: flex-end; color: #475569; font-size: 24px; font-weight: 700; }
</style></head><body>
<div class="brand">${logo}<span>krabka</span></div>
<div class="kicker">${c.kicker}</div><h1>${c.title}</h1><p>${c.subtitle}</p><div class="url">krabka.io</div>
</body></html>`;

const out = path.join(root, 'public', 'og');
fs.mkdirSync(out, { recursive: true });
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1200, height: 630 } });
for (const card of cards) {
  await page.setContent(html(card));
  await page.screenshot({ path: path.join(out, `${card.file}.png`) });
  console.log(`public/og/${card.file}.png`);
}
await browser.close();
