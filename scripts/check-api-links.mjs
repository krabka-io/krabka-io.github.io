// Fetches every API reference the /api directory links to and fails unless each
// one answers 200 with an HTML page that is not the old placeholder.
//
//     npm run check-api-links
//
// The references are published by each component's own repository to GitHub
// Pages, so this needs the network and is not part of `npm run build`. Run it
// after a repository first deploys its docs, and whenever a link changes.
import fs from 'node:fs';

const SITE = 'https://krabka.io';
const entries = JSON.parse(fs.readFileSync(new URL('../src/data/api-reference.json', import.meta.url), 'utf8'));

let failures = 0;
for (const entry of entries) {
  const url = new URL(entry.apiPath, SITE).href;
  try {
    const response = await fetch(url, { redirect: 'follow', signal: AbortSignal.timeout(20000) });
    const body = response.ok ? await response.text() : '';
    const placeholder = body.includes('is published upon official release tagging');
    if (!response.ok) {
      console.log(`✗ ${entry.id.padEnd(13)} ${url} -> HTTP ${response.status}`);
      failures += 1;
    } else if (placeholder) {
      console.log(`✗ ${entry.id.padEnd(13)} ${url} -> still the placeholder page`);
      failures += 1;
    } else {
      console.log(`✓ ${entry.id.padEnd(13)} ${url}`);
    }
  } catch (error) {
    console.log(`✗ ${entry.id.padEnd(13)} ${url} -> ${error.message}`);
    failures += 1;
  }
}

console.log(failures === 0 ? '\n✅ Every API reference resolves.' : `\n${failures} of ${entries.length} API references do not resolve.`);
process.exit(failures === 0 ? 0 : 1);
