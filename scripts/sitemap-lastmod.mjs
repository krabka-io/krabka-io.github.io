// Run after Astro writes XML, before byte sharding. Astro never sees page dates,
// so its chunk writer cannot infer misleading child-file modification dates.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readSitemap } from './sitemap.mjs';
import { repositoryState, resolveLastmod, validateAttestations } from './lastmod-provenance.mjs';
import { routeInputs } from './lastmod-routes.mjs';

const unescape = (text) => text.replace(/&(amp|lt|gt|quot|apos);/g, (_, entity) => ({ amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" })[entity]);

export function applyLastmod(dir, { root = process.cwd(), attestations = JSON.parse(fs.readFileSync(path.join(root, 'scripts/lastmod-reviewed.json'), 'utf8')), map = routeInputs, state = repositoryState(root) } = {}) {
  validateAttestations(attestations); // Supplied malformed dates fail, even on omitted routes.
  const indexFile = path.join(dir, 'sitemap-index.xml');
  let index = fs.readFileSync(indexFile, 'utf8');
  const children = readSitemap(index, 'sitemapindex', indexFile);
  const records = [];
  for (const child of children) {
    const file = path.join(dir, path.basename(new URL(child).pathname));
    const xml = fs.readFileSync(file, 'utf8');
    readSitemap(xml, 'urlset', file, { maxBytes: Infinity });
    const updated = xml.replace(/<url(?:\s[^>]*)?>[\s\S]*?<\/url>/g, (entry) => {
      const url = unescape(/<loc>([\s\S]*?)<\/loc>/.exec(entry)[1]);
      const route = new URL(url).pathname.replace(/\/$/, '') || '/';
      const record = resolveLastmod({ root, url, mapping: map(root, url), attestation: attestations[route], state });
      records.push(record);
      // A single owner for dates; no stale generated or hand-supplied fallback.
      entry = entry.replace(/<lastmod>[\s\S]*?<\/lastmod>/g, '');
      return record.date ? entry.replace('</loc>', `</loc><lastmod>${record.date}</lastmod>`) : entry;
    });
    readSitemap(updated, 'urlset', file, { maxBytes: Infinity });
    fs.writeFileSync(file, updated);
  }
  // Page dates cannot establish child modification times, including removals.
  index = index.replace(/<lastmod>[\s\S]*?<\/lastmod>/g, '');
  fs.writeFileSync(indexFile, index);
  records.sort((a, b) => a.url.localeCompare(b.url));
  fs.writeFileSync(path.join(dir, 'lastmod-report.json'), JSON.stringify({ version: 1, records }, null, 2) + '\n');
  const dated = records.filter((record) => record.date).length;
  console.log(`Lastmod: ${dated}/${records.length} verified dates; ${records.length - dated} omissions (dist/lastmod-report.json).`);
  return records;
}

export default function sitemapLastmod() {
  return { name: 'sitemap-verified-lastmod', hooks: { 'astro:build:done': ({ dir }) => applyLastmod(fileURLToPath(dir)) } };
}
