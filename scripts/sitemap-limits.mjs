// Astro splits by entry count. Apply byte-aware sharding before build validation.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MAX_BYTES, MAX_ENTRIES, readSitemap } from './sitemap.mjs';

export function splitSitemap(xml, { maxBytes = MAX_BYTES, maxEntries = MAX_ENTRIES } = {}) {
  const start = xml.indexOf('>', xml.indexOf('<urlset')) + 1;
  const end = xml.lastIndexOf('</urlset>');
  if (!start || end < start) throw new Error('Expected urlset for byte-aware splitting');
  const header = xml.slice(0, start);
  const footer = xml.slice(end);
  const entries = xml.slice(start, end).match(/<url(?:\s[^>]*)?>[\s\S]*?<\/url>/g) ?? [];
  if (xml.slice(start, end).replace(/<url(?:\s[^>]*)?>[\s\S]*?<\/url>/g, '').trim()) throw new Error('Unexpected content in sitemap; refusing lossy sharding');
  const overhead = Buffer.byteLength(header + footer);
  const chunks = [];
  let current = [];
  let bytes = overhead;
  for (const entry of entries) {
    const size = Buffer.byteLength(entry);
    if (overhead + size > maxBytes) throw new Error('A single sitemap entry exceeds the byte limit');
    if (current.length && (bytes + size > maxBytes || current.length >= maxEntries)) {
      chunks.push(header + current.join('') + footer);
      current = [];
      bytes = overhead;
    }
    current.push(entry);
    bytes += size;
  }
  if (current.length) chunks.push(header + current.join('') + footer);
  return chunks;
}

export default function sitemapLimits() {
  return {
    name: 'sitemap-protocol-limits',
    hooks: {
      'astro:build:done': ({ dir }) => {
        const base = fileURLToPath(dir);
        const indexFile = path.join(base, 'sitemap-index.xml');
        let index = fs.readFileSync(indexFile, 'utf8');
        for (const child of readSitemap(index, 'sitemapindex', indexFile)) {
          const name = path.basename(new URL(child).pathname);
          const file = path.join(base, name);
          const content = fs.readFileSync(file, 'utf8');
          const chunks = splitSitemap(content);
          if (chunks.length <= 1) continue;
          const references = [];
          chunks.forEach((chunk, i) => {
            const part = i === 0 ? name : name.replace(/\.xml$/, `-part-${i}.xml`);
            fs.writeFileSync(path.join(base, part), chunk);
            references.push(`<sitemap><loc>${new URL(part, child).href}</loc></sitemap>`);
          });
          const original = `<sitemap><loc>${child}</loc></sitemap>`;
          if (!index.includes(original)) throw new Error('Unexpected index serialization; cannot safely shard sitemap');
          index = index.replace(original, references.join(''));
        }
        // The final checker enforces the index's own byte and entry limits.
        fs.writeFileSync(indexFile, index);
      },
    },
  };
}
