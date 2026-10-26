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

const escape = (value) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&apos;');

export function enforceSitemapLimits(base, limits = {}) {
  const indexFile = path.join(base, 'sitemap-index.xml');
  const index = fs.readFileSync(indexFile, 'utf8');
  const references = [];
  for (const child of readSitemap(index, 'sitemapindex', indexFile)) {
    const name = path.basename(new URL(child).pathname);
    const chunks = splitSitemap(fs.readFileSync(path.join(base, name), 'utf8'), limits);
    chunks.forEach((chunk, i) => {
      const part = i === 0 ? name : name.replace(/\.xml$/, `-part-${i}.xml`);
      fs.writeFileSync(path.join(base, part), chunk);
      references.push(`<sitemap><loc>${escape(new URL(part, child).href)}</loc></sitemap>`);
    });
  }
  // Rebuild references structurally from the parsed locations. Index lastmod is
  // deliberately absent, even if an integration supplied it before this hook.
  const start = index.indexOf('>', index.indexOf('<sitemapindex')) + 1;
  const end = index.lastIndexOf('</sitemapindex>');
  if (!start || end < start) throw new Error('Expected sitemapindex');
  fs.writeFileSync(indexFile, index.slice(0, start) + references.join('') + index.slice(end));
}

export default function sitemapLimits() {
  return { name: 'sitemap-protocol-limits', hooks: { 'astro:build:done': ({ dir }) => enforceSitemapLimits(fileURLToPath(dir)) } };
}
