import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { scenarioFromHash } from '../public/playground/lab/scenarios.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DIST_DIR = path.resolve(process.argv[2] ?? path.join(ROOT, 'dist'));
const SITE = new URL('https://krabka.io');
// Component references are deployed by their own repositories; check-api-links
// checks these URLs against the live site rather than this build's files.
const apiPaths = JSON.parse(fs.readFileSync(path.join(ROOT, 'src/data/api-reference.json'), 'utf8')).map((entry) => entry.apiPath);

const decodeHtml = (text) => text.replace(/&(?:amp|quot|apos|lt|gt|#\d+|#x[\da-f]+);/gi, (entity) => {
  const named = { '&amp;': '&', '&quot;': '"', '&apos;': "'", '&lt;': '<', '&gt;': '>' };
  if (named[entity.toLowerCase()]) return named[entity.toLowerCase()];
  const code = entity.slice(2, -1);
  const value = code[0].toLowerCase() === 'x' ? parseInt(code.slice(1), 16) : Number(code);
  return value <= 0x10ffff ? String.fromCodePoint(value) : '\uFFFD';
});
const attribute = (tag, name) => {
  const match = tag.match(new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, 'i'));
  return match ? decodeHtml(match[1] ?? match[2] ?? match[3]) : null;
};
const markup = (content) => content.replace(/<!--[\s\S]*?-->|<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, '');
function htmlFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? htmlFiles(file) : entry.name.endsWith('.html') ? [file] : [];
  });
}
const files = htmlFiles(DIST_DIR);
if (!files.length) {
  console.error(`No built HTML pages in ${DIST_DIR}. Build the site before checking links.`);
  process.exit(1);
}
const cache = new Map();
function read(file) {
  if (!cache.has(file)) {
    const content = fs.readFileSync(file, 'utf8');
    const html = markup(content);
    const ids = new Set([...html.matchAll(/<[^>]+>/g)].flatMap(([tag]) => {
      const id = attribute(tag, 'id');
      const name = /^<a\b/i.test(tag) ? attribute(tag, 'name') : null;
      return [id, name].filter((value) => value !== null);
    }));
    cache.set(file, { content, html, ids });
  }
  return cache.get(file);
}
function embedded(content, id) {
  for (const [, tag, json] of content.matchAll(/(<script\b[^>]*>)([\s\S]*?)<\/script>/gi)) {
    if (attribute(tag, 'id') === id) return JSON.parse(json);
  }
  throw new Error(`Missing embedded data: ${id}`);
}
async function checkFragment(url, file) {
  if (!url.hash || !/\.(?:html|svg)$/i.test(file)) return;
  const fragment = decodeURIComponent(url.hash.slice(1));
  const target = read(file);
  const route = url.pathname.replace(/\/$/, '');
  if (route === '/docs/lab' && fragment.startsWith('s=')) {
    if (!await scenarioFromHash(url.hash)) throw new Error('Missing scenario in lab link');
    return;
  }
  if (route === '/docs/proof-explorer' && fragment.startsWith('session=')) {
    if (embedded(target.content, 'proof-sessions').sessions.some((session) => session.id === fragment.slice(8))) return;
  } else if (route === '/docs/verification-playground' && fragment.startsWith('kernel=')) {
    if (embedded(target.content, 'krabka-kernel-specs').some((kernel) => kernel.id === fragment.slice(7))) return;
  } else if (target.ids.has(fragment)) return;
  throw new Error(`Missing fragment #${fragment}`);
}

console.log(`Auditing ${files.length} HTML files in ${DIST_DIR}...`);
let total = 0;
const broken = [];
for (const file of files) {
  const source = path.relative(DIST_DIR, file).split(path.sep).join('/');
  const pagePath = '/' + source.replace(/(?:^|\/)index\.html$/, '/').replace(/^\//, '');
  for (const [tag] of read(file).html.matchAll(/<(?:a|area|link)\b[^>]*>/gi)) {
    const href = attribute(tag, 'href');
    if (href === null) continue;
    try {
      const url = new URL(href, new URL(pagePath, SITE));
      if (['mailto:', 'tel:', 'data:'].includes(url.protocol)) continue;
      if (!['http:', 'https:'].includes(url.protocol)) throw new Error(`Unsupported link protocol ${url.protocol}`);
      if (url.origin !== SITE.origin) continue;
      if (apiPaths.some((prefix) => url.pathname.startsWith(prefix))) continue;
      total++;
      const decodedPath = decodeURIComponent(url.pathname);
      const target = path.resolve(DIST_DIR, '.' + decodedPath);
      const relative = path.relative(DIST_DIR, target);
      if (relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('Link escapes the built site');
      const resolved = [target, path.join(target, 'index.html'), target + '.html'].find((candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile());
      if (!resolved) throw new Error(`Target not found: ${url.pathname}`);
      await checkFragment(url, resolved);
    } catch (error) {
      broken.push({ source, href, reason: error.message });
    }
  }
}
console.log(`Internal links checked: ${total}`);
for (const { source, href, reason } of broken) console.error(`${source} -> ${JSON.stringify(href)}: ${reason}`);
console.log(broken.length ? `FAIL: ${broken.length} broken link(s).` : 'PASS: Internal pages, files and fragments resolve.');
process.exitCode = broken.length ? 1 : 0;
