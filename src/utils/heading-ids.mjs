// Heading ids for every built page. Markdown headings get theirs from the
// renderer; a heading in an .astro page has none, so after the build this gives
// each h2 to h4 without one an id from its text. It runs before Pagefind reads
// dist/, so search results can link to a section, and the client script in
// headings.ts then finds the id already there.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { slugify } from './slugify.mjs';

const decode = (s) =>
  s.replace(/&(amp|lt|gt|quot|#39|#x27);/g, (_, e) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'", '#x27': "'" })[e]);

export function addHeadingIds(html) {
  const used = new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]));
  return html.replace(/<(h[234])((?:\s[^>]*)?)>([\s\S]*?)<\/\1>/g, (whole, tag, attrs, inner) => {
    if (/\sid=/.test(attrs)) return whole;
    const base = slugify(decode(inner.replace(/<[^>]+>/g, ''))) || 'section';
    let id = base;
    for (let i = 2; used.has(id); i++) id = `${base}-${i}`;
    used.add(id);
    return `<${tag} id="${id}"${attrs}>${inner}</${tag}>`;
  });
}

function htmlFiles(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const full = path.join(dir, e.name);
    return e.isDirectory() ? htmlFiles(full) : e.name.endsWith('.html') ? [full] : [];
  });
}

export default function headingIds() {
  return {
    name: 'heading-ids',
    hooks: {
      'astro:build:done': ({ dir }) => {
        for (const file of htmlFiles(fileURLToPath(dir))) {
          const html = fs.readFileSync(file, 'utf8');
          const out = addHeadingIds(html);
          if (out !== html) fs.writeFileSync(file, out);
        }
      },
    },
  };
}
