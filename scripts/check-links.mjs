import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIST_DIR = path.resolve(__dirname, '..', 'dist');

function getAllHtmlFiles(dir) {
  let results = [];
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      // Exclude generated javadoc/rustdoc/godoc raw pages from being crawlers
      // (they contain internal intra-doc javascript symbols)
      if (fullPath.includes('/api/') || fullPath.includes('\\api\\')) continue;
      results = results.concat(getAllHtmlFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith('.html')) {
      results.push(fullPath);
    }
  }
  return results;
}

const htmlFiles = getAllHtmlFiles(DIST_DIR);
console.log(`Auditing ${htmlFiles.length} HTML files in dist/...`);

let totalLinks = 0;
let brokenLinks = [];

const hrefRegex = /href=["']([^"'#]+)(#[^"'\s]*)?["']/g;

for (const file of htmlFiles) {
  const content = fs.readFileSync(file, 'utf8');
  let match;
  while ((match = hrefRegex.exec(content)) !== null) {
    const rawHref = match[1].trim();
    if (!rawHref) continue;
    totalLinks++;

    // Skip external links, mailto, javascript, data, tel
    if (
      rawHref.startsWith('http://') ||
      rawHref.startsWith('https://') ||
      rawHref.startsWith('mailto:') ||
      rawHref.startsWith('javascript:') ||
      rawHref.startsWith('data:') ||
      rawHref.startsWith('tel:')
    ) {
      continue;
    }

    // Resolve internal relative or absolute link
    let targetPath;
    if (rawHref.startsWith('/')) {
      targetPath = path.join(DIST_DIR, rawHref.slice(1));
    } else {
      targetPath = path.resolve(path.dirname(file), rawHref);
    }

    // Check existence
    const exists =
      fs.existsSync(targetPath) ||
      fs.existsSync(path.join(targetPath, 'index.html')) ||
      fs.existsSync(targetPath + '.html');

    if (!exists) {
      brokenLinks.push({
        source: path.relative(DIST_DIR, file),
        href: rawHref,
        resolved: path.relative(DIST_DIR, targetPath),
      });
    }
  }
}

console.log(`Total internal links checked: ${totalLinks}`);
if (brokenLinks.length === 0) {
  console.log('✅ PASS: All internal links resolve to valid pages/files!');
} else {
  console.error(`❌ FAIL: Found ${brokenLinks.length} broken link(s):`);
  for (const b of brokenLinks) {
    console.error(`  • In ${b.source} -> href="${b.href}" (target not found: ${b.resolved})`);
  }
  process.exit(1);
}
