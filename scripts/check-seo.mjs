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
      // Exclude generated compiler docs (javadoc, rustdoc, godoc) and raw chart repository index from site SEO checks
      if (
        fullPath.includes('/api/') ||
        fullPath.includes('\\api\\') ||
        fullPath.endsWith('/charts') ||
        fullPath.endsWith('\\charts') ||
        fullPath.includes('/charts/') ||
        fullPath.includes('\\charts\\')
      ) continue;
      results = results.concat(getAllHtmlFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith('.html')) {
      results.push(fullPath);
    }
  }
  return results;
}

const htmlFiles = getAllHtmlFiles(DIST_DIR);
console.log(`\n🔍 [seo-check] Running SEO audit across ${htmlFiles.length} site pages in dist/...\n`);

let passedCount = 0;
const issues = [];
const titles = new Map();

for (const file of htmlFiles) {
  const relPath = path.relative(DIST_DIR, file);
  const content = fs.readFileSync(file, 'utf8');
  const fileIssues = [];

  const is404 = relPath === '404.html';
  const isRedirect = /<meta\s+http-equiv=["']refresh["']/i.test(content);

  // Skip redirect stubs (e.g. /docs/api redirecting to /api)
  if (isRedirect) {
    passedCount++;
    continue;
  }

  // 1. Title Tag
  const titleMatch = content.match(/<title>([^<]*)<\/title>/i);
  if (!titleMatch || !titleMatch[1].trim()) {
    fileIssues.push('Missing or empty <title> tag');
  } else {
    const title = titleMatch[1].trim();
    if (title.length < 15) {
      fileIssues.push(`Short <title> (${title.length} chars): "${title}"`);
    } else if (title.length > 80) {
      fileIssues.push(`Long <title> (${title.length} chars, recommended <70): "${title}"`);
    }
    // Check for duplicate titles
    if (!is404) {
      if (titles.has(title)) {
        fileIssues.push(`Duplicate title shared with ${titles.get(title)}: "${title}"`);
      } else {
        titles.set(title, relPath);
      }
    }
  }

  // 2. Meta Description
  const descMatch = content.match(/<meta\s+name=["']description["']\s+content=(["'])([\s\S]*?)\1/i) ||
                    content.match(/<meta\s+content=(["'])([\s\S]*?)\1\s+name=["']description["']/i);
  if (!descMatch || !descMatch[2].trim()) {
    if (!is404) fileIssues.push('Missing or empty <meta name="description">');
  } else {
    const desc = descMatch[2].trim();
    if (desc.length < 30) {
      fileIssues.push(`Short meta description (${desc.length} chars): "${desc}"`);
    } else if (desc.length > 200) {
      fileIssues.push(`Long meta description (${desc.length} chars, recommended <160): "${desc.slice(0, 50)}..."`);
    }
  }

  // 3. Canonical Link
  const canonicalMatch = content.match(/<link\s+rel=["']canonical["']\s+href=(["'])([\s\S]*?)\1/i) ||
                        content.match(/<link\s+href=(["'])([\s\S]*?)\1\s+rel=["']canonical["']/i);
  if (!canonicalMatch && !is404) {
    fileIssues.push('Missing <link rel="canonical">');
  }

  // 4. Open Graph Tags
  const ogTitle = content.match(/<meta\s+property=["']og:title["']/i);
  const ogDesc = content.match(/<meta\s+property=["']og:description["']/i);
  const ogUrl = content.match(/<meta\s+property=["']og:url["']/i);
  const ogType = content.match(/<meta\s+property=["']og:type["']/i);

  if (!is404) {
    if (!ogTitle) fileIssues.push('Missing <meta property="og:title">');
    if (!ogDesc) fileIssues.push('Missing <meta property="og:description">');
    if (!ogUrl) fileIssues.push('Missing <meta property="og:url">');
    if (!ogType) fileIssues.push('Missing <meta property="og:type">');
  }

  // 5. Twitter Tags
  const twitterCard = content.match(/<meta\s+name=["']twitter:card["']/i);
  if (!twitterCard && !is404) {
    fileIssues.push('Missing <meta name="twitter:card">');
  }

  // 6. H1 Structure
  const h1Matches = [...content.matchAll(/<h1[^>]*>([\s\S]*?)<\/h1>/gi)];
  if (h1Matches.length === 0) {
    fileIssues.push('Missing <h1> heading');
  } else if (h1Matches.length > 1) {
    fileIssues.push(`Multiple <h1> headings found (${h1Matches.length})`);
  }

  // 7. Image Alt Attributes
  const imgMatches = [...content.matchAll(/<img\s+([^>]*?)>/gi)];
  for (const img of imgMatches) {
    const attrs = img[1];
    if (!/alt=["']/i.test(attrs)) {
      fileIssues.push(`Image missing alt attribute: <img ${attrs.slice(0, 40)}...>`);
    }
  }

  // 8. Structured Data JSON-LD
  const jsonLdMatches = [...content.matchAll(/<script\s+type=["']application\/ld\+json["']>([\s\S]*?)<\/script>/gi)];
  for (const match of jsonLdMatches) {
    try {
      JSON.parse(match[1]);
    } catch (e) {
      fileIssues.push(`Malformed JSON-LD structured data: ${e.message}`);
    }
  }

  if (fileIssues.length === 0) {
    passedCount++;
  } else {
    issues.push({ file: relPath, fileIssues });
  }
}

// 9. Check robots.txt and sitemap
const robotsExists = fs.existsSync(path.join(DIST_DIR, 'robots.txt'));
const sitemapExists = fs.existsSync(path.join(DIST_DIR, 'sitemap-index.xml')) || fs.existsSync(path.join(DIST_DIR, 'sitemap-0.xml'));

console.log(`📊 Summary of Results:`);
console.log(`  • Pages Audited: ${htmlFiles.length}`);
console.log(`  • Pages Passing 100% of Checks: ${passedCount}`);
console.log(`  • Pages with Issues/Warnings: ${issues.length}`);
console.log(`  • robots.txt: ${robotsExists ? '✅ Present' : '❌ Missing'}`);
console.log(`  • sitemap-index.xml: ${sitemapExists ? '✅ Present' : '❌ Missing'}\n`);

if (issues.length > 0) {
  console.log(`⚠️ Detailed Issues & Warnings by Page:\n`);
  for (const item of issues) {
    console.log(`📄 ${item.file}:`);
    for (const issue of item.fileIssues) {
      console.log(`   - ${issue}`);
    }
    console.log();
  }
} else {
  console.log(`🎉 100% PERFECT SEO SCORE! Every page satisfies technical SEO requirements.\n`);
}
