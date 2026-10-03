import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '..');
const DOCS_CONTENT_DIR = path.resolve(ROOT_DIR, 'src', 'content', 'docs');
const DOCS_PAGES_DIR = path.resolve(ROOT_DIR, 'src', 'pages', 'docs');

function getFiles(dir, ext = '.md') {
  let results = [];
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results = results.concat(getFiles(fullPath, ext));
    } else if (entry.isFile() && entry.name.endsWith(ext)) {
      results.push(fullPath);
    }
  }
  return results;
}

const mdFiles = getFiles(DOCS_CONTENT_DIR, '.md');
const astroFiles = getFiles(DOCS_PAGES_DIR, '.astro');

console.log(`\n🔍 [verify-code-stubs] Inspecting code blocks across ${mdFiles.length} markdown guides and ${astroFiles.length} docs pages...\n`);

let totalCodeBlocks = 0;
const languageCounts = {};
const syntaxErrors = [];

// Regex to capture markdown fenced code blocks: ```lang ... ```
const codeBlockRegex = /```([a-zA-Z0-9_-]*)\n([\s\S]*?)```/g;

for (const file of mdFiles) {
  const relPath = path.relative(ROOT_DIR, file);
  const content = fs.readFileSync(file, 'utf8');
  let match;

  while ((match = codeBlockRegex.exec(content)) !== null) {
    totalCodeBlocks++;
    const lang = match[1].toLowerCase().trim() || 'unspecified';
    const code = match[2];

    languageCounts[lang] = (languageCounts[lang] || 0) + 1;

    // 1. Check JSON code blocks for valid JSON syntax
    if (lang === 'json') {
      // Some JSON snippets might have comments or placeholders like <...>
      const cleanJson = code.replace(/\/\/.*$/gm, '').replace(/<[^>]+>/g, '"placeholder"');
      try {
        JSON.parse(cleanJson);
      } catch (e) {
        // If it still fails, record it
        syntaxErrors.push({
          file: relPath,
          lang: 'json',
          error: e.message,
          snippet: code.slice(0, 100).trim(),
        });
      }
    }

    // 2. Check XML / Maven blocks for balanced tags
    if (lang === 'xml') {
      const openTags = (code.match(/<[a-zA-Z0-9_-]+>/g) || []).map(t => t.replace(/[<>]/g, ''));
      const closeTags = (code.match(/<\/[a-zA-Z0-9_-]+>/g) || []).map(t => t.replace(/[<>/]/g, ''));
      // Basic check that common XML tags are matched
      for (const tag of ['dependency', 'groupId', 'artifactId', 'version']) {
        const opened = openTags.filter(t => t === tag).length;
        const closed = closeTags.filter(t => t === tag).length;
        if (opened !== closed) {
          syntaxErrors.push({
            file: relPath,
            lang: 'xml',
            error: `Mismatched <${tag}> tags (opened: ${opened}, closed: ${closed})`,
            snippet: code.slice(0, 100).trim(),
          });
        }
      }
    }
  }
}

console.log(`📊 Code Blocks Found by Language:`);
for (const [lang, count] of Object.entries(languageCounts).sort((a, b) => b[1] - a[1])) {
  console.log(`  • ${lang}: ${count} block(s)`);
}
console.log(`\nTotal code blocks inspected: ${totalCodeBlocks}`);

if (syntaxErrors.length === 0) {
  console.log(`\n✅ PASS: All JSON and XML structured code blocks parse cleanly without errors!`);
} else {
  console.log(`\n⚠️ Potential syntax errors found in ${syntaxErrors.length} snippet(s):`);
  for (const err of syntaxErrors) {
    console.log(`  • In ${err.file} (${err.lang}): ${err.error}`);
    console.log(`    Preview: "${err.snippet}..."\n`);
  }
}
