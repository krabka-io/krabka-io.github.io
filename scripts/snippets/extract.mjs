import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';

export const pages = {
  'get-started': ['deploySnippets.docker', 'deploySnippets.helm', 'clientSnippets.rust', 'clientSnippets.go', 'clientSnippets.java', 'clientSnippets.python', 'clientSnippets.javascript', 'clientSnippets.cli', 'inspectSnippet'],
  'docs/quickstart': ['composeCode', 'helmCode'],
  'docs/streams-rs': ['producerCode', 'consumerCode', 'shareConsumerCode', 'streamsCode'],
  'docs/streams-go': ['goInstallCode', 'goCode'],
  'docs/streams-java': ['javaGradle'],
};

export function astroSnippets(source, page, releaseNumber) {
  const scope = { releaseNumber };
  for (const name of new Set(pages[page].map(key => key.split('.')[0]))) {
    // Only the literal snippet declarations are evaluated, never page imports or scripts.
    const declaration = source.match(new RegExp(`const ${name} = (\x60(?:\\\\[\\s\\S]|[^\x60])*\x60|\\{[\\s\\S]*?^\\});`, 'm'));
    assert(declaration, `${page}: missing ${name}`);
    scope[name] = runInNewContext(`(${declaration[1]})`, scope, { timeout: 1000 });
  }
  const found = [];
  for (const [block] of source.matchAll(/<CodeBlock\b[\s\S]*?\/>/g)) {
    const expression = block.match(/code=\{([\s\S]*?)\}\s+lang=/)?.[1];
    const lang = block.match(/lang="([^"]+)"/)?.[1];
    assert(expression && lang, `${page}: unsupported CodeBlock: ${block}`);
    const key = expression === '`$ ${goInstallCode}`' ? 'goInstallCode' : expression;
    assert(pages[page].includes(key), `${page}: add CI coverage for ${key}`);
    found.push({ id: `${page}/${key}`, lang, code: scope[key.split('.')[0]][key.split('.')[1]] ?? scope[key] });
  }
  assert.deepEqual(found.map(s => s.id.split('/').at(-1)).sort(), [...pages[page]].sort(), `${page}: snippet coverage changed`);
  return found;
}

export function markdownSnippets(source, page) {
  const snippets = [];
  let heading = '';
  let index = 0;
  for (const match of source.matchAll(/^## (.+)$|^```(\w+)\r?\n([\s\S]*?)^```\s*$/gm)) {
    if (match[1]) { heading = match[1]; continue; }
    snippets.push({ id: `${page}/${++index}`, lang: match[2], code: match[3], heading });
  }
  assert(snippets.length, `${page}: no snippets found`);
  return snippets;
}

export function websiteSnippets(root) {
  const versions = JSON.parse(readFileSync(`${root}/src/data/versions.json`));
  return Object.keys(pages).flatMap(page => astroSnippets(
    readFileSync(`${root}/src/pages/${page}.astro`, 'utf8'), page,
    (versions['streams-java']?.activeRelease || 'v1.4.2').replace(/^v/, ''),
  ));
}
