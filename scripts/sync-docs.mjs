import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { rewriteDocLinks } from './rewrite-doc-links.mjs';

const ROOT_DIR = process.cwd();
const CONTENT_DOCS_DIR = path.join(ROOT_DIR, 'src', 'content', 'docs');

console.log('🦀 [sync-docs] Starting documentation synchronization...');

// 1. Ensure target directories exist
fs.mkdirSync(CONTENT_DOCS_DIR, { recursive: true });

// Component configurations
const COMPONENTS = [
  {
    name: 'streams-java',
    repo: 'krabka-streams-java',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-java'),
    docsSubdir: 'streams-java',
  },
  {
    name: 'streams-go',
    repo: 'krabka-streams-go',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-go'),
    docsSubdir: 'streams-go',
  },
  {
    name: 'broker',
    repo: 'krabka-broker',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-broker'),
    docsSubdir: 'broker',
  },
];

// Which commit each component's guides came from, shown in the footer of every
// synced page. The guides follow the default branch, not the latest release.
const sources = {};

for (const comp of COMPONENTS) {
  console.log(`\n📦 Processing component: ${comp.name} (${comp.repo})`);

  // --- Step A: Sync Markdown Guides ---
  const destDocsDir = path.join(CONTENT_DOCS_DIR, comp.docsSubdir);
  fs.rmSync(destDocsDir, { recursive: true, force: true });
  fs.mkdirSync(destDocsDir, { recursive: true });

  let sourceDocsDir = null;
  if (fs.existsSync(path.join(comp.localRepoDir, 'docs'))) {
    sourceDocsDir = path.join(comp.localRepoDir, 'docs');
    console.log(`  ✓ Found local docs directory at: ${sourceDocsDir}`);
  } else {
    // In CI or standalone clone: fetch docs using git sparse checkout
    const tempCloneDir = path.join('/tmp', `krabka-sync-${comp.repo}`);
    try {
      console.log(`  → Fetching docs from GitHub (krabka-io/${comp.repo})...`);
      fs.rmSync(tempCloneDir, { recursive: true, force: true });
      execSync(`git clone --depth 1 --filter=blob:none --sparse https://github.com/krabka-io/${comp.repo}.git ${tempCloneDir}`, { stdio: 'pipe' });
      execSync(`git -C ${tempCloneDir} sparse-checkout set docs`, { stdio: 'pipe' });
      if (fs.existsSync(path.join(tempCloneDir, 'docs'))) {
        sourceDocsDir = path.join(tempCloneDir, 'docs');
      }
    } catch (err) {
      console.warn(`  ⚠️ Could not sparse-checkout docs for ${comp.repo}: ${err.message}`);
    }
  }

  if (sourceDocsDir && fs.existsSync(sourceDocsDir)) {
    try {
      const [commit, date] = execSync(`git -C "${sourceDocsDir}" log -1 --format=%H%x09%cI`, { encoding: 'utf8' }).trim().split('\t');
      const branch = execSync(`git -C "${sourceDocsDir}" rev-parse --abbrev-ref HEAD`, { encoding: 'utf8' }).trim();
      sources[comp.docsSubdir] = { repo: comp.repo, branch, commit, date };
    } catch {
      // Not a git checkout; the footer then names the repository alone.
    }
    const files = fs.readdirSync(sourceDocsDir);
    let copiedCount = 0;
    for (const file of files) {
      // Exclude index.md so it doesn't collide with the root /docs/<module>.astro page
      if (file.endsWith('.md') && file !== 'index.md') {
        const srcFile = path.join(sourceDocsDir, file);
        const destFile = path.join(destDocsDir, file);
        let content = fs.readFileSync(srcFile, 'utf8');

        // Rewrite relative markdown links and code links to prevent 404 errors
        content = rewriteDocLinks(content, { docsSubdir: comp.docsSubdir, repo: comp.repo, sourceDocsDir });

        fs.writeFileSync(destFile, content);
        copiedCount++;
      }
    }
    console.log(`  ✓ Synced and transformed ${copiedCount} markdown guide(s) to ${destDocsDir}`);
  } else {
    console.log(`  ℹ️ No markdown guides found for ${comp.name}.`);
  }
}

fs.writeFileSync(path.join(CONTENT_DOCS_DIR, 'sources.json'), JSON.stringify(sources, null, 2) + '\n');

// --- Step C: Automatically Sync Versions from Source Repos ---
console.log('\n🔍 [sync-docs] Extracting component versions from source repositories...');
const versionsFilePath = path.join(ROOT_DIR, 'src', 'data', 'versions.json');
let versionsData = {};
if (fs.existsSync(versionsFilePath)) {
  try {
    versionsData = JSON.parse(fs.readFileSync(versionsFilePath, 'utf8'));
  } catch {
    versionsData = {};
  }
}

// 1. Java Streams (gradle.properties)
const javaGradleProps = path.resolve(ROOT_DIR, '..', 'krabka-streams-java', 'gradle.properties');
if (fs.existsSync(javaGradleProps)) {
  const match = fs.readFileSync(javaGradleProps, 'utf8').match(/^version=(.*)$/m);
  if (match) {
    versionsData['streams-java'] = versionsData['streams-java'] || {};
    versionsData['streams-java'].version = match[1].trim();
    console.log(`  ✓ Detected krabka-streams-java version: ${match[1].trim()}`);
  }
}

// 2. Rust components: the version is the first `version = "..."` of the root Cargo.toml.
const cargoComponents = {
  broker: 'krabka-broker',
  cli: 'krabka-cli',
  'client-rs': 'krabka-client-rs',
  protocol: 'krabka-protocol',
  operator: 'krabka-operator',
  connect: 'krabka-connect',
  gateway: 'krabka-gateway',
  rebalancer: 'krabka-rebalancer',
};
for (const [key, repoName] of Object.entries(cargoComponents)) {
  const cargoToml = path.resolve(ROOT_DIR, '..', repoName, 'Cargo.toml');
  if (!fs.existsSync(cargoToml)) continue;
  const match = fs.readFileSync(cargoToml, 'utf8').match(/^version = "([^"]+)"/m);
  if (match) {
    versionsData[key] = versionsData[key] || {};
    versionsData[key].version = match[1].trim();
    console.log(`  ✓ Detected ${repoName} version: ${match[1].trim()}`);
  }
}

// 3. Go Streams (MODULE.bazel)
const goModuleBazel = path.resolve(ROOT_DIR, '..', 'krabka-streams-go', 'MODULE.bazel');
if (fs.existsSync(goModuleBazel)) {
  const match = fs.readFileSync(goModuleBazel, 'utf8').match(/version = "([^"]+)"/);
  if (match) {
    versionsData['streams-go'] = versionsData['streams-go'] || {};
    versionsData['streams-go'].version = match[1].trim();
    console.log(`  ✓ Detected krabka-streams-go version: ${match[1].trim()}`);
  }
}

// 4. Latest published release tag. Uses the GitHub REST API (GITHUB_TOKEN when set), then `gh`.
// Without a release the entry keeps no `activeRelease` and pages fall back to `version`.
const releaseRepos = {
  'streams-java': 'krabka-streams-java',
  broker: 'krabka-broker',
  cli: 'krabka-cli',
  'streams-go': 'krabka-streams-go',
  connect: 'krabka-connect',
  gateway: 'krabka-gateway',
  rebalancer: 'krabka-rebalancer',
  'client-rs': 'krabka-client-rs',
  protocol: 'krabka-protocol',
};

function semverParts(tag) {
  const m = /^v(\d+)\.(\d+)\.(\d+)$/.exec(tag);
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : null;
}

function highestSemverTag(tags) {
  let best = null;
  for (const tag of tags) {
    const parts = semverParts(tag);
    if (!parts) continue;
    if (!best || parts.some((n, i) => n !== best.parts[i] && n > best.parts[i] && parts.slice(0, i).every((v, j) => v === best.parts[j]))) {
      best = { tag, parts };
    }
  }
  return best?.tag ?? null;
}

async function latestReleaseTag(repoName) {
  const headers = { Accept: 'application/vnd.github+json' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  try {
    const res = await fetch(`https://api.github.com/repos/krabka-io/${repoName}/releases/latest`, {
      headers,
      signal: AbortSignal.timeout(5000),
    });
    if (res.ok) return (await res.json()).tag_name ?? null;
    if (res.status === 404) {
      // Repositories that tag without publishing a GitHub Release.
      const tags = await fetch(`https://api.github.com/repos/krabka-io/${repoName}/tags?per_page=100`, {
        headers,
        signal: AbortSignal.timeout(5000),
      });
      if (tags.ok) return highestSemverTag((await tags.json()).map((t) => t.name));
    }
  } catch {
    // fall through to gh
  }
  try {
    const out = execSync(`gh release view -R krabka-io/${repoName} --json tagName --jq .tagName`, {
      stdio: 'pipe',
      timeout: 5000,
    }).toString().trim();
    if (out) return out;
  } catch {
    // `gh release view` only knows GitHub Releases; fall through to the tags.
  }
  try {
    const out = execSync(`gh api "repos/krabka-io/${repoName}/tags?per_page=100" --jq '.[].name'`, {
      stdio: 'pipe',
      timeout: 5000,
    }).toString();
    return highestSemverTag(out.split('\n').filter(Boolean));
  } catch {
    return null;
  }
}

for (const [key, repoName] of Object.entries(releaseRepos)) {
  const tag = await latestReleaseTag(repoName);
  if (tag && versionsData[key]) {
    versionsData[key].activeRelease = tag;
    console.log(`  ✓ Latest ${repoName} release: ${tag}`);
  } else {
    console.log(`  ℹ️ No release tag resolved for ${repoName}; keeping the recorded value`);
  }
}

fs.writeFileSync(versionsFilePath, JSON.stringify(versionsData, null, 2) + '\n');
console.log('  ✓ Synchronized src/data/versions.json with live repository versions & active release tags.');

console.log('\n✅ [sync-docs] Documentation synchronization complete!\n');
