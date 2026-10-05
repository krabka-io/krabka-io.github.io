import { execSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { rewriteDocLinks } from './rewrite-doc-links.mjs';
import { load } from 'js-yaml';

const ROOT_DIR = process.cwd();
const CONTENT_DOCS_DIR = path.join(ROOT_DIR, 'src', 'content', 'docs');

// Component configurations
export const COMPONENTS = [
  {
    name: 'streams-java',
    repo: 'krabka-streams-java',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-java'),
    docsSubdir: 'streams-java',
    required: ['getting-started.md', 'configuration.md'],
  },
  {
    name: 'streams-go',
    repo: 'krabka-streams-go',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-go'),
    docsSubdir: 'streams-go',
    required: ['getting-started.md', 'configuration.md'],
  },
  {
    name: 'broker',
    repo: 'krabka-broker',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-broker'),
    docsSubdir: 'broker',
    required: ['verification.md', 'config-reference.md', 'operations/backup-restore.md', 'operations/deploy.md', 'operations/runbooks/restore-from-archive.md', 'operations/metrics.md', 'operations/scaling.md'],
  },
  {
    name: 'operator', repo: 'krabka-operator', docsSubdir: 'operator',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-operator'),
    guides: [{ source: 'README.md', destination: 'reference.md' }],
    crdsDir: 'charts/krabka-operator/crds',
    requiredKinds: ['Kafka', 'KafkaNodePool', 'KafkaTopic', 'KafkaUser', 'KafkaConnector', 'KafkaRebalance', 'SchemaRegistry', 'KafkaGrpcGateway'],
  },
  {
    name: 'rebalancer', repo: 'krabka-rebalancer', docsSubdir: 'rebalancer',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-rebalancer'),
    guides: [
      { source: 'crates/rebalancer/README.md', destination: 'reference.md' },
      { source: 'crates/rebalancer/tests/broker/README.md', destination: 'broker-tests.md' },
    ],
  },
  {
    name: 'gateway', repo: 'krabka-gateway', docsSubdir: 'gateway',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-gateway'),
    guides: [
      { source: 'README.md', destination: 'reference.md' },
      { source: 'demo/gitlab/README.md', destination: 'gitlab-ingestion.md' },
      { source: 'demo/cloudevents/README.md', destination: 'cloudevents.md' },
      { source: 'demo/github-firehose/README.md', destination: 'github-firehose.md' },
    ],
  },
];

// Which commit each component's guides came from, shown in the footer of every
// synced page. The guides follow the default branch, not the latest release.
function markdownFiles(dir, prefix = '') {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const relative = path.posix.join(prefix, entry.name);
    // Publish the existing top-level guides and the operations/runbook tree.
    // Contributor style guides and design proposals remain in their repository.
    if (entry.isDirectory()) return prefix || entry.name === 'operations' ? markdownFiles(path.join(dir, entry.name), relative) : [];
    return entry.isFile() && /\.md$/i.test(entry.name) && relative.toLowerCase() !== 'index.md' ? [relative] : [];
  });
}

// Publish only after every required component has been fetched and validated.
// A failed fetch or missing guide leaves the previous content and provenance intact.
export function syncGuides(components = COMPONENTS, contentDir = CONTENT_DOCS_DIR) {
  fs.mkdirSync(path.dirname(contentDir), { recursive: true });
  const staged = fs.mkdtempSync(path.join(path.dirname(contentDir), '.docs-stage-'));
  const clones = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-docs-'));
  const backup = staged + '-previous';
  const sources = {};
  try {
    if (fs.existsSync(contentDir)) fs.cpSync(contentDir, staged, { recursive: true });
    for (const comp of components) {
      console.log(`\n📦 Processing component: ${comp.name} (${comp.repo})`);
      const destDocsDir = path.join(staged, comp.docsSubdir);
      fs.rmSync(destDocsDir, { recursive: true, force: true });
      fs.mkdirSync(destDocsDir, { recursive: true });
      let sourceRepoDir = comp.localRepoDir;
      if (!fs.existsSync(path.join(sourceRepoDir, comp.guides?.[0]?.source ?? 'docs'))) {
        const tempCloneDir = path.join(clones, comp.repo);
        console.log(`  → Fetching docs from GitHub (krabka-io/${comp.repo})...`);
        execFileSync('git', ['clone', '--depth', '1', '--filter=blob:none', '--sparse', `https://github.com/krabka-io/${comp.repo}.git`, tempCloneDir], { stdio: 'pipe', timeout: 120000 });
        const directories = comp.guides ? [...new Set(comp.guides.map(({ source }) => path.posix.dirname(source)).filter((dir) => dir !== '.'))] : ['docs'];
        if (comp.crdsDir) directories.push(comp.crdsDir);
        if (directories.length) execFileSync('git', ['-C', tempCloneDir, 'sparse-checkout', 'set', ...directories], { stdio: 'pipe', timeout: 120000 });
        sourceRepoDir = tempCloneDir;
      }
      const sourceDocsDir = path.join(sourceRepoDir, 'docs');
      if (!comp.guides && !fs.existsSync(sourceDocsDir)) throw new Error(`No documentation directory for ${comp.name}`);
      try {
        const [commit, date] = execFileSync('git', ['-C', sourceRepoDir, 'log', '-1', '--format=%H%x09%cI'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim().split('\t');
        const branch = execFileSync('git', ['-C', sourceRepoDir, 'rev-parse', '--abbrev-ref', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
        sources[comp.docsSubdir] = { repo: comp.repo, branch, commit, date };
      } catch {
        // Non-git local documentation has no commit provenance.
      }
      const guides = comp.guides ?? markdownFiles(sourceDocsDir).map((file) => ({ source: `docs/${file}`, destination: file }));
      const files = guides.map(({ destination }) => destination);
      if (!files.length) throw new Error(`No markdown guides found for ${comp.name}`);
      for (const required of comp.required ?? []) {
        const file = files.find((file) => file.toLowerCase() === required.toLowerCase());
        if (!file || !fs.readFileSync(path.join(sourceRepoDir, guides.find(({ destination }) => destination === file).source), 'utf8').trim()) throw new Error(`${comp.name}: required guide ${required} is missing or empty`);
      }
      if (new Set(files.map((file) => file.toLowerCase())).size !== files.length) throw new Error(`${comp.name}: duplicate documentation paths after case normalization`);
      const publishedFiles = new Set(files.map((file) => file.toLowerCase()));
      const publishedPaths = new Map(guides.map(({ source, destination }) => [source.toLowerCase(), destination]));
      if (sources[comp.docsSubdir]) sources[comp.docsSubdir].files = Object.fromEntries(guides.map(({ source, destination }) => [destination, source]));
      for (const { source, destination: file } of guides) {
        const srcFile = path.join(sourceRepoDir, source);
        const destFile = path.join(destDocsDir, file);
        let content = fs.readFileSync(srcFile, 'utf8');
        if (!content.trim()) throw new Error(`${comp.name}: required guide ${source} is empty`);

        // Rewrite relative markdown links and code links to prevent 404 errors
        content = rewriteDocLinks(content, { docsSubdir: comp.docsSubdir, repo: comp.repo, sourceFile: file, sourcePath: comp.guides ? source : undefined, sourceRef: sources[comp.docsSubdir]?.commit ?? 'main', publishedFiles, publishedPaths });

        fs.mkdirSync(path.dirname(destFile), { recursive: true });
        fs.writeFileSync(destFile, content);
      }
      if (comp.crdsDir) {
        const crdsDir = path.join(sourceRepoDir, comp.crdsDir);
        const resources = fs.readdirSync(crdsDir).filter((file) => /\.ya?ml$/i.test(file)).sort().map((file) => {
          const crd = load(fs.readFileSync(path.join(crdsDir, file), 'utf8'));
          if (crd?.kind !== 'CustomResourceDefinition' || !crd.spec?.names?.kind || !crd.spec?.versions?.length || crd.spec.versions.some((version) => !version.schema?.openAPIV3Schema)) throw new Error(`${comp.name}: invalid CRD ${file}`);
          return { ...crd, _sourcePath: `${comp.crdsDir}/${file}` };
        });
        for (const kind of comp.requiredKinds ?? []) if (!resources.some((crd) => crd.spec.names.kind === kind)) throw new Error(`${comp.name}: required CRD ${kind} is missing`);
        if (new Set(resources.map((crd) => crd.spec.names.kind)).size !== resources.length) throw new Error(`${comp.name}: duplicate CRD kind`);
        fs.writeFileSync(path.join(destDocsDir, 'crds.json'), JSON.stringify({ resources, source: { repo: comp.repo, path: comp.crdsDir, ...sources[comp.docsSubdir] } }, null, 2) + '\n');
      }
      console.log(`  ✓ Synced and transformed ${files.length} markdown guide(s) to ${destDocsDir}`);
    }
    fs.writeFileSync(path.join(staged, 'sources.json'), JSON.stringify(sources, null, 2) + '\n');
    if (fs.existsSync(contentDir)) fs.renameSync(contentDir, backup);
    try {
      fs.renameSync(staged, contentDir);
    } catch (error) {
      if (fs.existsSync(backup)) fs.renameSync(backup, contentDir);
      throw error;
    }
    fs.rmSync(backup, { recursive: true, force: true });
  } finally {
    fs.rmSync(staged, { recursive: true, force: true });
    fs.rmSync(clones, { recursive: true, force: true });
  }
}

async function main() {
  console.log('🦀 [sync-docs] Starting documentation synchronization...');
  syncGuides();

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
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
