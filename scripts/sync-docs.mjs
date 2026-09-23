import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const ROOT_DIR = process.cwd();
const CONTENT_DOCS_DIR = path.join(ROOT_DIR, 'src', 'content', 'docs');
const PUBLIC_API_DIR = path.join(ROOT_DIR, 'public', 'api');

console.log('🦀 [sync-docs] Starting documentation synchronization...');

// 1. Ensure target directories exist
fs.mkdirSync(CONTENT_DOCS_DIR, { recursive: true });
fs.mkdirSync(PUBLIC_API_DIR, { recursive: true });

// Component configurations
const COMPONENTS = [
  {
    name: 'streams-java',
    repo: 'krabka-streams-java',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-java'),
    docsSubdir: 'streams-java',
    assetName: 'javadoc-site.tar.gz',
    apiOutputDir: path.join(PUBLIC_API_DIR, 'streams-java'),
    localApiBuildDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-java', 'bazel-bin', 'javadoc-site'),
    docType: 'Javadoc',
  },
  {
    name: 'streams-go',
    repo: 'krabka-streams-go',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-go'),
    docsSubdir: 'streams-go',
    assetName: 'docsite-site.tar.gz',
    apiOutputDir: path.join(PUBLIC_API_DIR, 'streams-go'),
    localApiBuildDir: path.resolve(ROOT_DIR, '..', 'krabka-streams-go', 'bazel-bin', 'docsite-site'),
    docType: 'Godoc',
  },
  {
    name: 'broker',
    repo: 'krabka-broker',
    localRepoDir: path.resolve(ROOT_DIR, '..', 'krabka-broker'),
    docsSubdir: 'broker',
    assetName: 'rustdoc-site.tar.gz',
    apiOutputDir: path.join(PUBLIC_API_DIR, 'broker'),
    localApiBuildDir: path.resolve(ROOT_DIR, '..', 'krabka-broker', 'target', 'doc'),
    docType: 'Rustdoc',
  },
];

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
    const files = fs.readdirSync(sourceDocsDir);
    let copiedCount = 0;
    for (const file of files) {
      // Exclude index.md so it doesn't collide with the root /docs/<module>.astro page
      if (file.endsWith('.md') && file !== 'index.md') {
        const srcFile = path.join(sourceDocsDir, file);
        const destFile = path.join(destDocsDir, file);
        let content = fs.readFileSync(srcFile, 'utf8');

        // Rewrite relative markdown links and code links to prevent 404 errors
        content = content.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (match, label, target) => {
          // Ignore absolute URLs, anchors, and protocols
          if (target.startsWith('http://') || target.startsWith('https://') || target.startsWith('mailto:') || target.startsWith('#')) {
            return match;
          }

          const [rawPath, anchor] = target.split('#');
          const anchorSuffix = anchor ? `#${anchor}` : '';

          // 1. Link to sibling markdown guide in the same docs collection
          if (rawPath.endsWith('.md') && !rawPath.includes('/')) {
            const slug = rawPath.replace(/\.md$/, '');
            const destUrl = slug === 'index' ? `/docs/${comp.docsSubdir}` : `/docs/${comp.docsSubdir}/${slug}`;
            return `[${label}](${destUrl}${anchorSuffix})`;
          }

          // 2. Relative link to repository code/files (crates/, tests/, examples/, root docs, etc.)
          const cleanPath = rawPath.replace(/^(\.\.\/)+/, '').replace(/^\.\//, '');
          const isDocsSibling = fs.existsSync(path.join(sourceDocsDir, cleanPath));
          const repoPath = isDocsSibling ? `docs/${cleanPath}` : cleanPath;
          const githubUrl = `https://github.com/krabka-io/${comp.repo}/blob/main/${repoPath}${anchorSuffix}`;

          return `[${label}](${githubUrl})`;
        });

        fs.writeFileSync(destFile, content);
        copiedCount++;
      }
    }
    console.log(`  ✓ Synced and transformed ${copiedCount} markdown guide(s) to ${destDocsDir}`);
  } else {
    console.log(`  ℹ️ No markdown guides found for ${comp.name}.`);
  }

  // --- Step B: Sync Compiled API Reference ---
  const latestApiDir = path.join(comp.apiOutputDir, 'latest');
  let apiSynced = false;

  // 1. Try local Bazel build output if present
  if (comp.localApiBuildDir && fs.existsSync(comp.localApiBuildDir)) {
    console.log(`  ✓ Found local build output at ${comp.localApiBuildDir}`);
    execSync(`rm -rf "${latestApiDir}" && mkdir -p "${latestApiDir}" && cp -RL "${comp.localApiBuildDir}/." "${latestApiDir}/" && chmod -R u+w "${latestApiDir}"`);
    if (comp.name === 'broker' && !fs.existsSync(path.join(latestApiDir, 'index.html'))) {
      fs.writeFileSync(
        path.join(latestApiDir, 'index.html'),
        '<!doctype html><html><head><meta http-equiv="refresh" content="0; url=krabka_broker/index.html"><title>Redirecting to krabka_broker</title></head><body><a href="krabka_broker/index.html">Redirecting to krabka_broker...</a></body></html>'
      );
    }
    apiSynced = true;
    console.log(`  ✓ Synced local API build to ${latestApiDir}`);
  } else {
    fs.mkdirSync(latestApiDir, { recursive: true });
  }

  // 2. Try downloading latest release asset via GitHub CLI
  if (!apiSynced) {
    const downloadDir = path.join('/tmp', `api-download-${comp.name}`);
    fs.mkdirSync(downloadDir, { recursive: true });
    try {
      console.log(`  → Checking GitHub Releases for ${comp.assetName}...`);
      execSync(`gh release download --repo krabka-io/${comp.repo} -p "${comp.assetName}" -D ${downloadDir} --clobber`, {
        stdio: 'pipe',
        timeout: 15000,
      });
      const archivePath = path.join(downloadDir, comp.assetName);
      if (fs.existsSync(archivePath)) {
        execSync(`tar -xzf ${archivePath} -C ${latestApiDir}`, { stdio: 'pipe' });
        apiSynced = true;
        console.log(`  ✓ Unpacked release API archive into ${latestApiDir}`);
      }
    } catch {
      // Release asset not yet published or gh not available
    }
  }

  // 3. Fallback placeholder if no build or release archive exists yet
  if (!apiSynced && !fs.existsSync(path.join(latestApiDir, 'index.html'))) {
    console.log(`  ℹ️ Creating initial placeholder at ${latestApiDir}/index.html`);
    const placeholderHtml = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>${comp.name} ${comp.docType} Reference</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { background: #030712; color: #f3f4f6; font-family: ui-sans-serif, system-ui, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 1.5rem; }
    .card { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.1); border-radius: 0.75rem; padding: 2rem; max-width: 480px; width: 100%; text-align: left; }
    h1 { color: #f3f4f6; margin-top: 0; font-size: 1.25rem; font-weight: 700; }
    p { color: #9ca3af; font-size: 0.875rem; line-height: 1.6; }
    code { font-family: ui-monospace, monospace; color: #ff6a3d; background: rgba(255,255,255,0.05); padding: 0.15rem 0.35rem; border-radius: 0.25rem; }
    .nav-links { margin-top: 1.5rem; pt-4; border-top: 1px solid rgba(255,255,255,0.1); display: flex; flex-direction: column; gap: 0.5rem; }
    a { color: #ff6a3d; text-decoration: none; font-size: 0.875rem; font-weight: 600; }
    a:hover { text-decoration: underline; color: #ff8c69; }
    .muted { color: #6b7280; font-size: 0.75rem; margin-top: 1rem; }
  </style>
</head>
<body>
  <div class="card">
    <h1>${comp.name} · ${comp.docType} Reference</h1>
    <p>The compiler-generated ${comp.docType} tree is published upon official release tagging (<code>v*</code>) and local builds.</p>
    <div class="nav-links">
      <a href="../../../docs/${comp.docsSubdir}">Return to ${comp.name} Overview & Guides</a>
      <a href="../../../api">Browse All API References</a>
      <a href="https://github.com/krabka-io/${comp.repo}" target="_blank" rel="noopener noreferrer">View Source Repository on GitHub</a>
    </div>
    <div class="muted">Krabka Documentation Hub</div>
  </div>
</body>
</html>`;
    fs.writeFileSync(path.join(latestApiDir, 'index.html'), placeholderHtml);
  }
}

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

// 2. Broker & CLI (Cargo.toml)
const brokerCargoToml = path.resolve(ROOT_DIR, '..', 'krabka-broker', 'Cargo.toml');
if (fs.existsSync(brokerCargoToml)) {
  const match = fs.readFileSync(brokerCargoToml, 'utf8').match(/^version = "([^"]+)"/m);
  if (match) {
    versionsData['broker'] = versionsData['broker'] || {};
    versionsData['broker'].version = match[1].trim();
    versionsData['cli'] = versionsData['cli'] || {};
    versionsData['cli'].version = match[1].trim();
    console.log(`  ✓ Detected krabka-broker & cli version: ${match[1].trim()}`);
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

// 4. Extract active official release tags
const activeReleaseTags = {
  'streams-java': 'v1.1.0',
  'broker': 'v0.5.3',
  'streams-go': 'v0.1.0-dev',
};

for (const [key, defaultTag] of Object.entries(activeReleaseTags)) {
  let tag = defaultTag;
  try {
    const repoName = key === 'broker' ? 'krabka-broker' : (key === 'streams-java' ? 'krabka-streams-java' : 'krabka-streams-go');
    const out = execSync(`gh release view -R krabka-io/${repoName} --json tagName --jq .tagName`, { stdio: 'pipe', timeout: 5000 }).toString().trim();
    if (out) tag = out;
  } catch {
    // Keep default pinned tag
  }
  if (versionsData[key]) {
    versionsData[key].activeRelease = tag;
  }
}

fs.writeFileSync(versionsFilePath, JSON.stringify(versionsData, null, 2) + '\n');
console.log('  ✓ Synchronized src/data/versions.json with live repository versions & active release tags.');

console.log('\n✅ [sync-docs] Documentation synchronization complete!\n');
