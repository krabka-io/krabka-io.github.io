// Explicit inventory: adding an authored route requires review here.
// The final sitemap is reconciled against this map on every build.
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { kindSlug } from '../src/utils/operator-crds.mjs';
import { slug } from 'github-slugger';
import { importedInputs } from './lastmod-provenance.mjs';

export const authoredRoutes = {
  "/404": "src/pages/404.astro",
  "/api": "src/pages/api/index.astro",
  "/benchmarks/explorer": "src/pages/benchmarks/explorer.astro",
  "/benchmarks": "src/pages/benchmarks.astro",
  "/brand": "src/pages/brand.astro",
  "/docs/api": "src/pages/docs/api.astro",
  "/docs/architecture": "src/pages/docs/architecture.astro",
  "/docs/broker": "src/pages/docs/broker.astro",
  "/docs/browser-tools": "src/pages/docs/browser-tools.astro",
  "/docs/cli": "src/pages/docs/cli.astro",
  "/docs/connect-postgres": "src/pages/docs/connect-postgres.astro",
  "/docs/data-formats": "src/pages/docs/data-formats.astro",
  "/docs/gateway": "src/pages/docs/gateway.astro",
  "/docs/helm-charts": "src/pages/docs/helm-charts.astro",
  "/docs": "src/pages/docs/index.astro",
  "/docs/kip-matrix": "src/pages/docs/kip-matrix.md",
  "/docs/lab": "src/pages/docs/lab.astro",
  "/docs/migration": "src/pages/docs/migration.astro",
  "/docs/operations": "src/pages/docs/operations.astro",
  "/docs/operator/crds": "src/pages/docs/operator/crds.astro",
  "/docs/operator": "src/pages/docs/operator.astro",
  "/docs/playground": "src/pages/docs/playground.astro",
  "/docs/proof-explorer": "src/pages/docs/proof-explorer.astro",
  "/docs/qualification": "src/pages/docs/qualification.astro",
  "/docs/quickstart": "src/pages/docs/quickstart.astro",
  "/docs/rebalancer": "src/pages/docs/rebalancer.astro",
  "/docs/runtime-architecture": "src/pages/docs/runtime-architecture.astro",
  "/docs/streams-go": "src/pages/docs/streams-go.astro",
  "/docs/streams-java": "src/pages/docs/streams-java.astro",
  "/docs/streams-rs": "src/pages/docs/streams-rs.astro",
  "/docs/verification-playground": "src/pages/docs/verification-playground.astro",
  "/features/connect": "src/pages/features/connect.astro",
  "/features": "src/pages/features/index.astro",
  "/features/kraft": "src/pages/features/kraft.astro",
  "/features/operator": "src/pages/features/operator.astro",
  "/features/streams": "src/pages/features/streams.astro",
  "/features/tiered-storage": "src/pages/features/tiered-storage.astro",
  "/get-started": "src/pages/get-started.astro",
  "/": "src/pages/index.astro",
  "/news": "src/pages/news/index.astro",
  "/news/krabka-1-0-0": "src/pages/news/krabka-1-0-0.md",
  "/news/krabka-1-0-3": "src/pages/news/krabka-1-0-3.md",
  "/news/krabka-1-0-5": "src/pages/news/krabka-1-0-5.md",
  "/releases": "src/pages/releases.astro",
  "/search": "src/pages/search.astro",
  "/verification": "src/pages/verification.astro",
  "/versions": "src/pages/versions.astro"
};

// File-read/generated dependencies are explicit; generated snapshots are replaced
// with tracked source files or upstream per-file provenance, not build timestamps.
const benchmarkInputs = ['benchmarks/latest-openmessaging.md', 'benchmarks/openmessaging/**/provenance.json', 'benchmarks/openmessaging/**/trials.json', 'benchmarks/openmessaging/**/summary.md', 'benchmarks/openmessaging/**/intervals.json'];
const generatedInputs = {
  '/': ['src/pages/get-started.astro', ...benchmarkInputs],
  '/benchmarks': benchmarkInputs,
  '/benchmarks/explorer': ['benchmarks/archive/README.md', 'benchmarks/archive/*.tar.gz'],
  '/docs/lab': ['public/playground/lab/**', 'public/playground/*.html'],
  '/docs/proof-explorer': ['public/proofs/*.js', 'scripts/sync-proofs.mjs', 'src/data/verified-kernels.json', 'src/data/stateright-models.json'],
};
const generatedSnapshots = new Set(['src/data/benchmark-archive.json', 'src/data/proof-sessions.json', 'src/content/docs/operator/crds.json']);
const catalogRoutes = new Set(['/', '/verification', '/docs/verification-playground', '/docs/proof-explorer']);
const guideId = (file) => file.replace(/\.md$/i, '').split('/').map((part) => slug(part)).join('/');
const readJSON = (root, file, fallback) => fs.existsSync(path.join(root, file)) ? JSON.parse(fs.readFileSync(path.join(root, file), 'utf8')) : fallback;

function expandInputs(root, selectors) {
  return selectors.flatMap((selector) => {
    if (!selector.includes('*')) return [selector];
    // Include untracked (non-ignored) files too, so additions invalidate dates
    // until committed. Generated ignored output is not a content source.
    try {
      return execFileSync('git', ['-C', root, 'ls-files', '--cached', '--others', '--exclude-standard', '-z', '--', selector], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).split('\0').filter(Boolean);
    } catch {
      // Source archives still build: the resolver reports non-Git history and
      // omits dates before attempting to query these diagnostic selectors.
      return [selector];
    }
  });
}

export function routeInputs(root, url) {
  const route = new URL(url).pathname.replace(/\/$/, '') || '/';
  const provenance = readJSON(root, 'src/content/docs/lastmod-provenance.json', { components: {} });
  const crds = provenance.components.operator?.crds ?? {};
  if (route === '/docs/operator/crds' || route.startsWith('/docs/operator/crds/')) {
    const index = route === '/docs/operator/crds';
    const source = index ? authoredRoutes[route] : 'src/pages/docs/operator/crds/[kind].astro';
    const snapshot = readJSON(root, 'src/content/docs/operator/crds.json', { resources: [] });
    const resource = snapshot.resources.find((item) => route === `/docs/operator/crds/${kindSlug(item)}`);
    if (!index && !resource) return null;
    return { family: 'generated-crd', source,
      inputs: [...importedInputs(root, source)].filter((file) => !generatedSnapshots.has(file)).sort(),
      provenance: index ? Object.values(crds) : [crds[resource._sourcePath]],
      ...(!Object.keys(crds).length ? { gap: 'missing-crd-provenance' } : {}) };
  }
  if (authoredRoutes[route]) {
    const source = authoredRoutes[route];
    const imports = [...importedInputs(root, source)].filter((file) => !generatedSnapshots.has(file));
    const readsBenchmarks = imports.includes('src/utils/benchmarks.mjs');
    return { family: generatedInputs[route] || catalogRoutes.has(route) ? 'data-driven' : 'authored', source,
      inputs: [...new Set([...imports, ...expandInputs(root, [...(generatedInputs[route] ?? []), ...(readsBenchmarks ? benchmarkInputs : [])])])].sort(),
      provenance: catalogRoutes.has(route) ? [provenance.components.broker?.files?.['verification.md']] : [],
      ...(['/releases', '/versions'].includes(route) ? { gap: 'unpinned-external-api-history' } : {}) };
  }
  for (const [component, data] of Object.entries(provenance.components)) {
    for (const [destination, record] of Object.entries(data.files)) {
      if (route !== `/docs/${component}/${guideId(destination)}`) continue;
      return { family: 'synced-guide', source: 'src/pages/docs/[...slug].astro', inputs: [], provenance: [record] };
    }
  }
  return null;
}
