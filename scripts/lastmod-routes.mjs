// Explicit inventory: adding an authored route requires review here.
// The final sitemap is reconciled against this map on every build.
import fs from 'node:fs';
import path from 'node:path';
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

const generatedGaps = {
  '/': 'unreviewed-benchmark-and-verification-inputs',
  '/releases': 'release-edit-and-membership-history-unavailable',
  '/versions': 'live-version-snapshot-history-unavailable',
  '/benchmarks': 'benchmark-data-significance-unreviewed',
  '/benchmarks/explorer': 'benchmark-archive-significance-unreviewed',
  '/verification': 'catalog-and-proof-significance-unreviewed',
  '/docs/verification-playground': 'catalog-and-proof-significance-unreviewed',
  '/docs/proof-explorer': 'catalog-and-proof-significance-unreviewed',
  '/docs/operator/crds': 'crd-and-shared-input-significance-unreviewed',
};
// Known non-import inputs. Patterns describe incomplete families, not dated
// dependencies; reviewed records need exact files instead of these selectors.
const verificationInputs = ['src/content/docs/broker/verification.md', 'src/data/proof-sessions.json', 'src/data/verified-kernels.json', 'src/data/stateright-models.json', 'public/proofs/coma/**'];
const benchmarkInputs = ['benchmarks/latest-openmessaging.md', 'benchmarks/openmessaging/**/provenance.json', 'benchmarks/openmessaging/**/trials.json'];
const generatedInputs = {
  '/': [...verificationInputs, ...benchmarkInputs],
  '/releases': ['src/data/versions.json', 'src/data/ecosystem-versions.json', 'external:GitHub-release-collection-and-edit-history'],
  '/versions': ['src/data/ecosystem-versions.json', 'external:GitHub-repositories-tags-releases-and-comparisons'],
  '/benchmarks': benchmarkInputs,
  '/benchmarks/explorer': ['src/data/benchmark-archive.json', 'public/benchmarks/archive-data/**', 'benchmarks/archive/*.tar.gz'],
  '/verification': verificationInputs,
  '/docs/verification-playground': verificationInputs,
  '/docs/proof-explorer': [...verificationInputs, 'public/proofs/**', 'public/why3-web/**'],
  '/docs/operator/crds': ['src/content/docs/operator/crds.json'],
};

const guideId = (file) => file.replace(/\.md$/i, '').split('/').map((part) => slug(part)).join('/');

export function routeInputs(root, url) {
  const route = new URL(url).pathname.replace(/\/$/, '') || '/';
  if (authoredRoutes[route]) {
    const source = authoredRoutes[route];
    let imports = [source];
    try { imports = [...importedInputs(root, source)]; } catch { /* An omitted route can lack generated build inputs. */ }
    return { family: generatedInputs[route] ? 'data-driven' : 'authored', source,
      inputs: [...new Set([...imports, ...(generatedInputs[route] ?? [])])].sort(), gap: generatedGaps[route] };
  }
  const provenanceFile = path.join(root, 'src/content/docs/lastmod-provenance.json');
  const provenance = fs.existsSync(provenanceFile) ? JSON.parse(fs.readFileSync(provenanceFile, 'utf8')) : { components: {} };
  for (const [component, data] of Object.entries(provenance.components)) {
    for (const [destination, record] of Object.entries(data.files)) {
      if (route !== `/docs/${component}/${guideId(destination)}`) continue;
      const content = `src/content/docs/${component}/${destination}`;
      return { family: 'synced-guide', source: 'src/pages/docs/[...slug].astro',
        inputs: [content, 'src/pages/docs/[...slug].astro', 'scripts/rewrite-doc-links.mjs'], provenance: [record],
        gap: record.reason ?? (!record.historyComplete ? 'incomplete-upstream-history' : 'guide-significance-and-render-dependencies-unreviewed') };
    }
  }
  if (/^\/docs\/operator\/crds\//.test(route)) return { family: 'generated-crd', source: 'src/pages/docs/operator/crds/[kind].astro',
    inputs: ['src/pages/docs/operator/crds/[kind].astro', 'src/content/docs/operator/crds.json', 'src/utils/operator-crds.mjs'],
    provenance: Object.values(provenance.components.operator?.crds ?? {}), gap: 'crd-and-shared-input-significance-unreviewed' };
  return null;
}
