// Filesystem side of the verification data: reads the synced catalog and the
// two data files, then hands them to the pure join in `verification-data.ts`.
//
// The catalog is `src/content/docs/broker/verification.md`, which
// `scripts/sync-docs.mjs` writes before every build. When it is absent (a
// checkout that has not synced) the pages still build, from the data files
// alone, and say that the catalog rows are missing.

import fs from 'node:fs';
import path from 'node:path';

import kernelSpecs from '../data/verified-kernels.json';
import modelSpecs from '../data/stateright-models.json';
import { buildVerificationData, type ExplorerSpecInput, type ModelSpecInput, type VerificationData } from './verification-data.ts';

export const CATALOG_FILE = path.join('src', 'content', 'docs', 'broker', 'verification.md');

export function readCatalogMarkdown(root: string = process.cwd()): string | null {
  const file = path.resolve(root, CATALOG_FILE);
  try {
    return fs.readFileSync(file, 'utf8');
  } catch {
    return null;
  }
}

let cached: VerificationData | null = null;

export function loadVerificationData(): VerificationData {
  if (cached) return cached;
  const markdown = readCatalogMarkdown();
  if (markdown === null) {
    console.warn(`[verification] ${CATALOG_FILE} is missing; run \`npm run sync-docs\`. Rendering without the catalog rows.`);
  }
  cached = buildVerificationData(markdown, kernelSpecs as ExplorerSpecInput[], modelSpecs as ModelSpecInput[]);
  return cached;
}
