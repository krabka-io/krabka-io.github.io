import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function composition(entries) {
  const components = [
    {
      id: 'site',
      site_url: 'https://krabka.io/',
      repo: 'krabka-io/krabka-io.github.io',
      render_pages: ['index.html'],
    },
  ];
  const ids = new Set(['site']);
  const mounts = new Set(['/']);
  for (const entry of entries) {
    if (!/^[a-z0-9-]+$/.test(entry.id) || ids.has(entry.id)) {
      throw new Error(`Invalid or duplicate component id: ${entry.id}`);
    }
    if (!/^krabka-[a-z0-9-]+$/.test(entry.repo)) {
      throw new Error(`Invalid component repo: ${entry.repo}`);
    }
    // apiPath can point inside the deployed archive (Java uses api/index.html).
    const mount = `/${entry.repo}/`;
    if (mounts.has(mount) || !entry.apiPath?.startsWith(mount)) {
      throw new Error(`Invalid or duplicate archive mount: ${entry.apiPath}`);
    }
    const suffix = entry.apiPath.slice(mount.length);
    if (suffix && !/^(?:[a-z0-9-]+\/)+$/.test(suffix)) {
      throw new Error(`Invalid API entry path: ${entry.apiPath}`);
    }
    const extraRenderPages = entry.extraRenderPages ?? [];
    if (
      !Array.isArray(extraRenderPages) ||
      extraRenderPages.some(
        (page) =>
          typeof page !== 'string' || !/^(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_-]+\.html$/.test(page),
      )
    ) {
      throw new Error(`Invalid extra render pages: ${entry.id}`);
    }
    ids.add(entry.id);
    mounts.add(mount);
    components.push({
      id: entry.id,
      site_url: `https://krabka.io${mount}`,
      repo: `krabka-io/${entry.repo}`,
      render_pages: [`${suffix}index.html`, ...extraRenderPages],
    });
  }
  return { schema_version: 1, components };
}

export function checkLock(expected, lock) {
  if (lock.schema_version !== 1 || !Array.isArray(lock.components)) {
    throw new Error('Unsupported composition lock schema');
  }
  const identity = ({ id, site_url, repo, render_pages }) => ({ id, site_url, repo, render_pages });
  const ordered = (entries) => entries.map(identity).sort((a, b) => a.id.localeCompare(b.id));
  if (JSON.stringify(ordered(expected.components)) !== JSON.stringify(ordered(lock.components))) {
    throw new Error(
      'Lock component identities, mounts, or rendered pages differ from the site catalog',
    );
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const entries = JSON.parse(
    fs.readFileSync(new URL('../src/data/api-reference.json', import.meta.url)),
  );
  const desired = composition(entries);
  if (process.argv[2] === '--check-lock' && process.argv.length === 4) {
    checkLock(desired, JSON.parse(fs.readFileSync(process.argv[3], 'utf8')));
    console.error('Archive lock matches the site component catalog');
  } else if (process.argv.length === 2) {
    process.stdout.write(`${JSON.stringify(desired, null, 2)}\n`);
  } else {
    throw new Error('Usage: node scripts/site-archives.mjs [--check-lock lock.json]');
  }
}
