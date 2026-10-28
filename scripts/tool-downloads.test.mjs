import assert from 'node:assert/strict';
import {
  readFileSync,
  readdirSync,
  mkdirSync,
  mkdtempSync,
  writeFileSync,
  existsSync,
  rmSync,
} from 'node:fs';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { join, delimiter } from 'node:path';
import { fileURLToPath } from 'node:url';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const pins = JSON.parse(read('scripts/tool-pins.json'));

test('Cargo.lock wasm-bindgen version has all supported pinned platforms', () => {
  const version = read('playground/Cargo.lock').match(
    /name = "wasm-bindgen"\nversion = "([^"]+)"/,
  )[1];
  assert.equal(
    pins['wasm-bindgen'].version,
    version,
    'Update scripts/tool-pins.json; see docs/tool-downloads.md',
  );
  assert.deepEqual(Object.keys(pins['wasm-bindgen'].platforms).sort(), [
    'darwin-arm64',
    'darwin-x86_64',
    'linux-arm64',
    'linux-x86_64',
    'windows-x86_64',
  ]);
  for (const tool of Object.values(pins))
    for (const pin of Object.values(tool.platforms)) {
      assert.match(pin.sha256, /^[a-f0-9]{64}$/);
      assert.match(pin.url, /^https:\/\/github.com\//);
      assert.match(tool.provenance, /^https:\/\/api.github.com\/repos\//);
    }
});

test('installer fails closed and reconstructs installs from verified bytes', () => {
  const result = spawnSync('python3', ['scripts/tool-downloads-test.py'], {
    encoding: 'utf8',
    env: { ...process.env, PYTHONDONTWRITEBYTECODE: '1' },
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
});

test('native download entrypoints delegate to the verified installer', () => {
  assert.match(read('playground/build.sh'), /install-tool.py" wasm-bindgen --version/);
  assert.doesNotMatch(read('playground/build.sh'), /command -v wasm-bindgen/);
  assert.match(read('playground/broker-wasi/build.sh'), /source .*scripts\/wasi-sdk-env.sh/);
  assert.match(read('scripts/wasi-sdk-env.sh'), /install-tool.py" wasi-sdk/);
  assert.match(read('.github/workflows/playground.yml'), /source ..\/..\/scripts\/wasi-sdk-env.sh/);
  assert.match(read('.github/workflows/snippets.yml'), /python3 scripts\/install-tool.py kind/);
  // Inventory common downloader syntax, including embedded shell commands and
  // Python/JS helpers. This is a review guard, not a general data-flow analyzer.
  const inventory = {
    'why3-web/build-cvc4-web.sh': 2,
    'why3-web/build-why3-web.sh': 2,
    '.github/workflows/deploy.yml': 1, // compares live WASM, never executes it
    'scripts/install-tool.py': 1,
    'scripts/pages-promotion.mjs': 1, // authenticated metadata only; v8 downloads the artifact
    'scripts/sync-docs.mjs': 2,
    'scripts/check-api-links.mjs': 1,
    'scripts/check-production.mjs': 2,
    'scripts/check-snippets.mjs': 1,
  };
  const observed = {};
  function walk(dir) {
    for (const item of readdirSync(new URL(`../${dir}`, import.meta.url), {
      withFileTypes: true,
    })) {
      const path = `${dir}/${item.name}`;
      if (item.isDirectory()) {
        if (!['.tools', 'target', '__pycache__'].includes(item.name)) walk(path);
      } else if (
        /\.(sh|ya?ml|py|mjs)$/.test(path) &&
        !path.endsWith('.test.mjs') &&
        path !== 'scripts/tool-downloads-test.py'
      ) {
        const source = read(path);
        const calls =
          source.match(
            /\b(?:curl|wget)[ \t]+(?=[-$"'])|\b(?:fetch|urlopen)\s*\(|\bhttps?\.(?:get|request)\s*\(/g,
          ) ?? [];
        if (calls.length) observed[path] = calls.length;
        if (path.startsWith('why3-web/build-') && calls.length) {
          assert.match(source, /sha256sum -c -/);
          const fetches = source.split('\n').filter((line) => /^fetch /.test(line));
          assert.equal(fetches.length, path.includes('cvc4') ? 6 : 4);
          for (const line of fetches)
            assert.match(line, /^fetch "\$\{\w+_URL\}" "\$\{\w+_SHA256\}" /);
        }
      }
    }
  }
  for (const dir of ['scripts', 'playground', '.github', 'why3-web']) walk(dir);
  assert.deepEqual(
    observed,
    inventory,
    'Review new download clients and update the inventory only after verification',
  );
});

test('build caller stops on installer failure without executing PATH or printed tools', () => {
  const parent = fileURLToPath(new URL('../playground/.tools/', import.meta.url));
  mkdirSync(parent, { recursive: true });
  const fixture = mkdtempSync(join(parent, 'caller-failure-'));
  try {
    for (const dir of ['playground', 'scripts', 'bin']) mkdirSync(join(fixture, dir));
    writeFileSync(join(fixture, 'playground/build.sh'), read('playground/build.sh'));
    writeFileSync(
      join(fixture, 'playground/Cargo.lock'),
      '[[package]]\nname = "wasm-bindgen"\nversion = "0.2.129"\n',
    );
    const executable = (name, source) =>
      writeFileSync(join(fixture, 'bin', name), source, { mode: 0o755 });
    executable(
      'cargo',
      '#!/usr/bin/env bash\nif [[ "$1" == build ]]; then touch "$BUILD_SENTINEL"; fi\n',
    );
    executable('rustup', '#!/usr/bin/env bash\nexit 0\n');
    executable(
      'wasm-bindgen',
      '#!/usr/bin/env bash\ntouch "$TOOL_SENTINEL"\necho "wasm-bindgen 0.2.129"\n',
    );
    // A failed installer may print a path before it reports its failure.
    // The caller must respect its exit status, not execute that printed path.
    executable('python3', '#!/usr/bin/env bash\necho "$PRINTED_TOOL"\nexit 42\n');
    const toolSentinel = join(fixture, 'tool-executed');
    const buildSentinel = join(fixture, 'cargo-built');
    const env = {
      ...process.env,
      PATH: join(fixture, 'bin') + delimiter + process.env.PATH,
      PRINTED_TOOL: join(fixture, 'bin/wasm-bindgen'),
      TOOL_SENTINEL: toolSentinel,
      BUILD_SENTINEL: buildSentinel,
      GITHUB_ACTIONS: 'true',
    };
    delete env.WASM_BINDGEN;
    const result = spawnSync('bash', [join(fixture, 'playground/build.sh')], {
      env,
      encoding: 'utf8',
    });
    assert.equal(result.status, 42, result.stdout + result.stderr);
    assert.equal(
      existsSync(toolSentinel),
      false,
      'neither implicit PATH nor printed executable may run',
    );
    assert.equal(
      existsSync(buildSentinel),
      false,
      'cargo build must not run after failed verification',
    );
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});
