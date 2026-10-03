// Stage the pinned upstream browser distributions without a JS bundler.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

const [z3, cvc5, bundle] = process.argv.slice(2);
for (const name of ['z3-built.js', 'z3-built.wasm']) {
  fs.copyFileSync(path.join(z3, 'build', name), path.join(bundle, name));
}
fs.copyFileSync(path.join(z3, 'build/low-level/wrapper.__GENERATED__.js'), path.join(bundle, 'z3-api.js'));
fs.copyFileSync(path.join(z3, 'LICENSE.txt'), path.join(bundle, 'LICENSES/z3.LICENSE.txt'));
fs.copyFileSync(path.join(cvc5, 'cvc5.wasm'), path.join(bundle, 'cvc5.wasm'));

// The upstream release targets window only. Its runtime also supports workers;
// switch its environment flags and corresponding guard, leaving WASM unchanged.
const source = fs.readFileSync(path.join(cvc5, 'cvc5.js'), 'utf8');
const flags = 'var ENVIRONMENT_IS_WEB=true;var ENVIRONMENT_IS_WORKER=false;';
assert.equal(source.split(flags).length, 2, 'cvc5 release environment flags changed');
const guard = 'assert(!ENVIRONMENT_IS_WORKER,"worker environment detected but not enabled at build time.  Add `worker` to `-sENVIRONMENT` to enable.");';
assert.equal(source.split(guard).length, 2, 'cvc5 release environment guard changed');
fs.writeFileSync(path.join(bundle, 'cvc5.js'), source
  .replace(flags, 'var ENVIRONMENT_IS_WEB=false;var ENVIRONMENT_IS_WORKER=true;')
  .replace(guard, ''));
