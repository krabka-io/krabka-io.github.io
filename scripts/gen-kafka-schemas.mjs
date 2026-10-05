// Generates public/playground/lab/kafka-schemas.json, the message schemas the
// Cluster Lab's protocol analyzer decodes frames and log records with.
//
//   node scripts/gen-kafka-schemas.mjs [--ref origin/HEAD]
//
// Every input comes from the latest revision of the sibling repositories:
//   krabka-protocol  crates/protocol/schemas/*.json (the Kafka JSON message
//                    schemas its codecs are generated from) and the metadata
//                    record apiKeys of records/metadata/record.rs
//   krabka-broker    the Apache Kafka error table it pins, and the apiKeys of
//                    its private controller RPCs (crates/raft/src/wire.rs)
// It reads a checkout beside this repository (beside the main checkout when
// run from a git worktree), fetches it and reads `--ref` (default
// origin/HEAD) without touching the working tree; with no checkout it makes a
// shallow clone of the default branch. The revisions read are recorded in the
// output's `sources`.
//
// The output keeps what decoding needs (types, version ranges, tags, nesting)
// and each field's `about`, which the analyzer shows as the explanation.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const args = process.argv.slice(2);
const refArg = args.includes('--ref') ? args[args.indexOf('--ref') + 1] : 'origin/HEAD';
const ROOT = process.cwd();
const OUT = path.join(ROOT, 'public', 'playground', 'lab', 'kafka-schemas.json');
const git = (cwd, ...a) => execFileSync('git', a, { cwd, encoding: 'utf8', maxBuffer: 64 << 20 });

// The directory the sibling repositories sit in: beside the main checkout,
// which for a worktree is not this directory's parent.
function siblingsDir() {
  const common = git(ROOT, 'rev-parse', '--path-format=absolute', '--git-common-dir').trim();
  return path.dirname(path.dirname(common));
}

// A reader of one repository at the latest revision: `{ sha, read(path), list(dir) }`.
function repo(name) {
  const local = path.join(siblingsDir(), name);
  let dir = local;
  let ref = refArg;
  if (fs.existsSync(path.join(local, '.git'))) {
    git(local, 'fetch', '--quiet', 'origin');
  } else {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), `${name}-`));
    git(dir, 'clone', '--quiet', '--depth', '1', `https://github.com/krabka-io/${name}.git`, '.');
    ref = 'HEAD';
  }
  const sha = git(dir, 'rev-parse', ref).trim();
  return {
    name, sha,
    read: (p) => git(dir, 'show', `${sha}:${p}`),
    list: (p) => git(dir, 'ls-tree', '--name-only', `${sha}:${p}`).split('\n').filter(Boolean),
  };
}

// The schemas are JSON with // comments. Drop comments outside strings.
function parseCommented(text) {
  let out = '';
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      let j = i + 1;
      while (j < text.length && text[j] !== '"') j += text[j] === '\\' ? 2 : 1;
      out += text.slice(i, j + 1);
      i = j;
    } else if (ch === '/' && text[i + 1] === '/') {
      while (i < text.length && text[i] !== '\n') i++;
      out += '\n';
    } else out += ch;
  }
  return JSON.parse(out);
}

const compactField = (f) => {
  const out = { n: f.name, t: f.type, v: f.versions };
  if (f.nullableVersions) out.nv = f.nullableVersions;
  if (f.taggedVersions) out.tv = f.taggedVersions;
  if (f.tag !== undefined) out.tag = f.tag;
  if (f.flexibleVersions) out.fv = f.flexibleVersions;
  if (f.about) out.a = f.about.replace(/\s+/g, ' ').trim();
  if (f.fields) out.f = f.fields.map(compactField);
  return out;
};

const protocol = repo('krabka-protocol');
const broker = repo('krabka-broker');
const SCHEMAS = 'crates/protocol/schemas';
const out = {
  sources: { 'krabka-protocol': protocol.sha, 'krabka-broker': broker.sha },
  requests: {}, responses: {}, headers: {}, data: {}, apiNames: {}, private: {}, metadata: {}, errors: {},
};
for (const file of protocol.list(SCHEMAS).filter((f) => f.endsWith('.json')).sort()) {
  const s = parseCommented(protocol.read(`${SCHEMAS}/${file}`));
  const msg = { name: s.name, valid: s.validVersions, flexible: s.flexibleVersions || 'none', fields: (s.fields || []).map(compactField) };
  if (s.commonStructs?.length) msg.common = Object.fromEntries(s.commonStructs.map((c) => [c.name, (c.fields || []).map(compactField)]));
  if (s.type === 'request') {
    out.requests[s.apiKey] = msg;
    out.apiNames[s.apiKey] = s.name.replace(/Request$/, '');
  } else if (s.type === 'response') out.responses[s.apiKey] = msg;
  else if (s.type === 'header') out.headers[s.name] = msg;
  else out.data[s.name] = msg;
}

// Kafka's metadata-record apiKeys, as `KraftMetadataRecord::api_key` maps them.
const recordRs = protocol.read('crates/protocol/src/records/metadata/record.rs');
for (const [, variant, key] of recordRs.matchAll(/Self::([A-Za-z]+)\(_\) => (\d+),/g)) {
  const name = `${variant}Record`;
  if (!out.data[name]) throw new Error(`no schema for metadata record ${name}`);
  out.metadata[key] = name;
}
if (!Object.keys(out.metadata).length) throw new Error('no metadata record apiKeys in record.rs');

// krabka's private controller RPCs: hand-written codecs, so names only.
const wireRs = broker.read('crates/raft/src/wire.rs');
for (const [, doc, name, key] of wireRs.matchAll(/((?:\/\/\/[^\n]*\n)*)pub const API_KEY_([A-Z_]+): i16 = (\d+);/g)) {
  const pascal = name.toLowerCase().replace(/(^|_)([a-z])/g, (_, __, c) => c.toUpperCase());
  out.private[key] = { name: pascal, about: doc.replace(/\/\/\/ ?/g, '').replace(/\s+/g, ' ').trim() };
  out.apiNames[key] = `${pascal} (krabka)`;
}

const table = broker.read('crates/broker/src/codes/tests/kafka_error_table.rs');
for (const [, name, code] of table.matchAll(/\("([A-Z0-9_]+)",\s*(-?\d+)\)/g)) out.errors[code] = name;
if (!Object.keys(out.errors).length) throw new Error('no error codes in kafka_error_table.rs');

fs.writeFileSync(OUT, JSON.stringify(out));
console.log(`krabka-protocol ${protocol.sha.slice(0, 7)}, krabka-broker ${broker.sha.slice(0, 7)}: ${Object.keys(out.requests).length} requests, ${Object.keys(out.data).length} data schemas, ${Object.keys(out.metadata).length} metadata records, ${Object.keys(out.private).length} private APIs, ${Object.keys(out.errors).length} errors → ${path.relative(ROOT, OUT)} (${(fs.statSync(OUT).size / 1024).toFixed(0)} KiB)`);
