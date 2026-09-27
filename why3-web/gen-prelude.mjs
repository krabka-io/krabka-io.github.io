// Port of Creusot's prelude-generator (prelude-generator/src/main.rs) so the
// builder container needs no Rust toolchain. Usage:
//   node gen-prelude.mjs <creusot checkout>/prelude-generator <out dir>
// It writes int.coma and slice.coma from their templates and copies
// prelude.coma and float.coma, exactly as `creusot-install prelude` does.
import fs from 'node:fs';
import path from 'node:path';

const [src, out] = process.argv.slice(2);
if (!src || !out) {
  console.error('usage: gen-prelude.mjs <prelude-generator dir> <out dir>');
  process.exit(2);
}
fs.mkdirSync(out, { recursive: true });

const hex = (n) => `0x${n.toString(16).toUpperCase()}`;
const intTemplate = fs.readFileSync(path.join(src, 'int.in.coma'), 'utf8');
let ints = '';
for (const bits of [8, 16, 32, 64, 128]) {
  const b = BigInt(bits);
  ints += intTemplate
    .replaceAll('$bits_count$', String(bits))
    .replaceAll('$min_signed_value$', hex(1n << (b - 1n)))
    .replaceAll('$max_signed_value$', hex((1n << (b - 1n)) - 1n))
    .replaceAll('$max_unsigned_value$', hex((1n << b) - 1n))
    .replaceAll('$two_power_size$', `0x1${'0'.repeat(bits / 4)}`);
  ints += '\n';
}
fs.writeFileSync(path.join(out, 'int.coma'), ints);

const sliceTemplate = fs.readFileSync(path.join(src, 'slice.in.coma'), 'utf8');
let slices = '';
for (const bits of [16, 32, 64]) {
  slices += sliceTemplate.replaceAll('$bits_count$', String(bits));
  slices += '\n';
}
fs.writeFileSync(path.join(out, 'slice.coma'), slices);

for (const file of ['prelude.coma', 'float.coma']) {
  fs.copyFileSync(path.join(src, file), path.join(out, file));
}
console.log(`prelude written to ${out}: ${fs.readdirSync(out).join(', ')}`);
