import sharp from 'sharp';
import { readdirSync, mkdirSync } from 'node:fs';

// The original download paths are public contracts; only the previews change.
const output = 'public/brand/previews';
mkdirSync(output, { recursive: true });
const inputs = ['public/logo.png', ...readdirSync('public/brand').filter((name) => name.endsWith('.png')).map((name) => `public/brand/${name}`)];
for (const input of inputs) {
  const name = input.split('/').pop().replace(/\.png$/, '.webp');
  await sharp(input).resize({ width: 128, height: 176, fit: 'inside', withoutEnlargement: true }).webp({ quality: 90 }).toFile(`${output}/${name}`);
}
console.log(`Built ${inputs.length} brand previews; original assets were preserved.`);
