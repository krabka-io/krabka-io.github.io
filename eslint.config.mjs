// Lint for the Node scripts under scripts/. `npm run lint` runs it; the Astro
// site under src/ has its own checks.
import js from '@eslint/js';
import globals from 'globals';

export default [
  js.configs.recommended,
  {
    files: ['scripts/**/*.mjs'],
    languageOptions: { ecmaVersion: 'latest', sourceType: 'module', globals: { ...globals.node } },
    rules: {
      // `const { omitted, ...rest } = value` is how these scripts drop a key.
      'no-unused-vars': ['error', { ignoreRestSiblings: true }],
    },
  },
  {
    // These drive a browser through Playwright and pass functions to
    // page.evaluate, which run in the page with its globals.
    files: ['scripts/check-*.mjs', 'scripts/lab-check-lib.mjs'],
    languageOptions: { globals: { ...globals.node, ...globals.browser } },
  },
];
