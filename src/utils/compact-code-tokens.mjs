// Reuse the site's theme tokens instead of repeating a color style on each span.
// The selectors live in custom.css. Unknown styles stay inline unchanged.
const tones = new Set([
  'fg', 'dim', 'comment', 'keyword', 'call', 'type', 'string',
  'number', 'constant', 'macro', 'invalid',
]);

export default function compactCodeTokens() {
  return {
    name: 'krabka-compact-code-tokens',
    preprocess(_code, options) {
      // Shiki joins only adjacent tokens with identical styles, within a line.
      options.mergeSameStyleTokens = true;
    },
    span(node) {
      const match = /^color:var\(--code-([a-z]+)\);?$/.exec(node.properties.style ?? '');
      if (!match || !tones.has(match[1])) return;
      this.addClassToHast(node, `tok-${match[1]}`);
      delete node.properties.style;
    },
  };
}
