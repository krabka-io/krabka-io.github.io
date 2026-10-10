// Shiki theme for every code block on the site: the guides Markdown renders,
// and the <CodeBlock> component the hand-written pages use.
//
// The palette is the one the rustdoc theme in krabka-io/tooling gives the API
// reference (vermilion keywords, blue callables, green literals, violet
// macros), so a snippet reads the same in a guide and in the docs it links to.

// CSS variables let the same pre-rendered tokens follow the active site theme.
// The light-mode values are defined in theme-light.css, so switching themes
// does not require re-highlighting the page in JavaScript.
const c = {
  fg: 'var(--code-fg)',
  dim: 'var(--code-dim)',
  comment: 'var(--code-comment)',
  keyword: 'var(--code-keyword)',
  call: 'var(--code-call)',
  type: 'var(--code-type)',
  string: 'var(--code-string)',
  number: 'var(--code-number)',
  constant: 'var(--code-constant)',
  macro: 'var(--code-macro)',
  invalid: 'var(--code-invalid)',
};

const rule = (scope, foreground, fontStyle) => ({
  scope,
  settings: fontStyle ? { foreground, fontStyle } : { foreground },
});

export default {
  name: 'krabka-dark',
  type: 'dark',
  colors: {
    'editor.background': 'var(--code-bg)',
    'editor.foreground': c.fg,
  },
  tokenColors: [
    rule(['comment', 'punctuation.definition.comment', 'string.comment'], c.comment),
    rule(['punctuation', 'meta.brace', 'punctuation.definition.tag', 'punctuation.separator', 'punctuation.terminator'], c.dim),
    rule(['keyword', 'keyword.control', 'storage', 'storage.type', 'storage.modifier', 'keyword.other.fn', 'keyword.other.unsafe', 'variable.language', 'entity.name.tag'], c.keyword),
    rule(['keyword.operator', 'keyword.operator.assignment'], c.fg),
    rule(['entity.name.function', 'support.function', 'meta.function-call entity.name.function', 'entity.name.command', 'support.function.builtin', 'meta.function-call'], c.call),
    rule(['entity.name.function.macro', 'support.function.macro', 'meta.macro entity.name.function', 'entity.name.function.decorator', 'meta.annotation', 'storage.type.annotation', 'punctuation.definition.annotation'], c.macro),
    rule(['entity.name.type', 'entity.name.class', 'entity.name.namespace', 'entity.other.inherited-class', 'support.type', 'support.class', 'entity.name.type.parameter', 'storage.type.java', 'storage.type.primitive', 'storage.type.built-in'], c.type),
    rule(['string', 'string.quoted', 'string.unquoted', 'punctuation.definition.string', 'string.regexp'], c.string),
    rule(['constant.numeric', 'constant.numeric.integer', 'constant.numeric.float', 'constant.numeric.decimal'], c.number),
    rule(['constant', 'constant.language', 'constant.character', 'constant.other', 'support.constant', 'variable.other.constant'], c.constant),
    rule(['variable.parameter', 'variable.other', 'variable', 'meta.definition.variable', 'variable.other.readwrite'], c.fg),
    rule(['variable.other.property', 'variable.other.object.property', 'support.type.property-name', 'entity.name.tag.yaml', 'meta.object-literal.key', 'variable.other.member'], c.call),
    rule(['entity.name.section', 'entity.name.section.group-title', 'markup.heading', 'entity.name.tag.toml'], c.constant),
    rule(['meta.attribute', 'meta.attribute.rust', 'punctuation.definition.attribute'], c.dim),
    rule(['constant.other.option', 'constant.other.option.dash', 'variable.parameter.option'], c.type),
    rule(['string.unquoted.argument', 'string.unquoted.argument.shell'], c.fg),
    rule(['invalid', 'invalid.illegal'], c.invalid),
  ],
};
