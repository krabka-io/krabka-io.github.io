// Syntax highlighting for the Coma and Why3 text the proof explorer shows.
//
// Creusot writes Coma, which is Why3's ML-like language with labels and
// source-span markers. A generated file is several thousand lines of
// `let%span`, `[@expl:...]` labels and long propositions; without colour it is
// a wall of text. `highlightWhy(text)` returns HTML for a `<pre>`: one
// `.px-line` element per line (the stylesheet numbers them with a counter),
// each token in a `.px-hl-*` span. A block comment that runs over several
// lines keeps its colour on every line.
//
// The output is built from escaped text only, so it is safe to assign to
// innerHTML.

const KEYWORDS = new Set([
  "abstract", "any", "as", "axiom", "break", "by", "clone", "coinductive", "coma", "constant", "continue",
  "diverges", "do", "done", "downto", "else", "end", "ensures", "exception", "exists", "export", "external",
  "for", "forall", "fun", "function", "ghost", "goal", "if", "import", "in", "inductive", "invariant", "lemma",
  "let", "match", "meta", "module", "mutable", "not", "predicate", "private", "prop", "raise", "rec", "requires",
  "return", "returns", "scope", "so", "then", "theory", "to", "try", "type", "use", "val", "variant", "while", "with",
]);
const LITERALS = new Set(["true", "false", "None", "Some", "Ok", "Err", "Nil", "Cons"]);

// One alternation, tried in order at each position.
const TOKEN = new RegExp(
  [
    String.raw`(?<string>"(?:[^"\\]|\\.)*")`,
    String.raw`(?<attr>\[(?:@|%#?)[^\]]*\])`,
    String.raw`(?<number>\b\d+(?:\.\d+)?\b)`,
    String.raw`(?<path>\b(?:[A-Z][A-Za-z0-9_']*\.)+)`,
    String.raw`(?<kw2>(?:let|val|axiom|goal|lemma|function|predicate)%[a-z_]+)`,
    String.raw`(?<word>[A-Za-z_][A-Za-z0-9_']*)`,
    String.raw`(?<op><->|->|\/\\|\\\/|<>|<=|>=|:=|[-+*\/<>=|&!~^%])`,
  ].join("|"),
  "y",
);

function escapeHtml(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

const span = (cls, text) => `<span class="px-hl-${cls}">${escapeHtml(text)}</span>`;

// The index just past the end of the comment that `state.comment` says is open,
// scanning from `from`; the end of the line when it stays open.
function commentEnd(line, from, state) {
  let j = from;
  while (j < line.length && state.comment > 0) {
    if (line[j] === "(" && line[j + 1] === "*") {
      state.comment += 1;
      j += 2;
    } else if (line[j] === "*" && line[j + 1] === ")") {
      state.comment -= 1;
      j += 2;
    } else j += 1;
  }
  return j;
}

// Highlights one line. `state.comment` is the open `(*` depth carried between lines.
function highlightLine(line, state) {
  let out = "";
  let i = 0;
  const n = line.length;
  while (i < n) {
    if (state.comment > 0) {
      const j = commentEnd(line, i, state);
      out += span("comment", line.slice(i, j));
      i = j;
      continue;
    }
    if (line[i] === "(" && line[i + 1] === "*" && line[i + 2] !== ")") {
      state.comment = 1;
      const j = commentEnd(line, i + 2, state);
      out += span("comment", line.slice(i, j));
      i = j;
      continue;
    }
    TOKEN.lastIndex = i;
    const m = TOKEN.exec(line);
    if (!m) {
      // Whitespace or a character no rule names.
      const ch = line[i];
      out += escapeHtml(ch);
      i += 1;
      continue;
    }
    const g = m.groups;
    const text = m[0];
    if (g.string) out += span("string", text);
    else if (g.attr) out += span(text.startsWith("[@expl") ? "expl" : "attr", text);
    else if (g.kw2) out += span("keyword", text);
    else if (g.number) out += span("number", text);
    else if (g.path) out += span("type", text);
    else if (g.word) {
      if (KEYWORDS.has(text)) out += span("keyword", text);
      else if (LITERALS.has(text)) out += span("literal", text);
      else if (/^[A-Z]/.test(text)) out += span("type", text);
      else out += escapeHtml(text);
    } else if (g.op) out += span("op", text);
    i += text.length;
  }
  return out;
}

export function highlightWhy(text) {
  const state = { comment: 0 };
  return text
    .split("\n")
    .map((line) => `<span class="px-line">${highlightLine(line, state)}</span>`)
    .join("\n");
}
