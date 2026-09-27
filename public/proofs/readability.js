// ponytail: translate only simple Boolean conditions; use a parser for broader coverage.
export function obligationSummary(label = "", formula = "") {
  const result = /^\(result = ([A-Za-z_]\w*)\) = \((.+)\)$/.exec(formula);
  if (result) {
    const terms = result[2].split(" /\\ ");
    if (terms.every((term) => /^(?:not )?[A-Za-z_]\w*$/.test(term) && !/^(true|false)$/.test(term))) {
      return `Returns ${result[1]} exactly when ${terms.map((term) => term.startsWith("not ") ? `${term.slice(4)} is false` : `${term} is true`).join(" and ")}.`;
    }
  }
  if (label === "index in bounds") return "The index must stay within the collection's bounds.";
  if (/\bby zero$/.test(label)) return "The divisor must not be zero.";
  if (/\boverflow$/.test(label)) return "The arithmetic operation must stay within the number type's limits.";
  return "";
}

export const operators = new Map([
  ["/\\", "and"], ["\\/", "or"], ["not", "not"], ["->", "implies"],
  ["<->", "if and only if"], ["=", "equals"],
  ["<>", "not equal"], ["<=", "less than or equal"], [">=", "greater than or equal"],
  ["<", "less than"], [">", "greater than"], ["forall", "for every"], ["exists", "there exists"],
]);

const integerFunctions = new Map([
  ["gt_log_Int", ">"],
  ["ge_log_Int", ">="],
  ["cmp_log_Int", "compare integers"],
]);
const readableOperators = new Map([
  ["=", "=="], ["/\\", "&"], ["\\/", "||"], ["<>", "!="],
  ["<", "<"], [">", ">"], ["<=", "<="], [">=", ">="], ["not", "!"],
]);
const comparisonPattern = /^(gt_log_Int|ge_log_Int)\s+\(UInt(?:8|16|32|64|128)\.t'int\s+(\w+)\)\s+(-?\d+)$/;
const projectionPattern = /^UInt(?:8|16|32|64|128)\.t'int\s+\w+$/;

export function tokenText(token, useWords) {
  if (useWords && token.readable !== undefined) return token.readable;
  if (useWords && token.kind === "annotation" && /^(?:\[%#|\[@(?:expl:|stop_split\]))/.test(token.text)) return "";
  if (useWords && token.kind === "comparison") {
    const [, fn, name, value] = comparisonPattern.exec(token.text);
    return `${name} ${integerFunctions.get(fn)} ${value}`;
  }
  if (useWords && token.kind === "projection") return token.text.split(/\s+/)[1];
  if (useWords && integerFunctions.has(token.text)) return integerFunctions.get(token.text);
  if (useWords && /^UInt\d+\.t'int$/.test(token.text)) return "integer value of";
  if (useWords && /^UInt\d+\.t$/.test(token.text)) return `unsigned ${token.text.match(/\d+/)[0]}-bit integer`;
  if (useWords && token.text === "int") return "integer";
  if (useWords && token.text === "bool") return "boolean";
  return useWords && token.kind === "operator" ? ` ${readableOperators.get(token.text) ?? operators.get(token.text)} ` : token.text;
}

function basicTokens(formula) {
  return formula.split(/("(?:\\.|[^"\\])*"|\[@[^\]]*\]|\[%#[^\]]*\]|\b(?:gt|ge)_log_Int\s+\(UInt(?:8|16|32|64|128)\.t'int\s+\w+\)\s+-?\d+(?![\w'.])|\bUInt(?:8|16|32|64|128)\.t'int\s+\w+(?![\w'.])|\b(?:gt_log_Int|ge_log_Int|cmp_log_Int|UInt(?:8|16|32|64|128)\.t(?:'int)?)(?![\w'.])|<->|->|\/\\|\\\/|<>|<=|>=|[=<>]|\b(?:not|forall|exists|true|false|bool|int)\b|\b\d+\b)/g).filter(Boolean).map((text) => ({
    text,
    kind: text.startsWith("[@") || text.startsWith("[%#") ? "annotation" : comparisonPattern.test(text) ? "comparison" : projectionPattern.test(text) ? "projection" : operators.has(text) ? "operator" : /^(?:\d+|true|false)$/.test(text) ? "literal" : "plain",
    dataType: comparisonPattern.test(text) || projectionPattern.test(text) || integerFunctions.has(text) || /^UInt\d+\.t'int$/.test(text) || /^\d+$/.test(text) || text === "int" ? "integer" : /^(true|false|bool)$/.test(text) ? "boolean" : /^UInt\d+\.t$/.test(text) ? "unsigned" : null,
  }));
}

const conversion = /^(U?Int(?:8|16|32|64|128))\.(?:to_int|t'int|of_int)$/;
const comparison = /^(gt|ge)_log_Int$/;

// Read prefix-call arguments with balanced parentheses, including nested casts.
function integerApplication(text, start) {
  const fn = /^(?:U?Int(?:8|16|32|64|128)\.(?:to_int|t'int|of_int)|(?:gt|ge)_log_Int)\b/.exec(text.slice(start));
  if (!fn) return null;
  const atom = (offset) => {
    const leading = /^\s+/.exec(text.slice(offset));
    if (!leading) return null;
    offset += leading[0].length;
    const nested = integerApplication(text, offset);
    if (nested) return nested;
    if (text[offset] === '(') {
      let depth = 1;
      let end = offset + 1;
      for (; end < text.length && depth; end += 1) {
        if (text[end] === '(') depth += 1;
        if (text[end] === ')') depth -= 1;
      }
      if (depth) return null;
      const inner = formulaTokens(text.slice(offset + 1, end - 1)).map((t) => tokenText(t, true)).join('').trim();
      return { end, readable: /^[\w']+$/.test(inner) ? inner : `(${inner})` };
    }
    const value = /^(?:-?\d+|[A-Za-z_][\w']*)(?![\w'.])/.exec(text.slice(offset));
    return value ? { end: offset + value[0].length, readable: value[0] } : null;
  };
  const left = atom(start + fn[0].length);
  if (!left) return null;
  if (conversion.test(fn[0])) return left;
  const right = atom(left.end);
  if (!right) return null;
  const readable = `${left.readable} ${comparison.exec(fn[0])[1] === 'gt' ? '>' : '>='} ${right.readable}`;
  return { end: right.end, readable: /\bnot\s*$/.test(text.slice(0, start)) ? `(${readable})` : readable };
}

export function formulaTokens(formula) {
  const tokens = [];
  const candidates = /"(?:\\.|[^"\\])*"|\[@[^\]]*\]|\[%#[^\]]*\]|\b[A-Za-z_][\w']*:\s*U?Int(?:8|16|32|64|128)\.t\b|\b(?:U?Int(?:8|16|32|64|128)\.(?:to_int|t'int|of_int)|(?:gt|ge)_log_Int)\b/g;
  let offset = 0;
  for (const match of formula.matchAll(candidates)) {
    if (match.index < offset || /^["\[]/.test(match[0])) continue;
    const declaration = match[0].includes(':');
    const app = declaration ? { end: match.index + match[0].length, readable: match[0].split(':')[0] } : integerApplication(formula, match.index);
    if (!app) continue;
    tokens.push(...basicTokens(formula.slice(offset, match.index)));
    tokens.push({ text: formula.slice(match.index, app.end), readable: app.readable, kind: declaration ? 'plain' : 'projection', dataType: 'integer' });
    offset = app.end;
  }
  tokens.push(...basicTokens(formula.slice(offset)));
  return tokens;
}

// Format grouping only; never reorder conditions or infer operator precedence.
export function readableTokens(tokens) {
  const parts = tokens.flatMap((token) => {
    const display = tokenText(token, true);
    if (!display) return [];
    const chunks = token.kind === "plain" && token.readable === undefined ? display.split(/("(?:\\.|[^"\\])*"|\b(?:match|with|end|result|Some|None)\b|\.(?=\s|$)|[()|])/g) : [display];
    return chunks.filter((text) => text.trim()).map((text) => ({
      ...token,
      display: text.startsWith('"') ? text : text.trim().replace(/\s+/g, " "),
    }));
  });
  const stack = [];
  const groups = new Map();
  const logical = new Set(["/\\", "\\/", "->", "<->", "forall", "exists"]);
  parts.forEach((part, index) => {
    if (part.kind === "plain" && part.display === "(") stack.push({ start: index, multiline: false });
    else if (part.kind === "plain" && part.display === ")" && stack.length) {
      const group = stack.pop();
      group.end = index;
      groups.set(group.start, group);
      groups.set(index, group);
      if (group.multiline && stack.length) stack[stack.length - 1].multiline = true;
    } else if (logical.has(part.text) && stack.length) stack[stack.length - 1].multiline = true;
  });

  let depth = 0;
  let newLine = false;
  let previous = "";
  const matches = [];
  const quantifiers = [];
  return parts.map((part, index) => {
    let display = part.display;
    const syntax = part.kind === "plain" && part.readable === undefined;
    while (quantifiers.length && quantifiers[quantifiers.length - 1].end === index) {
      depth = quantifiers.pop().depth;
    }
    if (part.text === 'forall' || part.text === 'exists') {
      const enclosing = [...groups.values()].filter((group) => group.start < index && group.end > index).sort((a, b) => b.start - a.start)[0];
      quantifiers.push({ depth, end: enclosing?.end ?? parts.length, header: true });
    }
    const quantifier = quantifiers[quantifiers.length - 1];
    const quantifierColon = syntax && display === '.' && quantifier?.header;
    if (quantifierColon) { display = ':'; quantifier.header = false; }
    if (syntax && display === 'match') matches.push({ depth, branch: false, body: false });
    const match = matches[matches.length - 1];
    if (syntax && display === 'with' && match) {
      display = ':';
      newLine = false;
    }
    if (syntax && display === '|' && match) {
      depth = match.depth + 1;
      match.branch = true;
      match.body = false;
      display = 'case';
      newLine = true;
    }
    const branchArrow = part.text === '->' && match?.branch && !match.body;
    if (branchArrow) { display = ':'; match.body = true; }
    if (syntax && display === 'end' && match) {
      depth = match.depth;
      matches.pop();
      newLine = true;
    }
    const group = groups.get(index);
    const opening = group && group.start === index;
    const closing = group && !opening;
    if (closing && group.multiline) {
      depth -= 1;
      newLine = true;
    }
    if (logical.has(part.text) && !branchArrow) newLine = true;
    const prefix = index === 0 ? "" : newLine ? `\n${"  ".repeat(depth)}` : closing || previous === "(" || display === ':' || display.startsWith('.') ? "" : " ";
    newLine = false;
    if (opening && group.multiline) {
      depth += 1;
      newLine = true;
    }
    if (branchArrow) { depth = match.depth + 2; newLine = true; }
    if (quantifierColon) { depth += 1; newLine = true; }
    previous = display;
    const semantic = syntax && ({ result: ['return', 'returned value'], Some: ['some', '▣'], None: ['none', '□'] })[display];
    return { ...part, kind: semantic ? semantic[0] : syntax && ['match', 'case', 'end'].includes(display) ? 'operator' : part.kind, display: prefix + (semantic ? semantic[1] : display) };
  });
}
