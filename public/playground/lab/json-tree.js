// A collapsible JSON tree for the inspector. Objects and arrays fold; scalars
// are typed for colour. The set of expanded paths lives with the caller, so a
// tree rebuilt from a fresh snapshot keeps the branches the reader opened.

import { el } from "./dom.js";

const DEFAULT_OPEN_DEPTH = 1;
const MAX_ITEMS = 200;

// Build the tree for `value`. `expanded` is a `Set` of dotted paths the reader
// opened or closed explicitly; `path` is the prefix of this subtree.
export function jsonTree(value, opts = {}) {
  const expanded = opts.expanded || new Set();
  const collapsed = opts.collapsed || new Set();
  const root = el("div", "lab-json");
  root.appendChild(node("", value, "", 0, { expanded, collapsed, openDepth: opts.openDepth ?? DEFAULT_OPEN_DEPTH }));
  return root;
}

function node(key, value, path, depth, ctx) {
  const row = el("div", "lab-json-row");
  const isObj = value !== null && typeof value === "object";
  if (!isObj) {
    row.append(keyEl(key), scalar(value));
    return row;
  }
  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v])
    : Object.entries(value);
  const isArray = Array.isArray(value);
  const summary = isArray ? `[${entries.length}]` : `{${entries.length}}`;
  if (entries.length === 0) {
    row.append(keyEl(key), el("span", "lab-json-empty", isArray ? "[]" : "{}"));
    return row;
  }
  const open = ctx.expanded.has(path) || (!ctx.collapsed.has(path) && depth < ctx.openDepth);
  const details = el("details", "lab-json-branch");
  details.open = open;
  const sum = el("summary", "lab-json-summary");
  sum.append(keyEl(key), el("span", "lab-json-count", summary));
  details.appendChild(sum);
  details.addEventListener("toggle", () => {
    if (details.open) {
      ctx.expanded.add(path);
      ctx.collapsed.delete(path);
    } else {
      ctx.expanded.delete(path);
      ctx.collapsed.add(path);
    }
  });
  if (open) fill(details, entries, path, depth, ctx);
  else
    details.addEventListener(
      "toggle",
      () => {
        if (details.open && details.children.length === 1) fill(details, entries, path, depth, ctx);
      },
      { once: false },
    );
  row.appendChild(details);
  return row;
}

function fill(details, entries, path, depth, ctx) {
  const list = el("div", "lab-json-children");
  const shown = entries.slice(0, MAX_ITEMS);
  for (const [k, v] of shown) {
    list.appendChild(node(k, v, path ? `${path}.${k}` : k, depth + 1, ctx));
  }
  if (entries.length > MAX_ITEMS) {
    list.appendChild(el("div", "lab-json-more", `… ${entries.length - MAX_ITEMS} more`));
  }
  details.appendChild(list);
}

function keyEl(key) {
  const k = el("span", "lab-json-key", key === "" ? "" : `${key}: `);
  return k;
}

function scalar(value) {
  let cls = "lab-json-null";
  let text = "null";
  if (typeof value === "string") {
    cls = "lab-json-string";
    text = JSON.stringify(value.length > 200 ? `${value.slice(0, 200)}…` : value);
  } else if (typeof value === "number") {
    cls = "lab-json-number";
    text = String(value);
  } else if (typeof value === "boolean") {
    cls = "lab-json-bool";
    text = String(value);
  }
  return el("span", cls, text);
}
