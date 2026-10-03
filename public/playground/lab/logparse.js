// Turning what a real broker writes on stderr into log records, and choosing
// among them. Pure: no DOM, so `scripts/check-lab-logs.mjs` imports it in node.
//
// The broker writes one JSON object per line (`ts` in lab seconds since the
// process started, `level`, `target`, `message`, then the event's own fields).
// Anything else still gets a row: the compact text of the first broker build
// (`  12.345s  INFO krabka_broker::x: message k=v`) is read into the same
// record, and a line that is neither, such as a panic, is kept raw with a
// guessed level.

export const LEVELS = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
const RANK = Object.fromEntries(LEVELS.map((l, i) => [l, i]));

export function levelRank(level) {
  return RANK[level] ?? RANK.INFO;
}

/** "warn" / "Warning" / "ERR" as one of LEVELS, or null when it is none of them. */
export function normalizeLevel(text) {
  const u = String(text ?? "").trim().toUpperCase();
  if (u === "WARNING") return "WARN";
  if (u === "ERR" || u === "FATAL" || u === "CRITICAL") return "ERROR";
  return u in RANK ? u : null;
}

// ---- lines ----------------------------------------------------------------------------------------

const COMPACT = /^\s*(\d+(?:\.\d+)?)s\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+(?:([A-Za-z_][\w:.-]*):\s+)?(.*)$/;
const TRAILING_FIELDS = /((?:\s+[A-Za-z_][\w.]*=(?:"[^"]*"|\S+))+)\s*$/;
const FIELD = /([A-Za-z_][\w.]*)=("[^"]*"|\S+)/g;
const ERROR_WORDS = /\b(panic(?:ked)?|error|fatal|exception|abort(?:ed)?)\b/i;

function fromJson(raw) {
  let obj;
  try {
    obj = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
  const { ts, level, target, message, ...fields } = obj;
  const seconds = typeof ts === "number" ? ts : typeof ts === "string" && ts.trim() ? Number(ts) : NaN;
  // A subscriber that did not flatten the event nests its message under `fields`.
  const text = message ?? fields.fields?.message;
  return {
    format: "json",
    level: normalizeLevel(level) ?? "INFO",
    target: typeof target === "string" ? target : "",
    message: text == null ? "" : typeof text === "string" ? text : JSON.stringify(text),
    ts: Number.isFinite(seconds) ? seconds : null,
    fields,
    record: obj,
  };
}

function fromCompact(raw) {
  const m = COMPACT.exec(raw);
  if (!m) return null;
  let message = m[4];
  const fields = {};
  const tail = TRAILING_FIELDS.exec(message);
  if (tail) {
    for (const [, key, value] of tail[1].matchAll(FIELD)) fields[key] = value.replace(/^"(.*)"$/, "$1");
    message = message.slice(0, tail.index);
  }
  const ts = Number(m[1]);
  return { format: "text", level: m[2], target: m[3] || "", message, ts, fields, record: { ts, level: m[2], target: m[3] || "", message, ...fields } };
}

/**
 * One line as a record: `{ raw, format, level, target, message, ts, fields, record }`.
 * `format` is "json", "text" (the compact form) or "raw"; `record` is the
 * whole object for the first two and null for a raw line. `ts` is seconds
 * since the process started, or null. An empty line is null.
 */
export function parseLine(text) {
  const raw = String(text).replace(/\r$/, "");
  if (!raw.trim()) return null;
  const trimmed = raw.trim();
  const parsed = (trimmed.startsWith("{") && trimmed.endsWith("}") && fromJson(trimmed)) || fromCompact(raw);
  if (parsed) return { raw, ...parsed };
  return { raw, format: "raw", level: ERROR_WORDS.test(raw) ? "ERROR" : "INFO", target: "", message: raw, ts: null, fields: {}, record: null };
}

/** The raw lines of `entries`, one per line: what Download and Copy visible hand out. */
export function toNdjson(entries) {
  return entries.length ? `${entries.map((e) => e.raw).join("\n")}\n` : "";
}

// ---- level directives -----------------------------------------------------------------------------

// What the page sets as `KRABKA_LOG`: a tracing-subscriber `Targets` string.
const LEVEL_WORDS = ["trace", "debug", "info", "warn", "error", "off"];
const TARGET = /^[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*$/;

export const DIRECTIVE_EXAMPLES = ["info", "debug", "warn,krabka_broker=debug", "info,krabka_broker::request=debug"];

/**
 * Checks a level directive: comma-separated entries, each a bare level
 * (trace, debug, info, warn, error, off) or `target=level`. Returns
 * `{ ok: true, directive, entries }` with the text normalised, or
 * `{ ok: false, error }`. An empty text is valid and means the broker's own
 * default.
 */
export function parseDirective(text) {
  const source = String(text ?? "").trim();
  if (!source) return { ok: true, directive: "", entries: [] };
  const entries = [];
  for (const part of source.split(",").map((p) => p.trim())) {
    if (!part) return { ok: false, error: "an entry is empty: check for a stray comma" };
    const at = part.indexOf("=");
    const level = (at < 0 ? part : part.slice(at + 1)).trim().toLowerCase();
    if (at < 0) {
      if (!LEVEL_WORDS.includes(level)) return { ok: false, error: `"${part}" is not a level: write ${part}=debug to set a target's level` };
      if (entries.some((e) => e.target === null)) return { ok: false, error: "only one bare level can set the default" };
      entries.push({ target: null, level });
      continue;
    }
    const target = part.slice(0, at).trim();
    if (!TARGET.test(target)) return { ok: false, error: `"${target}" is not a target such as krabka_broker or krabka_broker::raft` };
    if (!LEVEL_WORDS.includes(level)) return { ok: false, error: `"${part.slice(at + 1).trim()}" is not a level: use ${LEVEL_WORDS.join(", ")}` };
    entries.push({ target, level });
  }
  return { ok: true, directive: entries.map((e) => (e.target === null ? e.level : `${e.target}=${e.level}`)).join(","), entries };
}

// ---- filtering ------------------------------------------------------------------------------------

/**
 * A search box's text as terms: bare words (all must appear in the message,
 * target or fields), quoted phrases, and `field:value` terms. A value may
 * start with `>`, `>=`, `<` or `<=` to compare numbers (or levels).
 */
export function parseQuery(text) {
  const terms = [];
  const fields = [];
  for (const token of String(text ?? "").match(/(?:[^\s"]|"[^"]*")+/g) ?? []) {
    const m = /^([A-Za-z_][\w.-]*):(.+)$/s.exec(token);
    if (!m) {
      terms.push(token.replace(/"/g, "").toLowerCase());
      continue;
    }
    let value = m[2].replace(/^"(.*)"$/s, "$1");
    const cmp = /^(>=|<=|>|<)(.*)$/s.exec(value);
    const op = cmp ? { ">": "gt", ">=": "gte", "<": "lt", "<=": "lte" }[cmp[1]] : "eq";
    if (cmp) value = cmp[2];
    fields.push({ key: m[1], op, value: value.toLowerCase(), num: value.trim() === "" ? NaN : Number(value) });
  }
  return { terms, fields };
}

function haystack(entry) {
  entry.hay ??= `${entry.message} ${entry.target} ${Object.entries(entry.fields).map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`).join(" ")}`.toLowerCase();
  return entry.hay;
}

// The value a `field:` term looks at: the record's own keys, plus `node`,
// `level`, `stream` and `message`.
function lookup(entry, key, nodeName) {
  switch (key) {
    case "node":
      return [String(entry.node), nodeName ? nodeName(entry.node) : ""];
    case "level":
      return entry.level;
    case "target":
      return entry.target;
    case "message":
      return entry.message;
    case "stream":
      return entry.stream;
    default: {
      let at = entry.fields;
      for (const step of key.split(".")) {
        if (at == null || typeof at !== "object" || !Object.hasOwn(at, step)) return undefined;
        at = at[step];
      }
      return at;
    }
  }
}

function matchField(entry, f, nodeName) {
  const v = lookup(entry, f.key, nodeName);
  if (v === undefined || v === null) return false;
  if (Array.isArray(v)) return v.some((x) => String(x).toLowerCase() === f.value);
  if (f.op !== "eq") {
    if (f.key === "level") {
      const want = normalizeLevel(f.value);
      if (!want) return false;
      const d = levelRank(entry.level) - levelRank(want);
      return f.op === "gt" ? d > 0 : f.op === "gte" ? d >= 0 : f.op === "lt" ? d < 0 : d <= 0;
    }
    const n = Number(v);
    if (Number.isNaN(n) || Number.isNaN(f.num)) return false;
    return f.op === "gt" ? n > f.num : f.op === "gte" ? n >= f.num : f.op === "lt" ? n < f.num : n <= f.num;
  }
  const text = String(typeof v === "object" ? JSON.stringify(v) : v).toLowerCase();
  // A target or a message is found by a part of it (`target:raft`); every other field is compared whole.
  return f.key === "target" || f.key === "message" ? text.includes(f.value) : text === f.value;
}

/** Whether `entry` passes the text query. `nodeName(id)` lets `node:` match a name. */
export function matchQuery(entry, query, nodeName) {
  for (const t of query.terms) if (!haystack(entry).includes(t)) return false;
  for (const f of query.fields) if (!matchField(entry, f, nodeName)) return false;
  return true;
}

/**
 * The entries a filter shows. `filter`: `{ minLevel, nodes, targets, prefix,
 * query }` where `nodes` and `targets` are Sets (empty or null: all), `prefix`
 * a target prefix and `query` a `parseQuery` result. Process markers ignore
 * the level and target filters: they explain the gaps between lines.
 */
export function filterEntries(entries, filter, nodeName) {
  const min = levelRank(filter.minLevel || "TRACE");
  const nodes = filter.nodes?.size ? filter.nodes : null;
  const targets = filter.targets?.size ? filter.targets : null;
  const prefix = filter.prefix || "";
  const query = filter.query && (filter.query.terms.length || filter.query.fields.length) ? filter.query : null;
  return entries.filter((e) => {
    if (nodes && !nodes.has(e.node)) return false;
    if (!e.marker) {
      if (levelRank(e.level) < min) return false;
      if (targets && !targets.has(e.target)) return false;
      if (prefix && !e.target.startsWith(prefix)) return false;
    }
    return !query || matchQuery(e, query, nodeName);
  });
}
