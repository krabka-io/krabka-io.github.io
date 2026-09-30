// Scenario persistence: export and import as JSON files, autosave to
// `localStorage`, and share links whose hash carries the whole document
// (`#s=<code>`, see `codec.js`).

import { download, readFileText } from "./dom.js";
import { encodeShare, decodeShare } from "./codec.js";
import { normalizeScenario } from "./world.js";

export const STORAGE_KEY = "krabka-lab.scenario";
const HASH_RE = /[#&]s=([^&]+)/;

// Check the shape of a document the page did not write itself and fill the
// defaults. Throws with a readable message.
export function validateScenario(doc) {
  if (!doc || typeof doc !== "object" || Array.isArray(doc)) throw new Error("a scenario is a JSON object");
  if (doc.version != null && Number(doc.version) !== 1) throw new Error(`unsupported scenario version ${doc.version}`);
  if (!Array.isArray(doc.nodes)) throw new Error("a scenario needs a `nodes` array");
  for (const n of doc.nodes) {
    if (!n || typeof n !== "object") throw new Error("every node is an object");
    if (!n.kind) throw new Error("every node needs a `kind`");
  }
  const ids = new Set();
  for (const n of doc.nodes) {
    const id = Number(n.id);
    if (id && ids.has(id)) throw new Error(`node id ${id} repeats`);
    if (id) ids.add(id);
  }
  return normalizeScenario(doc);
}

export function saveLocal(scenario) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(scenario));
    return true;
  } catch {
    return false;
  }
}

export function loadLocal() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    return validateScenario(JSON.parse(raw));
  } catch {
    return null;
  }
}

export function clearLocal() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Storage is unavailable; nothing was saved.
  }
}

export function exportScenario(scenario) {
  const slug = String(scenario.name || "scenario")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  download(`${slug || "scenario"}.lab.json`, JSON.stringify(scenario, null, 2));
}

export async function importScenario(file) {
  const text = await readFileText(file);
  let doc;
  try {
    doc = JSON.parse(text);
  } catch {
    throw new Error("that file is not JSON; import a scenario exported from the lab (.lab.json)");
  }
  return validateScenario(doc);
}

// The URL that reopens this scenario.
export async function shareLink(scenario) {
  const code = await encodeShare(scenario);
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = `s=${code}`;
  return url.toString();
}

// The scenario a page was opened with, if its hash carries one.
export async function scenarioFromHash(hash) {
  const m = HASH_RE.exec(hash || "");
  if (!m) return null;
  return validateScenario(await decodeShare(m[1]));
}
