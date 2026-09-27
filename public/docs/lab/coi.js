// Cross-origin isolation for the Cluster Lab page (/docs/lab/).
//
//   import { ensureCrossOriginIsolation } from "/docs/lab/coi.js";
//   const coi = await ensureCrossOriginIsolation();
//   if (!coi.isolated && !coi.reloading) explain(coi.reason);
//
// When the page is not isolated, the helper registers coi-sw.js (scope
// /docs/lab/, nothing else on the site) and reloads the page once; the
// service worker adds COOP and COEP to the page it serves. COEP starts as
// `credentialless`, which keeps cross-origin fonts and images loading. If the
// browser still does not isolate a page served that way (no credentialless
// support), the helper switches the service worker to `require-corp` and
// reloads once more, and remembers that choice for this browser. It never
// reloads more than that: when isolation stays unavailable it says why.
//
// Resolves to `{ isolated, via, coep, reason, reloading }`:
// - `isolated`: `window.crossOriginIsolated`;
// - `via`: "headers" (the server sent COOP and COEP), "service-worker", or null;
// - `coep`: the service worker's COEP value when `via` is "service-worker";
// - `reason`: why isolation is unavailable, in words, when it is;
// - `reloading`: true when the page is about to reload (the promise resolves first).

const SW_URL = new URL("./coi-sw.js", import.meta.url);
const SCOPE = new URL("./", import.meta.url);
const STATE_KEY = "krabka-coi"; // sessionStorage: the helper's own reloads
const MODE_KEY = "krabka-coi-coep"; // localStorage: the COEP value this browser needs

function storage(kind) {
  try {
    return globalThis[kind] ?? null;
  } catch {
    return null;
  }
}

/** Reads and clears the reload state left by a previous call on this tab. */
function takeState() {
  const store = storage("sessionStorage");
  try {
    const text = store?.getItem(STATE_KEY);
    store?.removeItem(STATE_KEY);
    return text ? JSON.parse(text) : null;
  } catch {
    return null;
  }
}

function saveState(state) {
  try {
    storage("sessionStorage")?.setItem(STATE_KEY, JSON.stringify(state));
  } catch {
    // Without sessionStorage the helper cannot tell its own reload apart; it still reloads at most once per call.
  }
}

function rememberMode(coep) {
  try {
    storage("localStorage")?.setItem(MODE_KEY, coep);
  } catch {
    // Not remembered; the helper probes again next time.
  }
}

function recallMode() {
  try {
    return storage("localStorage")?.getItem(MODE_KEY) === "require-corp" ? "require-corp" : "credentialless";
  } catch {
    return "credentialless";
  }
}

function isOurs(worker) {
  return Boolean(worker) && new URL(worker.scriptURL).pathname === SW_URL.pathname;
}

function coepOf(worker) {
  return new URL(worker.scriptURL).searchParams.get("coep") === "require-corp" ? "require-corp" : "credentialless";
}

const unavailable = (reason) => ({ isolated: false, via: null, coep: null, reason, reloading: false });

function activated(registration, timeoutMs) {
  const pending = registration.installing || registration.waiting;
  if (!pending) return registration.active ? Promise.resolve() : Promise.reject(new Error("no service worker to activate"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`it did not activate within ${timeoutMs} ms`)), timeoutMs);
    pending.addEventListener("statechange", () => {
      if (pending.state === "activated") {
        clearTimeout(timer);
        resolve();
      } else if (pending.state === "redundant") {
        clearTimeout(timer);
        reject(new Error("it became redundant while installing"));
      }
    });
  });
}

async function register(coep, timeoutMs) {
  const url = new URL(SW_URL);
  url.searchParams.set("coep", coep);
  const registration = await navigator.serviceWorker.register(url.href, { scope: SCOPE.pathname });
  if (!registration) throw new Error("the browser declined the registration");
  await activated(registration, timeoutMs);
}

function reloadWith(state, reload) {
  saveState(state);
  if (!reload) return { ...unavailable("reload the page to apply cross-origin isolation"), reloading: false };
  location.reload();
  return { ...unavailable("reloading to apply cross-origin isolation"), reloading: true };
}

/**
 * Makes the page cross-origin isolated, reloading it once if needed.
 * @param {object} [options]
 * @param {boolean} [options.reload] Reload by itself (default true); false leaves it to the caller.
 * @param {number} [options.timeoutMs] How long the service worker may take to activate.
 */
export async function ensureCrossOriginIsolation({ reload = true, timeoutMs = 10_000 } = {}) {
  const state = takeState();
  const container = typeof navigator !== "undefined" ? navigator.serviceWorker : undefined;
  const controller = container?.controller ?? null;
  if (globalThis.crossOriginIsolated) {
    if (isOurs(controller)) {
      rememberMode(coepOf(controller));
      return { isolated: true, via: "service-worker", coep: coepOf(controller), reason: null, reloading: false };
    }
    return { isolated: true, via: "headers", coep: null, reason: null, reloading: false };
  }
  if (!globalThis.isSecureContext) return unavailable("the page is not a secure context (https or localhost), so it cannot use a service worker");
  if (!container) return unavailable("service workers are unavailable in this browser (private browsing can disable them)");
  if (!location.pathname.startsWith(SCOPE.pathname)) {
    if (`${location.pathname}/` === SCOPE.pathname && reload && state?.step !== "slash") {
      saveState({ step: "slash" });
      location.replace(`${SCOPE.pathname}${location.search}${location.hash}`);
      return { ...unavailable("moving to the page's canonical URL"), reloading: true };
    }
    return unavailable(`this page (${location.pathname}) is outside the service worker's scope ${SCOPE.pathname}`);
  }
  if (isOurs(controller)) {
    // The service worker served this page, and the browser still did not isolate it.
    const coep = coepOf(controller);
    if (coep === "credentialless" && state?.step !== "degraded") {
      rememberMode("require-corp");
      try {
        await register("require-corp", timeoutMs);
      } catch (err) {
        return unavailable(`the service worker could not switch to COEP require-corp: ${err.message}`);
      }
      return reloadWith({ step: "degraded" }, reload);
    }
    return unavailable(`the browser did not isolate the page even with COEP ${coep} from the service worker`);
  }
  if (state?.step === "reloaded" || state?.step === "degraded") {
    return unavailable("the service worker did not take control of the page after a reload (a hard reload bypasses service workers: reload normally)");
  }
  try {
    await register(recallMode(), timeoutMs);
  } catch (err) {
    return unavailable(`the service worker could not be registered: ${err.message}`);
  }
  return reloadWith({ step: "reloaded" }, reload);
}

/** Removes the lab's service worker (the page loses isolation on its next load). */
export async function removeCrossOriginIsolation() {
  if (typeof navigator === "undefined" || !navigator.serviceWorker) return false;
  const registration = await navigator.serviceWorker.getRegistration(SCOPE.pathname);
  return registration && isOurs(registration.active) ? registration.unregister() : false;
}
