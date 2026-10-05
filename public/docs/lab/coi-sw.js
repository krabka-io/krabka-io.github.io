// Cross-origin isolation for the Cluster Lab on a static host.
//
// GitHub Pages cannot send the Cross-Origin-Opener-Policy and
// Cross-Origin-Embedder-Policy headers that make a page cross-origin
// isolated, and the lab's WASI runtime needs isolation for SharedArrayBuffer
// and Atomics.wait. This service worker adds the headers to the documents it
// controls. It lives in /docs/lab/, so it controls /docs/lab/ and nothing else
// on the site. Every page of the site registers it (BaseLayout.astro), so a
// visitor who reaches the lab from another page arrives isolated, without the
// reload `coi.js` falls back to.
//
// Only documents (navigations) and worker scripts get the headers; every other
// request goes to the network untouched, under the page's policy. The COEP
// value comes from the registration URL: `?coep=credentialless` (the default,
// so cross-origin fonts and images without CORP headers keep loading) or
// `?coep=require-corp` for browsers without credentialless. `coi.js` picks it.
//
// The lab's two WebAssembly modules (the lab module, about 6 MB, and the
// krabka broker, about 15 MB) are kept in a cache, cache first: the page asks
// for them with their content hash in `?v=` (lab.astro computes it at build
// time), so a cached module is the right one and a new build is a miss. A
// module fetched without `v` goes to the network as before.

const COEP = new URL(self.location.href).searchParams.get("coep") === "require-corp" ? "require-corp" : "credentialless";
const WASM_CACHE = "krabka-lab-wasm";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
  const request = event.request;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (request.method === "GET" && url.pathname.endsWith(".wasm") && url.searchParams.get("v")) {
    event.respondWith(cachedModule(event, url));
    return;
  }
  const isDocument = request.mode === "navigate";
  const isWorker = request.destination === "worker" || request.destination === "sharedworker";
  if (!isDocument && !isWorker) return;
  event.respondWith(isolate(request));
});

async function cachedModule(event, url) {
  const request = event.request;
  let cache = null;
  try {
    cache = await caches.open(WASM_CACHE);
    const hit = await cache.match(request);
    if (hit) return hit;
  } catch {
    // No Cache Storage (private browsing, quota): the network serves it.
  }
  const response = await fetch(request);
  if (cache && response.ok && response.type === "basic") {
    // Stored beside the page's own read of the body; an older build of the same module goes.
    const copy = response.clone();
    event.waitUntil(
      (async () => {
        for (const old of await cache.keys()) if (new URL(old.url).pathname === url.pathname && old.url !== request.url) await cache.delete(old);
        await cache.put(request, copy);
      })().catch(() => {}),
    );
  }
  return response;
}

async function isolate(request) {
  const response = await fetch(request);
  // Opaque and redirect responses cannot be rewritten; the browser follows redirects itself.
  if (response.status === 0 || response.type === "opaqueredirect" || response.type === "opaque") return response;
  const headers = new Headers(response.headers);
  headers.set("Cross-Origin-Opener-Policy", "same-origin");
  headers.set("Cross-Origin-Embedder-Policy", COEP);
  headers.set("Cross-Origin-Resource-Policy", "same-origin");
  const nullBody = [204, 205, 304].includes(response.status);
  return new Response(nullBody ? null : response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
