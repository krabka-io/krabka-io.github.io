// Cross-origin isolation for the Cluster Lab on a static host.
//
// GitHub Pages cannot send the Cross-Origin-Opener-Policy and
// Cross-Origin-Embedder-Policy headers that make a page cross-origin
// isolated, and the lab's WASI runtime needs isolation for SharedArrayBuffer
// and Atomics.wait. This service worker adds the headers to the documents it
// controls. It lives in /docs/lab/, so it controls /docs/lab/ and nothing else
// on the site.
//
// Only documents (navigations) and worker scripts get the headers; every other
// request goes to the network untouched, under the page's policy. The COEP
// value comes from the registration URL: `?coep=credentialless` (the default,
// so cross-origin fonts and images without CORP headers keep loading) or
// `?coep=require-corp` for browsers without credentialless. `coi.js` picks it.

const COEP = new URL(self.location.href).searchParams.get("coep") === "require-corp" ? "require-corp" : "credentialless";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
  const request = event.request;
  const isDocument = request.mode === "navigate";
  const isWorker = request.destination === "worker" || request.destination === "sharedworker";
  if (!isDocument && !isWorker) return;
  if (new URL(request.url).origin !== self.location.origin) return;
  event.respondWith(isolate(request));
});

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
