// The dedicated worker that runs one guest. host.js starts it from a blob URL
// that imports this module, so the worker inherits the page's cross-origin
// isolation even when a service worker (not a response header) provides it.
//
// Messages in: exactly one `{ t: "start", module, ring, clock, config, image }`.
// After that the worker is inside the guest (`_start`), in guest code or in
// `Atomics.wait`, and hears from the host only through the ring.
//
// Messages out: `hello`, `running`, then `batch` and `reply` messages while
// the guest runs, and a final `exit` (`reason` "return", "exit" or "trap"), or
// `failed` if the guest never started.

import { ProcExit, Wasi } from "./wasi.js";

function describe(err) {
  return {
    name: String((err && err.name) || "Error"),
    message: String(err && err.message !== undefined ? err.message : err),
    stack: String((err && err.stack) || ""),
  };
}

async function run({ module, ring, clock, config, image }) {
  const post = (message, transfer = []) => self.postMessage(message, transfer);
  const wasi = new Wasi({ config, ring, clock, image, post });
  const started = performance.now();
  const instance = await WebAssembly.instantiate(module, wasi.imports(module));
  wasi.attach(instance);
  const entry = instance.exports._start;
  if (typeof entry !== "function") {
    throw new Error("the module exports no _start: only WASI commands run here, not reactors");
  }
  wasi.flushOutput();
  post({
    t: "running",
    instantiateMs: performance.now() - started,
    layout: wasi.layout,
    env: wasi.envList,
    imports: WebAssembly.Module.imports(module).map((i) => `${i.module}.${i.name}`),
  });
  let exit;
  try {
    entry();
    exit = { reason: "return", code: 0 };
  } catch (err) {
    exit = err instanceof ProcExit ? { reason: "exit", code: err.code } : { reason: "trap", code: null, error: describe(err) };
  }
  try {
    wasi.finish();
  } catch (err) {
    exit.finishError = describe(err);
  }
  post({ t: "exit", ...exit, stats: wasi.snapshot() });
}

let started = false;
self.onmessage = (event) => {
  const message = event.data;
  if (!message || message.t !== "start" || started) return;
  started = true;
  run(message).catch((err) => self.postMessage({ t: "failed", error: describe(err) }));
};
self.postMessage({ t: "hello" });
