// End-to-end check of the browser WASI runtime (public/playground/wasi/) and
// the Cluster Lab's cross-origin isolation (public/docs/lab/coi*.js).
//
// 1. Builds the test guest (playground/wasi-guest) for wasm32-wasip1 with
//    cargo, from inside the crate so its .cargo/config.toml applies, and
//    optimises it with `wasm-opt -Oz`. The module stays in the crate's target
//    directory (or $CARGO_TARGET_DIR); nothing is staged into public/.
// 2. Serves a small virtual site: /playground/wasi/ and /docs/lab/ from
//    public/, the guest, and a harness page at /docs/lab/, the lab page's real
//    URL. Once with COOP/COEP response headers, once without them (the service
//    worker path), plus a cross-origin server for a CORP-less image.
// 3. Drives headless Chromium through Playwright: echo on two listeners, an
//    8 MiB transfer under backpressure, accepted, refused, piped and blocking
//    dials, the host-driven clock, `quiesce()`, the guest's file-system suite,
//    persistence across a page reload and a restart(), export and import,
//    kill(), exits, traps and unknown imports; then the service-worker path
//    and a browser without service workers. Prints timings.
//
// Usage: node scripts/check-wasi.mjs [--headed] [--only=headers,sw,none]
// Needs cargo with the wasm32-wasip1 target. Installs playwright-core (and
// binaryen when no wasm-opt is found) with `npm install --no-save`. Uses the
// Chromium in $PLAYWRIGHT_BROWSERS_PATH (default /opt/pw-browsers).

import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "..");
const crate = path.join(root, "playground", "wasi-guest");
const argv = process.argv.slice(2);
const HEADED = argv.includes("--headed");
const ONLY = (argv.find((a) => a.startsWith("--only=")) ?? "--only=headers,sw,none").slice(7).split(",");
process.env.PLAYWRIGHT_BROWSERS_PATH ??= "/opt/pw-browsers";

// ---- reporting --------------------------------------------------------------------------------

let passed = 0;
const failures = [];
const timings = [];

function check(name, ok, detail = "") {
  if (ok) {
    passed++;
    console.log(`  ok   ${name}${detail ? `: ${detail}` : ""}`);
  } else {
    failures.push(`${name}${detail ? ` (${detail})` : ""}`);
    console.log(`  FAIL ${name}${detail ? ` (${detail})` : ""}`);
  }
  return ok;
}

function timing(label, value) {
  timings.push([label, value]);
}

const ms = (value) => `${value.toFixed(value < 10 ? 2 : 1)} ms`;

// ---- build ------------------------------------------------------------------------------------

function run(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, { stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${cmd} ${args.join(" ")} exited with ${result.status}`);
}

function resolvable(name) {
  try {
    createRequire(path.join(root, "package.json")).resolve(name);
    return true;
  } catch {
    return false;
  }
}

function wasmOptOnPath() {
  try {
    execFileSync("wasm-opt", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

/**
 * Installs playwright-core and, without a wasm-opt on PATH, binaryen, with
 * `npm install --no-save`. Neither is in package.json, so npm prunes one when
 * it installs the other on its own: when either is missing, both go in one call.
 */
function ensureTools() {
  const wanted = [];
  if (!resolvable("playwright")) wanted.push("playwright-core");
  if (!wasmOptOnPath()) wanted.push("binaryen");
  const present = (name) => (name === "binaryen" ? fs.existsSync(path.join(root, "node_modules", "binaryen", "bin", "wasm-opt")) : resolvable(name));
  if (wanted.every(present)) return;
  console.log(`  installing ${wanted.join(" and ")} (npm install --no-save) ...`);
  try {
    run("npm", ["install", "--no-save", "--no-audit", "--no-fund", ...wanted], { cwd: root });
  } catch (err) {
    console.log(`  npm install failed (${err.message})`);
  }
}

// `[command, ...leading args]`: binaryen's npm wasm-opt is a Node script, run
// through Node itself because Windows cannot spawn the `.bin` shim directly.
function findWasmOpt() {
  if (process.argv.includes('--no-opt')) return null;
  const local = path.join(root, "node_modules", "binaryen", "bin", "wasm-opt");
  if (fs.existsSync(local)) return [process.execPath, local];
  if (wasmOptOnPath()) return ["wasm-opt"];
  console.log("  no wasm-opt: the guest runs unoptimised");
  return null;
}

function buildGuest() {
  const targetDir = process.env.CARGO_TARGET_DIR ? path.resolve(process.env.CARGO_TARGET_DIR) : path.join(crate, "target");
  const started = Date.now();
  try {
    execFileSync("rustup", ["target", "add", "wasm32-wasip1"], { cwd: crate, stdio: "ignore" });
  } catch {
    // No rustup: cargo reports a missing target itself.
  }
  run("cargo", ["build", "--release", "--target", "wasm32-wasip1"], {
    cwd: crate,
    env: { ...process.env, CARGO_TARGET_DIR: targetDir },
  });
  const cargoMs = Date.now() - started;
  const raw = path.join(targetDir, "wasm32-wasip1", "release", "krabka-wasi-guest.wasm");
  const optimised = path.join(targetDir, "wasm32-wasip1", "release", "krabka-wasi-guest.opt.wasm");
  const wasmOpt = findWasmOpt();
  let optMs = 0;
  let out = raw;
  if (wasmOpt) {
    const t = Date.now();
    run(wasmOpt[0], [
      ...wasmOpt.slice(1),
      "-Oz",
      "--enable-bulk-memory",
      "--enable-sign-ext",
      "--enable-mutable-globals",
      "--enable-nontrapping-float-to-int",
      "--enable-reference-types",
      "--enable-multivalue",
      raw,
      "-o",
      optimised,
    ]);
    optMs = Date.now() - t;
    out = optimised;
  }
  console.log(
    `  guest: cargo ${(cargoMs / 1000).toFixed(1)} s, wasm-opt ${(optMs / 1000).toFixed(1)} s; ` +
      `${fs.statSync(raw).size.toLocaleString("en")} -> ${fs.statSync(out).size.toLocaleString("en")} bytes (${out.startsWith(root) ? path.relative(root, out) : out})`,
  );
  return out;
}

/**
 * A hand-assembled command that calls `poll_oneoff` with no subscriptions and
 * two imports the shim does not know, and exits with
 * `poll_oneoff * 1e6 + sock_open * 1000 + mystery`: 28 052 052 when they answer
 * EINVAL, ENOSYS and ENOSYS.
 */
function unknownImportModule() {
  const utf8 = (s) => [...Buffer.from(s, "utf8")];
  const leb = (value) => {
    const out = [];
    for (;;) {
      const byte = value & 0x7f;
      value >>= 7;
      const done = (value === 0 && (byte & 0x40) === 0) || (value === -1 && (byte & 0x40) !== 0);
      out.push(done ? byte : byte | 0x80);
      if (done) return out;
    }
  };
  const vec = (items) => [...leb(items.length), ...items.flat()];
  const name = (s) => [...leb(utf8(s).length), ...utf8(s)];
  const section = (id, bytes) => [id, ...leb(bytes.length), ...bytes];
  const i32 = 0x7f;
  const types = vec([
    [0x60, 1, i32, 0], // 0: (i32) -> ()                 proc_exit
    [0x60, 3, i32, i32, i32, 1, i32], // 1: (i32 i32 i32) -> i32   sock_open
    [0x60, 0, 1, i32], // 2: () -> i32                  env.mystery
    [0x60, 4, i32, i32, i32, i32, 1, i32], // 3: (i32 x4) -> i32  poll_oneoff
    [0x60, 0, 0], // 4: () -> ()                     _start
  ]);
  const imports = vec([
    [...name("wasi_snapshot_preview1"), ...name("proc_exit"), 0x00, 0],
    [...name("wasi_snapshot_preview1"), ...name("sock_open"), 0x00, 1],
    [...name("env"), ...name("mystery"), 0x00, 2],
    [...name("wasi_snapshot_preview1"), ...name("poll_oneoff"), 0x00, 3],
  ]);
  const constant = (value) => [0x41, ...leb(value)];
  const body = [
    0, // no locals
    ...constant(0), ...constant(0), ...constant(0), ...constant(0), 0x10, 3, // poll_oneoff(0, 0, 0, 0)
    ...constant(1_000_000), 0x6c, // * 1e6
    ...constant(0), ...constant(0), ...constant(0), 0x10, 1, // sock_open(0, 0, 0)
    ...constant(1000), 0x6c, 0x6a, // * 1000, +
    0x10, 2, 0x6a, // + mystery()
    0x10, 0, // proc_exit(...)
    0x0b,
  ];
  const bytes = Uint8Array.from([
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
    ...section(1, types),
    ...section(2, imports),
    ...section(3, vec([[4]])),
    ...section(5, vec([[0x00, 1]])),
    ...section(7, vec([[...name("memory"), 0x02, 0], [...name("_start"), 0x00, 4]])),
    ...section(10, vec([[...leb(body.length), ...body]])),
  ]);
  if (!WebAssembly.validate(bytes)) throw new Error("the unknown-import module does not validate");
  return bytes;
}

// ---- serving ----------------------------------------------------------------------------------

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".wasm": "application/wasm",
  ".json": "application/json",
  ".md": "text/markdown; charset=utf-8",
  ".png": "image/png",
};

// A 1x1 PNG, served cross-origin without a Cross-Origin-Resource-Policy header.
const PIXEL = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64",
);

function harnessPage(imageUrl) {
  return `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>WASI runtime check</title></head>
<body>
<p>WASI runtime check harness (scripts/check-wasi.mjs)</p>
<img id="xo" alt="" src="${imageUrl}">
<script type="module">
  const params = new URLSearchParams(location.search);
  if (params.has("sw")) {
    const { ensureCrossOriginIsolation } = await import("/docs/lab/coi.js");
    window.coi = await ensureCrossOriginIsolation();
  }
  window.wasi = await import("/playground/wasi/host.js");
  window.ready = true;
</script>
</body>
</html>
`;
}

function serveSite({ headers, guest, unknownModule, imageUrl }) {
  const files = new Map([
    ["/wasi-test/guest.wasm", guest],
    ["/docs/lab/coi.js", path.join(root, "public", "docs", "lab", "coi.js")],
    ["/docs/lab/coi-sw.js", path.join(root, "public", "docs", "lab", "coi-sw.js")],
  ]);
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://localhost");
    const send = (status, type, body) => {
      const head = { "Content-Type": type, "Cache-Control": "no-store" };
      if (headers) {
        head["Cross-Origin-Opener-Policy"] = "same-origin";
        head["Cross-Origin-Embedder-Policy"] = "require-corp";
      }
      res.writeHead(status, head);
      res.end(body);
    };
    if (url.pathname === "/docs/lab") {
      res.writeHead(301, { Location: `/docs/lab/${url.search}` });
      res.end();
      return;
    }
    if (url.pathname === "/docs/lab/" || url.pathname === "/docs/lab/index.html") return send(200, MIME[".html"], harnessPage(imageUrl));
    if (url.pathname === "/docs/other/" || url.pathname === "/") {
      return send(200, MIME[".html"], `<!doctype html><meta charset="utf-8"><title>other</title><p>A page outside /docs/lab/.</p>`);
    }
    if (url.pathname === "/wasi-test/unknown.wasm") return send(200, MIME[".wasm"], unknownModule);
    let file = files.get(url.pathname);
    if (!file && url.pathname.startsWith("/playground/wasi/")) {
      file = path.join(root, "public", url.pathname);
      if (!file.startsWith(path.join(root, "public", "playground", "wasi"))) file = null;
    }
    if (!file || !fs.existsSync(file) || fs.statSync(file).isDirectory()) return send(404, "text/plain", "not found");
    send(200, MIME[path.extname(file)] ?? "application/octet-stream", fs.readFileSync(file));
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve({ origin: `http://127.0.0.1:${server.address().port}`, close: () => server.close() })));
}

function serveImage() {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "no-store" });
    res.end(PIXEL);
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve({ url: `http://127.0.0.1:${server.address().port}/pixel.png`, close: () => server.close() })));
}

// ---- browser ----------------------------------------------------------------------------------

async function loadPlaywright() {
  for (const name of ["playwright-core", "playwright"]) {
    if (!resolvable(name)) continue;
    const loaded = await import(name);
    return loaded.chromium ? loaded : loaded.default;
  }
  throw new Error("playwright-core is not installed and could not be installed (npm install --no-save playwright-core)");
}

function chromiumExecutable() {
  const base = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!base || !fs.existsSync(base)) return undefined;
  const dirs = fs
    .readdirSync(base)
    .filter((d) => /^chromium-\d+$/.test(d))
    .sort((a, b) => Number(b.split("-")[1]) - Number(a.split("-")[1]));
  for (const dir of dirs) {
    for (const sub of ["chrome-linux/chrome", "chrome-linux64/chrome", "chrome-mac/Chromium.app/Contents/MacOS/Chromium", "chrome-win/chrome.exe"]) {
      const candidate = path.join(base, dir, sub);
      if (fs.existsSync(candidate)) return candidate;
    }
  }
  return undefined;
}

/** Helpers installed in every page before its scripts run (serialised by Playwright). */
function installHelpers() {
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  class Reader {
    constructor(conn) {
      this.chunks = [];
      this.size = 0;
      this.waiters = [];
      this.closed = false;
      conn.on("data", (bytes) => {
        this.chunks.push(bytes);
        this.size += bytes.length;
        this.wake();
      });
      conn.on("end", () => {
        this.closed = true;
        this.wake();
      });
      conn.on("close", () => {
        this.closed = true;
        this.wake();
      });
    }

    wake() {
      for (const waiter of this.waiters.splice(0)) waiter();
    }

    flat() {
      if (this.chunks.length === 0) return new Uint8Array(0);
      if (this.chunks.length > 1) {
        const all = new Uint8Array(this.size);
        let at = 0;
        for (const chunk of this.chunks) {
          all.set(chunk, at);
          at += chunk.length;
        }
        this.chunks = [all];
      }
      return this.chunks[0];
    }

    async until(ready, timeoutMs, what) {
      const deadline = performance.now() + timeoutMs;
      while (!ready()) {
        if (this.closed) throw new Error(`the connection ended while waiting for ${what}`);
        const left = deadline - performance.now();
        if (left <= 0) throw new Error(`timed out waiting for ${what}`);
        await new Promise((resolve) => {
          const timer = setTimeout(resolve, left);
          this.waiters.push(() => {
            clearTimeout(timer);
            resolve();
          });
        });
      }
    }

    async take(n, timeoutMs = 15000) {
      await this.until(() => this.size >= n, timeoutMs, `${n} bytes`);
      const all = this.flat();
      const out = all.slice(0, n);
      this.chunks = [all.subarray(n)];
      this.size -= n;
      return out;
    }

    async line(timeoutMs = 15000) {
      let newline = -1;
      await this.until(
        () => {
          if (this.size === 0) return false;
          newline = this.flat().indexOf(10);
          return newline >= 0;
        },
        timeoutMs,
        "a line",
      );
      const all = this.flat();
      const text = dec.decode(all.subarray(0, newline));
      this.chunks = [all.subarray(newline + 1)];
      this.size -= newline + 1;
      return text;
    }

    async frame(timeoutMs = 15000) {
      const header = await this.take(4, timeoutMs);
      return this.take(new DataView(header.buffer).getUint32(0), timeoutMs);
    }
  }

  function frame(payload) {
    const bytes = typeof payload === "string" ? enc.encode(payload) : payload;
    const out = new Uint8Array(4 + bytes.length);
    new DataView(out.buffer).setUint32(0, bytes.length);
    out.set(bytes, 4);
    return out;
  }

  class Control {
    constructor(proc, port = 7070) {
      this.conn = proc.connect(port);
      this.reader = new Reader(this.conn);
    }

    async cmd(line, body = null, timeoutMs = 20000) {
      let payload = enc.encode(line);
      if (body) {
        const joined = new Uint8Array(payload.length + 1 + body.length);
        joined.set(payload);
        joined[payload.length] = 10;
        joined.set(body, payload.length + 1);
        payload = joined;
      }
      this.conn.send(frame(payload));
      const reply = await this.reader.frame(timeoutMs);
      const newline = reply.indexOf(10);
      return newline < 0 ? { line: dec.decode(reply), body: new Uint8Array(0) } : { line: dec.decode(reply.subarray(0, newline)), body: reply.slice(newline + 1) };
    }
  }

  /** Serves the other end of a guest's dial: reads frames, answers each upper-cased. */
  function upperPeer(conn) {
    const reader = new Reader(conn);
    (async () => {
      for (;;) {
        const request = await reader.frame(60_000).catch(() => null);
        if (!request) return;
        conn.send(frame(dec.decode(request).toUpperCase()));
      }
    })();
  }

  function summarize(samples) {
    const sorted = [...samples].sort((a, b) => a - b);
    const at = (p) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
    return { n: sorted.length, mean: sorted.reduce((a, b) => a + b, 0) / sorted.length, p50: at(0.5), p90: at(0.9), p99: at(0.99), max: sorted[sorted.length - 1] };
  }

  async function until(fn, timeoutMs = 15000, what = "a condition") {
    const deadline = performance.now() + timeoutMs;
    for (;;) {
      const value = await fn();
      if (value) return value;
      if (performance.now() > deadline) throw new Error(`timed out waiting for ${what}`);
      await sleep(10);
    }
  }

  function toBase64(bytes) {
    let binary = "";
    for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
    return btoa(binary);
  }

  function fromBase64(text) {
    return Uint8Array.from(atob(text), (c) => c.charCodeAt(0));
  }

  async function sha256(bytes) {
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    return [...digest].map((b) => b.toString(16).padStart(2, "0")).join("");
  }

  window.T = { Reader, Control, frame, upperPeer, summarize, until, sleep, enc, dec, toBase64, fromBase64, sha256 };
  window.S = {};
}

async function openHarness(context, origin, query = "") {
  const page = await context.newPage();
  const consoleLines = [];
  page.on("console", (message) => consoleLines.push(`[${message.type()}] ${message.text()}`));
  page.on("pageerror", (err) => consoleLines.push(`[pageerror] ${err.message}`));
  page.consoleLines = consoleLines;
  await page.goto(`${origin}/docs/lab/${query}`);
  await waitReady(page);
  return page;
}

/** Waits until the harness has loaded, across the helper's own reloads. */
async function waitReady(page, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const state = await page.evaluate(() => ({ ready: window.ready === true, coi: window.coi ?? null }));
      if (state.ready && !(state.coi && state.coi.reloading)) return state;
    } catch {
      // The page is navigating; try again.
    }
    if (Date.now() > deadline) throw new Error("the harness page did not load");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

async function step(page, name, fn, arg) {
  try {
    return await page.evaluate(fn, arg);
  } catch (err) {
    check(name, false, err.message.split("\n")[0]);
    console.log(page.consoleLines.slice(-25).map((line) => `      ${line}`).join("\n"));
    return null;
  }
}

// ---- in-page scenarios (serialised by Playwright; they see only window.T, window.S, window.wasi) ----

async function spawnMain({ volume, persistent = true }) {
  const { spawn, pipe } = window.wasi;
  const started = performance.now();
  const S = window.S;
  S.warnings = [];
  const ondial = (host, port, dial) => {
    if (host === "peer.test" && port === 7000) {
      const conn = dial.accept();
      window.T.upperPeer(conn);
      return conn;
    }
    if (host === "slow.test" && port === 7000) {
      return new Promise((resolve) =>
        setTimeout(() => {
          const conn = dial.accept();
          window.T.upperPeer(conn);
          resolve(conn);
        }, 50),
      );
    }
    if (host === "loop.test" && port === 9092) {
      const conn = dial.accept();
      pipe(conn, S.proc.connect(9092));
      return conn;
    }
    if (host === "clock.test" && port === 9092 && S.clockProc) {
      const conn = dial.accept();
      pipe(conn, S.clockProc.connect(9092));
      return conn;
    }
    return null;
  };
  S.proc = await spawn({
    module: "/wasi-test/guest.wasm",
    name: "guest-main",
    args: ["krabka-wasi-guest", "--check"],
    env: { RUST_BACKTRACE: "1", CHECK: "yes" },
    listeners: [9092, 9093, 7070, 9999],
    volume: persistent ? volume : null,
    ondial,
  });
  S.proc.on("warn", (text) => S.warnings.push(text));
  const running = performance.now();
  const control = new window.T.Control(S.proc);
  S.control = control;
  await window.T.until(() => control.conn.acceptedAt !== null, 15000, "the guest to accept");
  const pong = await control.cmd("PING");
  const env = await control.cmd("ENV");
  await window.T.until(() => S.proc.tail("stdout").some((line) => line.startsWith("ready ")), 15000, "the ready line");
  return {
    spawnMs: running - started,
    instantiateMs: S.proc.instantiateMs,
    startTimings: S.proc.startTimings,
    spawnToAcceptMs: control.conn.acceptedAt - started,
    connectToAcceptMs: control.conn.acceptedAt - control.conn.createdAt,
    pong: pong.line,
    env: window.T.dec.decode(env.body),
    layout: S.proc.layout,
    ready: S.proc.tail("stdout").find((line) => line.startsWith("ready ")),
    imports: S.proc.imports,
  };
}

async function echoFrames({ frames }) {
  const { S, T } = window;
  const accepts = [];
  for (let i = 0; i < 20; i++) {
    const probe = S.proc.connect(9092);
    await T.until(() => probe.acceptedAt !== null, 5000, "an accept");
    accepts.push(probe.acceptedAt - probe.createdAt);
    probe.close();
  }
  const plain = S.proc.connect(9092);
  const upper = S.proc.connect(9093);
  const plainReader = new T.Reader(plain);
  const upperReader = new T.Reader(upper);
  const text = (i, tag) => `${tag}-${String(i).padStart(6, "0")} hello from the host ${i * 7}`;
  const rtts = [];
  const pollsBefore = (await S.proc.stats()).guest.poll.calls;
  for (let i = 0; i < frames; i++) {
    const t = performance.now();
    plain.send(T.enc.encode(`${text(i, "frame")}\n`));
    const got = await plainReader.line();
    rtts.push(performance.now() - t);
    if (got !== text(i, "frame")) throw new Error(`listener 9092, frame ${i}: got ${JSON.stringify(got)}`);
  }
  const pollsPerFrame = ((await S.proc.stats()).guest.poll.calls - pollsBefore - 1) / frames;
  const started = performance.now();
  for (let i = 0; i < frames; i++) upper.send(T.enc.encode(`${text(i, "pipe")}\n`));
  for (let i = 0; i < frames; i++) {
    const got = await upperReader.line();
    if (got !== text(i, "pipe").toUpperCase()) throw new Error(`listener 9093, frame ${i}: got ${JSON.stringify(got)}`);
  }
  const pipelinedMs = performance.now() - started;
  plain.close();
  upper.close();
  // Idle: only the 100 ms heartbeat should wake the guest. A readiness spin would show thousands of polls.
  await T.sleep(100);
  const idle1 = (await S.proc.stats()).guest.poll;
  await T.sleep(1000);
  const idle2 = (await S.proc.stats()).guest.poll;
  return {
    rtt: T.summarize(rtts),
    accept: T.summarize(accepts),
    pipelinedMs,
    framesPerSec: frames / (pipelinedMs / 1000),
    pollsPerFrame,
    idlePolls: idle2.calls - idle1.calls,
    idleBusyPct: ((idle2.busyMs - idle1.busyMs) / 1000) * 100,
  };
}

async function bigTransfer({ megabytes, pauseMs }) {
  const { S, T } = window;
  const conn = S.proc.connect(9092);
  const total = megabytes << 20;
  const piece = 64 * 1024;
  const source = new Uint8Array(total);
  for (let at = 0; at < total; at += 65536) crypto.getRandomValues(source.subarray(at, at + 65536));
  const received = new Uint8Array(total);
  let got = 0;
  let drains = 0;
  let maxBuffered = 0;
  let ended = false;
  conn.on("drain", () => drains++);
  conn.on("data", (bytes) => {
    received.set(bytes, got);
    got += bytes.length;
  });
  conn.on("end", () => (ended = true));
  const before = (await S.proc.stats()).guest.net;
  const started = performance.now();
  const writing = (async () => {
    for (let at = 0; at < total; at += piece) {
      await conn.write(source.subarray(at, at + piece));
      maxBuffered = Math.max(maxBuffered, conn.bufferedAmount);
    }
    conn.end();
  })();
  if (pauseMs > 0) {
    await T.until(() => got >= 1 << 20, 30000, "the first MiB back");
    conn.pause();
    await T.sleep(pauseMs);
    maxBuffered = Math.max(maxBuffered, conn.bufferedAmount);
    conn.resume();
  }
  await T.until(() => got >= total && ended, 60000, `${megabytes} MiB back`);
  await writing;
  const elapsed = performance.now() - started;
  const after = (await S.proc.stats()).guest.net;
  const same = (await T.sha256(received)) === (await T.sha256(source));
  conn.close();
  return {
    same,
    got,
    elapsed,
    mbPerSec: total / (1 << 20) / (elapsed / 1000),
    drains,
    maxBuffered,
    guestWriteBlocked: after.writeBlocked - before.writeBlocked,
    guestReadAgain: after.readAgain - before.readAgain,
  };
}

async function dials() {
  const { S } = window;
  const time = async (line) => {
    const started = performance.now();
    const reply = await S.control.cmd(line);
    return { line: reply.line, ms: performance.now() - started };
  };
  const samples = [];
  for (let i = 0; i < 20; i++) samples.push((await time(`DIAL peer.test 7000 sample-${i}`)).ms);
  return {
    samples: window.T.summarize(samples),
    accepted: await time("DIAL peer.test 7000 hello-peer"),
    refused: await time("DIAL nowhere.test 1 anyone"),
    slow: await time("DIAL slow.test 7000 after-a-while"),
    piped: await time("DIAL loop.test 9092 through-the-host"),
    raw: await time("DIALRAW peer.test 7000 blocking-raw-calls"),
    rawRefused: await time("DIALRAW nowhere.test 2 nobody"),
    malformed: await time("DIAL bad:host 70000 x"),
    acceptRaw: await (async () => {
      const pending = S.control.cmd("ACCEPTRAW");
      await window.T.sleep(100);
      const conn = S.proc.connect(9999);
      const reader = new window.T.Reader(conn);
      conn.send(window.T.enc.encode("a blocking accept\n"));
      const echoed = await reader.line();
      const reply = await pending;
      conn.close();
      return { line: reply.line, echoed };
    })(),
  };
}

async function logLines() {
  const { S, T } = window;
  const stdout = [];
  const stderr = [];
  const offs = [S.proc.on("stdout", (line) => stdout.push(line)), S.proc.on("stderr", (line) => stderr.push(line))];
  await S.control.cmd("STDOUT héllo wörld ✓ from stdout");
  await S.control.cmd("STDERR a line on stderr");
  await T.until(() => stdout.length > 0 && stderr.length > 0, 5000, "log lines");
  offs.forEach((off) => off());
  return { stdout, stderr };
}

async function fsSuite() {
  const { S } = window;
  const started = performance.now();
  const reply = await S.control.cmd("FSTEST fs-check", null, 60000);
  const ls = await S.control.cmd("LS /data/fs-check/keep");
  return { line: reply.line, ms: performance.now() - started, ls: window.T.dec.decode(ls.body) };
}

async function readBack({ paths }) {
  const { S, T } = window;
  const out = {};
  for (const path of paths) {
    const reply = await S.control.cmd(`CAT ${path}`);
    out[path] = reply.line.startsWith("OK ") ? T.toBase64(reply.body) : `error: ${reply.line}`;
  }
  return out;
}

async function persistBeforeReload({ volume, paths }) {
  const { S, T, wasi } = window;
  const started = performance.now();
  await S.proc.flush();
  const flushMs = performance.now() - started;
  const stored = {};
  for (const path of paths) {
    const bytes = await wasi.readVolumeFile(volume, path.replace(/^\/data\//, ""));
    stored[path] = bytes ? T.toBase64(bytes) : null;
  }
  const archive = await wasi.exportVolume(volume);
  const stats = await S.proc.stats();
  return {
    flushMs,
    stored,
    usage: await wasi.usage(volume),
    volumes: await wasi.listVolumes(),
    archiveBytes: archive.length,
    archive: T.toBase64(archive),
    journal: stats.guest.journal,
    store: stats.host.store,
  };
}

async function respawnAndRead({ volume, paths, name }) {
  const { T, wasi } = window;
  const started = performance.now();
  const proc = await wasi.spawn({ module: "/wasi-test/guest.wasm", name, listeners: [9092, 9093, 7070], volume });
  const control = new T.Control(proc);
  const out = {};
  for (const path of paths) {
    const reply = await control.cmd(`CAT ${path}`);
    out[path] = reply.line.startsWith("OK ") ? T.toBase64(reply.body) : `error: ${reply.line}`;
  }
  const readMs = performance.now() - started;
  const stat = (await control.cmd("STAT /data/fs-check/big.bin")).line;
  window.S.respawned = proc;
  await proc.kill();
  return { readMs, files: out, stat };
}

async function importAndForget({ archive, volume }) {
  const { T, wasi } = window;
  const result = await wasi.importVolume(volume, T.fromBase64(archive));
  return { result, usage: await wasi.usage(volume) };
}

async function forgetVolume({ volume }) {
  const { wasi } = window;
  await wasi.forget(volume);
  return { usage: await wasi.usage(volume), volumes: (await wasi.listVolumes()).map((v) => v.id) };
}

async function restartKeepsVolume() {
  const { S, T } = window;
  const body = new Uint8Array(100_000);
  for (let at = 0; at < body.length; at += 65536) crypto.getRandomValues(body.subarray(at, at + 65536));
  const put = await S.control.cmd("PUT /data/restart/r.bin", body);
  const exits = [];
  const off = S.proc.on("exit", (info) => exits.push(info.reason));
  let oldClosed = false;
  S.control.conn.on("close", () => (oldClosed = true));
  const started = performance.now();
  await S.proc.restart();
  const restartMs = performance.now() - started;
  off();
  S.control = new T.Control(S.proc);
  const cat = await S.control.cmd("CAT /data/restart/r.bin");
  const keep = await S.control.cmd("CAT /data/fs-check/keep/blob.bin");
  return {
    put: put.line,
    same: cat.line === `OK ${body.length}` && (await T.sha256(cat.body)) === (await T.sha256(body)),
    keepLine: keep.line,
    restartMs,
    incarnation: S.proc.incarnation,
    exits,
    oldClosed,
    state: S.proc.state,
  };
}

async function hostClock() {
  const { S, T, wasi } = window;
  const clock = new wasi.WasiClock({ mode: "host", timeMs: 0, realtimeBaseMs: Date.UTC(2030, 0, 1) });
  S.clockProc = await wasi.spawn({ module: "/wasi-test/guest.wasm", name: "guest-clock", listeners: [9092, 9093, 7070], clock });
  const control = new T.Control(S.clockProc);
  const ticks = async () => Number((await control.cmd("TICKS")).line.split(" ")[1]);
  const now = async () => (await control.cmd("NOW")).line.split(" ").slice(1).map(BigInt);
  const atStart = await ticks();
  await T.sleep(300);
  const whilePaused = await ticks();
  const [mono0, real0] = await now();
  clock.advance(1000);
  await T.until(async () => (await ticks()) >= 10, 5000, "ten ticks after advancing 1000 ms");
  const afterAdvance = await ticks();
  const [mono1, real1] = await now();
  await T.sleep(300);
  const pausedAgain = await ticks();
  let slept = null;
  const sleeping = control.cmd("SLEEP 500").then((reply) => (slept = reply.line));
  await T.sleep(200);
  const beforeAdvance = slept;
  clock.advance(499);
  await T.sleep(150);
  const oneMsShort = slept;
  clock.advance(1);
  await sleeping;
  const crossProcess = await S.control.cmd("DIAL clock.test 9092 across-processes");
  clock.run();
  const realStart = await ticks();
  await T.sleep(550);
  const realAfter = await ticks();
  await S.clockProc.kill();
  return {
    atStart,
    whilePaused,
    afterAdvance,
    pausedAgain,
    beforeAdvance,
    oneMsShort,
    slept,
    monoDeltaNs: String(mono1 - mono0),
    realDeltaNs: String(real1 - real0),
    realtimeMs: Number(real1 / 1_000_000n),
    expectedRealtimeMs: Date.UTC(2030, 0, 1) + 1000,
    crossProcess: crossProcess.line,
    realModeTicks: realAfter - realStart,
  };
}

/**
 * `quiesce()` on a guest with a host-driven clock, the way the lab drives
 * one: at rest the guest waits on a timer no later than its first 100 ms
 * heartbeat; `quiesce()` answers only after the guest echoed what it was
 * sent; moving the clock from one reported deadline to the next up to 1 s
 * stops at every heartbeat (tokio's timer wheel also wakes at slot
 * boundaries in between) and fires all ten; bytes the guest leaves unread
 * behind a full window (on a listener it never accepts on) do not hold it up;
 * a killed process answers null.
 */
async function quiesceBarrier() {
  const { T, wasi } = window;
  const clock = new wasi.WasiClock({ mode: "host", timeMs: 0 });
  const proc = await wasi.spawn({ module: "/wasi-test/guest.wasm", name: "guest-quiesce", listeners: [9092, 9093, 7070, 9999], clock });
  const first = await proc.quiesce();
  const conn = proc.connect(9093);
  const reader = new T.Reader(conn);
  conn.send(T.enc.encode("barrier\n"));
  await proc.quiesce();
  const echoed = T.dec.decode(reader.flat());
  const stops = [];
  while (clock.now() < 1000 && stops.length < 200) {
    const next = await proc.quiesce();
    const to = Math.min(1000, next.deadlineMs ?? 1000);
    stops.push(to);
    clock.set(to);
  }
  const ticks = Number((await new T.Control(proc).cmd("TICKS")).line.split(" ")[1]);
  const unread = proc.connect(9999);
  unread.send(new Uint8Array(512 * 1024));
  const backpressured = await Promise.race([proc.quiesce(), T.sleep(3000).then(() => "no answer in 3 s")]);
  unread.close();
  conn.close();
  await proc.kill();
  const afterKill = await proc.quiesce();
  return { first, echoed, stops, ticks, backpressured, unreadBuffered: unread.bufferedAmount, afterKill };
}

async function killAndTraps() {
  const { T, wasi } = window;
  const spawnGuest = (name) => wasi.spawn({ module: "/wasi-test/guest.wasm", name, listeners: [9092, 9093, 7070] });

  const killed = await spawnGuest("guest-kill");
  const killControl = new T.Control(killed);
  await killControl.cmd("PING");
  let closeInfo = null;
  killControl.conn.on("close", (info) => (closeInfo = info));
  const killExit = killed.once("exit");
  await killed.kill();
  const killInfo = await killExit;

  const trapping = await spawnGuest("guest-trap");
  const traps = [];
  trapping.on("trap", (info) => traps.push(info));
  const trapExit = trapping.once("exit");
  new T.Control(trapping).conn.send(T.frame("PANIC boom from the check"));
  const trapInfo = await trapExit;

  const exiting = await spawnGuest("guest-exit");
  const exitExit = exiting.once("exit");
  new T.Control(exiting).conn.send(T.frame("EXIT 3"));
  const exitInfo = await exitExit;
  const finalStats = await exiting.stats();

  const unknown = await wasi.spawn({ module: "/wasi-test/unknown.wasm", name: "guest-unknown" });
  const warnings = [];
  unknown.on("warn", (text) => warnings.push(text));
  const unknownExit = await (unknown.state === "running" ? unknown.once("exit") : Promise.resolve(unknown.exitInfo));
  const unknownStats = await unknown.stats();

  return {
    killInfo,
    killState: killed.state,
    closeInfo,
    trapInfo,
    trap: traps[0] ? { name: traps[0].name, message: traps[0].message, stderrTail: traps[0].stderr.slice(-4) } : null,
    trapState: trapping.state,
    exitInfo,
    exitState: exiting.state,
    finalStatsUptime: finalStats.guest ? finalStats.guest.uptimeMs : null,
    unknownExit,
    unknownImports: unknownStats.guest ? unknownStats.guest.unknownImports : null,
    unknownCalls: unknownStats.guest ? { sock_open: unknownStats.guest.calls["wasi_snapshot_preview1.sock_open"], mystery: unknownStats.guest.calls["env.mystery"] } : null,
    warnings,
  };
}

async function edgeCases() {
  const { T, wasi } = window;
  const guest = (options) => wasi.spawn({ module: "/wasi-test/guest.wasm", listeners: [9092, 9093, 7070], ...options });
  const random = (n) => {
    const bytes = new Uint8Array(n);
    for (let at = 0; at < n; at += 65536) crypto.getRandomValues(bytes.subarray(at, at + 65536));
    return bytes;
  };
  const out = {};

  // A 4 KiB ring: records wrap, and the host waits for the worker's space notices.
  const small = await guest({ name: "guest-small-ring", ringBytes: 4096 });
  const conn = small.connect(9092);
  const reader = new T.Reader(conn);
  const payload = random(1 << 20);
  conn.send(payload);
  conn.end();
  const back = await reader.take(payload.length, 30000);
  out.smallRing = (await T.sha256(back)) === (await T.sha256(payload));
  await small.kill();

  // Journal backpressure: with 16 KiB allowed in flight the guest waits for IndexedDB commits.
  const slow = await guest({ name: "guest-journal", volume: "check-journal", journalMaxInFlight: 16 * 1024, journalIntervalMs: 1 });
  const control = new T.Control(slow);
  const body = random(512 * 1024);
  const put = await control.cmd("PUT /data/big.bin", body);
  await slow.flush();
  const stats = await slow.stats();
  const stored = await wasi.readVolumeFile("check-journal", "big.bin");
  out.journal = { put: put.line, waits: stats.guest.journal.waits, same: Boolean(stored) && (await T.sha256(stored)) === (await T.sha256(body)) };
  try {
    await guest({ name: "guest-duplicate", volume: "check-journal" });
    out.duplicate = "a second process started on the same volume";
  } catch (err) {
    out.duplicate = err.message;
  }
  await slow.kill();
  await wasi.forget("check-journal");

  // A reset: the guest's next read fails with ECONNRESET.
  const resetting = await guest({ name: "guest-reset" });
  const lines = [];
  resetting.on("stderr", (line) => lines.push(line));
  const victim = resetting.connect(9092);
  const victimReader = new T.Reader(victim);
  victim.send(T.enc.encode("ping\n"));
  await victimReader.line();
  victim.close({ reset: true });
  await T.until(() => lines.some((line) => /reset/i.test(line)), 5000, "the reset in the guest's log");
  out.reset = lines.find((line) => /reset/i.test(line));
  await resetting.kill();

  // A dial nobody answers times out.
  const waiting = await guest({ name: "guest-dial-timeout", dialTimeoutMs: 150, ondial: () => new Promise(() => {}) });
  out.dialTimeout = (await new T.Control(waiting).cmd("DIAL never.test 1 hello")).line;
  await waiting.kill();

  // A full backlog refuses the connection (the guest never accepts on its fourth listener).
  const busy = await guest({ name: "guest-backlog", listeners: [9092, 9093, 7070, 9999], backlog: 2 });
  const queued = [busy.connect(9999), busy.connect(9999), busy.connect(9999)];
  out.backlog = await Promise.race([queued[2].once("close"), T.sleep(3000).then(() => null)]);
  out.backlogOthers = queued.slice(0, 2).map((c) => c.state);
  await busy.kill();
  return out;
}

async function finalStats() {
  const { S } = window;
  const stats = await S.proc.stats();
  const calls = Object.entries(stats.guest.calls)
    .filter(([, n]) => n > 0)
    .sort((a, b) => b[1] - a[1]);
  return { calls, poll: stats.guest.poll, net: stats.guest.net, fs: stats.guest.fs, journal: stats.guest.journal, host: stats.host, faults: stats.guest.faults };
}

async function isolationProbe() {
  const image = document.getElementById("xo");
  await new Promise((resolve) => (image.complete ? resolve() : image.addEventListener("load", resolve) || image.addEventListener("error", resolve)));
  const registration = navigator.serviceWorker ? await navigator.serviceWorker.getRegistration() : null;
  return {
    isolated: self.crossOriginIsolated,
    coi: window.coi ?? null,
    controller: navigator.serviceWorker && navigator.serviceWorker.controller ? navigator.serviceWorker.controller.scriptURL : null,
    scope: registration ? registration.scope : null,
    crossOriginImageLoaded: image.naturalWidth === 1,
  };
}

// ---- scenario runners ---------------------------------------------------------------------------

const KEEP = [
  "/data/fs-check/keep/blob.bin",
  "/data/fs-check/keep/log.txt",
  "/data/fs-check/keep/sparse.bin",
  "/data/fs-check/keep/renamed.dat",
  "/data/fs-check/keep/nested/deep/file.txt",
];

const sameFiles = (a, b) => KEEP.every((p) => typeof a[p] === "string" && a[p] === b[p] && !a[p].startsWith("error"));

async function scenarioHeaders(browser, site) {
  console.log("\n== COOP/COEP headers (COEP require-corp)");
  const context = await browser.newContext();
  await context.addInitScript(installHelpers);
  let page = await openHarness(context, site.origin);
  const probe = await page.evaluate(isolationProbe);
  check("the page is cross-origin isolated by its headers", probe.isolated === true);
  check("require-corp blocks a CORP-less cross-origin image (what credentialless avoids)", probe.crossOriginImageLoaded === false);

  const volume = "check-main";
  const spawned = await step(page, "spawn", spawnMain, { volume });
  if (!spawned) return context.close();
  check("spawn: the guest accepted on the control listener", spawned.pong === "PONG", `running after ${ms(spawned.spawnMs)}, first accept after ${ms(spawned.spawnToAcceptMs)}`);
  check("dirs-first layout", JSON.stringify(spawned.layout) === JSON.stringify({ mount: 3, listeners: [4, 5, 6, 7], dial: 8 }), JSON.stringify(spawned.layout));
  check("the guest sees KRABKA_LISTEN_FDS, KRABKA_DIAL_FD and argv", /KRABKA_LISTEN_FDS=4,5,6,7\n/.test(spawned.env) && /KRABKA_DIAL_FD=8\n/.test(spawned.env) && /arg=--check/.test(spawned.env), spawned.env.trim().replace(/\n/g, " "));
  check("stdout arrives as lines", Boolean(spawned.ready), spawned.ready);
  const st = spawned.startTimings;
  timing("spawn() -> running", `${ms(spawned.spawnMs)} (volume image ${ms(st.imageMs)}, worker boot ${ms(st.bootMs)}, instantiate ${ms(st.instantiateMs)})`);
  timing("spawn() -> first accept", ms(spawned.spawnToAcceptMs));
  timing("connect() -> accept, right after spawn", ms(spawned.connectToAcceptMs));

  const echo = await step(page, "echo", echoFrames, { frames: 1000 });
  if (echo) {
    check("echo: 1,000 frames in order on listener 9092 and 1,000 on 9093", true, `RTT p50 ${ms(echo.rtt.p50)}, p99 ${ms(echo.rtt.p99)}`);
    timing("RTT, 1,000 sequential frames (p50 / p90 / p99 / max)", `${ms(echo.rtt.p50)} / ${ms(echo.rtt.p90)} / ${ms(echo.rtt.p99)} / ${ms(echo.rtt.max)}`);
    timing("pipelined echo, 1,000 frames", `${Math.round(echo.framesPerSec).toLocaleString("en")} frames/s (${ms(echo.pipelinedMs)})`);
    timing("connect() -> accept, idle guest (p50 / max of 20)", `${ms(echo.accept.p50)} / ${ms(echo.accept.max)}`);
    timing("poll_oneoff calls per sequential frame", echo.pollsPerFrame.toFixed(2));
    timing("idle guest (100 ms heartbeat): polls in 1 s, busy", `${echo.idlePolls}, ${echo.idleBusyPct.toFixed(2)} %`);
    check("an idle guest does not spin (edge-triggered readiness)", echo.idlePolls <= 40, `${echo.idlePolls} polls in 1 s, ${echo.idleBusyPct.toFixed(2)} % busy`);
  }

  const paused = await step(page, "8 MiB transfer with a paused reader", bigTransfer, { megabytes: 8, pauseMs: 200 });
  if (paused) {
    check("8 MiB echoed byte for byte through backpressure", paused.same && paused.got === 8 << 20, `${paused.got} bytes`);
    check("host sends waited for drain", paused.drains > 0 && paused.maxBuffered <= (1 << 20) + 64 * 1024, `${paused.drains} drains, max buffered ${paused.maxBuffered}`);
    check("guest writes met EAGAIN while the reader was paused", paused.guestWriteBlocked > 0, `${paused.guestWriteBlocked} blocked writes`);
  }
  const fast = await step(page, "8 MiB transfer", bigTransfer, { megabytes: 8, pauseMs: 0 });
  if (fast) {
    check("8 MiB echoed at full speed", fast.same, `${fast.mbPerSec.toFixed(1)} MB/s`);
    timing("8 MiB echo (host -> guest -> host)", `${ms(fast.elapsed)}, ${fast.mbPerSec.toFixed(1)} MB/s each way`);
  }

  const dialed = await step(page, "dials", dials);
  if (dialed) {
    check("a dial the host accepts", dialed.accepted.line === "DIALED HELLO-PEER", dialed.accepted.line);
    check("a dial the host refuses surfaces ECONNREFUSED", /^DIAL-ERR ConnectionRefused/.test(dialed.refused.line), dialed.refused.line);
    check("a dial accepted asynchronously", dialed.slow.line === "DIALED AFTER-A-WHILE", dialed.slow.line);
    check("a dial piped back into the guest's own listener", dialed.piped.line === "DIALED through-the-host", dialed.piped.line);
    check("blocking sock_send / sock_recv (peek, wait-all) / sock_shutdown", dialed.raw.line === "DIALED BLOCKING-RAW-CALLS", dialed.raw.line);
    check("a refused dial on a blocking socket", /^DIAL-ERR ConnectionRefused/.test(dialed.rawRefused.line), dialed.rawRefused.line);
    check("a malformed dial address is refused by the dialer", /^DIAL-ERR InvalidInput/.test(dialed.malformed.line) || /^DIAL-ERR/.test(dialed.malformed.line), dialed.malformed.line);
    check("a blocking accept on a listener without NONBLOCK", dialed.acceptRaw.line === "ACCEPTED a blocking accept" && dialed.acceptRaw.echoed === "A BLOCKING ACCEPT", JSON.stringify(dialed.acceptRaw));
    timing("DIAL command: dial, one frame each way (p50 / max of 20)", `${ms(dialed.samples.p50)} / ${ms(dialed.samples.max)}`);
    timing("DIAL command, refused", ms(dialed.refused.ms));
  }

  const lines = await step(page, "log lines", logLines);
  if (lines) {
    check("stdout line, UTF-8 intact", lines.stdout.includes("héllo wörld ✓ from stdout"), JSON.stringify(lines.stdout));
    check("stderr line", lines.stderr.includes("a line on stderr"));
  }

  const suite = await step(page, "fs suite", fsSuite);
  if (suite) {
    check("the guest's file-system suite", /^FS-OK \d+$/.test(suite.line), suite.line);
    timing("file-system suite", `${suite.line.replace("FS-OK ", "")} checks in ${ms(suite.ms)}`);
  }

  const clockResult = await step(page, "host-driven clock", hostClock);
  if (clockResult) {
    check("host clock: no ticks while the clock does not move", clockResult.atStart === 0 && clockResult.whilePaused === 0, `${clockResult.atStart}, ${clockResult.whilePaused}`);
    check("host clock: advancing 1000 ms fires ten 100 ms ticks", clockResult.afterAdvance === 10, String(clockResult.afterAdvance));
    check("host clock: paused again, no more ticks", clockResult.pausedAgain === 10, String(clockResult.pausedAgain));
    check("host clock: a 500 ms sleep waits for exactly 500 ms of host time", clockResult.beforeAdvance === null && clockResult.oneMsShort === null && /^SLEPT 500 500$/.test(clockResult.slept), `${clockResult.beforeAdvance} ${clockResult.oneMsShort} ${clockResult.slept}`);
    check("host clock: MONOTONIC and REALTIME move by the host's step", clockResult.monoDeltaNs === "1000000000" && clockResult.realDeltaNs === "1000000000", `${clockResult.monoDeltaNs} ${clockResult.realDeltaNs}`);
    check("host clock: REALTIME = base + host time", clockResult.realtimeMs === clockResult.expectedRealtimeMs, `${clockResult.realtimeMs}`);
    check("a dial piped to another guest process", clockResult.crossProcess === "DIALED across-processes", clockResult.crossProcess);
    check("switching the clock to real time resumes the ticks", clockResult.realModeTicks >= 3 && clockResult.realModeTicks <= 7, String(clockResult.realModeTicks));
  }

  const barrier = await step(page, "quiesce", quiesceBarrier);
  if (barrier) {
    const beats = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    const rising = barrier.stops.every((t, i) => i === 0 || t > barrier.stops[i - 1]);
    check("quiesce(): a guest at rest reports a timer no later than its first heartbeat", barrier.first && barrier.first.hostMs === 0 && barrier.first.deadlineMs > 0 && barrier.first.deadlineMs <= 100, JSON.stringify(barrier.first));
    check("quiesce() answers only after the guest answered its input", barrier.echoed === "BARRIER\n", JSON.stringify(barrier.echoed));
    check("stepping the clock from deadline to deadline fires every heartbeat on time", barrier.ticks === 10 && rising && beats.every((t) => barrier.stops.includes(t)), `${barrier.ticks} ticks, stops ${barrier.stops.join(" ")}`);
    check("quiesce() does not wait for bytes the guest leaves unread behind a full window", typeof barrier.backpressured === "object" && barrier.backpressured.deadlineMs > 1000, JSON.stringify(barrier.backpressured));
    check("quiesce() on a killed process resolves null", barrier.afterKill === null);
  }

  const stats = await step(page, "stats", finalStats);
  if (stats) {
    const calls = Object.fromEntries(stats.calls);
    check("stats: syscall counts, bytes in and out, blocked time", calls.poll_oneoff > 0 && stats.net.bytesIn > 8 << 20 && stats.net.bytesOut > 8 << 20 && stats.poll.blockedMs > 0, `${stats.calls.length} calls used, ${Math.round(stats.poll.blockedMs)} ms blocked`);
    check("no guest pointer faults", stats.faults === 0);
    console.log(`      calls: ${stats.calls.map(([n, c]) => `${n} ${c}`).join(", ")}`);
  }

  const before = await step(page, "read back", readBack, { paths: KEEP });
  const persisted = await step(page, "flush and export", persistBeforeReload, { volume, paths: KEEP });
  if (before && persisted) {
    check("IndexedDB holds the committed files byte for byte", sameFiles(before, persisted.stored));
    check("usage() and listVolumes()", persisted.usage.files > 0 && persisted.volumes.some((v) => v.id === volume), `${persisted.usage.files} files, ${persisted.usage.bytes} bytes, ${persisted.usage.chunks} chunks`);
    timing("flush() (journal -> IndexedDB)", ms(persisted.flushMs));
  }

  const restarted = await step(page, "restart", restartKeepsVolume);
  if (restarted) {
    check("restart() keeps the volume", restarted.same && restarted.keepLine === "OK 200000" && restarted.incarnation === 2 && restarted.state === "running", `${restarted.put}, ${restarted.keepLine}`);
    check("restart() closes the old connections and reports the old worker's exit", restarted.oldClosed && restarted.exits.includes("restart"));
    timing("restart() (flush, new worker, same volume)", ms(restarted.restartMs));
  }

  // Reload the page: every worker dies with it; the volume must come back from IndexedDB.
  const reloadStarted = Date.now();
  await page.reload();
  await waitReady(page);
  const after = await step(page, "respawn after reload", respawnAndRead, { volume, paths: KEEP, name: "guest-after-reload" });
  if (before && after) {
    check("after a page reload the files read back byte for byte", sameFiles(before, after.files));
    check("file times survive the reload to the nanosecond", after.stat === "OK file 200000 1600000000000000000", after.stat);
    timing("reload -> respawn -> read five files", `${Date.now() - reloadStarted} ms (respawn and reads ${ms(after.readMs)})`);
  }
  if (persisted) {
    const imported = await step(page, "import", importAndForget, { archive: persisted.archive, volume: "check-imported" });
    if (imported) check("exportVolume() -> importVolume()", imported.result.files === persisted.usage.files, `${imported.result.files} files, ${imported.result.bytes} bytes from a ${persisted.archiveBytes}-byte tar`);
    const fromImport = await step(page, "respawn on the imported volume", respawnAndRead, { volume: "check-imported", paths: KEEP, name: "guest-imported" });
    if (before && fromImport) check("a guest on the imported volume reads the same bytes", sameFiles(before, fromImport.files));
    const forgotten = await step(page, "forget", forgetVolume, { volume: "check-imported" });
    if (forgotten) check("forget() deletes a volume", forgotten.usage.files === 0 && !forgotten.volumes.includes("check-imported"));
  }

  const edges = await step(page, "edge cases", edgeCases);
  if (edges) {
    check("a 4 KiB ring: records wrap and the host waits for space", edges.smallRing === true);
    check("journal backpressure: the guest waits for IndexedDB and the data lands", edges.journal.put === "OK 524288" && edges.journal.waits > 0 && edges.journal.same, JSON.stringify(edges.journal));
    check("a volume runs in one process at a time", /in use/.test(edges.duplicate), edges.duplicate);
    check("a reset reaches the guest as ECONNRESET", Boolean(edges.reset), edges.reset);
    check("a dial nobody answers times out", /^DIAL-ERR TimedOut/.test(edges.dialTimeout), edges.dialTimeout);
    check("a full backlog refuses the next connection", edges.backlog && /backlog/.test(edges.backlog.reason) && edges.backlogOthers.every((s) => s === "pending"), JSON.stringify(edges.backlog));
  }

  const ends = await step(page, "kill, exit, trap, unknown imports", killAndTraps);
  if (ends) {
    check("kill() stops the guest and closes its connections", ends.killInfo.reason === "kill" && ends.killState === "killed" && ends.closeInfo !== null, JSON.stringify(ends.closeInfo));
    check("a panic is reported as a trap with the guest's stderr", ends.trapState === "trapped" && ends.trapInfo.reason === "trap" && ends.trap && /RuntimeError/.test(ends.trap.name) && ends.trap.stderrTail.some((l) => l.includes("boom from the check")), ends.trap ? `${ends.trap.name}: ${ends.trap.message}` : "no trap event");
    check("proc_exit(3) is reported as exit code 3", ends.exitInfo.reason === "exit" && ends.exitInfo.code === 3 && ends.exitState === "exited" && ends.finalStatsUptime > 0);
    check("poll_oneoff with no subscriptions answers EINVAL; unknown imports answer ENOSYS instead of trapping", ends.unknownExit && ends.unknownExit.code === 28_052_052, JSON.stringify(ends.unknownExit));
    check("unknown imports are logged once each", JSON.stringify(ends.unknownImports) === JSON.stringify(["wasi_snapshot_preview1.sock_open", "env.mystery"]), JSON.stringify(ends.unknownImports));
  }
  await context.close();
}

async function scenarioServiceWorker(browser, site) {
  console.log("\n== no headers: the lab's COI service worker");
  const context = await browser.newContext();
  await context.addInitScript(installHelpers);
  const page = await openHarness(context, site.origin, "?sw");
  const probe = await page.evaluate(isolationProbe);
  check("the helper made the page cross-origin isolated", probe.isolated === true && probe.coi && probe.coi.via === "service-worker", JSON.stringify(probe.coi));
  check("COEP credentialless: a CORP-less cross-origin image still loads", probe.coi && probe.coi.coep === "credentialless" && probe.crossOriginImageLoaded === true);
  check("the service worker is scoped to /docs/lab/", probe.scope === `${site.origin}/docs/lab/`, probe.scope);
  const other = await context.newPage();
  await other.goto(`${site.origin}/docs/other/`);
  const outside = await other.evaluate(() => ({ controlled: Boolean(navigator.serviceWorker && navigator.serviceWorker.controller), isolated: self.crossOriginIsolated }));
  check("pages outside /docs/lab/ are left alone", !outside.controlled && !outside.isolated, JSON.stringify(outside));
  await other.close();
  const redirected = await context.newPage();
  await redirected.goto(`${site.origin}/docs/lab`);
  await waitReady(redirected);
  check("/docs/lab redirects into the scope and is isolated", await redirected.evaluate(() => self.crossOriginIsolated && location.pathname === "/docs/lab/"));
  await redirected.close();

  const spawned = await step(page, "spawn behind the service worker", spawnMain, { volume: "sw-main" });
  if (spawned) check("spawn behind the service worker", spawned.pong === "PONG", `running after ${ms(spawned.spawnMs)}`);
  const echo = await step(page, "echo behind the service worker", echoFrames, { frames: 200 });
  if (echo) check("echo behind the service worker", true, `200 frames, RTT p50 ${ms(echo.rtt.p50)}`);
  const dialed = await step(page, "dials behind the service worker", dials);
  if (dialed) check("dials behind the service worker", dialed.accepted.line === "DIALED HELLO-PEER" && /ConnectionRefused/.test(dialed.refused.line));
  const suite = await step(page, "fs suite behind the service worker", fsSuite);
  if (suite) check("fs suite behind the service worker", /^FS-OK/.test(suite.line), suite.line);
  const before = await step(page, "read back", readBack, { paths: KEEP });
  await step(page, "flush", async () => window.S.proc.flush());
  await page.reload();
  const state = await waitReady(page);
  check("after a reload the service worker keeps the page isolated without another reload", state.coi && state.coi.isolated && !state.coi.reloading);
  const after = await step(page, "respawn after reload", respawnAndRead, { volume: "sw-main", paths: KEEP, name: "sw-after-reload" });
  if (before && after) check("the volume survives a reload behind the service worker", sameFiles(before, after.files));
  await context.close();
}

async function scenarioNoIsolation(browser, site) {
  console.log("\n== no headers, service workers blocked");
  const context = await browser.newContext({ serviceWorkers: "block" });
  await context.addInitScript(installHelpers);
  const page = await openHarness(context, site.origin, "?sw");
  const result = await page.evaluate(async () => {
    let error = null;
    try {
      await window.wasi.spawn({ module: "/wasi-test/guest.wasm", listeners: [7070] });
    } catch (err) {
      error = err.message;
    }
    return { coi: window.coi, isolated: self.crossOriginIsolated, error };
  });
  check("the helper reports why isolation is unavailable", result.coi && result.coi.isolated === false && result.coi.reloading === false && typeof result.coi.reason === "string", result.coi && result.coi.reason);
  check("spawn() refuses with a clear error", typeof result.error === "string" && /cross-origin isolated/.test(result.error), result.error);
  await context.close();
}

// ---- main ---------------------------------------------------------------------------------------

async function main() {
  console.log("WASI runtime check");
  ensureTools();
  const guest = buildGuest();
  const unknownModule = unknownImportModule();
  const { chromium } = await loadPlaywright();
  const executablePath = chromiumExecutable();
  const browser = await chromium.launch({ headless: !HEADED, executablePath, args: ["--no-sandbox"] });
  console.log(`  chromium ${browser.version()}${executablePath ? ` (${executablePath})` : ""}`);
  const image = await serveImage();
  const withHeaders = await serveSite({ headers: true, guest, unknownModule, imageUrl: image.url });
  const without = await serveSite({ headers: false, guest, unknownModule, imageUrl: image.url });
  try {
    if (ONLY.includes("headers")) await scenarioHeaders(browser, withHeaders);
    if (ONLY.includes("sw")) await scenarioServiceWorker(browser, without);
    if (ONLY.includes("none")) await scenarioNoIsolation(browser, without);
  } finally {
    await browser.close();
    withHeaders.close();
    without.close();
    image.close();
  }
  if (timings.length > 0) {
    console.log("\nTimings");
    const width = Math.max(...timings.map(([label]) => label.length));
    for (const [label, value] of timings) console.log(`  ${label.padEnd(width)}  ${value}`);
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length > 0) {
    for (const failure of failures) console.log(`  - ${failure}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
