// Real brokers as lab nodes: the page side of the `krabka-broker` node kind.
//
// The crate keeps an external node's slot in the world (links, faults,
// events, snapshots) but never runs it: this tab runs the real
// `krabka-broker`, compiled for `wasm32-wasip1`, in a Web Worker on the browser
// WASI runtime (`/playground/wasi/`), and this module carries the frames
// between the two. The process contract is `playground/docs/lab-real-broker.md`;
// the crate side is `playground/src/lab/external.rs`.
//
// Frames. The world holds a frame for an external node for its link latency
// and hands it over through `drainExternal()`. An `Open` to 9092 or 9093
// becomes a connection to that listener of the process, `Data` becomes bytes
// on it and `Close` its end. What the process writes goes back through
// `routeExternal()`: bytes on an accepted connection become `Data` frames from
// the node's listener endpoint to the client, its dials become connections
// from the node's client endpoint to the node behind the virtual address. A
// Kafka stream is cut into one `Data` frame per Kafka frame, length prefix
// included, the way the lab's nodes expect them; any other stream travels in
// the chunks the process writes. Frames for the process wait in this module
// while its connection's send buffer is full; none is dropped.
//
// Time. Every process of the tab reads one host-driven `WasiClock` that
// follows the world's clock. While a process runs, the world steps event by
// event (`world.js`), and before it lets time move on it waits until every
// process that got a frame, or whose timer fell due, has answered and blocked
// again (`WasiProcess.quiesce`). A process therefore answers at the logical
// instant its input arrived, its timers fire at their deadlines, a paused lab
// stops them, and the links' latencies hold end to end.
//
// Lifecycle. kill → the process is killed; restart → a new process on the
// same volume; wipe and a configuration change → the volume is forgotten and a
// fresh process starts; removal → killed and forgotten. A process that exits
// or traps by itself is reported through `exited`, and the page kills the node
// in the world. The module (about 18 MB) is downloaded and compiled once per
// tab, whatever the number of nodes, and the nodes show the download.

import { base64ToBytes, bytesToBase64 } from "./storage.js";

/** The node kind of a real broker. */
export const REAL_BROKER_KIND = "krabka-broker";
/** Where the site serves the broker's `wasm32-wasip1` module. */
export const BROKER_MODULE_URL = new URL("../broker/krabka-broker.wasm", import.meta.url).href;
/** What a node shows when that module is missing. */
export const MISSING_BUILD = "the real broker build is not on this site yet";
export const KAFKA_PORT = 9092;
export const CONTROLLER_PORT = 9093;
/** The process's listeners, in the order its descriptors are preopened. */
export const LISTENER_PORTS = [KAFKA_PORT, CONTROLLER_PORT];

const CLIENT_PORT = 0;
// A dial across a down link waits for the link as long as this much lab
// time, then fails with ETIMEDOUT: Kafka's longest connection setup
// (`socket.connection.setup.timeout.max.ms`).
export const BLACK_HOLE_DIAL_MS = 30_000;
// The page's configuration keys of a real broker, and where each lands in the
// JSON form of the broker's `broker.toml` (`FileConfig`) that the process
// gets as `KRABKA_CONFIG`. `voter` is not one: it decides `KRABKA_VOTERS`.
// The bounds fit the broker's field types (`i32`, `i16` and a duration).
const I32_MAX = 2_147_483_647;
export const CONFIG_KEYS = Object.freeze({
  voter: { type: "boolean" },
  rack: { type: "string", path: ["rack"] },
  num_partitions: { type: "integer", min: 1, max: I32_MAX, path: ["runtime", "num_partitions"] },
  default_replication_factor: { type: "integer", min: 1, max: 32767, path: ["runtime", "default_replication_factor"] },
  min_insync_replicas: { type: "integer", min: 1, max: I32_MAX, path: ["runtime", "default_min_insync_replicas"] },
  replica_lag_time_max_ms: { type: "integer", min: 1, max: I32_MAX, path: ["replica_lag_time_max"], unit: "ms" },
});
// A test (or a curious reader) can point this tab at another module of this
// site; the override lives in sessionStorage so it survives the isolation
// reload, and only same-origin URLs are taken.
const MODULE_OVERRIDE_KEY = "krabka-lab.broker-module";
// Kafka's `socket.request.max.bytes`: a larger length prefix is not a Kafka frame.
const MAX_KAFKA_FRAME = 100 * 1024 * 1024;
// The bytes a connection may hold in the runtime's send buffer; the rest waits here.
const HOLD_HIGH_WATER = 1 << 20;
const TAIL_LINES = 40;
const STATS_INTERVAL_MS = 1000;
const PUBLISH_DELAY_MS = 100;
// A dial times out on the lab's clock (`BLACK_HOLE_DIAL_MS`), so the
// runtime's wall-clock dial timeout never fires.
const NO_DIAL_TIMEOUT_MS = 2_147_483_647;
// Why the runtime closed a connection when the process went away: the world
// closes the lab side of every connection of a node it kills.
const PROCESS_GONE = new Set(["kill", "exit", "trap", "restart", "failed"]);
// Exits the lab caused itself.
const STOPPED_BY_LAB = new Set(["kill", "restart", "failed"]);
const RAW = "raw";
const encoder = new TextEncoder();

// The runtime is loaded on first use: most readers never add a real broker.
let runtimeModule = null;
function runtime() {
  runtimeModule ??= import("../wasi/host.js");
  return runtimeModule;
}

// Whether this browser keeps any real broker volume: the runtime's database
// exists. Asking first keeps the page from creating that database for a
// reader who never ran a real broker.
async function volumesKept() {
  try {
    if (typeof indexedDB === "undefined" || typeof indexedDB.databases !== "function") return true;
    const { DB_NAME } = await import("../wasi/volumes.js");
    return (await indexedDB.databases()).some((db) => db.name === DB_NAME);
  } catch {
    return false;
  }
}

// One download and compile per module URL, shared by every node of the tab:
// `{ promise, loaded, total, compiling }`, where `total` is 0 while unknown (a
// compressed response says only its compressed length).
const modules = new Map();

function loadModule(url) {
  let entry = modules.get(url);
  if (entry) return entry;
  entry = { promise: null, loaded: 0, total: 0, compiling: false };
  entry.promise = (async () => {
    const response = await fetch(url);
    if (!response.ok) {
      const err = new Error(`cannot fetch ${url}: HTTP ${response.status}`);
      err.status = response.status;
      throw err;
    }
    if (!response.headers.get("content-encoding")) entry.total = Number(response.headers.get("content-length")) || 0;
    if (!response.body) {
      entry.compiling = true;
      return WebAssembly.compile(await response.arrayBuffer());
    }
    // One branch compiles as it streams in, the other counts the bytes.
    const [compile, count] = response.body.tee();
    const counting = (async () => {
      const reader = count.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        entry.loaded += value.length;
      }
      entry.compiling = true;
    })();
    const module = await WebAssembly.compileStreaming(new Response(compile, { headers: { "content-type": "application/wasm" } }));
    await counting;
    return module;
  })();
  entry.promise.catch(() => {
    if (modules.get(url) === entry) modules.delete(url);
  });
  modules.set(url, entry);
  return entry;
}

function megabytes(n) {
  return `${(n / 1e6).toFixed(1)} MB`;
}

// ---- the contract -------------------------------------------------------------------------------

/** The virtual IPv4 address of node `id`: `10.0.(id >> 8).(id & 255)`, as `net::node_ip`. */
export function nodeIp(id) {
  const n = Number(id) >>> 0;
  return `10.0.${(n >>> 8) & 255}.${n & 255}`;
}

/** The node a virtual address names, or null: the inverse of `nodeIp`, as `net::node_for_ip`. */
export function nodeForIp(text) {
  const m = /^10\.0\.(0|[1-9]\d{0,2})\.(0|[1-9]\d{0,2})$/.exec(String(text).trim());
  if (!m) return null;
  const high = Number(m[1]);
  const low = Number(m[2]);
  if (high > 255 || low > 255) return null;
  const id = high * 256 + low;
  return id === 0 ? null : id;
}

/** The IndexedDB volume of a real broker: `<scenario id>/<node id>`. */
export function volumeName(scenarioId, nodeId) {
  return `${scenarioId}/${nodeId}`;
}

function base64Url(bytes) {
  return bytesToBase64(bytes).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * The Kafka cluster id of a scenario: 22 characters of URL-safe base64 of the
 * first 16 bytes of SHA-256("krabka-lab/cluster-id/<scenario id>"), with the
 * UUID version (8) and variant bits set. Like Kafka's `Uuid.randomUuid()` it
 * never starts with "-": the text is hashed again with "/1", "/2", ...
 * appended until it does not. A formatted volume therefore matches its
 * scenario across reloads.
 */
export async function clusterIdFor(scenarioId) {
  for (let salt = 0; ; salt++) {
    const text = `krabka-lab/cluster-id/${scenarioId}${salt ? `/${salt}` : ""}`;
    const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(text))).slice(0, 16);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const id = base64Url(bytes);
    if (!id.startsWith("-")) return id;
  }
}

/**
 * A real broker's configuration, checked: `{ voter, fileConfig }`. `voter`
 * (true by default) puts the node in `KRABKA_VOTERS`; every other key of
 * `CONFIG_KEYS` lands at its path in `fileConfig`, the JSON form of the
 * broker's `FileConfig`, with durations as the broker's unit strings
 * (`"10000ms"`). The keys come in `CONFIG_KEYS` order, whatever order the
 * scenario keeps them in, so the same configuration is the same
 * `KRABKA_CONFIG`. An unknown key or a value of the wrong type is an error,
 * as for the crate's node kinds.
 */
export function parseConfig(config) {
  const c = config && typeof config === "object" && !Array.isArray(config) ? config : {};
  const unknown = Object.keys(c).find((key) => !Object.hasOwn(CONFIG_KEYS, key));
  if (unknown !== undefined) throw new Error(`unknown config key \`${unknown}\``);
  const fileConfig = {};
  let voter = true;
  for (const [key, spec] of Object.entries(CONFIG_KEYS)) {
    const value = c[key];
    if (value === null || value === undefined) continue;
    if (spec.type === "boolean") {
      if (typeof value !== "boolean") throw new Error(`config key \`${key}\` is a boolean`);
    } else if (spec.type === "string") {
      if (typeof value !== "string" || !value) throw new Error(`config key \`${key}\` is a non-empty string`);
    } else if (!Number.isInteger(value) || value < spec.min || (spec.max !== undefined && value > spec.max)) {
      throw new Error(`config key \`${key}\` is an integer from ${spec.min}${spec.max !== undefined ? ` to ${spec.max}` : ""}`);
    }
    if (key === "voter") {
      voter = value;
      continue;
    }
    let at = fileConfig;
    for (const step of spec.path.slice(0, -1)) at = at[step] ??= {};
    at[spec.path[spec.path.length - 1]] = spec.unit ? `${value}${spec.unit}` : value;
  }
  return { voter, fileConfig };
}

// The settings the lab's old simulated broker shared with the real one, by
// the simulated name; the value is the real key.
const SIMULATED_CONFIG = Object.freeze({
  rack: "rack",
  voter: "voter",
  default_partitions: "num_partitions",
  default_replication_factor: "default_replication_factor",
  min_insync_replicas: "min_insync_replicas",
  replica_lag_time_max_ms: "replica_lag_time_max_ms",
});

/**
 * The configuration a real broker takes over from an old simulated broker's:
 * the settings both had, under the real names, and only the values the real
 * validator accepts (the simulated `-1` replication factor meant "every
 * broker" and has no real equivalent). Everything else is dropped, so the
 * converted node is valid rather than `invalid`.
 */
export function realConfigFromSimulated(old) {
  const out = {};
  if (!old || typeof old !== "object" || Array.isArray(old)) return out;
  for (const [from, to] of Object.entries(SIMULATED_CONFIG)) {
    if (old[from] == null) continue;
    try {
      parseConfig({ [to]: old[from] });
      out[to] = old[from];
    } catch {
      // The real broker would reject this value, so it does not come along.
    }
  }
  return out;
}

/** The ids of the scenario's real brokers that are KRaft voters, ascending. */
export function votersOf(scenario) {
  const ids = [];
  for (const n of scenario.nodes || []) {
    if (n.kind !== REAL_BROKER_KIND) continue;
    try {
      if (parseConfig(n.config).voter) ids.push(Number(n.id));
    } catch {
      // A node with a bad configuration never runs, so it votes for nobody.
    }
  }
  return ids.sort((a, b) => a - b);
}

/**
 * The environment of the process behind node `nodeId`, on top of what the
 * runtime sets (`KRABKA_LISTEN_FDS`, `KRABKA_LISTEN_PORTS`, `KRABKA_DIAL_FD`).
 * `logLevel` is a `KRABKA_LOG` directive; left out, the broker logs its default.
 */
export function processEnv({ nodeId, voters, clusterId, fileConfig, logLevel }) {
  return {
    KRABKA_NODE_ID: String(nodeId),
    KRABKA_HOST: nodeIp(nodeId),
    KRABKA_VOTERS: voters.map((id) => `${id}@${nodeIp(id)}:${CONTROLLER_PORT}`).join(","),
    KRABKA_CLUSTER_ID: clusterId,
    KRABKA_CONFIG: JSON.stringify(fileConfig),
    ...(logLevel ? { KRABKA_LOG: logLevel } : {}),
  };
}

/** Whether the scenario has a real broker. */
export function hasRealBroker(scenario) {
  return (scenario?.nodes || []).some((n) => n.kind === REAL_BROKER_KIND);
}

// ---- framing ------------------------------------------------------------------------------------

/** Whether `bytes` is exactly one Kafka frame: a big-endian length prefix and that many bytes. */
export function isKafkaFrame(bytes) {
  return bytes.length >= 4 && new DataView(bytes.buffer, bytes.byteOffset, 4).getInt32(0) === bytes.length - 4;
}

/**
 * Cuts a byte stream into Kafka frames, each with its 4-byte length prefix. A
 * length prefix that is negative or larger than `max` means the stream is not
 * Kafka: from there on the bytes pass through as they come.
 */
export class KafkaFramer {
  constructor(max = MAX_KAFKA_FRAME) {
    this.max = max;
    this.chunks = [];
    this.size = 0;
    this.need = -1;
    this.raw = false;
  }

  /** Bytes held back, waiting for the rest of their frame. */
  get buffered() {
    return this.size;
  }

  /** Feeds bytes; returns the frames they complete. */
  push(bytes) {
    if (this.raw) return bytes.length ? [bytes] : [];
    if (bytes.length) {
      this.chunks.push(bytes);
      this.size += bytes.length;
    }
    const out = [];
    for (;;) {
      if (this.need < 0) {
        if (this.size < 4) break;
        const head = this.take(4, true);
        const len = new DataView(head.buffer, head.byteOffset, 4).getInt32(0);
        if (len < 0 || len > this.max) {
          this.raw = true;
          if (this.size) out.push(this.take(this.size, false));
          break;
        }
        this.need = 4 + len;
      }
      if (this.size < this.need) break;
      out.push(this.take(this.need, false));
      this.need = -1;
    }
    return out;
  }

  // The first `n` held bytes, removed unless `peek`.
  take(n, peek) {
    const out = new Uint8Array(n);
    let at = 0;
    let i = 0;
    while (at < n) {
      const chunk = this.chunks[i];
      const m = Math.min(n - at, chunk.length);
      out.set(m === chunk.length ? chunk : chunk.subarray(0, m), at);
      at += m;
      if (peek) {
        i++;
        continue;
      }
      if (m === chunk.length) this.chunks.shift();
      else this.chunks[0] = chunk.subarray(m);
    }
    if (!peek) this.size -= n;
    return out;
  }
}

// ---- frames -------------------------------------------------------------------------------------

const endpoint = (e) => ({ node: Number(e.node), port: Number(e.port) });
const openFrame = (src, dst, conn) => ({ src: endpoint(src), dst: endpoint(dst), conn, payload: { kind: "open" } });
const closeFrame = (src, dst, conn) => ({ src: endpoint(src), dst: endpoint(dst), conn, payload: { kind: "close" } });
const dataFrame = (src, dst, conn, bytes) => ({ src: endpoint(src), dst: endpoint(dst), conn, payload: { kind: "data", data: bytesToBase64(bytes) } });

// Whether the link between two nodes drops frames: cut, or either node isolated.
function linkDown(world, a, b) {
  if (a === b) return false;
  const na = world.nodes.find((n) => n.id === a);
  const nb = world.nodes.find((n) => n.id === b);
  if (na?.isolated || nb?.isolated) return true;
  return (world.links || []).some((l) => l.cut && ((l.a === a && l.b === b) || (l.a === b && l.b === a)));
}

function summarizeStats(stats) {
  const g = stats?.guest;
  if (!g) return null;
  const round = (v) => (typeof v === "number" ? Math.round(v) : null);
  return {
    uptime_ms: round(g.uptimeMs),
    clock_ms: g.clock?.hostMs ?? null,
    polls: g.poll?.calls ?? null,
    blocked_ms: round(g.poll?.blockedMs),
    busy_ms: round(g.poll?.busyMs),
    bytes_in: g.net?.bytesIn ?? null,
    bytes_out: g.net?.bytesOut ?? null,
    sockets: g.net?.sockets ?? null,
    accepted: g.net?.accepted ?? null,
    dials: g.net?.dials ?? null,
    files: g.fs?.files ?? null,
    file_bytes: g.fs?.bytes ?? null,
    syncs: g.fs?.syncs ?? null,
    journal_flushes: g.journal?.flushes ?? null,
    journal_bytes: g.journal?.bytes ?? null,
  };
}

function describeExit(info) {
  if (info.reason === "trap") return `trapped: ${info.error?.message || "the guest trapped"}`;
  if (info.reason === "return") return "exited: its main returned";
  return `exited with code ${info.code}`;
}

// ---- one real broker ----------------------------------------------------------------------------

class RealNode {
  constructor(id) {
    this.id = id;
    // "loading" | "waiting" | "invalid" | "unavailable" | "booting" | "running"
    // | "exited" | "trapped" | "killed" | "failed"
    this.state = "loading";
    this.reason = "";
    this.gen = 0; // bumps whenever the process is stopped or replaced
    this.proc = null; // the current or last WasiProcess
    this.volume = null;
    this.env = null;
    this.incarnation = 0;
    this.origin = null; // why the next process starts: "restarted" | "wiped"; null is the first start
    this.startedAt = null;
    this.exit = null;
    this.pending = []; // frames that arrived before the process started
    this.servers = new Map(); // "<client node>:<client port>:<conn>" → Conn
    this.clients = new Map(); // conn id → Conn, the process's dials
    this.waitingDials = []; // dials across a down link: { target, port, dial, resolve, deadline }
    this.nextConn = 1;
    this.dirty = false; // got input or showed activity since it last blocked
    this.settling = null; // the quiesce in flight
    this.ready = false; // has blocked once since it started: it runs in lockstep
    this.lagging = false; // missed a lockstep wait; runs free until it blocks again
    this.deadline = Infinity; // lab ms of its next timer
    this.stats = null;
    this.notes = [];
    this.publishTimer = 0;
  }

  get running() {
    return this.proc !== null && this.proc.state === "running";
  }

  get inStep() {
    return this.ready && !this.lagging && this.running;
  }

  // A connection id of this node's client endpoint that no live dial uses.
  allocConn() {
    for (;;) {
      const id = this.nextConn;
      this.nextConn = id >= 0x7fff_ffff ? 1 : id + 1;
      if (!this.clients.has(id)) return id;
    }
  }
}

// ---- the host -----------------------------------------------------------------------------------

export class ExternalHost {
  // hooks:
  //   route(frames)            frames the processes sent, for `routeExternal`
  //   publish(id, state)       a node's state, for `applyRemoteSnapshot`
  //   world()                  the world snapshot now: nodes (kind, hosted, alive, isolated) and links
  //   now()                    the world's clock, in lab ms
  //   scenario()               the scenario document, with its id
  //   event(id, kind, detail)  a timeline entry about a process
  //   exited(id, exit)         a process ended by itself: the page kills the node
  //   logLevel(id)             the `KRABKA_LOG` directive the node starts with (optional; "" is the default)
  //   log(id, entry)           for the Logs tab (optional): a line `{ stream, text, base }`, `base` being the lab
  //                            time the process started at, or a lifecycle row `{ marker, message, level, detail }`
  constructor(hooks) {
    this.hooks = hooks;
    this.nodes = new Map();
    this.clock = null;
    this.clockMs = 0;
    this.out = [];
    this.outQueued = false;
    this.volumeBusy = new Map(); // volume → the stop that frees it
    this.isolation = null; // { isolated, reason } once the page asked
    this.statsTimer = 0;
  }

  // ---- the module ---------------------------------------------------------------------------

  /** The module URL this tab runs: the site's build, or a same-origin override. */
  moduleUrl() {
    try {
      const value = sessionStorage.getItem(MODULE_OVERRIDE_KEY);
      if (value) {
        const url = new URL(value, location.href);
        if (url.origin === location.origin) return url.href;
      }
    } catch {
      // No sessionStorage: the site's build.
    }
    return BROKER_MODULE_URL;
  }

  /** Runs another same-origin module from the next launch on (null: the site's build). For tests. */
  useModule(url) {
    try {
      if (url) sessionStorage.setItem(MODULE_OVERRIDE_KEY, new URL(url, location.href).href);
      else sessionStorage.removeItem(MODULE_OVERRIDE_KEY);
    } catch {
      // No sessionStorage: nothing to remember.
    }
  }

  /** Whether the module is on the site: false only for a 404, or when it cannot be reached at all. */
  async moduleAvailable() {
    try {
      const response = await fetch(this.moduleUrl(), { method: "HEAD", cache: "no-store" });
      return response.status !== 404;
    } catch {
      return false;
    }
  }

  /** What the page learned about cross-origin isolation: `{ isolated, reason }`. */
  setIsolation(info) {
    this.isolation = info;
    if (info && !info.isolated && info.reason) {
      for (const node of this.nodes.values()) {
        // `unavailable` also records a fresh timeline event, replacing the "page reloads once" promise.
        if (node.state === "unavailable" && node.reason !== MISSING_BUILD) this.unavailable(node, `cross-origin isolation is unavailable: ${info.reason}`);
      }
    }
  }

  // ---- lifecycle, driven by the world wrapper -----------------------------------------------

  /** Whether this tab hosts a real broker: the world then steps event by event. */
  get active() {
    return this.nodes.size > 0;
  }

  /** A new world: every process stops (volumes stay) and the clock starts over. */
  reset() {
    for (const node of this.nodes.values()) this.stopProcess(node);
    this.nodes.clear();
    this.clock = null;
    this.clockMs = 0;
    this.out = [];
  }

  /** Starts a process for every real broker this tab hosts, and stops the ones gone. */
  sync() {
    const world = this.hooks.world();
    if (!world) return;
    const doc = this.hooks.scenario();
    const present = new Set((doc.nodes || []).map((n) => n.id));
    const wanted = new Map();
    for (const n of world.nodes) if (n.kind === REAL_BROKER_KIND && n.hosted) wanted.set(n.id, n);
    for (const [id, node] of this.nodes) {
      if (wanted.has(id)) continue;
      this.stopProcess(node, { forget: !present.has(id) });
      this.nodes.delete(id);
    }
    for (const [id, n] of wanted) {
      const known = this.nodes.get(id);
      if (known) {
        if (known.state === "waiting" && doc.id) this.launch(known);
        continue;
      }
      const node = new RealNode(id);
      this.nodes.set(id, node);
      if (n.alive) this.launch(node);
      else this.setState(node, "killed", "");
    }
  }

  /** A fault the world applied. Link faults need nothing but a second look at waiting dials. */
  fault(fault) {
    const node = fault && fault.node != null ? this.nodes.get(Number(fault.node)) : null;
    switch (fault?.kind) {
      case "kill":
        if (node) this.halt(node);
        break;
      case "restart":
        if (node) this.relaunch(node, false);
        break;
      case "wipe":
        if (node) this.relaunch(node, true);
        break;
      default:
        break;
    }
    this.recheckDials();
  }

  /** The node left the scenario: its process stops and its volume is forgotten. */
  nodeRemoved(id) {
    const node = this.nodes.get(Number(id));
    if (!node) return;
    this.stopProcess(node, { forget: true });
    this.nodes.delete(node.id);
  }

  /** The node was rebuilt from a new configuration: it starts again from nothing. */
  nodeRebuilt(id) {
    this.nodeRemoved(id);
    this.sync();
  }

  halt(node) {
    const ended = node.state === "exited" || node.state === "trapped";
    const wasKilled = node.state === "killed";
    this.stopProcess(node);
    if (ended) this.publish(node);
    else {
      if (!wasKilled) this.mark(node, "killed", "process killed", "WARN");
      this.setState(node, "killed", "");
    }
  }

  relaunch(node, wipe) {
    node.origin = wipe ? "wiped" : "restarted";
    this.stopProcess(node, { forget: wipe });
    this.launch(node);
  }

  // Stops the node's process, if any, and forgets its volume when asked. Its
  // connections go quietly: the world closes the lab side of a node it kills.
  stopProcess(node, { forget = false } = {}) {
    node.gen++;
    const proc = node.proc;
    this.dropConns(node);
    for (const waiting of node.waitingDials.splice(0)) waiting.resolve(null);
    node.pending = [];
    node.ready = false;
    node.dirty = false;
    node.lagging = false;
    node.settling = null;
    node.deadline = Infinity;
    const killed = proc ? proc.kill().catch(() => {}) : Promise.resolve();
    const volume = node.volume;
    if (!volume || (!proc && !forget)) return;
    const before = this.volumeBusy.get(volume) ?? Promise.resolve();
    const done = Promise.all([before, killed]).then(async () => {
      if (forget) {
        try {
          await (await runtime()).forget(volume);
        } catch (err) {
          this.note(node, "error", `forgetting ${volume} failed: ${err.message}`);
        }
      }
    });
    this.volumeBusy.set(volume, done);
    done.finally(() => {
      if (this.volumeBusy.get(volume) === done) this.volumeBusy.delete(volume);
    });
  }

  // Starts a process for the node: the module, isolation, the environment,
  // then the worker on the node's volume.
  async launch(node) {
    const gen = ++node.gen;
    node.exit = null;
    node.stats = null;
    const doc = this.hooks.scenario();
    const spec = (doc.nodes || []).find((n) => n.id === node.id);
    let config;
    try {
      config = parseConfig(spec?.config);
    } catch (err) {
      this.setState(node, "invalid", `invalid config: ${err.message}`);
      return;
    }
    if (!doc.id) {
      this.setState(node, "waiting", "the scenario has no identity yet: it gets one when it is saved");
      return;
    }
    node.volume = volumeName(doc.id, node.id);
    this.setState(node, "loading", "downloading the broker");
    let wasi;
    let module;
    const loading = loadModule(this.moduleUrl());
    const progress = setInterval(() => {
      if (gen !== node.gen || node.state !== "loading") return;
      const size = loading.total ? `${megabytes(loading.loaded)} of ${megabytes(loading.total)}` : megabytes(loading.loaded);
      const reason = loading.compiling ? "compiling the broker" : `downloading the broker: ${size}`;
      if (reason !== node.reason) this.setState(node, "loading", reason);
    }, 250);
    try {
      wasi = await runtime();
      module = await loading.promise;
    } catch (err) {
      if (gen !== node.gen) return;
      this.unavailable(node, err && err.status === 404 ? MISSING_BUILD : `the broker module did not load: ${err.message}`);
      return;
    } finally {
      clearInterval(progress);
    }
    if (gen !== node.gen) return;
    if (wasi.isolationProblem()) {
      const reason = this.isolation && !this.isolation.isolated && this.isolation.reason
        ? `cross-origin isolation is unavailable: ${this.isolation.reason}`
        : "waiting for cross-origin isolation: the page reloads once to turn it on";
      this.unavailable(node, reason);
      return;
    }
    const env = processEnv({ nodeId: node.id, voters: votersOf(doc), clusterId: await clusterIdFor(doc.id), fileConfig: config.fileConfig, logLevel: this.hooks.logLevel?.(node.id) });
    await this.volumeBusy.get(node.volume);
    if (gen !== node.gen) return;
    this.clock ??= new wasi.WasiClock({ mode: "host", timeMs: this.clockMs });
    const proc = new wasi.WasiProcess(
      {
        name: `krabka-broker-${node.id}`,
        args: ["krabka-broker"],
        env,
        listeners: LISTENER_PORTS,
        volume: node.volume,
        clock: this.clock,
        ondial: (host, port, dial) => this.dial(node, gen, host, port, dial),
        dialTimeoutMs: NO_DIAL_TIMEOUT_MS,
      },
      module,
    );
    node.proc = proc;
    node.env = env;
    node.incarnation += 1;
    node.startedAt = this.clockMs;
    proc.on("exit", (info) => this.exited(node, gen, info));
    proc.on("warn", (text) => this.note(node, "warn", text));
    proc.on("error", (err) => this.note(node, "error", err && err.message ? err.message : String(err)));
    // A process that was replaced may still flush its last lines: only the current one is logged.
    for (const stream of ["stdout", "stderr"]) {
      proc.on(stream, (text) => {
        if (node.proc === proc) this.hooks.log?.(node.id, { stream, text, base: node.startedAt });
        this.publishSoon(node);
      });
    }
    const origin = node.origin ?? (node.incarnation > 1 ? "restarted" : "started");
    node.origin = null;
    const level = env.KRABKA_LOG ?? "";
    this.mark(node, origin, `process ${origin === "wiped" ? "restarted on a fresh disk" : origin} with ${level ? `KRABKA_LOG=${level}` : "the default log level"}`, "INFO", { incarnation: node.incarnation, log_level: level });
    this.setState(node, "booting", "");
    try {
      await proc.start();
    } catch (err) {
      if (gen !== node.gen) return;
      this.setState(node, "failed", err.message);
      this.mark(node, "failed", `process failed to start: ${err.message}`, "ERROR");
      this.event(node, "process_failed", { level: "error", reason: err.message });
      this.refusePending(node);
      return;
    }
    if (gen !== node.gen) {
      proc.kill().catch(() => {});
      return;
    }
    this.event(node, "process_start", { incarnation: node.incarnation, volume: node.volume, address: nodeIp(node.id) });
    for (const frame of node.pending.splice(0)) this.input(node, frame);
    this.flushOut();
    this.ensureStats();
    // In lockstep from the first time it waits for input.
    this.settle(node);
  }

  unavailable(node, reason) {
    const changed = node.state !== "unavailable" || node.reason !== reason;
    this.setState(node, "unavailable", reason);
    if (changed) this.event(node, "process_unavailable", { level: "warn", reason });
    this.refusePending(node);
  }

  // The process ended without the lab stopping it.
  exited(node, gen, info) {
    if (gen !== node.gen || STOPPED_BY_LAB.has(info.reason)) return;
    const trap = info.reason === "trap";
    const reason = describeExit(info);
    node.exit = { reason: info.reason, code: info.code ?? null, message: reason };
    node.gen++;
    this.dropConns(node);
    node.ready = false;
    node.dirty = false;
    node.settling = null;
    node.deadline = Infinity;
    this.setState(node, trap ? "trapped" : "exited", reason);
    this.mark(node, trap ? "trapped" : "exited", `process ${reason}`, trap || info.code ? "ERROR" : "INFO", { code: info.code ?? null });
    this.event(node, trap ? "process_trap" : "process_exit", { level: "error", reason, code: info.code ?? null });
    this.hooks.exited(node.id, node.exit);
  }

  // ---- frames from the world ----------------------------------------------------------------

  /** Frames `drainExternal` returned (`{ deliver_at, frame }`), each due now. */
  deliver(timedFrames) {
    for (const t of timedFrames) {
      const frame = t.frame ?? t;
      const node = this.nodes.get(Number(frame.dst?.node));
      if (node) this.input(node, frame);
      else this.refuse(frame);
    }
    this.flushOut();
  }

  input(node, frame) {
    if (!node.running) {
      if (node.state === "loading" || node.state === "booting") node.pending.push(frame);
      else this.refuse(frame);
      return;
    }
    node.dirty = true;
    if (Number(frame.dst.port) === CLIENT_PORT) this.toDial(node, frame);
    else this.toListener(node, frame);
  }

  // Nothing listens: an open is refused at once, like a reset; anything else is lost.
  refuse(frame) {
    if (frame.payload?.kind === "open") this.emit(closeFrame(frame.dst, frame.src, frame.conn));
  }

  refusePending(node) {
    for (const frame of node.pending.splice(0)) this.refuse(frame);
    this.flushOut();
  }

  toListener(node, frame) {
    const key = `${frame.src.node}:${frame.src.port}:${frame.conn}`;
    const kind = frame.payload?.kind;
    if (kind === "open") {
      const old = node.servers.get(key);
      if (old) this.dropConn(node, old);
      if (!LISTENER_PORTS.includes(Number(frame.dst.port))) {
        this.refuse(frame);
        return;
      }
      let wasi;
      try {
        wasi = node.proc.connect(Number(frame.dst.port));
      } catch {
        this.refuse(frame);
        return;
      }
      const conn = { key, wasi, local: endpoint(frame.dst), peer: endpoint(frame.src), id: frame.conn, framing: null, hold: [], held: 0, done: false };
      node.servers.set(key, conn);
      this.attach(node, conn);
      return;
    }
    const conn = node.servers.get(key);
    if (!conn) return;
    if (kind === "data") this.send(conn, base64ToBytes(frame.payload.data));
    else if (kind === "close") this.closeFromLab(node, conn);
  }

  toDial(node, frame) {
    const conn = node.clients.get(Number(frame.conn));
    if (!conn) return;
    const kind = frame.payload?.kind;
    if (kind === "data") this.send(conn, base64ToBytes(frame.payload.data));
    else if (kind === "close") this.closeFromLab(node, conn);
  }

  // Bytes for the process, in order. An accepted connection is a Kafka stream
  // when the lab's first message on it is exactly one Kafka frame.
  send(conn, bytes) {
    if (conn.framing === null) conn.framing = isKafkaFrame(bytes) ? new KafkaFramer() : RAW;
    conn.hold.push(bytes);
    conn.held += bytes.length;
    this.flushHold(conn);
  }

  flushHold(conn) {
    while (conn.hold.length > 0 && conn.wasi.bufferedAmount < HOLD_HIGH_WATER) {
      const bytes = conn.hold.shift();
      conn.held -= bytes.length;
      conn.wasi.send(bytes);
    }
  }

  // The lab side closed. What it sent before is the process's to read: all of
  // it goes to the runtime ahead of the close.
  closeFromLab(node, conn) {
    if (conn.done) return;
    conn.done = true;
    this.forgetConn(node, conn);
    for (const bytes of conn.hold.splice(0)) conn.wasi.send(bytes);
    conn.held = 0;
    conn.wasi.close();
  }

  // ---- the process's side of a connection ---------------------------------------------------

  attach(node, conn) {
    conn.wasi.on("data", (bytes) => this.fromProcess(node, conn, bytes));
    conn.wasi.on("end", () => this.endFromProcess(node, conn));
    conn.wasi.on("close", (info) => this.closedByRuntime(node, conn, info));
    conn.wasi.on("drain", () => this.flushHold(conn));
  }

  fromProcess(node, conn, bytes) {
    if (conn.done) return;
    this.activity(node);
    if (conn.framing === null) conn.framing = RAW;
    const messages = conn.framing === RAW ? [bytes] : conn.framing.push(bytes);
    for (const message of messages) this.emit(dataFrame(conn.local, conn.peer, conn.id, message));
  }

  // The process shut its write side. The lab has no half-open connection, so
  // this closes the connection both ways.
  endFromProcess(node, conn) {
    if (conn.done) return;
    this.activity(node);
    conn.done = true;
    this.forgetConn(node, conn);
    this.emit(closeFrame(conn.local, conn.peer, conn.id));
    conn.wasi.close();
  }

  closedByRuntime(node, conn, info) {
    if (conn.done) return;
    conn.done = true;
    this.forgetConn(node, conn);
    if (PROCESS_GONE.has(info?.reason)) return;
    this.activity(node);
    this.emit(closeFrame(conn.local, conn.peer, conn.id));
  }

  forgetConn(node, conn) {
    if (conn.key !== undefined) {
      if (node.servers.get(conn.key) === conn) node.servers.delete(conn.key);
    } else if (node.clients.get(conn.id) === conn) {
      node.clients.delete(conn.id);
    }
  }

  // Closes a connection without a word to the lab.
  dropConn(node, conn) {
    if (conn.done) return;
    conn.done = true;
    this.forgetConn(node, conn);
    conn.hold = [];
    conn.held = 0;
    try {
      conn.wasi.close({ reset: true });
    } catch {
      // The process is gone already.
    }
  }

  dropConns(node) {
    for (const conn of [...node.servers.values(), ...node.clients.values()]) this.dropConn(node, conn);
    node.servers.clear();
    node.clients.clear();
  }

  // ---- dials --------------------------------------------------------------------------------

  // The process dials `host:port`. A virtual address of a node that is up is a
  // connection through the world; a node that is down refuses it; an address
  // that is no node is unreachable; a dial across a down link waits for the
  // link, the way a SYN into a black hole does.
  dial(node, gen, host, port, dial) {
    if (gen !== node.gen) return null;
    this.activity(node);
    const advertised = host === "127.0.0.1" && port >= 9092 && port <= 19091;
    const target = advertised ? port - 9091 : nodeForIp(host);
    if (advertised) port = KAFKA_PORT;
    const world = target == null ? null : this.hooks.world();
    const peer = world ? world.nodes.find((n) => n.id === target && n.kind !== "admin") : null;
    if (!peer) {
      dial.refuse("EHOSTUNREACH");
      return undefined;
    }
    if (!peer.alive) return null;
    if (linkDown(world, node.id, target)) {
      const deadline = this.hooks.now() + BLACK_HOLE_DIAL_MS;
      return new Promise((resolve) => node.waitingDials.push({ target, port, dial, resolve, deadline }));
    }
    return this.openDial(node, target, port, dial);
  }

  // Dials that waited for a link past their deadline fail with ETIMEDOUT.
  expireDials(now) {
    let expired = false;
    for (const node of this.nodes.values()) {
      if (!node.waitingDials.length) continue;
      const still = [];
      for (const waiting of node.waitingDials) {
        if (waiting.deadline > now) {
          still.push(waiting);
          continue;
        }
        waiting.dial.refuse("ETIMEDOUT");
        waiting.resolve(undefined);
        node.dirty = true;
        expired = true;
      }
      node.waitingDials = still;
    }
    return expired;
  }

  openDial(node, target, port, dial) {
    const wasi = dial.accept();
    if (wasi.state !== "open") return wasi; // the process gave up on it meanwhile
    const id = node.allocConn();
    const conn = {
      wasi,
      local: { node: node.id, port: CLIENT_PORT },
      peer: { node: target, port },
      id,
      framing: LISTENER_PORTS.includes(port) ? new KafkaFramer() : RAW,
      hold: [],
      held: 0,
      done: false,
    };
    node.clients.set(id, conn);
    this.attach(node, conn);
    this.emit(openFrame(conn.local, conn.peer, id));
    this.flushOut();
    return wasi;
  }

  /** Takes another look at dials waiting for a link: the node behind it went down, or the link came back. */
  recheckDials() {
    let world = null;
    for (const node of this.nodes.values()) {
      if (!node.waitingDials.length) continue;
      world ??= this.hooks.world();
      if (!world) return;
      const still = [];
      for (const waiting of node.waitingDials) {
        const peer = world.nodes.find((n) => n.id === waiting.target);
        if (!peer || !peer.alive) waiting.resolve(null);
        else if (linkDown(world, node.id, waiting.target)) still.push(waiting);
        else waiting.resolve(this.openDial(node, waiting.target, waiting.port, waiting.dial));
      }
      node.waitingDials = still;
    }
  }

  // ---- to the world -------------------------------------------------------------------------

  emit(frame) {
    this.out.push(frame);
    if (!this.outQueued) {
      this.outQueued = true;
      queueMicrotask(() => this.flushOut());
    }
  }

  /** Routes every frame the processes sent so far. */
  flushOut() {
    this.outQueued = false;
    if (!this.out.length) return;
    const frames = this.out;
    this.out = [];
    this.hooks.route(frames);
  }

  // ---- lockstep -----------------------------------------------------------------------------

  // Output or a dial the lab did not ask for: the process is busy.
  activity(node) {
    if (!node.settling) node.dirty = true;
  }

  /**
   * The earliest timer of a process in lockstep, or the earliest deadline of
   * a dial waiting for a link, in lab ms; Infinity when none.
   */
  nextDeadline() {
    let deadline = Infinity;
    for (const node of this.nodes.values()) {
      if (node.inStep) deadline = Math.min(deadline, node.deadline);
      for (const waiting of node.waitingDials) deadline = Math.min(deadline, waiting.deadline);
    }
    return deadline;
  }

  /** Whether a process in lockstep has input or activity it has not settled. */
  get busy() {
    for (const node of this.nodes.values()) if (node.inStep && (node.dirty || node.settling)) return true;
    return false;
  }

  /**
   * The world reached `now`: the clock moves there first when a process gets
   * a frame or its timer falls due, then the frames go to their processes.
   */
  at(now, frames) {
    let wake = frames.length > 0;
    for (const node of this.nodes.values()) {
      if (node.inStep && node.deadline <= now) {
        node.deadline = Infinity;
        node.dirty = true;
        wake = true;
      }
      if (node.waitingDials.some((w) => w.deadline <= now)) wake = true;
    }
    if (wake) this.setClock(now);
    this.expireDials(now);
    if (frames.length) this.deliver(frames);
  }

  /** Moves the processes' clock to lab time `ms`; it never goes back. */
  setClock(ms) {
    if (ms > this.clockMs) this.clockMs = ms;
    if (this.clock && this.clock.now() < this.clockMs) this.clock.set(this.clockMs);
  }

  /**
   * Waits until every process in lockstep that has input or activity has
   * blocked again, at most `budgetMs` of wall time. A process that takes
   * longer runs free (lagging) until it blocks, so one busy process cannot
   * stall the lab.
   */
  async quiesce(budgetMs) {
    const waiting = [...this.nodes.values()].filter((n) => n.inStep && (n.dirty || n.settling));
    if (!waiting.length) return;
    let timer = 0;
    const late = new Promise((resolve) => {
      timer = setTimeout(() => resolve(true), budgetMs);
    });
    const tooLate = await Promise.race([Promise.all(waiting.map((n) => this.settle(n))).then(() => false), late]);
    clearTimeout(timer);
    if (tooLate) {
      for (const node of waiting) {
        if (node.settling && !node.lagging) {
          node.lagging = true;
          this.publish(node);
        }
      }
    }
    this.flushOut();
  }

  settle(node) {
    if (node.settling) return node.settling;
    if (!node.running) return Promise.resolve();
    node.dirty = false;
    const gen = node.gen;
    const settling = node.proc.quiesce().then(
      (info) => {
        if (gen !== node.gen || !info) return;
        node.deadline = info.deadlineMs == null ? Infinity : Math.ceil(info.deadlineMs - 1e-6);
        if (!node.ready) {
          node.ready = true;
          this.setState(node, "running", "");
          this.event(node, "process_ready", { address: nodeIp(node.id) });
        }
      },
      () => {},
    );
    node.settling = settling;
    settling.finally(() => {
      if (node.settling === settling) node.settling = null;
      if (node.lagging && gen === node.gen) {
        node.lagging = false;
        this.publish(node);
      }
      this.flushOut();
    });
    return settling;
  }

  // ---- observation --------------------------------------------------------------------------

  setState(node, state, reason) {
    node.state = state;
    node.reason = reason || "";
    this.publish(node);
  }

  note(node, level, text) {
    node.notes.push({ level, text: String(text) });
    if (node.notes.length > 10) node.notes.shift();
    this.publishSoon(node);
  }

  event(node, kind, detail) {
    this.hooks.event(node.id, kind, detail);
  }

  // A lifecycle row in the Logs tab, between the lines of one process and the next.
  mark(node, marker, message, level = "INFO", detail = {}) {
    this.hooks.log?.(node.id, { marker, message, level, detail });
  }

  publishSoon(node) {
    if (node.publishTimer) return;
    node.publishTimer = setTimeout(() => {
      node.publishTimer = 0;
      if (this.nodes.get(node.id) === node) this.publish(node);
    }, PUBLISH_DELAY_MS);
  }

  publish(node) {
    this.hooks.publish(node.id, this.snapshotOf(node));
  }

  /** What the inspector shows for a real broker: its process, connections, logs and runtime counters. */
  snapshotOf(node) {
    const proc = node.proc;
    let held = 0;
    for (const conn of [...node.servers.values(), ...node.clients.values()]) held += conn.held;
    let module = this.moduleUrl();
    try {
      module = new URL(module).pathname;
    } catch {
      // Keep the text as it is.
    }
    return {
      external: true,
      process: {
        state: node.state,
        reason: node.reason || null,
        lagging: node.lagging,
        incarnation: node.incarnation,
        address: nodeIp(node.id),
        volume: node.volume,
        module,
        started_at_ms: node.startedAt,
        exit: node.exit,
        notes: node.notes.slice(-5),
      },
      env: node.env,
      connections: { inbound: node.servers.size, outbound: node.clients.size, waiting_dials: node.waitingDials.length, held_bytes: held },
      stdout: proc ? proc.tail("stdout").slice(-TAIL_LINES) : [],
      stderr: proc ? proc.tail("stderr").slice(-TAIL_LINES) : [],
      runtime: node.stats,
    };
  }

  ensureStats() {
    if (this.statsTimer) return;
    this.statsTimer = setInterval(() => this.refreshStats(), STATS_INTERVAL_MS);
  }

  async refreshStats() {
    if (!this.nodes.size) {
      clearInterval(this.statsTimer);
      this.statsTimer = 0;
      return;
    }
    for (const node of [...this.nodes.values()]) {
      if (!node.running) continue;
      const gen = node.gen;
      try {
        const stats = await node.proc.stats();
        if (gen === node.gen) node.stats = summarizeStats(stats);
      } catch {
        // It stopped meanwhile.
      }
      if (this.nodes.get(node.id) === node) this.publish(node);
    }
  }

  /** The process behind a node, for tests and the curious. */
  process(id) {
    return this.nodes.get(Number(id))?.proc ?? null;
  }

  /** The node's state as the inspector shows it, or null. */
  state(id) {
    const node = this.nodes.get(Number(id));
    return node ? this.snapshotOf(node) : null;
  }

  // ---- volumes ------------------------------------------------------------------------------

  /** The volumes this browser keeps for a scenario's real brokers: `[{ node, volume, bytes, files, inUse }]`. */
  async volumes(scenarioId) {
    if (!scenarioId || !(await volumesKept())) return [];
    const wasi = await runtime();
    const prefix = `${scenarioId}/`;
    const list = (await wasi.listVolumes()).filter((v) => v.id.startsWith(prefix));
    return Promise.all(
      list.map(async (v) => {
        const node = Number(v.id.slice(prefix.length));
        const usage = await wasi.usage(v.id);
        const real = this.nodes.get(node);
        const inUse = Boolean(real && real.volume === v.id && real.proc && ["starting", "running"].includes(real.proc.state));
        return { node, volume: v.id, bytes: usage.bytes, files: usage.files, inUse };
      }),
    );
  }

  async volumeFiles(volume) {
    return (await runtime()).listVolumeFiles(volume);
  }

  async volumeFileRange(volume, path, offset, length) {
    return (await runtime()).readVolumeFileRange(volume, path, offset, length);
  }

  /** Forgets one volume; a process that runs on it keeps it. */
  async forgetVolume(volume) {
    const wasi = await runtime();
    await wasi.forget(volume);
  }

  /** Forgets every volume of a scenario that no process uses; returns how many are left in use. */
  async forgetScenarioVolumes(scenarioId) {
    let kept = 0;
    for (const v of await this.volumes(scenarioId)) {
      if (v.inUse) kept += 1;
      else await this.forgetVolume(v.volume);
    }
    return kept;
  }
}
