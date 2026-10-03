// Distributed hosting: several tabs, on one machine or across the internet,
// each run a share of the nodes and exchange frames over WebRTC data
// channels.
//
// Topology is a star. The tab that starts the session is the **hub**: it
// owns the scenario, decides which peer hosts which node, and relays frames
// and snapshots between the **spokes**. A spoke runs the nodes the hub
// assigned it, ships every frame for a node it does not host to the hub, and
// draws the rest of the cluster from the snapshots the hub relays.
//
// A node of a pinned kind (a real broker: its process and its volume live in
// the hub's browser) stays on the hub. The session never moves it, whoever
// asks.
//
// The site is static, so there is no signalling server. The hub makes an
// invite (its SDP offer, compressed into a `?join=` link); the spoke opens
// it, shows an answer code, and the person pastes that code back into the
// hub. One invite admits one spoke; make another for the next tab.
//
// Messages on the channel are JSON with a `t` tag:
//   hello     { name, id }                    both ways on open, and when a tab renames
//   scenario  { doc, hosting: {node: peer} } hub → spoke on every change
//   roster    { peers: [{ id, name, state }] } hub → spoke: every tab in the session,
//                                             so a spoke can name the other spokes
//   frames    { frames: [Frame] }             egress, routed by destination; the
//                                             sender's world holds each frame until
//                                             its clock reaches `deliver_at`, and the
//                                             receiver delivers it on arrival
//   snapshot  { node, state }                 a hosted node's state, relayed
//   fault     { fault }                       applied everywhere
//   takeover  { node }                        spoke asks to host a node
//   bye       {}

import { encodeShare, decodeShare } from "./codec.js";
import { kindOf } from "./kinds.js";

const ICE_SERVERS = [{ urls: "stun:stun.l.google.com:19302" }];
const GATHER_TIMEOUT_MS = 4000;
const SNAPSHOT_PERIOD_MS = 250;
// ICE reports "disconnected" for a Wi-Fi roam or a throttled tab and often
// recovers by itself; only "failed" is final.
const DISCONNECT_GRACE_MS = 8000;
// An accepted answer that never opens the channel (a stale or lost answer).
const CONNECT_TIMEOUT_MS = 30000;
export const NAME_MAX = 24;
const JOIN_RE = /[?&]join=([^&#]+)/;

const ADJECTIVES = ["amber", "brisk", "coral", "dusky", "eager", "flint", "gilt", "hazel", "ivory", "jade", "kelp", "lunar", "misty", "north", "ochre", "pearl", "quiet", "rusty", "sable", "tidal"];
const NOUNS = ["crab", "reef", "tide", "kelp", "gull", "shell", "wave", "cove", "dune", "buoy"];

export function randomPeerName() {
  const a = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const n = NOUNS[Math.floor(Math.random() * NOUNS.length)];
  return `${a}-${n}`;
}

export function randomPeerId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID().slice(0, 8);
  return Math.random().toString(36).slice(2, 10);
}

// A name from the other tab: text, trimmed, at most NAME_MAX characters.
function cleanName(name) {
  return typeof name === "string" ? name.trim().slice(0, NAME_MAX) : "";
}

export function joinCodeFromUrl(search) {
  const m = JOIN_RE.exec(search || "");
  return m ? m[1] : null;
}

export function webrtcAvailable() {
  return typeof RTCPeerConnection === "function";
}

// Wait until ICE gathering finished, or long enough that the candidates we
// have are the ones we will get.
function gathered(pc) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, GATHER_TIMEOUT_MS);
    pc.addEventListener("icegatheringstatechange", () => {
      if (pc.iceGatheringState === "complete") {
        clearTimeout(timer);
        resolve();
      }
    });
  });
}

class Peer {
  constructor(id, name) {
    this.id = id;
    this.name = name;
    this.state = "connecting"; // connecting | connected | closed
    this.pc = null;
    this.channel = null;
    this.remoteId = null;
  }

  send(msg) {
    if (this.state !== "connected" || !this.channel || this.channel.readyState !== "open") return false;
    try {
      this.channel.send(JSON.stringify(msg));
      return true;
    } catch {
      return false;
    }
  }

  close() {
    clearTimeout(this.graceTimer);
    this.state = "closed";
    try {
      this.channel?.close();
    } catch {
      // Already closed.
    }
    try {
      this.pc?.close();
    } catch {
      // Already closed.
    }
  }
}

export class Session {
  // hooks: onPeers(), onScenario(doc, hosting), onIngress(frames),
  // onRemoteSnapshot(nodeId, state), onFault(fault), onTakeover(nodeId, peerId),
  // onError(err, context), onLog(text), onHubLost() (a spoke's hub left or dropped),
  // hostedSnapshots() → [{ id, state }],
  // scenario() → doc
  constructor(hooks) {
    this.hooks = hooks;
    this.role = "solo";
    this.me = randomPeerId();
    this.name = randomPeerName();
    this.peers = [];
    this.hosting = new Map();
    this.hub = null; // the spoke's peer entry for the hub
    this.pending = null; // the invite waiting for its answer
    this.snapshotTimer = 0;
    this.lastHosting = "";
    this.hostingFor = null; // the scenario id the hosting map is for
    this.roster = []; // a spoke's view of the other spokes, relayed by the hub
  }

  get available() {
    return webrtcAvailable();
  }

  // Peers as the panels list them, this tab first. A spoke also lists the
  // other spokes the hub told it about; with the hub gone their state is unknown.
  peerList() {
    const out = [{ id: this.me, name: this.name, state: "connected", self: true }, ...this.peers.map((p) => ({ id: p.remoteId || p.id, name: p.name, state: p.state, self: false }))];
    if (this.role === "spoke") {
      const hubUp = this.hub?.state === "connected";
      for (const r of this.roster) if (!out.some((p) => p.id === r.id)) out.push({ id: r.id, name: r.name, state: hubUp ? r.state : "closed", self: false });
    }
    return out;
  }

  get peersKey() {
    return this.peerList()
      .map((p) => `${p.id}:${p.name}:${p.state}`)
      .join("|");
  }

  peerName(id) {
    if (id == null) return "?";
    if (id === this.me) return `${this.name} (this tab)`;
    const p = this.peerList().find((x) => x.id === id);
    return p ? p.name : `peer ${String(id).slice(0, 6)}`;
  }

  offlinePeers() {
    const out = new Set();
    for (const p of this.peerList()) if (!p.self && p.state !== "connected") out.add(p.id);
    return out;
  }

  // Every tab's peer list changed: tell the panels, and on the hub tell the
  // spokes, which only know the hub.
  changed() {
    this.hooks.onPeers();
    if (this.role !== "hub") return;
    const peers = this.peerList().map(({ id, name, state }) => ({ id, name, state }));
    for (const p of this.peers) p.send({ t: "roster", peers });
  }

  // Rename this tab and tell everyone; the hub relays the name to the spokes.
  setName(name) {
    this.name = String(name).trim().slice(0, NAME_MAX) || this.name;
    for (const p of this.peers) p.send({ t: "hello", name: this.name, id: this.me });
    this.changed();
  }

  // The ids this tab runs, or null for every node in solo mode.
  myHostedIds() {
    if (this.role === "solo") return null;
    const ids = [];
    for (const [node, peer] of this.hosting) if (peer === this.me) ids.push(node);
    return ids;
  }

  hostOf(nodeId) {
    if (this.role === "solo") return this.me;
    return this.hosting.get(nodeId) ?? (this.role === "hub" ? this.me : null);
  }

  // ---- hub -----------------------------------------------------------------------------------

  // Become the hub: every node of the scenario starts hosted here.
  becomeHub() {
    if (this.role === "hub") return;
    this.role = "hub";
    const doc = this.hooks.scenario();
    for (const n of doc.nodes || []) if (!this.hosting.has(n.id)) this.hosting.set(n.id, this.me);
    this.startSnapshots();
    this.hooks.onPeers();
  }

  // Make an invite link for one more spoke. Resolves to the URL.
  async createInvite(baseUrl) {
    if (!this.available) throw new Error("this browser has no WebRTC");
    this.becomeHub();
    const peer = new Peer(randomPeerId(), "invited peer");
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    peer.pc = pc;
    const channel = pc.createDataChannel("lab", { ordered: true });
    this.attachChannel(peer, channel);
    this.watch(peer, pc);
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await gathered(pc);
    const code = await encodeShare({ t: "offer", sdp: pc.localDescription.sdp, hub: this.name, id: this.me, oid: peer.id });
    if (this.pending) this.pending.close();
    this.pending = peer;
    const url = new URL(baseUrl || window.location.href);
    url.hash = "";
    url.search = `?join=${code}`;
    return url.toString();
  }

  // Accept the answer code a spoke produced for the pending invite.
  async acceptAnswer(code) {
    const peer = this.pending;
    if (!peer) throw new Error("no invite is waiting for an answer");
    const msg = await decodeShare(code);
    if (msg.t !== "answer" || !msg.sdp) throw new Error("that is not an answer code");
    // Answers carry the id of the invite they answer; older ones have none.
    if (msg.oid && msg.oid !== peer.id) throw new Error("that answer belongs to an older invite; paste the answer for the newest link");
    peer.name = cleanName(msg.name) || peer.name;
    peer.remoteId = msg.id || peer.id;
    try {
      await peer.pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
    } catch {
      throw new Error("that answer does not match this invite; paste the answer for the newest link");
    }
    this.pending = null;
    this.peers.push(peer);
    setTimeout(() => {
      if (peer.state !== "connecting") return;
      peer.close();
      this.hooks.onError(new Error(`${peer.name} did not connect; make a new invite`), "session");
      this.changed();
    }, CONNECT_TIMEOUT_MS);
    this.changed();
  }

  // Whether a node must stay in the tab that hosts it now: its kind is pinned.
  pinned(nodeId) {
    const node = (this.hooks.scenario().nodes || []).find((n) => n.id === Number(nodeId));
    return Boolean(node && kindOf(node.kind).pinned);
  }

  // The hub moves a node to another peer. The node restarts in the new tab. A
  // pinned node stays on the hub.
  setHost(nodeId, peerId) {
    if (this.role !== "hub") return false;
    if (peerId !== this.me && this.pinned(nodeId)) {
      this.hooks.onLog("A real broker stays in this tab: its process and its volume live in this browser");
      return false;
    }
    this.hosting.set(nodeId, peerId);
    this.broadcastScenario();
    this.changed();
    return true;
  }

  // A closed tab's nodes come back to the hub and the tab leaves the list.
  forgetPeer(peerId) {
    if (this.role !== "hub") return;
    for (const [node, peer] of this.hosting) if (peer === peerId) this.hosting.set(node, this.me);
    this.peers = this.peers.filter((p) => (p.remoteId || p.id) !== peerId || p.state !== "closed");
    this.broadcastScenario();
    this.changed();
  }

  // Every node the hub does not know yet is hosted by the hub; nodes that
  // left the scenario are forgotten. Called when the scenario changes. A
  // different scenario starts on the hub again: its node ids mean other nodes.
  syncHosting() {
    if (this.role !== "hub") return;
    const doc = this.hooks.scenario();
    if (doc.id && this.hostingFor && doc.id !== this.hostingFor) this.hosting.clear();
    if (doc.id) this.hostingFor = doc.id;
    const ids = new Set((doc.nodes || []).map((n) => n.id));
    for (const n of doc.nodes || []) {
      if (!this.hosting.has(n.id) || kindOf(n.kind).pinned) this.hosting.set(n.id, this.me);
    }
    for (const id of [...this.hosting.keys()]) if (!ids.has(id)) this.hosting.delete(id);
  }

  broadcastScenario() {
    if (this.role !== "hub") return;
    this.syncHosting();
    const msg = { t: "scenario", doc: this.hooks.scenario(), hosting: Object.fromEntries(this.hosting) };
    for (const p of this.peers) p.send(msg);
    const key = JSON.stringify(msg.hosting);
    if (key !== this.lastHosting) {
      this.lastHosting = key;
      this.hooks.onScenario(msg.doc, this.hosting);
    }
  }

  // ---- spoke -----------------------------------------------------------------------------------

  // Join with an invite code. Resolves to the answer code to hand back.
  async join(code) {
    if (!this.available) throw new Error("this browser has no WebRTC");
    const msg = await decodeShare(code);
    if (msg.t !== "offer" || !msg.sdp) throw new Error("that is not an invite code");
    this.role = "spoke";
    const peer = new Peer(msg.id || randomPeerId(), msg.hub || "host");
    peer.remoteId = msg.id || peer.id;
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    peer.pc = pc;
    pc.addEventListener("datachannel", (e) => this.attachChannel(peer, e.channel));
    this.watch(peer, pc);
    await pc.setRemoteDescription({ type: "offer", sdp: msg.sdp });
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    await gathered(pc);
    this.hub = peer;
    this.peers = [peer];
    this.startSnapshots();
    this.hooks.onPeers();
    return encodeShare({ t: "answer", sdp: pc.localDescription.sdp, name: this.name, id: this.me, oid: msg.oid });
  }

  // Whether the request reached the hub.
  requestTakeover(nodeId) {
    return this.role === "spoke" && this.hub && !this.pinned(nodeId) ? this.hub.send({ t: "takeover", node: nodeId }) : false;
  }

  // ---- the channel --------------------------------------------------------------------------------

  attachChannel(peer, channel) {
    peer.channel = channel;
    channel.addEventListener("open", () => {
      peer.state = "connected";
      peer.send({ t: "hello", name: this.name, id: this.me });
      if (this.role === "hub") {
        this.syncHosting();
        peer.send({ t: "scenario", doc: this.hooks.scenario(), hosting: Object.fromEntries(this.hosting) });
      }
      this.hooks.onLog(`${peer.name} connected`);
      this.changed();
    });
    channel.addEventListener("close", () => this.lost(peer, "disconnected"));
    channel.addEventListener("message", (e) => {
      let msg;
      try {
        msg = JSON.parse(e.data);
      } catch {
        return;
      }
      this.receive(peer, msg);
    });
  }

  // The peer is gone for good: say so, and tell the spoke its hub is gone.
  lost(peer, why) {
    clearTimeout(peer.graceTimer);
    if (peer.state === "closed") return;
    peer.state = "closed";
    this.hooks.onLog(`${peer.name} ${why}`);
    if (this.role === "spoke" && peer === this.hub) this.hooks.onHubLost();
    peer.close();
    this.changed();
  }

  watch(peer, pc) {
    pc.addEventListener("connectionstatechange", () => {
      const state = pc.connectionState;
      clearTimeout(peer.graceTimer);
      if (state === "failed" || state === "closed") this.lost(peer, state);
      else if (state === "disconnected") peer.graceTimer = setTimeout(() => this.lost(peer, state), DISCONNECT_GRACE_MS);
    });
  }

  receive(from, msg) {
    switch (msg.t) {
      case "hello":
        from.name = cleanName(msg.name) || from.name;
        if (msg.id) from.remoteId = msg.id;
        this.changed();
        break;
      case "roster":
        if (this.role !== "spoke" || !Array.isArray(msg.peers)) return;
        this.roster = msg.peers.filter((p) => p && typeof p.id === "string").map((p) => ({ id: p.id, name: cleanName(p.name) || `peer ${p.id.slice(0, 6)}`, state: p.state === "connected" ? "connected" : "closed" }));
        this.hooks.onPeers();
        break;
      case "scenario":
        if (this.role !== "spoke") return;
        this.hosting = new Map(Object.entries(msg.hosting || {}).map(([k, v]) => [Number(k), v]));
        this.hooks.onScenario(msg.doc, this.hosting);
        break;
      case "frames":
        this.routeFrames(msg.frames || [], from);
        break;
      case "snapshot": {
        const owner = from.remoteId || from.id;
        this.hooks.onRemoteSnapshot(msg.node, msg.state);
        if (this.role === "hub") for (const p of this.peers) if (p !== from) p.send({ t: "snapshot", node: msg.node, state: msg.state, owner });
        break;
      }
      case "fault":
        this.hooks.onFault(msg.fault);
        if (this.role === "hub") for (const p of this.peers) if (p !== from) p.send({ t: "fault", fault: msg.fault });
        break;
      case "takeover":
        if (this.role === "hub") {
          const id = from.remoteId || from.id;
          this.hooks.onTakeover(Number(msg.node), id);
        }
        break;
      case "bye":
        this.lost(from, "left");
        break;
      default:
    }
  }

  // Frames for nodes hosted elsewhere, as `drainEgress` produced them
  // (`{ deliver_at, frame }`). A spoke sends them to the hub; the hub sends
  // each to the peer hosting the destination.
  sendEgress(timedFrames) {
    if (this.role === "solo" || !timedFrames.length) return;
    const frames = timedFrames.map((t) => t.frame || t);
    if (this.role === "spoke") {
      this.hub?.send({ t: "frames", frames });
      return;
    }
    this.routeFrames(frames, null);
  }

  routeFrames(frames, from) {
    if (this.role === "spoke") {
      // Everything the hub sends a spoke is for a node the spoke hosts.
      this.hooks.onIngress(frames);
      return;
    }
    const local = [];
    const byPeer = new Map();
    for (const f of frames) {
      const dst = f.dst && f.dst.node != null ? Number(f.dst.node) : null;
      const host = dst == null ? this.me : this.hosting.get(dst) ?? this.me;
      if (host === this.me) local.push(f);
      else {
        if (!byPeer.has(host)) byPeer.set(host, []);
        byPeer.get(host).push(f);
      }
    }
    if (local.length) this.hooks.onIngress(local);
    for (const [host, list] of byPeer) {
      const peer = this.peers.find((p) => (p.remoteId || p.id) === host && p !== from);
      if (peer) peer.send({ t: "frames", frames: list });
    }
  }

  // A fault this tab applied; everyone else applies it too.
  broadcastFault(fault) {
    if (this.role === "solo") return;
    for (const p of this.peers) p.send({ t: "fault", fault });
  }

  startSnapshots() {
    if (this.snapshotTimer) return;
    this.snapshotTimer = setInterval(() => this.shipSnapshots(), SNAPSHOT_PERIOD_MS);
  }

  shipSnapshots() {
    if (this.role === "solo") return;
    const targets = this.peers.filter((p) => p.state === "connected");
    if (!targets.length) return;
    for (const { id, state } of this.hooks.hostedSnapshots()) {
      const msg = { t: "snapshot", node: id, state, owner: this.me };
      for (const p of targets) p.send(msg);
    }
  }

  leave() {
    for (const p of this.peers) {
      p.send({ t: "bye" });
      p.close();
    }
    this.pending?.close();
    this.pending = null;
    this.peers = [];
    this.hub = null;
    this.hosting = new Map();
    this.hostingFor = null;
    this.roster = [];
    this.role = "solo";
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    this.snapshotTimer = 0;
    this.hooks.onPeers();
  }
}
