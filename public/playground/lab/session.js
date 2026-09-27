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
// The site is static, so there is no signalling server. The hub makes an
// invite (its SDP offer, compressed into a `?join=` link); the spoke opens
// it, shows an answer code, and the person pastes that code back into the
// hub. One invite admits one spoke; make another for the next tab.
//
// Messages on the channel are JSON with a `t` tag:
//   hello     { name }                        both ways on open
//   scenario  { doc, hosting: {node: peer} } hub → spoke on every change
//   frames    { frames: [Frame] }             egress, routed by destination; the
//                                             sender's world holds each frame until
//                                             its clock reaches `deliver_at`, and the
//                                             receiver delivers it on arrival
//   snapshot  { node, state }                 a hosted node's state, relayed
//   fault     { fault }                       applied everywhere
//   takeover  { node }                        spoke asks to host a node
//   bye       {}

import { encodeShare, decodeShare } from "./codec.js";

const ICE_SERVERS = [{ urls: "stun:stun.l.google.com:19302" }];
const GATHER_TIMEOUT_MS = 4000;
const SNAPSHOT_PERIOD_MS = 250;
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
  // onError(err, context), onLog(text), hostedSnapshots() → [{ id, state }],
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
  }

  get available() {
    return webrtcAvailable();
  }

  // Peers as the panels list them, this tab first.
  peerList() {
    return [{ id: this.me, name: this.name, state: "connected", self: true }, ...this.peers.map((p) => ({ id: p.remoteId || p.id, name: p.name, state: p.state, self: false }))];
  }

  get peersKey() {
    return this.peerList()
      .map((p) => `${p.id}:${p.name}:${p.state}`)
      .join("|");
  }

  peerName(id) {
    if (id == null) return "?";
    if (id === this.me) return `${this.name} (this tab)`;
    const p = this.peers.find((x) => (x.remoteId || x.id) === id);
    return p ? p.name : `peer ${String(id).slice(0, 6)}`;
  }

  offlinePeers() {
    const out = new Set();
    for (const p of this.peers) if (p.state !== "connected") out.add(p.remoteId || p.id);
    return out;
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
    const code = await encodeShare({ t: "offer", sdp: pc.localDescription.sdp, hub: this.name, id: this.me });
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
    peer.name = msg.name || peer.name;
    peer.remoteId = msg.id || peer.id;
    await peer.pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
    this.pending = null;
    this.peers.push(peer);
    this.hooks.onPeers();
  }

  // The hub moves a node to another peer. The node restarts in the new tab.
  setHost(nodeId, peerId) {
    if (this.role !== "hub") return;
    this.hosting.set(nodeId, peerId);
    this.broadcastScenario();
    this.hooks.onPeers();
  }

  // Every node the hub does not know yet is hosted by the hub; nodes that
  // left the scenario are forgotten. Called when the scenario changes.
  syncHosting() {
    if (this.role !== "hub") return;
    const doc = this.hooks.scenario();
    const ids = new Set((doc.nodes || []).map((n) => n.id));
    for (const id of ids) if (!this.hosting.has(id)) this.hosting.set(id, this.me);
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
    return encodeShare({ t: "answer", sdp: pc.localDescription.sdp, name: this.name, id: this.me });
  }

  requestTakeover(nodeId) {
    if (this.role === "spoke" && this.hub) this.hub.send({ t: "takeover", node: nodeId });
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
      this.hooks.onPeers();
    });
    channel.addEventListener("close", () => {
      if (peer.state !== "closed") {
        peer.state = "closed";
        this.hooks.onLog(`${peer.name} disconnected`);
        this.hooks.onPeers();
      }
    });
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

  watch(peer, pc) {
    pc.addEventListener("connectionstatechange", () => {
      if (["failed", "closed", "disconnected"].includes(pc.connectionState) && peer.state !== "closed") {
        peer.state = "closed";
        this.hooks.onLog(`${peer.name} ${pc.connectionState}`);
        this.hooks.onPeers();
      }
    });
  }

  receive(from, msg) {
    switch (msg.t) {
      case "hello":
        from.name = msg.name || from.name;
        if (msg.id) from.remoteId = msg.id;
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
        from.close();
        this.hooks.onPeers();
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
    this.role = "solo";
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    this.snapshotTimer = 0;
    this.hooks.onPeers();
  }
}
