// A local kafkactl process is one external client node of the lab world.
// It sends complete Kafka frames; the world applies the same link rules as
// for every other external node before they reach a broker.
export const LOCAL_CLIENT_KIND = "local-client";
const ADDRESS = "ws://127.0.0.1:19092/bridge";

export class KafkactlBridge {
  constructor(hooks) {
    this.hooks = hooks;
    this.socket = null;
    this.connections = new Map();
    this.node = null;
    this.invalidBroker = false;
    this.state = "disconnected";
    this.reason = "";
  }

  status(state, reason = "") {
    this.state = state;
    this.reason = reason;
    this.hooks.status(state, reason);
    if (this.node != null) this.hooks.publish(this.node, { connected: state === "connected", reason });
  }

  connect(token) {
    this.disconnect();
    if (!token) throw new Error("Enter the token printed by kafkactl lab bridge.");
    this.status("connecting", "Waiting for the local bridge and browser permission");
    const socket = new WebSocket(ADDRESS);
    this.socket = socket;
    return new Promise((resolve, reject) => {
      let paired = false;
      let configured = false;
      socket.onopen = () => socket.send(JSON.stringify({ type: "hello", token }));
      socket.onmessage = (event) => {
        let message;
        try { message = JSON.parse(event.data); }
        catch { return; }
        if (message.type === "ready") {
          paired = true;
          this.sync();
        } else if (message.type === "configured") {
          configured = true;
          if (this.invalidBroker) this.status("error", "A real broker ID must be between 1 and 10000 to use the local bridge");
          else if (this.node == null) this.status("error", "Add a local kafkactl client node to this lab scenario");
          else if (message.brokers.length === 0) this.status("error", "Add a real broker to this lab scenario");
          else this.status("connected", `Listening for ${message.brokers.length} real broker${message.brokers.length === 1 ? "" : "s"}`);
          if (this.state === "error") reject(new Error(this.reason));
          else resolve();
        } else if (message.type === "error") {
          this.status("error", message.message);
          if (!configured) reject(new Error(message.message));
        } else if (message.type === "open" || message.type === "data" || message.type === "close") {
          this.fromLocal(message);
        }
      };
      socket.onerror = () => {
        if (!configured) reject(new Error("Cannot reach the local bridge. Start kafkactl lab bridge and allow local network access in the browser."));
      };
      socket.onclose = () => {
        if (this.socket !== socket) return;
        for (const [conn, broker] of this.connections) this.route(broker, conn, "close");
        this.connections.clear();
        this.socket = null;
        this.status("disconnected", "The local bridge stopped or the connection closed");
        if (!configured) reject(new Error(paired ? "The local bridge stopped during pairing." : "The local bridge rejected the connection. Check the token and page origin."));
      };
    });
  }

  disconnect() {
    if (this.socket) {
      const socket = this.socket;
      this.socket = null;
      socket.close();
    }
    for (const [conn, broker] of this.connections) this.route(broker, conn, "close");
    this.connections.clear();
    this.status("disconnected");
  }

  reset() {
    if (this.socket?.readyState === WebSocket.OPEN) {
      for (const conn of this.connections.keys()) this.socket.send(JSON.stringify({ type: "close", conn }));
    }
    this.connections.clear();
    this.sync();
  }

  sync() {
    const world = this.hooks.world();
    const client = world?.nodes?.find((n) => n.kind === LOCAL_CLIENT_KIND && n.hosted);
    this.node = client?.id ?? null;
    const invalid = world?.nodes?.some((n) => n.kind === "krabka-broker" && (n.id < 1 || n.id > 10000));
    this.invalidBroker = !!invalid;
    if (invalid) {
      this.status("error", "A real broker ID must be between 1 and 10000 to use the local bridge");
    }
    const brokers = this.node == null || invalid ? [] : (world?.nodes || []).filter((n) => n.kind === "krabka-broker" && n.hosted && n.id > 0 && n.id <= 10000).map((n) => n.id);
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({ type: "configure", brokers }));
    }
  }

  route(broker, conn, kind, data) {
    if (this.node == null) return;
    this.hooks.route([{ src: { node: this.node, port: 0 }, dst: { node: broker, port: 9092 }, conn, payload: kind === "data" ? { kind, data } : { kind } }]);
  }

  fromLocal(message) {
    const conn = Number(message.conn);
    if (!Number.isSafeInteger(conn) || conn <= 0) return;
    if (message.type === "open") {
      const broker = Number(message.broker);
      if (!Number.isInteger(broker) || broker <= 0 || broker > 10000) return;
      this.connections.set(conn, broker);
      this.route(broker, conn, "open");
    } else {
      const broker = this.connections.get(conn);
      if (broker == null) return;
      this.route(broker, conn, message.type, message.data);
      if (message.type === "close") this.connections.delete(conn);
    }
  }

  deliver(timedFrames) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    for (const t of timedFrames) {
      const frame = t.frame ?? t;
      if (!this.connections.has(Number(frame.conn))) continue;
      const kind = frame.payload?.kind;
      if (kind === "data") {
        if (this.socket.bufferedAmount > 16 * 1024 * 1024) {
          this.disconnect();
          this.status("error", "The local bridge cannot keep up; reconnect after the lab catches up");
          return;
        }
        this.socket.send(JSON.stringify({ type: "data", conn: frame.conn, data: frame.payload.data }));
      }
      else if (kind === "close") {
        this.connections.delete(Number(frame.conn));
        this.socket.send(JSON.stringify({ type: "close", conn: frame.conn }));
      }
    }
  }
}
