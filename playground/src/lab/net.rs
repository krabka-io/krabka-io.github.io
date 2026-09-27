//! The virtual network the lab's nodes talk over.
//!
//! Every node is a synchronous state machine behind the [`Node`] trait. It
//! receives [`Frame`]s and timer ticks through the world, and it sends frames
//! and arms its timer through [`Ctx`]. Nothing here opens a socket or reads a
//! clock: the world owns the logical clock, the delivery queue and the link
//! model, and the host page owns wall-clock time.
//!
//! A frame is one whole message on one logical connection. Over a Kafka
//! endpoint a [`Payload::Data`] frame is one Kafka frame *including* its
//! four-byte big-endian length prefix, so the bytes are exactly what TCP would
//! carry. Over an HTTP endpoint it is one complete HTTP/1.1 request or
//! response.

use bytes::Bytes;
use derive_more::{Display, From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Logical milliseconds since the world started.
pub type Millis = u64;

/// A node in the scenario. Ids are stable for the life of the world.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    Display,
    From,
    Into,
)]
#[serde(transparent)]
pub struct NodeId(pub u32);

/// The port a simulated broker listens on.
pub const KAFKA_PORT: u16 = 9092;
/// The port a simulated schema registry listens on.
pub const HTTP_PORT: u16 = 8081;
/// The port a client sends from. A client never listens.
pub const CLIENT_PORT: u16 = 0;

/// A listener or a client socket on a node.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub node: NodeId,
    pub port: u16,
}

impl Endpoint {
    #[must_use]
    pub const fn new(node: NodeId, port: u16) -> Self {
        Self { node, port }
    }

    /// The Kafka listener of `node`.
    #[must_use]
    pub const fn kafka(node: NodeId) -> Self {
        Self::new(node, KAFKA_PORT)
    }

    /// The HTTP listener of `node`.
    #[must_use]
    pub const fn http(node: NodeId) -> Self {
        Self::new(node, HTTP_PORT)
    }

    /// The client socket of `node`.
    #[must_use]
    pub const fn client(node: NodeId) -> Self {
        Self::new(node, CLIENT_PORT)
    }

    /// Whether this endpoint is a listener rather than a client socket.
    #[must_use]
    pub const fn is_listener(self) -> bool {
        self.port != CLIENT_PORT
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.node, self.port)
    }
}

/// A logical connection. The client side allocates the id, unique per client
/// node, so a connection is identified end to end by `(client endpoint, id)`.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    Serialize,
    Deserialize,
    Display,
    From,
    Into,
)]
#[serde(transparent)]
pub struct ConnId(pub u32);

/// What a frame carries.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Payload {
    /// The client opens a connection to the destination endpoint.
    Open,
    /// One complete message.
    Data(Bytes),
    /// Either side closes. In-flight frames on the connection are lost.
    Close,
}

impl Payload {
    /// The message bytes, if this is a data frame.
    #[must_use]
    pub fn data(&self) -> Option<&Bytes> {
        match self {
            Self::Data(bytes) => Some(bytes),
            Self::Open | Self::Close => None,
        }
    }
}

/// The JSON shape of a [`Payload`]: `{"kind":"open"}`, `{"kind":"close"}` or
/// `{"kind":"data","data":"<base64>"}`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum PayloadRepr {
    Open,
    Data { data: String },
    Close,
}

impl Serialize for Payload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::Engine as _;
        let repr = match self {
            Self::Open => PayloadRepr::Open,
            Self::Close => PayloadRepr::Close,
            Self::Data(bytes) => PayloadRepr::Data {
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
            },
        };
        repr.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use base64::Engine as _;
        Ok(match PayloadRepr::deserialize(deserializer)? {
            PayloadRepr::Open => Self::Open,
            PayloadRepr::Close => Self::Close,
            PayloadRepr::Data { data } => Self::Data(Bytes::from(
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(serde::de::Error::custom)?,
            )),
        })
    }
}

/// One message on one connection, addressed by endpoints.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Frame {
    pub src: Endpoint,
    pub dst: Endpoint,
    pub conn: ConnId,
    pub payload: Payload,
}

impl Frame {
    #[must_use]
    pub fn open(src: Endpoint, dst: Endpoint, conn: ConnId) -> Self {
        Self {
            src,
            dst,
            conn,
            payload: Payload::Open,
        }
    }

    #[must_use]
    pub fn data(src: Endpoint, dst: Endpoint, conn: ConnId, bytes: Bytes) -> Self {
        Self {
            src,
            dst,
            conn,
            payload: Payload::Data(bytes),
        }
    }

    #[must_use]
    pub fn close(src: Endpoint, dst: Endpoint, conn: ConnId) -> Self {
        Self {
            src,
            dst,
            conn,
            payload: Payload::Close,
        }
    }

    /// A frame back to the sender of this one, on the same connection.
    #[must_use]
    pub fn reply(&self, payload: Payload) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            conn: self.conn,
            payload,
        }
    }

    /// The endpoint of the client side of the connection this frame belongs to.
    #[must_use]
    pub fn client_endpoint(&self) -> Endpoint {
        if self.src.is_listener() {
            self.dst
        } else {
            self.src
        }
    }

    /// The `(client endpoint, connection)` pair that identifies the connection
    /// end to end.
    #[must_use]
    pub fn conn_key(&self) -> (Endpoint, ConnId) {
        (self.client_endpoint(), self.conn)
    }
}

/// A frame the host must carry to another world instance, with the logical
/// time the sending world would have delivered it at.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TimedFrame {
    pub deliver_at: Millis,
    pub frame: Frame,
}

/// A small deterministic generator (xorshift64*). Every node owns one, seeded
/// from the world seed and its id, so a scenario replays exactly.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // A zero state would stay zero forever.
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number in `0..n`; `0` when `n` is `0`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

/// What a node can do to the world while it handles a frame or a timer.
///
/// `Ctx` is the only way a node talks to the world. Frames queued with
/// [`Ctx::send`] leave when the call returns, in order.
pub struct Ctx<'a> {
    now: Millis,
    me: NodeId,
    outbox: &'a mut Vec<Frame>,
    timer: &'a mut Option<Millis>,
    events: &'a mut Vec<(&'static str, serde_json::Value)>,
    rng: &'a mut Rng,
}

impl<'a> Ctx<'a> {
    /// Build a context over caller-owned buffers. The world and the test
    /// harness call this; a node never does.
    #[must_use]
    pub fn new(
        now: Millis,
        me: NodeId,
        outbox: &'a mut Vec<Frame>,
        timer: &'a mut Option<Millis>,
        events: &'a mut Vec<(&'static str, serde_json::Value)>,
        rng: &'a mut Rng,
    ) -> Self {
        Self {
            now,
            me,
            outbox,
            timer,
            events,
            rng,
        }
    }

    /// The logical time of the frame or timer being handled.
    #[must_use]
    pub fn now(&self) -> Millis {
        self.now
    }

    /// The id of the node being called.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }

    /// Queue a frame. Delivery time is `now` plus the link latency. The frame is
    /// dropped when the link is cut or the destination is down.
    pub fn send(&mut self, frame: Frame) {
        self.outbox.push(frame);
    }

    /// Arm the node's single timer at absolute time `at`. Re-arming replaces the
    /// earlier deadline, so a node that needs several deadlines keeps its own
    /// heap and arms the earliest. A deadline at or before `now` fires on the
    /// next step.
    pub fn arm(&mut self, at: Millis) {
        *self.timer = Some(at);
    }

    /// Record a timeline event. `kind` is a short machine tag such as
    /// `"produce"` or `"elect"`; `detail` is the JSON the page renders. Set
    /// `detail["level"]` to `"warn"` or `"error"` to highlight it.
    pub fn event(&mut self, kind: &'static str, detail: serde_json::Value) {
        self.events.push((kind, detail));
    }

    /// A deterministic pseudo-random number in `0..n`.
    pub fn rand(&mut self, n: u64) -> u64 {
        self.rng.below(n)
    }
}

/// A simulated process: a broker, a schema registry, or a client application.
///
/// The world calls the methods one at a time, never concurrently. A node keeps
/// no wall-clock time and no `HashMap` whose iteration order reaches the wire;
/// use `BTreeMap` wherever order is observable. A node must only send frames
/// whose `src.node` is its own id.
pub trait Node {
    /// The node kind, as it appears in the scenario: `"broker"`,
    /// `"schema-registry"`, `"producer"`, `"consumer"`, `"streams"`, ...
    fn kind(&self) -> &'static str;

    /// The node boots, or boots again after a [`Fault::Restart`]. Durable state
    /// (logs, schemas) survives; connections and in-memory session state do not.
    ///
    /// [`Fault::Restart`]: crate::lab::world::Fault::Restart
    fn start(&mut self, ctx: &mut Ctx<'_>);

    /// The node halts. It receives no frame or timer until it starts again.
    fn stop(&mut self) {}

    /// A frame arrived for one of this node's endpoints.
    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame);

    /// The timer armed with [`Ctx::arm`] fired. The world advances the clock to
    /// the deadline before it calls, so `ctx.now()` is the deadline.
    fn on_timer(&mut self, ctx: &mut Ctx<'_>);

    /// A control command from the page or a scenario step. The command shape is
    /// node-kind specific and documented on the node type.
    ///
    /// # Errors
    /// Returns the node's own error text when the command is unknown or cannot
    /// be applied.
    fn control(
        &mut self,
        ctx: &mut Ctx<'_>,
        command: serde_json::Value,
    ) -> Result<serde_json::Value, String>;

    /// Observable state for the inspector, as JSON. The page calls this every
    /// animation frame, so keep it cheap.
    fn snapshot(&self) -> serde_json::Value;
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn payload_round_trips_through_json_with_base64_bytes() {
        let frames = [
            Frame::open(
                Endpoint::client(NodeId(1)),
                Endpoint::kafka(NodeId(2)),
                ConnId(7),
            ),
            Frame::data(
                Endpoint::client(NodeId(1)),
                Endpoint::kafka(NodeId(2)),
                ConnId(7),
                Bytes::from_static(&[0, 0, 0, 2, 0xFF, 0x10]),
            ),
            Frame::close(
                Endpoint::kafka(NodeId(2)),
                Endpoint::client(NodeId(1)),
                ConnId(7),
            ),
        ];
        for frame in frames {
            let json = serde_json::to_string(&frame).unwrap();
            let back: Frame = serde_json::from_str(&json).unwrap();
            assert!(back == frame);
        }
        let json = serde_json::to_value(Payload::Data(Bytes::from_static(b"hi"))).unwrap();
        assert!(json == serde_json::json!({"kind": "data", "data": "aGk="}));
    }

    #[test]
    fn reply_swaps_endpoints_and_keeps_the_connection() {
        let req = Frame::data(
            Endpoint::client(NodeId(1)),
            Endpoint::kafka(NodeId(2)),
            ConnId(3),
            Bytes::new(),
        );
        let resp = req.reply(Payload::Close);
        assert!(resp.src == Endpoint::kafka(NodeId(2)));
        assert!(resp.dst == Endpoint::client(NodeId(1)));
        assert!(resp.conn == ConnId(3));
        assert!(req.conn_key() == resp.conn_key());
        assert!(req.conn_key() == (Endpoint::client(NodeId(1)), ConnId(3)));
    }

    #[test]
    fn rng_is_deterministic_and_bounded() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let xs: Vec<u64> = (0..16).map(|_| a.below(10)).collect();
        let ys: Vec<u64> = (0..16).map(|_| b.below(10)).collect();
        assert!(xs == ys);
        assert!(xs.iter().all(|&x| x < 10));
        assert!(Rng::new(0).next_u64() != Rng::new(1).next_u64());
        assert!(Rng::new(5).below(0) == 0);
    }

    #[test]
    fn ctx_collects_sends_timer_and_events() {
        let mut outbox = Vec::new();
        let mut timer = None;
        let mut events = Vec::new();
        let mut rng = Rng::new(1);
        let mut ctx = Ctx::new(
            100,
            NodeId(4),
            &mut outbox,
            &mut timer,
            &mut events,
            &mut rng,
        );
        assert!(ctx.now() == 100);
        assert!(ctx.me() == NodeId(4));
        ctx.send(Frame::open(
            Endpoint::client(NodeId(4)),
            Endpoint::kafka(NodeId(1)),
            ConnId(0),
        ));
        ctx.arm(250);
        ctx.arm(300);
        ctx.event("test", serde_json::json!({"n": 1}));
        let r = ctx.rand(3);
        assert!(r < 3);
        assert!(outbox.len() == 1);
        assert!(timer == Some(300));
        assert!(events == vec![("test", serde_json::json!({"n": 1}))]);
    }
}
