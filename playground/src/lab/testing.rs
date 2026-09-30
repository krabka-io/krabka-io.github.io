//! The test harness, and two diagnostic node kinds every scenario can use.
//!
//! [`TestWorld`] wraps a [`World`] with the run-until helpers the module tests
//! share. [`EchoNode`] and [`PingerNode`] exercise
//! the network without any protocol: `echo` answers every data frame with the
//! same bytes; `pinger` opens a connection to a target and pings it on a
//! period. They also serve the page as a latency probe.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde_json::{Value, json};

use super::{
    LabError, config_field, config_field_or,
    net::{
        ConnId, Ctx, DurableImage, DurableOp, Endpoint, Frame, Millis, Node, NodeId, Payload, Rng,
    },
    scenario::{NodeSpec, Scenario},
    world::World,
};

/// A world plus the helpers tests need.
pub struct TestWorld {
    world: Option<World>,
}

impl TestWorld {
    /// Build and start a world from a scenario.
    ///
    /// # Panics
    /// Panics when the scenario is invalid; a test scenario is a literal.
    #[must_use]
    pub fn from_scenario(scenario: &Scenario) -> Self {
        Self {
            world: Some(World::from_scenario(scenario).expect("test scenario builds")),
        }
    }

    /// Build a world from a scenario and the durable images a page keeps
    /// across a reload, hosting `hosted` (empty = all), and start it.
    ///
    /// # Panics
    /// Panics when the scenario is invalid; a test scenario is a literal.
    #[must_use]
    pub fn from_scenario_with_state(
        scenario: &Scenario,
        hosted: &[NodeId],
        images: BTreeMap<NodeId, DurableImage>,
    ) -> Self {
        Self {
            world: Some(
                World::from_scenario_with_state(scenario, hosted, images)
                    .expect("test scenario builds"),
            ),
        }
    }

    /// Build and start a world from a scenario JSON literal.
    ///
    /// # Panics
    /// Panics when the JSON or the scenario is invalid.
    #[must_use]
    pub fn from_json(json: &str) -> Self {
        let scenario: Scenario = serde_json::from_str(json).expect("scenario json parses");
        Self::from_scenario(&scenario)
    }

    /// # Panics
    /// Panics after [`TestWorld::take`].
    #[must_use]
    pub fn world(&self) -> &World {
        self.world.as_ref().expect("world present")
    }

    /// # Panics
    /// Panics after [`TestWorld::take`].
    pub fn world_mut(&mut self) -> &mut World {
        self.world.as_mut().expect("world present")
    }

    /// Take the world out; the harness is unusable afterwards.
    ///
    /// # Panics
    /// Panics when the world was already taken.
    pub fn take(&mut self) -> World {
        self.world.take().expect("world present")
    }

    /// Advance logical time by `ms`, running everything due.
    pub fn run_for(&mut self, ms: Millis) {
        let until = self.world().now() + ms;
        self.world_mut().step_until(until);
    }

    /// Step until `pred` holds or `max_ms` of logical time passed. Returns
    /// whether the predicate held. The predicate is checked after every step,
    /// so a fleeting state is not missed.
    pub fn run_until(&mut self, mut pred: impl FnMut(&World) -> bool, max_ms: Millis) -> bool {
        let deadline = self.world().now() + max_ms;
        loop {
            if pred(self.world()) {
                return true;
            }
            let world = self.world_mut();
            if !world.step_once(deadline) {
                return pred(self.world());
            }
        }
    }

    /// A node's snapshot.
    ///
    /// # Panics
    /// Panics when the node does not exist.
    #[must_use]
    pub fn snapshot(&self, id: NodeId) -> Value {
        self.world().node_snapshot(id).expect("node exists")
    }

    /// The events of one kind, oldest first.
    #[must_use]
    pub fn events_of_kind(&self, kind: &str) -> Vec<super::events::Event> {
        self.world()
            .events()
            .filter(|e| e.kind == kind)
            .cloned()
            .collect()
    }
}

/// Owned buffers behind a [`Ctx`], for unit tests that drive a node without a
/// world.
pub struct CtxBuffers {
    pub now: Millis,
    pub me: NodeId,
    pub outbox: Vec<Frame>,
    pub timer: Option<Millis>,
    pub events: Vec<(&'static str, Value)>,
    pub durable: Vec<DurableOp>,
    pub rng: Rng,
}

impl CtxBuffers {
    #[must_use]
    pub fn new(me: NodeId) -> Self {
        Self {
            now: 0,
            me,
            outbox: Vec::new(),
            timer: None,
            events: Vec::new(),
            durable: Vec::new(),
            rng: Rng::new(1),
        }
    }

    /// Run `f` with a context over these buffers at time `now`.
    pub fn with<T>(&mut self, now: Millis, f: impl FnOnce(&mut Ctx<'_>) -> T) -> T {
        self.now = now;
        let mut ctx = Ctx::new(
            now,
            self.me,
            &mut self.outbox,
            &mut self.timer,
            &mut self.events,
            &mut self.durable,
            &mut self.rng,
        );
        f(&mut ctx)
    }

    /// Take every durable op recorded so far.
    pub fn take_durable(&mut self) -> Vec<DurableOp> {
        std::mem::take(&mut self.durable)
    }

    /// Take every frame queued so far.
    pub fn take_frames(&mut self) -> Vec<Frame> {
        std::mem::take(&mut self.outbox)
    }
}

/// Answers every data frame with the same bytes on the same connection.
///
/// The frame counter is durable: the node persists it under the key-value
/// store `counters` and restores it through [`Node::load`], so the storage
/// path can be exercised without a broker.
pub struct EchoNode {
    id: NodeId,
    frames: u64,
    closes: u64,
    started: u64,
    last_seq: Option<u64>,
}

impl EchoNode {
    /// # Errors
    /// Never fails; the signature matches the other node kinds.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        Ok(Self {
            id: spec.id,
            frames: 0,
            closes: 0,
            started: 0,
            last_seq: None,
        })
    }
}

impl Node for EchoNode {
    fn kind(&self) -> &'static str {
        "echo"
    }

    fn load(&mut self, image: DurableImage) {
        self.frames = image
            .kv
            .get("counters")
            .and_then(|kv| kv.get("frames"))
            .and_then(|v| std::str::from_utf8(&v.0).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
    }

    fn start(&mut self, _ctx: &mut Ctx<'_>) {
        self.started += 1;
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.dst.node != self.id {
            return;
        }
        self.frames += 1;
        ctx.persist(DurableOp::Put {
            store: "counters".to_string(),
            key: "frames".to_string(),
            value: Bytes::from(self.frames.to_string()),
        });
        match &frame.payload {
            Payload::Data(bytes) => {
                if let Some(seq) = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.strip_prefix("ping "))
                {
                    self.last_seq = seq.parse().ok();
                }
                let echo = frame.reply(Payload::Data(bytes.clone()));
                ctx.send(echo);
            }
            Payload::Close => self.closes += 1,
            Payload::Open => {}
        }
    }

    fn on_timer(&mut self, _ctx: &mut Ctx<'_>) {}

    fn control(&mut self, _ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        Err(format!("echo has no commands: {command}"))
    }

    fn snapshot(&self) -> Value {
        json!({
            "frames": self.frames,
            "closes": self.closes,
            "started": self.started,
            "last_seq": self.last_seq,
        })
    }
}

/// Opens a connection to `target` and sends `ping N` every `period_ms`.
///
/// Config: `{ "target": <node id>, "port": 9092?, "period_ms": 100? }`.
/// Command: `{ "cmd": "ping" }` sends one ping now.
pub struct PingerNode {
    id: NodeId,
    target: Endpoint,
    period: Millis,
    conn: ConnId,
    open: bool,
    seq: u64,
    echoes: u64,
    closes: u64,
    /// Send time of every ping still waiting for its echo.
    pending: Vec<(u64, Millis)>,
    rtt_sum: Millis,
}

impl PingerNode {
    /// # Errors
    /// Returns a config error when `target` is missing.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let target: NodeId = config_field(spec, "target")?;
        let port: u16 = config_field_or(spec, "port", super::net::KAFKA_PORT)?;
        let period: Millis = config_field_or(spec, "period_ms", 100)?;
        Ok(Self {
            id: spec.id,
            target: Endpoint::new(target, port),
            period: period.max(1),
            conn: ConnId(0),
            open: false,
            seq: 0,
            echoes: 0,
            closes: 0,
            pending: Vec::new(),
            rtt_sum: 0,
        })
    }

    fn ensure_open(&mut self, ctx: &mut Ctx<'_>) {
        if !self.open {
            self.conn = ConnId(self.conn.0 + 1);
            self.open = true;
            self.pending.clear();
            ctx.send(Frame::open(
                Endpoint::client(self.id),
                self.target,
                self.conn,
            ));
        }
    }

    fn ping(&mut self, ctx: &mut Ctx<'_>) {
        self.ensure_open(ctx);
        self.seq += 1;
        self.pending.push((self.seq, ctx.now()));
        ctx.send(Frame::data(
            Endpoint::client(self.id),
            self.target,
            self.conn,
            Bytes::from(format!("ping {}", self.seq)),
        ));
    }
}

impl Node for PingerNode {
    fn kind(&self) -> &'static str {
        "pinger"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.open = false;
        self.ensure_open(ctx);
        ctx.arm(ctx.now() + self.period);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.conn != self.conn {
            return;
        }
        match &frame.payload {
            Payload::Data(bytes) => {
                // Only an echo of a ping this pinger sent counts; anything
                // else on the connection would skew the mean round trip.
                if let Some(seq) = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.strip_prefix("ping "))
                    .and_then(|s| s.parse::<u64>().ok())
                    && let Some(pos) = self.pending.iter().position(|(s, _)| *s == seq)
                {
                    let (_, sent) = self.pending.remove(pos);
                    self.echoes += 1;
                    self.rtt_sum += ctx.now() - sent;
                }
            }
            Payload::Close => {
                self.closes += 1;
                self.open = false;
            }
            Payload::Open => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        self.ping(ctx);
        ctx.arm(ctx.now() + self.period);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        match command.get("cmd").and_then(Value::as_str) {
            Some("ping") => {
                self.ping(ctx);
                Ok(json!({ "seq": self.seq }))
            }
            other => Err(format!("unknown pinger command {other:?}")),
        }
    }

    fn snapshot(&self) -> Value {
        let mean_rtt = self.rtt_sum.checked_div(self.echoes).unwrap_or(0);
        json!({
            "target": self.target.node,
            "open": self.open,
            "sent": self.seq,
            "echoes": self.echoes,
            "closes": self.closes,
            "pending": self.pending.len(),
            "mean_rtt_ms": mean_rtt,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::world::Fault;

    #[test]
    fn pinger_reports_round_trip_time() {
        let mut w = TestWorld::from_json(
            r#"{"version":1,"links":{"default_latency_ms":25},"nodes":[
                {"id":1,"kind":"echo"},
                {"id":2,"kind":"pinger","config":{"target":1,"period_ms":200}}]}"#,
        );
        assert!(!w.run_until(|_| false, 1_000));
        assert!(w.snapshot(NodeId(2))["mean_rtt_ms"] == 50);
        assert!(w.snapshot(NodeId(2))["echoes"] == 4);
        // The fifth ping left at 1000 and is still on the wire.
        assert!(w.snapshot(NodeId(2))["pending"] == 1);
        assert!(w.snapshot(NodeId(1))["last_seq"] == 4);
    }

    #[test]
    fn echo_restores_its_frame_counter_from_a_durable_image() {
        let scenario = r#"{"version":1,"nodes":[
            {"id":1,"kind":"echo"},
            {"id":2,"kind":"pinger","config":{"target":1,"period_ms":100}}]}"#;
        let mut w = TestWorld::from_json(scenario);
        w.run_for(350);
        let ops = w.world_mut().drain_durable();
        assert!(ops.iter().all(|(node, _)| *node == NodeId(1)));
        let mut image = DurableImage::default();
        for (_, op) in ops {
            image.apply(op);
        }
        let frames = w.snapshot(NodeId(1))["frames"].as_u64().unwrap();
        assert!(frames == 4);
        // A reload: the host folds the ops and hands the image to the new node.
        let scenario: Scenario = serde_json::from_str(scenario).unwrap();
        let mut reloaded = World::from_scenario_with_state(
            &scenario,
            &[],
            std::collections::BTreeMap::from([(NodeId(1), image)]),
        )
        .unwrap();
        assert!(reloaded.node_snapshot(NodeId(1)).unwrap()["frames"] == 4);
        reloaded.step_until(150);
        assert!(reloaded.node_snapshot(NodeId(1)).unwrap()["frames"] == 6);
        // A wipe clears the host's stores too.
        reloaded.fault(Fault::Wipe { node: NodeId(1) });
        let ops = reloaded.drain_durable();
        assert!(
            ops.iter()
                .any(|(node, op)| *node == NodeId(1) && *op == DurableOp::ClearAll)
        );
    }

    #[test]
    fn ctx_buffers_drive_a_node_without_a_world() {
        let spec = NodeSpec::new(9, "pinger", "p", json!({ "target": 1 }));
        let mut node = PingerNode::from_spec(&spec).unwrap();
        let mut buffers = CtxBuffers::new(NodeId(9));
        buffers.with(0, |ctx| node.start(ctx));
        let frames = buffers.take_frames();
        assert!(
            frames
                == vec![Frame::open(
                    Endpoint::client(NodeId(9)),
                    Endpoint::kafka(NodeId(1)),
                    ConnId(1)
                )]
        );
        assert!(buffers.timer == Some(100));
        buffers.with(100, |ctx| node.on_timer(ctx));
        let frames = buffers.take_frames();
        assert!(frames.len() == 1);
        assert!(frames[0].payload.data() == Some(&Bytes::from_static(b"ping 1")));
    }
}
