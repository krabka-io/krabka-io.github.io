//! A cluster of controllers wired by hand, for the module tests.
//!
//! [`ControllerNode`] is the [`Node`] a broker would be if it were nothing but
//! its controller. [`Cluster`] moves the frames each node's [`CtxBuffers`]
//! collected to their destination after a latency and fires timers in time
//! order, with the ordering rules of `World::route`: FIFO per connection and
//! `(time, sequence)` order overall. It adds the two faults the quorum tests
//! need: a drop hook and per-frame latency jitter, which reorders frames on
//! different connections.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

use serde_json::{Value, json};
use uuid::Uuid;

use super::{ControllerCore, RAFT_PORT};
use crate::lab::{
    net::{
        ConnId, Ctx, DurableImage, DurableOp, Endpoint, Frame, Millis, Node, NodeId, Payload, Rng,
    },
    testing::CtxBuffers,
};

/// The cluster id every test cluster runs under.
pub const CLUSTER_ID: Uuid = Uuid::from_u128(0x1ab);

/// A node that is only a controller: every call goes to the core, and the
/// timer is re-armed from the core's next deadline afterwards, which is what
/// a broker does around its embedded controller.
pub struct ControllerNode {
    pub core: ControllerCore,
}

impl ControllerNode {
    pub fn new(me: NodeId, voters: &[NodeId]) -> Self {
        Self {
            core: ControllerCore::new(me, voters, CLUSTER_ID),
        }
    }

    fn arm(&self, ctx: &mut Ctx<'_>) {
        if let Some(at) = self.core.next_deadline() {
            ctx.arm(at);
        }
    }
}

impl Node for ControllerNode {
    fn kind(&self) -> &'static str {
        "controller"
    }

    fn load(&mut self, image: DurableImage) {
        self.core.load(&image);
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.core.start(ctx);
        self.arm(ctx);
    }

    fn stop(&mut self) {
        self.core.stop();
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        self.core.on_frame(ctx, frame);
        self.arm(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        self.core.on_timer(ctx);
        self.arm(ctx);
    }

    // `{"cmd": "propose", "records": [<MetadataRecord>...]}` proposes a batch
    // and answers `{"offset": n}`, or the leader it knows as an error.
    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let result = match command.get("cmd").and_then(Value::as_str) {
            Some("propose") => {
                let records = serde_json::from_value(command["records"].clone())
                    .map_err(|e| format!("records: {e}"))?;
                self.core
                    .propose(ctx, records)
                    .map(|id| json!({ "offset": id.0 }))
                    .map_err(|e| e.to_string())
            }
            other => Err(format!("unknown controller command {other:?}")),
        };
        self.arm(ctx);
        result
    }

    fn snapshot(&self) -> Value {
        self.core.snapshot()
    }
}

/// One slot of the cluster.
struct Slot {
    node: ControllerNode,
    buffers: CtxBuffers,
    alive: bool,
    timer_gen: u64,
}

enum Item {
    Deliver(Frame),
    Timer { node: NodeId, generation: u64 },
}

struct Scheduled {
    at: Millis,
    seq: u64,
    item: Item,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

/// A fault hook: `true` drops the frame.
type DropHook = Box<dyn FnMut(&Frame) -> bool>;

/// The hand-wired cluster.
pub struct Cluster {
    slots: BTreeMap<NodeId, Slot>,
    queue: BinaryHeap<Reverse<Scheduled>>,
    now: Millis,
    seq: u64,
    latency: Millis,
    /// Extra latency in `0..jitter`, drawn per frame, so frames on different
    /// connections overtake each other.
    jitter: Millis,
    drop: Option<DropHook>,
    /// Open connections, as `World` tracks them, so a kill closes them.
    conns: BTreeMap<(Endpoint, ConnId), Endpoint>,
    last_delivery: BTreeMap<(Endpoint, ConnId, bool), Millis>,
    rng: Rng,
    /// Every leader observed per epoch, checked after every step.
    leaders_by_epoch: BTreeMap<u32, BTreeSet<NodeId>>,
    pub events: Vec<(Millis, NodeId, &'static str, Value)>,
    /// Every durable op each node recorded, in order, as the host would keep
    /// them.
    pub durable: BTreeMap<NodeId, Vec<DurableOp>>,
    pub delivered: u64,
}

impl Cluster {
    /// A cluster of `voters` plus `observers`, every node started at time 0
    /// with a 5 ms link latency.
    pub fn new(voters: &[NodeId], observers: &[NodeId]) -> Self {
        let mut cluster = Self {
            slots: BTreeMap::new(),
            queue: BinaryHeap::new(),
            now: 0,
            seq: 0,
            latency: 5,
            jitter: 0,
            drop: None,
            conns: BTreeMap::new(),
            last_delivery: BTreeMap::new(),
            rng: Rng::new(0x5eed),
            leaders_by_epoch: BTreeMap::new(),
            events: Vec::new(),
            durable: BTreeMap::new(),
            delivered: 0,
        };
        for &id in voters.iter().chain(observers) {
            cluster.slots.insert(
                id,
                Slot {
                    node: ControllerNode::new(id, voters),
                    buffers: CtxBuffers::new(id),
                    alive: true,
                    timer_gen: 0,
                },
            );
        }
        let ids: Vec<NodeId> = cluster.slots.keys().copied().collect();
        for id in ids {
            cluster.call(id, Node::start);
        }
        cluster
    }

    pub fn now(&self) -> Millis {
        self.now
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.slots.keys().copied().collect()
    }

    /// The nodes that are not killed.
    pub fn live_ids(&self) -> Vec<NodeId> {
        self.slots
            .iter()
            .filter(|(_, slot)| slot.alive)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn node(&self, id: NodeId) -> &ControllerCore {
        &self.slots[&id].node.core
    }

    /// The durable state of a node, folded from its ops as the host folds
    /// them.
    pub fn durable_image(&self, id: NodeId) -> DurableImage {
        let mut image = DurableImage::default();
        for op in self.durable.get(&id).into_iter().flatten() {
            image.apply(op.clone());
        }
        image
    }

    /// Run `f` against a node's core behind a fresh context, then route what
    /// it sent and re-arm its timer.
    pub fn with_node<T>(
        &mut self,
        id: NodeId,
        f: impl FnOnce(&mut ControllerCore, &mut Ctx<'_>) -> T,
    ) -> T {
        let mut out = None;
        self.call(id, |node, ctx| {
            out = Some(f(&mut node.core, ctx));
            node.arm(ctx);
        });
        out.expect("the node was called")
    }

    pub fn set_drop(&mut self, hook: impl FnMut(&Frame) -> bool + 'static) {
        self.drop = Some(Box::new(hook));
    }

    pub fn clear_drop(&mut self) {
        self.drop = None;
    }

    pub fn set_jitter(&mut self, jitter: Millis) {
        self.jitter = jitter;
    }

    /// The node halts: its timers stop, frames to it are lost, and its peers
    /// see their connections to it close.
    pub fn kill(&mut self, id: NodeId) {
        let slot = self.slots.get_mut(&id).expect("node exists");
        slot.alive = false;
        slot.buffers.timer = None;
        slot.node.stop();
        let kept: Vec<Reverse<Scheduled>> = std::mem::take(&mut self.queue)
            .into_iter()
            .filter(|Reverse(s)| match &s.item {
                Item::Deliver(f) => f.dst.node != id,
                Item::Timer { .. } => true,
            })
            .collect();
        self.queue = kept.into_iter().collect();
        let affected: Vec<((Endpoint, ConnId), Endpoint)> = self
            .conns
            .iter()
            .filter(|((client, _), server)| client.node == id || server.node == id)
            .map(|(k, v)| (*k, *v))
            .collect();
        for ((client, conn), server) in affected {
            self.conns.remove(&(client, conn));
            let (src, dst) = if client.node == id {
                (client, server)
            } else {
                (server, client)
            };
            let at = self.now + self.latency;
            self.schedule(at, Item::Deliver(Frame::close(src, dst, conn)));
        }
    }

    /// The node boots again from its durable state.
    pub fn restart(&mut self, id: NodeId) {
        self.slots.get_mut(&id).expect("node exists").alive = true;
        self.call(id, Node::start);
    }

    pub fn leaders(&self) -> Vec<NodeId> {
        self.slots
            .values()
            .filter(|slot| slot.alive && slot.node.core.is_leader())
            .map(|slot| slot.node.core.me())
            .collect()
    }

    /// Whether every live node knows `leader` as the leader of one epoch.
    pub fn all_follow(&self, leader: NodeId) -> bool {
        let epoch = self.node(leader).epoch();
        self.node(leader).is_leader()
            && self.slots.values().filter(|slot| slot.alive).all(|slot| {
                slot.node.core.leader() == Some(leader) && slot.node.core.epoch() == epoch
            })
    }

    /// The single live leader, if the live nodes agree on one.
    pub fn settled_leader(&self) -> Option<NodeId> {
        match self.leaders().as_slice() {
            [leader] if self.all_follow(*leader) => Some(*leader),
            _ => None,
        }
    }

    /// Every epoch had at most one leader, over the whole run so far.
    pub fn assert_one_leader_per_epoch(&self) {
        for (epoch, leaders) in &self.leaders_by_epoch {
            assert2::assert!(leaders.len() == 1, "epoch {epoch} had leaders {leaders:?}");
        }
    }

    pub fn run_for(&mut self, ms: Millis) {
        let until = self.now + ms;
        self.step_until(until);
    }

    pub fn step_until(&mut self, until: Millis) {
        while self.step_once(until) {}
    }

    /// Step until `pred` holds or `max_ms` passed; the predicate is checked
    /// after every step.
    pub fn run_until(&mut self, mut pred: impl FnMut(&Self) -> bool, max_ms: Millis) -> bool {
        let deadline = self.now + max_ms;
        loop {
            if pred(self) {
                return true;
            }
            if !self.step_once(deadline) {
                return pred(self);
            }
        }
    }

    fn step_once(&mut self, until: Millis) -> bool {
        let due = self.queue.peek().is_some_and(|Reverse(s)| s.at <= until);
        if !due {
            self.now = self.now.max(until);
            return false;
        }
        let Some(Reverse(Scheduled { at, item, .. })) = self.queue.pop() else {
            return false;
        };
        self.now = self.now.max(at);
        match item {
            Item::Deliver(frame) => {
                let dst = frame.dst.node;
                if self.slots.get(&dst).is_some_and(|slot| slot.alive) {
                    self.delivered += 1;
                    if matches!(frame.payload, Payload::Close) {
                        self.conns.remove(&frame.conn_key());
                    }
                    self.call(dst, |node, ctx| node.on_frame(ctx, frame));
                }
            }
            Item::Timer { node, generation } => {
                let fire = self
                    .slots
                    .get(&node)
                    .is_some_and(|slot| slot.alive && slot.timer_gen == generation);
                if fire {
                    self.call(node, Node::on_timer);
                }
            }
        }
        self.record_leaders();
        true
    }

    fn record_leaders(&mut self) {
        for slot in self.slots.values() {
            if slot.alive && slot.node.core.is_leader() {
                self.leaders_by_epoch
                    .entry(slot.node.core.epoch())
                    .or_default()
                    .insert(slot.node.core.me());
            }
        }
    }

    fn schedule(&mut self, at: Millis, item: Item) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            seq: self.seq,
            item,
        }));
    }

    /// Hand a node to `f` behind a context, then route its frames, arm its
    /// timer and keep its events.
    fn call(&mut self, id: NodeId, f: impl FnOnce(&mut ControllerNode, &mut Ctx<'_>)) {
        let now = self.now;
        let slot = self.slots.get_mut(&id).expect("node exists");
        slot.buffers.timer = None;
        slot.buffers.with(now, |ctx| f(&mut slot.node, ctx));
        let frames = slot.buffers.take_frames();
        let timer = slot.buffers.timer.take();
        let events = std::mem::take(&mut slot.buffers.events);
        let durable = slot.buffers.take_durable();
        self.durable.entry(id).or_default().extend(durable);
        if let Some(at) = timer {
            slot.timer_gen += 1;
            let generation = slot.timer_gen;
            self.schedule(
                at.max(now),
                Item::Timer {
                    node: id,
                    generation,
                },
            );
        }
        self.events.extend(
            events
                .into_iter()
                .map(|(kind, detail)| (now, id, kind, detail)),
        );
        for frame in frames {
            self.route(id, frame);
        }
    }

    /// `World::route` without the link table: the drop hook, the base latency
    /// plus jitter, and the FIFO floor per connection and direction.
    fn route(&mut self, from: NodeId, frame: Frame) {
        assert2::assert!(frame.src.node == from, "a node may only speak for itself");
        assert2::assert!(
            frame.dst.port == RAFT_PORT || frame.src.port == RAFT_PORT,
            "controllers only speak on the raft port: {frame:?}"
        );
        let key = frame.conn_key();
        match frame.payload {
            Payload::Open => {
                self.conns.insert(key, frame.dst);
            }
            Payload::Close => {
                self.conns.remove(&key);
            }
            Payload::Data(_) => {}
        }
        if let Some(drop) = self.drop.as_mut()
            && drop(&frame)
        {
            return;
        }
        let latency = self.latency + self.rng.below(self.jitter);
        let direction = !frame.src.is_listener();
        let floor = self
            .last_delivery
            .get(&(key.0, key.1, direction))
            .copied()
            .unwrap_or(0);
        let at = (self.now + latency).max(floor);
        self.last_delivery.insert((key.0, key.1, direction), at);
        self.schedule(at, Item::Deliver(frame));
    }
}
