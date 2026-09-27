//! A harness that drives one client-side object against fake brokers with a
//! small deterministic scheduler: frames take `latency` to cross a link, the
//! client's deadline and the brokers' timers fire in time order, and a broker
//! can be killed so the client sees a `Close`.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use assert2::assert;

use super::{
    ClientEvent, KafkaClient, Producer, ProducerEvent,
    consumer::{Consumer, ConsumerEvent},
    fake_broker::{ClusterState, FakeBroker, Seen},
};
use crate::lab::{
    net::{ConnId, Ctx, Endpoint, Frame, Millis, Node, NodeId, Payload, Rng},
    testing::CtxBuffers,
};

/// The node id the harness gives the client.
pub const CLIENT_NODE: NodeId = NodeId(100);

/// A fake cluster of three brokers, ids 1 to 3 on nodes 1 to 3, the first
/// the controller and the group coordinator, with `topics` at replication
/// factor 3.
#[must_use]
pub fn cluster(topics: &[(&str, i32)]) -> Rc<RefCell<ClusterState>> {
    let state = ClusterState::new(&[(1, 1), (2, 2), (3, 3)]);
    for (topic, partitions) in topics {
        state.borrow_mut().add_topic(topic, *partitions, 3);
    }
    state
}

/// A client with Kafka's default options, bootstrapped at `bootstrap`.
#[must_use]
pub fn client(bootstrap: &[u32]) -> KafkaClient {
    KafkaClient::new(
        bootstrap
            .iter()
            .map(|node| Endpoint::kafka(NodeId(*node)))
            .collect(),
        "test",
        super::ClientOptions::default(),
    )
}

/// The client-side object the harness drives.
pub trait Driven {
    type Event;
    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<Self::Event>, Option<Millis>);
    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<Self::Event>, Option<Millis>);
}

impl Driven for KafkaClient {
    type Event = ClientEvent;

    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<ClientEvent>, Option<Millis>) {
        let events = self.on_frame(ctx, frame);
        (events, self.next_deadline(ctx.now()))
    }

    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ClientEvent>, Option<Millis>) {
        self.on_tick(ctx)
    }
}

impl Driven for Producer {
    type Event = ProducerEvent;

    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<ProducerEvent>, Option<Millis>) {
        self.on_frame(ctx, frame)
    }

    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ProducerEvent>, Option<Millis>) {
        self.on_tick(ctx)
    }
}

impl Driven for Consumer {
    type Event = ConsumerEvent;

    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<ConsumerEvent>, Option<Millis>) {
        self.on_frame(ctx, frame)
    }

    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ConsumerEvent>, Option<Millis>) {
        self.on_tick(ctx)
    }
}

/// Several consumers, each on a node of its own, driven by one harness. The
/// harness delivers a frame for any node that is not a broker here, and the
/// wrapper hands it to the member on that node.
pub struct Members {
    members: Vec<Member>,
}

struct Member {
    consumer: Consumer,
    bufs: CtxBuffers,
    alive: bool,
}

impl Members {
    /// One member per `(node, consumer)`.
    #[must_use]
    pub fn new(members: Vec<(NodeId, Consumer)>) -> Self {
        Self {
            members: members
                .into_iter()
                .map(|(node, consumer)| {
                    let mut bufs = CtxBuffers::new(node);
                    bufs.rng = Rng::new(u64::from(node.0));
                    Member {
                        consumer,
                        bufs,
                        alive: true,
                    }
                })
                .collect(),
        }
    }

    /// The consumer of member `i`.
    #[must_use]
    pub fn get(&self, i: usize) -> &Consumer {
        &self.members[i].consumer
    }

    /// Run `f` on member `i` behind its own context, and send what it sent.
    pub fn with<T>(
        &mut self,
        outer: &mut Ctx<'_>,
        i: usize,
        f: impl FnOnce(&mut Consumer, &mut Ctx<'_>) -> T,
    ) -> T {
        let member = &mut self.members[i];
        let consumer = &mut member.consumer;
        let result = member.bufs.with(outer.now(), |ctx| f(consumer, ctx));
        for frame in member.bufs.take_frames() {
            outer.send(frame);
        }
        result
    }

    /// Member `i` stops without a word: no frame reaches it and it sends
    /// none, as a crashed process whose connections stay open.
    pub fn crash(&mut self, i: usize) {
        self.members[i].alive = false;
    }
}

impl Driven for Members {
    type Event = (usize, ConsumerEvent);

    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<Self::Event>, Option<Millis>) {
        let Some(i) = self
            .members
            .iter()
            .position(|m| m.alive && m.bufs.me == frame.dst.node)
        else {
            return (Vec::new(), None);
        };
        let (events, _) = self.with(ctx, i, |consumer, ctx| consumer.on_frame(ctx, frame));
        let deadline = self.deadline(ctx.now());
        (events.into_iter().map(|e| (i, e)).collect(), deadline)
    }

    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<Self::Event>, Option<Millis>) {
        let mut events = Vec::new();
        for i in 0..self.members.len() {
            if !self.members[i].alive {
                continue;
            }
            let (ticked, _) = self.with(ctx, i, Consumer::on_tick);
            events.extend(ticked.into_iter().map(|e| (i, e)));
        }
        (events, self.deadline(ctx.now()))
    }
}

impl Members {
    fn deadline(&self, now: Millis) -> Option<Millis> {
        self.members
            .iter()
            .filter(|m| m.alive)
            .filter_map(|m| m.consumer.next_deadline(now))
            .min()
    }
}

/// A scheduled frame delivery or broker timer.
enum Item {
    Deliver(Frame),
    BrokerTimer { node: NodeId, generation: u64 },
}

struct Scheduled {
    at: Millis,
    seq: u64,
    item: Item,
}

/// A broker slot of the harness.
struct BrokerSlot {
    broker: FakeBroker,
    bufs: CtxBuffers,
    alive: bool,
    timer_generation: u64,
}

/// The harness. See the module documentation.
pub struct Harness<C: Driven> {
    pub client: C,
    bufs: CtxBuffers,
    brokers: BTreeMap<NodeId, BrokerSlot>,
    pub state: Rc<RefCell<ClusterState>>,
    now: Millis,
    seq: u64,
    pub latency: Millis,
    queue: Vec<Scheduled>,
    client_deadline: Option<Millis>,
    conns: BTreeMap<(Endpoint, ConnId), Endpoint>,
    pub events: Vec<C::Event>,
    pub delivered: u64,
    /// Steps taken at the current logical time, to catch a node that keeps
    /// asking for a tick without making progress.
    steps_now: (Millis, u32),
}

impl<C: Driven> Harness<C> {
    /// A harness over `state`, with one fake broker per broker of the
    /// cluster state.
    pub fn new(client: C, state: Rc<RefCell<ClusterState>>) -> Self {
        let brokers = state
            .borrow()
            .brokers
            .iter()
            .map(|(id, node)| {
                (
                    *node,
                    BrokerSlot {
                        broker: FakeBroker::new(*node, *id, Rc::clone(&state)),
                        bufs: CtxBuffers::new(*node),
                        alive: true,
                        timer_generation: 0,
                    },
                )
            })
            .collect();
        let mut harness = Self {
            client,
            bufs: CtxBuffers::new(CLIENT_NODE),
            brokers,
            state,
            now: 0,
            seq: 0,
            latency: 5,
            queue: Vec::new(),
            client_deadline: Some(0),
            conns: BTreeMap::new(),
            events: Vec::new(),
            delivered: 0,
            steps_now: (0, 0),
        };
        let nodes: Vec<NodeId> = harness.brokers.keys().copied().collect();
        for node in nodes {
            harness.call_broker(node, FakeBroker::start);
        }
        harness
    }

    pub fn now(&self) -> Millis {
        self.now
    }

    /// Run `f` on the client at the current time, route what it sent, and
    /// tick it once, as a node calls `on_tick` after it hands the client
    /// work.
    pub fn with_client<T>(&mut self, f: impl FnOnce(&mut C, &mut Ctx<'_>) -> T) -> T {
        let now = self.now;
        let client = &mut self.client;
        let result = self.bufs.with(now, |ctx| f(client, ctx));
        self.after_client_call();
        self.tick();
        result
    }

    /// Tick the client now.
    pub fn tick(&mut self) {
        let now = self.now;
        let client = &mut self.client;
        let (events, deadline) = self.bufs.with(now, |ctx| client.tick(ctx));
        self.events.extend(events);
        self.client_deadline = deadline;
        self.after_client_call();
    }

    fn after_client_call(&mut self) {
        if let Some(at) = self.bufs.timer.take() {
            self.client_deadline = Some(self.client_deadline.map_or(at, |d| d.min(at)));
        }
        let frames = self.bufs.take_frames();
        for frame in frames {
            self.route(frame);
        }
    }

    fn call_broker(&mut self, node: NodeId, f: impl FnOnce(&mut FakeBroker, &mut Ctx<'_>)) {
        let now = self.now;
        let Some(slot) = self.brokers.get_mut(&node) else {
            return;
        };
        if !slot.alive {
            return;
        }
        let broker = &mut slot.broker;
        slot.bufs.with(now, |ctx| f(broker, ctx));
        let timer = slot.bufs.timer.take().map(|at| {
            slot.timer_generation += 1;
            (at, slot.timer_generation)
        });
        let frames = slot.bufs.take_frames();
        if let Some((at, generation)) = timer {
            self.schedule(at.max(now), Item::BrokerTimer { node, generation });
        }
        for frame in frames {
            self.route(frame);
        }
    }

    fn schedule(&mut self, at: Millis, item: Item) {
        self.seq += 1;
        self.queue.push(Scheduled {
            at,
            seq: self.seq,
            item,
        });
    }

    fn route(&mut self, frame: Frame) {
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
        let dead = self.brokers.get(&frame.dst.node).is_some_and(|s| !s.alive)
            || self.brokers.get(&frame.src.node).is_some_and(|s| !s.alive);
        if dead {
            return;
        }
        let at = self.now + self.latency;
        self.schedule(at, Item::Deliver(frame));
    }

    fn pop_next(&mut self, until: Millis) -> Option<Scheduled> {
        let index = self
            .queue
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| (s.at, s.seq))
            .map(|(i, _)| i)?;
        if self.queue[index].at > until {
            return None;
        }
        Some(self.queue.swap_remove(index))
    }

    /// Run one item due at or before `until`; `false` when nothing is.
    ///
    /// # Panics
    /// Panics when a million steps run at one logical time: a deadline that
    /// never moves would spin a real world forever.
    pub fn step_once(&mut self, until: Millis) -> bool {
        if self.steps_now.0 == self.now {
            self.steps_now.1 += 1;
            assert!(
                self.steps_now.1 < 1_000_000,
                "busy loop: a million steps at {} ms",
                self.now
            );
        } else {
            self.steps_now = (self.now, 0);
        }
        let next_at = self.queue.iter().map(|s| s.at).min().unwrap_or(Millis::MAX);
        let client_at = self.client_deadline.unwrap_or(Millis::MAX);
        if client_at <= next_at && client_at <= until {
            self.now = self.now.max(client_at);
            self.client_deadline = None;
            self.tick();
            return true;
        }
        let Some(item) = self.pop_next(until) else {
            self.now = self.now.max(until);
            return false;
        };
        self.now = self.now.max(item.at);
        match item.item {
            Item::Deliver(frame) => {
                self.delivered += 1;
                if self.brokers.contains_key(&frame.dst.node) {
                    let node = frame.dst.node;
                    self.call_broker(node, |broker, ctx| broker.on_frame(ctx, frame));
                } else {
                    let now = self.now;
                    let client = &mut self.client;
                    let (events, deadline) = self.bufs.with(now, |ctx| client.frame(ctx, frame));
                    self.events.extend(events);
                    self.client_deadline = match (self.client_deadline, deadline) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    self.after_client_call();
                }
            }
            Item::BrokerTimer { node, generation } => {
                let current = self.brokers.get(&node).map(|s| s.timer_generation);
                if current == Some(generation) {
                    self.call_broker(node, FakeBroker::on_timer);
                }
            }
        }
        true
    }

    /// Advance `ms` of logical time.
    pub fn run_for(&mut self, ms: Millis) {
        let until = self.now + ms;
        while self.step_once(until) {}
        self.now = until;
    }

    /// Step until `pred` holds or `max_ms` passed; returns whether it held.
    pub fn run_until(&mut self, mut pred: impl FnMut(&Self) -> bool, max_ms: Millis) -> bool {
        let until = self.now + max_ms;
        loop {
            if pred(self) {
                return true;
            }
            if !self.step_once(until) {
                return pred(self);
            }
        }
    }

    /// Take the events collected so far.
    pub fn take_events(&mut self) -> Vec<C::Event> {
        std::mem::take(&mut self.events)
    }

    /// The requests the brokers saw for `api_key`.
    pub fn seen(&self, api_key: i16) -> Vec<Seen> {
        self.state.borrow().seen(api_key)
    }

    /// Kill a broker: its queued frames are dropped and every connection to it
    /// closes from the broker side, as the world does for a killed node.
    pub fn kill_broker(&mut self, node: NodeId) {
        let Some(slot) = self.brokers.get_mut(&node) else {
            return;
        };
        slot.alive = false;
        self.queue.retain(|s| match &s.item {
            Item::Deliver(f) => f.src.node != node && f.dst.node != node,
            Item::BrokerTimer { node: n, .. } => *n != node,
        });
        let affected: Vec<((Endpoint, ConnId), Endpoint)> = self
            .conns
            .iter()
            .filter(|(_, server)| server.node == node)
            .map(|(k, v)| (*k, *v))
            .collect();
        for ((client, conn), server) in affected {
            self.conns.remove(&(client, conn));
            let at = self.now + self.latency;
            self.schedule(at, Item::Deliver(Frame::close(server, client, conn)));
        }
    }

    /// Bring a killed broker back.
    pub fn restart_broker(&mut self, node: NodeId) {
        if let Some(slot) = self.brokers.get_mut(&node) {
            slot.alive = true;
        }
        self.call_broker(node, FakeBroker::start);
    }

    /// Frames delivered to and from the brokers so far.
    pub fn open_connections(&self) -> usize {
        self.conns.len()
    }
}
