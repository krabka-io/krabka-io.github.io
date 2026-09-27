//! The world: every node, the links between them, the delivery queue and the
//! logical clock.
//!
//! The world is a discrete-event simulator. Every queued item is a frame to
//! deliver or a timer to fire, ordered by `(time, sequence)`, so a run is
//! deterministic for a seed and a sequence of inputs. The page drives it with
//! [`World::step_until`] from its animation loop and reads
//! [`World::snapshot`] back.
//!
//! A world can be *partial*: it holds every node of the scenario but runs only
//! the nodes the page marked as hosted. A frame for a node hosted elsewhere
//! leaves through [`World::drain_egress`], crosses a WebRTC data channel, and
//! enters the other world through [`World::push_ingress`].

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

use serde::{Deserialize, Serialize};

use super::{
    LabError, build_node,
    events::{Event, EventLog},
    external::REAL_BROKER_KIND,
    net::{
        ConnId, Ctx, DurableImage, DurableOp, Endpoint, Frame, KAFKA_PORT, Millis, Node, NodeId,
        Payload, Rng, TimedFrame,
    },
    scenario::{LinkOverride, NodeSpec, Scenario, TopicSpec},
};

/// A fault the page injects. Faults apply immediately at the current time.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Fault {
    /// The node halts; its durable state survives and its connections close.
    Kill {
        node: NodeId,
    },
    /// The node boots again from its durable state.
    Restart {
        node: NodeId,
    },
    /// The node boots again from nothing, as if its disk were lost.
    Wipe {
        node: NodeId,
    },
    /// Cut the link between two nodes, both ways.
    Partition {
        a: NodeId,
        b: NodeId,
    },
    Heal {
        a: NodeId,
        b: NodeId,
    },
    /// Cut every link of the node.
    Isolate {
        node: NodeId,
    },
    Reconnect {
        node: NodeId,
    },
    /// Set the one-way latency of a link.
    Latency {
        a: NodeId,
        b: NodeId,
        ms: Millis,
    },
    /// Set the probability, in parts per thousand, that a data frame on the
    /// link is lost.
    Loss {
        a: NodeId,
        b: NodeId,
        permille: u32,
    },
}

/// The link parameters of one pair of nodes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
struct Link {
    latency_ms: Option<Millis>,
    loss_permille: u32,
    cut: bool,
}

/// One node's slot in the world.
struct Slot {
    spec: NodeSpec,
    node: Box<dyn Node>,
    alive: bool,
    isolated: bool,
    timer: Option<Millis>,
    timer_gen: u64,
    rng: Rng,
    /// The last snapshot a remote host sent for a node this world does not
    /// run, or the page reported for an external node it runs.
    remote_snapshot: Option<serde_json::Value>,
    /// The node runs outside the world, in a process the page hosts.
    external: bool,
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

/// A frame on the wire, for the page to animate.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct InFlight {
    pub src: NodeId,
    pub dst: NodeId,
    pub at: Millis,
    pub label: String,
}

/// One node as the page sees it.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub id: NodeId,
    pub kind: String,
    pub name: String,
    pub x: f64,
    pub y: f64,
    pub hosted: bool,
    pub alive: bool,
    pub isolated: bool,
    pub state: serde_json::Value,
}

/// One link with a non-default parameter.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LinkSnapshot {
    pub a: NodeId,
    pub b: NodeId,
    pub latency_ms: Millis,
    pub loss_permille: u32,
    pub cut: bool,
}

/// The whole world as the page renders it.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct WorldSnapshot {
    pub now: Millis,
    pub seed: u64,
    pub name: String,
    pub default_latency_ms: Millis,
    pub nodes: Vec<NodeSnapshot>,
    pub links: Vec<LinkSnapshot>,
    pub in_flight: Vec<InFlight>,
    pub event_count: usize,
    /// Frames delivered per `(src, dst)` pair since the world started.
    pub delivered: Vec<(NodeId, NodeId, u64)>,
}

/// The simulator.
pub struct World {
    seed: u64,
    id: String,
    name: String,
    now: Millis,
    seq: u64,
    default_latency: Millis,
    nodes: BTreeMap<NodeId, Slot>,
    links: BTreeMap<(NodeId, NodeId), Link>,
    queue: BinaryHeap<Reverse<Scheduled>>,
    /// Open connections: the client `(endpoint, conn)` and the listener it
    /// opened to.
    conns: BTreeMap<(Endpoint, ConnId), Endpoint>,
    /// The latest delivery time queued per connection and direction (`true`
    /// is client to server), so a link that gets faster mid-flight cannot let a
    /// later frame overtake an earlier one.
    last_delivery: BTreeMap<(Endpoint, ConnId, bool), Millis>,
    hosted: Option<BTreeSet<NodeId>>,
    /// The hidden admin node that creates the scenario's topics, with the
    /// broker it bootstraps from. The admin is hosted wherever that broker is.
    admin: Option<(NodeId, NodeId)>,
    egress: Vec<TimedFrame>,
    durable: Vec<(NodeId, DurableOp)>,
    /// Frames due at external nodes this world hosts, for the page to hand
    /// to their processes.
    external_out: Vec<TimedFrame>,
    events: EventLog,
    delivered: BTreeMap<(NodeId, NodeId), u64>,
    topics: Vec<TopicSpec>,
    /// The ids of the controller quorum's voters: fixed when the scenario
    /// loads, as a static `KRaft` quorum is fixed when its cluster starts.
    quorum_voters: Vec<u32>,
    rng: Rng,
}

impl World {
    /// An empty world with `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            id: String::new(),
            name: String::new(),
            now: 0,
            seq: 0,
            default_latency: super::scenario::DEFAULT_LATENCY_MS,
            nodes: BTreeMap::new(),
            links: BTreeMap::new(),
            queue: BinaryHeap::new(),
            conns: BTreeMap::new(),
            last_delivery: BTreeMap::new(),
            hosted: None,
            admin: None,
            egress: Vec::new(),
            durable: Vec::new(),
            external_out: Vec::new(),
            events: EventLog::default(),
            delivered: BTreeMap::new(),
            topics: Vec::new(),
            quorum_voters: Vec::new(),
            rng: Rng::new(seed ^ 0xA5A5_5A5A),
        }
    }

    /// Build a world from a scenario and start every node.
    ///
    /// # Errors
    /// Returns an error when a node kind is unknown, a node id repeats, or a
    /// node rejects its configuration.
    pub fn from_scenario(scenario: &Scenario) -> Result<Self, LabError> {
        Self::from_scenario_hosted(scenario, &[])
    }

    /// Build a world from a scenario and start only the `hosted` nodes; an
    /// empty list hosts every node. The other nodes never start here, so no
    /// connection is opened and then torn down when hosting is decided later.
    ///
    /// # Errors
    /// Returns an error when a node kind is unknown, a node id repeats, or a
    /// node rejects its configuration.
    pub fn from_scenario_hosted(scenario: &Scenario, hosted: &[NodeId]) -> Result<Self, LabError> {
        Self::from_scenario_with_state(scenario, hosted, BTreeMap::new())
    }

    /// Build a world from a scenario, hosting `hosted` (empty = all), and hand
    /// each node in `images` its durable state before it starts. This is how
    /// the page restores what `IndexedDB` kept across a reload.
    ///
    /// # Errors
    /// Returns an error when a node kind is unknown, a node id repeats, or a
    /// node rejects its configuration.
    pub fn from_scenario_with_state(
        scenario: &Scenario,
        hosted: &[NodeId],
        mut images: BTreeMap<NodeId, DurableImage>,
    ) -> Result<Self, LabError> {
        if scenario.version != super::scenario::SCENARIO_VERSION {
            return Err(LabError::InvalidScenario(format!(
                "unsupported scenario version {}",
                scenario.version
            )));
        }
        let mut world = Self::new(scenario.seed);
        world.id.clone_from(&scenario.id);
        world.name.clone_from(&scenario.name);
        world.default_latency = scenario.links.default_latency_ms;
        world.topics.clone_from(&scenario.topics);
        if !hosted.is_empty() {
            world.hosted = Some(hosted.iter().copied().collect());
        }
        if let Some(spec) = scenario.nodes.iter().find(|s| s.id.0 == 0) {
            return Err(LabError::InvalidScenario(format!(
                "node `{}` has id 0, which is reserved for `add_node` to allocate",
                spec.display_name()
            )));
        }
        // Links first: a node's startup frames must already see a cut or slow link.
        for o in &scenario.link_overrides {
            world.apply_link_override(o);
        }
        world.quorum_voters = scenario
            .nodes
            .iter()
            .filter(|spec| wants_quorum_vote(spec))
            .map(|spec| spec.id.0)
            .collect();
        world.quorum_voters.sort_unstable();
        for spec in &scenario.nodes {
            let image = images.remove(&spec.id).filter(|image| !image.is_empty());
            world.add_node_with_state(spec.clone(), image)?;
        }
        if !scenario.topics.is_empty() {
            world.add_admin_for_topics()?;
        }
        Ok(world)
    }

    /// The admin client that creates the scenario's topics. It is a real node
    /// of kind `admin`, hidden from the builder, bootstrapped at the brokers
    /// of the scenario, simulated or real.
    fn add_admin_for_topics(&mut self) -> Result<(), LabError> {
        let bootstrap: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|s| s.spec.kind == "broker" || s.spec.kind == REAL_BROKER_KIND)
            .map(|s| s.spec.id)
            .collect();
        if bootstrap.is_empty() {
            return Err(LabError::InvalidScenario(
                "topics need at least one broker".to_string(),
            ));
        }
        let id = self.next_free_id();
        let broker = bootstrap[0];
        self.admin = Some((id, broker));
        if let Some(set) = &mut self.hosted
            && set.contains(&broker)
        {
            set.insert(id);
        }
        let spec = NodeSpec::new(
            id.0,
            "admin",
            "scenario-admin",
            serde_json::json!({ "bootstrap": bootstrap, "topics": self.topics }),
        );
        self.add_node(spec).map(|_| ())
    }

    fn next_free_id(&self) -> NodeId {
        NodeId(self.nodes.keys().last().map_or(1, |id| id.0 + 1))
    }

    /// Add a node and start it. The spec's id must be free; an id of `0` asks
    /// the world to pick the next free one.
    ///
    /// # Errors
    /// Returns an error when the id is taken, the kind is unknown, or the node
    /// rejects its configuration.
    pub fn add_node(&mut self, spec: NodeSpec) -> Result<NodeId, LabError> {
        self.add_node_with_state(spec, None)
    }

    /// Add a node, hand it `image` when there is one, and start it.
    ///
    /// # Errors
    /// Returns an error when the id is taken, the kind is unknown, or the node
    /// rejects its configuration.
    pub fn add_node_with_state(
        &mut self,
        mut spec: NodeSpec,
        image: Option<DurableImage>,
    ) -> Result<NodeId, LabError> {
        if spec.id.0 == 0 {
            spec.id = self.next_free_id();
        }
        if self.nodes.contains_key(&spec.id) {
            return Err(LabError::InvalidScenario(format!(
                "node id {} is taken",
                spec.id
            )));
        }
        if spec.name.is_empty() {
            spec.name = spec.display_name();
        }
        let quorum = self.quorum_role(&spec)?;
        let mut node = build_node(&quorum.spec)?;
        if let Some(image) = image {
            node.load(image);
        }
        let id = spec.id;
        self.join_quorum(id, &quorum);
        let external = node.external();
        let rng = Rng::new(self.seed ^ (u64::from(id.0) << 32) ^ u64::from(id.0));
        self.nodes.insert(
            id,
            Slot {
                spec,
                node,
                alive: true,
                isolated: false,
                timer: None,
                timer_gen: 0,
                rng,
                remote_snapshot: None,
                external,
            },
        );
        self.record(Some(id), "node_added", serde_json::json!({}));
        if self.is_hosted(id) {
            self.call(id, |node, ctx| node.start(ctx));
        }
        Ok(id)
    }

    /// The spec a node is built from, with its place in the controller quorum.
    ///
    /// A broker that names no `controller_quorum_voters` gets the scenario's
    /// quorum and votes only when it is one of its voters. A broker added
    /// after the scenario loaded therefore joins as an observer until the
    /// scenario loads again, except the first voter of a world with no
    /// quorum yet, which starts one. A voter of the loaded quorum stays one.
    fn quorum_role(&self, spec: &NodeSpec) -> Result<QuorumRole, LabError> {
        let mut built = spec.clone();
        let names_voters = spec.config.get("controller_quorum_voters").is_some();
        if spec.kind != "broker" || names_voters {
            return Ok(QuorumRole {
                spec: built,
                starts_quorum: false,
                observes: false,
            });
        }
        let id = spec.id.0;
        let wants = wants_quorum_vote(spec);
        let in_quorum = self.quorum_voters.contains(&id);
        if in_quorum && !wants {
            return Err(LabError::config(
                spec,
                "the controller quorum is static: this broker stays a voter until the scenario loads again",
            ));
        }
        let starts_quorum = wants && self.quorum_voters.is_empty();
        let mut voters = self.quorum_voters.clone();
        if starts_quorum {
            voters.push(id);
        }
        let is_voter = in_quorum || starts_quorum;
        if built.config.is_null() {
            built.config = serde_json::json!({});
        }
        if let Some(config) = built.config.as_object_mut() {
            config.insert(
                "controller_quorum_voters".to_string(),
                serde_json::json!(voters),
            );
            config.insert("voter".to_string(), serde_json::json!(is_voter));
        }
        Ok(QuorumRole {
            spec: built,
            starts_quorum,
            observes: wants && !is_voter,
        })
    }

    /// Record a built node's place in the controller quorum.
    fn join_quorum(&mut self, id: NodeId, role: &QuorumRole) {
        if role.starts_quorum {
            self.quorum_voters.push(id.0);
        }
        if role.observes {
            self.record(
                Some(id),
                "quorum_observer",
                serde_json::json!({ "level": "info", "voters": self.quorum_voters }),
            );
        }
    }

    /// Remove a node. Its connections close and its queued frames are dropped.
    pub fn remove_node(&mut self, id: NodeId) {
        if !self.nodes.contains_key(&id) {
            return;
        }
        self.purge_frames(|f| f.src.node == id || f.dst.node == id);
        self.close_connections_of(id, false);
        self.nodes.remove(&id);
        self.links.retain(|(a, b), _| *a != id && *b != id);
        self.durable.push((id, DurableOp::ClearAll));
        self.record(Some(id), "node_removed", serde_json::json!({}));
    }

    /// Replace a node's spec. The node is rebuilt from the new configuration
    /// and started from nothing, like [`Fault::Wipe`].
    ///
    /// # Errors
    /// Returns an error when the node does not exist or rejects the new
    /// configuration.
    pub fn update_node(&mut self, id: NodeId, spec: NodeSpec) -> Result<(), LabError> {
        let mut spec = spec;
        spec.id = id;
        if spec.name.is_empty() {
            spec.name = spec.display_name();
        }
        if !self.nodes.contains_key(&id) {
            return Err(LabError::NoSuchNode(id));
        }
        let quorum = self.quorum_role(&spec)?;
        let node = build_node(&quorum.spec)?;
        self.join_quorum(id, &quorum);
        let slot = self.nodes.get_mut(&id).ok_or(LabError::NoSuchNode(id))?;
        slot.spec = spec;
        slot.node = node;
        slot.alive = true;
        slot.timer = None;
        self.purge_frames(|f| f.src.node == id || f.dst.node == id);
        self.close_connections_of(id, false);
        self.durable.push((id, DurableOp::ClearAll));
        self.record(Some(id), "node_updated", serde_json::json!({}));
        if self.is_hosted(id) {
            self.call(id, |node, ctx| node.start(ctx));
        }
        Ok(())
    }

    /// Move a node on the canvas. Positions are echoed, never read.
    pub fn set_position(&mut self, id: NodeId, x: f64, y: f64) {
        if let Some(slot) = self.nodes.get_mut(&id) {
            slot.spec.x = x;
            slot.spec.y = y;
        }
    }

    /// The current scenario, positions included.
    #[must_use]
    pub fn scenario(&self) -> Scenario {
        let mut s = Scenario::empty(self.seed);
        s.id.clone_from(&self.id);
        s.name.clone_from(&self.name);
        s.links.default_latency_ms = self.default_latency;
        s.nodes = self
            .nodes
            .values()
            .filter(|slot| slot.spec.kind != "admin")
            .map(|slot| slot.spec.clone())
            .collect();
        s.topics.clone_from(&self.topics);
        s.link_overrides = self
            .links
            .iter()
            .filter(|(_, l)| l.latency_ms.is_some() || l.loss_permille != 0 || l.cut)
            .map(|(&(a, b), l)| LinkOverride {
                a,
                b,
                latency_ms: l.latency_ms,
                loss_permille: (l.loss_permille != 0).then_some(l.loss_permille),
                cut: l.cut,
            })
            .collect();
        s
    }

    #[must_use]
    pub fn now(&self) -> Millis {
        self.now
    }

    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The ids of every node, in ascending order.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.keys().copied()
    }

    /// The spec of a node.
    #[must_use]
    pub fn spec(&self, id: NodeId) -> Option<&NodeSpec> {
        self.nodes.get(&id).map(|s| &s.spec)
    }

    /// The topics the scenario creates at start.
    #[must_use]
    pub fn topics(&self) -> &[TopicSpec] {
        &self.topics
    }

    // ---- stepping ---------------------------------------------------------------

    /// Run every frame delivery and timer due at or before `until`, in order,
    /// and leave the clock at `until`. Returns the number of steps taken.
    pub fn step_until(&mut self, until: Millis) -> usize {
        let mut steps = 0;
        while self.step_once(until) {
            steps += 1;
        }
        steps
    }

    /// Run the next due item if it is due at or before `until`. Returns
    /// `false`, with the clock at `until`, when nothing is due by then.
    pub fn step_once(&mut self, until: Millis) -> bool {
        if !self.has_work_by(until) {
            self.now = self.now.max(until);
            return false;
        }
        let Some(Reverse(Scheduled { at, item, .. })) = self.queue.pop() else {
            return false;
        };
        self.now = self.now.max(at);
        match item {
            Item::Deliver(frame) => self.deliver(frame),
            Item::Timer { node, generation } => self.fire_timer(node, generation),
        }
        true
    }

    /// Whether anything is queued at or before `until`.
    #[must_use]
    pub fn has_work_by(&self, until: Millis) -> bool {
        self.queue.peek().is_some_and(|Reverse(s)| s.at <= until)
    }

    fn schedule(&mut self, at: Millis, item: Item) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            seq: self.seq,
            item,
        }));
    }

    fn deliver(&mut self, frame: Frame) {
        let dst = frame.dst.node;
        if !self.is_hosted(dst) {
            self.egress.push(TimedFrame {
                deliver_at: self.now,
                frame,
            });
            return;
        }
        let Some(slot) = self.nodes.get(&dst) else {
            return;
        };
        if !slot.alive {
            // A killed process on a live host: its TCP stack refuses a new
            // connection at once (a reset), so the client sees "connection
            // refused" instead of waiting out a setup timeout. Anything else
            // sent to it is lost.
            if frame.payload == Payload::Open {
                self.route(dst, Frame::close(frame.dst, frame.src, frame.conn));
            }
            return;
        }
        let external = slot.external;
        *self.delivered.entry((frame.src.node, dst)).or_insert(0) += 1;
        if matches!(frame.payload, Payload::Close) {
            self.forget_conn(frame.conn_key());
        }
        if external {
            self.external_out.push(TimedFrame {
                deliver_at: self.now,
                frame,
            });
            return;
        }
        self.call(dst, |node, ctx| node.on_frame(ctx, frame));
    }

    fn fire_timer(&mut self, id: NodeId, generation: u64) {
        let Some(slot) = self.nodes.get_mut(&id) else {
            return;
        };
        if !slot.alive || slot.timer_gen != generation || slot.timer.is_none() {
            return;
        }
        slot.timer = None;
        self.call(id, |node, ctx| node.on_timer(ctx));
    }

    /// Hand the node to `f` behind a fresh [`Ctx`], then apply what it queued:
    /// frames are routed, the timer is armed, events are recorded.
    fn call(&mut self, id: NodeId, f: impl FnOnce(&mut dyn Node, &mut Ctx<'_>)) {
        let Some(slot) = self.nodes.get_mut(&id) else {
            return;
        };
        let mut outbox = Vec::new();
        let mut timer = None;
        let mut events = Vec::new();
        let mut durable = Vec::new();
        let now = self.now;
        {
            let mut ctx = Ctx::new(
                now,
                id,
                &mut outbox,
                &mut timer,
                &mut events,
                &mut durable,
                &mut slot.rng,
            );
            f(slot.node.as_mut(), &mut ctx);
        }
        if let Some(at) = timer {
            self.arm(id, at);
        }
        self.durable.extend(durable.into_iter().map(|op| (id, op)));
        for (kind, detail) in events {
            self.events.push(now, Some(id), kind, detail);
        }
        for frame in outbox {
            self.route(id, frame);
        }
    }

    fn arm(&mut self, id: NodeId, at: Millis) {
        let Some(slot) = self.nodes.get_mut(&id) else {
            return;
        };
        slot.timer_gen += 1;
        slot.timer = Some(at);
        let generation = slot.timer_gen;
        self.schedule(
            at.max(self.now),
            Item::Timer {
                node: id,
                generation,
            },
        );
    }

    /// Queue a frame a node sent: apply the link model, then either schedule
    /// the delivery or hand it to the host for a node hosted elsewhere.
    fn route(&mut self, from: NodeId, frame: Frame) {
        if frame.src.node != from {
            // A node may only speak for itself.
            return;
        }
        let key = frame.conn_key();
        if frame.payload == Payload::Open {
            self.conns.insert(key, frame.dst);
        }
        let closing = frame.payload == Payload::Close;
        let (a, b) = (frame.src.node, frame.dst.node);
        let link = self.link(a, b);
        let isolated = |w: &Self, n: NodeId| w.nodes.get(&n).is_some_and(|s| s.isolated);
        if a != b && (link.cut || isolated(self, a) || isolated(self, b)) {
            if closing {
                self.forget_conn(key);
            }
            return;
        }
        if matches!(frame.payload, Payload::Data(_))
            && link.loss_permille > 0
            && self.rng.below(1000) < u64::from(link.loss_permille)
        {
            self.record(
                Some(from),
                "frame_lost",
                serde_json::json!({ "to": b, "level": "warn" }),
            );
            return;
        }
        let latency = if a == b {
            0
        } else {
            link.latency_ms.unwrap_or(self.default_latency)
        };
        let direction = !frame.src.is_listener();
        let floor = self
            .last_delivery
            .get(&(key.0, key.1, direction))
            .copied()
            .unwrap_or(0);
        let at = (self.now + latency).max(floor);
        self.last_delivery.insert((key.0, key.1, direction), at);
        if self.is_hosted(b) {
            self.schedule(at, Item::Deliver(frame));
        } else {
            self.egress.push(TimedFrame {
                deliver_at: at,
                frame,
            });
        }
        // The close took its place behind the connection's earlier frames;
        // only now can the floors go.
        if closing {
            self.forget_conn(key);
        }
    }

    fn link(&self, a: NodeId, b: NodeId) -> Link {
        self.links.get(&pair(a, b)).copied().unwrap_or_default()
    }

    fn link_mut(&mut self, a: NodeId, b: NodeId) -> &mut Link {
        self.links.entry(pair(a, b)).or_default()
    }

    fn apply_link_override(&mut self, o: &LinkOverride) {
        let link = self.link_mut(o.a, o.b);
        if o.latency_ms.is_some() {
            link.latency_ms = o.latency_ms;
        }
        if let Some(p) = o.loss_permille {
            link.loss_permille = p;
        }
        link.cut = o.cut;
    }

    /// Drop every queued frame `pred` selects.
    fn purge_frames(&mut self, pred: impl Fn(&Frame) -> bool) {
        let kept: Vec<Reverse<Scheduled>> = std::mem::take(&mut self.queue)
            .into_iter()
            .filter(|Reverse(s)| match &s.item {
                Item::Deliver(f) => !pred(f),
                Item::Timer { .. } => true,
            })
            .collect();
        self.queue = kept.into_iter().collect();
        // Frames waiting for another host, or for an external process, are
        // on the same wire.
        self.egress.retain(|t| !pred(&t.frame));
        self.external_out.retain(|t| !pred(&t.frame));
    }

    /// Forget a connection and the delivery floors that kept its frames in
    /// order, so a later connection that reuses the id starts fresh.
    fn forget_conn(&mut self, key: (Endpoint, ConnId)) {
        self.conns.remove(&key);
        self.last_delivery.remove(&(key.0, key.1, true));
        self.last_delivery.remove(&(key.0, key.1, false));
    }

    /// Close every connection with `id` on either side. Every peer gets a
    /// `Close` frame, the way its TCP stack reports a reset, and so does the
    /// node itself when `notify_self` is set, which is what an isolated but
    /// live node needs; a node that halted, was rebuilt or moved away never
    /// sees its own closes.
    fn close_connections_of(&mut self, id: NodeId, notify_self: bool) {
        let affected: Vec<((Endpoint, ConnId), Endpoint)> = self
            .conns
            .iter()
            .filter(|((client, _), server)| client.node == id || server.node == id)
            .map(|(k, v)| (*k, *v))
            .collect();
        for ((client, conn), server) in affected {
            self.forget_conn((client, conn));
            let at = self.now
                + self
                    .link(client.node, server.node)
                    .latency_ms
                    .unwrap_or(self.default_latency);
            let (own, peer) = if client.node == id {
                (client, server)
            } else {
                (server, client)
            };
            self.schedule(at, Item::Deliver(Frame::close(own, peer, conn)));
            if notify_self {
                self.schedule(at, Item::Deliver(Frame::close(peer, own, conn)));
            }
        }
    }

    /// The number of per-connection delivery floors still tracked.
    #[cfg(test)]
    fn delivery_floors(&self) -> usize {
        self.last_delivery.len()
    }

    /// Close every connection that crosses the `a`–`b` link.
    fn close_connections_between(&mut self, a: NodeId, b: NodeId) {
        let affected: Vec<((Endpoint, ConnId), Endpoint)> = self
            .conns
            .iter()
            .filter(|((client, _), server)| pair(client.node, server.node) == pair(a, b))
            .map(|(k, v)| (*k, *v))
            .collect();
        for ((client, conn), server) in affected {
            self.forget_conn((client, conn));
            let at = self.now + self.link(a, b).latency_ms.unwrap_or(self.default_latency);
            self.schedule(at, Item::Deliver(Frame::close(server, client, conn)));
            self.schedule(at, Item::Deliver(Frame::close(client, server, conn)));
        }
    }

    // ---- faults -----------------------------------------------------------------

    /// Apply a fault now.
    pub fn fault(&mut self, fault: Fault) {
        let detail = serde_json::to_value(fault).unwrap_or(serde_json::Value::Null);
        match fault {
            Fault::Kill { node } => {
                if let Some(slot) = self.nodes.get_mut(&node)
                    && slot.alive
                {
                    slot.alive = false;
                    slot.timer = None;
                    slot.node.stop();
                }
                // A halted node's unsent frames are lost, like the bytes still
                // in a crashed machine's buffers.
                self.purge_frames(|f| f.src.node == node || f.dst.node == node);
                self.close_connections_of(node, false);
            }
            Fault::Restart { node } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.alive = true;
                    if self.is_hosted(node) {
                        self.call(node, |n, ctx| n.start(ctx));
                    }
                }
            }
            Fault::Wipe { node } => {
                if let Some(slot) = self.nodes.get(&node) {
                    let spec = slot.spec.clone();
                    // A rejected config was accepted once, so rebuilding cannot fail.
                    let _ = self.update_node(node, spec);
                }
            }
            Fault::Partition { a, b } => {
                self.link_mut(a, b).cut = true;
                self.purge_frames(|f| pair(f.src.node, f.dst.node) == pair(a, b));
                self.close_connections_between(a, b);
            }
            Fault::Heal { a, b } => {
                self.link_mut(a, b).cut = false;
            }
            Fault::Isolate { node } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.isolated = true;
                }
                self.purge_frames(|f| (f.src.node == node) != (f.dst.node == node));
                self.close_connections_of(node, true);
            }
            Fault::Reconnect { node } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.isolated = false;
                }
            }
            Fault::Latency { a, b, ms } => {
                self.link_mut(a, b).latency_ms = Some(ms);
            }
            Fault::Loss { a, b, permille } => {
                self.link_mut(a, b).loss_permille = permille.min(1000);
            }
        }
        self.record(None, "fault", detail);
    }

    /// Send a control command to a node.
    ///
    /// # Errors
    /// Returns the node's own error text, or a message when the node does not
    /// exist, is down, or is hosted elsewhere.
    pub fn control(
        &mut self,
        id: NodeId,
        command: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let slot = self.nodes.get(&id).ok_or_else(|| format!("no node {id}"))?;
        if !slot.alive {
            return Err(format!("node {id} is down"));
        }
        if !self.is_hosted(id) {
            return Err(format!("node {id} is hosted by another peer"));
        }
        let mut result = Err("node did not answer".to_string());
        self.call(id, |node, ctx| result = node.control(ctx, command));
        result
    }

    fn record(&mut self, node: Option<NodeId>, kind: &str, detail: serde_json::Value) {
        self.events.push(self.now, node, kind, detail);
    }

    // ---- observation ------------------------------------------------------------

    /// The events with an index of at least `index`.
    #[must_use]
    pub fn events_since(&self, index: usize) -> Vec<Event> {
        self.events.since(index)
    }

    /// The number of events recorded so far.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Every retained event.
    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }

    /// A node's own snapshot, or the last one its remote host sent.
    #[must_use]
    pub fn node_snapshot(&self, id: NodeId) -> Option<serde_json::Value> {
        let slot = self.nodes.get(&id)?;
        Some(if self.is_hosted(id) && !slot.external {
            slot.node.snapshot()
        } else if slot.external {
            slot.remote_snapshot
                .clone()
                .unwrap_or_else(|| slot.node.snapshot())
        } else {
            slot.remote_snapshot
                .clone()
                .unwrap_or(serde_json::Value::Null)
        })
    }

    /// Frames delivered from `a` to `b` since the world started.
    #[must_use]
    pub fn delivered_between(&self, a: NodeId, b: NodeId) -> u64 {
        self.delivered.get(&(a, b)).copied().unwrap_or(0)
    }

    /// The whole world for the page.
    #[must_use]
    pub fn snapshot(&self) -> WorldSnapshot {
        let nodes = self
            .nodes
            .values()
            .map(|slot| NodeSnapshot {
                id: slot.spec.id,
                kind: slot.spec.kind.clone(),
                name: slot.spec.name.clone(),
                x: slot.spec.x,
                y: slot.spec.y,
                hosted: self.is_hosted(slot.spec.id),
                alive: slot.alive,
                isolated: slot.isolated,
                state: self
                    .node_snapshot(slot.spec.id)
                    .unwrap_or(serde_json::Value::Null),
            })
            .collect();
        let links = self
            .links
            .iter()
            .filter(|(_, l)| l.latency_ms.is_some() || l.loss_permille != 0 || l.cut)
            .map(|(&(a, b), l)| LinkSnapshot {
                a,
                b,
                latency_ms: l.latency_ms.unwrap_or(self.default_latency),
                loss_permille: l.loss_permille,
                cut: l.cut,
            })
            .collect();
        let in_flight = self
            .queue
            .iter()
            .filter_map(|Reverse(s)| match &s.item {
                Item::Deliver(f) if f.src.node != f.dst.node => Some(InFlight {
                    src: f.src.node,
                    dst: f.dst.node,
                    at: s.at,
                    label: frame_label(f),
                }),
                _ => None,
            })
            .collect();
        WorldSnapshot {
            now: self.now,
            seed: self.seed,
            name: self.name.clone(),
            default_latency_ms: self.default_latency,
            nodes,
            links,
            in_flight,
            event_count: self.events.len(),
            delivered: self
                .delivered
                .iter()
                .map(|(&(a, b), &n)| (a, b, n))
                .collect(),
        }
    }

    // ---- distributed hosting ----------------------------------------------------

    fn is_hosted(&self, id: NodeId) -> bool {
        self.hosted.as_ref().is_none_or(|set| set.contains(&id))
    }

    /// Run only `nodes` here; every other node is hosted by another peer. An
    /// empty list means this world runs everything. A node that becomes hosted
    /// starts; one that stops being hosted halts and drops its connections.
    pub fn set_hosted(&mut self, nodes: &[NodeId]) {
        let mut new: Option<BTreeSet<NodeId>> = if nodes.is_empty() {
            None
        } else {
            Some(nodes.iter().copied().collect())
        };
        if let (Some(set), Some((admin, broker))) = (&mut new, self.admin)
            && set.contains(&broker)
        {
            set.insert(admin);
        }
        let before: Vec<(NodeId, bool)> = self
            .nodes
            .keys()
            .map(|&id| (id, self.is_hosted(id)))
            .collect();
        self.hosted = new;
        for (id, was) in before {
            let is = self.is_hosted(id);
            if was && !is {
                // The node's connections reset when it moves: what is in
                // flight on them is lost, as for a kill, so nothing stale
                // reaches its new host on a connection it never opened.
                self.purge_frames(|f| f.src.node == id || f.dst.node == id);
                self.close_connections_of(id, false);
                if let Some(slot) = self.nodes.get_mut(&id) {
                    slot.timer = None;
                    slot.node.stop();
                }
            } else if !was && is && self.nodes.get(&id).is_some_and(|slot| slot.alive) {
                // A node killed while it was hosted elsewhere stays down until
                // a restart; only a live node starts here.
                self.call(id, |node, ctx| node.start(ctx));
            }
        }
    }

    /// The ids this world runs, or every id when it runs everything.
    #[must_use]
    pub fn hosted(&self) -> Vec<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|&id| self.is_hosted(id))
            .collect()
    }

    /// Frames for nodes hosted elsewhere, queued since the last drain.
    pub fn drain_egress(&mut self) -> Vec<TimedFrame> {
        std::mem::take(&mut self.egress)
    }

    /// Durable-state ops recorded since the last drain, in order, for the
    /// host to write to `IndexedDB`.
    pub fn drain_durable(&mut self) -> Vec<(NodeId, DurableOp)> {
        std::mem::take(&mut self.durable)
    }

    /// Frames that reached external nodes this world hosts since the last
    /// drain, in delivery order. Each is due now: the world held it for its
    /// link latency, so the page hands it to the process at once.
    pub fn drain_external(&mut self) -> Vec<TimedFrame> {
        std::mem::take(&mut self.external_out)
    }

    /// Route frames an external process sent, as its node, through the link
    /// model at the current time. Frames from a node that is not an external
    /// node this world hosts, or that is down, are dropped.
    pub fn route_external(&mut self, frames: Vec<Frame>) {
        for frame in frames {
            let src = frame.src.node;
            let sends = self.is_hosted(src)
                && self
                    .nodes
                    .get(&src)
                    .is_some_and(|slot| slot.external && slot.alive);
            if sends {
                self.route(src, frame);
            }
        }
    }

    /// Frames that arrived from another peer. They deliver at the current
    /// time, in order; the sender's link model already applied. Data and
    /// closes on a connection this world never saw open are dropped, as a
    /// TCP stack drops segments for a connection it does not have.
    pub fn push_ingress(&mut self, frames: Vec<Frame>) {
        for frame in frames {
            let key = frame.conn_key();
            match frame.payload {
                Payload::Open => {
                    self.conns.insert(key, frame.dst);
                }
                Payload::Close => {
                    if !self.conns.contains_key(&key) {
                        continue;
                    }
                    self.forget_conn(key);
                }
                Payload::Data(_) => {
                    if !self.conns.contains_key(&key) {
                        continue;
                    }
                }
            }
            let now = self.now;
            self.schedule(now, Item::Deliver(frame));
        }
    }

    /// Record the state a remote host reported for a node it runs.
    pub fn apply_remote_snapshot(&mut self, id: NodeId, snapshot: serde_json::Value) {
        if let Some(slot) = self.nodes.get_mut(&id) {
            slot.remote_snapshot = Some(snapshot);
        }
    }
}

/// Whether `spec` is a broker that asks to vote in the controller quorum:
/// its `voter` key, true unless set to false.
fn wants_quorum_vote(spec: &NodeSpec) -> bool {
    spec.kind == "broker"
        && spec
            .config
            .get("voter")
            .and_then(serde_json::Value::as_bool)
            != Some(false)
}

/// A node's spec as it is built, with its place in the controller quorum.
struct QuorumRole {
    /// The spec with the quorum filled in.
    spec: NodeSpec,
    /// The node is the first voter of a world with no quorum yet.
    starts_quorum: bool,
    /// The node asked to vote but joins an existing quorum as an observer.
    observes: bool,
}

/// The unordered pair key of a link.
fn pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a <= b { (a, b) } else { (b, a) }
}

/// A short label for a frame on the wire: the Kafka api name of a request, the
/// first token of an HTTP message, or the payload kind.
fn frame_label(frame: &Frame) -> String {
    match &frame.payload {
        Payload::Open => "open".to_string(),
        Payload::Close => "close".to_string(),
        Payload::Data(bytes) => {
            if frame.dst.port == KAFKA_PORT {
                bytes
                    .get(4..6)
                    .map(|b| i16::from_be_bytes([b[0], b[1]]))
                    .and_then(krabka_protocol::ApiKey::from_i16)
                    .map_or_else(
                        || "request".to_string(),
                        |k| <&'static str>::from(k).to_string(),
                    )
            } else if frame.src.port == KAFKA_PORT {
                "response".to_string()
            } else {
                let line = bytes
                    .split(|&b| b == b'\r' || b == b'\n')
                    .next()
                    .unwrap_or(&[]);
                let text = String::from_utf8_lossy(line);
                let mut words = text.split_whitespace();
                match (words.next(), words.next()) {
                    (Some(m), Some(p)) if !m.starts_with("HTTP/") => {
                        let path: String = p.chars().take(24).collect();
                        format!("{m} {path}")
                    }
                    (Some(_), Some(status)) => format!("HTTP {status}"),
                    _ => "http".to_string(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::lab::testing::TestWorld;

    /// A broker's config for the quorum tests: its id and nothing else.
    fn broker(id: u32) -> NodeSpec {
        NodeSpec::new(id, "broker", "", serde_json::json!({ "broker_id": id }))
    }

    /// Each broker's `(voters, voter)` as its snapshot reports them.
    fn quorum_roles(world: &World, ids: &[u32]) -> Vec<(serde_json::Value, serde_json::Value)> {
        ids.iter()
            .map(|&id| {
                let quorum = &world.node_snapshot(NodeId(id)).unwrap()["quorum"];
                (quorum["voters"].clone(), quorum["voter"].clone())
            })
            .collect()
    }

    #[test]
    fn the_quorum_is_the_scenarios_brokers_and_a_later_broker_observes() {
        let mut scenario = Scenario::empty(7);
        scenario.nodes = vec![broker(1), broker(2), broker(3)];
        let mut world = World::from_scenario(&scenario).unwrap();
        world.add_node(broker(4)).unwrap();

        let voters = serde_json::json!([1, 2, 3]);
        assert!(
            quorum_roles(&world, &[1, 2, 3, 4])
                == vec![
                    (voters.clone(), serde_json::json!(true)),
                    (voters.clone(), serde_json::json!(true)),
                    (voters.clone(), serde_json::json!(true)),
                    (voters.clone(), serde_json::json!(false)),
                ]
        );
        let observed: Vec<(Option<NodeId>, serde_json::Value)> = world
            .events()
            .filter(|e| e.kind == "quorum_observer")
            .map(|e| (e.node, e.detail.clone()))
            .collect();
        assert!(
            observed
                == vec![(
                    Some(NodeId(4)),
                    serde_json::json!({ "level": "info", "voters": [1, 2, 3] })
                )]
        );
        // The scenario keeps what its author wrote.
        let written: Vec<serde_json::Value> = world
            .scenario()
            .nodes
            .iter()
            .map(|n| n.config.clone())
            .collect();
        assert!(written == (1..=4).map(|id| broker(id).config).collect::<Vec<_>>());
    }

    #[test]
    fn the_first_broker_of_an_empty_world_starts_the_quorum() {
        let mut world = World::from_scenario(&Scenario::empty(7)).unwrap();
        world.add_node(broker(5)).unwrap();
        world.add_node(broker(6)).unwrap();

        let voters = serde_json::json!([5]);
        assert!(
            quorum_roles(&world, &[5, 6])
                == vec![
                    (voters.clone(), serde_json::json!(true)),
                    (voters, serde_json::json!(false)),
                ]
        );
    }

    #[test]
    fn a_voter_of_the_loaded_quorum_stays_one() {
        let mut scenario = Scenario::empty(7);
        scenario.nodes = vec![broker(1), broker(2), broker(3)];
        let mut world = World::from_scenario(&scenario).unwrap();

        let observer = NodeSpec::new(
            2,
            "broker",
            "",
            serde_json::json!({ "broker_id": 2, "voter": false }),
        );
        let refused = world.update_node(NodeId(2), observer);
        assert!(let Err(LabError::Config { .. }) = refused);
        assert!(
            quorum_roles(&world, &[2])
                == vec![(serde_json::json!([1, 2, 3]), serde_json::json!(true))]
        );
        // A broker that names its own quorum keeps it.
        let named = NodeSpec::new(
            7,
            "broker",
            "",
            serde_json::json!({ "broker_id": 7, "controller_quorum_voters": [7] }),
        );
        world.add_node(named).unwrap();
        assert!(
            quorum_roles(&world, &[7]) == vec![(serde_json::json!([7]), serde_json::json!(true))]
        );
    }

    /// Two echo nodes and one ticker; the ticker pings the echo every 100 ms.
    fn scenario() -> Scenario {
        serde_json::from_value(serde_json::json!({
            "version": 1, "seed": 7, "links": { "default_latency_ms": 10 },
            "nodes": [
                { "id": 1, "kind": "echo", "name": "echo-a" },
                { "id": 2, "kind": "echo", "name": "echo-b" },
                { "id": 3, "kind": "pinger", "config": { "target": 1, "period_ms": 100 } }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn frames_arrive_after_the_link_latency_in_fifo_order() {
        let mut w = TestWorld::from_scenario(&scenario());
        // The pinger opens at t=0 and pings at t=100, 200, ...; each ping is
        // echoed back 10 ms + 10 ms later.
        w.run_for(105);
        // The open arrived at 10; the ping sent at 100 is still on the wire.
        assert!(w.world().delivered_between(NodeId(3), NodeId(1)) == 1);
        assert!(w.world().delivered_between(NodeId(1), NodeId(3)) == 0);
        w.run_for(20);
        assert!(w.world().delivered_between(NodeId(1), NodeId(3)) == 1);
        assert!(w.snapshot(NodeId(3))["echoes"] == 1);
        assert!(w.snapshot(NodeId(1))["frames"] == 2);
        assert!(w.world().now() == 125);
    }

    #[test]
    fn step_once_advances_the_clock_when_idle() {
        let mut w = TestWorld::from_scenario(&scenario());
        let mut world = w.take();
        assert!(world.step_until(50) >= 1); // the open frame and the start timer
        assert!(world.now() == 50);
        assert!(!world.step_once(60));
        assert!(world.now() == 60);
    }

    #[test]
    fn partition_drops_in_flight_frames_and_closes_connections() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(101); // a ping is on the wire toward node 1
        assert!(w.world().snapshot().in_flight.len() == 1);
        w.world_mut().fault(Fault::Partition {
            a: NodeId(1),
            b: NodeId(3),
        });
        // The ping was purged; both sides get a synthesized close.
        assert!(
            w.world()
                .snapshot()
                .in_flight
                .iter()
                .all(|f| f.label == "close")
        );
        w.run_for(50);
        assert!(w.snapshot(NodeId(3))["closes"] == 1);
        assert!(w.snapshot(NodeId(1))["closes"] == 1);
        assert!(w.snapshot(NodeId(3))["echoes"] == 0);
        // Healing lets the pinger reconnect (it reopens on the next ping).
        w.world_mut().fault(Fault::Heal {
            a: NodeId(1),
            b: NodeId(3),
        });
        w.run_for(300);
        assert!(w.snapshot(NodeId(3))["echoes"].as_u64().unwrap() >= 1);
        assert!(w.world().events().any(|e| e.kind == "fault"));
    }

    #[test]
    fn kill_stops_delivery_and_restart_keeps_durable_state() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(250);
        let before = w.snapshot(NodeId(1))["frames"].as_u64().unwrap();
        assert!(before >= 3);
        w.world_mut().fault(Fault::Kill { node: NodeId(1) });
        w.run_for(300);
        assert!(w.snapshot(NodeId(1))["frames"] == before);
        assert!(w.snapshot(NodeId(1))["started"] == 1);
        w.world_mut().fault(Fault::Restart { node: NodeId(1) });
        w.run_for(300);
        assert!(w.snapshot(NodeId(1))["started"] == 2);
        assert!(w.snapshot(NodeId(1))["frames"].as_u64().unwrap() > before);
        w.world_mut().fault(Fault::Wipe { node: NodeId(1) });
        assert!(w.snapshot(NodeId(1))["frames"] == 0);
        assert!(w.snapshot(NodeId(1))["started"] == 1);
    }

    #[test]
    fn kill_loses_the_killed_nodes_unsent_frames() {
        let mut w = TestWorld::from_scenario(&scenario());
        // The ping arrives at 110 and the echo is on the wire until 120.
        w.run_for(111);
        w.world_mut().fault(Fault::Kill { node: NodeId(1) });
        w.run_for(100);
        assert!(w.snapshot(NodeId(3))["echoes"] == 0);
        assert!(w.snapshot(NodeId(3))["closes"] == 1);
    }

    #[test]
    fn a_killed_node_refuses_new_connections_at_once() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(50);
        // The kill resets the open connection: its close reaches the pinger
        // at 60.
        w.world_mut().fault(Fault::Kill { node: NodeId(1) });
        w.run_for(15);
        assert!(w.snapshot(NodeId(3))["closes"] == 1);
        // At 100 the pinger opens a new connection; it reaches the dead node
        // at 110 and the refusal travels back over the same 10 ms link.
        w.run_for(50);
        assert!(w.snapshot(NodeId(3))["closes"] == 1);
        w.run_for(10);
        assert!(w.snapshot(NodeId(3))["closes"] == 2);
        assert!(w.snapshot(NodeId(3))["open"] == false);
    }

    #[test]
    fn a_node_that_moves_away_takes_no_stale_frames_with_it() {
        let mut w = TestWorld::from_scenario(&scenario());
        // The ping sent at 100 is on the wire to the echo until 110.
        w.run_for(105);
        let world = w.world_mut();
        world.set_hosted(&[NodeId(2), NodeId(3)]);
        world.step_until(150);
        let egress = world.drain_egress();
        assert!(egress.is_empty(), "{egress:?}");
    }

    #[test]
    fn ingress_on_a_connection_never_opened_is_dropped() {
        let mut world = World::from_scenario_hosted(&scenario(), &[NodeId(1)]).unwrap();
        let client = Endpoint::client(NodeId(3));
        let server = Endpoint::kafka(NodeId(1));
        world.push_ingress(vec![
            Frame::data(client, server, ConnId(7), Bytes::from_static(b"stray")),
            Frame::close(client, server, ConnId(7)),
        ]);
        world.step_until(1);
        assert!(world.node_snapshot(NodeId(1)).unwrap()["frames"] == 0);
        world.push_ingress(vec![
            Frame::open(client, server, ConnId(8)),
            Frame::data(client, server, ConnId(8), Bytes::from_static(b"hello")),
        ]);
        world.step_until(2);
        assert!(world.node_snapshot(NodeId(1)).unwrap()["frames"] == 2);
    }

    /// A scenario with an external node 1 (a real broker the page runs) and a
    /// pinger that pings it.
    fn external_scenario() -> Scenario {
        serde_json::from_value(serde_json::json!({
            "version": 1, "seed": 7, "links": { "default_latency_ms": 10 },
            "nodes": [
                { "id": 1, "kind": "krabka-broker" },
                { "id": 3, "kind": "pinger", "config": { "target": 1, "period_ms": 100 } }
            ]
        }))
        .unwrap()
    }

    /// Play the page's part for external node 1: take what is due for it and
    /// echo every data frame back through the link model, as the real process
    /// behind it would answer on the same connection.
    fn echo_externally(world: &mut World) -> Vec<TimedFrame> {
        let due = world.drain_external();
        let replies = due
            .iter()
            .filter_map(|t| {
                let bytes = t.frame.payload.data()?;
                Some(Frame::data(
                    t.frame.dst,
                    t.frame.src,
                    t.frame.conn,
                    bytes.clone(),
                ))
            })
            .collect();
        world.route_external(replies);
        due
    }

    #[test]
    fn an_external_node_gets_its_frames_when_due_and_answers_through_the_links() {
        let mut world = World::from_scenario(&external_scenario()).unwrap();
        let mut seen = Vec::new();
        for until in (5..=320).step_by(5) {
            world.step_until(until);
            for timed in echo_externally(&mut world) {
                assert!(timed.deliver_at == until, "{timed:?}");
                seen.push((timed.deliver_at, timed.frame.payload.clone()));
            }
        }
        // The open and each ping arrive after the 10 ms link latency.
        assert!(seen[0] == (10, Payload::Open));
        assert!(seen[1].0 == 110);
        let pinger = world.node_snapshot(NodeId(3)).unwrap();
        assert!(pinger["echoes"] == 3);
        assert!(pinger["mean_rtt_ms"] == 20);
        assert!(world.node_snapshot(NodeId(1)).unwrap()["external"] == true);
    }

    #[test]
    fn a_killed_external_node_refuses_and_sends_nothing() {
        let mut world = World::from_scenario(&external_scenario()).unwrap();
        world.step_until(50);
        echo_externally(&mut world);
        world.fault(Fault::Kill { node: NodeId(1) });
        world.step_until(400);
        assert!(echo_externally(&mut world).is_empty());
        // Its process's late words are dropped too.
        world.route_external(vec![Frame::data(
            Endpoint::kafka(NodeId(1)),
            Endpoint::client(NodeId(3)),
            ConnId(1),
            Bytes::from_static(b"late"),
        )]);
        world.step_until(500);
        let pinger = world.node_snapshot(NodeId(3)).unwrap();
        assert!(pinger["echoes"] == 0);
        assert!(pinger["closes"].as_u64().unwrap() >= 2);
    }

    #[test]
    fn scenario_topics_on_real_brokers_go_through_an_admin_bootstrapped_at_them() {
        // Every broker of this scenario is a real one the page runs. Its
        // topics still get the world's admin node, the next free id, which
        // opens its first connection to one of those brokers.
        let scenario: Scenario = serde_json::from_value(serde_json::json!({
            "version": 1, "seed": 7, "links": { "default_latency_ms": 10 },
            "nodes": [
                { "id": 1, "kind": "krabka-broker" },
                { "id": 2, "kind": "krabka-broker" }
            ],
            "topics": [{ "name": "orders", "partitions": 3, "replication_factor": 2 }]
        }))
        .unwrap();
        let mut world = World::from_scenario(&scenario).unwrap();
        world.step_until(50);
        let opened: Vec<(Endpoint, Endpoint)> = world
            .drain_external()
            .into_iter()
            .filter(|timed| timed.frame.payload == Payload::Open)
            .map(|timed| (timed.frame.src, timed.frame.dst))
            .collect();
        let admin = Endpoint::client(NodeId(3));
        assert!(
            opened == [(admin, Endpoint::kafka(NodeId(1)))]
                || opened == [(admin, Endpoint::kafka(NodeId(2)))],
            "{opened:?}"
        );
    }

    #[test]
    fn only_external_nodes_route_external_frames() {
        let mut world = World::from_scenario(&scenario()).unwrap();
        world.step_until(5);
        // Node 3 is an ordinary pinger: the page cannot speak for it.
        world.route_external(vec![Frame::open(
            Endpoint::client(NodeId(3)),
            Endpoint::kafka(NodeId(2)),
            ConnId(99),
        )]);
        world.step_until(50);
        assert!(world.node_snapshot(NodeId(2)).unwrap()["frames"] == 0);
    }

    #[test]
    fn virtual_addresses_name_nodes_both_ways() {
        for (node, ip) in [(1, "10.0.0.1"), (254, "10.0.0.254"), (258, "10.0.1.2")] {
            let ip: std::net::Ipv4Addr = ip.parse().unwrap();
            assert!(crate::lab::net::node_ip(NodeId(node)) == ip);
            assert!(crate::lab::net::node_for_ip(ip) == Some(NodeId(node)));
        }
        for ip in ["10.0.0.0", "10.1.0.1", "192.168.0.1"] {
            assert!(
                crate::lab::net::node_for_ip(ip.parse().unwrap()).is_none(),
                "{ip}"
            );
        }
    }

    #[test]
    fn faults_purge_frames_waiting_for_another_host() {
        let mut world = World::from_scenario_hosted(&scenario(), &[NodeId(3)]).unwrap();
        world.step_until(100); // open + ping wait in egress
        world.fault(Fault::Partition {
            a: NodeId(1),
            b: NodeId(3),
        });
        let egress = world.drain_egress();
        assert!(egress.iter().all(|t| t.frame.payload == Payload::Close));
    }

    #[test]
    fn a_closed_connection_leaves_no_delivery_floor_behind() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(101);
        assert!(w.world().delivery_floors() == 1); // the pinger's connection, client to server
        w.world_mut().fault(Fault::Partition {
            a: NodeId(1),
            b: NodeId(3),
        });
        assert!(w.world().delivery_floors() == 0);
        w.world_mut().fault(Fault::Heal {
            a: NodeId(1),
            b: NodeId(3),
        });
        w.run_for(200); // the pinger reopens and its echoes flow again
        assert!(w.world().delivery_floors() == 2);
    }

    #[test]
    fn a_close_never_overtakes_the_connections_earlier_frames() {
        // A node that closes right after sending data, on a link that just got
        // faster, must still deliver the data first.
        let scenario: Scenario = serde_json::from_value(serde_json::json!({
            "version": 1, "links": { "default_latency_ms": 100 },
            "nodes": [
                { "id": 1, "kind": "echo" },
                { "id": 3, "kind": "pinger", "config": { "target": 1, "period_ms": 1000 } }
            ]
        }))
        .unwrap();
        let mut world = World::from_scenario(&scenario).unwrap();
        world.step_until(50); // the open is on the wire until 100
        world.fault(Fault::Latency {
            a: NodeId(1),
            b: NodeId(3),
            ms: 1,
        });
        world.push_ingress(vec![Frame::close(
            Endpoint::client(NodeId(3)),
            Endpoint::kafka(NodeId(1)),
            ConnId(1),
        )]);
        // Ingress frames deliver at once: this one is a peer's close arriving
        // on the wire, so it must still queue behind the open.
        world.step_until(200);
        let snap = world.node_snapshot(NodeId(1)).unwrap();
        assert!(snap["frames"] == 2);
        assert!(snap["closes"] == 1);
    }

    #[test]
    fn isolation_closes_the_isolated_nodes_own_connections() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(50);
        w.world_mut().fault(Fault::Isolate { node: NodeId(3) });
        // The close reaches the isolated node itself after one link latency.
        w.run_for(10);
        assert!(w.snapshot(NodeId(3))["closes"] == 1);
        assert!(w.snapshot(NodeId(3))["open"] == false);
        w.world_mut().fault(Fault::Reconnect { node: NodeId(3) });
        w.run_for(300);
        assert!(w.snapshot(NodeId(3))["echoes"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn link_overrides_apply_before_the_nodes_start() {
        let mut s = scenario();
        s.link_overrides.push(LinkOverride {
            a: NodeId(1),
            b: NodeId(3),
            latency_ms: None,
            loss_permille: None,
            cut: true,
        });
        let mut w = TestWorld::from_scenario(&s);
        w.run_for(250);
        assert!(w.snapshot(NodeId(1))["frames"] == 0);
    }

    #[test]
    fn a_takeover_keeps_a_killed_node_down() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(50);
        let world = w.world_mut();
        world.fault(Fault::Kill { node: NodeId(1) });
        world.set_hosted(&[NodeId(3)]);
        world.set_hosted(&[NodeId(1), NodeId(3)]);
        world.step_until(400);
        let snap = world.snapshot();
        let n1 = snap.nodes.iter().find(|n| n.id == NodeId(1)).unwrap();
        assert!(!n1.alive);
        assert!(n1.state["started"] == 1);
        world.fault(Fault::Restart { node: NodeId(1) });
        assert!(world.node_snapshot(NodeId(1)).unwrap()["started"] == 2);
    }

    #[test]
    fn loss_drops_data_frames_deterministically() {
        let mut a = TestWorld::from_scenario(&scenario());
        a.world_mut().fault(Fault::Loss {
            a: NodeId(1),
            b: NodeId(3),
            permille: 500,
        });
        a.run_for(5_000);
        let echoes_a = a.snapshot(NodeId(3))["echoes"].as_u64().unwrap();
        assert!(echoes_a > 5 && echoes_a < 45);
        let mut b = TestWorld::from_scenario(&scenario());
        b.world_mut().fault(Fault::Loss {
            a: NodeId(1),
            b: NodeId(3),
            permille: 500,
        });
        b.run_for(5_000);
        assert!(b.snapshot(NodeId(3))["echoes"] == echoes_a);
        assert!(a.world().events().any(|e| e.kind == "frame_lost"));
    }

    #[test]
    fn latency_change_never_reorders_a_connection() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(101);
        // A frame is in flight at 110; dropping the latency to 0 must not let a
        // later frame on the same connection arrive before it.
        w.world_mut().fault(Fault::Latency {
            a: NodeId(1),
            b: NodeId(3),
            ms: 0,
        });
        w.world_mut()
            .control(NodeId(3), serde_json::json!({ "cmd": "ping" }))
            .unwrap();
        w.run_for(30);
        assert!(w.snapshot(NodeId(1))["last_seq"] == 2);
        assert!(w.snapshot(NodeId(3))["echoes"] == 2);
    }

    #[test]
    fn partial_world_routes_frames_for_remote_nodes_to_egress() {
        let mut world = World::from_scenario_hosted(&scenario(), &[NodeId(3)]).unwrap();
        world.step_until(100);
        let egress = world.drain_egress();
        assert!(egress.len() == 2); // open + ping, queued the moment they were sent
        assert!(egress.iter().all(|t| t.frame.dst.node == NodeId(1)));
        assert!(egress[0].deliver_at == 10);
        assert!(egress[1].deliver_at == 110);
        // Feed them into a second world that hosts node 1 only.
        let mut other = World::from_scenario_hosted(&scenario(), &[NodeId(1)]).unwrap();
        other.push_ingress(egress.into_iter().map(|t| t.frame).collect());
        other.step_until(5);
        let back = other.drain_egress();
        assert!(back.len() == 1);
        assert!(back[0].frame.dst.node == NodeId(3));
        assert!(back[0].frame.payload.data() == Some(&Bytes::from_static(b"ping 1")));
        other.apply_remote_snapshot(NodeId(3), serde_json::json!({"echoes": 9}));
        let snap = other.snapshot();
        let n3 = snap.nodes.iter().find(|n| n.id == NodeId(3)).unwrap();
        assert!(!n3.hosted);
        assert!(n3.state["echoes"] == 9);
        assert!(
            snap.nodes
                .iter()
                .find(|n| n.id == NodeId(1))
                .unwrap()
                .hosted
        );
    }

    #[test]
    fn unhosting_a_node_closes_the_connections_its_peers_hold() {
        let mut w = TestWorld::from_scenario(&scenario());
        w.run_for(50); // the pinger's connection to node 1 is open
        let world = w.world_mut();
        world.set_hosted(&[NodeId(3)]);
        world.step_until(100);
        // Node 1 now lives elsewhere: the pinger saw a close and reopened.
        assert!(world.node_snapshot(NodeId(3)).unwrap()["closes"] == 1);
        let egress = world.drain_egress();
        let opens = egress
            .iter()
            .filter(|t| t.frame.payload == Payload::Open)
            .count();
        assert!(opens == 1);
        assert!(!world.hosted().contains(&NodeId(1)));
    }

    #[test]
    fn scenario_echoes_positions_overrides_and_hides_the_admin() {
        let mut s = scenario();
        s.nodes[0].x = 5.0;
        s.link_overrides.push(LinkOverride {
            a: NodeId(1),
            b: NodeId(2),
            latency_ms: Some(50),
            loss_permille: None,
            cut: false,
        });
        let mut world = World::from_scenario(&s).unwrap();
        world.set_position(NodeId(2), 7.0, 8.0);
        let back = world.scenario();
        assert!(back.nodes[0] == s.nodes[0]);
        assert!((back.nodes[1].x - 7.0).abs() < f64::EPSILON);
        assert!((back.nodes[1].y - 8.0).abs() < f64::EPSILON);
        assert!(back.link_overrides == s.link_overrides);
        assert!(back.nodes.iter().all(|n| n.kind != "admin"));
        assert!(back.nodes[2].name == "pinger-3");
    }

    #[test]
    fn unknown_kinds_and_duplicate_ids_are_rejected() {
        let s: Scenario = serde_json::from_value(serde_json::json!({
            "version": 1, "nodes": [{ "id": 1, "kind": "toaster" }]
        }))
        .unwrap();
        assert!(matches!(
            World::from_scenario(&s),
            Err(LabError::UnknownNodeKind(_))
        ));
        let s: Scenario = serde_json::from_value(serde_json::json!({
            "version": 1, "nodes": [{ "id": 1, "kind": "echo" }, { "id": 1, "kind": "echo" }]
        }))
        .unwrap();
        assert!(matches!(
            World::from_scenario(&s),
            Err(LabError::InvalidScenario(_))
        ));
        let s: Scenario = serde_json::from_value(serde_json::json!({
            "version": 1, "nodes": [{ "id": 0, "kind": "echo" }]
        }))
        .unwrap();
        assert!(matches!(
            World::from_scenario(&s),
            Err(LabError::InvalidScenario(_))
        ));
    }

    #[test]
    fn frame_labels_name_kafka_requests_and_http_methods() {
        let kafka = Frame::data(
            Endpoint::client(NodeId(1)),
            Endpoint::kafka(NodeId(2)),
            ConnId(0),
            Bytes::from_static(&[0, 0, 0, 8, 0, 18, 0, 4, 0, 0, 0, 1]),
        );
        assert!(frame_label(&kafka) == "ApiVersions");
        assert!(frame_label(&kafka.reply(Payload::Data(Bytes::new()))) == "response");
        let http = Frame::data(
            Endpoint::client(NodeId(1)),
            Endpoint::http(NodeId(2)),
            ConnId(0),
            Bytes::from_static(b"POST /subjects/orders-value/versions HTTP/1.1\r\n\r\n"),
        );
        assert!(frame_label(&http) == "POST /subjects/orders-value/v");
        let resp = http.reply(Payload::Data(Bytes::from_static(
            b"HTTP/1.1 200 OK\r\n\r\n",
        )));
        assert!(frame_label(&resp) == "HTTP 200");
    }
}
