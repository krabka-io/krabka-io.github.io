//! A partition rebalancer: a node that reads the cluster's layout through
//! `Metadata`, proposes reassignments that even out the replicas and the
//! leaders per broker, and carries them out with
//! `AlterPartitionReassignments` and then `ElectLeaders` (preferred), as an
//! operator does with `kafka-reassign-partitions` and
//! `kafka-leader-election`.
//!
//! The real `krabka-rebalancer` crate is not linked: it runs on tokio's
//! multi-threaded runtime and serves its API with axum and reqwest, none of
//! which build for `wasm32-unknown-unknown`. This node plans with the same
//! two goals over the same requests, small enough to read in one sitting.
//!
//! Config: `{ "bootstrap": [node ids], "goals": ["replica_count", "leader_count"],
//! "interval_ms": 10000, "execute": true }`. Every `interval_ms` the node
//! sends `Metadata` and `ListPartitionReassignments`, and plans over the
//! topics that are not internal:
//!
//! - `replica_count`: while the broker with the most replicas holds two more
//!   than the one with the fewest, move one replica of a partition from the
//!   first to the second (a partition that has none on the second).
//! - `leader_count`: the same with preferred leaders, the first replica of
//!   each list: put the broker with the fewest first in a partition where the
//!   broker with the most is first.
//!
//! With `execute` the node sends the proposals in one
//! `AlterPartitionReassignments`, unless a reassignment is still in
//! progress. Once nothing moves, it asks for a preferred election of every
//! partition whose leader is not its first replica, so leadership follows the
//! new lists.
//!
//! Commands: `{"cmd":"plan"}` plans now without executing, `{"cmd":"execute"}`
//! plans and executes now, `{"cmd":"pause"}` and `{"cmd":"resume"}` stop and
//! start the periodic runs.
//!
//! Snapshot: `{"proposals":[{"topic","partition","from","to","reason"}],"executed":n,
//! "balance":{"<broker id>":{"replicas","leaders"}},"state","paused","execute",
//! "goals","last_plan_at","reassigning","errors","client":{..}}`.
//! Events: `rebalance_planned`, `rebalance_executed`, `leaders_elected`,
//! `rebalance_error`.

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    elect_leaders_response::ElectLeadersResponse,
    list_partition_reassignments_request::ListPartitionReassignmentsRequest,
    list_partition_reassignments_response::ListPartitionReassignmentsResponse,
    metadata_request::MetadataRequest, metadata_response::MetadataResponse,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::admin::{elect_leaders, election_errors, reassignment, reassignment_errors};
use crate::lab::{
    LabError,
    client::{ClientError, ClientEvent, ClientOptions, KafkaClient, RequestId, Response, Target},
    codes, config_field, config_field_or,
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::NodeSpec,
};

/// The default wait between runs.
const INTERVAL_MS: Millis = 10_000;
/// The `timeout_ms` of the list request.
const LIST_TIMEOUT_MS: i32 = 30_000;

/// What the rebalancer evens out.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Goal {
    ReplicaCount,
    LeaderCount,
}

impl Goal {
    const fn name(self) -> &'static str {
        match self {
            Self::ReplicaCount => "replica_count",
            Self::LeaderCount => "leader_count",
        }
    }
}

/// One partition of the layout the plan starts from.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Placement {
    pub topic: String,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

/// A proposed move.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Proposal {
    pub topic: String,
    pub partition: i32,
    pub from: Vec<i32>,
    pub to: Vec<i32>,
    pub reason: String,
}

/// The broker of `counts` with the most, and the one with the fewest; the
/// lower id wins a tie.
fn extremes(counts: &BTreeMap<i32, usize>) -> Option<((i32, usize), (i32, usize))> {
    let most = counts
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then(b.0.cmp(a.0)))?;
    let fewest = counts
        .iter()
        .min_by(|a, b| a.1.cmp(b.1).then(a.0.cmp(b.0)))?;
    Some(((*most.0, *most.1), (*fewest.0, *fewest.1)))
}

/// Count, per broker of `brokers`, what `pick` takes from each list.
fn count<'a>(
    brokers: &[i32],
    lists: impl Iterator<Item = &'a Vec<i32>>,
    pick: impl Fn(&'a Vec<i32>) -> Vec<i32>,
) -> BTreeMap<i32, usize> {
    let mut counts: BTreeMap<i32, usize> = brokers.iter().map(|b| (*b, 0)).collect();
    for list in lists {
        for b in pick(list) {
            if let Some(n) = counts.get_mut(&b) {
                *n += 1;
            }
        }
    }
    counts
}

/// The moves that even out `layout` over `brokers` for `goals`. See the
/// module documentation. Deterministic: partitions in layout order, ties to
/// the lower broker id.
#[must_use]
pub fn plan(layout: &[Placement], brokers: &[i32], goals: &BTreeSet<Goal>) -> Vec<Proposal> {
    let mut lists: Vec<Vec<i32>> = layout.iter().map(|p| p.replicas.clone()).collect();
    let mut reasons: Vec<BTreeSet<Goal>> = vec![BTreeSet::new(); layout.len()];
    // Each step moves one replica toward the mean, so this bounds the loop.
    let steps = lists.iter().map(Vec::len).sum::<usize>() + 1;
    if goals.contains(&Goal::ReplicaCount) {
        for _ in 0..steps {
            let counts = count(brokers, lists.iter(), Clone::clone);
            let Some(((most, high), (fewest, low))) = extremes(&counts) else {
                break;
            };
            if high <= low + 1 {
                break;
            }
            let Some(i) = lists
                .iter()
                .position(|l| l.contains(&most) && !l.contains(&fewest))
            else {
                break;
            };
            for r in &mut lists[i] {
                if *r == most {
                    *r = fewest;
                }
            }
            reasons[i].insert(Goal::ReplicaCount);
        }
    }
    if goals.contains(&Goal::LeaderCount) {
        for _ in 0..steps {
            let counts = count(brokers, lists.iter(), |l| {
                l.first().copied().into_iter().collect()
            });
            let Some(((most, high), (fewest, low))) = extremes(&counts) else {
                break;
            };
            if high <= low + 1 {
                break;
            }
            let Some(i) = lists
                .iter()
                .position(|l| l.first() == Some(&most) && l.contains(&fewest))
            else {
                break;
            };
            if let Some(at) = lists[i].iter().position(|r| *r == fewest) {
                lists[i].swap(0, at);
            }
            reasons[i].insert(Goal::LeaderCount);
        }
    }
    layout
        .iter()
        .zip(lists)
        .zip(reasons)
        .filter(|((p, to), _)| p.replicas != *to)
        .map(|((p, to), reasons)| Proposal {
            topic: p.topic.clone(),
            partition: p.partition,
            from: p.replicas.clone(),
            to,
            reason: reasons
                .iter()
                .map(|g| g.name())
                .collect::<Vec<_>>()
                .join(", "),
        })
        .collect()
}

/// What a request of the rebalancer is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    Metadata,
    Ongoing,
    Alter(usize),
    Elect,
}

/// The rebalancer node. See the module documentation.
pub struct RebalancerNode {
    id: NodeId,
    bootstrap: Vec<NodeId>,
    goals: BTreeSet<Goal>,
    interval: Millis,
    execute: bool,
    client: KafkaClient,
    paused: bool,
    next_at: Millis,
    pending: BTreeMap<RequestId, Step>,
    /// This run executes what it plans.
    run_executes: bool,
    layout: Option<(Vec<i32>, Vec<Placement>)>,
    reassigning: Option<usize>,
    proposals: Vec<Proposal>,
    executed: u64,
    balance: BTreeMap<i32, (usize, usize)>,
    state: String,
    last_plan_at: Option<Millis>,
    errors: Vec<String>,
}

impl RebalancerNode {
    /// # Errors
    /// Returns a config error when `bootstrap` is missing or empty, a goal is
    /// unknown, or a field is unknown.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bootstrap: Vec<NodeId> = config_field(spec, "bootstrap")?;
        if bootstrap.is_empty() {
            return Err(LabError::config(
                spec,
                "`bootstrap` needs at least one broker",
            ));
        }
        let goals: BTreeSet<Goal> = config_field_or(
            spec,
            "goals",
            BTreeSet::from([Goal::ReplicaCount, Goal::LeaderCount]),
        )?;
        let interval: Millis = config_field_or(spec, "interval_ms", INTERVAL_MS)?;
        if interval == 0 {
            return Err(LabError::config(spec, "`interval_ms` must be positive"));
        }
        let execute: bool = config_field_or(spec, "execute", true)?;
        for key in spec
            .config
            .as_object()
            .map(|o| o.keys())
            .into_iter()
            .flatten()
        {
            if !matches!(
                key.as_str(),
                "bootstrap" | "goals" | "interval_ms" | "execute"
            ) {
                return Err(LabError::config(
                    spec,
                    format!("unknown config field `{key}`"),
                ));
            }
        }
        Ok(Self {
            id: spec.id,
            client: build_client(&bootstrap, spec.id),
            bootstrap,
            goals,
            interval,
            execute,
            paused: false,
            next_at: 0,
            pending: BTreeMap::new(),
            run_executes: execute,
            layout: None,
            reassigning: None,
            proposals: Vec::new(),
            executed: 0,
            balance: BTreeMap::new(),
            state: "starting".to_string(),
            last_plan_at: None,
            errors: Vec::new(),
        })
    }

    fn drive(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            if let ClientEvent::Response { id, result } = event {
                self.on_response(ctx, id, result);
            }
        }
        let now = ctx.now();
        if self.pending.is_empty() && !self.paused && now >= self.next_at {
            self.start_run(ctx, self.execute);
        }
        let run = (self.pending.is_empty() && !self.paused).then_some(self.next_at);
        if let Some(at) = self.client.next_deadline(now).into_iter().chain(run).min() {
            ctx.arm(at.max(now));
        }
    }

    fn start_run(&mut self, ctx: &mut Ctx<'_>, execute: bool) {
        self.next_at = ctx.now() + self.interval;
        self.run_executes = execute;
        self.layout = None;
        self.reassigning = None;
        self.errors.clear();
        self.state = "reading the cluster".to_string();
        let id = self.client.send(
            ctx,
            Target::Any,
            MetadataRequest {
                topics: None,
                allow_auto_topic_creation: false,
                ..Default::default()
            },
        );
        self.pending.insert(id, Step::Metadata);
        let id = self.client.send(
            ctx,
            Target::Controller,
            ListPartitionReassignmentsRequest {
                timeout_ms: LIST_TIMEOUT_MS,
                topics: None,
                ..Default::default()
            },
        );
        self.pending.insert(id, Step::Ongoing);
    }

    fn fail(&mut self, ctx: &mut Ctx<'_>, what: &str, problem: &str) {
        let text = format!("{what}: {problem}");
        ctx.event(
            "rebalance_error",
            json!({ "message": text, "level": "warn" }),
        );
        self.errors.push(text);
        self.state = "error".to_string();
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<Response, ClientError>,
    ) {
        let Some(step) = self.pending.remove(&id) else {
            return;
        };
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                // A run whose reads failed plans nothing.
                self.pending.clear();
                return self.fail(ctx, step_name(step), &error.to_string());
            }
        };
        match step {
            Step::Metadata => match response.downcast::<MetadataResponse>() {
                Some(r) => self.layout = Some(layout_of(&r)),
                None => return self.fail(ctx, "Metadata", "unexpected answer"),
            },
            Step::Ongoing => match response.downcast::<ListPartitionReassignmentsResponse>() {
                Some(r) if r.error_code == codes::NONE => {
                    self.reassigning = Some(r.topics.iter().map(|t| t.partitions.len()).sum());
                }
                Some(r) => {
                    // A broker that cannot list reassignments: plan anyway.
                    self.reassigning = Some(0);
                    self.errors.push(format!(
                        "ListPartitionReassignments: error {}",
                        r.error_code
                    ));
                }
                None => self.reassigning = Some(0),
            },
            Step::Alter(n) => {
                let outcome = response
                    .downcast::<AlterPartitionReassignmentsResponse>()
                    .map(reassignment_errors)
                    .and_then(|rows| rows.into_iter().find(|(code, _)| *code != codes::NONE));
                match outcome {
                    None => {
                        self.executed += n as u64;
                        self.state = format!("reassigning {n} partition(s)");
                        ctx.event("rebalance_executed", json!({ "partitions": n }));
                    }
                    Some((code, message)) => {
                        self.fail(
                            ctx,
                            "AlterPartitionReassignments",
                            &format!("error {code} {}", message.unwrap_or_default()),
                        );
                    }
                }
                return;
            }
            Step::Elect => {
                let failed = response
                    .downcast::<ElectLeadersResponse>()
                    .map(election_errors)
                    .and_then(|rows| rows.into_iter().find(|(code, _)| *code != codes::NONE));
                match failed {
                    None => {
                        self.state = "balanced".to_string();
                        ctx.event("leaders_elected", json!({}));
                    }
                    Some((code, message)) => self.fail(
                        ctx,
                        "ElectLeaders",
                        &format!("error {code} {}", message.unwrap_or_default()),
                    ),
                }
                return;
            }
        }
        if let (Some((brokers, layout)), Some(reassigning)) =
            (self.layout.clone(), self.reassigning)
        {
            self.on_layout(ctx, &brokers, &layout, reassigning);
        }
    }

    /// Both reads are in: plan, and execute when this run does.
    fn on_layout(
        &mut self,
        ctx: &mut Ctx<'_>,
        brokers: &[i32],
        layout: &[Placement],
        reassigning: usize,
    ) {
        let now = ctx.now();
        self.balance = brokers.iter().map(|b| (*b, (0, 0))).collect();
        for p in layout {
            for r in &p.replicas {
                if let Some(entry) = self.balance.get_mut(r) {
                    entry.0 += 1;
                }
            }
            if let Some(entry) = self.balance.get_mut(&p.leader) {
                entry.1 += 1;
            }
        }
        self.proposals = plan(layout, brokers, &self.goals);
        self.last_plan_at = Some(now);
        ctx.event(
            "rebalance_planned",
            json!({ "proposals": self.proposals.len(), "reassigning": reassigning }),
        );
        if !self.run_executes {
            self.state = format!("planned {} move(s)", self.proposals.len());
            return;
        }
        if reassigning > 0 {
            self.state = format!("waiting for {reassigning} reassignment(s)");
            return;
        }
        if !self.proposals.is_empty() {
            let rows: Vec<(String, i32, Option<Vec<i32>>)> = self
                .proposals
                .iter()
                .map(|p| (p.topic.clone(), p.partition, Some(p.to.clone())))
                .collect();
            let id = self
                .client
                .send(ctx, Target::Controller, reassignment(&rows));
            self.pending.insert(id, Step::Alter(rows.len()));
            self.state = "executing".to_string();
            return;
        }
        // Nothing to move: let leadership follow the replica lists.
        let mut elect: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for p in layout {
            if p.replicas
                .first()
                .is_some_and(|first| *first != p.leader && p.isr.contains(first))
            {
                elect.entry(p.topic.clone()).or_default().push(p.partition);
            }
        }
        if elect.is_empty() {
            self.state = "balanced".to_string();
            return;
        }
        let rows: Vec<(String, Vec<i32>)> = elect.into_iter().collect();
        let id = self
            .client
            .send(ctx, Target::Controller, elect_leaders(0, &rows));
        self.pending.insert(id, Step::Elect);
        self.state = "electing preferred leaders".to_string();
    }
}

fn build_client(bootstrap: &[NodeId], id: NodeId) -> KafkaClient {
    KafkaClient::new(
        bootstrap.iter().map(|n| Endpoint::kafka(*n)).collect(),
        &format!("rebalancer-{id}"),
        ClientOptions::default(),
    )
}

const fn step_name(step: Step) -> &'static str {
    match step {
        Step::Metadata => "Metadata",
        Step::Ongoing => "ListPartitionReassignments",
        Step::Alter(_) => "AlterPartitionReassignments",
        Step::Elect => "ElectLeaders",
    }
}

/// The live brokers and the partitions of the topics that are not internal.
fn layout_of(response: &MetadataResponse) -> (Vec<i32>, Vec<Placement>) {
    let brokers = response.brokers.iter().map(|b| b.node_id).collect();
    let mut layout = Vec::new();
    for topic in &response.topics {
        let Some(name) = topic.name.as_ref() else {
            continue;
        };
        if topic.is_internal || name.starts_with("__") || topic.error_code != codes::NONE {
            continue;
        }
        for p in &topic.partitions {
            layout.push(Placement {
                topic: name.clone(),
                partition: p.partition_index,
                leader: p.leader_id,
                replicas: p.replica_nodes.clone(),
                isr: p.isr_nodes.clone(),
            });
        }
    }
    layout.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    (brokers, layout)
}

impl Node for RebalancerNode {
    fn kind(&self) -> &'static str {
        "rebalancer"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.client = build_client(&self.bootstrap, self.id);
        self.pending.clear();
        self.next_at = 0;
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let events = self.client.on_frame(ctx, frame);
        self.drive(ctx, events);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let cmd = command.get("cmd").and_then(Value::as_str).unwrap_or("");
        match cmd {
            "plan" | "execute" => {
                if !self.pending.is_empty() {
                    return Err("a run is in progress".to_string());
                }
                self.start_run(ctx, cmd == "execute");
            }
            "pause" => self.paused = true,
            "resume" => {
                self.paused = false;
                self.next_at = self.next_at.min(ctx.now());
            }
            other => return Err(format!("unknown rebalancer command {other:?}")),
        }
        self.drive(ctx, Vec::new());
        Ok(json!({ "cmd": cmd, "state": self.state, "paused": self.paused }))
    }

    fn snapshot(&self) -> Value {
        let proposals: Vec<Value> = self
            .proposals
            .iter()
            .map(|p| {
                json!({
                    "topic": p.topic,
                    "partition": p.partition,
                    "from": p.from,
                    "to": p.to,
                    "reason": p.reason,
                })
            })
            .collect();
        let balance: serde_json::Map<String, Value> = self
            .balance
            .iter()
            .map(|(b, (replicas, leaders))| {
                (
                    b.to_string(),
                    json!({ "replicas": replicas, "leaders": leaders }),
                )
            })
            .collect();
        json!({
            "proposals": proposals,
            "executed": self.executed,
            "balance": balance,
            "state": self.state,
            "paused": self.paused,
            "execute": self.execute,
            "goals": self.goals.iter().map(|g| g.name()).collect::<Vec<_>>(),
            "last_plan_at": self.last_plan_at,
            "reassigning": self.reassigning,
            "errors": self.errors,
            "client": self.client.snapshot(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;
    use serde_json::json;

    use super::{Goal, Placement, plan};
    use crate::lab::apps::admin::tests::Remote;

    fn placed(topic: &str, partition: i32, replicas: &[i32]) -> Placement {
        Placement {
            topic: topic.to_string(),
            partition,
            leader: replicas[0],
            replicas: replicas.to_vec(),
            isr: replicas.to_vec(),
        }
    }

    #[test]
    fn the_plan_evens_replicas_then_preferred_leaders() {
        // Four single-replica partitions all on broker 1, of three brokers.
        let layout: Vec<Placement> = (0..4).map(|p| placed("orders", p, &[1])).collect();
        let both = BTreeSet::from([Goal::ReplicaCount, Goal::LeaderCount]);
        let moves: Vec<(i32, Vec<i32>, String)> = plan(&layout, &[1, 2, 3], &both)
            .into_iter()
            .map(|p| (p.partition, p.to, p.reason))
            .collect();
        assert!(
            moves
                == [
                    (0, vec![2], "replica_count".to_string()),
                    (1, vec![3], "replica_count".to_string()),
                ]
        );
        // Replicas even already, but broker 1 is first everywhere: only the
        // order changes.
        let layout: Vec<Placement> = (0..3).map(|p| placed("orders", p, &[1, 2, 3])).collect();
        let moves: Vec<(i32, Vec<i32>, String)> = plan(&layout, &[1, 2, 3], &both)
            .into_iter()
            .map(|p| (p.partition, p.to, p.reason))
            .collect();
        assert!(
            moves
                == [
                    (0, vec![2, 1, 3], "leader_count".to_string()),
                    (1, vec![3, 2, 1], "leader_count".to_string()),
                ]
        );
        // A goal left out is not pursued; a balanced layout plans nothing.
        let replicas_only = BTreeSet::from([Goal::ReplicaCount]);
        assert!(plan(&layout, &[1, 2, 3], &replicas_only).is_empty());
        let balanced = [
            placed("t", 0, &[1, 2]),
            placed("t", 1, &[2, 3]),
            placed("t", 2, &[3, 1]),
        ];
        assert!(plan(&balanced, &[1, 2, 3], &both).is_empty());
    }

    #[test]
    fn the_rebalancer_executes_its_plan_then_elects_preferred_leaders() {
        let mut remote = Remote::with_node(
            "rebalancer",
            json!({ "bootstrap": [1], "interval_ms": 1000 }),
        );
        {
            let mut state = remote.state.borrow_mut();
            state.add_topic("orders", 3, 1);
            for p in state
                .topics
                .get_mut("orders")
                .unwrap()
                .partitions
                .values_mut()
            {
                p.leader = 1;
                p.replicas = vec![1];
                p.isr = vec![1];
            }
        }
        remote.run_for(200);
        let snap = remote.snapshot();
        assert!(snap["executed"] == 2, "{snap}");
        assert!(
            snap["proposals"]
                == json!([
                    { "topic": "orders", "partition": 0, "from": [1], "to": [2], "reason": "replica_count" },
                    { "topic": "orders", "partition": 1, "from": [1], "to": [3], "reason": "replica_count" },
                ])
        );
        let replicas: Vec<Vec<i32>> = remote.state.borrow().topics["orders"]
            .partitions
            .values()
            .map(|p| p.replicas.clone())
            .collect();
        assert!(replicas == [vec![2], vec![3], vec![1]]);
        // The next run finds nothing to move and the leaders where they
        // belong (the fake moves a leader off a dropped replica).
        remote.run_for(1_000);
        let snap = remote.snapshot();
        assert!(snap["proposals"] == json!([]));
        assert!(snap["state"] == "balanced");
        assert!(
            snap["balance"]
                == json!({
                    "1": { "replicas": 1, "leaders": 1 },
                    "2": { "replicas": 1, "leaders": 1 },
                    "3": { "replicas": 1, "leaders": 1 },
                })
        );
        assert!(remote.events("rebalance_executed") == [json!({ "partitions": 2 })]);

        // Paused, it runs only on command; `plan` never executes.
        remote
            .world
            .control(super::super::admin::tests::ADMIN, json!({ "cmd": "pause" }))
            .unwrap();
        remote
            .state
            .borrow_mut()
            .topics
            .get_mut("orders")
            .unwrap()
            .partitions
            .get_mut(&0)
            .unwrap()
            .replicas = vec![1];
        remote.run_for(3_000);
        assert!(remote.events("rebalance_planned").len() == 2);
        remote
            .world
            .control(super::super::admin::tests::ADMIN, json!({ "cmd": "plan" }))
            .unwrap();
        remote.run_for(100);
        let snap = remote.snapshot();
        assert!(snap["proposals"].as_array().unwrap().len() == 1);
        assert!(snap["executed"] == 2);
        assert!(snap["paused"] == true);
        assert!(
            remote
                .world
                .control(super::super::admin::tests::ADMIN, json!({ "cmd": "spin" }))
                .unwrap_err()
                == "unknown rebalancer command \"spin\""
        );
    }
}
