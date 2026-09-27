//! A streams group member's side of KIP-1071: the `StreamsGroupHeartbeat`
//! requests it sends and what it makes of the answers.
//!
//! The member follows Kafka's `StreamsGroupHeartbeatRequestManager` and
//! `StreamsMembershipManager`. It joins at epoch 0 with the topology, its
//! process id, the rebalance timeout, empty client tags and three empty task
//! lists; afterwards each heartbeat leaves those out (`-1` and null, "unchanged
//! since the last heartbeat") and carries the three task lists only when the
//! tasks the member owns differ from the lists it last sent, as Kafka's
//! `HeartbeatState.LastSentFields` does. Every heartbeat echoes the endpoint
//! information epoch of the last answer. A response that carries task lists
//! is the member's new assignment; the node reconciles toward it (it closes
//! the tasks it lost, after their commit, and opens the new ones) and reports
//! the tasks it owns, which the member sends at once, since the coordinator
//! hands a task to a member only after its previous owner reported it gone.
//! `FENCED_MEMBER_EPOCH` and `UNKNOWN_MEMBER_ID` make the member give up every
//! task and join again from epoch 0; the coordinator errors retry after a
//! backoff; a refused topology is fatal.
//!
//! The type does no I/O: the node sends what [`Membership::poll`] builds and
//! hands the answer to [`Membership::on_response`].

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    common::{
        streams_group_heartbeat_request::task_ids::TaskIds as RequestTaskIds,
        streams_group_heartbeat_response::task_ids::TaskIds as ResponseTaskIds,
    },
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Topology as WireTopology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use serde_json::{Value, json};

use crate::lab::{codes, net::Millis};

/// The member epoch of a join.
const JOIN_EPOCH: i32 = 0;

/// The rebalance timeout of a heartbeat after the join: unchanged.
const UNCHANGED_REBALANCE_TIMEOUT_MS: i32 = -1;

/// The wait before a heartbeat that failed goes again: Kafka's
/// `retry.backoff.ms`.
const RETRY_BACKOFF_MS: Millis = 100;

/// The heartbeat interval before the coordinator names one.
const INITIAL_HEARTBEAT_INTERVAL_MS: Millis = 5_000;

/// Tasks of one role: subtopology id to partitions.
pub type TaskSet = BTreeMap<String, BTreeSet<i32>>;

/// The tasks of the three roles.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Tasks {
    pub active: TaskSet,
    pub standby: TaskSet,
    pub warmup: TaskSet,
}

impl Tasks {
    fn from_response(response: &StreamsGroupHeartbeatResponse) -> Option<Self> {
        let role = |ids: &Option<Vec<ResponseTaskIds>>| {
            ids.as_ref().map(|ids| {
                let mut set = TaskSet::new();
                for t in ids {
                    set.entry(t.subtopology_id.clone())
                        .or_default()
                        .extend(t.partitions.iter().copied());
                }
                set
            })
        };
        // The coordinator sends the three lists together.
        Some(Self {
            active: role(&response.active_tasks)?,
            standby: role(&response.standby_tasks).unwrap_or_default(),
            warmup: role(&response.warmup_tasks).unwrap_or_default(),
        })
    }

    fn wire(set: &TaskSet) -> Vec<RequestTaskIds> {
        set.iter()
            .filter(|(_, partitions)| !partitions.is_empty())
            .map(|(subtopology, partitions)| RequestTaskIds {
                subtopology_id: subtopology.clone(),
                partitions: partitions.iter().copied().collect(),
                ..Default::default()
            })
            .collect()
    }

    /// Every `(subtopology, partition)` of a role.
    #[must_use]
    pub fn list(set: &TaskSet) -> Vec<(String, i32)> {
        set.iter()
            .flat_map(|(s, ps)| ps.iter().map(move |p| (s.clone(), *p)))
            .collect()
    }

    fn json(set: &TaskSet) -> Value {
        Value::Array(
            Self::list(set)
                .into_iter()
                .map(|(s, p)| Value::String(format!("{s}_{p}")))
                .collect(),
        )
    }
}

/// Where the member stands.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MemberState {
    /// A join heartbeat is due or on the wire.
    Joining,
    /// The member holds an epoch.
    Stable,
    /// The coordinator refused the member for good.
    Failed { code: i16, message: String },
}

/// What an answer means for the node.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MembershipEvent {
    /// The member joined with an id and an epoch.
    Joined { member_id: String, epoch: i32 },
    /// The coordinator sent a new assignment; the node reconciles to it.
    Assigned(Tasks),
    /// The status list of the group changed: `(code, name, detail)`.
    Status(Vec<(i8, &'static str, String)>),
    /// The member was fenced; every task is lost and the member joins again.
    Fenced { code: i16 },
    /// The heartbeat failed and goes again after a backoff.
    Retry { code: i16 },
    /// The member stopped for good.
    Failed { code: i16, message: String },
}

/// The name of a KIP-1071 status code.
#[must_use]
pub const fn status_name(code: i8) -> &'static str {
    match code {
        0 => "STALE_TOPOLOGY",
        1 => "MISSING_SOURCE_TOPICS",
        2 => "INCORRECTLY_PARTITIONED_TOPICS",
        3 => "MISSING_INTERNAL_TOPICS",
        4 => "SHUTDOWN_APPLICATION",
        5 => "ASSIGNMENT_DELAYED",
        _ => "UNKNOWN_STATUS",
    }
}

/// The member. See the module documentation.
pub struct Membership {
    group_id: String,
    member_id: String,
    process_id: String,
    rebalance_timeout_ms: i32,
    topology: WireTopology,
    epoch: i32,
    state: MemberState,
    heartbeat_interval_ms: Millis,
    next_heartbeat_at: Millis,
    /// A report of changed tasks waits for this after a failed heartbeat.
    backoff_until: Millis,
    in_flight: bool,
    /// The tasks the member runs.
    owned: Tasks,
    /// The task lists the last accepted heartbeat carried; `None` sends them
    /// again.
    last_sent: Option<Tasks>,
    /// The lists of the heartbeat on the wire.
    sending: Option<Tasks>,
    /// The endpoint information epoch of the last answer.
    endpoint_epoch: i32,
    status: Vec<(i8, &'static str, String)>,
    heartbeats: u64,
}

impl Membership {
    /// A member of `group_id` that joins at `now`.
    #[must_use]
    pub fn new(
        group_id: &str,
        member_id: &str,
        process_id: &str,
        rebalance_timeout_ms: i32,
        topology: WireTopology,
        now: Millis,
    ) -> Self {
        Self {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            process_id: process_id.to_string(),
            rebalance_timeout_ms,
            topology,
            epoch: JOIN_EPOCH,
            state: MemberState::Joining,
            heartbeat_interval_ms: INITIAL_HEARTBEAT_INTERVAL_MS,
            next_heartbeat_at: now,
            backoff_until: now,
            in_flight: false,
            owned: Tasks::default(),
            last_sent: None,
            sending: None,
            endpoint_epoch: 0,
            status: Vec::new(),
            heartbeats: 0,
        }
    }

    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    #[must_use]
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    #[must_use]
    pub fn state(&self) -> &MemberState {
        &self.state
    }

    /// The tasks the member reports as owned.
    #[must_use]
    pub fn owned(&self) -> &Tasks {
        &self.owned
    }

    /// Record the tasks the node runs now; a change goes out with the next
    /// heartbeat, at once.
    pub fn set_owned(&mut self, tasks: Tasks) {
        self.owned = tasks;
    }

    fn owned_changed(&self) -> bool {
        self.last_sent.as_ref() != Some(&self.owned)
    }

    /// The heartbeat to send at `now`, if one is due: a join, a heartbeat on
    /// the interval, or one that reports a change of the owned tasks.
    pub fn poll(&mut self, now: Millis) -> Option<StreamsGroupHeartbeatRequest> {
        if self.in_flight || matches!(self.state, MemberState::Failed { .. }) {
            return None;
        }
        let joining = self.state == MemberState::Joining;
        let due = now >= self.next_heartbeat_at;
        if !(due || (!joining && self.owned_changed() && now >= self.backoff_until)) {
            return None;
        }
        let request = if joining {
            // Kafka refuses a join whose task lists are absent or not empty.
            self.sending = Some(Tasks::default());
            StreamsGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: JOIN_EPOCH,
                endpoint_information_epoch: self.endpoint_epoch,
                process_id: Some(self.process_id.clone()),
                rebalance_timeout_ms: self.rebalance_timeout_ms,
                topology: Some(self.topology.clone()),
                client_tags: Some(Vec::new()),
                active_tasks: Some(Vec::new()),
                standby_tasks: Some(Vec::new()),
                warmup_tasks: Some(Vec::new()),
                ..Default::default()
            }
        } else {
            let lists = self.owned_changed().then(|| self.owned.clone());
            self.sending.clone_from(&lists);
            StreamsGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: self.epoch,
                endpoint_information_epoch: self.endpoint_epoch,
                rebalance_timeout_ms: UNCHANGED_REBALANCE_TIMEOUT_MS,
                active_tasks: lists.as_ref().map(|t| Tasks::wire(&t.active)),
                standby_tasks: lists.as_ref().map(|t| Tasks::wire(&t.standby)),
                warmup_tasks: lists.as_ref().map(|t| Tasks::wire(&t.warmup)),
                ..Default::default()
            }
        };
        self.in_flight = true;
        self.heartbeats += 1;
        Some(request)
    }

    /// Apply the answer to the heartbeat on the wire: the response, or
    /// `None` when the request failed on the way.
    pub fn on_response(
        &mut self,
        now: Millis,
        response: Option<StreamsGroupHeartbeatResponse>,
    ) -> Vec<MembershipEvent> {
        self.in_flight = false;
        let sent = self.sending.take();
        let Some(response) = response else {
            return self.retry(now, codes::NETWORK_EXCEPTION);
        };
        match response.error_code {
            codes::NONE => self.accepted(now, &response, sent),
            codes::FENCED_MEMBER_EPOCH | codes::UNKNOWN_MEMBER_ID | codes::GROUP_ID_NOT_FOUND => {
                self.epoch = JOIN_EPOCH;
                self.state = MemberState::Joining;
                self.owned = Tasks::default();
                self.last_sent = None;
                self.status.clear();
                self.next_heartbeat_at = now;
                self.backoff_until = now;
                vec![MembershipEvent::Fenced {
                    code: response.error_code,
                }]
            }
            codes::COORDINATOR_NOT_AVAILABLE
            | codes::NOT_COORDINATOR
            | codes::COORDINATOR_LOAD_IN_PROGRESS
            | codes::NETWORK_EXCEPTION
            | codes::REQUEST_TIMED_OUT => self.retry(now, response.error_code),
            code => {
                let message = response
                    .error_message
                    .unwrap_or_else(|| format!("error code {code}"));
                self.state = MemberState::Failed {
                    code,
                    message: message.clone(),
                };
                vec![MembershipEvent::Failed { code, message }]
            }
        }
    }

    fn retry(&mut self, now: Millis, code: i16) -> Vec<MembershipEvent> {
        // Kafka's `HeartbeatState.reset`: the next heartbeat reports the
        // owned tasks again.
        self.last_sent = None;
        self.next_heartbeat_at = now + RETRY_BACKOFF_MS;
        self.backoff_until = self.next_heartbeat_at;
        vec![MembershipEvent::Retry { code }]
    }

    fn accepted(
        &mut self,
        now: Millis,
        response: &StreamsGroupHeartbeatResponse,
        sent: Option<Tasks>,
    ) -> Vec<MembershipEvent> {
        let mut events = Vec::new();
        if !response.member_id.is_empty() {
            self.member_id.clone_from(&response.member_id);
        }
        if let Ok(interval) = Millis::try_from(response.heartbeat_interval_ms)
            && interval > 0
        {
            self.heartbeat_interval_ms = interval;
        }
        self.next_heartbeat_at = now + self.heartbeat_interval_ms;
        self.backoff_until = now;
        if let Some(sent) = sent {
            self.last_sent = Some(sent);
        }
        if self.state == MemberState::Joining {
            self.state = MemberState::Stable;
            events.push(MembershipEvent::Joined {
                member_id: self.member_id.clone(),
                epoch: response.member_epoch,
            });
        }
        self.epoch = response.member_epoch;
        self.endpoint_epoch = response.endpoint_information_epoch;
        // A null status list leaves the last one standing.
        if let Some(list) = &response.status {
            let status: Vec<(i8, &'static str, String)> = list
                .iter()
                .map(|s| {
                    (
                        s.status_code,
                        status_name(s.status_code),
                        s.status_detail.clone(),
                    )
                })
                .collect();
            if status != self.status {
                self.status.clone_from(&status);
                events.push(MembershipEvent::Status(status));
            }
        }
        if let Some(tasks) = Tasks::from_response(response) {
            events.push(MembershipEvent::Assigned(tasks));
        }
        events
    }

    /// When the member needs [`Membership::poll`] again.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        if self.in_flight || matches!(self.state, MemberState::Failed { .. }) {
            return None;
        }
        if self.state == MemberState::Stable && self.owned_changed() {
            return Some(now.max(self.backoff_until));
        }
        Some(self.next_heartbeat_at)
    }

    /// The member for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let state = match &self.state {
            MemberState::Joining => json!("joining"),
            MemberState::Stable => json!("stable"),
            MemberState::Failed { code, message } => {
                json!({ "failed": code, "message": message })
            }
        };
        let status: Vec<Value> = self
            .status
            .iter()
            .map(|(code, name, detail)| json!({ "code": code, "name": name, "detail": detail }))
            .collect();
        json!({
            "member_id": self.member_id,
            "process_id": self.process_id,
            "member_epoch": self.epoch,
            "state": state,
            "heartbeats": self.heartbeats,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "status": status,
            "owned_active": Tasks::json(&self.owned.active),
            "owned_standby": Tasks::json(&self.owned.standby),
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

    use super::*;

    /// Tasks of one role as `(subtopology, partitions)` pairs.
    type Pairs<'a> = &'a [(&'a str, &'a [i32])];

    fn member() -> Membership {
        Membership::new(
            "app",
            "m-1",
            "p-1",
            300_000,
            WireTopology {
                epoch: 0,
                ..Default::default()
            },
            0,
        )
    }

    fn answer(epoch: i32, tasks: Option<(Pairs<'_>, Pairs<'_>)>) -> StreamsGroupHeartbeatResponse {
        let ids = |set: Pairs<'_>| {
            set.iter()
                .map(|(s, ps)| ResponseTaskIds {
                    subtopology_id: (*s).to_string(),
                    partitions: ps.to_vec(),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
        };
        StreamsGroupHeartbeatResponse {
            member_id: "m-1".to_string(),
            member_epoch: epoch,
            heartbeat_interval_ms: 5_000,
            status: Some(Vec::new()),
            active_tasks: tasks.map(|(a, _)| ids(a)),
            standby_tasks: tasks.map(|(_, s)| ids(s)),
            warmup_tasks: tasks.map(|_| Vec::new()),
            ..Default::default()
        }
    }

    fn set(pairs: Pairs<'_>) -> TaskSet {
        pairs
            .iter()
            .map(|(s, ps)| ((*s).to_string(), ps.iter().copied().collect()))
            .collect()
    }

    fn wire(pairs: Pairs<'_>) -> Vec<RequestTaskIds> {
        pairs
            .iter()
            .map(|(s, ps)| RequestTaskIds {
                subtopology_id: (*s).to_string(),
                partitions: ps.to_vec(),
                ..Default::default()
            })
            .collect()
    }

    /// The join `member()` sends.
    fn join() -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: "app".to_string(),
            member_id: "m-1".to_string(),
            member_epoch: 0,
            process_id: Some("p-1".to_string()),
            rebalance_timeout_ms: 300_000,
            topology: Some(WireTopology::default()),
            client_tags: Some(Vec::new()),
            active_tasks: Some(Vec::new()),
            standby_tasks: Some(Vec::new()),
            warmup_tasks: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// A heartbeat after the join: no process id, no rebalance timeout, and
    /// the active and standby lists when they changed.
    fn heartbeat(
        epoch: i32,
        lists: Option<(Pairs<'_>, Pairs<'_>)>,
    ) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: "app".to_string(),
            member_id: "m-1".to_string(),
            member_epoch: epoch,
            rebalance_timeout_ms: -1,
            active_tasks: lists.map(|(a, _)| wire(a)),
            standby_tasks: lists.map(|(_, s)| wire(s)),
            warmup_tasks: lists.map(|_| Vec::new()),
            ..Default::default()
        }
    }

    #[test]
    fn the_join_carries_the_topology_and_three_empty_lists() {
        let mut m = member();
        assert!(m.poll(0) == Some(join()));
        // One heartbeat at a time.
        assert!(m.poll(0).is_none());
        assert!(m.next_deadline(0).is_none());
    }

    #[test]
    fn the_task_lifecycle_follows_the_scripted_coordinator() {
        let mut m = member();
        m.poll(0).unwrap();
        // The coordinator accepts the join and delays the assignment.
        let detail = "Assignment delayed due to the configured initial rebalance delay.";
        let mut delayed = answer(1, Some((&[], &[])));
        delayed.status = Some(vec![Status {
            status_code: 5,
            status_detail: detail.to_string(),
            ..Default::default()
        }]);
        assert!(
            m.on_response(10, Some(delayed))
                == vec![
                    MembershipEvent::Joined {
                        member_id: "m-1".to_string(),
                        epoch: 1
                    },
                    MembershipEvent::Status(vec![(5, "ASSIGNMENT_DELAYED", detail.to_string())]),
                    MembershipEvent::Assigned(Tasks::default()),
                ]
        );
        // Nothing changed, so the next heartbeat waits for the interval and
        // carries no lists.
        assert!(m.poll(10).is_none());
        assert!(m.next_deadline(10) == Some(5_010));
        assert!(m.poll(5_010) == Some(heartbeat(1, None)));

        // The assignment arrives: tasks 0_0 and 0_1 active, 1_0 standby.
        let events = m.on_response(
            5_020,
            Some(answer(2, Some((&[("0", &[0, 1])], &[("1", &[0])])))),
        );
        let assigned = Tasks {
            active: set(&[("0", &[0, 1])]),
            standby: set(&[("1", &[0])]),
            warmup: TaskSet::new(),
        };
        assert!(
            events
                == vec![
                    MembershipEvent::Status(Vec::new()),
                    MembershipEvent::Assigned(assigned.clone()),
                ]
        );
        // The node opens the tasks and reports them at once.
        m.set_owned(assigned);
        assert!(m.next_deadline(5_020) == Some(5_020));
        assert!(m.poll(5_020) == Some(heartbeat(2, Some((&[("0", &[0, 1])], &[("1", &[0])])))));
        assert!(m.on_response(5_030, Some(answer(2, None))).is_empty());
        // Once acknowledged, the lists stay off the wire.
        assert!(m.poll(10_030) == Some(heartbeat(2, None)));

        // A rebalance takes 0_1 and the standby away: the node closes them
        // and reports.
        assert!(
            m.on_response(10_040, Some(answer(2, Some((&[("0", &[0])], &[])))))
                == vec![MembershipEvent::Assigned(Tasks {
                    active: set(&[("0", &[0])]),
                    ..Tasks::default()
                })]
        );
        m.set_owned(Tasks {
            active: set(&[("0", &[0])]),
            ..Tasks::default()
        });
        assert!(m.poll(10_040) == Some(heartbeat(2, Some((&[("0", &[0])], &[])))));
    }

    #[test]
    fn a_null_status_leaves_the_last_one_and_the_endpoint_epoch_is_echoed() {
        let mut m = member();
        m.poll(0).unwrap();
        let detail = "Source topics orders are missing.";
        let mut first = answer(1, Some((&[], &[])));
        first.status = Some(vec![Status {
            status_code: 1,
            status_detail: detail.to_string(),
            ..Default::default()
        }]);
        first.endpoint_information_epoch = 4;
        m.on_response(1, Some(first));
        assert!(
            m.poll(5_001)
                == Some(StreamsGroupHeartbeatRequest {
                    endpoint_information_epoch: 4,
                    ..heartbeat(1, None)
                })
        );
        let mut second = answer(1, None);
        second.status = None;
        second.endpoint_information_epoch = 4;
        assert!(m.on_response(5_002, Some(second)).is_empty());
        assert!(
            m.snapshot()
                == json!({
                    "member_id": "m-1",
                    "process_id": "p-1",
                    "member_epoch": 1,
                    "state": "stable",
                    "heartbeats": 2,
                    "heartbeat_interval_ms": 5_000,
                    "status": [{ "code": 1, "name": "MISSING_SOURCE_TOPICS", "detail": detail }],
                    "owned_active": [],
                    "owned_standby": [],
                })
        );
    }

    #[test]
    fn a_fenced_member_loses_its_tasks_and_joins_again() {
        let mut m = member();
        m.poll(0).unwrap();
        m.on_response(5, Some(answer(3, Some((&[("0", &[0])], &[])))));
        m.set_owned(Tasks {
            active: set(&[("0", &[0])]),
            ..Tasks::default()
        });
        m.poll(5).unwrap();
        let fenced = StreamsGroupHeartbeatResponse {
            error_code: codes::FENCED_MEMBER_EPOCH,
            ..Default::default()
        };
        assert!(
            m.on_response(9, Some(fenced))
                == vec![MembershipEvent::Fenced {
                    code: codes::FENCED_MEMBER_EPOCH
                }]
        );
        assert!(*m.owned() == Tasks::default());
        assert!(m.poll(9) == Some(join()));
    }

    #[test]
    fn coordinator_errors_and_lost_requests_retry_and_resend_the_lists() {
        let mut m = member();
        m.poll(0).unwrap();
        m.on_response(5, Some(answer(1, Some((&[("0", &[2])], &[])))));
        m.set_owned(Tasks {
            active: set(&[("0", &[2])]),
            ..Tasks::default()
        });
        m.poll(5).unwrap();
        m.on_response(6, Some(answer(1, None)));
        // A lost heartbeat: retry after the backoff with the lists again.
        assert!(m.poll(5_006) == Some(heartbeat(1, None)));
        assert!(
            m.on_response(5_010, None)
                == vec![MembershipEvent::Retry {
                    code: codes::NETWORK_EXCEPTION
                }]
        );
        assert!(m.poll(5_050).is_none());
        assert!(m.poll(5_110) == Some(heartbeat(1, Some((&[("0", &[2])], &[])))));
        let moved = StreamsGroupHeartbeatResponse {
            error_code: codes::NOT_COORDINATOR,
            ..Default::default()
        };
        assert!(
            m.on_response(5_120, Some(moved))
                == vec![MembershipEvent::Retry {
                    code: codes::NOT_COORDINATOR
                }]
        );
    }

    #[test]
    fn a_refused_topology_stops_the_member() {
        let mut m = member();
        m.poll(0).unwrap();
        let refused = StreamsGroupHeartbeatResponse {
            error_code: codes::STREAMS_INVALID_TOPOLOGY,
            error_message: Some("bad".to_string()),
            ..Default::default()
        };
        assert!(
            m.on_response(3, Some(refused))
                == vec![MembershipEvent::Failed {
                    code: codes::STREAMS_INVALID_TOPOLOGY,
                    message: "bad".to_string()
                }]
        );
        assert!(m.poll(100_000).is_none());
        assert!(m.next_deadline(3).is_none());
    }
}
