//! The broker's lifecycle against the active controller, Kafka's
//! `BrokerLifecycleManager`.
//!
//! On start the broker registers with the active controller
//! (`BrokerRegistration`, a new incarnation id per start), and once the
//! controller answers it sends a heartbeat at once and then one every
//! `broker.heartbeat.interval.ms`. A failed or timed-out attempt tries again
//! after the same interval. The broker moves through Kafka's states:
//! `STARTING` until a heartbeat answer says it caught up with the metadata
//! log up to its own registration, `RECOVERY` until the controller unfences
//! it, then `RUNNING`. It asks to stay fenced (`want_fence`) until it caught
//! up, and its heartbeats in `STARTING` follow each other after 10 ms, as
//! Kafka's do.
//!
//! Kafka's `BrokerServer` enables the client listener only once the broker
//! was unfenced, so until then the broker keeps the requests of its client
//! connections queued, unanswered, as a socket nobody accepts yet would.

use krabka_metadata::feature_registry;
use krabka_protocol::{
    owned::{
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_registration_request::{
            BrokerRegistrationRequest, Feature, Listener as RegistrationListener,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    BrokerNode, LAB_CLUSTER_ID,
    channel::{ChannelOutcome, ControllerRequest, ControllerResponse, Purpose},
    cluster::{broker_host, cluster_id_string},
};
use crate::lab::{
    codes,
    net::{Ctx, KAFKA_PORT, Millis},
};

/// Kafka's `broker.heartbeat.interval.ms` default.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: Millis = 2_000;
/// Kafka's `broker.session.timeout.ms` default.
pub const DEFAULT_SESSION_TIMEOUT_MS: Millis = 9_000;
/// The pause between heartbeats while the broker catches up, Kafka's
/// `STARTING` schedule.
pub const CATCH_UP_HEARTBEAT_MS: Millis = 10;
/// Kafka's `SecurityProtocol.PLAINTEXT` id.
const PLAINTEXT_PROTOCOL: i16 = 0;

/// Kafka's `BrokerState` as the lifecycle moves through it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerState {
    /// Registering, and catching up with the metadata log.
    Starting,
    /// Caught up, waiting for the controller to unfence the broker.
    Recovery,
    /// Unfenced at least once since the broker started.
    Running,
}

impl BrokerState {
    /// Kafka's name of the state.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Recovery => "RECOVERY",
            Self::Running => "RUNNING",
        }
    }
}

/// Whether a registration or a heartbeat is on its way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Communication {
    /// Nothing is on its way.
    Idle,
    /// A request is on its way.
    InFlight,
    /// A request is on its way, and the next one came due meanwhile: the
    /// next schedule is immediate, Kafka's `nextSchedulingShouldBeImmediate`.
    InFlightThenImmediate,
}

/// The lifecycle of one broker incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lifecycle {
    /// Where the broker is.
    pub state: BrokerState,
    /// KIP-631: the id of this process of the broker, new at every start.
    pub incarnation_id: Uuid,
    /// Whether the controller accepted the registration.
    pub registered: bool,
    /// The broker epoch the registration got, `-1` before.
    pub broker_epoch: i64,
    /// Whether the broker may be unfenced: it caught up once.
    pub ready_to_unfence: bool,
    /// Whether the last heartbeat answer said the broker is fenced.
    pub fenced: bool,
    /// Whether a registration or a heartbeat is on its way.
    communication: Communication,
    /// When the next registration or heartbeat goes out.
    next_at: Option<Millis>,
}

impl Lifecycle {
    /// A lifecycle that has not started.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: BrokerState::Starting,
            incarnation_id: Uuid::nil(),
            registered: false,
            broker_epoch: -1,
            ready_to_unfence: false,
            fenced: true,
            communication: Communication::Idle,
            next_at: None,
        }
    }

    /// Start a new incarnation: register at once.
    pub fn start(&mut self, now: Millis, incarnation_id: Uuid) {
        *self = Self {
            incarnation_id,
            next_at: Some(now),
            ..Self::new()
        };
    }

    /// Whether the client listener serves requests: the broker was unfenced
    /// once since it started.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.state == BrokerState::Running
    }

    /// When the lifecycle needs the timer.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.next_at
    }

    /// Kafka's `scheduleNextCommunication`, once the answer arrived: at once
    /// when the next one came due while the request was on its way.
    fn schedule(&mut self, now: Millis, delay: Millis) {
        let immediate = self.communication == Communication::InFlightThenImmediate;
        self.communication = Communication::Idle;
        self.next_at = Some(now + if immediate { 0 } else { delay });
    }

    /// Kafka's `scheduleNextCommunicationAfterFailure`: never at once.
    fn schedule_after_failure(&mut self, now: Millis, interval: Millis) {
        self.communication = Communication::Idle;
        self.next_at = Some(now + interval);
    }

    /// The lifecycle for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "state": self.state.name(),
            "registered": self.registered,
            "broker_epoch": self.broker_epoch,
            "fenced": self.fenced,
            "incarnation_id": self.incarnation_id.to_string(),
        })
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerNode {
    /// Send the registration or the heartbeat that is due.
    pub(super) fn poll_lifecycle(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.lifecycle.next_at.is_none_or(|at| at > now) {
            return;
        }
        self.lifecycle.next_at = None;
        if self.lifecycle.communication != Communication::Idle {
            self.lifecycle.communication = Communication::InFlightThenImmediate;
            return;
        }
        let request = if self.lifecycle.registered {
            ControllerRequest::BrokerHeartbeat(self.heartbeat_request())
        } else {
            ControllerRequest::BrokerRegistration(self.registration_request())
        };
        self.lifecycle.communication = Communication::InFlight;
        self.heartbeat_channel
            .enqueue(now, request, Purpose::Lifecycle);
    }

    fn registration_request(&self) -> BrokerRegistrationRequest {
        let me = self.config.broker_id;
        BrokerRegistrationRequest {
            broker_id: me,
            cluster_id: cluster_id_string(LAB_CLUSTER_ID),
            incarnation_id: WireUuid(self.lifecycle.incarnation_id.into_bytes()),
            listeners: vec![RegistrationListener {
                name: "PLAINTEXT".to_string(),
                host: broker_host(me),
                port: KAFKA_PORT,
                security_protocol: PLAINTEXT_PROTOCOL,
                ..RegistrationListener::default()
            }],
            features: feature_registry()
                .iter()
                .map(|feature| {
                    let (min, max) = feature.supported_range();
                    Feature {
                        name: feature.name().to_string(),
                        min_supported_version: min,
                        max_supported_version: max,
                        ..Feature::default()
                    }
                })
                .collect(),
            rack: self.config.rack.clone(),
            is_migrating_zk_broker: false,
            log_dirs: vec![WireUuid(self.log_dir_id().into_bytes())],
            previous_broker_epoch: -1,
            ..BrokerRegistrationRequest::default()
        }
    }

    fn heartbeat_request(&self) -> BrokerHeartbeatRequest {
        BrokerHeartbeatRequest {
            broker_id: self.config.broker_id,
            broker_epoch: self.lifecycle.broker_epoch,
            current_metadata_offset: self.quorum.applied,
            want_fence: !self.lifecycle.ready_to_unfence,
            want_shut_down: false,
            ..BrokerHeartbeatRequest::default()
        }
    }

    /// The KIP-858 id of the broker's one log directory: stable for the
    /// broker id.
    fn log_dir_id(&self) -> Uuid {
        Uuid::from_u64_pair(0x6c61_622d_6c6f_6764, u64::from(self.id.0))
    }

    /// The answer to a registration or a heartbeat, or its timeout.
    pub(super) fn on_lifecycle_outcome(&mut self, ctx: &mut Ctx<'_>, outcome: ChannelOutcome) {
        let now = ctx.now();
        let interval = self.config.broker_heartbeat_interval_ms;
        match outcome {
            ChannelOutcome::Response(ControllerResponse::BrokerRegistration(response)) => {
                if response.error_code == codes::NONE {
                    self.lifecycle.registered = true;
                    self.lifecycle.broker_epoch = response.broker_epoch;
                    ctx.event(
                        "broker_registered",
                        json!({ "broker_epoch": response.broker_epoch, "level": "info" }),
                    );
                    self.lifecycle.schedule(now, 0);
                } else {
                    ctx.event(
                        "broker_registration_failed",
                        json!({ "error_code": response.error_code, "level": "warn" }),
                    );
                    self.lifecycle.schedule_after_failure(now, interval);
                }
            }
            ChannelOutcome::Response(ControllerResponse::BrokerHeartbeat(response)) => {
                if response.error_code != codes::NONE {
                    ctx.event(
                        "broker_heartbeat_failed",
                        json!({ "error_code": response.error_code, "level": "warn" }),
                    );
                    self.lifecycle.schedule_after_failure(now, interval);
                    return;
                }
                let was_fenced = self.lifecycle.fenced;
                self.lifecycle.fenced = response.is_fenced;
                match self.lifecycle.state {
                    BrokerState::Starting => {
                        if response.is_caught_up {
                            self.lifecycle.state = BrokerState::Recovery;
                            // The lab broker has no log to recover, so it is
                            // ready to be unfenced as soon as it caught up.
                            self.lifecycle.ready_to_unfence = true;
                        }
                        self.lifecycle.schedule(now, CATCH_UP_HEARTBEAT_MS);
                    }
                    BrokerState::Recovery => {
                        if !response.is_fenced {
                            self.lifecycle.state = BrokerState::Running;
                            ctx.event("broker_unfenced", json!({ "level": "info" }));
                        }
                        self.lifecycle.schedule(now, interval);
                    }
                    BrokerState::Running => {
                        if response.is_fenced && !was_fenced {
                            ctx.event("broker_fenced", json!({ "level": "warn" }));
                        } else if !response.is_fenced && was_fenced {
                            ctx.event("broker_unfenced", json!({ "level": "info" }));
                        }
                        self.lifecycle.schedule(now, interval);
                    }
                }
            }
            ChannelOutcome::Response(_)
            | ChannelOutcome::TimedOut
            | ChannelOutcome::VersionMismatch => {
                self.lifecycle.schedule_after_failure(now, interval);
            }
        }
    }
}
