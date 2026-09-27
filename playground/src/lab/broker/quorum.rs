//! The controller inside the broker: the node's share of the metadata quorum
//! and, while the node leads the quorum, the active controller.
//!
//! Every broker runs a [`ControllerCore`], as a voter or as an observer, and
//! applies every committed batch of the metadata log to its image, in log
//! order, so every broker's image is the replay of the same log. The node
//! whose core leads the quorum becomes the active controller once it has
//! applied everything up to the leader-change marker that opened its
//! leadership, as Kafka's `QuorumController` claims leadership only then. The
//! active controller decides with [`ControllerDecisions`] over an image of
//! its own: the committed image plus every record it proposed, so a decision
//! sees the effect of the ones before it, as Kafka's controller replays its
//! own records before they commit.
//!
//! A controller request answers once its records commit, or, when it wrote
//! nothing, once every record written before it commits, as Kafka's
//! `ControllerWriteEvent` completes. A controller that loses its leadership
//! answers every write still waiting with `NOT_CONTROLLER`, as Kafka's
//! `renounce` fails its purgatory.

use krabka_kraft_core::types::Epoch;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::Encode;
use serde_json::{Value, json};

use super::{
    BrokerNode, TICK_MS,
    dispatch::{HoldReason, Outcome, Reply, RequestCtx, Step, encode_reply},
};
use crate::lab::{
    controller::{ControllerCore, ControllerDecisions, decisions::ControllerConfig},
    net::{Ctx, Millis, NodeId},
};

/// How often the active controller fences brokers whose session expired.
pub const SESSION_CHECK_MS: Millis = TICK_MS;

/// Kafka's `newWrongControllerException` message when no controller is
/// known, which also fails every write a controller that renounced its
/// leadership still held.
pub const NO_ACTIVE_CONTROLLER_MESSAGE: &str = "No controller appears to be active.";

/// The active controller: the decisions and the image it decides over.
pub struct ActiveController {
    /// The quorum epoch this node leads at.
    pub epoch: Epoch,
    /// The decisions.
    pub decisions: ControllerDecisions,
    /// The committed image plus every record this controller proposed.
    pub image: MetadataImage,
    /// The offset of the last batch this controller wrote: the leader-change
    /// marker, until it proposes one.
    pub last_written: i64,
    /// The last offset of this controller's epoch that committed.
    pub committed: i64,
    next_session_check: Millis,
}

/// The node's share of the quorum.
pub struct Quorum {
    /// The quorum driver.
    pub core: ControllerCore,
    /// The active controller, while this node is it.
    pub active: Option<ActiveController>,
    /// The offset of the last committed batch the broker applied, `-1`
    /// before the first. A heartbeat reports it as the broker's metadata
    /// offset.
    pub applied: i64,
    /// The epoch and last committed offset of the controller this node was
    /// until it lost its leadership, so a write that committed before still
    /// answers as committed.
    retired: Option<(Epoch, i64)>,
}

impl Quorum {
    /// The node's share of the quorum `core` drives, before it applied
    /// anything.
    #[must_use]
    pub fn new(core: ControllerCore) -> Self {
        Self {
            core,
            active: None,
            applied: -1,
            retired: None,
        }
    }

    /// Whether a write of the controller at `epoch`, at `offset`, committed.
    fn committed(&self, epoch: Epoch, offset: i64) -> bool {
        let live = self
            .active
            .as_ref()
            .is_some_and(|a| a.epoch == epoch && a.committed >= offset);
        let retired = self
            .retired
            .is_some_and(|(retired, committed)| retired == epoch && committed >= offset);
        live || retired
    }

    /// The controller this node knows is active: itself when it is, else
    /// the quorum leader it knows.
    #[must_use]
    pub fn controller(&self) -> Option<NodeId> {
        self.core.leader()
    }

    /// Kafka's `ControllerExceptions.newWrongControllerException` message
    /// for the leader this node knows.
    #[must_use]
    pub fn not_controller_message(&self) -> String {
        match self.core.leader() {
            Some(leader) => format!("The active controller appears to be node {leader}."),
            None => NO_ACTIVE_CONTROLLER_MESSAGE.to_string(),
        }
    }

    /// Forget what a stopped node knew: the active controller, the retired
    /// one, and the applied offset, which a restart rebuilds from the log.
    pub fn reset(&mut self) {
        self.active = None;
        self.retired = None;
    }

    /// The quorum for the inspector: the core's view and whether this node
    /// is the active controller.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let mut snapshot = self.core.snapshot();
        snapshot["active"] = json!(self.active.is_some());
        snapshot["metadata_offset"] = json!(self.applied);
        snapshot
    }
}

/// A controller answer that waits for its records to commit.
#[derive(Debug)]
pub struct PendingWrite {
    /// The epoch of the controller that wrote.
    epoch: Epoch,
    /// The offset that must commit.
    offset: i64,
    /// The answer once it has.
    reply: Reply,
    /// The answer when the controller lost its leadership first.
    not_controller: Reply,
}

impl BrokerNode {
    /// The controller settings, from the broker's config: combined mode
    /// shares one configuration between the broker and its controller.
    pub(super) fn controller_config(&self) -> ControllerConfig {
        ControllerConfig {
            broker_session_timeout_ms: self.config.broker_session_timeout_ms,
            default_partitions: self.config.default_partitions,
            default_replication_factor: self.config.default_replication_factor,
            ..ControllerConfig::default()
        }
    }

    /// Apply what the quorum committed, follow the node's leadership, and
    /// run the active controller's session check.
    pub(super) fn drive_quorum(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        loop {
            let batches = self.quorum.core.take_committed();
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                self.apply_metadata(ctx, &batch.records);
                self.quorum.applied = batch.offset;
                if let Some(active) = &mut self.quorum.active
                    && active.epoch == batch.epoch
                {
                    active.committed = batch.offset;
                }
            }
        }
        let leading = self
            .quorum
            .core
            .is_leader()
            .then(|| self.quorum.core.epoch());
        if let Some(active) = &self.quorum.active
            && Some(active.epoch) != leading
        {
            self.quorum.retired = Some((active.epoch, active.committed));
            ctx.event(
                "controller",
                json!({ "active": false, "epoch": active.epoch, "level": "warn" }),
            );
            self.quorum.active = None;
        }
        let claimable = self
            .quorum
            .core
            .epoch_start_offset()
            .is_some_and(|start| self.quorum.applied >= start);
        if self.quorum.active.is_none()
            && let Some(epoch) = leading
            && claimable
        {
            let mut decisions =
                ControllerDecisions::new(self.controller_config(), ctx.rand(u64::MAX));
            decisions.activate(&self.image, now);
            self.quorum.active = Some(ActiveController {
                epoch,
                decisions,
                image: self.image.clone(),
                last_written: self.quorum.applied,
                committed: self.quorum.applied,
                next_session_check: now + SESSION_CHECK_MS,
            });
            ctx.event(
                "controller",
                json!({ "active": true, "epoch": epoch, "level": "info" }),
            );
        }
        let due = self
            .quorum
            .active
            .as_ref()
            .is_some_and(|active| now >= active.next_session_check);
        if due {
            let records = self.quorum.active.as_mut().map_or_else(Vec::new, |active| {
                active.next_session_check = now + SESSION_CHECK_MS;
                active.decisions.expire_sessions(&active.image, now)
            });
            if !records.is_empty() {
                ctx.event(
                    "broker_session_expired",
                    json!({ "records": records.len(), "level": "warn" }),
                );
                // A proposal that finds no leadership is dropped: the next
                // active controller expires the sessions again.
                let _ = self.controller_propose(ctx, records);
            }
        }
    }

    /// When the active controller next checks sessions.
    pub(super) fn controller_deadline(&self) -> Option<Millis> {
        self.quorum.active.as_ref().map(|a| a.next_session_check)
    }

    /// Propose records as the active controller and replay them on its
    /// image. Returns the offset they were appended at.
    pub(super) fn controller_propose(
        &mut self,
        ctx: &mut Ctx<'_>,
        records: Vec<MetadataRecord>,
    ) -> Option<i64> {
        let active = self.quorum.active.as_mut()?;
        let replay = records.clone();
        let offset = self.quorum.core.propose(ctx, records).ok()?.0;
        for record in &replay {
            active.image.apply(record);
        }
        active.last_written = offset;
        Some(offset)
    }

    /// Answer a controller request that decided `records`: once they commit
    /// when there are any, else once everything written before commits. A
    /// node that is not the active controller answers `not_controller` at
    /// once, with the message naming the controller it knows; a controller
    /// that loses its leadership before the commit answers it with
    /// [`NO_ACTIVE_CONTROLLER_MESSAGE`], as Kafka's `renounce` fails its
    /// waiting writes.
    pub(super) fn controller_write<R: Encode>(
        &mut self,
        ctx: &mut Ctx<'_>,
        req: &RequestCtx,
        records: Vec<MetadataRecord>,
        response: R,
        not_controller: impl Fn(&str) -> R,
    ) -> Outcome<R> {
        let Some(active) = &self.quorum.active else {
            return Outcome::Reply(not_controller(&self.quorum.not_controller_message()));
        };
        let epoch = active.epoch;
        let offset = if records.is_empty() {
            active.last_written
        } else {
            match self.controller_propose(ctx, records) {
                Some(offset) => offset,
                None => {
                    return Outcome::Reply(not_controller(&self.quorum.not_controller_message()));
                }
            }
        };
        if self.quorum.committed(epoch, offset) {
            return Outcome::Reply(response);
        }
        match (
            encode_reply(&response, req.version),
            encode_reply(&not_controller(NO_ACTIVE_CONTROLLER_MESSAGE), req.version),
        ) {
            (Ok(reply), Ok(not_controller)) => {
                Outcome::Hold(HoldReason::ControllerWrite(Box::new(PendingWrite {
                    epoch,
                    offset,
                    reply,
                    not_controller,
                })))
            }
            _ => Outcome::Close,
        }
    }

    /// Wrap a held controller answer in another reply shape, as an
    /// `Envelope` wraps the request it carries.
    pub(super) fn rewrap_controller_write(
        pending: PendingWrite,
        wrap: &dyn Fn(&Reply, bool) -> Option<Reply>,
    ) -> Option<PendingWrite> {
        let PendingWrite {
            epoch,
            offset,
            reply,
            not_controller,
        } = pending;
        Some(PendingWrite {
            epoch,
            offset,
            reply: wrap(&reply, false)?,
            not_controller: wrap(&not_controller, true)?,
        })
    }

    /// Run a held controller answer again: committed, refused, or waiting.
    pub(super) fn retry_controller_write(&mut self, pending: PendingWrite) -> Step {
        if self.quorum.committed(pending.epoch, pending.offset) {
            return Step::Reply(pending.reply);
        }
        let live = self
            .quorum
            .active
            .as_ref()
            .is_some_and(|a| a.epoch == pending.epoch);
        if live {
            Step::Hold(HoldReason::ControllerWrite(Box::new(pending)))
        } else {
            Step::Reply(pending.not_controller)
        }
    }
}
