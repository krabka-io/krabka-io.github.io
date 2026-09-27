//! Producer ids for `InitProducerId`: Kafka's `RPCProducerIdManager`.
//!
//! The broker hands out ids from a block the active controller allocated to
//! it (`AllocateProducerIds` on the `forwarding` channel). It asks for the
//! next block once 90% of the current one is used, one request at a time,
//! and keeps the answer until the current block runs out. With no id left
//! and no next block yet, `InitProducerId` answers
//! `COORDINATOR_LOAD_IN_PROGRESS`, which a producer retries, as Kafka does
//! on its first request after a start. A failed allocation waits 50 ms
//! before the next attempt.

use krabka_protocol::owned::allocate_producer_ids_request::AllocateProducerIdsRequest;
use serde_json::{Value, json};

use super::{
    BrokerNode,
    channel::{ChannelOutcome, ControllerRequest, ControllerResponse, Purpose},
};
use crate::lab::{
    codes,
    net::{Ctx, Millis},
};

/// Kafka's `RPCProducerIdManager.RETRY_BACKOFF_MS`.
pub const PRODUCER_ID_RETRY_BACKOFF_MS: Millis = 50;

/// Kafka's `PID_PREFETCH_THRESHOLD`, in percent of a block.
pub const PRODUCER_ID_PREFETCH_PERCENT: i64 = 90;

/// A block of producer ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Block {
    /// The first id of the block.
    first: i64,
    /// How many ids it holds.
    size: i64,
    /// The next id to hand out.
    next: i64,
}

impl Block {
    /// Kafka's `ProducerIdsBlock.EMPTY`.
    const EMPTY: Self = Self {
        first: -1,
        size: 0,
        next: -1,
    };

    fn claim(&mut self) -> Option<i64> {
        (self.next < self.first + self.size).then(|| {
            let id = self.next;
            self.next += 1;
            id
        })
    }

    /// Whether the block is used past the prefetch threshold.
    fn nearly_used(&self) -> bool {
        self.next >= self.first + self.size * PRODUCER_ID_PREFETCH_PERCENT / 100
    }
}

/// The broker's producer-id blocks.
#[derive(Debug)]
pub struct ProducerIdManager {
    current: Block,
    next_block: Option<Block>,
    in_flight: bool,
    /// No request goes out before this time.
    backoff_until: Millis,
}

impl Default for ProducerIdManager {
    fn default() -> Self {
        Self {
            current: Block::EMPTY,
            next_block: None,
            in_flight: false,
            backoff_until: 0,
        }
    }
}

impl ProducerIdManager {
    /// Forget every block, as a restart does.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// The blocks for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "next_id": (self.current.size > 0).then_some(self.current.next),
            "block_end": (self.current.size > 0).then_some(self.current.first + self.current.size),
            "next_block": self.next_block.map(|b| b.first),
            "requesting": self.in_flight,
        })
    }
}

impl BrokerNode {
    /// Kafka's `generateProducerId`: the next id, or
    /// `COORDINATOR_LOAD_IN_PROGRESS` while the broker waits for a block.
    ///
    /// # Errors
    /// Returns `COORDINATOR_LOAD_IN_PROGRESS` when no id is left.
    pub fn generate_producer_id(&mut self, ctx: &mut Ctx<'_>) -> Result<i64, i16> {
        self.maybe_request_producer_ids(ctx);
        if let Some(id) = self.producer_ids.current.claim() {
            return Ok(id);
        }
        let Some(block) = self.producer_ids.next_block.take() else {
            return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
        };
        self.producer_ids.current = block;
        self.producer_ids.in_flight = false;
        let id = self
            .producer_ids
            .current
            .claim()
            .ok_or(codes::COORDINATOR_LOAD_IN_PROGRESS)?;
        self.maybe_request_producer_ids(ctx);
        Ok(id)
    }

    /// Ask the controller for the next block when the current one is nearly
    /// used and no request is out.
    fn maybe_request_producer_ids(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let ids = &mut self.producer_ids;
        if now < ids.backoff_until || ids.in_flight || ids.next_block.is_some() {
            return;
        }
        if !ids.current.nearly_used() {
            return;
        }
        ids.in_flight = true;
        ids.backoff_until = 0;
        let request = AllocateProducerIdsRequest {
            broker_id: self.config.broker_id,
            broker_epoch: self.lifecycle.broker_epoch,
            ..AllocateProducerIdsRequest::default()
        };
        self.forwarding_channel.enqueue(
            now,
            ControllerRequest::AllocateProducerIds(request),
            Purpose::ProducerIds,
        );
    }

    /// Kafka's `handleAllocateProducerIdsResponse`, and its timeout.
    pub(super) fn on_producer_ids_outcome(&mut self, ctx: &mut Ctx<'_>, outcome: &ChannelOutcome) {
        let now = ctx.now();
        let block = match outcome {
            ChannelOutcome::Response(ControllerResponse::AllocateProducerIds(response))
                if response.error_code == codes::NONE =>
            {
                let size = i64::from(response.producer_id_len);
                let valid = response.producer_id_start >= 0
                    && size > 0
                    && response.producer_id_start.checked_add(size).is_some();
                valid.then_some(Block {
                    first: response.producer_id_start,
                    size,
                    next: response.producer_id_start,
                })
            }
            ChannelOutcome::Response(ControllerResponse::AllocateProducerIds(response)) => {
                ctx.event(
                    "producer_ids_failed",
                    json!({ "error_code": response.error_code, "level": "warn" }),
                );
                None
            }
            ChannelOutcome::Response(_)
            | ChannelOutcome::TimedOut
            | ChannelOutcome::VersionMismatch => None,
        };
        if block.is_some() {
            self.producer_ids.next_block = block;
        } else {
            self.producer_ids.backoff_until = now + PRODUCER_ID_RETRY_BACKOFF_MS;
            self.producer_ids.in_flight = false;
        }
    }
}
