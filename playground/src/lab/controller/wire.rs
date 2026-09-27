//! The messages controllers exchange on their raft links.
//!
//! Every message is one [`RaftMessage`] serialized as JSON in a
//! [`Payload::Data`](crate::lab::net::Payload::Data) frame. The variants are
//! the consensus core's wire-crossing [`Event`](krabka_kraft_core::Event)s
//! with the sender left implicit in the frame, plus the fetch response, which
//! carries the log entries a follower is missing so replication happens over
//! the link instead of by copying logs. The lab does not claim KIP-595 wire
//! fidelity for these frames; the state machine behind them is the real one.

use bytes::Bytes;
use krabka_kraft_core::types::{Epoch, LogOffsetMetadata};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::log::Entry;
use crate::lab::net::NodeId;

/// A position in the log: an offset and the epoch of the entry before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPoint {
    pub epoch: Epoch,
    pub offset: i64,
}

impl From<LogOffsetMetadata> for LogPoint {
    fn from(point: LogOffsetMetadata) -> Self {
        Self {
            epoch: point.epoch,
            offset: point.offset,
        }
    }
}

impl From<LogPoint> for LogOffsetMetadata {
    fn from(point: LogPoint) -> Self {
        Self {
            offset: point.offset,
            epoch: point.epoch,
        }
    }
}

/// One message between two controllers. The frame names the sender.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RaftMessage {
    /// KIP-595 `Vote`, as a KIP-996 pre-vote or a binding vote, addressed to
    /// one voter.
    VoteRequest {
        cluster_id: Uuid,
        /// The voter the request is addressed to.
        voter_id: NodeId,
        candidate_epoch: Epoch,
        candidate: NodeId,
        last_epoch: Epoch,
        last_offset: i64,
        pre_vote: bool,
    },
    /// The answer to a `Vote`. The candidate matches it to its round by its
    /// own role, so it carries no pre-vote flag, as Kafka's does not.
    VoteResponse { epoch: Epoch, granted: bool },
    /// A leader announces its epoch.
    BeginQuorumEpoch { leader_epoch: Epoch },
    /// A resigning leader asks the voters to elect, most caught up first.
    EndQuorumEpoch {
        leader_epoch: Epoch,
        preferred_successors: Vec<NodeId>,
    },
    /// A follower or observer asks the leader for the log from
    /// `fetch_offset`, claiming its last entry belongs to `fetch_epoch`.
    Fetch {
        /// Matches the response to the request, so a stale response is
        /// dropped.
        correlation: u64,
        fetch_epoch: Epoch,
        fetch_offset: i64,
        /// The fetcher's high watermark. A leader whose own is higher answers
        /// at once rather than hold the fetch, as Kafka's does since KIP-1166.
        high_watermark: i64,
    },
    /// The answer to a `Fetch`.
    FetchResponse(FetchResponse),
}

/// The answer to a `Fetch`.
///
/// From the leader, `leader_id` is the sender, and the message carries the
/// entries from `start_offset`, or the point where the follower's log
/// diverges. From any other node, `leader_id` is that node's view of the
/// leader, or none, and the message carries no entries: it redirects the fetch
/// the way Kafka's `NOT_LEADER_OR_FOLLOWER` with `currentLeader` does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchResponse {
    pub correlation: u64,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Epoch,
    pub diverging: Option<LogPoint>,
    pub high_watermark: i64,
    /// The offset of the first entry, which is the offset the fetch named.
    pub start_offset: i64,
    pub entries: Vec<Entry>,
}

/// A frame that is not a well-formed [`RaftMessage`].
#[derive(Debug, Error)]
#[error("malformed raft frame: {0}")]
pub struct MalformedFrame(#[from] serde_json::Error);

impl RaftMessage {
    /// The frame bytes of this message.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        // Every field is a plain number, string, list or record, so the
        // serialization cannot fail.
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    /// The message a frame carries.
    ///
    /// # Errors
    /// Returns [`MalformedFrame`] when the bytes are not a message.
    pub fn decode(bytes: &[u8]) -> Result<Self, MalformedFrame> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// A short name for the timeline and the inspector.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::VoteRequest { pre_vote: true, .. } => "pre-vote",
            Self::VoteRequest { .. } => "vote",
            Self::VoteResponse { .. } => "vote-response",
            Self::BeginQuorumEpoch { .. } => "begin-epoch",
            Self::EndQuorumEpoch { .. } => "end-epoch",
            Self::Fetch { .. } => "fetch",
            Self::FetchResponse(_) => "fetch-response",
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{DeleteTopicRecord, MetadataRecord};

    use super::*;

    #[test]
    fn every_message_round_trips_through_json() {
        let messages = vec![
            RaftMessage::VoteRequest {
                cluster_id: Uuid::from_u128(9),
                voter_id: NodeId(2),
                candidate_epoch: 3,
                candidate: NodeId(1),
                last_epoch: 2,
                last_offset: 17,
                pre_vote: true,
            },
            RaftMessage::VoteResponse {
                epoch: 3,
                granted: false,
            },
            RaftMessage::BeginQuorumEpoch { leader_epoch: 4 },
            RaftMessage::EndQuorumEpoch {
                leader_epoch: 4,
                preferred_successors: vec![NodeId(3), NodeId(2)],
            },
            RaftMessage::Fetch {
                correlation: 12,
                fetch_epoch: 4,
                fetch_offset: 20,
                high_watermark: 18,
            },
            RaftMessage::FetchResponse(FetchResponse {
                correlation: 12,
                leader_id: Some(NodeId(1)),
                leader_epoch: 4,
                diverging: Some(LogPoint {
                    epoch: 3,
                    offset: 18,
                }),
                high_watermark: 18,
                start_offset: 20,
                entries: vec![Entry {
                    epoch: 4,
                    records: vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                        name: "orders".into(),
                    })],
                }],
            }),
            RaftMessage::FetchResponse(FetchResponse {
                correlation: 13,
                leader_id: None,
                leader_epoch: 4,
                diverging: None,
                high_watermark: -1,
                start_offset: 0,
                entries: vec![],
            }),
        ];
        for message in messages {
            let back = RaftMessage::decode(&message.encode()).unwrap();
            assert!(back == message);
        }
        assert!(RaftMessage::decode(b"{\"kind\":\"toast\"}").is_err());
        assert!(
            RaftMessage::BeginQuorumEpoch { leader_epoch: 1 }.encode()
                == Bytes::from_static(b"{\"kind\":\"begin_quorum_epoch\",\"leader_epoch\":1}")
        );
        // The fetch response's fields sit beside the kind tag, like every
        // other message's.
        let redirect = RaftMessage::FetchResponse(FetchResponse {
            correlation: 1,
            leader_id: None,
            leader_epoch: 2,
            diverging: None,
            high_watermark: -1,
            start_offset: 0,
            entries: vec![],
        });
        assert!(
            serde_json::from_slice::<serde_json::Value>(&redirect.encode()).unwrap()
                == serde_json::json!({
                    "kind": "fetch_response", "correlation": 1, "leader_id": null,
                    "leader_epoch": 2, "diverging": null, "high_watermark": -1,
                    "start_offset": 0, "entries": []
                })
        );
    }

    #[test]
    fn log_points_convert_both_ways() {
        let point = LogOffsetMetadata {
            offset: 5,
            epoch: 2,
        };
        assert!(LogOffsetMetadata::from(LogPoint::from(point)) == point);
    }
}
