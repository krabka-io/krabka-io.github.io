//! The classic consumer protocol: the `ConsumerProtocolSubscription` and
//! `ConsumerProtocolAssignment` bytes of `JoinGroup` and `SyncGroup`, and
//! Kafka's `RangeAssignor`.
//!
//! Kafka's `ConsumerProtocol.serializeSubscription` writes a two-byte version
//! before the message and reads one back; both messages are sent at version
//! 3, the highest Kafka 4.3 defines.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        consumer_protocol_assignment::{self, ConsumerProtocolAssignment},
        consumer_protocol_subscription::{self, ConsumerProtocolSubscription},
    },
};

/// The version both metadata messages are sent at.
pub const PROTOCOL_VERSION: i16 = 3;

/// The protocol name of the range assignor in `JoinGroup`.
pub const RANGE_PROTOCOL: &str = "range";

/// A decoded subscription of a group member.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Subscription {
    pub topics: Vec<String>,
    pub owned: Vec<(String, i32)>,
    pub generation_id: i32,
    pub rack_id: Option<String>,
    pub user_data: Option<Bytes>,
}

fn by_topic(partitions: &[(String, i32)]) -> BTreeMap<&str, Vec<i32>> {
    let mut map: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in partitions {
        map.entry(topic.as_str()).or_default().push(*partition);
    }
    for list in map.values_mut() {
        list.sort_unstable();
    }
    map
}

/// The `JoinGroup` metadata of a member that subscribes to `topics` and owns
/// `owned`: sorted topics, owned partitions by topic, the generation, the
/// rack, and no user data (the range assignor sends none).
#[must_use]
pub fn encode_subscription(
    topics: &[String],
    owned: &[(String, i32)],
    generation_id: i32,
    rack_id: Option<&str>,
) -> Bytes {
    let mut sorted = topics.to_vec();
    sorted.sort();
    sorted.dedup();
    let message = ConsumerProtocolSubscription {
        topics: sorted,
        user_data: None,
        owned_partitions: by_topic(owned)
            .into_iter()
            .map(
                |(topic, partitions)| consumer_protocol_subscription::TopicPartition {
                    topic: topic.to_string(),
                    partitions,
                    ..Default::default()
                },
            )
            .collect(),
        generation_id,
        rack_id: rack_id.map(str::to_string),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(2 + message.encoded_len(PROTOCOL_VERSION));
    buf.put_i16(PROTOCOL_VERSION);
    // Every field of the message encodes at the version the crate generated
    // it for; the codec cannot refuse it.
    let _ = message.encode(&mut buf, PROTOCOL_VERSION);
    buf.freeze()
}

/// Decode a member's subscription bytes, at the version they name (clamped
/// to the one the client knows, as Kafka's `ConsumerProtocol` does).
#[must_use]
pub fn decode_subscription(bytes: &[u8]) -> Option<Subscription> {
    let mut cursor = bytes;
    if cursor.remaining() < 2 {
        return None;
    }
    let version = cursor.get_i16().clamp(0, PROTOCOL_VERSION);
    let message = ConsumerProtocolSubscription::decode(&mut cursor, version).ok()?;
    Some(Subscription {
        topics: message.topics,
        owned: message
            .owned_partitions
            .into_iter()
            .flat_map(|tp| {
                tp.partitions
                    .into_iter()
                    .map(move |p| (tp.topic.clone(), p))
            })
            .collect(),
        generation_id: message.generation_id,
        rack_id: message.rack_id,
        user_data: message.user_data,
    })
}

/// The `SyncGroup` assignment bytes for `partitions`.
#[must_use]
pub fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    let message = ConsumerProtocolAssignment {
        assigned_partitions: by_topic(partitions)
            .into_iter()
            .map(
                |(topic, partitions)| consumer_protocol_assignment::TopicPartition {
                    topic: topic.to_string(),
                    partitions,
                    ..Default::default()
                },
            )
            .collect(),
        user_data: None,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(2 + message.encoded_len(PROTOCOL_VERSION));
    buf.put_i16(PROTOCOL_VERSION);
    let _ = message.encode(&mut buf, PROTOCOL_VERSION);
    buf.freeze()
}

/// Decode assignment bytes into sorted `(topic, partition)` pairs.
#[must_use]
pub fn decode_assignment(bytes: &[u8]) -> Option<Vec<(String, i32)>> {
    let mut cursor = bytes;
    if cursor.remaining() < 2 {
        return None;
    }
    let version = cursor.get_i16().clamp(0, PROTOCOL_VERSION);
    let message = ConsumerProtocolAssignment::decode(&mut cursor, version).ok()?;
    let mut partitions: Vec<(String, i32)> = message
        .assigned_partitions
        .into_iter()
        .flat_map(|tp| {
            tp.partitions
                .into_iter()
                .map(move |p| (tp.topic.clone(), p))
        })
        .collect();
    partitions.sort();
    Some(partitions)
}

/// Kafka's `RangeAssignor.assignPartitions`: for each topic, the subscribed
/// members sorted by member id each take a contiguous range of partitions,
/// `partitions / members` long, and the first `partitions % members` members
/// take one more. Every member gets an entry, empty when nothing is
/// assigned to it.
#[must_use]
pub fn range_assign(
    members: &[(String, Vec<String>)],
    partition_counts: &BTreeMap<String, i32>,
) -> BTreeMap<String, Vec<(String, i32)>> {
    let mut assignment: BTreeMap<String, Vec<(String, i32)>> = members
        .iter()
        .map(|(member, _)| (member.clone(), Vec::new()))
        .collect();
    let mut subscribers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (member, topics) in members {
        for topic in topics {
            subscribers.entry(topic).or_default().push(member);
        }
    }
    for (topic, mut members) in subscribers {
        let Some(&count) = partition_counts.get(topic) else {
            continue;
        };
        if count <= 0 {
            continue;
        }
        members.sort_unstable();
        members.dedup();
        let n = i32::try_from(members.len()).unwrap_or(i32::MAX);
        let per_member = count / n;
        let extra = count % n;
        for (i, member) in members.iter().enumerate() {
            let i = i32::try_from(i).unwrap_or(i32::MAX);
            let start = per_member * i + i.min(extra);
            let length = per_member + i32::from(i < extra);
            if let Some(list) = assignment.get_mut(*member) {
                list.extend((start..start + length).map(|p| (topic.to_string(), p)));
            }
        }
    }
    for list in assignment.values_mut() {
        list.sort();
    }
    assignment
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn subscription_bytes_carry_a_version_prefix_and_round_trip() {
        let bytes = encode_subscription(
            &[s("orders"), s("audit"), s("orders")],
            &[(s("orders"), 2), (s("orders"), 0)],
            7,
            Some("rack-a"),
        );
        assert!(&bytes[..2] == [0, 3]);
        let decoded = decode_subscription(&bytes).unwrap();
        assert!(
            decoded
                == Subscription {
                    topics: vec![s("audit"), s("orders")],
                    owned: vec![(s("orders"), 0), (s("orders"), 2)],
                    generation_id: 7,
                    rack_id: Some(s("rack-a")),
                    user_data: None,
                }
        );
        assert!(decode_subscription(&[0]).is_none());
    }

    #[test]
    fn assignment_bytes_round_trip_sorted() {
        let bytes = encode_assignment(&[(s("b"), 1), (s("a"), 3), (s("a"), 0)]);
        assert!(&bytes[..2] == [0, 3]);
        assert!(decode_assignment(&bytes) == Some(vec![(s("a"), 0), (s("a"), 3), (s("b"), 1)]));
        assert!(decode_assignment(&[]).is_none());
    }

    #[test]
    fn range_assignor_matches_kafkas_ranges() {
        let counts: BTreeMap<String, i32> = [(s("t1"), 5), (s("t2"), 2), (s("t3"), 0)]
            .into_iter()
            .collect();
        let members = vec![
            (s("m2"), vec![s("t1"), s("t2")]),
            (s("m1"), vec![s("t1"), s("t2"), s("t3")]),
            (s("m3"), vec![s("t1")]),
            (s("m4"), vec![]),
        ];
        let assignment = range_assign(&members, &counts);
        let expected: BTreeMap<String, Vec<(String, i32)>> = [
            (s("m1"), vec![(s("t1"), 0), (s("t1"), 1), (s("t2"), 0)]),
            (s("m2"), vec![(s("t1"), 2), (s("t1"), 3), (s("t2"), 1)]),
            (s("m3"), vec![(s("t1"), 4)]),
            (s("m4"), vec![]),
        ]
        .into_iter()
        .collect();
        assert!(assignment == expected);
    }
}
