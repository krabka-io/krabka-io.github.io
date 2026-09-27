//! The server-side `uniform` assignor of KIP-848.
//!
//! The assignor spreads the partitions of the subscribed topics over the
//! members so that every member ends within one partition of every other,
//! and keeps a partition with its current owner where that balance leaves
//! room. It is deterministic: members are visited in member id order, topics
//! in topic id order and partitions in index order.
//!
//! Members that all subscribe to the same topics get Kafka's homogeneous
//! strategy: each member keeps its current partitions up to its quota, and the
//! partitions nobody keeps fill the members with room, in member order. Any
//! other group gets the heterogeneous strategy: each member keeps the current
//! partitions of the topics it still subscribes to, an unassigned partition
//! goes to the least loaded subscriber of its topic, and a final pass moves
//! partitions from the most to the least loaded subscriber of a topic while
//! the two differ by more than one.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::{MemberId, TopicId};

/// The partitions of a member, by topic.
pub type Partitions = BTreeMap<TopicId, BTreeSet<i32>>;

/// One member as the assignor sees it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MemberSpec {
    pub id: MemberId,
    /// The topics the member subscribes to that exist.
    pub subscribed: BTreeSet<TopicId>,
    /// The member's current target assignment, for stickiness.
    pub current: Partitions,
}

/// Compute the target assignment of every member in `members` over the
/// `partitions` (partition count by topic) of the subscribed topics.
#[must_use]
pub fn assign(
    members: &[MemberSpec],
    partitions: &BTreeMap<TopicId, i32>,
) -> BTreeMap<MemberId, Partitions> {
    let mut members: Vec<&MemberSpec> = members.iter().collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    let homogeneous = members
        .windows(2)
        .all(|pair| pair[0].subscribed == pair[1].subscribed);
    let owners = if homogeneous {
        assign_homogeneous(&members, partitions)
    } else {
        assign_heterogeneous(&members, partitions)
    };
    let mut out: BTreeMap<MemberId, Partitions> = members
        .iter()
        .map(|m| (m.id.clone(), Partitions::new()))
        .collect();
    for ((topic, partition), owner) in owners {
        out.entry(owner)
            .or_default()
            .entry(topic)
            .or_default()
            .insert(partition);
    }
    out
}

/// The partitions of `topics` that exist, in topic and partition order.
fn all_partitions<'a>(
    topics: impl IntoIterator<Item = &'a TopicId>,
    partitions: &BTreeMap<TopicId, i32>,
) -> Vec<(TopicId, i32)> {
    topics
        .into_iter()
        .filter_map(|topic| partitions.get(topic).map(|count| (*topic, *count)))
        .flat_map(|(topic, count)| (0..count.max(0)).map(move |p| (topic, p)))
        .collect()
}

/// Kafka's `UniformHomogeneousAssignmentBuilder`.
fn assign_homogeneous(
    members: &[&MemberSpec],
    partitions: &BTreeMap<TopicId, i32>,
) -> BTreeMap<(TopicId, i32), MemberId> {
    let Some(first) = members.first() else {
        return BTreeMap::new();
    };
    let universe = all_partitions(&first.subscribed, partitions);
    let n = members.len();
    let base = universe.len() / n;
    let mut extras = universe.len() % n;
    let mut owners: BTreeMap<(TopicId, i32), MemberId> = BTreeMap::new();
    let mut counts: Vec<usize> = Vec::with_capacity(n);
    // Each member keeps its current partitions up to the base quota, in
    // partition order; the members that own more than the base take the
    // extra quotas first, in member order.
    let current: Vec<Vec<(TopicId, i32)>> = members
        .iter()
        .map(|m| {
            m.current
                .iter()
                .flat_map(|(topic, ps)| ps.iter().map(move |p| (*topic, *p)))
                .filter(|tp| {
                    partitions.get(&tp.0).is_some_and(|count| tp.1 < *count)
                        && first.subscribed.contains(&tp.0)
                })
                .collect()
        })
        .collect();
    let mut quotas: Vec<usize> = vec![base; n];
    for (i, owned) in current.iter().enumerate() {
        if extras > 0 && owned.len() > base {
            quotas[i] += 1;
            extras -= 1;
        }
    }
    for quota in &mut quotas {
        if extras > 0 && *quota == base {
            *quota += 1;
            extras -= 1;
        }
    }
    for (i, owned) in current.iter().enumerate() {
        let mut kept = 0;
        for tp in owned {
            if kept < quotas[i] && !owners.contains_key(tp) {
                owners.insert(*tp, members[i].id.clone());
                kept += 1;
            }
        }
        counts.push(kept);
    }
    let unassigned: Vec<(TopicId, i32)> = universe
        .into_iter()
        .filter(|tp| !owners.contains_key(tp))
        .collect();
    let mut next = unassigned.into_iter();
    for (i, member) in members.iter().enumerate() {
        while counts[i] < quotas[i] {
            let Some(tp) = next.next() else {
                return owners;
            };
            owners.insert(tp, member.id.clone());
            counts[i] += 1;
        }
    }
    owners
}

/// Kafka's `UniformHeterogeneousAssignmentBuilder`, with its balancing pass
/// reduced to a move of one partition at a time from the most to the least
/// loaded subscriber of each topic.
fn assign_heterogeneous(
    members: &[&MemberSpec],
    partitions: &BTreeMap<TopicId, i32>,
) -> BTreeMap<(TopicId, i32), MemberId> {
    let topics: BTreeSet<TopicId> = members
        .iter()
        .flat_map(|m| m.subscribed.iter().copied())
        .filter(|topic| partitions.contains_key(topic))
        .collect();
    let subscribers: BTreeMap<TopicId, Vec<usize>> = topics
        .iter()
        .map(|topic| {
            (
                *topic,
                members
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| m.subscribed.contains(topic))
                    .map(|(i, _)| i)
                    .collect(),
            )
        })
        .collect();
    let mut owners: BTreeMap<(TopicId, i32), usize> = BTreeMap::new();
    let mut loads: Vec<usize> = vec![0; members.len()];
    for (i, member) in members.iter().enumerate() {
        for (topic, ps) in &member.current {
            if !member.subscribed.contains(topic) {
                continue;
            }
            let Some(count) = partitions.get(topic) else {
                continue;
            };
            for p in ps.iter().filter(|p| **p < *count) {
                if let std::collections::btree_map::Entry::Vacant(slot) = owners.entry((*topic, *p))
                {
                    slot.insert(i);
                    loads[i] += 1;
                }
            }
        }
    }
    for tp in all_partitions(&topics, partitions) {
        if owners.contains_key(&tp) {
            continue;
        }
        let Some(least) = subscribers[&tp.0]
            .iter()
            .copied()
            .min_by_key(|i| (loads[*i], *i))
        else {
            continue;
        };
        owners.insert(tp, least);
        loads[least] += 1;
    }
    // Balance each topic: while its most loaded subscriber has two more
    // partitions than its least loaded one, move one partition over.
    for topic in &topics {
        let subs = &subscribers[topic];
        while let Some(most) = subs
            .iter()
            .copied()
            .max_by_key(|i| (loads[*i], std::cmp::Reverse(*i)))
        {
            let Some(least) = subs.iter().copied().min_by_key(|i| (loads[*i], *i)) else {
                break;
            };
            if loads[most] <= loads[least] + 1 {
                break;
            }
            let moved = owners
                .iter()
                .rev()
                .find(|((t, _), owner)| t == topic && **owner == most)
                .map(|(tp, _)| *tp);
            let Some(tp) = moved else {
                break;
            };
            owners.insert(tp, least);
            loads[most] -= 1;
            loads[least] += 1;
        }
    }
    owners
        .into_iter()
        .map(|(tp, i)| (tp, members[i].id.clone()))
        .collect()
}
