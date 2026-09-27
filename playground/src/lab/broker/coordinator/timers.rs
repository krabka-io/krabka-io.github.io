//! The coordinator's deadlines, ordered by logical time.
//!
//! Every deadline is armed with a [`TimerKey`] that says what to check when it
//! fires. The group state keeps the deadline it armed, so a key that fires
//! for an older deadline is stale and does nothing; the coordinator also
//! cancels the older deadline when it re-arms one, so
//! [`Timers::next`] is the earliest deadline that still matters.

use std::collections::BTreeMap;

use super::ids::{GroupId, MemberId};
use crate::lab::net::Millis;

/// What a fired deadline asks the coordinator to check.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TimerKey {
    /// A classic member's session, or the join timeout of a new member.
    ClassicSession { group: GroupId, member: MemberId },
    /// A `MEMBER_ID_REQUIRED` id that has not joined yet.
    ClassicPending { group: GroupId, member: MemberId },
    /// The deadline of a classic rebalance round.
    ClassicJoin { group: GroupId },
    /// The deadline for every member of a generation to send `SyncGroup`.
    ClassicSync { group: GroupId, generation: i32 },
    /// A consumer (KIP-848) member's session.
    ConsumerSession { group: GroupId, member: MemberId },
    /// The time a consumer member has to revoke its partitions.
    ConsumerRebalance { group: GroupId, member: MemberId },
    /// A streams (KIP-1071) member's session.
    StreamsSession { group: GroupId, member: MemberId },
    /// The time a streams member has to revoke its tasks.
    StreamsRebalance { group: GroupId, member: MemberId },
    /// The end of a streams group's initial rebalance delay.
    StreamsInitialRebalance { group: GroupId },
}

/// The armed deadlines.
#[derive(Default, Debug)]
pub struct Timers {
    due: BTreeMap<Millis, Vec<TimerKey>>,
}

impl Timers {
    /// Arm `key` at `at`.
    pub fn arm(&mut self, at: Millis, key: TimerKey) {
        self.due.entry(at).or_default().push(key);
    }

    /// Cancel `key` at `at`, if it is armed there.
    pub fn cancel(&mut self, at: Millis, key: &TimerKey) {
        if let Some(keys) = self.due.get_mut(&at) {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.due.remove(&at);
            }
        }
    }

    /// Cancel `key` at `old` when it was armed, and arm it at `at`.
    pub fn rearm(&mut self, old: Option<Millis>, at: Millis, key: TimerKey) {
        if let Some(old) = old {
            self.cancel(old, &key);
        }
        self.arm(at, key);
    }

    /// The earliest armed deadline.
    #[must_use]
    pub fn next(&self) -> Option<Millis> {
        self.due.keys().next().copied()
    }

    /// Take every deadline at or before `now`, earliest first.
    pub fn pop_due(&mut self, now: Millis) -> Vec<(Millis, TimerKey)> {
        let later = self.due.split_off(&(now.saturating_add(1)));
        let due = std::mem::replace(&mut self.due, later);
        due.into_iter()
            .flat_map(|(at, keys)| keys.into_iter().map(move |key| (at, key)))
            .collect()
    }

    /// The number of armed deadlines.
    #[must_use]
    pub fn len(&self) -> usize {
        self.due.values().map(Vec::len).sum()
    }

    /// Whether no deadline is armed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.due.is_empty()
    }
}
