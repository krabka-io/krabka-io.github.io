//! The timeline: what happened in the world, in order.
//!
//! Nodes append through [`Ctx::event`](super::net::Ctx::event); the world
//! appends faults and lifecycle changes of its own. The log is bounded, so a
//! long session does not grow without limit; the page asks for the events
//! after the last index it rendered.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::net::{Millis, NodeId};

/// How many events the log keeps before it forgets the oldest.
pub const EVENT_LOG_CAPACITY: usize = 20_000;

/// One timeline entry.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Event {
    /// Position in the whole history, monotonic even after old events are
    /// forgotten.
    pub index: usize,
    pub at: Millis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeId>,
    pub kind: String,
    pub detail: serde_json::Value,
}

/// A bounded, append-only event log.
#[derive(Debug, Default)]
pub struct EventLog {
    /// The index of the oldest retained event.
    base: usize,
    items: VecDeque<Event>,
}

impl EventLog {
    pub fn push(
        &mut self,
        at: Millis,
        node: Option<NodeId>,
        kind: &str,
        detail: serde_json::Value,
    ) -> usize {
        let index = self.base + self.items.len();
        self.items.push_back(Event {
            index,
            at,
            node,
            kind: kind.to_string(),
            detail,
        });
        if self.items.len() > EVENT_LOG_CAPACITY {
            self.items.pop_front();
            self.base += 1;
        }
        index
    }

    /// The number of events ever recorded, which is also the next index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.base + self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every retained event with an index of at least `index`.
    #[must_use]
    pub fn since(&self, index: usize) -> Vec<Event> {
        let skip = index.saturating_sub(self.base);
        self.items.iter().skip(skip).cloned().collect()
    }

    /// Every retained event.
    pub fn iter(&self) -> impl Iterator<Item = &Event> {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn indexes_stay_monotonic_after_the_log_forgets_old_events() {
        let mut log = EventLog::default();
        for i in 0..(EVENT_LOG_CAPACITY + 10) {
            let index = log.push(i as Millis, None, "tick", serde_json::json!(i));
            assert!(index == i);
        }
        assert!(log.len() == EVENT_LOG_CAPACITY + 10);
        let tail = log.since(EVENT_LOG_CAPACITY + 5);
        assert!(tail.len() == 5);
        assert!(tail[0].index == EVENT_LOG_CAPACITY + 5);
        // Asking for a forgotten index returns everything retained.
        assert!(log.since(0).len() == EVENT_LOG_CAPACITY);
        assert!(log.since(0)[0].index == 10);
    }
}
