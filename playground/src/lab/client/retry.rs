//! Kafka's retry classes of error codes, and its exponential backoff.
//!
//! Apache Kafka's clients do not keep a list of retriable codes. They map each
//! code to an exception class in `org.apache.kafka.common.protocol.Errors` and
//! test the class: `RetriableException` means "send again", and its subclass
//! `InvalidMetadataException` means "refresh metadata, then send again".
//! [`class`] gives the same answer for a code without the exception objects.

use crate::lab::net::Millis;

/// The retry class of one Kafka error code.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorClass {
    /// `NONE`.
    None,
    /// The exception extends `InvalidMetadataException`: refresh metadata and
    /// send again.
    InvalidMetadata,
    /// The exception extends `RetriableException` but not
    /// `InvalidMetadataException`: send again.
    Retriable,
    /// Every other code, and a code Kafka does not define.
    NotRetriable,
}

impl ErrorClass {
    /// Whether a client sends the request again after this code.
    #[must_use]
    pub const fn is_retriable(self) -> bool {
        matches!(self, Self::InvalidMetadata | Self::Retriable)
    }
}

/// The retry class of `code`, from the `Errors` table of Apache Kafka 4.3.
#[must_use]
pub const fn class(code: i16) -> ErrorClass {
    match code {
        0 => ErrorClass::None,
        // UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE,
        // NOT_LEADER_OR_FOLLOWER, REPLICA_NOT_AVAILABLE, NETWORK_EXCEPTION,
        // KAFKA_STORAGE_ERROR, LISTENER_NOT_FOUND, FENCED_LEADER_EPOCH,
        // PREFERRED_LEADER_NOT_AVAILABLE, ELIGIBLE_LEADERS_NOT_AVAILABLE,
        // ELECTION_NOT_NEEDED, UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID.
        3 | 5 | 6 | 9 | 13 | 56 | 72 | 74 | 80 | 83 | 84 | 100 | 103 => ErrorClass::InvalidMetadata,
        // CORRUPT_MESSAGE, REQUEST_TIMED_OUT, COORDINATOR_LOAD_IN_PROGRESS,
        // COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, NOT_ENOUGH_REPLICAS,
        // NOT_ENOUGH_REPLICAS_AFTER_APPEND, NOT_CONTROLLER,
        // CONCURRENT_TRANSACTIONS, FETCH_SESSION_ID_NOT_FOUND,
        // INVALID_FETCH_SESSION_EPOCH, UNKNOWN_LEADER_EPOCH,
        // OFFSET_NOT_AVAILABLE, UNSTABLE_OFFSET_COMMIT,
        // THROTTLING_QUOTA_EXCEEDED, FETCH_SESSION_TOPIC_ID_ERROR,
        // SHARE_SESSION_NOT_FOUND, INVALID_SHARE_SESSION_EPOCH,
        // SHARE_SESSION_LIMIT_REACHED.
        2 | 7 | 14 | 15 | 16 | 19 | 20 | 41 | 51 | 70 | 71 | 75 | 78 | 88 | 89 | 106 | 122
        | 123 | 133 => ErrorClass::Retriable,
        _ => ErrorClass::NotRetriable,
    }
}

/// The backoff before attempt `attempts + 1`, as Kafka's `ExponentialBackoff`
/// with multiplier 2 and jitter 0.2 computes it: `initial * 2^attempts`,
/// times a factor in `[0.8, 1.2)` chosen by `jitter` (a value in `0..400`
/// from the node's generator), and never above `max`.
#[must_use]
pub fn exponential_backoff(initial: Millis, max: Millis, attempts: u32, jitter: u64) -> Millis {
    if initial >= max {
        return max;
    }
    let term = initial.saturating_mul(1_u64 << attempts.min(40)).min(max);
    let jittered = term.saturating_mul(800 + jitter % 400) / 1000;
    jittered.min(max)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn error_classes_follow_kafkas_exception_hierarchy() {
        let cases = [
            (0, ErrorClass::None),
            (3, ErrorClass::InvalidMetadata),
            (5, ErrorClass::InvalidMetadata),
            (6, ErrorClass::InvalidMetadata),
            (13, ErrorClass::InvalidMetadata),
            (74, ErrorClass::InvalidMetadata),
            (7, ErrorClass::Retriable),
            (15, ErrorClass::Retriable),
            (16, ErrorClass::Retriable),
            (19, ErrorClass::Retriable),
            (20, ErrorClass::Retriable),
            (41, ErrorClass::Retriable),
            (1, ErrorClass::NotRetriable),
            (22, ErrorClass::NotRetriable),
            (25, ErrorClass::NotRetriable),
            (35, ErrorClass::NotRetriable),
            (45, ErrorClass::NotRetriable),
            (110, ErrorClass::NotRetriable),
            (-1, ErrorClass::NotRetriable),
            (9_999, ErrorClass::NotRetriable),
        ];
        for (code, expected) in cases {
            assert!(class(code) == expected, "code {code}");
        }
        assert!(ErrorClass::Retriable.is_retriable());
        assert!(ErrorClass::InvalidMetadata.is_retriable());
        assert!(!ErrorClass::None.is_retriable());
        assert!(!ErrorClass::NotRetriable.is_retriable());
    }

    #[test]
    fn backoff_doubles_jitters_and_caps_as_kafka_does() {
        // `jitter` 200 is the factor 1.0.
        let cases = [
            ("first", 50, 1_000, 0, 200, 50),
            ("second", 50, 1_000, 1, 200, 100),
            ("fourth", 50, 1_000, 3, 200, 400),
            ("low jitter", 50, 1_000, 3, 0, 320),
            ("high jitter", 50, 1_000, 3, 399, 479),
            ("capped", 50, 1_000, 10, 200, 1_000),
            ("capped after jitter", 50, 1_000, 5, 399, 1_000),
            ("flat when max equals initial", 100, 100, 5, 300, 100),
            ("initial above max", 2_000, 1_000, 0, 0, 1_000),
        ];
        for (name, initial, max, attempts, jitter, expected) in cases {
            assert!(
                exponential_backoff(initial, max, attempts, jitter) == expected,
                "{name}"
            );
        }
    }
}
