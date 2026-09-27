//! Kafka wire-level error codes the lab answers with.
//!
//! Per-(topic, partition) and per-request response fields carry these `i16`
//! values. JVM clients react to specific codes, so a substitution changes client
//! behaviour. Every constant comes from `Errors.values()` of Apache Kafka 4.3, in
//! the order and with the names the broker crate uses in its own `codes`
//! module.

#![allow(dead_code)]

pub const NONE: i16 = 0;
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const CORRUPT_MESSAGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const REPLICA_NOT_AVAILABLE: i16 = 9;
pub const MESSAGE_TOO_LARGE: i16 = 10;
pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
pub const KAFKA_STORAGE_ERROR: i16 = 56;
pub const LOG_DIR_NOT_FOUND: i16 = 57;
pub const NETWORK_EXCEPTION: i16 = 13;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const ILLEGAL_SASL_STATE: i16 = 34;
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
pub const STALE_BROKER_EPOCH: i16 = 77;
pub const BROKER_ID_NOT_REGISTERED: i16 = 102;
pub const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const NOT_CONTROLLER: i16 = 41;
pub const TOPIC_DELETION_DISABLED: i16 = 73;
pub const INVALID_REQUEST: i16 = 42;
pub const INVALID_REGULAR_EXPRESSION: i16 = 128;
pub const REBOOTSTRAP_REQUIRED: i16 = 129;
pub const INVALID_RECORD: i16 = 87;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const INVALID_GROUP_ID: i16 = 24;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const INVALID_TIMESTAMP: i16 = 32;
pub const MEMBER_ID_REQUIRED: i16 = 79;
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
pub const UNSTABLE_OFFSET_COMMIT: i16 = 88;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
pub const INVALID_TXN_STATE: i16 = 48;
pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
pub const CONCURRENT_TRANSACTIONS: i16 = 51;
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 52;
pub const PRODUCER_FENCED: i16 = 90;
pub const TRANSACTION_ABORTABLE: i16 = 120;
pub const FENCED_INSTANCE_ID: i16 = 82;
pub const STALE_MEMBER_EPOCH: i16 = 113;
pub const FENCED_MEMBER_EPOCH: i16 = 110;
pub const UNSUPPORTED_ASSIGNOR: i16 = 112;
pub const UNRELEASED_INSTANCE_ID: i16 = 111;
pub const MISMATCHED_ENDPOINT_TYPE: i16 = 114;
pub const UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;
pub const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
pub const INVALID_RECORD_STATE: i16 = 121;
pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
pub const FENCED_STATE_EPOCH: i16 = 124;
pub const STREAMS_INVALID_TOPOLOGY: i16 = 130;
pub const STREAMS_INVALID_TOPOLOGY_EPOCH: i16 = 131;
pub const STREAMS_TOPOLOGY_FENCED: i16 = 132;
pub const SHARE_SESSION_LIMIT_REACHED: i16 = 133;
pub const INVALID_CONFIG: i16 = 40;
pub const NON_EMPTY_GROUP: i16 = 68;
pub const GROUP_ID_NOT_FOUND: i16 = 69;
pub const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
pub const RESOURCE_NOT_FOUND: i16 = 91;
pub const UNACCEPTABLE_CREDENTIAL: i16 = 93;
pub const DUPLICATE_RESOURCE: i16 = 92;
pub const INVALID_UPDATE_VERSION: i16 = 95;
pub const FEATURE_UPDATE_FAILED: i16 = 96;
pub const PRINCIPAL_DESERIALIZATION_FAILURE: i16 = 97;
pub const INVALID_VOTER_KEY: i16 = 125;
pub const DUPLICATE_VOTER: i16 = 126;
pub const VOTER_NOT_FOUND: i16 = 127;
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
pub const SECURITY_DISABLED: i16 = 54;
pub const OPERATION_NOT_ATTEMPTED: i16 = 55;
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
pub const INVALID_REQUIRED_ACKS: i16 = 21;
pub const FENCED_LEADER_EPOCH: i16 = 74;
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
pub const OFFSET_NOT_AVAILABLE: i16 = 78;
pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
pub const INELIGIBLE_REPLICA: i16 = 107;
pub const OFFSET_MOVED_TO_TIERED_STORAGE: i16 = 109;
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 83;
pub const ELECTION_NOT_NEEDED: i16 = 84;
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;
pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;
pub const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;
pub const INVALID_PRINCIPAL_TYPE: i16 = 67;
pub const SNAPSHOT_NOT_FOUND: i16 = 98;
pub const POSITION_OUT_OF_RANGE: i16 = 99;
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
pub const UNKNOWN_CONTROLLER_ID: i16 = 116;
pub const INVALID_REGISTRATION: i16 = 119;
pub const UNKNOWN_TOPIC_ID: i16 = 100;
pub const INCONSISTENT_TOPIC_ID: i16 = 103;
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
pub const TELEMETRY_TOO_LARGE: i16 = 118;
pub const POLICY_VIOLATION: i16 = 44;

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn codes_the_clients_branch_on_keep_their_kafka_values() {
        assert!(NONE == 0);
        assert!(UNKNOWN_SERVER_ERROR == -1);
        assert!(UNKNOWN_TOPIC_OR_PARTITION == 3);
        assert!(NOT_LEADER_OR_FOLLOWER == 6);
        assert!(REQUEST_TIMED_OUT == 7);
        assert!(NOT_COORDINATOR == 16);
        assert!(UNSUPPORTED_VERSION == 35);
        assert!(TOPIC_ALREADY_EXISTS == 36);
        assert!(NOT_ENOUGH_REPLICAS == 19);
        assert!(NOT_ENOUGH_REPLICAS_AFTER_APPEND == 20);
        assert!(OUT_OF_ORDER_SEQUENCE_NUMBER == 45);
        assert!(UNKNOWN_MEMBER_ID == 25);
        assert!(REBALANCE_IN_PROGRESS == 27);
        assert!(FENCED_MEMBER_EPOCH == 110);
        assert!(UNRELEASED_INSTANCE_ID == 111);
    }
}
