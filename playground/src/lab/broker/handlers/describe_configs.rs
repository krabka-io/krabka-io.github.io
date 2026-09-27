//! `DescribeConfigs` (api key 32): a topic's or a broker's effective
//! configuration and where each value comes from.
//!
//! A topic reports every key of Kafka's `LogConfig`, not only its overrides,
//! with `DYNAMIC_TOPIC_CONFIG (1)` on an override, `STATIC_BROKER_CONFIG (4)`
//! on a value the broker's own config sets, and `DEFAULT_CONFIG (5)` on the
//! rest; `include_synonyms` adds the chain behind each value, and the
//! `configuration_keys` filter keeps only the named keys (an empty list asks
//! for all). A broker resource must name this broker; the empty name is the
//! cluster default and reports nothing. Every other resource type gets an
//! empty list and no error, which the JVM admin client accepts. An unknown
//! topic is `UNKNOWN_TOPIC_OR_PARTITION` and a name Kafka refuses is
//! `INVALID_TOPIC_EXCEPTION`.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        DescribeConfigsSynonym,
    },
};

use super::super::{
    BrokerConfig, BrokerNode, DEFAULT_RETENTION_MS, cluster,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// `ConfigSource` bytes of `DescribeConfigsResponse`.
pub const SOURCE_DYNAMIC_TOPIC: i8 = 1;
/// `STATIC_BROKER_CONFIG`: the broker's own config sets the value.
pub const SOURCE_STATIC_BROKER: i8 = 4;
/// `DEFAULT_CONFIG`: Kafka's default.
pub const SOURCE_DEFAULT: i8 = 5;

/// `ResourceType` bytes.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;
/// The broker resource type.
pub const RESOURCE_TYPE_BROKER: i8 = 4;

/// `ConfigDef.Type` ordinals, the `config_type` byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigType {
    Boolean = 1,
    String = 2,
    Int = 3,
    Long = 5,
    Double = 6,
    List = 7,
}

/// One key of Kafka's `LogConfig`: its type, its default, and the broker
/// config it inherits from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicConfigKey {
    /// The topic config name.
    pub name: &'static str,
    /// The value's type.
    pub config_type: ConfigType,
    /// Kafka's default value.
    pub default: &'static str,
    /// The broker config the topic config inherits from, if any.
    pub broker_key: Option<&'static str>,
}

const fn key(
    name: &'static str,
    config_type: ConfigType,
    default: &'static str,
    broker_key: Option<&'static str>,
) -> TopicConfigKey {
    TopicConfigKey {
        name,
        config_type,
        default,
        broker_key,
    }
}

/// Kafka's topic configs, sorted as `LogConfig.configKeys()` sorts them.
pub const TOPIC_CONFIGS: &[TopicConfigKey] = &[
    key(
        "cleanup.policy",
        ConfigType::List,
        "delete",
        Some("log.cleanup.policy"),
    ),
    key(
        "compression.gzip.level",
        ConfigType::Int,
        "-1",
        Some("compression.gzip.level"),
    ),
    key(
        "compression.lz4.level",
        ConfigType::Int,
        "9",
        Some("compression.lz4.level"),
    ),
    key(
        "compression.type",
        ConfigType::String,
        "producer",
        Some("compression.type"),
    ),
    key(
        "compression.zstd.level",
        ConfigType::Int,
        "3",
        Some("compression.zstd.level"),
    ),
    key(
        "delete.retention.ms",
        ConfigType::Long,
        "86400000",
        Some("log.cleaner.delete.retention.ms"),
    ),
    key(
        "file.delete.delay.ms",
        ConfigType::Long,
        "60000",
        Some("log.segment.delete.delay.ms"),
    ),
    key(
        "flush.messages",
        ConfigType::Long,
        "9223372036854775807",
        Some("log.flush.interval.messages"),
    ),
    key(
        "flush.ms",
        ConfigType::Long,
        "9223372036854775807",
        Some("log.flush.interval.ms"),
    ),
    key(
        "follower.replication.throttled.replicas",
        ConfigType::List,
        "",
        None,
    ),
    key(
        "index.interval.bytes",
        ConfigType::Int,
        "4096",
        Some("log.index.interval.bytes"),
    ),
    key(
        "leader.replication.throttled.replicas",
        ConfigType::List,
        "",
        None,
    ),
    key(
        "local.retention.bytes",
        ConfigType::Long,
        "-2",
        Some("log.local.retention.bytes"),
    ),
    key(
        "local.retention.ms",
        ConfigType::Long,
        "-2",
        Some("log.local.retention.ms"),
    ),
    key(
        "max.compaction.lag.ms",
        ConfigType::Long,
        "9223372036854775807",
        Some("log.cleaner.max.compaction.lag.ms"),
    ),
    key(
        "max.message.bytes",
        ConfigType::Int,
        "1048588",
        Some("message.max.bytes"),
    ),
    key(
        "message.timestamp.after.max.ms",
        ConfigType::Long,
        "3600000",
        Some("log.message.timestamp.after.max.ms"),
    ),
    key(
        "message.timestamp.before.max.ms",
        ConfigType::Long,
        "9223372036854775807",
        Some("log.message.timestamp.before.max.ms"),
    ),
    key(
        "message.timestamp.type",
        ConfigType::String,
        "CreateTime",
        Some("log.message.timestamp.type"),
    ),
    key(
        "min.cleanable.dirty.ratio",
        ConfigType::Double,
        "0.5",
        Some("log.cleaner.min.cleanable.ratio"),
    ),
    key(
        "min.compaction.lag.ms",
        ConfigType::Long,
        "0",
        Some("log.cleaner.min.compaction.lag.ms"),
    ),
    key(
        "min.insync.replicas",
        ConfigType::Int,
        "1",
        Some("min.insync.replicas"),
    ),
    key(
        "preallocate",
        ConfigType::Boolean,
        "false",
        Some("log.preallocate"),
    ),
    key(
        "remote.log.copy.disable",
        ConfigType::Boolean,
        "false",
        None,
    ),
    key(
        "remote.log.delete.on.disable",
        ConfigType::Boolean,
        "false",
        None,
    ),
    key("remote.storage.enable", ConfigType::Boolean, "false", None),
    key(
        "retention.bytes",
        ConfigType::Long,
        "-1",
        Some("log.retention.bytes"),
    ),
    key(
        "retention.ms",
        ConfigType::Long,
        "604800000",
        Some("log.retention.ms"),
    ),
    key(
        "segment.bytes",
        ConfigType::Int,
        "1073741824",
        Some("log.segment.bytes"),
    ),
    key(
        "segment.index.bytes",
        ConfigType::Int,
        "10485760",
        Some("log.index.size.max.bytes"),
    ),
    key(
        "segment.jitter.ms",
        ConfigType::Long,
        "0",
        Some("log.roll.jitter.ms"),
    ),
    key(
        "segment.ms",
        ConfigType::Long,
        "604800000",
        Some("log.roll.ms"),
    ),
    key(
        "unclean.leader.election.enable",
        ConfigType::Boolean,
        "false",
        Some("unclean.leader.election.enable"),
    ),
];

/// One entry of a resource's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigEntry {
    /// The config name.
    pub name: String,
    /// The effective value.
    pub value: Option<String>,
    /// Whether an alter may change it.
    pub read_only: bool,
    /// Where the value comes from, a `SOURCE_*` byte.
    pub config_source: i8,
    /// Whether the value is withheld.
    pub is_sensitive: bool,
    /// The `ConfigType` byte.
    pub config_type: i8,
    /// The chain behind the value, most specific first: `(name, value, source)`.
    pub synonyms: Vec<(String, String, i8)>,
}

/// Check the value of one topic config as `LogConfig.validate` does.
fn validate_value(key: &TopicConfigKey, value: &str) -> Result<(), String> {
    let invalid = |reason: &str| {
        format!(
            "Invalid value {value} for configuration {}: {reason}",
            key.name
        )
    };
    match key.config_type {
        ConfigType::Int => value
            .trim()
            .parse::<i32>()
            .map(|_| ())
            .map_err(|_| invalid("Not a number of type INT")),
        ConfigType::Long => value
            .trim()
            .parse::<i64>()
            .map(|_| ())
            .map_err(|_| invalid("Not a number of type LONG")),
        ConfigType::Double => value
            .trim()
            .parse::<f64>()
            .map(|_| ())
            .map_err(|_| invalid("Not a number of type DOUBLE")),
        ConfigType::Boolean => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "false" => Ok(()),
            _ => Err(invalid("Expected value to be either true or false")),
        },
        ConfigType::List if key.name == "cleanup.policy" => {
            if value
                .split(',')
                .all(|p| matches!(p.trim(), "delete" | "compact"))
            {
                Ok(())
            } else {
                Err(invalid("String must be one of: compact, delete"))
            }
        }
        ConfigType::String if key.name == "compression.type" => {
            if matches!(
                value,
                "uncompressed" | "zstd" | "lz4" | "snappy" | "gzip" | "producer"
            ) {
                Ok(())
            } else {
                Err(invalid(
                    "String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer",
                ))
            }
        }
        ConfigType::String if key.name == "message.timestamp.type" => {
            if matches!(value, "CreateTime" | "LogAppendTime") {
                Ok(())
            } else {
                Err(invalid("String must be one of: CreateTime, LogAppendTime"))
            }
        }
        ConfigType::List | ConfigType::String => Ok(()),
    }
}

/// Validate the configs a topic creation carries, in the form Kafka stores.
///
/// # Errors
/// Returns the message an `INVALID_CONFIG` row carries: a null value, an
/// unknown key, or a value of the wrong type.
pub fn validate_topic_configs(
    configs: &[(String, Option<String>)],
) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (name, value) in configs {
        let Some(value) = value else {
            return Err(format!(
                "Null value not supported for topic configs: {name}"
            ));
        };
        let Some(key) = TOPIC_CONFIGS.iter().find(|k| k.name == name) else {
            return Err(format!("Unknown topic config name: {name}"));
        };
        validate_value(key, value)?;
        out.insert(name.clone(), value.trim().to_string());
    }
    Ok(out)
}

/// The static layer the broker config supplies for a topic key, when it is
/// not Kafka's default.
fn static_layer(config: &BrokerConfig, key: &TopicConfigKey) -> Option<String> {
    match key.name {
        "retention.ms" if config.log_retention_ms != DEFAULT_RETENTION_MS => {
            Some(config.log_retention_ms.to_string())
        }
        "min.insync.replicas" if config.min_insync_replicas != 1 => {
            Some(config.min_insync_replicas.to_string())
        }
        _ => None,
    }
}

/// A topic's effective configuration: every key, with the layer its value
/// came from. `wanted` filters the keys.
pub fn effective_topic_configs(
    config: &BrokerConfig,
    overrides: Option<&BTreeMap<String, String>>,
    wanted: Option<&[String]>,
) -> Vec<ConfigEntry> {
    TOPIC_CONFIGS
        .iter()
        .filter(|key| wanted.is_none_or(|w| w.iter().any(|k| k == key.name)))
        .map(|key| {
            let mut synonyms: Vec<(String, String, i8)> = Vec::new();
            if let Some(value) = overrides.and_then(|o| o.get(key.name)) {
                synonyms.push((key.name.to_string(), value.clone(), SOURCE_DYNAMIC_TOPIC));
            }
            let broker_key = key.broker_key.unwrap_or(key.name);
            if let Some(value) = static_layer(config, key) {
                synonyms.push((broker_key.to_string(), value, SOURCE_STATIC_BROKER));
            }
            synonyms.push((
                broker_key.to_string(),
                key.default.to_string(),
                SOURCE_DEFAULT,
            ));
            let (_, value, source) = synonyms[0].clone();
            ConfigEntry {
                name: key.name.to_string(),
                value: Some(value),
                read_only: false,
                config_source: source,
                is_sensitive: false,
                config_type: key.config_type as i8,
                synonyms,
            }
        })
        .collect()
}

/// This broker's own configuration, as a `BROKER` resource reports it.
fn broker_entries(config: &BrokerConfig, wanted: Option<&[String]>) -> Vec<ConfigEntry> {
    let rows: [(&str, String, i8, bool, ConfigType); 12] = [
        (
            "auto.create.topics.enable",
            "true".into(),
            SOURCE_DEFAULT,
            false,
            ConfigType::Boolean,
        ),
        (
            "broker.id",
            config.broker_id.to_string(),
            SOURCE_STATIC_BROKER,
            true,
            ConfigType::Int,
        ),
        (
            "default.replication.factor",
            config.default_replication_factor.to_string(),
            SOURCE_STATIC_BROKER,
            false,
            ConfigType::Int,
        ),
        (
            "delete.topic.enable",
            "true".into(),
            SOURCE_DEFAULT,
            false,
            ConfigType::Boolean,
        ),
        (
            "log.message.timestamp.type",
            "CreateTime".into(),
            SOURCE_DEFAULT,
            false,
            ConfigType::String,
        ),
        (
            "log.retention.ms",
            config.log_retention_ms.to_string(),
            SOURCE_STATIC_BROKER,
            false,
            ConfigType::Long,
        ),
        (
            "message.max.bytes",
            "1048588".into(),
            SOURCE_DEFAULT,
            false,
            ConfigType::Int,
        ),
        (
            "min.insync.replicas",
            config.min_insync_replicas.to_string(),
            SOURCE_STATIC_BROKER,
            false,
            ConfigType::Int,
        ),
        (
            "num.partitions",
            config.default_partitions.to_string(),
            SOURCE_STATIC_BROKER,
            false,
            ConfigType::Int,
        ),
        (
            "offsets.topic.num.partitions",
            cluster::CONSUMER_OFFSETS_PARTITIONS.to_string(),
            SOURCE_DEFAULT,
            false,
            ConfigType::Int,
        ),
        (
            "offsets.topic.replication.factor",
            cluster::CONSUMER_OFFSETS_REPLICATION_FACTOR.to_string(),
            SOURCE_DEFAULT,
            false,
            ConfigType::Int,
        ),
        (
            "replica.lag.time.max.ms",
            config.replica_lag_time_max_ms.to_string(),
            SOURCE_STATIC_BROKER,
            false,
            ConfigType::Long,
        ),
    ];
    rows.into_iter()
        .filter(|(name, ..)| wanted.is_none_or(|w| w.iter().any(|k| k == name)))
        .map(
            |(name, value, source, read_only, config_type)| ConfigEntry {
                name: name.to_string(),
                value: Some(value.clone()),
                read_only,
                config_source: source,
                is_sensitive: false,
                config_type: config_type as i8,
                synonyms: vec![(name.to_string(), value, source)],
            },
        )
        .collect()
}

fn wire_entry(entry: ConfigEntry, include_synonyms: bool) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: entry.name,
        value: entry.value,
        read_only: entry.read_only,
        config_source: entry.config_source,
        is_sensitive: entry.is_sensitive,
        synonyms: if include_synonyms {
            entry
                .synonyms
                .into_iter()
                .map(|(name, value, source)| DescribeConfigsSynonym {
                    name,
                    value: Some(value),
                    source,
                    ..DescribeConfigsSynonym::default()
                })
                .collect()
        } else {
            Vec::new()
        },
        config_type: entry.config_type,
        documentation: None,
        ..DescribeConfigsResourceResult::default()
    }
}

fn describe_one(
    node: &BrokerNode,
    resource: &DescribeConfigsResource,
    include_synonyms: bool,
) -> DescribeConfigsResult {
    let result = |error_code: i16, message: Option<String>, configs: Vec<ConfigEntry>| {
        DescribeConfigsResult {
            error_code,
            error_message: message,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            configs: configs
                .into_iter()
                .map(|e| wire_entry(e, include_synonyms))
                .collect(),
            ..DescribeConfigsResult::default()
        }
    };
    let wanted: Option<&[String]> = resource
        .configuration_keys
        .as_deref()
        .filter(|keys| !keys.is_empty());
    match resource.resource_type {
        RESOURCE_TYPE_TOPIC => {
            if let Err(message) = cluster::validate_topic_name(&resource.resource_name) {
                return result(codes::INVALID_TOPIC_EXCEPTION, Some(message), Vec::new());
            }
            if node.image().topic(&resource.resource_name).is_none() {
                return result(codes::UNKNOWN_TOPIC_OR_PARTITION, None, Vec::new());
            }
            let overrides = node.image().topic_config(&resource.resource_name);
            result(
                codes::NONE,
                None,
                effective_topic_configs(node.config(), overrides, wanted),
            )
        }
        RESOURCE_TYPE_BROKER => {
            if resource.resource_name.is_empty() {
                return result(codes::NONE, None, Vec::new());
            }
            match resource.resource_name.parse::<i32>() {
                Ok(id) if id == node.broker_id() => {
                    result(codes::NONE, None, broker_entries(node.config(), wanted))
                }
                Ok(id) => result(
                    codes::INVALID_REQUEST,
                    Some(format!(
                        "Unexpected broker id, expected {} but received {id}",
                        node.broker_id()
                    )),
                    Vec::new(),
                ),
                Err(_) => result(
                    codes::INVALID_REQUEST,
                    Some(format!(
                        "Broker id must be an integer, but it is: {}",
                        resource.resource_name
                    )),
                    Vec::new(),
                ),
            }
        }
        _ => result(codes::NONE, None, Vec::new()),
    }
}

/// Serve a `DescribeConfigs`.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: DescribeConfigsRequest,
) -> Outcome<DescribeConfigsResponse> {
    let DescribeConfigsRequest {
        resources,
        include_synonyms,
        ..
    } = request;
    Outcome::Reply(DescribeConfigsResponse {
        results: resources
            .iter()
            .map(|resource| describe_one(node, resource, include_synonyms))
            .collect(),
        ..DescribeConfigsResponse::default()
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn topic_config_validation_follows_log_config() {
        let ok = validate_topic_configs(&[
            ("retention.ms".into(), Some("1000".into())),
            ("cleanup.policy".into(), Some("compact,delete".into())),
        ])
        .unwrap();
        assert!(ok["retention.ms"] == "1000");
        for (name, value) in [
            ("retention.ms", Some("soon")),
            ("bogus", Some("1")),
            ("retention.ms", None),
            ("cleanup.policy", Some("purge")),
            ("message.timestamp.type", Some("Never")),
            ("preallocate", Some("maybe")),
        ] {
            let error =
                validate_topic_configs(&[(name.into(), value.map(str::to_owned))]).unwrap_err();
            assert!(!error.is_empty(), "{name}");
        }
    }

    #[test]
    fn effective_configs_report_every_key_with_its_source() {
        let config = BrokerConfig {
            broker_id: 1,
            rack: None,
            voter: true,
            default_partitions: 1,
            default_replication_factor: -1,
            min_insync_replicas: 2,
            log_retention_ms: DEFAULT_RETENTION_MS,
            replica_lag_time_max_ms: 10_000,
            request_timeout_ms: 30_000,
        };
        let overrides = BTreeMap::from([("retention.ms".to_string(), "5".to_string())]);
        let entries = effective_topic_configs(&config, Some(&overrides), None);
        assert!(entries.len() == TOPIC_CONFIGS.len());
        let retention = entries.iter().find(|e| e.name == "retention.ms").unwrap();
        assert!(
            retention.value.as_deref() == Some("5")
                && retention.config_source == SOURCE_DYNAMIC_TOPIC
        );
        assert!(retention.synonyms.len() == 2 && retention.synonyms[1].0 == "log.retention.ms");
        let min_isr = entries
            .iter()
            .find(|e| e.name == "min.insync.replicas")
            .unwrap();
        assert!(
            min_isr.value.as_deref() == Some("2") && min_isr.config_source == SOURCE_STATIC_BROKER
        );
        let policy = entries.iter().find(|e| e.name == "cleanup.policy").unwrap();
        assert!(
            policy.value.as_deref() == Some("delete") && policy.config_source == SOURCE_DEFAULT
        );
        assert!(policy.config_type == ConfigType::List as i8);
        let only = effective_topic_configs(&config, None, Some(&["segment.ms".to_string()]));
        assert!(only.len() == 1 && only[0].name == "segment.ms");
    }
}
