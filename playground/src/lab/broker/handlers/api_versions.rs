//! `ApiVersions` (api key 18): the served version range of every api, and
//! the KIP-584 features.
//!
//! From v3 the request carries the KIP-511 client software name and version,
//! which must match `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`; an invalid
//! one answers `INVALID_REQUEST`. From v5 (KIP-1242) a client may name the
//! cluster and node it meant to reach, and a mismatch answers
//! `REBOOTSTRAP_REQUIRED`. A request at a version the broker does not serve is
//! answered by the dispatcher with [`unsupported_version`] at v0, as Kafka
//! does for this one api. The supported features are the metadata crate's
//! registry ([`supported_features`]); the finalized ones come from the image
//! ([`finalized_features`]).

use krabka_metadata::{MetadataImage, feature_registry, metadata_version::KRAFT_VERSION_FEATURE};
use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    api_versions_response::{ApiVersionsResponse, FinalizedFeatureKey, SupportedFeatureKey},
};

use super::super::{BrokerNode, cluster, dispatch};
use crate::lab::{codes, net::Ctx};

/// The first request version with the KIP-511 client software fields.
const CLIENT_INFO_MIN_VERSION: i16 = 3;
/// The first request version with the KIP-1242 routing identity.
const ROUTING_IDENTITY_MIN_VERSION: i16 = 5;
/// The first version whose JVM client accepts a supported minimum of zero
/// (KAFKA-17011), which `kraft.version` needs.
const ZERO_MIN_VERSION: i16 = 4;

/// The `supported_features` of an answer at `version`: every feature of the
/// registry with its supported range. A minimum of zero reads as one, as
/// older JVM clients cannot parse it, except `kraft.version` from v4.
#[must_use]
pub fn supported_features(version: i16) -> Vec<SupportedFeatureKey> {
    feature_registry()
        .iter()
        .map(|feature| {
            let (min, max) = feature.supported_range();
            SupportedFeatureKey {
                name: feature.name().to_string(),
                min_version: if feature.name() == KRAFT_VERSION_FEATURE
                    && version >= ZERO_MIN_VERSION
                {
                    min
                } else {
                    min.max(1)
                },
                max_version: max,
                ..SupportedFeatureKey::default()
            }
        })
        .collect()
}

/// The `finalized_features` of the image, Kafka's
/// `KRaftMetadataCache.features`: each finalized level as both bounds, and
/// `kraft.version` once it is above zero.
#[must_use]
pub fn finalized_features(image: &MetadataImage) -> Vec<FinalizedFeatureKey> {
    let mut features: Vec<FinalizedFeatureKey> = image
        .finalized_features()
        .iter()
        .map(|(name, level)| FinalizedFeatureKey {
            name: name.clone(),
            max_version_level: *level,
            min_version_level: *level,
            ..FinalizedFeatureKey::default()
        })
        .collect();
    let kraft_version = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
    if kraft_version > 0 {
        features.push(FinalizedFeatureKey {
            name: KRAFT_VERSION_FEATURE.to_string(),
            max_version_level: kraft_version,
            min_version_level: kraft_version,
            ..FinalizedFeatureKey::default()
        });
    }
    features
}

/// Kafka's `ApiVersionsRequest.isValid` on a KIP-511 name or version.
#[must_use]
pub fn is_valid_client_info(value: &str) -> bool {
    let bytes = value.as_bytes();
    let (Some(first), Some(last)) = (bytes.first(), bytes.last()) else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.'))
}

/// The v0 answer to an `ApiVersions` request at a version the broker does
/// not serve: `UNSUPPORTED_VERSION` with the table, so the client retries at
/// a version in it.
#[must_use]
pub fn unsupported_version() -> ApiVersionsResponse {
    ApiVersionsResponse {
        error_code: codes::UNSUPPORTED_VERSION,
        api_keys: dispatch::api_versions_table(),
        ..ApiVersionsResponse::default()
    }
}

/// Serve an `ApiVersions` at a version the broker serves.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &dispatch::RequestCtx,
    request: ApiVersionsRequest,
) -> dispatch::Outcome<ApiVersionsResponse> {
    let error_code = if req.version >= CLIENT_INFO_MIN_VERSION
        && (!is_valid_client_info(&request.client_software_name)
            || !is_valid_client_info(&request.client_software_version))
    {
        Some(codes::INVALID_REQUEST)
    } else if req.version >= ROUTING_IDENTITY_MIN_VERSION {
        match (&request.cluster_id, request.node_id) {
            (None, -1) => None,
            (Some(_), -1) | (None, _) => Some(codes::INVALID_REQUEST),
            (Some(cluster_id), node_id)
                if *cluster_id != cluster::cluster_id_string(node.image().cluster_id())
                    || node_id != node.broker_id() =>
            {
                Some(codes::REBOOTSTRAP_REQUIRED)
            }
            (Some(_), _) => None,
        }
    } else {
        None
    };
    if let Some(error_code) = error_code {
        return dispatch::Outcome::Reply(ApiVersionsResponse {
            error_code,
            ..ApiVersionsResponse::default()
        });
    }
    if req.version >= CLIENT_INFO_MIN_VERSION
        && let Some(conn) = node.conns.get_mut(&req.conn)
    {
        conn.software_name = request.client_software_name;
        conn.software_version = request.client_software_version;
    }
    dispatch::Outcome::Reply(ApiVersionsResponse {
        api_keys: dispatch::api_versions_table(),
        supported_features: supported_features(req.version),
        finalized_features_epoch: node.image().finalized_features_epoch(),
        finalized_features: finalized_features(node.image()),
        ..ApiVersionsResponse::default()
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn supported_features_clamp_a_zero_minimum_for_older_clients() {
        let kraft = |version| {
            supported_features(version)
                .into_iter()
                .find(|f| f.name == KRAFT_VERSION_FEATURE)
                .map(|f| (f.min_version, f.max_version))
        };
        assert!(kraft(3) == Some((1, 1)));
        assert!(kraft(4) == Some((0, 1)));
        assert!(
            supported_features(4)
                .iter()
                .all(|f| f.name == KRAFT_VERSION_FEATURE || f.min_version >= 1)
        );
        assert!(supported_features(4).len() == feature_registry().len());
    }

    #[test]
    fn finalized_features_follow_the_image() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        assert!(finalized_features(&image).is_empty());
        image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "group.version".into(),
                level: 1,
            },
        ));
        assert!(
            finalized_features(&image)
                == vec![FinalizedFeatureKey {
                    name: "group.version".into(),
                    max_version_level: 1,
                    min_version_level: 1,
                    ..FinalizedFeatureKey::default()
                }]
        );
    }

    #[test]
    fn client_info_rule_matches_the_jvm() {
        for (value, valid) in [
            ("apache-kafka-java", true),
            ("3.7.0", true),
            ("a", true),
            ("", false),
            ("-x", false),
            ("x-", false),
            ("a b", false),
            ("a_b", false),
        ] {
            assert!(is_valid_client_info(value) == valid, "{value:?}");
        }
    }
}
