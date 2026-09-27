//! Request framing, response decoding, version negotiation, and the boxed
//! body and decoder a pending request carries through the client.
//!
//! A request leaves as one Kafka frame: a four-byte big-endian length, a
//! `RequestHeader` at version 1 or 2 (KIP-482: version 2 from the first
//! flexible version of the api), and the body at the negotiated version. A
//! response comes back as a length, a `ResponseHeader` at version 0 or 1, and
//! the body, decoded at the version the request was sent with.

use std::{any::Any, collections::BTreeMap};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{
    ApiKey, Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        request_header::RequestHeader, response_header::ResponseHeader,
    },
};

use super::{ClientError, CoordinatorKey, RequestId, Target};
use crate::lab::net::Millis;

/// The header version of a request sent at `version`: 2 from the first
/// flexible version of the api, 1 before it.
#[must_use]
pub fn request_header_version(flexible_min: i16, version: i16) -> i16 {
    1 + i16::from(version >= flexible_min)
}

/// The header version of the response to a request sent at `version`: 1 when
/// the body is flexible, except for `ApiVersions`, whose response header stays
/// at version 0 so a client can read the error of a version it does not speak.
#[must_use]
pub fn response_header_version(api_key: i16, flexible_min: i16, version: i16) -> i16 {
    i16::from(api_key != ApiVersionsRequest::API_KEY && version >= flexible_min)
}

/// The name of an api key, as Kafka prints it.
#[must_use]
pub fn api_name(api_key: i16) -> &'static str {
    ApiKey::from_i16(api_key).map_or("Unknown", <&'static str>::from)
}

/// A request body the client can encode at any version. This is the
/// object-safe form of [`Encode`] a boxed pending request needs.
pub trait RequestBody {
    /// Encode the body at `version`.
    ///
    /// # Errors
    /// Returns the codec error when the body cannot be encoded at `version`.
    fn encode_body(&self, buf: &mut BytesMut, version: i16) -> Result<(), ProtocolError>;
    /// The encoded size at `version`.
    fn body_len(&self, version: i16) -> usize;
}

impl<R: Encode> RequestBody for R {
    fn encode_body(&self, buf: &mut BytesMut, version: i16) -> Result<(), ProtocolError> {
        self.encode(buf, version)
    }

    fn body_len(&self, version: i16) -> usize {
        self.encoded_len(version)
    }
}

/// Decodes a response body at the version its request was sent with, into the
/// typed response behind `dyn Any`. [`super::Response::downcast`] gives it back.
pub type Decoder = Box<dyn Fn(&[u8], i16) -> Result<Box<dyn Any>, ProtocolError>>;

/// The decoder of the response to `R`.
#[must_use]
pub fn decoder_for<R>() -> Decoder
where
    R: ProtocolRequest,
    R::Response: 'static,
{
    Box::new(|bytes: &[u8], version: i16| {
        let mut cursor = bytes;
        let body = R::Response::decode(&mut cursor, version)?;
        if !cursor.is_empty() {
            return Err(ProtocolError::InvalidValue(
                "bytes after the end of the response body",
            ));
        }
        Ok(Box::new(body) as Box<dyn Any>)
    })
}

/// The constants of a request type the client needs after the type is boxed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ApiSpec {
    pub key: i16,
    pub name: &'static str,
    /// The lowest version the client can send.
    pub min: i16,
    /// The highest version the client sends: Kafka's `latestVersion(false)`.
    pub max: i16,
    /// The first flexible version, or `i16::MAX` when none is.
    pub flexible_min: i16,
}

impl ApiSpec {
    /// The constants of `R`.
    #[must_use]
    pub fn of<R: ProtocolRequest>() -> Self {
        Self {
            key: R::API_KEY,
            name: api_name(R::API_KEY),
            min: R::MIN_VERSION,
            max: R::LATEST_STABLE_VERSION,
            flexible_min: R::FLEXIBLE_MIN,
        }
    }
}

/// Why the client sent a request: for the caller, or for its own caches.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Purpose {
    /// The caller's request; its completion becomes a
    /// [`super::ClientEvent::Response`].
    User,
    /// A `Metadata` request that refreshes the cache.
    Metadata,
    /// A `FindCoordinator` lookup for one key.
    FindCoordinator(CoordinatorKey),
}

/// A request that has not completed: queued for a target, waiting for the
/// target to become known, or in flight on a connection.
pub struct Outbound {
    pub id: RequestId,
    pub purpose: Purpose,
    pub target: Target,
    pub api: ApiSpec,
    pub body: Box<dyn RequestBody>,
    pub decoder: Decoder,
    /// When the request fails with [`ClientError::Timeout`] if it was not
    /// sent by then; once sent, the connection counts its own timeout.
    pub deadline: Millis,
    /// The request expects no answer (`acks=0` produce); it completes with an
    /// empty body once written.
    pub oneway: bool,
}

impl Outbound {
    /// Box `req` with its decoder.
    pub fn new<R>(
        id: RequestId,
        purpose: Purpose,
        target: Target,
        req: R,
        deadline: Millis,
        oneway: bool,
    ) -> Self
    where
        R: ProtocolRequest + 'static,
        R::Response: 'static,
    {
        Self {
            id,
            purpose,
            target,
            api: ApiSpec::of::<R>(),
            body: Box::new(req),
            decoder: decoder_for::<R>(),
            deadline,
            oneway,
        }
    }
}

/// Encode one request frame: length prefix, header, body.
///
/// # Errors
/// Returns the codec error when the body cannot be encoded at `version`, or
/// when the frame would not fit an `i32` length.
pub fn frame_request(
    api: ApiSpec,
    version: i16,
    correlation_id: i32,
    client_id: &str,
    body: &dyn RequestBody,
) -> Result<Bytes, ProtocolError> {
    let header_version = request_header_version(api.flexible_min, version);
    let header = RequestHeader {
        request_api_key: api.key,
        request_api_version: version,
        correlation_id,
        client_id: Some(client_id.to_string()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    let len = header.encoded_len(header_version) + body.body_len(version);
    let length =
        i32::try_from(len).map_err(|_| ProtocolError::InvalidValue("request longer than 2 GiB"))?;
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_i32(length);
    header.encode(&mut buf, header_version)?;
    body.encode_body(&mut buf, version)?;
    Ok(buf.freeze())
}

/// The correlation id of a response frame, read before the header is decoded:
/// it is the first field after the length prefix in every header version.
#[must_use]
pub fn correlation_id_of(frame: &[u8]) -> Option<i32> {
    frame
        .get(4..8)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

/// Check the length prefix, decode the response header at `header_version`,
/// and return it with the body bytes that follow.
///
/// # Errors
/// Returns the codec error when the length prefix does not match the frame or
/// the header does not decode.
pub fn response_body(
    frame: &[u8],
    header_version: i16,
) -> Result<(ResponseHeader, &[u8]), ProtocolError> {
    let mut buf = frame;
    if buf.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4 - buf.remaining(),
        });
    }
    let length = usize::try_from(buf.get_i32())
        .map_err(|_| ProtocolError::InvalidValue("negative response length"))?;
    if buf.remaining() != length {
        return Err(ProtocolError::InvalidValue(
            "response length prefix does not match the frame",
        ));
    }
    let header = ResponseHeader::decode(&mut buf, header_version)?;
    Ok((header, buf))
}

/// Decode an `ApiVersions` response body sent for a request at `version`.
///
/// Kafka's `ApiVersionsResponse.parse` falls back to version 0: a broker that
/// does not support `version` answers with a version 0 `UNSUPPORTED_VERSION`
/// body.
///
/// # Errors
/// Returns the codec error when the body decodes at neither version.
pub fn decode_api_versions(
    body: &[u8],
    version: i16,
) -> Result<ApiVersionsResponse, ProtocolError> {
    let decode = |version: i16| {
        let mut cursor = body;
        let response = ApiVersionsResponse::decode(&mut cursor, version)?;
        if cursor.is_empty() {
            Ok(response)
        } else {
            Err(ProtocolError::InvalidValue(
                "bytes after the end of the ApiVersions response",
            ))
        }
    };
    match decode(version) {
        Err(_) if version != 0 => decode(0),
        result => result,
    }
}

/// The `ApiVersions` version to retry with after an `UNSUPPORTED_VERSION`
/// answer at `version`: the highest version the broker lists for
/// `ApiVersions`, or 0 when it lists none, and never `version` itself. This
/// is Kafka's `NetworkClient.handleApiVersionsResponse`.
#[must_use]
pub fn api_versions_retry_version(response: &ApiVersionsResponse, version: i16) -> i16 {
    response
        .api_keys
        .iter()
        .find(|key| key.api_key == ApiVersionsRequest::API_KEY)
        .map_or(0, |key| key.max_version)
        .min(version - 1)
        .max(0)
}

/// The version ranges one broker advertised, and the negotiation against the
/// client's own ranges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionTable {
    ranges: BTreeMap<i16, (i16, i16)>,
}

impl VersionTable {
    /// The table of a decoded `ApiVersions` response.
    #[must_use]
    pub fn from_response(response: &ApiVersionsResponse) -> Self {
        Self::from_entries(
            response
                .api_keys
                .iter()
                .map(|key| (key.api_key, key.min_version, key.max_version)),
        )
    }

    /// A table from `(api_key, min, max)` rows.
    pub fn from_entries(entries: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        Self {
            ranges: entries
                .into_iter()
                .map(|(key, min, max)| (key, (min, max)))
                .collect(),
        }
    }

    /// The version the client sends `api` at: the highest version both sides
    /// support, as Kafka's `NodeApiVersions.latestUsableVersion` picks it.
    ///
    /// # Errors
    /// Returns [`ClientError::UnsupportedVersion`] when the broker does not
    /// list the api or the ranges do not overlap.
    pub fn negotiate(&self, api: ApiSpec) -> Result<i16, ClientError> {
        let (broker_min, broker_max) = self.ranges.get(&api.key).copied().unwrap_or((0, -1));
        let chosen = api.max.min(broker_max);
        if chosen < api.min || chosen < broker_min {
            return Err(ClientError::UnsupportedVersion {
                api: api.name,
                broker_min,
                broker_max,
                client_min: api.min,
                client_max: api.max,
            });
        }
        Ok(chosen)
    }

    /// The advertised `(min, max)` of an api key.
    #[must_use]
    pub fn range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.ranges.get(&api_key).copied()
    }

    /// How many api keys the broker listed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}
