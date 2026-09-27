//! The connection pipeline: request frames in, response frames out, one
//! connection's requests answered in order.
//!
//! A [`Connection`] holds what the broker learnt from the peer and a FIFO of
//! the requests it has not answered yet. The head of the queue may be held;
//! the requests behind it wait, so a connection's answers come back in the
//! order its requests were sent, as they do from Kafka.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    ApiKey, Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{request_header::RequestHeader, response_header::ResponseHeader},
};
use thiserror::Error;

use super::dispatch::{self, HoldReason, Listener};
use crate::lab::net::Millis;

/// Kafka's `socket.request.max.bytes`: the largest frame the broker reads.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Kafka's `ClientInformation.UNKNOWN_NAME_OR_VERSION`.
pub const UNKNOWN_CLIENT_SOFTWARE: &str = "unknown";

/// Why a frame closed the connection.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameError {
    #[error("length prefix {prefix} does not match the {actual} bytes that follow it")]
    BadLengthPrefix { prefix: i64, actual: usize },
    #[error("frame of {0} bytes is larger than the {MAX_FRAME_BYTES} byte maximum")]
    TooLarge(usize),
    #[error("request of {0} bytes is shorter than a request header")]
    TooShort(usize),
    #[error("unknown api key {0}")]
    UnknownApiKey(i16),
    #[error("request header does not decode: {0}")]
    Header(String),
}

impl From<ProtocolError> for FrameError {
    fn from(error: ProtocolError) -> Self {
        Self::Header(error.to_string())
    }
}

/// One request parsed off the wire, body still encoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedRequest {
    /// The request's api.
    pub api_key: ApiKey,
    /// The request's version.
    pub version: i16,
    /// The correlation id the response echoes.
    pub correlation_id: i32,
    /// The header's `client_id`.
    pub client_id: Option<String>,
    /// The request body, still encoded.
    pub body: Bytes,
    /// The header and the body as they arrived, without the length prefix.
    pub raw: Bytes,
}

/// Split a frame into its header and body.
///
/// # Errors
/// Returns why the frame is not a Kafka request; the broker closes the
/// connection on it.
pub fn parse_request_frame(frame: &Bytes) -> Result<ParsedRequest, FrameError> {
    if frame.len() < 4 {
        return Err(FrameError::BadLengthPrefix {
            prefix: -1,
            actual: frame.len(),
        });
    }
    let prefix = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
    let body = frame.slice(4..);
    if i64::from(prefix) != i64::try_from(body.len()).unwrap_or(i64::MAX) {
        return Err(FrameError::BadLengthPrefix {
            prefix: i64::from(prefix),
            actual: body.len(),
        });
    }
    if body.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge(body.len()));
    }
    parse_request_bytes(body)
}

/// Split a request without its length prefix into its header and body: the
/// bytes an `Envelope` carries as `request_data`.
///
/// # Errors
/// Returns why the bytes are not a Kafka request.
pub fn parse_request_bytes(body: Bytes) -> Result<ParsedRequest, FrameError> {
    if body.len() < 8 {
        return Err(FrameError::TooShort(body.len()));
    }
    let raw_key = i16::from_be_bytes([body[0], body[1]]);
    let version = i16::from_be_bytes([body[2], body[3]]);
    let api_key = ApiKey::from_i16(raw_key).ok_or(FrameError::UnknownApiKey(raw_key))?;
    let header_version = dispatch::request_header_version(api_key, version);
    let mut cursor: &[u8] = &body;
    let header = RequestHeader::decode(&mut cursor, header_version)?;
    let header_len = body.len() - cursor.len();
    Ok(ParsedRequest {
        api_key,
        version,
        correlation_id: header.correlation_id,
        client_id: header.client_id,
        body: body.slice(header_len..),
        raw: body,
    })
}

/// A response frame: the length prefix, the response header at the version
/// the api and body version call for, and the body.
///
/// # Panics
/// Never: a response header encodes at every version this crate names.
#[must_use]
pub fn response_frame(api_key: ApiKey, version: i16, correlation_id: i32, body: &[u8]) -> Bytes {
    let header = ResponseHeader {
        correlation_id,
        ..ResponseHeader::default()
    };
    let header_version = dispatch::response_header_version(api_key, version);
    let header_len = header.encoded_len(header_version);
    let mut frame = BytesMut::with_capacity(4 + header_len + body.len());
    frame.put_i32(i32::try_from(header_len + body.len()).unwrap_or(i32::MAX));
    header
        .encode(&mut frame, header_version)
        .expect("response header encodes at versions 0 and 1");
    frame.put_slice(body);
    frame.freeze()
}

/// A request frame: the length prefix, the request header at the version the
/// request's flexible framing calls for (KIP-482), and the body. The
/// broker's replication links, its channels to the controller and the test
/// client all send with it.
///
/// # Errors
/// Returns the codec error when the request cannot be encoded at `version`.
pub fn request_frame<R: ProtocolRequest>(
    version: i16,
    correlation_id: i32,
    client_id: &str,
    request: &R,
) -> Result<Bytes, ProtocolError> {
    let header = RequestHeader {
        request_api_key: R::API_KEY,
        request_api_version: version,
        correlation_id,
        client_id: Some(client_id.to_string()),
        ..RequestHeader::default()
    };
    let header_version = if version >= R::FLEXIBLE_MIN { 2 } else { 1 };
    let body_len = header.encoded_len(header_version) + request.encoded_len(version);
    let mut frame = BytesMut::with_capacity(4 + body_len);
    frame.put_i32(i32::try_from(body_len).unwrap_or(i32::MAX));
    header.encode(&mut frame, header_version)?;
    request.encode(&mut frame, version)?;
    Ok(frame.freeze())
}

/// Split a response frame into its correlation id and body, reading the
/// header at the version `api_key` and `version` call for.
///
/// # Errors
/// Returns the codec error when the frame is shorter than its prefix says or
/// the header does not decode.
pub fn parse_response_frame(
    frame: &Bytes,
    api_key: ApiKey,
    version: i16,
) -> Result<(i32, Bytes), ProtocolError> {
    if frame.len() < 4 {
        return Err(ProtocolError::InvalidValue(
            "response frame shorter than its length prefix",
        ));
    }
    let prefix = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
    let body = frame.slice(4..);
    if i64::from(prefix) != i64::try_from(body.len()).unwrap_or(i64::MAX) {
        return Err(ProtocolError::InvalidValue(
            "response length prefix does not match the frame",
        ));
    }
    parse_response_bytes(&body, api_key, version)
}

/// Split a response without its length prefix into its correlation id and
/// body: the bytes an `Envelope` answer carries as `response_data`.
///
/// # Errors
/// Returns the codec error when the header does not decode.
pub fn parse_response_bytes(
    body: &Bytes,
    api_key: ApiKey,
    version: i16,
) -> Result<(i32, Bytes), ProtocolError> {
    let mut cursor: &[u8] = body;
    let header = ResponseHeader::decode(
        &mut cursor,
        dispatch::response_header_version(api_key, version),
    )?;
    let header_len = body.len() - cursor.len();
    Ok((header.correlation_id, body.slice(header_len..)))
}

/// A request waiting on its connection.
#[derive(Debug)]
pub struct QueuedRequest {
    /// The request's api.
    pub api_key: ApiKey,
    /// The request's version.
    pub version: i16,
    /// The correlation id the response echoes.
    pub correlation_id: i32,
    /// The header's `client_id`.
    pub client_id: Option<String>,
    /// The request body, still encoded.
    pub body: Bytes,
    /// The header and the body as they arrived, for forwarding.
    pub raw: Bytes,
    /// When the request arrived.
    pub received_at: Millis,
    /// Set while the request is held at the head of the queue.
    pub held: Option<HoldReason>,
}

/// The server side of one client connection.
#[derive(Debug)]
pub struct Connection {
    /// The listener the connection came in on.
    pub listener: Listener,
    /// The client id of the last request.
    pub client_id: Option<String>,
    /// KIP-511 software name and version from the first `ApiVersions` v3+.
    pub software_name: String,
    /// The KIP-511 software version.
    pub software_version: String,
    /// When the connection opened.
    pub opened_at: Millis,
    /// How many requests arrived on it.
    pub requests: u64,
    /// The requests not answered yet, oldest first; the head may be held.
    pub queue: VecDeque<QueuedRequest>,
}

impl Connection {
    /// A connection to `listener` opened at `opened_at`, with no request
    /// yet.
    #[must_use]
    pub fn new(listener: Listener, opened_at: Millis) -> Self {
        Self {
            listener,
            client_id: None,
            software_name: UNKNOWN_CLIENT_SOFTWARE.to_string(),
            software_version: UNKNOWN_CLIENT_SOFTWARE.to_string(),
            opened_at,
            requests: 0,
            queue: VecDeque::new(),
        }
    }

    /// Whether the head request is held.
    #[must_use]
    pub fn is_blocked(&self) -> bool {
        self.queue.front().is_some_and(|r| r.held.is_some())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{ProtocolRequest, owned::metadata_request::MetadataRequest};

    use super::*;

    fn frame_for(api: i16, version: i16, header_version: i16, body: &[u8]) -> Bytes {
        let header = RequestHeader {
            request_api_key: api,
            request_api_version: version,
            correlation_id: 77,
            client_id: Some("c".into()),
            ..RequestHeader::default()
        };
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        header.encode(&mut buf, header_version).unwrap();
        buf.put_slice(body);
        let len = i32::try_from(buf.len() - 4).unwrap();
        buf[..4].copy_from_slice(&len.to_be_bytes());
        buf.freeze()
    }

    #[test]
    fn parses_flexible_and_non_flexible_headers() {
        for (version, header_version) in [(8, 1), (9, 2)] {
            let body = dispatch::encode_body(&MetadataRequest::default(), version).unwrap();
            let frame = frame_for(MetadataRequest::API_KEY, version, header_version, &body);
            let parsed = parse_request_frame(&frame).unwrap();
            assert!(parsed.api_key == ApiKey::Metadata);
            assert!(parsed.version == version);
            assert!(parsed.correlation_id == 77);
            assert!(parsed.client_id.as_deref() == Some("c"));
            assert!(parsed.body == body);
        }
    }

    #[test]
    fn malformed_frames_are_refused() {
        assert!(parse_request_frame(&Bytes::from_static(&[0, 0])).is_err());
        assert!(matches!(
            parse_request_frame(&Bytes::from_static(&[0, 0, 0, 9, 1, 2, 3])),
            Err(FrameError::BadLengthPrefix {
                prefix: 9,
                actual: 3
            })
        ));
        assert!(matches!(
            parse_request_frame(&Bytes::from_static(&[0, 0, 0, 3, 0, 3, 0])),
            Err(FrameError::TooShort(3))
        ));
        let unknown = frame_for(9_999, 0, 1, &[]);
        assert!(parse_request_frame(&unknown) == Err(FrameError::UnknownApiKey(9_999)));
        let truncated = Bytes::from_static(&[0, 0, 0, 8, 0, 3, 0, 8, 0, 0, 0, 1]);
        assert!(matches!(
            parse_request_frame(&truncated),
            Err(FrameError::Header(_))
        ));
    }

    #[test]
    fn request_and_response_frames_round_trip() {
        let request = MetadataRequest::default();
        let frame = request_frame(12, 9, "test", &request).unwrap();
        let parsed = parse_request_frame(&frame).unwrap();
        assert!(parsed.api_key == ApiKey::Metadata);
        assert!(parsed.version == 12);
        assert!(parsed.correlation_id == 9);
        assert!(parsed.client_id.as_deref() == Some("test"));
        assert!(parsed.body == dispatch::encode_body(&request, 12).unwrap());
        let response = response_frame(ApiKey::Metadata, 12, 9, b"body");
        let (correlation, body) = parse_response_frame(&response, ApiKey::Metadata, 12).unwrap();
        assert!(correlation == 9);
        assert!(body.as_ref() == b"body");
    }

    #[test]
    fn response_frames_carry_the_header_the_api_calls_for() {
        let plain = response_frame(ApiKey::Metadata, 8, 5, b"xy");
        assert!(plain.as_ref() == [0, 0, 0, 6, 0, 0, 0, 5, b'x', b'y']);
        let flexible = response_frame(ApiKey::Metadata, 9, 5, b"xy");
        assert!(flexible.as_ref() == [0, 0, 0, 7, 0, 0, 0, 5, 0, b'x', b'y']);
        let api_versions = response_frame(ApiKey::ApiVersions, 3, 5, b"xy");
        assert!(api_versions == plain);
    }
}
