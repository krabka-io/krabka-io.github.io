//! Helpers for tests that speak to a broker over the wire: a client that
//! frames requests, decoders for the replies, and record-batch builders.
//!
//! A test injects the client's frames with `World::push_ingress` from a node
//! id the world does not host, and reads the broker's replies back from
//! `World::drain_egress`, so no client node kind is needed.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    ApiKey, Decode, ProtocolError, ProtocolRequest,
    records::{Record, RecordBatch},
};
use thiserror::Error;

use super::conn::{parse_response_frame, request_frame};
use crate::lab::net::{ConnId, Endpoint, Frame, NodeId, Payload};

/// Why a frame is not the response a test expected.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ResponseError {
    /// The frame opens or closes a connection.
    #[error("expected a data frame, got {0}")]
    NotData(String),
    /// The request type names an api key the codec does not know.
    #[error("unknown api key {0}")]
    UnknownApi(i16),
    /// The header or the body does not decode.
    #[error("the response does not decode: {0}")]
    Frame(#[from] ProtocolError),
    /// The body decoded with bytes left over.
    #[error("{0} trailing bytes after the response body")]
    TrailingBytes(usize),
}

/// A scripted Kafka client: one connection from a node id of its own.
#[derive(Clone, Debug)]
pub struct TestClient {
    /// The node id the client's frames come from; the world does not host
    /// it.
    pub node: NodeId,
    /// The client's connection.
    pub conn: ConnId,
    /// The `client_id` of every request header.
    pub client_id: String,
    next_correlation: i32,
}

impl TestClient {
    /// A client at node `node` on connection `conn`.
    #[must_use]
    pub fn new(node: u32, conn: u32) -> Self {
        Self {
            node: NodeId(node),
            conn: ConnId(conn),
            client_id: "test-client".to_string(),
            next_correlation: 0,
        }
    }

    /// The client's endpoint.
    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::client(self.node)
    }

    /// The frame that opens the connection to `broker`'s client listener.
    #[must_use]
    pub fn open(&self, broker: NodeId) -> Frame {
        self.open_to(Endpoint::kafka(broker))
    }

    /// The frame that opens the connection to a listener.
    #[must_use]
    pub fn open_to(&self, listener: Endpoint) -> Frame {
        Frame::open(self.endpoint(), listener, self.conn)
    }

    /// The frame that closes the connection to `broker`'s client listener.
    #[must_use]
    pub fn close(&self, broker: NodeId) -> Frame {
        Frame::close(self.endpoint(), Endpoint::kafka(broker), self.conn)
    }

    /// A request frame to `broker`'s client listener at `version`, with the
    /// next correlation id.
    ///
    /// # Panics
    /// Panics when the request does not encode at `version`.
    pub fn request<R: ProtocolRequest>(
        &mut self,
        broker: NodeId,
        version: i16,
        request: &R,
    ) -> Frame {
        self.request_to(Endpoint::kafka(broker), version, request)
    }

    /// A request frame to a listener at `version`, with the next correlation
    /// id: the controller listener of a broker takes the
    /// `--bootstrap-controller` requests (KIP-919).
    ///
    /// # Panics
    /// Panics when the request does not encode at `version`.
    pub fn request_to<R: ProtocolRequest>(
        &mut self,
        listener: Endpoint,
        version: i16,
        request: &R,
    ) -> Frame {
        let correlation = self.next_correlation;
        self.next_correlation += 1;
        let bytes = request_frame(version, correlation, &self.client_id, request)
            .expect("test request encodes");
        Frame::data(self.endpoint(), listener, self.conn, bytes)
    }

    /// The correlation id of the last request.
    #[must_use]
    pub fn last_correlation(&self) -> i32 {
        self.next_correlation - 1
    }
}

/// Decode a response frame to `R` at `version`: the correlation id and the
/// body.
///
/// # Errors
/// Returns why the frame is not a whole `R` response: not a data frame, a
/// header or body that does not decode, or bytes after the body.
pub fn decode_response<R: ProtocolRequest>(
    frame: &Frame,
    version: i16,
) -> Result<(i32, R::Response), ResponseError> {
    let Payload::Data(bytes) = &frame.payload else {
        return Err(ResponseError::NotData(format!("{:?}", frame.payload)));
    };
    let api_key = ApiKey::from_i16(R::API_KEY).ok_or(ResponseError::UnknownApi(R::API_KEY))?;
    let (correlation, body) = parse_response_frame(bytes, api_key, version)?;
    let mut cursor: &[u8] = &body;
    let response = R::Response::decode(&mut cursor, version)?;
    if !cursor.is_empty() {
        return Err(ResponseError::TrailingBytes(cursor.len()));
    }
    Ok((correlation, response))
}

/// A batch of `values` with `key-<i>` keys, timestamps from `base_timestamp`.
#[must_use]
pub fn batch(values: &[&str], base_timestamp: i64) -> RecordBatch {
    let last = i32::try_from(values.len()).unwrap_or(0).saturating_sub(1);
    let mut batch = RecordBatch {
        base_timestamp,
        max_timestamp: base_timestamp + i64::from(last),
        last_offset_delta: last,
        ..RecordBatch::default()
    };
    for (i, value) in values.iter().enumerate() {
        batch.records.push(Record {
            timestamp_delta: i64::try_from(i).unwrap_or(0),
            offset_delta: i32::try_from(i).unwrap_or(0),
            key: Some(Bytes::from(format!("key-{i}"))),
            value: Some(Bytes::from((*value).to_string())),
            ..Record::default()
        });
    }
    batch
}

/// A batch with an idempotent producer's identity and sequence.
#[must_use]
pub fn idempotent_batch(
    values: &[&str],
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> RecordBatch {
    RecordBatch {
        producer_id,
        producer_epoch,
        base_sequence,
        ..batch(values, 0)
    }
}

/// The wire bytes of a batch.
///
/// # Panics
/// Panics when the batch does not encode.
#[must_use]
pub fn encode_batch(batch: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).expect("batch encodes");
    buf.freeze()
}

/// Every batch of a records run.
///
/// # Panics
/// Panics when a batch does not decode.
#[must_use]
pub fn decode_batches(bytes: &[u8]) -> Vec<RecordBatch> {
    let mut cursor = bytes;
    let mut batches = Vec::new();
    while !cursor.is_empty() {
        batches.push(RecordBatch::decode(&mut cursor).expect("batch decodes"));
    }
    batches
}
