//! HTTP/1.1 messages over the lab's frames.
//!
//! A data frame on an HTTP endpoint carries one complete request or response,
//! so the parser needs no streaming state: it reads the head with `httparse`,
//! takes the body by `Content-Length`, and reports how many bytes it consumed
//! so a frame that pipelines several requests is split in order. Both halves
//! live here: the registry parses requests and encodes responses, and a client
//! node encodes requests and parses responses with the same types.

use std::fmt::Write as _;

use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// The content type of every registry response, Confluent's vendor type.
pub const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// How many headers a message may carry before it is malformed.
const MAX_HEADERS: usize = 32;

/// Why a frame is not a well-formed HTTP message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum HttpError {
    #[error("malformed HTTP message: {0}")]
    Malformed(String),
    /// The frame ends before the head or the declared body does.
    #[error("incomplete HTTP message: the frame ends before the message does")]
    Incomplete,
    /// Only `Content-Length` framing is supported; a frame is one whole message.
    #[error("unsupported transfer encoding `{0}`; send a Content-Length")]
    UnsupportedTransferEncoding(String),
}

/// One HTTP/1.1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// The method, upper case as the client sent it.
    pub method: String,
    /// The path without the query string, percent-encoded as sent.
    pub path: String,
    /// The query parameters, percent-decoded, in order.
    pub query: Vec<(String, String)>,
    pub content_type: Option<String>,
    pub body: Bytes,
    /// The client asked for the connection to close after the response
    /// (`Connection: close`, or HTTP/1.0 without keep-alive).
    pub close: bool,
}

impl HttpRequest {
    /// A request without a body or query; the client-side builder.
    #[must_use]
    pub fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            query: Vec::new(),
            content_type: None,
            body: Bytes::new(),
            close: false,
        }
    }

    /// Add a query parameter; it is percent-encoded when the request is
    /// encoded.
    #[must_use]
    pub fn with_query(mut self, key: &str, value: &str) -> Self {
        self.query.push((key.to_string(), value.to_string()));
        self
    }

    /// Set a JSON body with the vendor content type.
    #[must_use]
    pub fn with_json<T: Serialize + ?Sized>(mut self, value: &T) -> Self {
        self.body = Bytes::from(serde_json::to_vec(value).unwrap_or_default());
        self.content_type = Some(CONTENT_TYPE.to_string());
        self
    }

    /// Parse one request from the front of `bytes`. Returns the request and
    /// the number of bytes it occupied, so a caller can parse the next one.
    ///
    /// # Errors
    /// Returns [`HttpError::Incomplete`] when the head or the declared body
    /// runs past the end of `bytes`, and [`HttpError::Malformed`] for anything
    /// `httparse` rejects or a `Content-Length` that is not a number.
    pub fn parse(bytes: &[u8]) -> Result<(Self, usize), HttpError> {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Request::new(&mut headers);
        let head_len = match parsed.parse(bytes) {
            Ok(httparse::Status::Complete(n)) => n,
            Ok(httparse::Status::Partial) => return Err(HttpError::Incomplete),
            Err(e) => return Err(HttpError::Malformed(e.to_string())),
        };
        let method = parsed
            .method
            .ok_or_else(|| HttpError::Malformed("missing method".to_string()))?
            .to_string();
        let target = parsed
            .path
            .ok_or_else(|| HttpError::Malformed("missing request target".to_string()))?;
        let (path, query) = split_target(target);
        let head = Head::read(parsed.headers, parsed.version)?;
        let end = head
            .content_length
            .and_then(|n| head_len.checked_add(n))
            .unwrap_or(head_len);
        let body = bytes.get(head_len..end).ok_or(HttpError::Incomplete)?;
        Ok((
            Self {
                method,
                path,
                query,
                content_type: head.content_type,
                body: Bytes::copy_from_slice(body),
                close: head.close,
            },
            end,
        ))
    }

    /// The path split on `/`, each segment percent-decoded, empty segments
    /// dropped: `/subjects/a%2Fb/versions` gives `["subjects", "a/b",
    /// "versions"]`.
    #[must_use]
    pub fn segments(&self) -> Vec<String> {
        self.path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| percent_decode(s, false))
            .collect()
    }

    /// The first value of a query parameter.
    #[must_use]
    pub fn query(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Whether a boolean query parameter is `true` (case-insensitive), the way
    /// Jersey reads `@QueryParam boolean`.
    #[must_use]
    pub fn flag(&self, key: &str) -> bool {
        self.query(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// The body as JSON, if it parses.
    #[must_use]
    pub fn body_json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }

    /// The wire bytes of the request: one complete HTTP/1.1 message.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = format!("{} {}", self.method, self.path);
        for (i, (k, v)) in self.query.iter().enumerate() {
            out.push(if i == 0 { '?' } else { '&' });
            out.push_str(&percent_encode(k));
            out.push('=');
            out.push_str(&percent_encode(v));
        }
        out.push_str(" HTTP/1.1\r\nHost: registry\r\n");
        if let Some(ct) = &self.content_type {
            let _ = write!(out, "Content-Type: {ct}\r\n");
        }
        let _ = write!(out, "Content-Length: {}\r\n", self.body.len());
        if self.close {
            out.push_str("Connection: close\r\n");
        }
        out.push_str("\r\n");
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(&self.body);
        Bytes::from(bytes)
    }
}

/// One HTTP/1.1 response. The content type is always [`CONTENT_TYPE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

impl HttpResponse {
    /// A response with a JSON body.
    #[must_use]
    pub fn json<T: Serialize + ?Sized>(status: u16, value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => Self { status, body },
            Err(e) => Self::error(500, 50001, format!("Error while serialising: {e}")),
        }
    }

    /// `200 OK` with a JSON body.
    #[must_use]
    pub fn ok<T: Serialize + ?Sized>(value: &T) -> Self {
        Self::json(200, value)
    }

    /// `200 OK` with a verbatim body: the raw-schema endpoints.
    #[must_use]
    pub fn raw(body: String) -> Self {
        Self { status: 200, body }
    }

    /// An error with the Confluent body `{"error_code":N,"message":"..."}`.
    #[must_use]
    pub fn error(status: u16, error_code: i32, message: impl Into<String>) -> Self {
        Self::json(
            status,
            &serde_json::json!({ "error_code": error_code, "message": message.into() }),
        )
    }

    /// The body as JSON, if it parses.
    #[must_use]
    pub fn body_json(&self) -> Option<Value> {
        serde_json::from_str(&self.body).ok()
    }

    /// The wire bytes: `HTTP/1.1 <code> <reason>`, the content type, the
    /// content length, a blank line, the body.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n\r\n",
            self.status,
            reason(self.status),
            self.body.len()
        );
        let mut bytes = head.into_bytes();
        bytes.extend_from_slice(self.body.as_bytes());
        Bytes::from(bytes)
    }

    /// Parse one response from the front of `bytes`; the client's half.
    /// Without a `Content-Length` the body runs to the end of the frame.
    ///
    /// # Errors
    /// Returns [`HttpError::Incomplete`] when the head or the declared body
    /// runs past the end of `bytes`, and [`HttpError::Malformed`] for anything
    /// `httparse` rejects.
    pub fn parse(bytes: &[u8]) -> Result<(Self, usize), HttpError> {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Response::new(&mut headers);
        let head_len = match parsed.parse(bytes) {
            Ok(httparse::Status::Complete(n)) => n,
            Ok(httparse::Status::Partial) => return Err(HttpError::Incomplete),
            Err(e) => return Err(HttpError::Malformed(e.to_string())),
        };
        let status = parsed
            .code
            .ok_or_else(|| HttpError::Malformed("missing status".to_string()))?;
        let head = Head::read(parsed.headers, parsed.version)?;
        let end = head
            .content_length
            .and_then(|n| head_len.checked_add(n))
            .unwrap_or(bytes.len());
        let body = bytes.get(head_len..end).ok_or(HttpError::Incomplete)?;
        Ok((
            Self {
                status,
                body: String::from_utf8_lossy(body).into_owned(),
            },
            end,
        ))
    }
}

/// The headers both message kinds read.
struct Head {
    content_length: Option<usize>,
    content_type: Option<String>,
    close: bool,
}

impl Head {
    fn read(headers: &[httparse::Header<'_>], version: Option<u8>) -> Result<Self, HttpError> {
        let mut head = Self {
            content_length: None,
            content_type: None,
            // HTTP/1.0 closes unless keep-alive is asked for.
            close: version == Some(0),
        };
        for header in headers {
            let value = String::from_utf8_lossy(header.value);
            let value = value.trim();
            if header.name.eq_ignore_ascii_case("content-length") {
                head.content_length = Some(value.parse().map_err(|_| {
                    HttpError::Malformed(format!("Content-Length `{value}` is not a length"))
                })?);
            } else if header.name.eq_ignore_ascii_case("content-type") {
                head.content_type = Some(value.to_string());
            } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
                if !value.eq_ignore_ascii_case("identity") {
                    return Err(HttpError::UnsupportedTransferEncoding(value.to_string()));
                }
            } else if header.name.eq_ignore_ascii_case("connection") {
                if value.eq_ignore_ascii_case("close") {
                    head.close = true;
                } else if value.eq_ignore_ascii_case("keep-alive") {
                    head.close = false;
                }
            }
        }
        Ok(head)
    }
}

/// Split a request target into the path and the decoded query pairs.
fn split_target(target: &str) -> (String, Vec<(String, String)>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let pairs = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (percent_decode(k, true), percent_decode(v, true))
        })
        .collect();
    (path.to_string(), pairs)
}

/// The reason phrase of a status code.
#[must_use]
pub fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

/// Decode `%XX` escapes; in a query string `+` is a space as well. Invalid
/// escapes are kept as written, and the result is UTF-8 with replacement.
#[must_use]
pub fn percent_decode(s: &str, query: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                // A slice through a multi-byte character is not UTF-8, so it
                // is not a valid escape either.
                let escape = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|hex| u8::from_str_radix(hex, 16).ok());
                if let Some(b) = escape {
                    out.push(b);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b'+' if query => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Encode everything but the unreserved characters as `%XX`.
#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn request_with_body_parses_and_reports_its_length() {
        let raw = b"POST /subjects/orders-value/versions?normalize=TRUE&x=a%20b+c HTTP/1.1\r\nHost: r\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\nContent-Length: 15\r\n\r\n{\"schema\":\"{}\"}";
        let (req, used) = HttpRequest::parse(raw).unwrap();
        assert!(used == raw.len());
        assert!(
            req == HttpRequest {
                method: "POST".into(),
                path: "/subjects/orders-value/versions".into(),
                query: vec![
                    ("normalize".into(), "TRUE".into()),
                    ("x".into(), "a b c".into())
                ],
                content_type: Some(CONTENT_TYPE.into()),
                body: Bytes::from_static(b"{\"schema\":\"{}\"}"),
                close: false,
            }
        );
        assert!(req.flag("normalize"));
        assert!(!req.flag("deleted"));
        assert!(req.segments() == vec!["subjects", "orders-value", "versions"]);
        assert!(req.body_json() == Some(serde_json::json!({ "schema": "{}" })));
    }

    #[test]
    fn request_without_body_or_length_has_an_empty_body() {
        let raw = b"GET /subjects/a%2Fb/ HTTP/1.1\r\nConnection: close\r\n\r\n";
        let (req, used) = HttpRequest::parse(raw).unwrap();
        assert!(used == raw.len());
        assert!(req.body.is_empty());
        assert!(req.close);
        assert!(req.segments() == vec!["subjects", "a/b"]);
        let (old, _) = HttpRequest::parse(b"GET / HTTP/1.0\r\n\r\n").unwrap();
        assert!(old.close);
        assert!(old.segments().is_empty());
    }

    #[test]
    fn pipelined_requests_parse_one_after_another() {
        let raw = b"GET /a HTTP/1.1\r\n\r\nPOST /b HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}GET /c HTTP/1.1\r\n\r\n";
        let mut at = 0;
        let mut paths = Vec::new();
        while at < raw.len() {
            let (req, used) = HttpRequest::parse(&raw[at..]).unwrap();
            paths.push(req.path);
            at += used;
        }
        assert!(paths == vec!["/a", "/b", "/c"]);
    }

    #[test]
    fn malformed_requests_are_rejected() {
        for (name, raw, expected) in [
            (
                "garbage",
                &b"\x00\x01 not http\r\n\r\n"[..],
                HttpError::Malformed(httparse::Error::Token.to_string()),
            ),
            (
                "truncated head",
                b"GET /a HTTP/1.1\r\nHost",
                HttpError::Incomplete,
            ),
            (
                "short body",
                b"POST /a HTTP/1.1\r\nContent-Length: 9\r\n\r\n{}",
                HttpError::Incomplete,
            ),
            (
                "bad length",
                b"POST /a HTTP/1.1\r\nContent-Length: nine\r\n\r\n",
                HttpError::Malformed("Content-Length `nine` is not a length".into()),
            ),
            (
                "chunked",
                b"POST /a HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                HttpError::UnsupportedTransferEncoding("chunked".into()),
            ),
        ] {
            assert!(HttpRequest::parse(raw) == Err(expected.clone()), "{name}");
        }
    }

    #[test]
    fn response_encodes_the_documented_shape_and_parses_back() {
        let resp = HttpResponse::ok(&serde_json::json!({ "id": 7 }));
        assert!(
            resp.encode()
                == Bytes::from_static(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\nContent-Length: 8\r\n\r\n{\"id\":7}"
                )
        );
        let (back, used) = HttpResponse::parse(&resp.encode()).unwrap();
        assert!(back == resp);
        assert!(used == resp.encode().len());
        let err = HttpResponse::error(404, 40401, "Subject 'x' not found.");
        assert!(err.encode().starts_with(b"HTTP/1.1 404 Not Found\r\n"));
        assert!(err.body == r#"{"error_code":40401,"message":"Subject 'x' not found."}"#);
        // Without a length the body runs to the end of the frame.
        let (tail, _) = HttpResponse::parse(b"HTTP/1.1 200 OK\r\n\r\n[1,2]").unwrap();
        assert!(tail.body == "[1,2]");
        assert!(HttpResponse::parse(b"HTTP/1.1 200").unwrap_err() == HttpError::Incomplete);
    }

    #[test]
    fn client_side_request_encodes_query_and_body() {
        let req = HttpRequest::new("POST", "/subjects/s/versions")
            .with_query("normalize", "true")
            .with_query("q", "a b/c")
            .with_json(&serde_json::json!({ "schema": "{}" }));
        let bytes = req.encode();
        let (back, used) = HttpRequest::parse(&bytes).unwrap();
        assert!(used == bytes.len());
        assert!(back == req);
        assert!(
            bytes.starts_with(b"POST /subjects/s/versions?normalize=true&q=a%20b%2Fc HTTP/1.1\r\n")
        );
    }

    #[test]
    fn percent_decoding_handles_escapes_plus_and_junk() {
        assert!(percent_decode("a%2Fb%20c", false) == "a/b c");
        assert!(percent_decode("a+b", false) == "a+b");
        assert!(percent_decode("a+b%zz%2", true) == "a b%zz%2");
        assert!(percent_encode("a b/c~d") == "a%20b%2Fc~d");
    }
}
