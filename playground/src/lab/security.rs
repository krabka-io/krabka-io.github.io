//! Lab-only SSPI transport adapters. Kerberos authenticates both ends before
//! an application sees an Open; SSPI seals each subsequent message.
//! The embedded KDC and generated node identities are demonstration credentials,
//! not Windows logon credentials. Brokers see plaintext behind the adapter;
//! ACL scenarios bind its verified identity through the broker's SASL path.
//! This transport is not Kafka SASL/GSSAPI or TLS.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use kdc::config::{DomainUser, KerberosServer};
use picky_asn1::wrapper::{ExplicitContextTag0, OctetStringAsn1, Optional};
use picky_krb::messages::KdcProxyMessage;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sspi::{
    AuthIdentity, BufferType, ClientRequestFlags, Credentials, CredentialsBuffers,
    DataRepresentation, EncryptionFlags, Kerberos, KerberosConfig, SecurityBuffer,
    SecurityBufferRef, SecurityStatus, ServerRequestFlags, Sspi, SspiImpl, Username,
    generator::NetworkRequest, kerberos::ServerProperties, network_client::NetworkClient,
};

use super::net::{ConnId, Endpoint, Frame, Payload};

const REALM: &str = "LAB.KRABKA";
const TOKEN: &[u8] = b"SSPI\x01";
const SEALED: &[u8] = b"SSPI\x03";
const MAX_PENDING: usize = 100 * 1024 * 1024;
const MAX_REPLAYS: usize = 65_536;
const SKEW: i64 = 300;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecurityMode {
    #[default]
    Plaintext,
    KerberosEncrypted,
}

impl SecurityMode {
    #[must_use]
    pub fn is_plaintext(&self) -> bool {
        *self == Self::Plaintext
    }
}

type Key = (Endpoint, ConnId, Endpoint);

struct Context {
    generation: u64,
    kerberos: Kerberos,
    credentials: Option<CredentialsBuffers>,
    client: bool,
    ready: bool,
    principal: Option<String>,
    pending: VecDeque<Frame>,
    pending_bytes: usize,
    sent: u64,
    received: u64,
}

/// A decoded arrival and the transport's replies, which still cross the link.
#[derive(Default)]
pub struct Arrival {
    pub plain: Vec<Frame>,
    pub wire: Vec<Frame>,
    pub principal: Option<String>,
}

/// Node-local contexts also work when the peer is hosted in another browser.
pub struct Transport {
    pub mode: SecurityMode,
    scenario: String,
    contexts: BTreeMap<Key, Context>,
    replays: BTreeMap<[u8; 32], i64>,
    generation: u64,
}

fn name(endpoint: Endpoint) -> String {
    format!("node-{}", endpoint.node.0)
}
fn host(endpoint: Endpoint) -> String {
    format!("{}.lab.krabka", name(endpoint))
}
fn password(endpoint: Endpoint) -> String {
    format!("lab-only-{}", name(endpoint))
}

impl Transport {
    #[must_use]
    pub fn new(mode: SecurityMode, scenario: &str) -> Self {
        Self {
            mode,
            scenario: scenario.to_owned(),
            contexts: BTreeMap::new(),
            replays: BTreeMap::new(),
            generation: 0,
        }
    }

    fn service_key(&self, endpoint: Endpoint) -> Vec<u8> {
        Sha256::digest(format!(
            "krabka-lab/sspi/{}/{}",
            self.scenario,
            host(endpoint)
        ))
        .to_vec()
    }

    fn kdc(&self, client: Endpoint, server: Endpoint) -> LocalKdc {
        LocalKdc {
            host: host(server),
            config: KerberosServer {
                realm: REALM.to_owned(),
                users: vec![DomainUser {
                    username: format!("{}@{REALM}", name(client)),
                    password: password(client),
                    salt: format!("{REALM}{}", name(client)),
                }],
                max_time_skew: SKEW as u64,
                krbtgt_key: Sha256::digest(format!("krabka-lab/krbtgt/{}", self.scenario)).to_vec(),
                ticket_decryption_key: Some(self.service_key(server)),
                service_user: None,
            },
        }
    }

    fn context(&mut self, endpoint: Endpoint, client: bool) -> Result<Context, String> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or("SSPI generation exhausted")?;
        let config = KerberosConfig {
            kdc_url: Some("tcp://kdc.lab.krabka:88".parse().map_err(err)?),
            client_computer_name: host(endpoint),
        };
        let kerberos = if client {
            Kerberos::new_client_from_config(config).map_err(err)?
        } else {
            // The existing minimal KDC issues TERMSRV tickets. This is an
            // SSPI lab adapter, not a Kafka SASL service.
            let properties = ServerProperties::new(
                &["TERMSRV", &host(endpoint)],
                None,
                Duration::from_secs(SKEW as u64),
                Some(self.service_key(endpoint).into()),
            )
            .map_err(err)?;
            Kerberos::new_server_from_config(config, properties).map_err(err)?
        };
        let identity = AuthIdentity {
            username: Username::new_down_level_logon_name(&name(endpoint), REALM).map_err(err)?,
            password: password(endpoint).into(),
        };
        let credentials =
            Some(CredentialsBuffers::try_from(Credentials::AuthIdentity(identity)).map_err(err)?);
        Ok(Context {
            generation: self.generation,
            kerberos,
            credentials,
            client,
            ready: false,
            principal: None,
            pending: VecDeque::new(),
            pending_bytes: 0,
            sent: 0,
            received: 0,
        })
    }

    /// Plaintext emitted by an application. Only the returned wire frames are
    /// scheduled/captured, so application payloads cannot leak into Network.
    ///
    /// # Errors
    /// Returns an error for invalid credentials, missing contexts or queue limits.
    pub fn send(&mut self, frame: Frame) -> Result<Vec<Frame>, String> {
        if self.mode.is_plaintext() {
            return Ok(vec![frame]);
        }
        let (client, conn) = frame.conn_key();
        let key = (client, conn, frame.src);
        match &frame.payload {
            Payload::Open => {
                let mut ctx = self.context(frame.src, true)?;
                let token = ctx.initiate(None, &host(frame.dst), &self.kdc(client, frame.dst))?;
                self.contexts.insert(key, ctx);
                Ok(vec![
                    frame.clone(),
                    Frame {
                        payload: tagged(TOKEN, &token),
                        ..frame
                    },
                ])
            }
            Payload::Close => {
                self.contexts.remove(&key);
                Ok(vec![frame])
            }
            Payload::Data(_) => {
                let ctx = self
                    .contexts
                    .get_mut(&key)
                    .ok_or("SSPI connection is not open")?;
                if ctx.ready {
                    Ok(vec![ctx.protect(&frame)?])
                } else {
                    let size = frame.payload.data().map_or(0, Bytes::len);
                    if ctx.pending_bytes.saturating_add(size) > MAX_PENDING {
                        return Err("SSPI handshake queue exceeds 100 MiB".to_owned());
                    }
                    ctx.pending_bytes += size;
                    ctx.pending.push_back(frame);
                    Ok(Vec::new())
                }
            }
        }
    }

    /// Wire bytes arriving at the node's adapter. An unverified peer never
    /// opens the underlying listener; malformed/tampered messages fail closed.
    ///
    /// # Errors
    /// Returns an error for authentication, MAC, replay or framing failures.
    pub fn receive(&mut self, frame: Frame) -> Result<Arrival, String> {
        if self.mode.is_plaintext() {
            return Ok(Arrival {
                plain: vec![frame],
                ..Arrival::default()
            });
        }
        let (client, conn) = frame.conn_key();
        let key = (client, conn, frame.dst);
        match &frame.payload {
            Payload::Open => {
                let context = self.context(frame.dst, false)?;
                self.contexts.insert(key, context);
                Ok(Arrival::default())
            }
            Payload::Close => {
                self.contexts.remove(&key);
                Ok(Arrival {
                    plain: vec![frame],
                    ..Arrival::default()
                })
            }
            Payload::Data(bytes) => {
                if bytes.starts_with(TOKEN) && frame.dst.is_listener() {
                    let hash: [u8; 32] = Sha256::digest(&bytes[TOKEN.len()..]).into();
                    let now = time::OffsetDateTime::now_utc().unix_timestamp();
                    // An accepted authenticator may be SKEW seconds in the
                    // future, then remain valid for another SKEW seconds.
                    self.replays.retain(|_, at| *at >= now - 2 * SKEW);
                    if self.replays.len() >= MAX_REPLAYS || self.replays.insert(hash, now).is_some()
                    {
                        return Err("SSPI authenticator replay or replay cache full".to_owned());
                    }
                }
                let ctx = self
                    .contexts
                    .get_mut(&key)
                    .ok_or("SSPI connection is not open")?;
                if let Some(token) = bytes.strip_prefix(TOKEN) {
                    if ctx.ready {
                        return Err("SSPI authentication already complete".to_owned());
                    }
                    let mut result = Arrival::default();
                    if ctx.client {
                        let kdc = LocalKdc::unused(); // AP-REP verification never requests the KDC.
                        let out = ctx.initiate(Some(token), &host(frame.src), &kdc)?;
                        if !ctx.ready || !out.is_empty() {
                            return Err("unexpected SSPI continuation".to_owned());
                        }
                        result.principal = Some(format!("TERMSRV/{}", host(frame.src)));
                        while let Some(pending) = ctx.pending.pop_front() {
                            result.wire.push(ctx.protect(&pending)?);
                        }
                        ctx.pending_bytes = 0;
                    } else {
                        let out = ctx.accept(token)?;
                        if !ctx.ready {
                            return Err("unexpected SSPI continuation".to_owned());
                        }
                        let principal = ctx.kerberos.query_context_names().map_err(err)?.username;
                        if !principal
                            .inner()
                            .eq_ignore_ascii_case(&format!("{}@{REALM}", name(client)))
                            && !principal
                                .inner()
                                .eq_ignore_ascii_case(&format!("{REALM}\\{}", name(client)))
                        {
                            return Err(
                                "Kerberos principal does not match the sending node".to_owned()
                            );
                        }
                        ctx.principal = Some(format!("{}@{REALM}", name(client)));
                        result.principal.clone_from(&ctx.principal);
                        result.wire.push(frame.reply(tagged(TOKEN, &out)));
                        result.plain.push(Frame::open(frame.src, frame.dst, conn));
                    }
                    Ok(result)
                } else {
                    if !ctx.ready {
                        return Err("SSPI data before authentication".to_owned());
                    }
                    let token = bytes
                        .strip_prefix(SEALED)
                        .ok_or("SSPI protection mode mismatch")?;
                    let plain = ctx.unprotect(token)?;
                    Ok(Arrival {
                        plain: vec![Frame::data(frame.src, frame.dst, conn, plain)],
                        ..Arrival::default()
                    })
                }
            }
        }
    }

    pub fn forget_node(&mut self, node: super::net::NodeId) {
        self.contexts
            .retain(|(client, _, local), _| client.node != node && local.node != node);
    }

    /// The accepted identity, only after verification at the receiving node.
    #[must_use]
    pub fn principal(&self, frame: &Frame) -> Option<String> {
        let (client, conn) = frame.conn_key();
        self.contexts
            .get(&(client, conn, frame.dst))
            .and_then(|ctx| ctx.principal.clone())
    }

    /// The pending context incarnation, for a lab-clock setup timeout.
    #[must_use]
    pub fn pending_generation(&self, frame: &Frame, local: Endpoint) -> Option<u64> {
        let (client, conn) = frame.conn_key();
        self.contexts
            .get(&(client, conn, local))
            .filter(|ctx| !ctx.ready)
            .map(|ctx| ctx.generation)
    }
}

impl Context {
    fn initiate(
        &mut self,
        token: Option<&[u8]>,
        server: &str,
        kdc: &LocalKdc,
    ) -> Result<Vec<u8>, String> {
        let mut input = vec![SecurityBuffer::new(
            token.unwrap_or_default().to_vec(),
            BufferType::Token,
        )];
        let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
        let target = format!("TERMSRV/{server}");
        let mut builder = self
            .kerberos
            .initialize_security_context()
            .with_credentials_handle(&mut self.credentials)
            .with_context_requirements(
                ClientRequestFlags::MUTUAL_AUTH
                    | ClientRequestFlags::INTEGRITY
                    | ClientRequestFlags::CONFIDENTIALITY
                    | ClientRequestFlags::SEQUENCE_DETECT
                    | ClientRequestFlags::REPLAY_DETECT,
            )
            .with_target_data_representation(DataRepresentation::Native)
            .with_target_name(&target)
            .with_input(&mut input)
            .with_output(&mut output);
        let result = self
            .kerberos
            .initialize_security_context_impl(&mut builder)
            .map_err(err)?
            .resolve_with_client(kdc)
            .map_err(err)?;
        self.ready = result.status == SecurityStatus::Ok;
        Ok(output.remove(0).buffer)
    }

    fn accept(&mut self, token: &[u8]) -> Result<Vec<u8>, String> {
        let mut input = vec![SecurityBuffer::new(token.to_vec(), BufferType::Token)];
        let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
        let builder = self
            .kerberos
            .accept_security_context()
            .with_credentials_handle(&mut self.credentials)
            .with_context_requirements(
                ServerRequestFlags::MUTUAL_AUTH
                    | ServerRequestFlags::INTEGRITY
                    | ServerRequestFlags::CONFIDENTIALITY
                    | ServerRequestFlags::SEQUENCE_DETECT
                    | ServerRequestFlags::REPLAY_DETECT,
            )
            .with_target_data_representation(DataRepresentation::Native)
            .with_input(&mut input)
            .with_output(&mut output);
        let result = self
            .kerberos
            .accept_security_context_impl(builder)
            .map_err(err)?
            .resolve_to_result()
            .map_err(err)?;
        self.ready = result.status == SecurityStatus::Ok;
        Ok(output.remove(0).buffer)
    }

    fn protect(&mut self, frame: &Frame) -> Result<Frame, String> {
        let application = frame.payload.data().ok_or("missing application bytes")?;
        if application.len() > MAX_PENDING {
            return Err("SSPI record exceeds 100 MiB".to_owned());
        }
        let size = self
            .kerberos
            .query_context_sizes()
            .map_err(err)?
            .security_trailer as usize;
        let mut token = vec![0; size];
        // Bind the complete message to a monotonically increasing number.
        // SSPI verifies its MAC; this check independently rejects replay/order.
        let mut data = self.sent.to_be_bytes().to_vec();
        data.extend_from_slice(application);
        let mut buffers = [
            SecurityBufferRef::token_buf(&mut token),
            SecurityBufferRef::data_buf(&mut data),
        ];
        self.kerberos
            .encrypt_message(EncryptionFlags::empty(), &mut buffers)
            .map_err(err)?;
        let mut bytes = buffers[0].data().to_vec();
        bytes.extend_from_slice(buffers[1].data());
        self.sent = self.sent.checked_add(1).ok_or("SSPI sequence exhausted")?;
        Ok(Frame {
            payload: tagged(SEALED, &bytes),
            src: frame.src,
            dst: frame.dst,
            conn: frame.conn,
        })
    }

    fn unprotect(&mut self, bytes: &[u8]) -> Result<Bytes, String> {
        if bytes.len() > MAX_PENDING + 1024 {
            return Err("SSPI record exceeds 100 MiB".to_owned());
        }
        // RFC 4121 WRAP header, Sealed flag. This SSPI revision returns empty
        // DecryptionFlags even for signed records. Verify the token first,
        // then require the actual protection rather than trusting our envelope.
        let sealed = bytes.get(2).is_some_and(|flags| flags & 0b10 != 0);
        let mut bytes = bytes.to_vec();
        let mut buffers = [
            SecurityBufferRef::stream_buf(&mut bytes),
            SecurityBufferRef::data_buf(&mut []),
        ];
        self.kerberos.decrypt_message(&mut buffers).map_err(err)?;
        if !sealed {
            return Err("SSPI record must provide confidentiality".to_owned());
        }
        let data = buffers[1].data();
        let number = data.get(..8).ok_or("truncated SSPI sequence")?;
        if number != self.received.to_be_bytes() {
            return Err("SSPI message replay or out of order".to_owned());
        }
        self.received = self
            .received
            .checked_add(1)
            .ok_or("SSPI sequence exhausted")?;
        Ok(Bytes::copy_from_slice(&data[8..]))
    }
}

struct LocalKdc {
    config: KerberosServer,
    host: String,
}

impl LocalKdc {
    fn unused() -> Self {
        Self {
            host: String::new(),
            config: KerberosServer {
                realm: REALM.to_owned(),
                users: Vec::new(),
                max_time_skew: SKEW as u64,
                krbtgt_key: Vec::new(),
                ticket_decryption_key: None,
                service_user: None,
            },
        }
    }
}

impl NetworkClient for LocalKdc {
    fn send(&self, request: &NetworkRequest) -> sspi::Result<Vec<u8>> {
        if self.host.is_empty() {
            return Err(sspi::Error::new(
                sspi::ErrorKind::InternalError,
                "unexpected KDC request after AP-REP",
            ));
        }
        let message = KdcProxyMessage {
            kerb_message: ExplicitContextTag0::from(OctetStringAsn1::from(request.data.clone())),
            target_domain: Optional::default(),
            dclocator_hint: Optional::default(),
        };
        Ok(
            kdc::handle_kdc_proxy_message(message, &self.config, &self.host)?
                .kerb_message
                .0
                .0,
        )
    }
}

fn tagged(prefix: &[u8], bytes: &[u8]) -> Payload {
    let mut data = prefix.to_vec();
    data.extend_from_slice(bytes);
    Payload::Data(data.into())
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[must_use]
pub fn wire_label(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(TOKEN) {
        Some("Kerberos token")
    } else if bytes.starts_with(SEALED) {
        Some("SSPI encrypted")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab::net::NodeId;

    fn open() -> Frame {
        Frame::open(
            Endpoint::client(NodeId(2)),
            Endpoint::kafka(NodeId(1)),
            ConnId(7),
        )
    }

    fn connected(mode: SecurityMode) -> (Transport, Transport) {
        let mut client = Transport::new(mode, "test");
        let mut server = Transport::new(mode, "test");
        let mut frames = client.send(open()).unwrap();
        assert!(server.receive(frames.remove(0)).unwrap().plain.is_empty());
        // A Kafka request queued before the mutual handshake must wait.
        assert!(
            client
                .send(Frame {
                    payload: Payload::Data(Bytes::from_static(b"private record")),
                    ..open()
                })
                .unwrap()
                .is_empty()
        );
        let accepted = server.receive(frames.remove(0)).unwrap();
        assert_eq!(accepted.plain, vec![open()]);
        assert!(accepted.principal.unwrap().contains("node-2"));
        let ready = client
            .receive(accepted.wire.into_iter().next().unwrap())
            .unwrap();
        assert!(ready.principal.is_some());
        assert_eq!(ready.wire.len(), 1);
        let record = ready.wire.into_iter().next().unwrap();
        if mode == SecurityMode::KerberosEncrypted {
            assert!(
                !record
                    .payload
                    .data()
                    .unwrap()
                    .windows(14)
                    .any(|bytes| bytes == b"private record")
            );
        }
        assert_eq!(
            server.receive(record.clone()).unwrap().plain[0]
                .payload
                .data()
                .unwrap()
                .as_ref(),
            b"private record"
        );
        assert!(server.receive(record).is_err(), "replay must fail");
        (client, server)
    }

    #[test]
    fn kerberos_mutual_authentication_encryption_and_fail_closed() {
        let mode = SecurityMode::KerberosEncrypted;
        let (mut client, mut server) = connected(mode);
        // Reply direction, including a payload larger than the capture.
        let reply = Frame::data(
            open().dst,
            open().src,
            open().conn,
            Bytes::from(vec![42; 70_000]),
        );
        let wire = server.send(reply.clone()).unwrap().remove(0);
        assert_eq!(client.receive(wire).unwrap().plain, vec![reply]);
        // A valid signed-only Kerberos token, disguised by the sealed envelope,
        // must fail our confidentiality requirement even though SSPI accepts it.
        use picky_krb::{
            constants::key_usages::INITIATOR_SEAL,
            crypto::aes::{AesSize, checksum_sha_aes},
            gss_api::WrapToken,
        };
        let keys = client
            .contexts
            .get(&(open().src, open().conn, open().src))
            .unwrap()
            .kerberos
            .query_context_session_key()
            .unwrap();
        let mut wrap = WrapToken::with_seq_number(1);
        wrap.flags &= !0b10;
        let mut clear = 1_u64.to_be_bytes().to_vec();
        clear.extend_from_slice(b"signed plaintext");
        let mut input = clear.clone();
        input.extend_from_slice(&wrap.header());
        let checksum = checksum_sha_aes(
            keys.session_key.as_ref(),
            INITIATOR_SEAL,
            &input,
            &AesSize::Aes256,
        )
        .unwrap();
        wrap.ec = u16::try_from(checksum.len()).unwrap();
        clear.extend_from_slice(&checksum);
        wrap.checksum = clear;
        let mut signed = Vec::new();
        wrap.encode(&mut signed).unwrap();
        let misleading = Frame {
            payload: tagged(SEALED, &signed),
            ..open()
        };
        assert_eq!(
            server.receive(misleading).err().unwrap(),
            "SSPI record must provide confidentiality"
        );
        let mut wrong_mode = client
            .send(Frame::data(
                open().src,
                open().dst,
                open().conn,
                Bytes::from_static(b"wrong mode"),
            ))
            .unwrap()
            .remove(0);
        let mut bytes = wrong_mode.payload.data().unwrap().to_vec();
        bytes[4] = 2;
        wrong_mode.payload = Payload::Data(bytes.into());
        assert!(
            server.receive(wrong_mode).is_err(),
            "a signed envelope must be rejected"
        );
        let mut wire = client
            .send(Frame {
                payload: Payload::Data(Bytes::from_static(b"next")),
                ..open()
            })
            .unwrap()
            .remove(0);
        if let Payload::Data(bytes) = &mut wire.payload {
            let mut data = bytes.to_vec();
            let last = data.len() - 1;
            data[last] ^= 1;
            *bytes = data.into();
        }
        assert!(
            server.receive(wire).is_err(),
            "modified ciphertext/signature must fail"
        );

        let mut impostor = Transport::new(mode, "another-cluster");
        let mut client = Transport::new(mode, "test");
        let mut frames = client.send(open()).unwrap();
        impostor.receive(frames.remove(0)).unwrap();
        assert!(
            impostor.receive(frames.remove(0)).is_err(),
            "wrong service key must fail"
        );

        let mut server = Transport::new(mode, "test");
        let mut client = Transport::new(mode, "test");
        let mut frames = client.send(open()).unwrap();
        server.receive(frames.remove(0)).unwrap();
        let token = frames.remove(0);
        server.receive(token.clone()).unwrap();
        // Cover future-dated authenticators: one skew window is too short.
        for at in server.replays.values_mut() {
            *at -= SKEW + 1;
        }
        server.receive(open()).unwrap();
        assert!(
            server.receive(token).is_err(),
            "AP-REQ replay on a new context must fail"
        );

        let mut server = Transport::new(mode, "test");
        let mut client = Transport::new(mode, "test");
        let mut frames = client.send(open()).unwrap();
        for frame in &mut frames {
            frame.src.node = NodeId(3);
        }
        server.receive(frames.remove(0)).unwrap();
        assert!(
            server.receive(frames.remove(0)).is_err(),
            "authenticated principal must match the claimed sender"
        );

        let mut transport = Transport::new(mode, "test");
        let mut context = transport.context(open().src, true).unwrap();
        context.credentials = Some(
            CredentialsBuffers::try_from(Credentials::AuthIdentity(AuthIdentity {
                username: Username::new_down_level_logon_name("node-2", REALM).unwrap(),
                password: "wrong-password".to_owned().into(),
            }))
            .unwrap(),
        );
        assert!(
            context
                .initiate(
                    None,
                    &host(open().dst),
                    &transport.kdc(open().src, open().dst)
                )
                .is_err(),
            "bad credentials must fail at the KDC"
        );
    }

    #[test]
    fn encrypted_world_links_survive_pause_partition_and_peer_hosting() {
        use crate::lab::{
            scenario::{NodeSpec, Scenario},
            world::{Fault, World},
        };
        use serde_json::json;
        let scenario = Scenario {
            security: SecurityMode::KerberosEncrypted,
            nodes: vec![
                NodeSpec::new(1, "echo", "echo", json!({})),
                NodeSpec::new(
                    2,
                    "pinger",
                    "pinger",
                    json!({"target": 1, "period_ms": 100}),
                ),
            ],
            ..Scenario::empty(7)
        };
        let mut world = World::from_scenario(&scenario).unwrap();
        world.step_until(500);
        assert!(
            world.node_snapshot(NodeId(2)).unwrap()["echoes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(world.scenario().security, scenario.security);
        let wire = world.drain_wire();
        assert!(
            wire.frames
                .iter()
                .filter(|f| f.kind == "data")
                .all(|f| wire_label(&crate::lab::net::b64::decode(&f.bytes).unwrap()).is_some())
        );
        world.fault(Fault::Pause { node: NodeId(1) });
        world.step_until(700);
        world.fault(Fault::Resume { node: NodeId(1) });
        world.step_until(900);
        assert!(!world.events().any(|e| e.kind == "sspi_failed"));
        let before = world.node_snapshot(NodeId(2)).unwrap()["echoes"]
            .as_u64()
            .unwrap();
        world.fault(Fault::Partition {
            a: NodeId(1),
            b: NodeId(2),
        });
        world.step_until(1000);
        world.fault(Fault::Heal {
            a: NodeId(1),
            b: NodeId(2),
        });
        world.step_until(32000);
        assert!(
            world.node_snapshot(NodeId(2)).unwrap()["echoes"]
                .as_u64()
                .unwrap()
                > before
        );
        let failures: Vec<_> = world.events().filter(|e| e.kind == "sspi_failed").collect();
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert_eq!(
            failures[0].detail["reason"],
            "SSPI authentication timed out after 30 s"
        );

        let mut server = World::from_scenario_hosted(&scenario, &[NodeId(1)]).unwrap();
        let mut client = World::from_scenario_hosted(&scenario, &[NodeId(2)]).unwrap();
        let mut to_server = Vec::new();
        let mut to_client = Vec::new();
        for now in (0..1000).step_by(10) {
            server.step_until(now);
            client.step_until(now);
            to_server.extend(client.drain_egress());
            to_client.extend(server.drain_egress());
            let mut due_server = Vec::new();
            let mut due_client = Vec::new();
            to_server.retain(|t| {
                if t.deliver_at <= now {
                    due_server.push(t.frame.clone());
                    false
                } else {
                    true
                }
            });
            to_client.retain(|t| {
                if t.deliver_at <= now {
                    due_client.push(t.frame.clone());
                    false
                } else {
                    true
                }
            });
            server.push_ingress(due_server);
            client.push_ingress(due_client);
        }
        assert!(
            client.node_snapshot(NodeId(2)).unwrap()["echoes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            !client
                .events()
                .chain(server.events())
                .any(|e| e.kind == "sspi_failed")
        );
    }
}
