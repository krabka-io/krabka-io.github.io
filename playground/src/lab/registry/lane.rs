//! Several clients in one node.
//!
//! A [`KafkaClient`](crate::lab::client::KafkaClient) numbers the
//! connections it opens from 1 on its node's client endpoint, so two clients
//! in one node would open the same `(client endpoint, connection)` pair, and
//! the world and the brokers would take the two for one connection. The
//! registry runs five clients, as Confluent's does: the admin client that
//! sets up `_schemas`, the producer, the reader, the group member of the
//! leader election, and the HTTP client that forwards writes to the primary.
//! A [`Lane`] gives each a range of connection ids of its own. It runs the
//! client behind a context of its own, adds the lane's base to the id of
//! every frame the client sends, and takes it off every frame it hands back.
//! Everything else the client does through its context passes through
//! unchanged.

use crate::lab::net::{ConnId, Ctx, Frame, Rng};

/// How many connection ids one lane holds.
pub const LANE_SIZE: u32 = 1 << 20;

/// How many lanes fit in the connection id space.
pub const LANES: u32 = u32::MAX / LANE_SIZE + 1;

/// The lane of the store's admin client.
pub const ADMIN: u32 = 0;
/// The lane of the store's reader.
pub const READER: u32 = 1;
/// The lane of the store's producer.
pub const PRODUCER: u32 = 2;
/// The lane of the leader election's group member.
pub const ELECTOR: u32 = 3;
/// The lane of the HTTP client that forwards writes to the primary.
pub const FORWARDER: u32 = 4;
/// The lanes of one start of the node.
const ROLES: u32 = 5;

/// The lane index of `role` in the node's `generation`-th start, so the
/// clients of a restarted node open connections that no peer confuses with
/// those of the start before.
#[must_use]
pub fn index(generation: u32, role: u32) -> u32 {
    generation.wrapping_mul(ROLES).wrapping_add(role)
}

/// One client of a node, on a range of connection ids of its own.
pub struct Lane<T> {
    inner: T,
    index: u32,
    rng: Rng,
}

impl<T> Lane<T> {
    /// `inner` on lane `index` (taken modulo [`LANES`]). `seed` seeds the
    /// random numbers the client draws through its context.
    #[must_use]
    pub fn new(inner: T, index: u32, seed: u64) -> Self {
        Self {
            inner,
            index: index % LANES,
            rng: Rng::new(seed),
        }
    }

    /// The client.
    #[must_use]
    pub fn get(&self) -> &T {
        &self.inner
    }

    /// The client, for the calls that send nothing.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    fn base(&self) -> u32 {
        self.index * LANE_SIZE
    }

    /// Whether a connection id on the wire belongs to this lane.
    #[must_use]
    pub fn owns(&self, conn: ConnId) -> bool {
        conn.0 / LANE_SIZE == self.index
    }

    /// A frame from the wire as the client knows it: the lane's base taken
    /// off its connection id.
    #[must_use]
    pub fn inbound(&self, mut frame: Frame) -> Frame {
        frame.conn = ConnId(frame.conn.0.wrapping_sub(self.base()));
        frame
    }

    /// Run `f` on the client behind a context of the lane's own, then send
    /// what it queued through `ctx`, every frame on the lane's range of
    /// connection ids.
    pub fn run<R>(&mut self, ctx: &mut Ctx<'_>, f: impl FnOnce(&mut T, &mut Ctx<'_>) -> R) -> R {
        let mut outbox = Vec::new();
        let mut timer = None;
        let mut events = Vec::new();
        let mut durable = Vec::new();
        let result = {
            let mut inner = Ctx::new(
                ctx.now(),
                ctx.me(),
                &mut outbox,
                &mut timer,
                &mut events,
                &mut durable,
                &mut self.rng,
            );
            f(&mut self.inner, &mut inner)
        };
        let base = self.base();
        for mut frame in outbox {
            frame.conn = ConnId(frame.conn.0.wrapping_add(base));
            ctx.send(frame);
        }
        for (kind, detail) in events {
            ctx.event(kind, detail);
        }
        for op in durable {
            ctx.persist(op);
        }
        if let Some(at) = timer {
            ctx.arm(at);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::metadata_request::MetadataRequest;

    use super::*;
    use crate::lab::{
        client::{ClientOptions, KafkaClient, Target},
        net::{Endpoint, NodeId, Payload},
        testing::CtxBuffers,
    };

    fn client() -> KafkaClient {
        KafkaClient::new(
            vec![Endpoint::kafka(NodeId(1))],
            "lane",
            ClientOptions::default(),
        )
    }

    #[test]
    fn two_clients_in_one_node_open_distinct_connections() {
        let mut buffers = CtxBuffers::new(NodeId(4));
        let mut first = Lane::new(client(), 1, 7);
        let mut second = Lane::new(client(), 2, 8);
        for lane in [&mut first, &mut second] {
            buffers.with(0, |ctx| {
                lane.run(ctx, |client, ctx| {
                    client.send(ctx, Target::Any, MetadataRequest::default())
                })
            });
        }
        // Each client opened its first connection, and sent ApiVersions on it.
        let frames = buffers.take_frames();
        let opens: Vec<(ConnId, Endpoint)> = frames
            .iter()
            .filter(|f| f.payload == Payload::Open)
            .map(|f| (f.conn, f.dst))
            .collect();
        let broker = Endpoint::kafka(NodeId(1));
        assert!(
            opens
                == vec![
                    (ConnId(LANE_SIZE + 1), broker),
                    (ConnId(2 * LANE_SIZE + 1), broker)
                ]
        );
        assert!(frames.iter().all(|f| f.src == Endpoint::client(NodeId(4))));
        // A frame back on the second lane's connection is the second
        // client's connection 1, and only that lane claims it.
        let reply = frames[frames.len() - 1].reply(Payload::Close);
        assert!(!first.owns(reply.conn));
        assert!(second.owns(reply.conn));
        assert!(second.inbound(reply.clone()).conn == ConnId(1));
        assert!(
            second.inbound(reply) == Frame::close(broker, Endpoint::client(NodeId(4)), ConnId(1))
        );
    }

    #[test]
    fn lane_indexes_wrap_around_the_connection_id_space() {
        let lane = Lane::new((), LANES + 3, 1);
        assert!(lane.owns(ConnId(3 * LANE_SIZE + 9)));
        assert!(!lane.owns(ConnId(9)));
        assert!(LANES == 4096);
    }
}
