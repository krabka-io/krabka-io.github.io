//! `DeleteTopics` (api key 20) on the controller listener: the active
//! controller deletes topics.
//!
//! Rows follow `ControllerApis.deleteTopics`. From v6 a row names its topic
//! by name or by id: a row with both or with neither answers
//! `INVALID_REQUEST`, a name or an id given twice answers `INVALID_REQUEST`
//! once, an id no topic has answers `UNKNOWN_TOPIC_ID`, a name no topic has
//! `UNKNOWN_TOPIC_OR_PARTITION`, and a name whose topic an id already names
//! `INVALID_REQUEST`. Every other topic is deleted, all in one commit, and
//! its row carries its name and id; the answer waits for the commit. The
//! lookups read the node's view, as Kafka's read events do on any
//! controller, and the deletion needs the active controller: another node
//! answers `NOT_CONTROLLER` on every row of the request. Kafka shuffles the
//! rows so their positions reveal nothing; the lab answers the refusals
//! first, in that order, then the deletions.

use krabka_metadata::{DeleteTopicRecord, MetadataImage, MetadataRecord};
use krabka_protocol::{
    owned::{
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{
    super::{
        BrokerNode,
        dispatch::{Outcome, RequestCtx},
    },
    forwarded::delete_topics_error,
    uuid_of, wire_uuid,
};
use crate::lab::{codes, net::Ctx};

fn row(
    name: Option<String>,
    topic_id: WireUuid,
    error_code: i16,
    message: Option<&str>,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name,
        topic_id,
        error_code,
        error_message: message.map(str::to_owned),
        ..DeletableTopicResult::default()
    }
}

/// Kafka's `addProvidedName` and its id twin: a value seen twice moves to the
/// duplicates and stays there.
fn provide<T: PartialEq>(provided: &mut Vec<T>, duplicates: &mut Vec<T>, value: T) {
    if duplicates.contains(&value) {
        return;
    }
    if let Some(at) = provided.iter().position(|p| *p == value) {
        provided.remove(at);
        duplicates.push(value);
    } else {
        provided.push(value);
    }
}

/// The names and ids a request provides, after Kafka's first pass: the rows
/// it refuses outright, and the duplicate ids a name may still collide with.
struct Provided {
    refused: Vec<DeletableTopicResult>,
    names: Vec<String>,
    ids: Vec<WireUuid>,
    duplicate_ids: Vec<WireUuid>,
}

impl Provided {
    fn of(topic_names: &[String], topics: &[DeleteTopicState]) -> Self {
        let mut refused = Vec::new();
        let (mut names, mut duplicate_names) = (Vec::new(), Vec::new());
        let (mut ids, mut duplicate_ids) = (Vec::new(), Vec::new());
        for name in topic_names {
            provide(&mut names, &mut duplicate_names, name.clone());
        }
        for topic in topics {
            match (topic.name.clone(), topic.topic_id) {
                (None, WireUuid::ZERO) => refused.push(row(
                    None,
                    WireUuid::ZERO,
                    codes::INVALID_REQUEST,
                    Some("Neither topic name nor id were specified."),
                )),
                (None, id) => provide(&mut ids, &mut duplicate_ids, id),
                (Some(name), WireUuid::ZERO) => provide(&mut names, &mut duplicate_names, name),
                (Some(name), id) => refused.push(row(
                    Some(name),
                    id,
                    codes::INVALID_REQUEST,
                    Some("You may not specify both topic name and topic id."),
                )),
            }
        }
        refused.extend(duplicate_names.into_iter().map(|name| {
            row(
                Some(name),
                WireUuid::ZERO,
                codes::INVALID_REQUEST,
                Some("Duplicate topic name."),
            )
        }));
        refused.extend(duplicate_ids.iter().map(|id| {
            row(
                None,
                *id,
                codes::INVALID_REQUEST,
                Some("Duplicate topic id."),
            )
        }));
        Self {
            refused,
            names,
            ids,
            duplicate_ids,
        }
    }

    /// Resolve the provided ids and names against `image`, as Kafka's
    /// `findTopicNames` and `findTopicIds` do: the topics to delete, by id,
    /// in the order the request named them, and a refused row for the rest.
    fn resolve(
        mut self,
        image: &MetadataImage,
    ) -> (Vec<DeletableTopicResult>, Vec<(WireUuid, String)>) {
        let mut deleting: Vec<(WireUuid, String)> = Vec::new();
        for id in self.ids {
            match image.topic_name_by_id(&uuid_of(id)) {
                Some(name) => deleting.push((id, name.to_owned())),
                None => self.refused.push(row(
                    None,
                    id,
                    codes::UNKNOWN_TOPIC_ID,
                    Some("This server does not host this topic ID."),
                )),
            }
        }
        for name in self.names {
            let Some(id) = image.topic(&name).map(|t| wire_uuid(t.topic_id)) else {
                self.refused.push(row(
                    Some(name),
                    WireUuid::ZERO,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    Some("This server does not host this topic-partition."),
                ));
                continue;
            };
            if self.duplicate_ids.contains(&id) || deleting.iter().any(|(named, _)| *named == id) {
                deleting.retain(|(named, _)| *named != id);
                self.duplicate_ids.push(id);
                self.refused.push(row(
                    Some(name),
                    id,
                    codes::INVALID_REQUEST,
                    Some("The provided topic name maps to an ID that was already supplied."),
                ));
            } else {
                deleting.push((id, name));
            }
        }
        (self.refused, deleting)
    }
}

/// Serve a `DeleteTopics` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    DeleteTopicsRequest {
        topic_names,
        topics,
        ..
    }: DeleteTopicsRequest,
) -> Outcome<DeleteTopicsResponse> {
    let image = node
        .quorum
        .active
        .as_ref()
        .map_or(&node.image, |active| &active.image);
    let (mut responses, deleting) = Provided::of(&topic_names, &topics).resolve(image);
    let not_controller = |message: &str| {
        delete_topics_error(
            &topic_names,
            &topics,
            req.version,
            codes::NOT_CONTROLLER,
            Some(message),
        )
    };
    if deleting.is_empty() {
        return Outcome::Reply(DeleteTopicsResponse {
            responses,
            ..DeleteTopicsResponse::default()
        });
    }
    if node.quorum.active.is_none() {
        return Outcome::Reply(not_controller(&node.quorum.not_controller_message()));
    }
    let records: Vec<MetadataRecord> = deleting
        .iter()
        .map(|(_, name)| MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name: name.clone() }))
        .collect();
    responses.extend(
        deleting
            .into_iter()
            .map(|(id, name)| row(Some(name), id, codes::NONE, None)),
    );
    let response = DeleteTopicsResponse {
        responses,
        ..DeleteTopicsResponse::default()
    };
    node.controller_write(ctx, req, records, response, not_controller)
}
