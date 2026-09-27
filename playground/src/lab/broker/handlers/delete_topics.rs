//! `DeleteTopics` (api key 20), through the local controller.
//!
//! Rows follow `ControllerApis.deleteTopics`. From v6 a row names its topic
//! by name or by id: a row with both or with neither answers
//! `INVALID_REQUEST`, a name or an id given twice answers `INVALID_REQUEST`
//! once, an id no topic has answers `UNKNOWN_TOPIC_ID`, a name no topic has
//! `UNKNOWN_TOPIC_OR_PARTITION`, and a name whose topic an id already names
//! `INVALID_REQUEST`. Every other topic is deleted, all in one commit, and
//! its row carries its name and id. Kafka shuffles the rows so their
//! positions reveal nothing; the lab answers the refusals first, in that
//! order, then the deletions.

use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    owned::{
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{
    super::{
        BrokerNode, LocalController,
        dispatch::{Outcome, RequestCtx},
    },
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

/// Serve a `DeleteTopics`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: DeleteTopicsRequest,
) -> Outcome<DeleteTopicsResponse> {
    let DeleteTopicsRequest {
        topic_names,
        topics,
        ..
    } = request;
    let mut responses = Vec::new();
    let (mut names, mut duplicate_names) = (Vec::new(), Vec::new());
    let (mut ids, mut duplicate_ids) = (Vec::new(), Vec::new());
    for name in topic_names {
        provide(&mut names, &mut duplicate_names, name);
    }
    for topic in topics {
        match (topic.name, topic.topic_id) {
            (None, WireUuid::ZERO) => responses.push(row(
                None,
                WireUuid::ZERO,
                codes::INVALID_REQUEST,
                Some("Neither topic name nor id were specified."),
            )),
            (None, id) => provide(&mut ids, &mut duplicate_ids, id),
            (Some(name), WireUuid::ZERO) => provide(&mut names, &mut duplicate_names, name),
            (Some(name), id) => responses.push(row(
                Some(name),
                id,
                codes::INVALID_REQUEST,
                Some("You may not specify both topic name and topic id."),
            )),
        }
    }
    responses.extend(duplicate_names.into_iter().map(|name| {
        row(
            Some(name),
            WireUuid::ZERO,
            codes::INVALID_REQUEST,
            Some("Duplicate topic name."),
        )
    }));
    responses.extend(duplicate_ids.iter().map(|id| {
        row(
            None,
            *id,
            codes::INVALID_REQUEST,
            Some("Duplicate topic id."),
        )
    }));
    let mut deleting: Vec<(WireUuid, String)> = Vec::new();
    for id in ids {
        match node.image().topic_name_by_id(&uuid_of(id)) {
            Some(name) => deleting.push((id, name.to_owned())),
            None => responses.push(row(
                None,
                id,
                codes::UNKNOWN_TOPIC_ID,
                Some("This server does not host this topic ID."),
            )),
        }
    }
    for name in names {
        let Some(id) = node.image().topic(&name).map(|t| wire_uuid(t.topic_id)) else {
            responses.push(row(
                Some(name),
                WireUuid::ZERO,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("This server does not host this topic-partition."),
            ));
            continue;
        };
        if duplicate_ids.contains(&id) || deleting.iter().any(|(named, _)| *named == id) {
            deleting.retain(|(named, _)| *named != id);
            duplicate_ids.push(id);
            responses.push(row(
                Some(name),
                id,
                codes::INVALID_REQUEST,
                Some("The provided topic name maps to an ID that was already supplied."),
            ));
        } else {
            deleting.push((id, name));
        }
    }
    let records: Vec<MetadataRecord> = deleting
        .iter()
        .map(|(_, name)| LocalController::delete_topic(name))
        .collect();
    if !records.is_empty() {
        node.apply_metadata(ctx, &records);
    }
    responses.extend(
        deleting
            .into_iter()
            .map(|(id, name)| row(Some(name), id, codes::NONE, None)),
    );
    Outcome::Reply(DeleteTopicsResponse {
        responses,
        ..DeleteTopicsResponse::default()
    })
}
