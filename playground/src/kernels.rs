//! A JSON front door to the Creusot-verified decision kernels of
//! `krabka-verified`.
//!
//! The website page `/docs/verified` lets a reader type inputs into a form and
//! watch the same pure function the broker runs decide. [`run_kernel`] is the
//! whole seam: JavaScript names a kernel and hands over its inputs as one JSON
//! object, and gets one JSON object back. Every field is checked before the
//! kernel runs. That covers the shape of the input, the integer ranges of the
//! Rust parameters, and every Creusot `#[requires]` clause of the kernel. A
//! violated precondition never reaches the kernel body, because outside a
//! Creusot build a `#[requires]` clause is documentation, not a runtime check.
//!
//! Reply shape:
//!
//! ```json
//! { "ok": true, "result": 3 }
//! { "ok": true, "result": "Duplicate", "detail": { "base_offset": 100 } }
//! { "ok": false, "error": "precondition violated: majority must be between 1 and follower_offsets.len() + 1" }
//! ```

use std::fmt::Debug;

use krabka_ids::{LeaderEpoch, Offset};
use krabka_verified::{
    authz::acl_decision,
    compaction::{BatchMeta, RecordMeta, RetainDecision, TxnDataState, retain_decision},
    consensus::{
        election_has_quorum, election_jitter_ms, log_is_up_to_date, majority_size,
        recompute_high_watermark,
    },
    isr::isr_maintenance_selected,
    leader_epoch::{EpochEntry, epoch_and_offset_for_entries},
    log_index::offset_index_lookup,
    producer::{ProducerBatch, ProducerDecision, producer_decision},
    quota::{QuotaCandidatePresence, UserClientQuotaFacts, user_client_quota_precedence},
    stretch::{
        min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
    },
    throttle::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume},
    vote::vote_admission_decision,
};
use serde_json::{Map, Value, json};
use wasm_bindgen::prelude::*;

/// The kernels [`run_kernel`] knows, in the order the page lists them.
const KERNEL_NAMES: [&str; 16] = [
    "majority_size",
    "election_has_quorum",
    "log_is_up_to_date",
    "recompute_high_watermark",
    "election_jitter_ms",
    "plan_consume",
    "acl_decision",
    "producer_decision",
    "epoch_and_offset_for_entries",
    "offset_index_lookup",
    "stretch_durability",
    "quorum_survives_any_single_site_loss",
    "user_client_quota_precedence",
    "isr_maintenance_selected",
    "retain_decision",
    "vote_admission_decision",
];

/// The prefix every precondition failure message starts with.
const PRECONDITION: &str = "precondition violated: ";

/// The stretch kernels bound their inputs at this value to keep the `i64`
/// arithmetic away from overflow. See `krabka_verified::stretch`.
const STRETCH_BOUND: i64 = 1024;

/// What a kernel decided: the value the page shows, and any payload a data
/// variant carried with it.
struct Outcome {
    result: Value,
    detail: Option<Value>,
}

impl Outcome {
    /// A bare result with no detail.
    fn of(result: impl Into<Value>) -> Self {
        Self {
            result: result.into(),
            detail: None,
        }
    }

    /// A result with a payload the page shows next to it.
    fn with_detail(result: impl Into<Value>, detail: Value) -> Self {
        Self {
            result: result.into(),
            detail: Some(detail),
        }
    }
}

/// A kernel evaluation: the outcome, or a message that says what was wrong
/// with the input.
type KernelResult = Result<Outcome, String>;

/// The names [`run_kernel`] accepts, as a JSON array of strings.
#[wasm_bindgen]
#[must_use]
pub fn kernel_names() -> String {
    serde_json::to_string(&KERNEL_NAMES).unwrap_or_else(|_| "[]".to_string())
}

/// Evaluate the kernel `name` on the JSON object `input_json`.
///
/// The reply is a JSON object. On success it is
/// `{"ok":true,"result":<value>}`, plus a `"detail"` object when the decision
/// carried a payload, such as the `base_offset` of a duplicate producer batch.
/// On failure it is `{"ok":false,"error":"<message>"}`. The message names the
/// offending field for a missing, mistyped, or out-of-range input, and starts
/// with `precondition violated: ` when the inputs are well-formed but break a
/// Creusot `#[requires]` clause of the kernel.
#[wasm_bindgen]
#[must_use]
pub fn run_kernel(name: &str, input_json: &str) -> String {
    // A panic hook so a Rust panic surfaces as a readable console error
    // instead of an opaque `unreachable executed` trap.
    console_error_panic_hook::set_once();
    let reply = match evaluate(name, input_json) {
        Ok(Outcome { result, detail }) => {
            let mut reply = json!({ "ok": true, "result": result });
            if let Some(detail) = detail {
                reply["detail"] = detail;
            }
            reply
        }
        Err(error) => json!({ "ok": false, "error": error }),
    };
    reply.to_string()
}

/// Parse the input, pick the kernel, and run it.
fn evaluate(name: &str, input_json: &str) -> KernelResult {
    let value: Value =
        serde_json::from_str(input_json).map_err(|e| format!("input is not valid JSON: {e}"))?;
    let Value::Object(fields) = value else {
        return Err("input must be a JSON object".to_string());
    };
    let input = Input::root(&fields);
    match name {
        "majority_size" => majority_size_kernel(&input),
        "election_has_quorum" => election_has_quorum_kernel(&input),
        "log_is_up_to_date" => log_is_up_to_date_kernel(&input),
        "recompute_high_watermark" => recompute_high_watermark_kernel(&input),
        "election_jitter_ms" => election_jitter_ms_kernel(&input),
        "plan_consume" => plan_consume_kernel(&input),
        "acl_decision" => acl_decision_kernel(&input),
        "producer_decision" => producer_decision_kernel(&input),
        "epoch_and_offset_for_entries" => epoch_and_offset_for_entries_kernel(&input),
        "offset_index_lookup" => offset_index_lookup_kernel(&input),
        "stretch_durability" => stretch_durability_kernel(&input),
        "quorum_survives_any_single_site_loss" => quorum_survives_kernel(&input),
        "user_client_quota_precedence" => user_client_quota_precedence_kernel(&input),
        "isr_maintenance_selected" => isr_maintenance_selected_kernel(&input),
        "retain_decision" => retain_decision_kernel(&input),
        "vote_admission_decision" => vote_admission_decision_kernel(&input),
        other => Err(format!(
            "unknown kernel `{other}`; kernel_names() lists the ones available"
        )),
    }
}

/// Fail with a precondition message unless `holds`. `clause` is the plain
/// English reading of the Creusot `#[requires]` clause.
fn require(holds: bool, clause: &str) -> Result<(), String> {
    if holds {
        Ok(())
    } else {
        Err(format!("{PRECONDITION}{clause}"))
    }
}

/// The name of a unit variant, for enums whose `Debug` output is the variant
/// name alone.
fn variant<T: Debug>(value: &T) -> String {
    format!("{value:?}")
}

// ---------------------------------------------------------------------------
// Input access
// ---------------------------------------------------------------------------

/// A typed view over one JSON object, with a path prefix so an error inside a
/// nested object names the full field, for example `last.epoch`.
struct Input<'a> {
    fields: &'a Map<String, Value>,
    path: String,
}

impl<'a> Input<'a> {
    /// The top-level input object.
    fn root(fields: &'a Map<String, Value>) -> Self {
        Self {
            fields,
            path: String::new(),
        }
    }

    /// The field name as an error message shows it.
    fn label(&self, name: &str) -> String {
        if self.path.is_empty() {
            format!("`{name}`")
        } else {
            format!("`{}.{name}`", self.path)
        }
    }

    /// The raw value of `name`, or a "missing field" error.
    fn raw(&self, name: &str) -> Result<&'a Value, String> {
        self.fields
            .get(name)
            .ok_or_else(|| format!("missing field {}", self.label(name)))
    }

    /// The raw value of `name`, with JSON `null` read as "absent".
    fn optional(&self, name: &str) -> Result<Option<&'a Value>, String> {
        let value = self.raw(name)?;
        Ok(if value.is_null() { None } else { Some(value) })
    }

    /// A boolean field.
    fn boolean(&self, name: &str) -> Result<bool, String> {
        self.raw(name)?
            .as_bool()
            .ok_or_else(|| format!("field {} must be true or false", self.label(name)))
    }

    /// A string field.
    fn string(&self, name: &str) -> Result<&'a str, String> {
        self.raw(name)?
            .as_str()
            .ok_or_else(|| format!("field {} must be a string", self.label(name)))
    }

    /// A signed integer field that must fit `T`.
    fn signed<T: TryFrom<i64>>(&self, name: &str) -> Result<T, String> {
        signed_value(self.raw(name)?, &self.label(name))
    }

    /// An unsigned integer field that must fit `T`.
    fn unsigned<T: TryFrom<u64>>(&self, name: &str) -> Result<T, String> {
        unsigned_value(self.raw(name)?, &self.label(name))
    }

    /// A signed integer field that may be JSON `null`.
    fn optional_signed<T: TryFrom<i64>>(&self, name: &str) -> Result<Option<T>, String> {
        self.optional(name)?
            .map(|value| signed_value(value, &self.label(name)))
            .transpose()
    }

    /// An array field, as its raw elements.
    fn array(&self, name: &str) -> Result<&'a [Value], String> {
        self.raw(name)?
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| format!("field {} must be an array", self.label(name)))
    }

    /// An array of signed integers that each fit `T`.
    fn signed_array<T: TryFrom<i64>>(&self, name: &str) -> Result<Vec<T>, String> {
        self.array(name)?
            .iter()
            .enumerate()
            .map(|(index, value)| signed_value(value, &self.element_label(name, index)))
            .collect()
    }

    /// A nested object field, or `None` for JSON `null`.
    fn optional_object(&self, name: &str) -> Result<Option<Input<'a>>, String> {
        self.optional(name)?
            .map(|value| object_value(value, &self.label(name)))
            .transpose()
    }

    /// The elements of an array field, each viewed as a nested object.
    fn object_array(&self, name: &str) -> Result<Vec<Input<'a>>, String> {
        self.array(name)?
            .iter()
            .enumerate()
            .map(|(index, value)| object_value(value, &self.element_label(name, index)))
            .collect()
    }

    /// The label of element `index` of the array field `name`.
    fn element_label(&self, name: &str, index: usize) -> String {
        let label = self.label(name);
        let inner = label.trim_matches('`');
        format!("`{inner}[{index}]`")
    }
}

/// Read `value` as a nested object whose errors carry `label` as prefix.
fn object_value<'a>(value: &'a Value, label: &str) -> Result<Input<'a>, String> {
    let fields = value
        .as_object()
        .ok_or_else(|| format!("field {label} must be an object"))?;
    Ok(Input {
        fields,
        path: label.trim_matches('`').to_string(),
    })
}

/// Read a JSON number as a signed integer that fits `T`. Floats, non-numbers,
/// and out-of-range values are rejected with the field's `label`.
fn signed_value<T: TryFrom<i64>>(value: &Value, label: &str) -> Result<T, String> {
    value
        .as_i64()
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| integer_error::<T>(label))
}

/// Read a JSON number as an unsigned integer that fits `T`. Floats,
/// non-numbers, negatives, and out-of-range values are rejected with the
/// field's `label`.
fn unsigned_value<T: TryFrom<u64>>(value: &Value, label: &str) -> Result<T, String> {
    value
        .as_u64()
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| integer_error::<T>(label))
}

/// The message for a field that is not a whole number in the range of `T`.
fn integer_error<T>(label: &str) -> String {
    format!(
        "field {label} must be a whole number that fits {}",
        std::any::type_name::<T>()
    )
}

// ---------------------------------------------------------------------------
// Consensus
// ---------------------------------------------------------------------------

/// `consensus::majority_size`: the strict-majority size for `voter_count`
/// voters.
fn majority_size_kernel(input: &Input<'_>) -> KernelResult {
    let voter_count: usize = input.unsigned("voter_count")?;
    Ok(Outcome::of(majority_size(voter_count)))
}

/// `consensus::election_has_quorum`: whether `current_grants` votes elect a
/// candidate among `voter_count` voters.
fn election_has_quorum_kernel(input: &Input<'_>) -> KernelResult {
    let voter_count: usize = input.unsigned("voter_count")?;
    let current_grants: usize = input.unsigned("current_grants")?;
    Ok(Outcome::of(election_has_quorum(voter_count, current_grants)))
}

/// `consensus::log_is_up_to_date`: the KIP-595 rule for granting a vote.
fn log_is_up_to_date_kernel(input: &Input<'_>) -> KernelResult {
    let my_epoch: u32 = input.unsigned("my_epoch")?;
    let my_end: i64 = input.signed("my_end")?;
    let cand_epoch: u32 = input.unsigned("cand_epoch")?;
    let cand_offset: i64 = input.signed("cand_offset")?;
    Ok(Outcome::of(log_is_up_to_date(
        my_epoch,
        my_end,
        cand_epoch,
        cand_offset,
    )))
}

/// `consensus::recompute_high_watermark`: the leader's high watermark after a
/// round of follower fetches.
fn recompute_high_watermark_kernel(input: &Input<'_>) -> KernelResult {
    let log_end: i64 = input.signed("log_end")?;
    let follower_offsets: Vec<i64> = input.signed_array("follower_offsets")?;
    let majority: usize = input.unsigned("majority")?;
    let epoch_start_offset: i64 = input.signed("epoch_start_offset")?;
    let current_hwm: i64 = input.signed("current_hwm")?;
    let leader_counts = input.boolean("leader_counts")?;

    require(
        (1..=follower_offsets.len() + 1).contains(&majority),
        "majority must be between 1 and follower_offsets.len() + 1",
    )?;
    require(
        leader_counts || majority <= follower_offsets.len(),
        "when leader_counts is false, majority must be at most follower_offsets.len()",
    )?;
    require(
        current_hwm <= log_end,
        "current_hwm must be at most log_end",
    )?;
    require(
        follower_offsets.iter().all(|&offset| offset <= log_end),
        "every follower offset must be at most log_end",
    )?;

    Ok(Outcome::of(recompute_high_watermark(
        log_end,
        &follower_offsets,
        majority,
        epoch_start_offset,
        current_hwm,
        leader_counts,
    )))
}

/// `consensus::election_jitter_ms`: the per-node, per-epoch election timeout
/// jitter.
fn election_jitter_ms_kernel(input: &Input<'_>) -> KernelResult {
    let me: u64 = input.unsigned("me")?;
    let epoch: u32 = input.unsigned("epoch")?;
    let base_ms: u64 = input.unsigned("base_ms")?;
    Ok(Outcome::of(election_jitter_ms(me, epoch, base_ms)))
}

// ---------------------------------------------------------------------------
// Throttling and authorization
// ---------------------------------------------------------------------------

/// `throttle::plan_consume`: one token-bucket consume step.
fn plan_consume_kernel(input: &Input<'_>) -> KernelResult {
    let available = AvailableTokens(input.unsigned("available")?);
    let refill = RefillTokens(input.unsigned("refill")?);
    let burst = BurstCapacity(input.unsigned("burst")?);
    let requested = RequestedTokens(input.unsigned("requested")?);
    let (granted, new_available) = plan_consume(available, refill, burst, requested);
    Ok(Outcome::of(json!({
        "granted": granted.0,
        "new_available": new_available.0,
    })))
}

/// `authz::acl_decision`: super-user bypass, deny wins, default deny.
fn acl_decision_kernel(input: &Input<'_>) -> KernelResult {
    let super_user = input.boolean("super_user")?;
    let saw_allow = input.boolean("saw_allow")?;
    let saw_deny = input.boolean("saw_deny")?;
    Ok(Outcome::of(variant(&acl_decision(
        super_user, saw_allow, saw_deny,
    ))))
}

// ---------------------------------------------------------------------------
// Log
// ---------------------------------------------------------------------------

/// `producer::producer_decision`: append, duplicate, out of order, or fenced.
fn producer_decision_kernel(input: &Input<'_>) -> KernelResult {
    let last = input
        .optional_object("last")?
        .map(|batch| {
            Ok::<_, String>(ProducerBatch {
                epoch: batch.signed("epoch")?,
                last_sequence: batch.signed("last_sequence")?,
                last_offset_delta: batch.optional_signed("last_offset_delta")?,
                base_offset: batch.signed("base_offset")?,
            })
        })
        .transpose()?;
    let producer_epoch: i16 = input.signed("producer_epoch")?;
    let base_sequence: i32 = input.signed("base_sequence")?;
    let last_offset_delta: i32 = input.signed("last_offset_delta")?;

    Ok(
        match producer_decision(last, producer_epoch, base_sequence, last_offset_delta) {
            ProducerDecision::Append => Outcome::of("Append"),
            ProducerDecision::Duplicate { base_offset } => {
                Outcome::with_detail("Duplicate", json!({ "base_offset": base_offset }))
            }
            ProducerDecision::OutOfOrder => Outcome::of("OutOfOrder"),
            ProducerDecision::Fenced => Outcome::of("Fenced"),
        },
    )
}

/// `leader_epoch::epoch_and_offset_for_entries`: the KIP-101 truncation point
/// a follower gets for a requested epoch.
fn epoch_and_offset_for_entries_kernel(input: &Input<'_>) -> KernelResult {
    let entries = input
        .object_array("entries")?
        .iter()
        .map(|entry| {
            Ok::<_, String>(EpochEntry {
                epoch: LeaderEpoch(entry.signed("epoch")?),
                start_offset: Offset(entry.signed("start_offset")?),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let requested_epoch = LeaderEpoch(input.signed("requested_epoch")?);
    let log_end_offset = Offset(input.signed("log_end_offset")?);

    require(
        entries
            .iter()
            .zip(entries.iter().skip(1))
            .all(|(a, b)| a.epoch.0 < b.epoch.0 && a.start_offset.0 < b.start_offset.0),
        "entries must be strictly increasing in both epoch and start_offset",
    )?;

    let (found_epoch, end_offset) =
        epoch_and_offset_for_entries(&entries, requested_epoch, log_end_offset);
    Ok(Outcome::of(json!({
        "found_epoch": found_epoch.0,
        "end_offset": end_offset.0,
    })))
}

/// `log_index::offset_index_lookup`: the file position of the last index
/// entry at or before `target`.
fn offset_index_lookup_kernel(input: &Input<'_>) -> KernelResult {
    let entries = input
        .array("entries")?
        .iter()
        .enumerate()
        .map(|(index, pair)| index_pair(pair, &input.element_label("entries", index)))
        .collect::<Result<Vec<_>, _>>()?;
    let target: u32 = input.unsigned("target")?;

    require(
        entries
            .iter()
            .zip(entries.iter().skip(1))
            .all(|(a, b)| a.0 < b.0),
        "entries must be strictly increasing in their first element",
    )?;

    Ok(Outcome::of(offset_index_lookup(&entries, target)))
}

/// Read one `[relative_offset, position]` pair of an offset index.
fn index_pair(value: &Value, label: &str) -> Result<(u32, u32), String> {
    let pair = value.as_array().filter(|pair| pair.len() == 2).ok_or_else(|| {
        format!("field {label} must be a two-element array [relative_offset, position]")
    })?;
    let inner = label.trim_matches('`');
    Ok((
        unsigned_value(&pair[0], &format!("`{inner}[0]`"))?,
        unsigned_value(&pair[1], &format!("`{inner}[1]`"))?,
    ))
}

// ---------------------------------------------------------------------------
// Stretch clusters
// ---------------------------------------------------------------------------

/// `stretch::site_loss_survivors` and `stretch::min_insync_is_site_loss_safe`
/// together: how many replicas survive a site loss, how many the largest site
/// holds, and whether `min_insync` stays satisfiable.
fn stretch_durability_kernel(input: &Input<'_>) -> KernelResult {
    let rf: i64 = input.signed("rf")?;
    let sites: i64 = input.signed("sites")?;
    let min_insync: i64 = input.signed("min_insync")?;

    require(
        (1..=STRETCH_BOUND).contains(&rf),
        "rf must be between 1 and 1024",
    )?;
    require(
        (1..=STRETCH_BOUND).contains(&sites),
        "sites must be between 1 and 1024",
    )?;

    let survivors = site_loss_survivors(rf, sites);
    Ok(Outcome::of(json!({
        "survivors": survivors,
        "largest_site": rf - survivors,
        "min_insync_safe": min_insync_is_site_loss_safe(rf, sites, min_insync),
    })))
}

/// `stretch::quorum_survives_any_single_site_loss`: whether the voters left
/// after any one site drops still form a majority.
fn quorum_survives_kernel(input: &Input<'_>) -> KernelResult {
    let voters_per_site: Vec<i64> = input.signed_array("voters_per_site")?;

    require(
        voters_per_site.len() <= 1024,
        "voters_per_site must hold at most 1024 sites",
    )?;
    require(
        voters_per_site
            .iter()
            .all(|&voters| (0..=STRETCH_BOUND).contains(&voters)),
        "every voters_per_site entry must be between 0 and 1024",
    )?;

    Ok(Outcome::of(quorum_survives_any_single_site_loss(
        &voters_per_site,
    )))
}

// ---------------------------------------------------------------------------
// Quotas, ISR, compaction, and votes
// ---------------------------------------------------------------------------

/// `quota::user_client_quota_precedence`: which quota entity wins for a
/// (user, client-id) pair. Each field is `true` when that candidate is
/// present.
fn user_client_quota_precedence_kernel(input: &Input<'_>) -> KernelResult {
    let presence = |name: &str| -> Result<QuotaCandidatePresence, String> {
        Ok(if input.boolean(name)? {
            QuotaCandidatePresence::Present
        } else {
            QuotaCandidatePresence::Absent
        })
    };
    let facts = UserClientQuotaFacts {
        exact_pair: presence("exact_pair")?,
        exact_client_default_user: presence("exact_client_default_user")?,
        default_client_exact_user: presence("default_client_exact_user")?,
        default_pair: presence("default_pair")?,
        exact_user: presence("exact_user")?,
        exact_client: presence("exact_client")?,
        default_user: presence("default_user")?,
        default_client: presence("default_client")?,
    };
    Ok(Outcome::of(variant(&user_client_quota_precedence(facts))))
}

/// `isr::isr_maintenance_selected`: whether a replica stays in the ISR after
/// a maintenance pass.
fn isr_maintenance_selected_kernel(input: &Input<'_>) -> KernelResult {
    let facts = (
        input.boolean("assigned")?,
        input.boolean("is_leader")?,
        input.boolean("in_isr")?,
        input.boolean("fetch_recent")?,
        input.boolean("caught_up_recent")?,
    );
    Ok(Outcome::of(isr_maintenance_selected(facts)))
}

/// `compaction::retain_decision`: keep, delete, or start the tombstone
/// horizon for one record under KIP-534 compaction.
fn retain_decision_kernel(input: &Input<'_>) -> KernelResult {
    let rec = RecordMeta {
        has_key: input.boolean("has_key")?,
        has_value: input.boolean("has_value")?,
    };
    let batch = BatchMeta {
        is_control: input.boolean("is_control")?,
        producer_id: input.signed("producer_id")?,
        existing_horizon: input.optional_signed("existing_horizon")?,
    };
    let is_newest_for_key = input.boolean("is_newest_for_key")?;
    let txn = match input.string("txn")? {
        "NotTransactional" => TxnDataState::NotTransactional,
        "DataSurvives" => TxnDataState::DataSurvives,
        "DataFullyGone" => TxnDataState::DataFullyGone,
        _ => {
            return Err(
                "field `txn` must be one of NotTransactional, DataSurvives, DataFullyGone"
                    .to_string(),
            );
        }
    };
    let now_ms: i64 = input.signed("now_ms")?;
    let delete_retention_ms: i64 = input.signed("delete_retention_ms")?;

    Ok(
        match retain_decision(rec, batch, is_newest_for_key, txn, now_ms, delete_retention_ms) {
            RetainDecision::Keep => Outcome::of("Keep"),
            RetainDecision::Delete => Outcome::of("Delete"),
            RetainDecision::SetHorizon(horizon) => {
                Outcome::with_detail("SetHorizon", json!({ "horizon": horizon }))
            }
        },
    )
}

/// `vote::vote_admission_decision`: ignore, deny, or consider an incoming
/// KIP-595 Vote request.
fn vote_admission_decision_kernel(input: &Input<'_>) -> KernelResult {
    let voter_id: u64 = input.unsigned("voter_id")?;
    let local_id: u64 = input.unsigned("local_id")?;
    let target_directory_matches = input.boolean("target_directory_matches")?;
    let cluster_matches = input.boolean("cluster_matches")?;
    let local_is_voter = input.boolean("local_is_voter")?;
    let candidate_is_voter = input.boolean("candidate_is_voter")?;
    Ok(Outcome::of(variant(&vote_admission_decision(
        voter_id,
        local_id,
        target_directory_matches,
        cluster_matches,
        local_is_voter,
        candidate_is_voter,
    ))))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    /// Run a kernel and parse its reply back into a value.
    fn run(name: &str, input: &Value) -> Value {
        serde_json::from_str(&run_kernel(name, &input.to_string())).expect("reply is valid JSON")
    }

    /// The `error` string of a failed reply.
    fn error_of(name: &str, input: &Value) -> String {
        let reply = run(name, input);
        assert2::assert!(reply["ok"] == false, "reply {reply} should have failed");
        reply["error"].as_str().expect("error is a string").to_string()
    }

    /// Check every `(kernel, input, expected reply)` row.
    fn check_rows(rows: &[(&str, Value, Value)]) {
        for (name, input, expected) in rows {
            let reply = run(name, input);
            assert2::assert!(reply == *expected, "kernel {name}");
        }
    }

    #[test]
    fn kernel_names_lists_every_bound_kernel() {
        let names: Value = serde_json::from_str(&kernel_names()).unwrap();
        let names: Vec<&str> = names
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert2::assert!(names == KERNEL_NAMES.to_vec());
        for name in names {
            // Every listed name dispatches to a kernel; an empty object fails
            // on a missing field, never on an unknown name.
            let error = error_of(name, &json!({}));
            assert2::assert!(error.starts_with("missing field"), "kernel {name}: {error}");
        }
    }

    #[test]
    fn consensus_and_log_kernels_happy_paths() {
        check_rows(&[
            (
                "majority_size",
                json!({ "voter_count": 5 }),
                json!({ "ok": true, "result": 3 }),
            ),
            (
                "election_has_quorum",
                json!({ "voter_count": 5, "current_grants": 3 }),
                json!({ "ok": true, "result": true }),
            ),
            (
                "log_is_up_to_date",
                json!({ "my_epoch": 5, "my_end": 100, "cand_epoch": 5, "cand_offset": 100 }),
                json!({ "ok": true, "result": true }),
            ),
            (
                "recompute_high_watermark",
                json!({
                    "log_end": 10, "follower_offsets": [9, 8], "majority": 2,
                    "epoch_start_offset": 0, "current_hwm": 0, "leader_counts": true
                }),
                json!({ "ok": true, "result": 9 }),
            ),
            (
                "election_jitter_ms",
                json!({ "me": 1, "epoch": 0, "base_ms": 1000 }),
                json!({ "ok": true, "result": 485 }),
            ),
            (
                "producer_decision",
                json!({
                    "last": { "epoch": 1, "last_sequence": 9, "last_offset_delta": 4, "base_offset": 100 },
                    "producer_epoch": 1, "base_sequence": 5, "last_offset_delta": 4
                }),
                json!({ "ok": true, "result": "Duplicate", "detail": { "base_offset": 100 } }),
            ),
            (
                "producer_decision",
                json!({ "last": null, "producer_epoch": 1, "base_sequence": 0, "last_offset_delta": 0 }),
                json!({ "ok": true, "result": "Append" }),
            ),
            (
                "epoch_and_offset_for_entries",
                json!({
                    "entries": [{ "epoch": 1, "start_offset": 0 }, { "epoch": 3, "start_offset": 10 }],
                    "requested_epoch": 2, "log_end_offset": 20
                }),
                json!({ "ok": true, "result": { "found_epoch": 1, "end_offset": 10 } }),
            ),
            (
                "offset_index_lookup",
                json!({ "entries": [[0, 0], [10, 100], [20, 200]], "target": 15 }),
                json!({ "ok": true, "result": 100 }),
            ),
        ]);
    }

    #[test]
    fn policy_kernels_happy_paths() {
        check_rows(&[
            (
                "plan_consume",
                json!({ "available": 5, "refill": 3, "burst": 10, "requested": 4 }),
                json!({ "ok": true, "result": { "granted": 4, "new_available": 4 } }),
            ),
            (
                "acl_decision",
                json!({ "super_user": false, "saw_allow": true, "saw_deny": false }),
                json!({ "ok": true, "result": "AllowAcl" }),
            ),
            (
                "stretch_durability",
                json!({ "rf": 6, "sites": 3, "min_insync": 3 }),
                json!({
                    "ok": true,
                    "result": { "survivors": 4, "largest_site": 2, "min_insync_safe": true }
                }),
            ),
            (
                "quorum_survives_any_single_site_loss",
                json!({ "voters_per_site": [2, 2, 1] }),
                json!({ "ok": true, "result": true }),
            ),
            (
                "user_client_quota_precedence",
                json!({
                    "exact_pair": false, "exact_client_default_user": false,
                    "default_client_exact_user": false, "default_pair": false,
                    "exact_user": true, "exact_client": true,
                    "default_user": false, "default_client": false
                }),
                json!({ "ok": true, "result": "ExactUser" }),
            ),
            (
                "isr_maintenance_selected",
                json!({
                    "assigned": true, "is_leader": false, "in_isr": true,
                    "fetch_recent": true, "caught_up_recent": false
                }),
                json!({ "ok": true, "result": true }),
            ),
            (
                "retain_decision",
                json!({
                    "has_key": true, "has_value": false, "is_control": false,
                    "producer_id": 7, "existing_horizon": null, "is_newest_for_key": true,
                    "txn": "NotTransactional", "now_ms": 1000, "delete_retention_ms": 500
                }),
                json!({ "ok": true, "result": "SetHorizon", "detail": { "horizon": 1500 } }),
            ),
            (
                "vote_admission_decision",
                json!({
                    "voter_id": 1, "local_id": 1, "target_directory_matches": true,
                    "cluster_matches": true, "local_is_voter": true, "candidate_is_voter": true
                }),
                json!({ "ok": true, "result": "Consider" }),
            ),
        ]);
    }

    #[test]
    fn precondition_violations_are_reported_before_the_kernel_runs() {
        let rows = [
            (
                "recompute_high_watermark",
                json!({
                    "log_end": 10, "follower_offsets": [9, 8], "majority": 0,
                    "epoch_start_offset": 0, "current_hwm": 0, "leader_counts": true
                }),
                "majority must be between 1 and follower_offsets.len() + 1",
            ),
            (
                "recompute_high_watermark",
                json!({
                    "log_end": 10, "follower_offsets": [11], "majority": 1,
                    "epoch_start_offset": 0, "current_hwm": 0, "leader_counts": true
                }),
                "every follower offset must be at most log_end",
            ),
            (
                "stretch_durability",
                json!({ "rf": 0, "sites": 3, "min_insync": 1 }),
                "rf must be between 1 and 1024",
            ),
            (
                "epoch_and_offset_for_entries",
                json!({
                    "entries": [{ "epoch": 3, "start_offset": 0 }, { "epoch": 1, "start_offset": 10 }],
                    "requested_epoch": 2, "log_end_offset": 20
                }),
                "entries must be strictly increasing in both epoch and start_offset",
            ),
            (
                "offset_index_lookup",
                json!({ "entries": [[10, 100], [10, 200]], "target": 15 }),
                "entries must be strictly increasing in their first element",
            ),
            (
                "quorum_survives_any_single_site_loss",
                json!({ "voters_per_site": [2, -1] }),
                "every voters_per_site entry must be between 0 and 1024",
            ),
        ];
        for (name, input, clause) in rows {
            let error = error_of(name, &input);
            assert2::assert!(error == format!("precondition violated: {clause}"), "kernel {name}");
        }
    }

    #[test]
    fn unknown_kernel_names_are_rejected() {
        let error = error_of("no_such_kernel", &json!({}));
        assert2::assert!(error.contains("unknown kernel `no_such_kernel`"));
    }

    #[test]
    fn malformed_inputs_name_the_field() {
        let rows = [
            ("majority_size", json!({}), "missing field `voter_count`"),
            (
                "majority_size",
                json!({ "voter_count": -1 }),
                "field `voter_count` must be a whole number that fits usize",
            ),
            (
                "majority_size",
                json!({ "voter_count": 2.5 }),
                "field `voter_count` must be a whole number that fits usize",
            ),
            (
                "producer_decision",
                json!({ "last": { "epoch": 70000 }, "producer_epoch": 1, "base_sequence": 0, "last_offset_delta": 0 }),
                "field `last.epoch` must be a whole number that fits i16",
            ),
            (
                "recompute_high_watermark",
                json!({ "log_end": 10, "follower_offsets": [1, "x"] }),
                "field `follower_offsets[1]` must be a whole number that fits i64",
            ),
            (
                "retain_decision",
                json!({
                    "has_key": true, "has_value": false, "is_control": false,
                    "producer_id": 7, "existing_horizon": null, "is_newest_for_key": true,
                    "txn": "Sometimes", "now_ms": 1000, "delete_retention_ms": 500
                }),
                "field `txn` must be one of NotTransactional, DataSurvives, DataFullyGone",
            ),
        ];
        for (name, input, expected) in rows {
            assert2::assert!(error_of(name, &input) == expected, "kernel {name}");
        }
        assert2::assert!(error_of("majority_size", &json!([1])) == "input must be a JSON object");
        let reply: Value = serde_json::from_str(&run_kernel("majority_size", "{nope")).unwrap();
        assert2::assert!(reply["ok"] == false);
        assert2::assert!(
            reply["error"]
                .as_str()
                .unwrap()
                .starts_with("input is not valid JSON")
        );
    }
}
