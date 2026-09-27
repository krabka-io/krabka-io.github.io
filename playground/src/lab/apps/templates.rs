//! Record templates: the placeholders a producer fills in for each record's
//! key, value and headers, and a streams `map {set}` op fills in per record.
//!
//! A template is text with placeholders in braces:
//!
//! | Placeholder | Value |
//! | --- | --- |
//! | `{seq}` | the record's sequence number, counted from 0 |
//! | `{seq % n}` | the sequence number modulo `n` |
//! | `{now}` | the logical time in milliseconds |
//! | `{rand a b}` | an integer in `a..=b`, from the node's deterministic generator |
//! | `{pick a\|b\|c}` | one of the options, chosen with the same generator |
//! | `{uuid}` | a version 4 UUID derived from the sequence number alone |
//!
//! `{{` and `}}` stand for literal braces. Whitespace inside the braces is
//! ignored around the parts: `{seq%10}` and `{ seq % 10 }` are the same.
//!
//! A [`JsonTemplate`] is a JSON document whose string values are templates.
//! A string that is exactly one numeric placeholder (`{seq}`, `{seq % n}`,
//! `{now}` or `{rand a b}`) becomes a JSON number, so `{"id": "{seq}"}`
//! renders as `{"id": 7}`. Object keys are literal.

use serde_json::{Map, Value};
use thiserror::Error;

use crate::lab::net::Millis;

/// Why a template does not parse.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TemplateError {
    #[error("unclosed `{{` in template `{0}`")]
    Unclosed(String),
    #[error("unmatched `}}` in template `{0}`; write `}}}}` for a literal brace")]
    Unmatched(String),
    #[error("unknown placeholder `{{{0}}}`")]
    Unknown(String),
    #[error("placeholder `{{{placeholder}}}`: {reason}")]
    Invalid { placeholder: String, reason: String },
}

/// One placeholder of a template.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Placeholder {
    /// `{seq}`.
    Seq,
    /// `{seq % n}`, with `n > 0`.
    SeqMod(u64),
    /// `{now}`.
    Now,
    /// `{rand a b}`: an integer in `low..=high`.
    Rand { low: i64, high: i64 },
    /// `{pick a|b|c}`: one of the options.
    Pick(Vec<String>),
    /// `{uuid}`.
    Uuid,
}

impl Placeholder {
    fn parse(inner: &str) -> Result<Self, TemplateError> {
        let text = inner.trim();
        let invalid = |reason: &str| TemplateError::Invalid {
            placeholder: text.to_string(),
            reason: reason.to_string(),
        };
        if text == "seq" {
            return Ok(Self::Seq);
        }
        if text == "now" {
            return Ok(Self::Now);
        }
        if text == "uuid" {
            return Ok(Self::Uuid);
        }
        if let Some(rest) = text.strip_prefix("seq")
            && let Some(modulus) = rest.trim_start().strip_prefix('%')
        {
            let n: u64 = modulus
                .trim()
                .parse()
                .map_err(|_| invalid("the modulus is not a whole number"))?;
            if n == 0 {
                return Err(invalid("the modulus must be at least 1"));
            }
            return Ok(Self::SeqMod(n));
        }
        let mut words = text.split_whitespace();
        match words.next() {
            Some("rand") => {
                let bounds: Vec<&str> = words.collect();
                let [low, high] = bounds.as_slice() else {
                    return Err(invalid("write `{rand a b}` with two integers"));
                };
                let low: i64 = low
                    .parse()
                    .map_err(|_| invalid("the lower bound is not an integer"))?;
                let high: i64 = high
                    .parse()
                    .map_err(|_| invalid("the upper bound is not an integer"))?;
                if low > high {
                    return Err(invalid("the lower bound is above the upper bound"));
                }
                Ok(Self::Rand { low, high })
            }
            Some("pick") => {
                let options = text["pick".len()..].trim();
                if options.is_empty() {
                    return Err(invalid("write `{pick a|b|c}` with at least one option"));
                }
                Ok(Self::Pick(
                    options.split('|').map(|o| o.trim().to_string()).collect(),
                ))
            }
            _ => Err(TemplateError::Unknown(text.to_string())),
        }
    }

    /// Whether the placeholder yields a number.
    const fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Seq | Self::SeqMod(_) | Self::Now | Self::Rand { .. }
        )
    }

    fn render(&self, scope: &mut Scope<'_>) -> Value {
        match self {
            Self::Seq => Value::from(scope.seq),
            Self::SeqMod(n) => Value::from(scope.seq % n),
            Self::Now => Value::from(scope.now),
            Self::Rand { low, high } => {
                let span = i128::from(*high) - i128::from(*low) + 1;
                let draw = (scope.rand)(u64::try_from(span).unwrap_or(u64::MAX));
                let value = i128::from(*low) + i128::from(draw);
                Value::from(i64::try_from(value).unwrap_or(*high))
            }
            Self::Pick(options) => {
                let n = u64::try_from(options.len()).unwrap_or(u64::MAX);
                let index = usize::try_from((scope.rand)(n)).unwrap_or(0);
                Value::String(options.get(index).cloned().unwrap_or_default())
            }
            Self::Uuid => Value::String(uuid_for(scope.seq)),
        }
    }
}

/// What a template reads while it renders one record.
pub struct Scope<'a> {
    /// The record's sequence number.
    pub seq: u64,
    /// The logical time in milliseconds.
    pub now: Millis,
    /// A deterministic draw in `0..n`: [`Ctx::rand`](crate::lab::net::Ctx::rand)
    /// for a node.
    pub rand: &'a mut dyn FnMut(u64) -> u64,
}

/// One piece of a template.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Part {
    Text(String),
    Hole(Placeholder),
}

/// A parsed text template. See the module documentation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Template {
    parts: Vec<Part>,
}

impl Template {
    /// Parse `text`.
    ///
    /// # Errors
    /// Returns the first problem: an unclosed or unmatched brace, an unknown
    /// placeholder, or a placeholder with bad arguments.
    pub fn parse(text: &str) -> Result<Self, TemplateError> {
        let mut parts = Vec::new();
        let mut literal = String::new();
        let mut chars = text.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    literal.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    literal.push('}');
                }
                '}' => return Err(TemplateError::Unmatched(text.to_string())),
                '{' => {
                    let mut inner = String::new();
                    let mut closed = false;
                    for c in chars.by_ref() {
                        if c == '}' {
                            closed = true;
                            break;
                        }
                        inner.push(c);
                    }
                    if !closed {
                        return Err(TemplateError::Unclosed(text.to_string()));
                    }
                    if !literal.is_empty() {
                        parts.push(Part::Text(std::mem::take(&mut literal)));
                    }
                    parts.push(Part::Hole(Placeholder::parse(&inner)?));
                }
                c => literal.push(c),
            }
        }
        if !literal.is_empty() {
            parts.push(Part::Text(literal));
        }
        Ok(Self { parts })
    }

    /// The placeholders of the template, in order.
    pub fn placeholders(&self) -> impl Iterator<Item = &Placeholder> {
        self.parts.iter().filter_map(|p| match p {
            Part::Hole(h) => Some(h),
            Part::Text(_) => None,
        })
    }

    /// Render the template as text.
    pub fn render(&self, scope: &mut Scope<'_>) -> String {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Hole(hole) => match hole.render(scope) {
                    Value::String(s) => out.push_str(&s),
                    other => out.push_str(&other.to_string()),
                },
            }
        }
        out
    }

    /// Render the template as a JSON value: the number itself when the
    /// template is exactly one numeric placeholder, else a string.
    pub fn render_json(&self, scope: &mut Scope<'_>) -> Value {
        match self.parts.as_slice() {
            [Part::Hole(hole)] if hole.is_numeric() => hole.render(scope),
            _ => Value::String(self.render(scope)),
        }
    }
}

/// One node of a [`JsonTemplate`].
#[derive(Clone, PartialEq, Debug)]
enum JsonNode {
    /// A number, a boolean or a null, copied as is.
    Literal(Value),
    Text(Template),
    Array(Vec<JsonNode>),
    Object(Vec<(String, JsonNode)>),
}

impl JsonNode {
    fn parse(doc: &Value) -> Result<Self, TemplateError> {
        Ok(match doc {
            Value::String(text) => Self::Text(Template::parse(text)?),
            Value::Array(items) => {
                Self::Array(items.iter().map(Self::parse).collect::<Result<_, _>>()?)
            }
            Value::Object(fields) => Self::Object(
                fields
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), Self::parse(v)?)))
                    .collect::<Result<_, TemplateError>>()?,
            ),
            other => Self::Literal(other.clone()),
        })
    }

    fn render(&self, scope: &mut Scope<'_>) -> Value {
        match self {
            Self::Literal(value) => value.clone(),
            Self::Text(template) => template.render_json(scope),
            Self::Array(items) => Value::Array(items.iter().map(|i| i.render(scope)).collect()),
            Self::Object(fields) => {
                let mut map = Map::new();
                for (key, node) in fields {
                    map.insert(key.clone(), node.render(scope));
                }
                Value::Object(map)
            }
        }
    }
}

/// A JSON document whose string values are templates. See the module
/// documentation.
#[derive(Clone, PartialEq, Debug)]
pub struct JsonTemplate {
    root: JsonNode,
}

impl JsonTemplate {
    /// Parse every string value of `doc` as a template.
    ///
    /// # Errors
    /// Returns the first string value that does not parse.
    pub fn parse(doc: &Value) -> Result<Self, TemplateError> {
        Ok(Self {
            root: JsonNode::parse(doc)?,
        })
    }

    /// Render the document for one record.
    pub fn render(&self, scope: &mut Scope<'_>) -> Value {
        self.root.render(scope)
    }
}

/// A version 4 UUID that depends on `seq` alone, in its hyphenated form.
#[must_use]
pub fn uuid_for(seq: u64) -> String {
    let high = splitmix64(seq);
    let low = splitmix64(seq ^ 0xD1B5_4A32_D192_ED03);
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    uuid::Builder::from_random_bytes(bytes)
        .into_uuid()
        .hyphenated()
        .to_string()
}

/// The `SplitMix64` finalizer: a fixed bijective mix of 64 bits.
const fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    /// Render with a generator that returns the draws in order, and record
    /// every bound it was asked for.
    fn render_with(template: &str, seq: u64, draws: &[u64]) -> (String, Vec<u64>) {
        let parsed = Template::parse(template).unwrap();
        let mut asked = Vec::new();
        let mut draws = draws.iter().copied();
        let mut rand = |n: u64| {
            asked.push(n);
            draws.next().unwrap_or(0)
        };
        let mut scope = Scope {
            seq,
            now: 1_234,
            rand: &mut rand,
        };
        let out = parsed.render(&mut scope);
        (out, asked)
    }

    #[test]
    fn every_placeholder_renders() {
        // (template, seq, draws, rendered, bounds asked for)
        type Case = (&'static str, u64, Vec<u64>, String, Vec<u64>);
        let uuid = uuid_for(7);
        let cases: Vec<Case> = vec![
            ("order {seq}", 7, vec![], "order 7".to_string(), vec![]),
            (
                "customer-{seq % 10}",
                27,
                vec![],
                "customer-7".to_string(),
                vec![],
            ),
            ("c{seq%3}", 5, vec![], "c2".to_string(), vec![]),
            ("{ seq % 4 }", 9, vec![], "1".to_string(), vec![]),
            ("t={now}", 0, vec![], "t=1234".to_string(), vec![]),
            ("{rand 1 500}", 0, vec![41], "42".to_string(), vec![500]),
            ("{rand -5 5}", 0, vec![0], "-5".to_string(), vec![11]),
            ("{rand 3 3}", 0, vec![0], "3".to_string(), vec![1]),
            (
                "{pick red|green|blue}",
                0,
                vec![1],
                "green".to_string(),
                vec![3],
            ),
            ("{pick a | b }", 0, vec![1], "b".to_string(), vec![2]),
            ("{uuid}", 7, vec![], uuid, vec![]),
            (
                "{{literal}} {seq}",
                1,
                vec![],
                "{literal} 1".to_string(),
                vec![],
            ),
            ("plain", 0, vec![], "plain".to_string(), vec![]),
        ];
        for (template, seq, draws, expected, asked) in cases {
            assert!(
                render_with(template, seq, &draws) == (expected, asked),
                "{template}"
            );
        }
    }

    #[test]
    fn a_bad_template_names_its_problem() {
        let cases = [
            ("{seq", TemplateError::Unclosed("{seq".to_string())),
            ("a}b", TemplateError::Unmatched("a}b".to_string())),
            ("{nope}", TemplateError::Unknown("nope".to_string())),
            (
                "{seq % 0}",
                TemplateError::Invalid {
                    placeholder: "seq % 0".to_string(),
                    reason: "the modulus must be at least 1".to_string(),
                },
            ),
            (
                "{seq % x}",
                TemplateError::Invalid {
                    placeholder: "seq % x".to_string(),
                    reason: "the modulus is not a whole number".to_string(),
                },
            ),
            (
                "{rand 5 1}",
                TemplateError::Invalid {
                    placeholder: "rand 5 1".to_string(),
                    reason: "the lower bound is above the upper bound".to_string(),
                },
            ),
            (
                "{rand 1}",
                TemplateError::Invalid {
                    placeholder: "rand 1".to_string(),
                    reason: "write `{rand a b}` with two integers".to_string(),
                },
            ),
            (
                "{pick }",
                TemplateError::Invalid {
                    placeholder: "pick".to_string(),
                    reason: "write `{pick a|b|c}` with at least one option".to_string(),
                },
            ),
        ];
        for (template, expected) in cases {
            assert!(Template::parse(template) == Err(expected), "{template}");
        }
    }

    #[test]
    fn a_json_template_renders_numbers_for_whole_numeric_placeholders() {
        let template = JsonTemplate::parse(&json!({
            "id": "{seq}",
            "total": "{rand 1 500}",
            "label": "order {seq}",
            "at": "{now}",
            "color": "{pick red|blue}",
            "fixed": 3.5,
            "flag": true,
            "none": null,
            "tags": ["{seq % 2}", "x"],
            "nested": { "uuid": "{uuid}" },
        }))
        .unwrap();
        // Object keys render in sorted order: `color` draws before `total`.
        let mut draws = [1_u64, 99].into_iter();
        let mut rand = |_: u64| draws.next().unwrap_or(0);
        let mut scope = Scope {
            seq: 3,
            now: 250,
            rand: &mut rand,
        };
        assert!(
            template.render(&mut scope)
                == json!({
                    "id": 3,
                    "total": 100,
                    "label": "order 3",
                    "at": 250,
                    "color": "blue",
                    "fixed": 3.5,
                    "flag": true,
                    "none": null,
                    "tags": [1, "x"],
                    "nested": { "uuid": uuid_for(3) },
                })
        );
    }

    #[test]
    fn a_json_template_refuses_a_bad_string_anywhere() {
        let err = JsonTemplate::parse(&json!({ "a": [1, { "b": "{bogus}" }] })).unwrap_err();
        assert!(err == TemplateError::Unknown("bogus".to_string()));
    }

    #[test]
    fn the_uuid_is_a_version_4_uuid_fixed_by_the_sequence_number() {
        let a = uuid_for(1);
        assert!(a == uuid_for(1));
        assert!(a != uuid_for(2));
        let parsed = uuid::Uuid::parse_str(&a).unwrap();
        assert!(parsed.get_version_num() == 4);
        assert!(parsed.get_variant() == uuid::Variant::RFC4122);
    }

    #[test]
    fn placeholders_are_listed_in_order() {
        let template = Template::parse("{seq}-{now}-{uuid}").unwrap();
        let listed: Vec<&Placeholder> = template.placeholders().collect();
        assert!(listed == vec![&Placeholder::Seq, &Placeholder::Now, &Placeholder::Uuid]);
    }
}
