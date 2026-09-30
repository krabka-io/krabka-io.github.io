//! The broker's log: one JSON object per line on stderr, which the lab's Logs
//! tab reads.
//!
//! Each line has `ts` (seconds since the process started, a number with
//! millisecond precision: the lab's clock, because the page drives the WASI
//! clock), `level` (`TRACE` through `ERROR`), `target` and `message`, then the
//! event's own fields at the top level. A field that is called like one of
//! those four keys gets a `field_` prefix, so it cannot shadow them.
//!
//! `KRABKA_LOG` picks the levels: a comma-separated list whose entries are a
//! bare level (`trace`, `debug`, `info`, `warn`, `error`, `off`) or
//! `target=level`, for example `warn,krabka_broker=debug`. Unset or empty, the
//! default applies: `INFO`, with the broker's request and connection log at
//! `WARN`.

use std::{
    fmt::{self, Write as _},
    io,
    time::{Duration, Instant},
};

use serde_json::Value;
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    filter::{LevelFilter, ParseError, Targets},
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    layer::SubscriberExt as _,
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

/// The environment variable that holds the log directive.
const ENV_VAR: &str = "KRABKA_LOG";
/// The broker's module that logs each request and connection.
const REQUEST_LOG: &str = "krabka_broker::network::dispatch";
/// The keys every line has, which an event's fields must not reuse.
const RESERVED: [&str; 4] = ["ts", "level", "target", "message"];

/// Installs the JSON log on stderr, stamped with the time since `started`.
/// An unparsable `KRABKA_LOG` logs one `ERROR` line and leaves the default.
pub fn init(started: Instant) {
    let directive = std::env::var(ENV_VAR).ok();
    let (filter, rejected) = filter(directive.as_deref());
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::stderr)
                .event_format(JsonLines(started)),
        )
        .with(filter)
        .init();
    if let Some(why) = rejected {
        tracing::error!("{why}");
    }
}

/// The filter for a `KRABKA_LOG` value, and why the value was refused when it
/// was. The broker logs every request it dispatches and every connection it
/// accepts at `INFO`, where Kafka's default logging configuration keeps its
/// request logger (`kafka.request.logger`) at `WARN` and logs accepted
/// connections at `DEBUG`; left in, they would drown the lab's log.
fn filter(directive: Option<&str>) -> (Targets, Option<String>) {
    let default = || {
        Targets::new()
            .with_default(Level::INFO)
            .with_target(REQUEST_LOG, Level::WARN)
    };
    match directive.map(str::trim).filter(|text| !text.is_empty()) {
        None => (default(), None),
        Some(text) => match parse(text) {
            Ok(targets) => (targets, None),
            Err(why) => (
                default(),
                Some(format!(
                    "{ENV_VAR} {text:?} is not a log directive, so the default applies: {why}"
                )),
            ),
        },
    }
}

/// Parses a directive. `Targets` reads a bare word that is no level as a
/// target at `TRACE`, so `inf` would silence everything else; here it is an
/// error.
fn parse(text: &str) -> Result<Targets, String> {
    for entry in text.split(',') {
        let entry = entry.trim();
        if !entry.contains('=') && entry.parse::<LevelFilter>().is_err() {
            return Err(format!("{entry:?} is neither a level nor target=level"));
        }
    }
    text.parse().map_err(|err: ParseError| err.to_string())
}

/// Formats an event as one JSON line.
struct JsonLines(Instant);

impl<S, N> FormatEvent<S, N> for JsonLines
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        writeln!(
            writer,
            "{}",
            render(
                self.0.elapsed(),
                *meta.level(),
                meta.target(),
                fields.message.as_deref(),
                &fields.rest
            )
        )
    }
}

/// An event's fields: the message apart, the others in order.
#[derive(Default)]
struct Fields {
    message: Option<String>,
    rest: Vec<(String, Value)>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: Value) {
        let name = field.name();
        if name == "message" {
            self.message = Some(match value {
                Value::String(text) => text,
                other => other.to_string(),
            });
        } else if RESERVED.contains(&name) {
            self.rest.push((format!("field_{name}"), value));
        } else {
            self.rest.push((name.to_owned(), value));
        }
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, Value::String(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::String(value.to_owned()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        // Not a JSON number when it is NaN or infinite: `null` then.
        self.put(field, value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::String(value.to_string()));
    }
}

/// The line for one event, without its newline. The four fixed keys come
/// first, in the order the lab's viewer shows them.
fn render(
    elapsed: Duration,
    level: Level,
    target: &str,
    message: Option<&str>,
    fields: &[(String, Value)],
) -> String {
    let mut line = format!(
        "{{\"ts\":{}.{:03},\"level\":\"{level}\",\"target\":{},\"message\":{}",
        elapsed.as_secs(),
        elapsed.subsec_millis(),
        Value::from(target),
        Value::from(message.unwrap_or_default()),
    );
    for (name, value) in fields {
        let _ = write!(line, ",{}:{value}", Value::from(name.as_str()));
    }
    line.push('}');
    line
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A stderr stand-in that keeps what the layer writes.
    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Runs `emit` under the filter of `directive` and returns the lines it
    /// logged, parsed.
    fn logged(directive: Option<&str>, emit: impl FnOnce()) -> Vec<Value> {
        let sink = Sink::default();
        let writer = sink.clone();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || writer.clone())
                    .event_format(JsonLines(Instant::now())),
            )
            .with(filter(directive).0);
        tracing::subscriber::with_default(subscriber, emit);
        let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        text.lines()
            .map(|line| serde_json::from_str(line).expect("every line is JSON"))
            .collect()
    }

    fn emit_every_level() {
        tracing::trace!("t");
        tracing::debug!("d");
        tracing::info!("i");
        tracing::warn!("w");
        tracing::error!("e");
    }

    fn levels(lines: &[Value]) -> Vec<&str> {
        lines.iter().map(|l| l["level"].as_str().unwrap()).collect()
    }

    #[test]
    fn the_default_is_info() {
        for directive in [None, Some(""), Some("  ")] {
            let lines = logged(directive, emit_every_level);
            assert_eq!(levels(&lines), ["INFO", "WARN", "ERROR"]);
        }
    }

    #[test]
    fn a_bare_level_sets_every_target() {
        assert_eq!(
            levels(&logged(Some("trace"), emit_every_level)),
            ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]
        );
        assert_eq!(
            levels(&logged(Some("warn"), emit_every_level)),
            ["WARN", "ERROR"]
        );
        assert!(logged(Some("off"), emit_every_level).is_empty());
    }

    #[test]
    fn a_target_entry_overrides_the_bare_level() {
        let directive = format!("warn,{}=debug", module_path!());
        let lines = logged(Some(&directive), emit_every_level);
        assert_eq!(levels(&lines), ["DEBUG", "INFO", "WARN", "ERROR"]);
        let lines = logged(Some("warn,other=trace"), emit_every_level);
        assert_eq!(levels(&lines), ["WARN", "ERROR"]);
    }

    #[test]
    fn the_request_log_stays_at_warn_by_default_only() {
        let (default, _) = filter(None);
        assert!(!default.would_enable(REQUEST_LOG, &Level::INFO));
        assert!(default.would_enable(REQUEST_LOG, &Level::WARN));
        assert!(default.would_enable("krabka_broker::raft", &Level::INFO));
        let (verbose, _) = filter(Some("debug"));
        assert!(verbose.would_enable(REQUEST_LOG, &Level::DEBUG));
    }

    #[test]
    fn an_unparsable_directive_falls_back_and_says_why() {
        for bad in ["info,krabka=loud", "verbose", "debug,a=b=c"] {
            let (targets, why) = filter(Some(bad));
            let why = why.unwrap_or_else(|| panic!("{bad:?} was accepted"));
            assert!(why.contains(bad) && why.contains("default applies"), "{why}");
            assert!(targets.would_enable("x", &Level::INFO));
            assert!(!targets.would_enable("x", &Level::DEBUG));
        }
        assert!(filter(Some("info,krabka_broker=debug")).1.is_none());
    }

    #[test]
    fn a_line_has_the_lab_keys_and_the_events_fields() {
        let lines = logged(Some("info"), || {
            tracing::info!(
                node_id = 2,
                voter = true,
                ratio = 0.5,
                name = "raft",
                mode = ?Level::INFO,
                ts = 7,
                "starting the \"broker\""
            );
        });
        let [line] = &lines[..] else { panic!("{lines:?}") };
        assert_eq!(line["level"], "INFO");
        assert_eq!(line["target"], module_path!());
        assert_eq!(line["mode"], "Level(Info)");
        assert_eq!(line["message"], "starting the \"broker\"");
        assert_eq!(line["node_id"], 2);
        assert_eq!(line["voter"], true);
        assert_eq!(line["ratio"], 0.5);
        assert_eq!(line["name"], "raft");
        assert_eq!(line["field_ts"], 7);
        assert!(line["ts"].is_f64() || line["ts"].is_u64(), "{line}");
    }

    #[test]
    fn the_timestamp_is_seconds_with_milliseconds() {
        let line = render(
            Duration::from_micros(12_345_678),
            Level::WARN,
            "a::b",
            Some("m"),
            &[],
        );
        assert_eq!(
            line,
            r#"{"ts":12.345,"level":"WARN","target":"a::b","message":"m"}"#
        );
    }
}
