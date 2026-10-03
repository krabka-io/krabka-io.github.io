//! JSON log lines on stderr, the way the real broker writes them
//! (`playground/broker-wasi/src/logging.rs`), so the lab's Logs tab and its
//! level settings can be tested without the broker's 13.6 MB module.
//!
//! One object per line: `ts` (seconds since the process started, to the
//! millisecond), `level` (`TRACE` through `ERROR`), `target`, `message`, then
//! the event's fields. `KRABKA_LOG` picks the levels, in the broker's
//! syntax: comma-separated bare levels (`off` too) and `target=level`
//! entries, the longest matching target prefix winning; unset or empty is
//! `info`. A value that does not parse logs one `ERROR` and means `info`.

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::Instant;

/// Levels in increasing severity. `Off` is a filter only, never an event's.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl Level {
    fn parse(text: &str) -> Option<Self> {
        Some(match text.trim().to_ascii_lowercase().as_str() {
            "trace" => Self::Trace,
            "debug" => Self::Debug,
            "info" => Self::Info,
            "warn" => Self::Warn,
            "error" => Self::Error,
            "off" => Self::Off,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error | Self::Off => "ERROR",
        }
    }
}

/// What `KRABKA_LOG` allows.
struct Filter {
    /// The level of a target no entry names.
    default: Level,
    /// `target=level` entries.
    targets: Vec<(String, Level)>,
}

impl Filter {
    fn parse(text: &str) -> Result<Self, String> {
        let mut filter = Self { default: Level::Info, targets: Vec::new() };
        for entry in text.split(',') {
            let entry = entry.trim();
            match entry.split_once('=') {
                Some((target, level)) => filter.targets.push((
                    target.trim().to_owned(),
                    Level::parse(level).ok_or_else(|| format!("{entry:?} has no level"))?,
                )),
                None => {
                    filter.default = Level::parse(entry)
                        .ok_or_else(|| format!("{entry:?} is neither a level nor target=level"))?;
                }
            }
        }
        Ok(filter)
    }

    fn allows(&self, level: Level, target: &str) -> bool {
        let floor = self
            .targets
            .iter()
            .filter(|(prefix, _)| {
                target == prefix
                    || target.strip_prefix(prefix.as_str()).is_some_and(|rest| rest.starts_with("::"))
            })
            .max_by_key(|(prefix, _)| prefix.len())
            .map_or(self.default, |(_, level)| *level);
        level >= floor
    }
}

static FILTER: OnceLock<Filter> = OnceLock::new();
static STARTED: OnceLock<Instant> = OnceLock::new();

/// Reads `KRABKA_LOG` and starts the `ts` clock at `started`.
pub fn init(started: Instant) {
    let _ = STARTED.set(started);
    let directive = std::env::var("KRABKA_LOG").unwrap_or_default();
    let filter = if directive.trim().is_empty() {
        Filter::parse("info")
    } else {
        Filter::parse(&directive)
    };
    match filter {
        Ok(filter) => {
            let _ = FILTER.set(filter);
        }
        Err(why) => {
            let _ = FILTER.set(Filter::parse("info").expect("info is a level"));
            log(
                Level::Error,
                "krabka_wasi_guest::log",
                &format!("KRABKA_LOG {directive:?} is not a log directive, so the default applies: {why}"),
                &[],
            );
        }
    }
}

/// Logs one event, when the filter allows it. `fields` are extra keys with
/// their values already in JSON form.
pub fn log(level: Level, target: &str, message: &str, fields: &[(&str, String)]) {
    if FILTER.get().is_some_and(|filter| !filter.allows(level, target)) {
        return;
    }
    let elapsed = STARTED.get().map_or_else(Default::default, Instant::elapsed);
    let mut line = format!(
        "{{\"ts\":{}.{:03},\"level\":\"{}\",\"target\":{},\"message\":{}",
        elapsed.as_secs(),
        elapsed.subsec_millis(),
        level.name(),
        quote(target),
        quote(message),
    );
    for (key, value) in fields {
        let _ = write!(line, ",{}:{value}", quote(key));
    }
    line.push('}');
    eprintln!("{line}");
}

/// `text` as a JSON string.
pub fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
