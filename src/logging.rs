use std::fmt;

use chrono::Local;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{
    fmt::{format, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
};

/// Custom event formatter.
///
/// Output (plain):
/// ```text
/// 10:00:00  INFO  services::build               Loading 'STIB'...
/// 10:00:01 DEBUG  ingestion::gtfs::gtfs          - 0 nodes without geo data
/// 10:00:05  WARN  ingestion::gtfs::sncb         Pattern: 3 routed, 1 fallback
/// ```
///
/// When writing to a TTY the level and target are ANSI-coloured.
/// The `maas_rs::` crate prefix is stripped from module paths.
pub struct MaasFormat;

impl<S, N> FormatEvent<S, N> for MaasFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let level = *meta.level();
        let target = meta
            .target()
            .strip_prefix("maas_rs::")
            .unwrap_or(meta.target());
        let now = Local::now().format("%H:%M:%S");

        if writer.has_ansi_escapes() {
            let (color, label) = level_style(level);
            // dim time, coloured level, dim target
            write!(
                writer,
                "\x1b[2m{now}\x1b[0m {color}{label}\x1b[0m \x1b[2m{target:<36}\x1b[0m ",
            )?;
        } else {
            write!(writer, "{now} {} {target:<36} ", level_label(level))?;
        }

        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

fn level_label(level: Level) -> &'static str {
    match level {
        Level::TRACE => "TRACE",
        Level::DEBUG => "DEBUG",
        Level::INFO => " INFO",
        Level::WARN => " WARN",
        Level::ERROR => "ERROR",
    }
}

fn level_style(level: Level) -> (&'static str, &'static str) {
    // (ANSI color escape, right-padded label)
    match level {
        Level::TRACE => ("\x1b[35m", "TRACE"), // magenta
        Level::DEBUG => ("\x1b[36m", "DEBUG"), // cyan
        Level::INFO => ("\x1b[32m", " INFO"),  // green
        Level::WARN => ("\x1b[33m", " WARN"),  // yellow
        Level::ERROR => ("\x1b[31m", "ERROR"), // red
    }
}

pub fn init(log_level: &str) {
    use tracing_subscriber::{filter::LevelFilter, fmt};

    let level = match log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::TRACE,
        "debug" => LevelFilter::DEBUG,
        "warn" | "warning" => LevelFilter::WARN,
        "error" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    };

    fmt().event_format(MaasFormat).with_max_level(level).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_label_padding() {
        // All labels must be exactly 5 chars so columns align
        assert_eq!(level_label(Level::TRACE).len(), 5);
        assert_eq!(level_label(Level::DEBUG).len(), 5);
        assert_eq!(level_label(Level::INFO).len(), 5);
        assert_eq!(level_label(Level::WARN).len(), 5);
        assert_eq!(level_label(Level::ERROR).len(), 5);
    }

    #[test]
    fn test_level_style_label_padding() {
        for level in [Level::TRACE, Level::DEBUG, Level::INFO, Level::WARN, Level::ERROR] {
            let (_, label) = level_style(level);
            assert_eq!(label.len(), 5, "label for {level} must be 5 chars");
        }
    }
}
