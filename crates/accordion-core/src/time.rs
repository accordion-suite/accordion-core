//! Timestamps in the database's format, for times Accordion records outside
//! a SQL statement.

use std::time::Duration;

/// `since_epoch` as ISO 8601 in UTC with milliseconds, such as
/// `2026-10-22T07:00:49.220Z`, the same format as [`crate::sql_now`].
pub(crate) fn format(since_epoch: Duration) -> String {
    let secs = since_epoch.as_secs();
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let rest = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        since_epoch.subsec_millis()
    )
}

/// The date `days` after January 1, 1970, by Howard Hinnant's algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_database_format() {
        assert_eq!(format(Duration::ZERO), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format(Duration::from_millis(1_792_652_449_220)),
            "2026-10-22T07:00:49.220Z"
        );
        // A leap day.
        assert_eq!(
            format(Duration::from_secs(951_782_400)),
            "2000-02-29T00:00:00.000Z"
        );
    }
}
