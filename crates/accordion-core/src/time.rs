//! Timestamps in the database's format, for times Accordion records or
//! reads outside a SQL statement.

use std::time::{Duration, SystemTime};

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

/// Reads a timestamp in the database's format back into a time.
pub(crate) fn parse(text: &str) -> Option<SystemTime> {
    let bytes = text.as_bytes();
    let well_formed = bytes.len() == 24
        && bytes.iter().enumerate().all(|(i, &b)| match i {
            4 | 7 => b == b'-',
            10 => b == b'T',
            13 | 16 => b == b':',
            19 => b == b'.',
            23 => b == b'Z',
            _ => b.is_ascii_digit(),
        });
    if !well_formed {
        return None;
    }
    let number = |range: std::ops::Range<usize>| text[range].parse::<u32>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let millis = number(20..23)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let days = u64::try_from(days_from_civil(i64::from(year), month, day)).ok()?;
    let secs = days * 86_400 + u64::from(hour * 3600 + minute * 60 + second);
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis.into()))
}

/// Days from January 1, 1970 to a date, by Howard Hinnant's algorithm.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let month = i64::from(month);
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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

    #[test]
    fn reads_its_own_format_back() {
        for millis in [0, 951_782_400_000, 1_792_652_449_220, 4_102_444_799_999] {
            let time = SystemTime::UNIX_EPOCH + Duration::from_millis(millis);
            let text = format(Duration::from_millis(millis));
            assert_eq!(parse(&text), Some(time), "{text}");
        }
        assert_eq!(parse("2026-13-01T00:00:00.000Z"), None);
        assert_eq!(parse("2026-10-22 07:00:49.220Z"), None);
        assert_eq!(parse("yesterday"), None);
    }
}
