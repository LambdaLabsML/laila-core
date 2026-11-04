//! Python `datetime` (`datetime`, `timedelta`, `timezone`) with
//! CPython-identical formatting and parsing, `no_std` + `alloc`.
//!
//! Only fixed-offset time zones exist (`timezone.utc` and
//! `timezone(timedelta(..))`), which is all `datetime.fromisoformat` can
//! produce and all laila emits. Where CPython consults the platform's local
//! time (naive `fromtimestamp`, naive `timestamp()`, naive `astimezone`) this
//! port uses UTC instead -- i.e. naive values behave as `utcfromtimestamp` /
//! "naive UTC"; laila only ever creates aware UTC values.
//!
//! Current time: this module has no clock. `DateTime::now_from(secs)` is
//! `datetime.now(UTC)` for a `time.time()` reading; the integrator wires
//! `now()` as `DateTime::now_from(crate::python::stdlib::time.time())` behind
//! the `std` feature.
//!
//! Calendar arithmetic uses Howard Hinnant's `days_from_civil` /
//! `civil_from_days` on integers (no floating point drift); wall-clock values
//! are exact microsecond counts since 1970-01-01 in `i64` (year 1..9999 spans
//! ±3.2e17 µs).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use crate::_compat::fmath;
use crate::python::builtins::IsInstance;
use crate::python::errors::ExcKind;
use crate::python::object::{Arg, FnvHasher, IntoArg, PyArg, PyObject};
use crate::python::{LailaError, Result};

/// `datetime.MINYEAR`.
pub const MINYEAR: i64 = 1;
/// `datetime.MAXYEAR`.
pub const MAXYEAR: i64 = 9999;
/// `timedelta.max.days`.
pub const MAX_DELTA_DAYS: i64 = 999_999_999;

const US_PER_SECOND: i64 = 1_000_000;
const US_PER_DAY: i64 = 86_400 * US_PER_SECOND;
/// `days_from_civil(1, 1, 1)`: 0001-01-01 relative to 1970-01-01.
const DAYS_0001_01_01: i64 = -719_162;

const DAY_NAMES: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];
const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

// ---------------------------------------------------------------------------
// Civil calendar (Howard Hinnant, "chrono-Compatible Low-Level Date Algorithms").
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 of the proleptic Gregorian date `y-m-d`.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The proleptic Gregorian date `(y, m, d)` of a day count since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `calendar.isleap(y)`.
pub fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Number of days in month `m` of year `y`.
pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        _ => 28,
    }
}

/// `date.toordinal()`: 0001-01-01 is day 1.
pub fn ymd_to_ord(y: i64, m: u32, d: u32) -> i64 {
    days_from_civil(y, m, d) - DAYS_0001_01_01 + 1
}

/// `date.weekday()`: Monday is 0.
pub fn weekday(y: i64, m: u32, d: u32) -> u32 {
    ((ymd_to_ord(y, m, d) + 6).rem_euclid(7)) as u32
}

/// Ordinal of the Monday starting ISO week 1 of `year`.
fn iso_week1_monday(year: i64) -> i64 {
    let first_day = ymd_to_ord(year, 1, 1);
    let first_weekday = (first_day + 6) % 7;
    let mut week1_monday = first_day - first_weekday;
    if first_weekday > 3 {
        week1_monday += 7;
    }
    week1_monday
}

/// `(year, month, day)` of a proleptic Gregorian ordinal (`date.fromordinal`).
fn ord_to_ymd(ord: i64) -> (i64, u32, u32) {
    civil_from_days(ord + DAYS_0001_01_01 - 1)
}

fn div_floor(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

/// Correctly rounded `num / den` as `f64` (Python's `int / int`): the exact
/// quotient is rounded to nearest-even once, unlike `num as f64 / den as f64`.
fn ratio_to_f64(num: i128, den: i128) -> f64 {
    if num == 0 {
        return 0.0;
    }
    let neg = (num < 0) != (den < 0);
    let (mut n, d) = (num.unsigned_abs(), den.unsigned_abs());
    // Scale so that the integer quotient has exactly 53 bits.
    let mut exp: i32 = 0;
    while n / d < (1u128 << 52) {
        n <<= 1;
        exp -= 1;
    }
    let mut dd = d;
    while n / dd >= (1u128 << 53) {
        dd <<= 1;
        exp += 1;
    }
    let mut q = n / dd;
    let r = n % dd;
    // Round half to even on the exact remainder.
    if 2 * r > dd || (2 * r == dd && q & 1 == 1) {
        q += 1;
    }
    let mut v = q as f64; // exact: q <= 2^53
                          // Apply the binary exponent in exact power-of-two steps.
    while exp > 0 {
        v *= 2.0;
        exp -= 1;
    }
    while exp < 0 {
        v /= 2.0;
        exp += 1;
    }
    if neg {
        -v
    } else {
        v
    }
}

/// CPython's `_PyTime_ROUND_HALF_EVEN` on a double.
pub fn round_half_even(x: f64) -> f64 {
    let rounded = fmath::round(x);
    if (x - rounded).abs() == 0.5 {
        2.0 * fmath::round(x / 2.0)
    } else {
        rounded
    }
}

// ---------------------------------------------------------------------------
// timedelta
// ---------------------------------------------------------------------------

/// `datetime.timedelta`, normalised like CPython: `0 <= seconds < 86400`,
/// `0 <= microseconds < 1000000`, `|days| <= 999999999`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeDelta {
    pub days: i32,
    pub seconds: i32,
    pub microseconds: i32,
}

impl TimeDelta {
    /// `timedelta(0)`.
    pub const ZERO: TimeDelta = TimeDelta {
        days: 0,
        seconds: 0,
        microseconds: 0,
    };

    /// `timedelta(days=.., seconds=.., microseconds=..)` (integers; the sum
    /// is normalised). `OverflowError` beyond `timedelta.max`.
    pub fn new(days: i64, seconds: i64, microseconds: i64) -> Result<TimeDelta> {
        let total = days as i128 * US_PER_DAY as i128
            + seconds as i128 * US_PER_SECOND as i128
            + microseconds as i128;
        Self::from_micros(total)
    }

    /// A `timedelta` from a total microsecond count.
    pub fn from_micros(total_us: i128) -> Result<TimeDelta> {
        let day_us = US_PER_DAY as i128;
        let days = total_us.div_euclid(day_us);
        let rem = total_us.rem_euclid(day_us);
        if days < -(MAX_DELTA_DAYS as i128) || days > MAX_DELTA_DAYS as i128 {
            return Err(LailaError::new(
                ExcKind::OverflowError,
                alloc::format!("days={days}; must have magnitude <= {MAX_DELTA_DAYS}"),
            ));
        }
        Ok(TimeDelta {
            days: days as i32,
            seconds: (rem / US_PER_SECOND as i128) as i32,
            microseconds: (rem % US_PER_SECOND as i128) as i32,
        })
    }

    /// `timedelta(seconds=s)` for a float number of seconds (round half even
    /// to microseconds, like CPython).
    pub fn from_seconds_f64(secs: f64) -> Result<TimeDelta> {
        let us = round_half_even(secs * 1e6);
        Self::from_micros(us as i128)
    }

    /// The exact total in microseconds.
    pub fn total_micros(&self) -> i128 {
        self.days as i128 * US_PER_DAY as i128
            + self.seconds as i128 * US_PER_SECOND as i128
            + self.microseconds as i128
    }

    /// `timedelta.total_seconds()`: `total_microseconds / 10**6` as Python's
    /// true division (correctly rounded).
    pub fn total_seconds(&self) -> f64 {
        ratio_to_f64(self.total_micros(), US_PER_SECOND as i128)
    }

    /// `bool(td)`.
    pub fn __bool__(&self) -> bool {
        *self != Self::ZERO
    }

    /// `-td`.
    pub fn __neg__(&self) -> Result<TimeDelta> {
        Self::from_micros(-self.total_micros())
    }

    /// `abs(td)`.
    pub fn __abs__(&self) -> Result<TimeDelta> {
        if self.days < 0 {
            self.__neg__()
        } else {
            Ok(*self)
        }
    }

    /// `td + other`.
    pub fn __add__(&self, other: TimeDelta) -> Result<TimeDelta> {
        Self::from_micros(self.total_micros() + other.total_micros())
    }

    /// `td - other`.
    pub fn __sub__(&self, other: TimeDelta) -> Result<TimeDelta> {
        Self::from_micros(self.total_micros() - other.total_micros())
    }

    /// `td * n`.
    pub fn __mul__(&self, n: i64) -> Result<TimeDelta> {
        Self::from_micros(self.total_micros() * n as i128)
    }

    /// `repr(td)`: `datetime.timedelta(days=1, seconds=1, microseconds=500000)`, `datetime.timedelta(0)`.
    pub fn __repr__(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.days != 0 {
            parts.push(alloc::format!("days={}", self.days));
        }
        if self.seconds != 0 {
            parts.push(alloc::format!("seconds={}", self.seconds));
        }
        if self.microseconds != 0 {
            parts.push(alloc::format!("microseconds={}", self.microseconds));
        }
        if parts.is_empty() {
            parts.push("0".into());
        }
        alloc::format!("datetime.timedelta({})", parts.join(", "))
    }

    /// `str(td)`: `1 day, 0:00:01.500000`, `-1 day, 23:59:59.999999`.
    pub fn __str__(&self) -> String {
        let us = self.microseconds;
        let (minutes, seconds) = (self.seconds / 60, self.seconds % 60);
        let (hours, minutes) = (minutes / 60, minutes % 60);
        let days = self.days;
        let hms = if us != 0 {
            alloc::format!("{hours}:{minutes:02}:{seconds:02}.{us:06}")
        } else {
            alloc::format!("{hours}:{minutes:02}:{seconds:02}")
        };
        if days != 0 {
            let plural = if days == 1 || days == -1 { "" } else { "s" };
            alloc::format!("{days} day{plural}, {hms}")
        } else {
            hms
        }
    }

    /// `hash(td)`.
    pub fn __hash__(&self) -> i64 {
        let mut h = FnvHasher::default();
        (self.days, self.seconds, self.microseconds).hash(&mut h);
        (h.finish() >> 1) as i64
    }
}

// ---------------------------------------------------------------------------
// timezone
// ---------------------------------------------------------------------------

/// `datetime.timezone`: a fixed UTC offset (`timezone.utc` when zero).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimeZone {
    offset: TimeDelta,
}

impl TimeZone {
    /// `datetime.timezone.utc` (also `datetime.UTC`).
    pub const UTC: TimeZone = TimeZone {
        offset: TimeDelta::ZERO,
    };

    /// `timezone(offset)`; `ValueError` unless strictly within ±24 hours.
    pub fn new(offset: TimeDelta) -> Result<TimeZone> {
        let bad = (offset.days == -1 && offset.seconds == 0 && offset.microseconds < 1)
            || offset.days < -1
            || offset.days >= 1;
        if bad {
            return Err(LailaError::value_error(alloc::format!(
                "offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not {}",
                offset.__repr__()
            )));
        }
        Ok(TimeZone { offset })
    }

    /// `timezone.utc`.
    pub fn utc() -> TimeZone {
        Self::UTC
    }

    /// `tz.utcoffset(dt)`: the fixed offset, whatever `dt` is.
    pub fn utcoffset(&self, _dt: impl IntoArg) -> TimeDelta {
        self.offset
    }

    /// `tz.tzname(None)`: `UTC`, `UTC+05:30`, `UTC-03:00`, `UTC+05:30:15.500000`.
    pub fn tzname(&self) -> String {
        if self.offset == TimeDelta::ZERO {
            return "UTC".into();
        }
        alloc::format!("UTC{}", format_utcoffset(":", self.offset))
    }

    /// `str(tz)` is `tz.tzname(None)`.
    pub fn __str__(&self) -> String {
        self.tzname()
    }

    /// `repr(tz)`: `datetime.timezone.utc` or `datetime.timezone(datetime.timedelta(seconds=19800))`.
    pub fn __repr__(&self) -> String {
        if self.offset == TimeDelta::ZERO {
            "datetime.timezone.utc".into()
        } else {
            alloc::format!("datetime.timezone({})", self.offset.__repr__())
        }
    }

    /// `hash(tz)`.
    pub fn __hash__(&self) -> i64 {
        self.offset.__hash__() ^ 0x747a
    }
}

/// CPython's `format_utcoffset`: `+HH<sep>MM[<sep>SS[.ffffff]]`.
fn format_utcoffset(sep: &str, offset: TimeDelta) -> String {
    let (sign, offset) = if offset.days < 0 {
        // A normalised negative offset has days == -1; negation cannot overflow.
        (
            '-',
            TimeDelta::from_micros(-offset.total_micros()).unwrap_or(TimeDelta::ZERO),
        )
    } else {
        ('+', offset)
    };
    let us = offset.microseconds;
    let (minutes, seconds) = (offset.seconds / 60, offset.seconds % 60);
    let (hours, minutes) = (minutes / 60, minutes % 60);
    if us != 0 {
        alloc::format!("{sign}{hours:02}{sep}{minutes:02}{sep}{seconds:02}.{us:06}")
    } else if seconds != 0 {
        alloc::format!("{sign}{hours:02}{sep}{minutes:02}{sep}{seconds:02}")
    } else {
        alloc::format!("{sign}{hours:02}{sep}{minutes:02}")
    }
}

// ---------------------------------------------------------------------------
// datetime
// ---------------------------------------------------------------------------

/// `datetime.datetime`: naive (`tzinfo == None`) or aware with a fixed
/// offset, microsecond precision, `fold` always 0.
#[derive(Clone, Copy, Debug)]
pub struct DateTime {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
    pub tzinfo: Option<TimeZone>,
}

/// CPython's `check_date_args`.
fn check_date_args(year: i64, month: i64, day: i64) -> Result<()> {
    if !(MINYEAR..=MAXYEAR).contains(&year) {
        return Err(LailaError::value_error(alloc::format!(
            "year must be in {MINYEAR}..{MAXYEAR}, not {year}"
        )));
    }
    if !(1..=12).contains(&month) {
        return Err(LailaError::value_error(alloc::format!(
            "month must be in 1..12, not {month}"
        )));
    }
    let dim = days_in_month(year, month as u32) as i64;
    if day < 1 || day > dim {
        return Err(LailaError::value_error(alloc::format!(
            "day {day} must be in range 1..{dim} for month {month} in year {year}"
        )));
    }
    Ok(())
}

/// CPython's `check_time_args`.
fn check_time_args(h: i64, m: i64, s: i64, us: i64) -> Result<()> {
    if !(0..=23).contains(&h) {
        return Err(LailaError::value_error(alloc::format!(
            "hour must be in 0..23, not {h}"
        )));
    }
    if !(0..=59).contains(&m) {
        return Err(LailaError::value_error(alloc::format!(
            "minute must be in 0..59, not {m}"
        )));
    }
    if !(0..=59).contains(&s) {
        return Err(LailaError::value_error(alloc::format!(
            "second must be in 0..59, not {s}"
        )));
    }
    if !(0..=999_999).contains(&us) {
        return Err(LailaError::value_error(alloc::format!(
            "microsecond must be in 0..999999, not {us}"
        )));
    }
    Ok(())
}

fn timestamp_overflow() -> LailaError {
    LailaError::new(
        ExcKind::OverflowError,
        "timestamp out of range for platform time_t",
    )
}

impl DateTime {
    /// `datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)`
    /// with CPython's range checks and messages.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        minute: i64,
        second: i64,
        microsecond: i64,
        tzinfo: Option<TimeZone>,
    ) -> Result<DateTime> {
        check_date_args(year, month, day)?;
        check_time_args(hour, minute, second, microsecond)?;
        Ok(DateTime {
            year: year as i32,
            month: month as u8,
            day: day as u8,
            hour: hour as u8,
            minute: minute as u8,
            second: second as u8,
            microsecond: microsecond as u32,
            tzinfo,
        })
    }

    /// `dt.year` (attribute spelling; Tier 2 lowers attributes to methods).
    pub fn year(&self) -> i64 {
        self.year as i64
    }
    pub fn month(&self) -> i64 {
        self.month as i64
    }
    pub fn day(&self) -> i64 {
        self.day as i64
    }
    pub fn hour(&self) -> i64 {
        self.hour as i64
    }
    pub fn minute(&self) -> i64 {
        self.minute as i64
    }
    pub fn second(&self) -> i64 {
        self.second as i64
    }
    pub fn microsecond(&self) -> i64 {
        self.microsecond as i64
    }
    /// `dt.tzinfo`
    pub fn tzinfo(&self) -> Option<TimeZone> {
        self.tzinfo
    }
    pub fn fold(&self) -> i64 {
        0
    }

    /// `datetime(year, month, day)` (midnight, naive).
    pub fn from_ymd(year: i64, month: i64, day: i64) -> Result<DateTime> {
        Self::new(year, month, day, 0, 0, 0, 0, None)
    }

    /// `dt.replace(tzinfo=tz)`: same wall clock, different (or no) zone.
    pub fn replace_tzinfo(&self, tzinfo: Option<TimeZone>) -> DateTime {
        DateTime { tzinfo, ..*self }
    }

    /// Wall-clock microseconds since 1970-01-01T00:00:00 (ignores `tzinfo`).
    fn wall_us(&self) -> i64 {
        let days = days_from_civil(self.year as i64, self.month as u32, self.day as u32);
        days * US_PER_DAY
            + (self.hour as i64 * 3600 + self.minute as i64 * 60 + self.second as i64)
                * US_PER_SECOND
            + self.microsecond as i64
    }

    /// Microseconds since the epoch of the instant (aware), or of the wall
    /// clock read as UTC (naive).
    fn utc_us(&self) -> i128 {
        self.wall_us() as i128 - self.tzinfo.map(|tz| tz.offset.total_micros()).unwrap_or(0)
    }

    /// Split a wall-clock microsecond count into civil fields (any year).
    fn fields_from_us(us: i128) -> (i64, u32, u32, u32, u32, u32, u32) {
        let day_us = US_PER_DAY as i128;
        let days = us.div_euclid(day_us);
        let rem = us.rem_euclid(day_us) as i64;
        let (y, m, d) = civil_from_days(days as i64);
        let secs = rem / US_PER_SECOND;
        let micro = (rem % US_PER_SECOND) as u32;
        (
            y,
            m,
            d,
            (secs / 3600) as u32,
            ((secs / 60) % 60) as u32,
            (secs % 60) as u32,
            micro,
        )
    }

    /// A `datetime` from a wall-clock microsecond count; `OverflowError:
    /// date value out of range` outside years 1..9999 (`datetime + timedelta`).
    fn from_wall_us(us: i128, tzinfo: Option<TimeZone>) -> Result<DateTime> {
        let (y, m, d, h, mi, s, micro) = Self::fields_from_us(us);
        if !(MINYEAR..=MAXYEAR).contains(&y) {
            return Err(LailaError::new(
                ExcKind::OverflowError,
                "date value out of range",
            ));
        }
        Ok(DateTime {
            year: y as i32,
            month: m as u8,
            day: d as u8,
            hour: h as u8,
            minute: mi as u8,
            second: s as u8,
            microsecond: micro,
            tzinfo,
        })
    }

    /// `datetime.fromtimestamp(secs, UTC)` when `utc`, else
    /// `datetime.utcfromtimestamp(secs)` (naive; this port has no local zone).
    /// Rounds to microseconds half-to-even like CPython; `ValueError` for
    /// NaN or a year outside 1..9999, `OverflowError` beyond `time_t`.
    pub fn from_timestamp(secs: f64, utc: bool) -> Result<DateTime> {
        if secs.is_nan() {
            return Err(LailaError::value_error("Invalid value NaN (not a number)"));
        }
        // _PyTime_DoubleToDenominator(secs, 1e6, ROUND_HALF_EVEN)
        let mut intpart = fmath::trunc(secs);
        let mut floatpart = secs - intpart;
        floatpart *= 1e6;
        floatpart = round_half_even(floatpart);
        if floatpart >= 1e6 {
            floatpart -= 1e6;
            intpart += 1.0;
        } else if floatpart < 0.0 {
            floatpart += 1e6;
            intpart -= 1.0;
        }
        if !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&intpart) {
            return Err(timestamp_overflow());
        }
        let timet = intpart as i64;
        let us = floatpart as i64;
        let (y, m, d, h, mi, s, micro) =
            Self::fields_from_us(timet as i128 * US_PER_SECOND as i128 + us as i128);
        check_date_args(y, m as i64, d as i64)?;
        Ok(DateTime {
            year: y as i32,
            month: m as u8,
            day: d as u8,
            hour: h as u8,
            minute: mi as u8,
            second: s as u8,
            microsecond: micro,
            tzinfo: if utc { Some(TimeZone::UTC) } else { None },
        })
    }

    /// `datetime.utcfromtimestamp(secs)`.
    pub fn utcfromtimestamp(secs: f64) -> Result<DateTime> {
        Self::from_timestamp(secs, false)
    }

    /// `datetime.now(UTC)` for a `time.time()` reading `secs` (see the module
    /// docs: the integrator supplies the clock).
    pub fn now_from(secs: f64) -> Result<DateTime> {
        Self::from_timestamp(secs, true)
    }

    /// `datetime.now(UTC).isoformat(timespec="milliseconds")` for a clock
    /// reading -- the stamp laila's objects and memory records carry.
    pub fn now_iso_ms_from(secs: f64) -> Result<String> {
        Self::now_from(secs)?.isoformat_with('T', "milliseconds")
    }

    /// `datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")`
    /// as the logger writes it.
    pub fn ts_to_iso_z(secs: f64) -> Result<String> {
        Ok(Self::now_from(secs)?
            .isoformat_with('T', "auto")?
            .replace("+00:00", "Z"))
    }

    /// `dt.timestamp()`: aware -> `(dt - epoch).total_seconds()`; naive ->
    /// the wall clock read as UTC (CPython would use the local zone).
    pub fn timestamp(&self) -> f64 {
        match self.tzinfo {
            Some(_) => ratio_to_f64(self.utc_us(), US_PER_SECOND as i128),
            None => {
                let seconds = div_floor(self.wall_us(), US_PER_SECOND);
                seconds as f64 + self.microsecond as f64 / 1e6
            }
        }
    }

    /// `dt.utcoffset()`.
    pub fn utcoffset(&self) -> Option<TimeDelta> {
        self.tzinfo.map(|tz| tz.offset)
    }

    /// `dt.tzname()`.
    pub fn tzname(&self) -> Option<String> {
        self.tzinfo.map(|tz| tz.tzname())
    }

    /// `dt.astimezone(tz)`; a naive `dt` is read as UTC (CPython: local time).
    pub fn astimezone(&self, tz: TimeZone) -> Result<DateTime> {
        Self::from_wall_us(self.utc_us() + tz.offset.total_micros(), Some(tz))
    }

    /// `dt.weekday()`: Monday is 0.
    pub fn weekday(&self) -> u32 {
        weekday(self.year as i64, self.month as u32, self.day as u32)
    }

    /// `dt.isoweekday()`: Monday is 1.
    pub fn isoweekday(&self) -> u32 {
        self.weekday() + 1
    }

    /// `dt.toordinal()`.
    pub fn toordinal(&self) -> i64 {
        ymd_to_ord(self.year as i64, self.month as u32, self.day as u32)
    }

    /// `dt.isocalendar()` as `(year, week, weekday)`.
    pub fn isocalendar(&self) -> (i64, u32, u32) {
        let mut year = self.year as i64;
        let mut week1_monday = iso_week1_monday(year);
        let today = self.toordinal();
        let mut week = (today - week1_monday).div_euclid(7);
        let day = (today - week1_monday).rem_euclid(7);
        if week < 0 {
            year -= 1;
            week1_monday = iso_week1_monday(year);
            week = (today - week1_monday).div_euclid(7);
        } else if week >= 52 && today >= iso_week1_monday(year + 1) {
            year += 1;
            week = 0;
        }
        (year, week as u32 + 1, day as u32 + 1)
    }

    /// `dt.date()` as `(year, month, day)`.
    pub fn date(&self) -> (i64, u32, u32) {
        (self.year as i64, self.month as u32, self.day as u32)
    }

    // -- arithmetic ------------------------------------------------------------

    /// `dt + td` (`OverflowError: date value out of range`).
    pub fn __add__(&self, td: TimeDelta) -> Result<DateTime> {
        Self::from_wall_us(self.wall_us() as i128 + td.total_micros(), self.tzinfo)
    }

    /// `dt - td`.
    pub fn sub_delta(&self, td: TimeDelta) -> Result<DateTime> {
        Self::from_wall_us(self.wall_us() as i128 - td.total_micros(), self.tzinfo)
    }

    /// `dt - other` -> `timedelta`; mixing naive and aware is a `TypeError`.
    pub fn __sub__(&self, other: &DateTime) -> Result<TimeDelta> {
        if self.tzinfo.is_some() != other.tzinfo.is_some() {
            return Err(LailaError::type_error(
                "can't subtract offset-naive and offset-aware datetimes",
            ));
        }
        TimeDelta::from_micros(self.utc_us() - other.utc_us())
    }

    /// `dt == other`: naive compares wall clocks, aware compares instants,
    /// naive vs aware is `False`.
    pub fn __eq__(&self, other: &DateTime) -> bool {
        match (self.tzinfo, other.tzinfo) {
            (None, None) => self.wall_us() == other.wall_us(),
            (Some(_), Some(_)) => self.utc_us() == other.utc_us(),
            _ => false,
        }
    }

    /// `hash(dt)`.
    pub fn __hash__(&self) -> i64 {
        let mut h = FnvHasher::default();
        match self.tzinfo {
            None => (0u8, self.wall_us()).hash(&mut h),
            Some(_) => (1u8, self.utc_us()).hash(&mut h),
        }
        (h.finish() >> 1) as i64
    }

    // -- formatting ------------------------------------------------------------

    /// `dt.isoformat(sep='T', timespec='auto')`; `timespec` is one of
    /// `auto`, `hours`, `minutes`, `seconds`, `milliseconds`, `microseconds`
    /// (`ValueError: Unknown timespec value` otherwise). Aware values get
    /// the `+HH:MM[:SS[.ffffff]]` suffix.
    /// `dt.isoformat(sep='T', timespec='auto')` -- Python-shaped arguments.
    pub fn isoformat(&self, args: impl Into<crate::python::Args>) -> Result<String> {
        let a: crate::python::Args = args.into();
        let sep = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("sep"));
        let sep = match &sep {
            Arg::None => 'T',
            Arg::Str(s) if s.chars().count() == 1 => s.chars().next().unwrap_or('T'),
            Arg::Str(_) => {
                return Err(LailaError::type_error(
                    "isoformat() argument 1 must be a unicode character, not str",
                ))
            }
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "isoformat() argument 1 must be a unicode character, not {}",
                    other.type_name()
                )))
            }
        };
        let ts = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("timespec"));
        let ts = match &ts {
            Arg::None => String::from("auto"),
            other => other.__str__(),
        };
        self.isoformat_with(sep, &ts)
    }

    pub fn isoformat_with(&self, sep: char, timespec: &str) -> Result<String> {
        let (y, mo, d, h, mi, s) = (
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
        );
        let us = self.microsecond;
        let spec = if timespec == "auto" {
            if us == 0 {
                "seconds"
            } else {
                "microseconds"
            }
        } else {
            timespec
        };
        let date = alloc::format!("{y:04}-{mo:02}-{d:02}{sep}");
        let mut out = match spec {
            "hours" => alloc::format!("{date}{h:02}"),
            "minutes" => alloc::format!("{date}{h:02}:{mi:02}"),
            "seconds" => alloc::format!("{date}{h:02}:{mi:02}:{s:02}"),
            "milliseconds" => alloc::format!("{date}{h:02}:{mi:02}:{s:02}.{:03}", us / 1000),
            "microseconds" => alloc::format!("{date}{h:02}:{mi:02}:{s:02}.{us:06}"),
            _ => return Err(LailaError::value_error("Unknown timespec value")),
        };
        if let Some(tz) = self.tzinfo {
            out.push_str(&format_utcoffset(":", tz.offset));
        }
        Ok(out)
    }

    /// `str(dt)` = `dt.isoformat(sep=' ')`.
    pub fn __str__(&self) -> String {
        self.isoformat_with(' ', "auto").unwrap_or_default()
    }

    /// `repr(dt)`: `datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone.utc)`
    /// (trailing zero seconds / microseconds elided like CPython).
    pub fn __repr__(&self) -> String {
        let mut out = alloc::format!(
            "datetime.datetime({}, {}, {}, {}, {}",
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute
        );
        if self.microsecond != 0 {
            out.push_str(&alloc::format!(", {}, {}", self.second, self.microsecond));
        } else if self.second != 0 {
            out.push_str(&alloc::format!(", {}", self.second));
        }
        if let Some(tz) = self.tzinfo {
            out.push_str(&alloc::format!(", tzinfo={}", tz.__repr__()));
        }
        out.push(')');
        out
    }

    /// `dt.strftime(fmt)` in the C locale: `%a %A %b %B %c %C %d %D %e %F %g
    /// %G %h %H %I %j %k %l %m %M %n %p %P %r %R %S %t %T %u %U %V %w %W %x
    /// %X %y %Y %z %:z %Z %%`, with glibc's `-`/`_`/`0`/`^` flags on numeric
    /// and name fields. Unknown directives are copied through, as glibc does.
    pub fn strftime(&self, fmt: &str) -> String {
        let chars: Vec<char> = fmt.chars().collect();
        let mut out = String::with_capacity(fmt.len() + 16);
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            i += 1;
            if c != '%' {
                out.push(c);
                continue;
            }
            if i >= chars.len() {
                out.push('%');
                break;
            }
            // glibc flags: '-' no padding, '_' space padding, '0' zero padding,
            // '^' upper case.
            let mut flag = None;
            if matches!(chars[i], '-' | '_' | '0' | '^') {
                flag = Some(chars[i]);
                i += 1;
                if i >= chars.len() {
                    out.push('%');
                    out.push(flag.unwrap());
                    break;
                }
            }
            let d = chars[i];
            i += 1;
            if d == ':' && i < chars.len() && chars[i] == 'z' {
                i += 1;
                if let Some(tz) = self.tzinfo {
                    out.push_str(&format_utcoffset(":", tz.offset));
                }
                continue;
            }
            match self.directive(d) {
                Some(piece) => out.push_str(&apply_flag(piece, flag)),
                None => {
                    out.push('%');
                    if let Some(f) = flag {
                        out.push(f);
                    }
                    out.push(d);
                }
            }
        }
        out
    }

    fn directive(&self, d: char) -> Option<Piece> {
        let y = self.year as i64;
        let wd = self.weekday();
        let yday = self.toordinal() - ymd_to_ord(y, 1, 1) + 1;
        let (iso_year, iso_week, _) = self.isocalendar();
        let hour12 = match self.hour % 12 {
            0 => 12,
            h => h,
        };
        let sunday_week = (yday + 6 - ((wd as i64 + 1) % 7)) / 7;
        let monday_week = (yday + 6 - wd as i64) / 7;
        Some(match d {
            'a' => Piece::text(&DAY_NAMES[wd as usize][..3]),
            'A' => Piece::text(DAY_NAMES[wd as usize]),
            'b' | 'h' => Piece::text(&MONTH_NAMES[self.month as usize - 1][..3]),
            'B' => Piece::text(MONTH_NAMES[self.month as usize - 1]),
            'c' => Piece::text(&alloc::format!(
                "{} {} {:>2} {:02}:{:02}:{:02} {}",
                &DAY_NAMES[wd as usize][..3],
                &MONTH_NAMES[self.month as usize - 1][..3],
                self.day,
                self.hour,
                self.minute,
                self.second,
                y
            )),
            'C' => Piece::num(y / 100, 2, '0'),
            'd' => Piece::num(self.day as i64, 2, '0'),
            'D' => Piece::text(&alloc::format!(
                "{:02}/{:02}/{:02}",
                self.month,
                self.day,
                y % 100
            )),
            'e' => Piece::num(self.day as i64, 2, ' '),
            'F' => Piece::text(&alloc::format!("{y:04}-{:02}-{:02}", self.month, self.day)),
            'g' => Piece::num(iso_year % 100, 2, '0'),
            'G' => Piece::num(iso_year, 4, '0'),
            'H' => Piece::num(self.hour as i64, 2, '0'),
            'I' => Piece::num(hour12 as i64, 2, '0'),
            'j' => Piece::num(yday, 3, '0'),
            'k' => Piece::num(self.hour as i64, 2, ' '),
            'l' => Piece::num(hour12 as i64, 2, ' '),
            'm' => Piece::num(self.month as i64, 2, '0'),
            'M' => Piece::num(self.minute as i64, 2, '0'),
            'n' => Piece::text("\n"),
            'p' => Piece::text(if self.hour < 12 { "AM" } else { "PM" }),
            'P' => Piece::text(if self.hour < 12 { "am" } else { "pm" }),
            'r' => Piece::text(&alloc::format!(
                "{:02}:{:02}:{:02} {}",
                hour12,
                self.minute,
                self.second,
                if self.hour < 12 { "AM" } else { "PM" }
            )),
            'R' => Piece::text(&alloc::format!("{:02}:{:02}", self.hour, self.minute)),
            'S' => Piece::num(self.second as i64, 2, '0'),
            't' => Piece::text("\t"),
            'T' | 'X' => Piece::text(&alloc::format!(
                "{:02}:{:02}:{:02}",
                self.hour,
                self.minute,
                self.second
            )),
            'u' => Piece::num(wd as i64 + 1, 1, '0'),
            'U' => Piece::num(sunday_week, 2, '0'),
            'V' => Piece::num(iso_week as i64, 2, '0'),
            'w' => Piece::num(((wd + 1) % 7) as i64, 1, '0'),
            'W' => Piece::num(monday_week, 2, '0'),
            'x' => Piece::text(&alloc::format!(
                "{:02}/{:02}/{:02}",
                self.month,
                self.day,
                y % 100
            )),
            'y' => Piece::num(y % 100, 2, '0'),
            'Y' => Piece::num(y, 4, '0'),
            'f' => Piece::num(self.microsecond as i64, 6, '0'),
            'z' => Piece::text(
                &self
                    .tzinfo
                    .map(|tz| format_utcoffset("", tz.offset))
                    .unwrap_or_default(),
            ),
            'Z' => Piece::text(&self.tzname().unwrap_or_default()),
            '%' => Piece::text("%"),
            _ => return None,
        })
    }

    // -- parsing ---------------------------------------------------------------

    /// `datetime.fromisoformat(s)` (CPython 3.11+ grammar): `YYYY-MM-DD`,
    /// `YYYYMMDD`, `YYYY-Www[-D]`, `YYYYWww[D]`, any single separator
    /// character, `HH[:MM[:SS[.f{1,6}]]]` / `HHMM[SS[.f]]` (`,` also marks
    /// the fraction; digits beyond 6 are truncated), `Z` / `±HH[:MM[:SS[.ffffff]]]`
    /// / `±HHMM[SS]`, `24:00:00` rolling to the next day.
    pub fn fromisoformat(s: &str) -> Result<DateTime> {
        let invalid = || {
            LailaError::value_error(alloc::format!(
                "Invalid isoformat string: {}",
                crate::python::pyrepr::str_repr(s)
            ))
        };
        Self::parse_isoformat(s).ok_or_else(invalid)?
    }

    /// `Some(Ok(dt))` on success, `Some(Err(e))` for a range error with its
    /// own message, `None` for "Invalid isoformat string".
    fn parse_isoformat(s: &str) -> Option<Result<DateTime>> {
        if s.chars().count() < 7 {
            return None; // All valid ISO 8601 strings are at least 7 characters long
        }
        // NUL-terminated byte view, as CPython parses the UTF-8 buffer.
        let mut b: Vec<u8> = s.as_bytes().to_vec();
        b.push(0);
        let len = s.len();
        let sep = find_isoformat_datetime_separator(&b, len)?;
        let (year, month, day) = parse_isoformat_date(&b, sep)?;
        let mut time = (0i64, 0i64, 0i64, 0i64); // hour, minute, second, microsecond
        let mut tz: Option<(i64, i64)> = None; // offset seconds, offset microseconds
        if len > sep {
            // The separator may be any (multi-byte) character.
            let p = sep
                + match b[sep] & 0xf0 {
                    x if x & 0x80 == 0 => 1,
                    0xe0 => 3,
                    0xf0 => 4,
                    _ => 2,
                };
            if p > len {
                return None;
            }
            let (h, mi, sec, us, tzres) = parse_isoformat_time(&b[p..=len])?;
            time = (h, mi, sec, us);
            tz = tzres;
        }
        let (hour, minute, second, microsecond) = time;
        let tzinfo = match tz {
            None => None,
            Some((0, 0)) => Some(TimeZone::UTC),
            Some((secs, us)) => {
                let delta = match TimeDelta::new(0, secs, us) {
                    Ok(d) => d,
                    Err(e) => return Some(Err(e)),
                };
                match TimeZone::new(delta) {
                    Ok(tz) => Some(tz),
                    Err(e) => return Some(Err(e)),
                }
            }
        };
        let (mut year, mut month, mut day, mut hour) = (year, month, day, hour);
        if hour == 24 && (1..=12).contains(&month) {
            let d_in_month = days_in_month(year, month as u32) as i64;
            if day <= d_in_month {
                if minute == 0 && second == 0 && microsecond == 0 {
                    // Calculate midnight of the next day
                    hour = 0;
                    day += 1;
                    if day > d_in_month {
                        day = 1;
                        month += 1;
                        if month > 12 {
                            month = 1;
                            year += 1;
                        }
                    }
                } else {
                    return Some(Err(LailaError::value_error(
                        "minute, second, and microsecond must be 0 when hour is 24",
                    )));
                }
            }
        }
        Some(Self::new(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tzinfo,
        ))
    }
}

/// One formatted `strftime` field before glibc flags are applied.
struct Piece {
    text: String,
    numeric: Option<(i64, usize, char)>,
}

impl Piece {
    fn text(s: &str) -> Piece {
        Piece {
            text: s.into(),
            numeric: None,
        }
    }
    fn num(v: i64, width: usize, pad: char) -> Piece {
        Piece {
            text: pad_num(v, width, pad),
            numeric: Some((v, width, pad)),
        }
    }
}

fn pad_num(v: i64, width: usize, pad: char) -> String {
    let digits = v.abs().to_string();
    let mut out = String::new();
    if v < 0 {
        out.push('-');
    }
    for _ in digits.len()..width {
        out.push(pad);
    }
    out.push_str(&digits);
    out
}

fn apply_flag(piece: Piece, flag: Option<char>) -> String {
    match (flag, piece.numeric) {
        (Some('-'), Some((v, _, _))) => v.to_string(),
        (Some('_'), Some((v, w, _))) => pad_num(v, w, ' '),
        (Some('0'), Some((v, w, _))) => pad_num(v, w, '0'),
        (Some('^'), _) => piece.text.to_uppercase(),
        _ => piece.text,
    }
}

fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

/// CPython's `parse_digits`: exactly `num_digits` ASCII digits at `p`.
fn parse_digits(b: &[u8], p: usize, num_digits: usize) -> Option<(usize, i64)> {
    let mut v: i64 = 0;
    for i in 0..num_digits {
        let c = *b.get(p + i)?;
        if !is_digit(c) {
            return None;
        }
        v = v * 10 + (c - b'0') as i64;
    }
    Some((p + num_digits, v))
}

/// CPython's `_find_isoformat_datetime_separator` (byte position of the
/// date/time separator; `None` where CPython returns -1).
fn find_isoformat_datetime_separator(b: &[u8], len: usize) -> Option<usize> {
    if len == 7 {
        return Some(7);
    }
    if b[4] == b'-' {
        // YYYY-???
        if b[5] == b'W' {
            // YYYY-W??
            if len < 8 {
                return None;
            }
            if len > 8 && b[8] == b'-' {
                // YYYY-Www-D (10) or YYYY-Www-HH (8)
                if len == 9 {
                    return None;
                }
                if len > 10 && is_digit(b[10]) {
                    return Some(8);
                }
                Some(10)
            } else {
                // YYYY-Www (8)
                Some(8)
            }
        } else {
            // YYYY-MM-DD (10)
            Some(10)
        }
    } else if b[4] == b'W' {
        // YYYYWww (7) or YYYYWwwd (8)
        let mut idx = 7;
        while idx < len && is_digit(b[idx]) {
            idx += 1;
        }
        if idx < 9 {
            Some(idx)
        } else if idx % 2 == 0 {
            Some(7)
        } else {
            Some(8)
        }
    } else {
        // YYYYMMDD (8)
        Some(8)
    }
}

/// CPython's `parse_isoformat_date` (+ `iso_to_ymd`) on `b[..len]`.
fn parse_isoformat_date(b: &[u8], len: usize) -> Option<(i64, i64, i64)> {
    let (mut p, year) = parse_digits(b, 0, 4)?;
    let uses_separator = b[p] == b'-';
    if uses_separator {
        p += 1;
    }
    if b[p] == b'W' {
        // This is an isocalendar-style date string
        p += 1;
        let (np, iso_week) = parse_digits(b, p, 2)?;
        p = np;
        let iso_day = if p < len {
            if uses_separator {
                if b[p] != b'-' {
                    return None;
                }
                p += 1;
            }
            parse_digits(b, p, 1)?.1
        } else {
            1
        };
        return iso_to_ymd(year, iso_week, iso_day);
    }
    let (np, month) = parse_digits(b, p, 2)?;
    p = np;
    if uses_separator {
        if b[p] != b'-' {
            return None;
        }
        p += 1;
    }
    let (_, day) = parse_digits(b, p, 2)?;
    Some((year, month, day))
}

/// CPython's `iso_to_ymd`.
fn iso_to_ymd(iso_year: i64, iso_week: i64, iso_day: i64) -> Option<(i64, i64, i64)> {
    if !(MINYEAR..=MAXYEAR).contains(&iso_year) {
        return None;
    }
    if iso_week <= 0 || iso_week >= 53 {
        let mut out_of_range = true;
        if iso_week == 53 {
            // ISO years have 53 weeks in it on years starting with a Thursday
            // and on leap years starting on Wednesday
            let first_weekday = weekday(iso_year, 1, 1);
            if first_weekday == 3 || (first_weekday == 2 && is_leap(iso_year)) {
                out_of_range = false;
            }
        }
        if out_of_range {
            return None;
        }
    }
    if iso_day <= 0 || iso_day >= 8 {
        return None;
    }
    let day_1 = iso_week1_monday(iso_year);
    let day_offset = (iso_week - 1) * 7 + iso_day - 1;
    let (y, m, d) = ord_to_ymd(day_1 + day_offset);
    Some((y, m as i64, d as i64))
}

/// CPython's `parse_hh_mm_ss_ff` on `t[..end]` (`t` is NUL-terminated past
/// `end`). Returns `(hour, minute, second, microsecond, more)` where `more`
/// mirrors the C return value 1 ("not at the end of the string").
fn parse_hh_mm_ss_ff(t: &[u8], end: usize) -> Option<(i64, i64, i64, i64, bool)> {
    let mut vals = [0i64; 3];
    let mut p = 0usize;
    let mut has_separator = true;
    // Parse [HH[:?MM[:?SS]]]
    for i in 0..3 {
        let (np, v) = parse_digits(t, p, 2)?;
        vals[i] = v;
        p = np;
        let c = t[p];
        p += 1;
        if i == 0 {
            has_separator = c == b':';
        }
        if c == b'.' || c == b',' {
            if i < 2 {
                return None; // Decimal mark on hour or minute
            }
            if p >= end {
                return None; // Decimal mark not followed by any digit
            }
            break;
        } else if p >= end {
            return Some((vals[0], vals[1], vals[2], 0, c != 0));
        } else if has_separator && c == b':' {
            if i == 2 {
                return None; // Malformed microsecond separator
            }
            continue;
        } else if !has_separator {
            p -= 1;
        } else {
            return None; // Malformed time separator
        }
    }
    // Parse fractional components (also reached, as in CPython, when a
    // separator-less time runs past its six digits).
    let len_remains = end - p;
    let to_parse = len_remains.min(6);
    let (np, mut microsecond) = parse_digits(t, p, to_parse)?;
    p = np;
    const CORRECTION: [i64; 5] = [100_000, 10_000, 1000, 100, 10];
    if to_parse < 6 {
        microsecond *= CORRECTION[to_parse - 1];
    }
    while is_digit(t[p]) {
        p += 1; // skip truncated digits
    }
    Some((vals[0], vals[1], vals[2], microsecond, t[p] != 0))
}

/// CPython's `parse_isoformat_time` on a NUL-terminated buffer `t`
/// (`t[len] == 0`). Returns the time fields and the optional
/// `(offset_seconds, offset_microseconds)`.
#[allow(clippy::type_complexity)]
fn parse_isoformat_time(t: &[u8]) -> Option<(i64, i64, i64, i64, Option<(i64, i64)>)> {
    let len = t.len() - 1;
    let mut tzinfo_pos = 0;
    while tzinfo_pos < len {
        if matches!(t[tzinfo_pos], b'Z' | b'+' | b'-') {
            break;
        }
        tzinfo_pos += 1;
    }
    let (h, m, s, us, more) = parse_hh_mm_ss_ff(t, tzinfo_pos)?;
    if tzinfo_pos == len {
        // We know that there's no time zone, so if there's stuff at the
        // end of the string it's an error.
        return if more {
            None
        } else {
            Some((h, m, s, us, None))
        };
    }
    // Special case UTC / Zulu time.
    if t[tzinfo_pos] == b'Z' {
        return if t[tzinfo_pos + 1] != 0 {
            None
        } else {
            Some((h, m, s, us, Some((0, 0))))
        };
    }
    let tzsign: i64 = if t[tzinfo_pos] == b'-' { -1 } else { 1 };
    let (tzh, tzm, tzs, tzus, more) =
        parse_hh_mm_ss_ff(&t[tzinfo_pos + 1..], len - tzinfo_pos - 1)?;
    if more {
        return None;
    }
    let tzoffset = tzsign * (tzh * 3600 + tzm * 60 + tzs);
    Some((h, m, s, us, Some((tzoffset, tzus * tzsign))))
}

// ---------------------------------------------------------------------------
// Python object protocol
// ---------------------------------------------------------------------------

impl PartialEq for DateTime {
    fn eq(&self, other: &DateTime) -> bool {
        self.__eq__(other)
    }
}

impl PartialOrd for DateTime {
    /// `None` for naive vs aware (Python raises `TypeError` there).
    fn partial_cmp(&self, other: &DateTime) -> Option<core::cmp::Ordering> {
        match (self.tzinfo, other.tzinfo) {
            (None, None) => Some(self.wall_us().cmp(&other.wall_us())),
            (Some(_), Some(_)) => Some(self.utc_us().cmp(&other.utc_us())),
            _ => None,
        }
    }
}

macro_rules! arg_glue {
    ($t:ty) => {
        impl IntoArg for $t {
            fn into_arg(self) -> Arg {
                Arg::Object(Arc::new(self))
            }
        }
        impl PyArg for $t {
            fn to_arg(&self) -> Arg {
                Arg::Object(Arc::new(*self))
            }
        }
        impl IsInstance for $t {
            fn isinstance(x: &Arg) -> bool {
                matches!(x, Arg::Object(o) if o.as_any().is::<$t>())
            }
        }
        impl PartialEq<Arg> for $t {
            fn eq(&self, other: &Arg) -> bool {
                matches!(other, Arg::Object(o) if o.as_any().downcast_ref::<$t>() == Some(self))
            }
        }
    };
}
arg_glue!(DateTime);
arg_glue!(TimeDelta);
arg_glue!(TimeZone);

impl PyObject for DateTime {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "datetime.datetime"
    }
    fn __repr__(&self) -> String {
        DateTime::__repr__(self)
    }
    fn __str__(&self) -> String {
        DateTime::__str__(self)
    }
    fn __call_method__(&self, name: &str, args: crate::python::Args) -> Result<Arg> {
        let first = |n: &str| {
            args.positional
                .first()
                .cloned()
                .unwrap_or_else(|| args.get(n))
        };
        Ok(match name {
            "isoformat" => Arg::from(self.isoformat(args)?.as_str()),
            "timestamp" => Arg::Float(self.timestamp()),
            "strftime" => Arg::from(self.strftime(&first("format").__str__()).as_str()),
            "weekday" => Arg::Int(self.weekday() as i64),
            "isoweekday" => Arg::Int(self.isoweekday() as i64),
            "toordinal" => Arg::Int(self.toordinal()),
            "utcoffset" => self.utcoffset().map(IntoArg::into_arg).unwrap_or(Arg::None),
            "tzname" => self
                .tzname()
                .map(|s| Arg::from(s.as_str()))
                .unwrap_or(Arg::None),
            "astimezone" => {
                let tz = first("tz");
                match tz.downcast_ref::<TimeZone>() {
                    Some(tz) => self.astimezone(*tz)?.into_arg(),
                    None if tz.is_none() => self.astimezone(TimeZone::UTC)?.into_arg(),
                    None => {
                        return Err(LailaError::type_error(alloc::format!(
                            "astimezone() argument must be a tzinfo instance, not {}",
                            tz.type_name()
                        )))
                    }
                }
            }
            "replace" => {
                if !args.__contains__("tzinfo")? || !args.positional.is_empty() {
                    return Err(LailaError::type_error(
                        "replace(): laila-rs supports the keyword form `replace(tzinfo=..)` only",
                    ));
                }
                let tz = args.get("tzinfo");
                let tz = match tz.downcast_ref::<TimeZone>() {
                    Some(tz) => Some(*tz),
                    None if tz.is_none() => None,
                    None => {
                        return Err(LailaError::type_error(
                            "tzinfo argument must be None or of a tzinfo subclass",
                        ))
                    }
                };
                self.replace_tzinfo(tz).into_arg()
            }
            "date" => {
                let (y, m, d) = self.date();
                Arg::Tuple(alloc::vec![Arg::Int(y), Arg::Int(m as i64), Arg::Int(d as i64)].into())
            }
            "isocalendar" => {
                let (y, w, d) = self.isocalendar();
                Arg::Tuple(alloc::vec![Arg::Int(y), Arg::Int(w as i64), Arg::Int(d as i64)].into())
            }
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'datetime.datetime' object has no attribute '{name}'"
                )))
            }
        })
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DateTime>()
            .is_some_and(|o| self.__eq__(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(DateTime::__hash__(self))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "year" => Arg::Int(self.year as i64),
            "month" => Arg::Int(self.month as i64),
            "day" => Arg::Int(self.day as i64),
            "hour" => Arg::Int(self.hour as i64),
            "minute" => Arg::Int(self.minute as i64),
            "second" => Arg::Int(self.second as i64),
            "microsecond" => Arg::Int(self.microsecond as i64),
            "fold" => Arg::Int(0),
            "tzinfo" => self.tzinfo.map(IntoArg::into_arg).unwrap_or(Arg::None),
            // `dt.timestamp()` lowers to the zero-arg attribute spelling.
            "timestamp" => Arg::Float(self.timestamp()),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'datetime.datetime' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl PyObject for TimeDelta {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "datetime.timedelta"
    }
    fn __repr__(&self) -> String {
        TimeDelta::__repr__(self)
    }
    fn __str__(&self) -> String {
        TimeDelta::__str__(self)
    }
    fn __call_method__(&self, name: &str, _args: crate::python::Args) -> Result<Arg> {
        match name {
            "total_seconds" => Ok(Arg::Float(self.total_seconds())),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'datetime.timedelta' object has no attribute '{name}'"
            ))),
        }
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<TimeDelta>() == Some(self)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(TimeDelta::__hash__(self))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "days" => Arg::Int(self.days as i64),
            "seconds" => Arg::Int(self.seconds as i64),
            "microseconds" => Arg::Int(self.microseconds as i64),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'datetime.timedelta' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl PyObject for TimeZone {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "datetime.timezone"
    }
    fn __repr__(&self) -> String {
        TimeZone::__repr__(self)
    }
    fn __str__(&self) -> String {
        TimeZone::__str__(self)
    }
    fn __call_method__(&self, name: &str, _args: crate::python::Args) -> Result<Arg> {
        match name {
            "utcoffset" => Ok(self.offset.into_arg()),
            "tzname" => Ok(Arg::from(self.tzname().as_str())),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'datetime.timezone' object has no attribute '{name}'"
            ))),
        }
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<TimeZone>() == Some(self)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(TimeZone::__hash__(self))
    }
}

/// `str(x)` for the three types through `Arg::__str__` would fall back to
/// `repr`; these helpers give Python's `str()` text.
pub fn str_of(a: &Arg) -> Option<String> {
    if let Some(dt) = a.downcast_ref::<DateTime>() {
        return Some(dt.__str__());
    }
    if let Some(td) = a.downcast_ref::<TimeDelta>() {
        return Some(td.__str__());
    }
    if let Some(tz) = a.downcast_ref::<TimeZone>() {
        return Some(tz.__str__());
    }
    None
}
