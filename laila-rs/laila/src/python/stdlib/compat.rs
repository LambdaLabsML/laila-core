//! `from datetime import UTC, datetime, timedelta, timezone`,
//! `from dotmap import DotMap`, `import ast`, `from cryptography.fernet import
//! Fernet, MultiFernet, InvalidToken` -- Python-spelled facades over
//! `crate::_compat` / `crate::_codecs`.
//!
//! Lower-case Python classes (`datetime`, `timedelta`, `timezone`) are unit
//! structs: the value carries the classmethods (`datetime.now(UTC)`) and the
//! `CallValue` impl makes the bare call (`datetime(2024, 1, 2)`) construct.

use alloc::string::String;
use alloc::vec::Vec;

use super::super::args::Args;
use super::super::builtins::CallValue;
use super::super::errors::{LailaError, Result};
use super::super::object::{Arg, IntoArg};
use crate::_compat::datetime::{DateTime, TimeDelta, TimeZone};

fn pos_or_kw(a: &Args, i: usize, name: &str) -> Arg {
    a.positional.get(i).cloned().unwrap_or_else(|| a.get(name))
}

fn int_of(a: &Arg, what: &str) -> Result<i64> {
    match a {
        Arg::Int(i) => Ok(*i),
        Arg::Bool(b) => Ok(i64::from(*b)),
        other => Err(LailaError::type_error(alloc::format!(
            "'{}' object cannot be interpreted as an integer{}",
            other.type_name(),
            what
        ))),
    }
}

fn tz_of(a: &Arg) -> Result<Option<TimeZone>> {
    match a {
        Arg::None => Ok(None),
        other => match other.downcast_ref::<TimeZone>() {
            Some(tz) => Ok(Some(*tz)),
            None => Err(LailaError::type_error(alloc::format!(
                "tzinfo argument must be None or of a tzinfo subclass, not type '{}'",
                other.type_name()
            ))),
        },
    }
}

fn now_secs() -> f64 {
    #[cfg(feature = "std")]
    {
        super::time.time()
    }
    #[cfg(not(feature = "std"))]
    {
        0.0
    }
}

// --------------------------------------------------------------------------
// datetime
// --------------------------------------------------------------------------

/// `datetime.UTC` / `timezone.utc`.
#[allow(non_upper_case_globals)]
pub static UTC: TimeZone = TimeZone::UTC;

/// `datetime.datetime` -- the class object. Also doubles as the module
/// (`import datetime; datetime.datetime.now(...)`) via the accessor methods.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct datetime;

impl datetime {
    /// `datetime.datetime` (module attribute access).
    pub fn datetime(&self) -> datetime {
        datetime
    }
    /// `datetime.timedelta`
    pub fn timedelta(&self) -> timedelta {
        timedelta
    }
    /// `datetime.timezone`
    pub fn timezone(&self) -> timezone {
        timezone
    }
    /// `datetime.UTC`
    #[allow(non_snake_case)]
    pub fn UTC(&self) -> TimeZone {
        TimeZone::UTC
    }
    #[allow(non_snake_case)]
    pub fn MINYEAR(&self) -> i64 {
        crate::_compat::datetime::MINYEAR
    }
    #[allow(non_snake_case)]
    pub fn MAXYEAR(&self) -> i64 {
        crate::_compat::datetime::MAXYEAR
    }

    /// `datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None, *, fold=0)`
    pub fn __new__(args: impl Into<Args>) -> Result<DateTime> {
        let a: Args = args.into();
        let names = [
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "microsecond",
            "tzinfo",
        ];
        if a.positional.len() > names.len() {
            return Err(LailaError::type_error(alloc::format!(
                "function takes at most 9 arguments ({} given)",
                a.positional.len() + 1
            )));
        }
        let get = |i: usize| pos_or_kw(&a, i, names[i]);
        for (i, n) in names.iter().enumerate().take(3) {
            if get(i).is_none() {
                return Err(LailaError::type_error(alloc::format!(
                    "function missing required argument '{n}' (pos {})",
                    i + 1
                )));
            }
        }
        let num = |i: usize| -> Result<i64> {
            let v = get(i);
            if v.is_none() {
                Ok(0)
            } else {
                int_of(&v, "")
            }
        };
        DateTime::new(
            num(0)?,
            num(1)?,
            num(2)?,
            num(3)?,
            num(4)?,
            num(5)?,
            num(6)?,
            tz_of(&get(7))?,
        )
    }

    /// `datetime.now(tz=None)` -- naive values are UTC wall-clock (see
    /// `_compat::datetime`).
    pub fn now(&self, args: impl Into<Args>) -> Result<DateTime> {
        let a: Args = args.into();
        let tz = tz_of(&pos_or_kw(&a, 0, "tz"))?;
        let dt = DateTime::now_from(now_secs())?;
        Ok(match tz {
            Some(tz) => dt.astimezone(tz)?,
            None => dt.replace_tzinfo(None),
        })
    }
    /// `datetime.utcnow()`
    pub fn utcnow(&self) -> Result<DateTime> {
        Ok(DateTime::now_from(now_secs())?.replace_tzinfo(None))
    }
    /// `datetime.fromtimestamp(timestamp, tz=None)`
    pub fn fromtimestamp(&self, args: impl Into<Args>) -> Result<DateTime> {
        let a: Args = args.into();
        let ts = pos_or_kw(&a, 0, "timestamp");
        let secs = ts.as_f64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as an integer",
                ts.type_name()
            ))
        })?;
        let tz = tz_of(&pos_or_kw(&a, 1, "tz"))?;
        let dt = DateTime::from_timestamp(secs, true)?;
        Ok(match tz {
            Some(tz) => dt.astimezone(tz)?,
            None => dt.replace_tzinfo(None),
        })
    }
    /// `datetime.utcfromtimestamp(timestamp)`
    pub fn utcfromtimestamp(&self, timestamp: impl IntoArg) -> Result<DateTime> {
        let ts = timestamp.into_arg();
        let secs = ts.as_f64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as an integer",
                ts.type_name()
            ))
        })?;
        DateTime::utcfromtimestamp(secs)
    }
    /// `datetime.fromisoformat(date_string)`
    pub fn fromisoformat(&self, date_string: impl IntoArg) -> Result<DateTime> {
        let s = date_string.into_arg();
        match &s {
            Arg::Str(t) => DateTime::fromisoformat(t),
            other => Err(LailaError::type_error(alloc::format!(
                "fromisoformat: argument must be str, not {}",
                other.type_name()
            ))),
        }
    }
    /// `datetime.min`
    pub fn min(&self) -> DateTime {
        DateTime::new(1, 1, 1, 0, 0, 0, 0, None).expect("datetime.min")
    }
    /// `datetime.max`
    pub fn max(&self) -> DateTime {
        DateTime::new(9999, 12, 31, 23, 59, 59, 999_999, None).expect("datetime.max")
    }
}

impl CallValue for datetime {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Ok(datetime::__new__(args)?.into_arg())
    }
}

/// `datetime.timedelta` -- the class object.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct timedelta;

impl timedelta {
    /// `timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)`
    /// -- ints or floats, normalised like CPython (float parts rounded
    /// half-even to microseconds).
    pub fn __new__(args: impl Into<Args>) -> Result<TimeDelta> {
        let a: Args = args.into();
        let names = [
            "days",
            "seconds",
            "microseconds",
            "milliseconds",
            "minutes",
            "hours",
            "weeks",
        ];
        if a.positional.len() > names.len() {
            return Err(LailaError::type_error(alloc::format!(
                "__new__() takes at most 7 arguments ({} given)",
                a.positional.len()
            )));
        }
        for (k, _) in &a.keywords {
            if !names.contains(&k.as_str()) {
                return Err(LailaError::type_error(alloc::format!(
                    "'{k}' is an invalid keyword argument for __new__()"
                )));
            }
        }
        let us_per = [
            86_400_000_000f64,
            1_000_000f64,
            1f64,
            1_000f64,
            60_000_000f64,
            3_600_000_000f64,
            604_800_000_000f64,
        ];
        let mut total_int: i128 = 0;
        let mut total_frac: f64 = 0.0;
        for (i, n) in names.iter().enumerate() {
            let v = pos_or_kw(&a, i, n);
            match v {
                Arg::None => {}
                Arg::Int(x) => total_int += x as i128 * us_per[i] as i128,
                Arg::Bool(b) => total_int += i128::from(b) * us_per[i] as i128,
                Arg::Float(f) => total_frac += f * us_per[i],
                Arg::Long(_) => {
                    return Err(LailaError::new(
                        super::super::errors::ExcKind::OverflowError,
                        "Python int too large to convert to C int",
                    ))
                }
                other => {
                    return Err(LailaError::type_error(alloc::format!(
                        "unsupported type for timedelta {} component: {}",
                        n,
                        other.type_name()
                    )))
                }
            }
        }
        let frac_us = crate::_compat::datetime::round_half_even(total_frac);
        TimeDelta::from_micros(total_int + frac_us as i128)
    }
    /// `timedelta.min`
    pub fn min(&self) -> TimeDelta {
        TimeDelta::new(-crate::_compat::datetime::MAX_DELTA_DAYS, 0, 0).expect("timedelta.min")
    }
    /// `timedelta.max`
    pub fn max(&self) -> TimeDelta {
        TimeDelta::new(crate::_compat::datetime::MAX_DELTA_DAYS, 86_399, 999_999)
            .expect("timedelta.max")
    }
    /// `timedelta.resolution`
    pub fn resolution(&self) -> TimeDelta {
        TimeDelta::new(0, 0, 1).expect("timedelta.resolution")
    }
}

impl CallValue for timedelta {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Ok(timedelta::__new__(args)?.into_arg())
    }
}

/// `datetime.timezone` -- the class object.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct timezone;

impl timezone {
    /// `timezone(offset, name=None)` (names are not kept: laila only emits
    /// fixed offsets).
    pub fn __new__(args: impl Into<Args>) -> Result<TimeZone> {
        let a: Args = args.into();
        let off = pos_or_kw(&a, 0, "offset");
        match off.downcast_ref::<TimeDelta>() {
            Some(td) => TimeZone::new(*td),
            None => Err(LailaError::type_error(alloc::format!(
                "offset must be a timedelta, not {}",
                off.type_name()
            ))),
        }
    }
    /// `timezone.utc`
    pub fn utc(&self) -> TimeZone {
        TimeZone::UTC
    }
}

impl CallValue for timezone {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Ok(timezone::__new__(args)?.into_arg())
    }
}

// --------------------------------------------------------------------------
// dotmap / ast / cryptography.fernet
// --------------------------------------------------------------------------

/// `from dotmap import DotMap` -- `DotMap(...)` lowers to `DotMap::new(args)`.
pub use crate::_compat::dotmap::DotMap;

/// `import ast`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ast_mod;
#[allow(non_upper_case_globals)]
pub static ast: ast_mod = ast_mod;
impl ast_mod {
    /// `ast.literal_eval(node_or_string)`
    pub fn literal_eval(&self, node_or_string: impl IntoArg) -> Result<Arg> {
        let s = node_or_string.into_arg();
        match &s {
            Arg::Str(t) => crate::_compat::pyliteral::literal_eval(t),
            Arg::Bytes(_) | Arg::ByteArray(_) => {
                let b = s.as_bytes_like().unwrap_or_default();
                let t = super::super::methods::utf8_decode(&b)?;
                crate::_compat::pyliteral::literal_eval(&t)
            }
            other => Err(LailaError::type_error(alloc::format!(
                "source code string cannot be of type '{}'",
                other.type_name()
            ))),
        }
    }
}

/// `cryptography.fernet.InvalidToken` (exception class marker).
pub use super::super::errors::InvalidToken;
/// `from cryptography.fernet import Fernet, MultiFernet, InvalidToken`
pub use crate::_codecs::fernet::{Fernet, MultiFernet};

/// `import cryptography` -> `cryptography.fernet.Fernet(key)`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct cryptography_mod;
#[allow(non_upper_case_globals)]
pub static cryptography: cryptography_mod = cryptography_mod;
impl cryptography_mod {
    pub fn fernet(&self) -> fernet_mod {
        fernet_mod
    }
}

/// `cryptography.fernet` submodule: `fernet.Fernet(key)`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct fernet_mod;
impl fernet_mod {
    #[allow(non_snake_case)]
    pub fn Fernet(&self, args: impl Into<Args>) -> Result<Fernet> {
        Fernet::new(args)
    }
    #[allow(non_snake_case)]
    pub fn MultiFernet(&self, args: impl Into<Args>) -> Result<MultiFernet> {
        MultiFernet::new(args)
    }
}

/// `laila.entry.compdata.transformation.encryption.encryption.key_fingerprint(key)`
pub fn key_fingerprint(key: impl IntoArg) -> Result<String> {
    let k = key.into_arg();
    let raw: Vec<u8> = match &k {
        Arg::Str(s) => s.as_bytes().to_vec(),
        other => other.as_bytes_like().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "key must be str or bytes, not {}",
                other.type_name()
            ))
        })?,
    };
    Ok(crate::_codecs::fernet::key_fingerprint(&raw))
}
