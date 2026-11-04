//! CPython `json` module semantics (`json.dumps` / `json.loads`) on `Arg`.
//!
//! ```text
//! dumps(obj, *, indent=None, separators=None, sort_keys=False, ensure_ascii=True, allow_nan=True, default=None) -> str
//! loads(s) -> object            (strict decoder, CPython's JSONDecodeError texts)
//! ```
//!
//! Points where this differs from a naive serialiser and matters for byte
//! identity with Python-written data:
//!   - default separators `", "` / `": "` (`","` + `": "` once `indent` is set);
//!   - `ensure_ascii=True` escapes every non-ASCII code point as `\uXXXX`
//!     (surrogate pairs above the BMP), lowercase hex;
//!   - floats print as `repr(float)` (`1e-05`, `1.0`), `NaN` / `Infinity` /
//!     `-Infinity` when `allow_nan`;
//!   - ints of any size are bare digits (`Arg::Long` in and out);
//!   - non-`str` dict keys are coerced like CPython (`True` -> `"true"`,
//!     `1.5` -> `"1.5"`, `None` -> `"null"`), other key types raise
//!     `TypeError("keys must be str, int, float, bool or None, not tuple")`;
//!   - `sort_keys` sorts the *original* keys with Python ordering (so int
//!     keys sort numerically and mixed `str`/`int` keys raise `TypeError`);
//!   - circular containers raise `ValueError("Circular reference detected")`;
//!   - the decoder reports errors as `"<msg>: line L column C (char N)"` with
//!     CPython 3.13+ texts (including `Illegal trailing comma before end of
//!     object/array`); positions are code-point indices as in Python.
//!
//! Known deviations: a lone surrogate from a `\uD800`-style escape cannot
//! live in a Rust `str` and decodes to U+FFFD; `loads_bytes` only handles
//! UTF-8 (CPython also sniffs UTF-16/32 by NUL patterns); CPython's 4300-digit
//! `int()` limit is not enforced.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;

use num_bigint::BigInt;

use crate::python::errors::ExcKind;
use crate::python::object::{Arg, Callable, PyMap};
use crate::python::pyrepr::float_repr;
use crate::python::sync::RwLock;
use crate::python::{Args, LailaError, Result};

// --------------------------------------------------------------------------
// dumps
// --------------------------------------------------------------------------

/// Keyword arguments of `json.dumps` (the `skipkeys`, `check_circular` and
/// `cls` knobs are not modelled: circular checks are always on).
#[derive(Clone)]
pub struct DumpsOptions {
    /// `indent=None | int | str`
    pub indent: Option<Arg>,
    /// `separators=(item_separator, key_separator)`
    pub separators: Option<(String, String)>,
    /// `sort_keys=False`
    pub sort_keys: bool,
    /// `ensure_ascii=True`
    pub ensure_ascii: bool,
    /// `allow_nan=True`
    pub allow_nan: bool,
    /// `default=None` -- called with the unserialisable object, must return
    /// a serialisable one.
    pub default: Option<Callable>,
}

impl Default for DumpsOptions {
    fn default() -> Self {
        DumpsOptions {
            indent: None,
            separators: None,
            sort_keys: false,
            ensure_ascii: true,
            allow_nan: true,
            default: None,
        }
    }
}

impl core::fmt::Debug for DumpsOptions {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DumpsOptions")
            .field("indent", &self.indent)
            .field("separators", &self.separators)
            .field("sort_keys", &self.sort_keys)
            .field("ensure_ascii", &self.ensure_ascii)
            .field("allow_nan", &self.allow_nan)
            .field("default", &self.default.as_ref().map(|_| "<function>"))
            .finish()
    }
}

/// `json.dumps(obj, **opts)`
pub fn dumps(obj: &Arg, opts: &DumpsOptions) -> Result<String> {
    // `' ' * indent` (CPython: `if isinstance(indent, int): indent = ' ' * indent`)
    let indent: Option<String> = match &opts.indent {
        None | Some(Arg::None) => None,
        Some(Arg::Str(s)) => Some(s.to_string()),
        Some(a) if a.as_i64().is_some() => {
            Some(" ".repeat(a.as_i64().unwrap_or(0).max(0) as usize))
        }
        Some(Arg::Long(l)) => {
            use num_traits::Signed;
            if l.is_negative() {
                Some(String::new())
            } else {
                return Err(LailaError::new(
                    ExcKind::OverflowError,
                    "cannot fit 'int' into an index-sized integer",
                ));
            }
        }
        Some(other) => {
            return Err(LailaError::type_error(alloc::format!(
                "can't multiply sequence by non-int of type '{}'",
                other.type_name()
            )))
        }
    };
    let (item_sep, key_sep): (String, String) = match &opts.separators {
        Some((a, b)) => (a.clone(), b.clone()),
        None if indent.is_some() => (",".into(), ": ".into()),
        None => (", ".into(), ": ".into()),
    };
    let mut enc = Encoder {
        item_sep,
        key_sep,
        indent,
        sort_keys: opts.sort_keys,
        ensure_ascii: opts.ensure_ascii,
        allow_nan: opts.allow_nan,
        default: opts.default.as_ref(),
        markers: Vec::new(),
    };
    let mut out = String::new();
    enc.enc(obj, 0, &mut out)?;
    Ok(out)
}

struct Encoder<'a> {
    item_sep: String,
    key_sep: String,
    indent: Option<String>,
    sort_keys: bool,
    ensure_ascii: bool,
    allow_nan: bool,
    default: Option<&'a Callable>,
    /// `id()`s of the containers currently being encoded (CPython's `markers`).
    markers: Vec<usize>,
}

/// Python's `id(o)` for the `Arc`-backed variants (`None` for value types
/// and the laila handles, which the encoder never recurses into).
fn obj_id(a: &Arg) -> Option<usize> {
    Some(match a {
        Arg::List(l) => Arc::as_ptr(l) as *const () as usize,
        Arg::Dict(d) => Arc::as_ptr(d) as *const () as usize,
        Arg::Tuple(t) => Arc::as_ptr(t) as *const () as usize,
        Arg::Set(s) => Arc::as_ptr(s) as *const () as usize,
        Arg::FrozenSet(s) => Arc::as_ptr(s) as *const () as usize,
        Arg::Bytes(b) => Arc::as_ptr(b) as *const () as usize,
        Arg::ByteArray(b) => Arc::as_ptr(b) as *const () as usize,
        Arg::Str(s) => Arc::as_ptr(s) as *const () as usize,
        Arg::Long(l) => Arc::as_ptr(l) as *const () as usize,
        Arg::NDArray(n) => Arc::as_ptr(n) as *const () as usize,
        Arg::Callable(c) => Arc::as_ptr(c) as *const () as usize,
        Arg::Exception(e) => Arc::as_ptr(e) as *const () as usize,
        Arg::Object(o) => Arc::as_ptr(o) as *const () as usize,
        _ => return None,
    })
}

fn circular() -> LailaError {
    LailaError::value_error("Circular reference detected")
}

impl Encoder<'_> {
    /// `_iterencode(o, level)`
    fn enc(&mut self, o: &Arg, level: usize, out: &mut String) -> Result<()> {
        match o {
            Arg::None => out.push_str("null"),
            Arg::Bool(true) => out.push_str("true"),
            Arg::Bool(false) => out.push_str("false"),
            Arg::Int(i) => out.push_str(&i.to_string()),
            Arg::Long(l) => out.push_str(&l.to_string()),
            Arg::Float(f) => out.push_str(&self.floatstr(*f)?),
            Arg::Str(s) => encode_string(s, self.ensure_ascii, out),
            Arg::List(l) => {
                let items = l.read().clone();
                self.enc_list(o, &items, level, out)?;
            }
            Arg::Tuple(t) => self.enc_list(o, t, level, out)?,
            Arg::Dict(d) => {
                let items: Vec<(Arg, Arg)> = d
                    .read()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                self.enc_dict(o, items, level, out)?;
            }
            other => match self.default {
                Some(dflt) => {
                    let id = obj_id(other);
                    if let Some(id) = id {
                        if self.markers.contains(&id) {
                            return Err(circular());
                        }
                        self.markers.push(id);
                    }
                    let replaced = dflt(Args {
                        positional: alloc::vec![other.clone()],
                        keywords: Vec::new(),
                    })?;
                    self.enc(&replaced, level, out)?;
                    if id.is_some() {
                        self.markers.pop();
                    }
                }
                None => {
                    return Err(LailaError::type_error(alloc::format!(
                        "Object of type {} is not JSON serializable",
                        other.type_name()
                    )))
                }
            },
        }
        Ok(())
    }

    /// `_floatstr(o)`
    fn floatstr(&self, f: f64) -> Result<String> {
        if f.is_nan() || f.is_infinite() {
            if !self.allow_nan {
                return Err(LailaError::value_error(alloc::format!(
                    "Out of range float values are not JSON compliant: {}",
                    float_repr(f)
                )));
            }
            return Ok(if f.is_nan() {
                "NaN"
            } else if f > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            }
            .into());
        }
        Ok(float_repr(f))
    }

    /// Opens a container: writes the newline+indent after the bracket and
    /// returns `(item separator, closing newline+indent)`.
    fn open(&self, level: usize, out: &mut String) -> (String, Option<String>) {
        match &self.indent {
            None => (self.item_sep.clone(), None),
            Some(ind) => {
                let mut nl = String::from("\n");
                for _ in 0..level + 1 {
                    nl.push_str(ind);
                }
                out.push_str(&nl);
                let mut close = String::from("\n");
                for _ in 0..level {
                    close.push_str(ind);
                }
                (alloc::format!("{}{}", self.item_sep, nl), Some(close))
            }
        }
    }

    fn mark(&mut self, o: &Arg) -> Result<()> {
        if let Some(id) = obj_id(o) {
            if self.markers.contains(&id) {
                return Err(circular());
            }
            self.markers.push(id);
        }
        Ok(())
    }

    /// `_iterencode_list(lst, level)` (lists and tuples)
    fn enc_list(&mut self, o: &Arg, items: &[Arg], level: usize, out: &mut String) -> Result<()> {
        if items.is_empty() {
            out.push_str("[]");
            return Ok(());
        }
        self.mark(o)?;
        out.push('[');
        let (sep, close) = self.open(level, out);
        for (i, v) in items.iter().enumerate() {
            if i > 0 {
                out.push_str(&sep);
            }
            self.enc(v, level + 1, out)?;
        }
        if let Some(c) = close {
            out.push_str(&c);
        }
        out.push(']');
        self.markers.pop();
        Ok(())
    }

    /// `_iterencode_dict(dct, level)`
    fn enc_dict(
        &mut self,
        o: &Arg,
        mut items: Vec<(Arg, Arg)>,
        level: usize,
        out: &mut String,
    ) -> Result<()> {
        if items.is_empty() {
            out.push_str("{}");
            return Ok(());
        }
        self.mark(o)?;
        if self.sort_keys {
            sort_items(&mut items)?;
        }
        out.push('{');
        let (sep, close) = self.open(level, out);
        for (i, (k, v)) in items.iter().enumerate() {
            if i > 0 {
                out.push_str(&sep);
            }
            let ks = self.key_str(k)?;
            encode_string(&ks, self.ensure_ascii, out);
            out.push_str(&self.key_sep);
            self.enc(v, level + 1, out)?;
        }
        if let Some(c) = close {
            out.push_str(&c);
        }
        out.push('}');
        self.markers.pop();
        Ok(())
    }

    /// Key coercion of `_iterencode_dict`: str as-is, float via `_floatstr`,
    /// `True`/`False`/`None` as JSON constants, int via `int.__repr__`.
    fn key_str(&self, k: &Arg) -> Result<String> {
        Ok(match k {
            Arg::Str(s) => s.to_string(),
            Arg::Float(f) => self.floatstr(*f)?,
            Arg::Bool(true) => "true".into(),
            Arg::Bool(false) => "false".into(),
            Arg::None => "null".into(),
            Arg::Int(i) => i.to_string(),
            Arg::Long(l) => l.to_string(),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "keys must be str, int, float, bool or None, not {}",
                    other.type_name()
                )))
            }
        })
    }
}

/// `sorted(dct.items())`: a stable merge sort on the *original* keys with
/// Python ordering; incomparable keys raise Python's `TypeError`.
fn sort_items(items: &mut Vec<(Arg, Arg)>) -> Result<()> {
    fn cmp(a: &Arg, b: &Arg) -> Option<Ordering> {
        // numeric `<` never raises, even for NaN
        if a.as_f64().is_some() && b.as_f64().is_some() {
            return Some(a.py_cmp(b).unwrap_or(Ordering::Equal));
        }
        a.py_cmp(b)
    }
    fn merge_sort(v: Vec<(Arg, Arg)>, err: &mut Option<LailaError>) -> Vec<(Arg, Arg)> {
        if v.len() <= 1 {
            return v;
        }
        let mut v = v;
        let right = v.split_off(v.len() / 2);
        let left = merge_sort(v, err);
        let right = merge_sort(right, err);
        let mut out = Vec::with_capacity(left.len() + right.len());
        let mut li = left.into_iter().peekable();
        let mut ri = right.into_iter().peekable();
        loop {
            match (li.peek(), ri.peek()) {
                (Some(l), Some(r)) => {
                    let take_right = match cmp(&r.0, &l.0) {
                        Some(Ordering::Less) => true,
                        Some(_) => false,
                        None => {
                            if err.is_none() {
                                *err = Some(LailaError::type_error(alloc::format!(
                                    "'<' not supported between instances of '{}' and '{}'",
                                    r.0.type_name(),
                                    l.0.type_name()
                                )));
                            }
                            false
                        }
                    };
                    if take_right {
                        out.push(ri.next().unwrap());
                    } else {
                        out.push(li.next().unwrap());
                    }
                }
                (Some(_), None) => out.push(li.next().unwrap()),
                (None, Some(_)) => out.push(ri.next().unwrap()),
                (None, None) => break,
            }
        }
        out
    }
    let mut err = None;
    let sorted = merge_sort(core::mem::take(items), &mut err);
    *items = sorted;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `json.encoder.encode_basestring_ascii` / `encode_basestring` (appends,
/// including the surrounding quotes).
pub fn encode_string(s: &str, ensure_ascii: bool, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&alloc::format!("\\u{:04x}", c as u32)),
            c if ensure_ascii && (c as u32) > 0x7e => {
                let mut buf = [0u16; 2];
                for u in c.encode_utf16(&mut buf) {
                    out.push_str(&alloc::format!("\\u{:04x}", u));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// --------------------------------------------------------------------------
// loads
// --------------------------------------------------------------------------

/// `json.decoder.JSONDecodeError(msg, doc, pos)`: `str(exc)` is
/// `"<msg>: line L column C (char N)"` with code-point positions.
fn decode_error(doc: &str, msg: &str, pos: usize) -> LailaError {
    let upto = &doc[..pos];
    let lineno = upto.matches('\n').count() + 1;
    let colno = match upto.rfind('\n') {
        Some(b) => upto[b + 1..].chars().count() + 1,
        None => upto.chars().count() + 1,
    };
    LailaError::new(
        ExcKind::JSONDecodeError,
        alloc::format!(
            "{msg}: line {lineno} column {colno} (char {})",
            upto.chars().count()
        ),
    )
}

/// `json.loads(s)` for a `str`.
pub fn loads(s: &str) -> Result<Arg> {
    if s.starts_with('\u{feff}') {
        return Err(decode_error(
            s,
            "Unexpected UTF-8 BOM (decode using utf-8-sig)",
            0,
        ));
    }
    decode(s)
}

/// `JSONDecoder.decode(s)`
fn decode(s: &str) -> Result<Arg> {
    let mut p = Parser {
        s,
        b: s.as_bytes(),
        i: 0,
    };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err(p.fail("Extra data", p.i));
    }
    Ok(v)
}

/// `json.loads(b)` for `bytes` / `bytearray`: UTF-8 (a leading BOM is
/// stripped as `utf-8-sig`, like `json.detect_encoding`; the BOM check of
/// `loads` applies to `str` input only, as in CPython).
pub fn loads_bytes(b: &[u8]) -> Result<Arg> {
    let b = b.strip_prefix(b"\xef\xbb\xbf").unwrap_or(b);
    decode(decode_utf8(b)?)
}

/// `bytes.decode("utf-8")` with CPython's `UnicodeDecodeError` texts.
fn decode_utf8(b: &[u8]) -> Result<&str> {
    core::str::from_utf8(b).map_err(|e| {
        let start = e.valid_up_to();
        let msg = match e.error_len() {
            None => {
                let end = b.len() - 1;
                if end > start {
                    alloc::format!(
                        "'utf-8' codec can't decode bytes in position {start}-{end}: unexpected end of data"
                    )
                } else {
                    alloc::format!(
                        "'utf-8' codec can't decode byte 0x{:02x} in position {start}: unexpected end of data",
                        b[start]
                    )
                }
            }
            Some(_) => {
                let byte = b[start];
                let reason = if (0xc2..=0xf4).contains(&byte) {
                    "invalid continuation byte"
                } else {
                    "invalid start byte"
                };
                alloc::format!(
                    "'utf-8' codec can't decode byte 0x{byte:02x} in position {start}: {reason}"
                )
            }
        };
        LailaError::new(ExcKind::UnicodeDecodeError, msg)
    })
}

/// Port of `_json.c`'s `scan_once_unicode` / `scanstring_unicode` /
/// `_parse_object_unicode` / `_parse_array_unicode` / `_match_number_unicode`
/// (strict mode). Positions are byte offsets into `s`; `fail` converts them
/// to code points.
struct Parser<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn fail(&self, msg: &str, pos: usize) -> LailaError {
        decode_error(self.s, msg, pos)
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    /// `WHITESPACE = r'[ \t\n\r]*'`
    fn ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.peek() {
            self.i += 1;
        }
    }

    fn starts_with(&self, lit: &[u8]) -> bool {
        self.b[self.i..].starts_with(lit)
    }

    /// `scan_once_unicode(s, idx)`
    fn value(&mut self) -> Result<Arg> {
        let Some(c) = self.peek() else {
            return Err(self.fail("Expecting value", self.i));
        };
        match c {
            b'"' => {
                let begin = self.i;
                self.i += 1;
                Ok(Arg::Str(self.string(begin)?.into()))
            }
            b'{' => self.object(),
            b'[' => self.array(),
            b'n' if self.starts_with(b"null") => {
                self.i += 4;
                Ok(Arg::None)
            }
            b't' if self.starts_with(b"true") => {
                self.i += 4;
                Ok(Arg::Bool(true))
            }
            b'f' if self.starts_with(b"false") => {
                self.i += 5;
                Ok(Arg::Bool(false))
            }
            b'N' if self.starts_with(b"NaN") => {
                self.i += 3;
                Ok(Arg::Float(f64::NAN))
            }
            b'I' if self.starts_with(b"Infinity") => {
                self.i += 8;
                Ok(Arg::Float(f64::INFINITY))
            }
            b'-' if self.starts_with(b"-Infinity") => {
                self.i += 9;
                Ok(Arg::Float(f64::NEG_INFINITY))
            }
            _ => self.number(),
        }
    }

    /// `_match_number_unicode`: `-?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?`
    fn number(&mut self) -> Result<Arg> {
        let b = self.b;
        let start = self.i;
        let mut idx = start;
        let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
        if b.get(idx) == Some(&b'-') {
            idx += 1;
        }
        match b.get(idx) {
            Some(b'1'..=b'9') => {
                idx += 1;
                while digit(idx) {
                    idx += 1;
                }
            }
            Some(b'0') => idx += 1,
            _ => return Err(self.fail("Expecting value", start)),
        }
        let mut is_float = false;
        if b.get(idx) == Some(&b'.') && digit(idx + 1) {
            is_float = true;
            idx += 2;
            while digit(idx) {
                idx += 1;
            }
        }
        if let Some(b'e' | b'E') = b.get(idx) {
            let e_start = idx;
            idx += 1;
            if let Some(b'-' | b'+') = b.get(idx) {
                idx += 1;
            }
            while digit(idx) {
                idx += 1;
            }
            if digit(idx - 1) {
                is_float = true;
            } else {
                idx = e_start;
            }
        }
        let text = &self.s[start..idx];
        self.i = idx;
        if is_float {
            // Rust's parser is correctly rounded, like CPython's `float()`.
            let f: f64 = text
                .parse()
                .map_err(|_| self.fail("Expecting value", start))?;
            Ok(Arg::Float(f))
        } else if let Ok(i) = text.parse::<i64>() {
            Ok(Arg::Int(i))
        } else {
            let big: BigInt = text
                .parse()
                .map_err(|_| self.fail("Expecting value", start))?;
            Ok(Arg::from_bigint(big))
        }
    }

    /// `scanstring_unicode(s, end=begin+1, strict=True)`; `begin` is the
    /// index of the opening quote and `self.i` is just past it.
    fn string(&mut self, begin: usize) -> Result<String> {
        let b = self.b;
        let len = b.len();
        let mut out = String::new();
        let mut i = begin + 1;
        loop {
            let start = i;
            while i < len {
                let c = b[i];
                if c == b'"' || c == b'\\' {
                    break;
                }
                if c < 0x20 {
                    return Err(self.fail("Invalid control character at", i));
                }
                i += 1;
            }
            if i >= len {
                return Err(self.fail("Unterminated string starting at", begin));
            }
            out.push_str(&self.s[start..i]);
            if b[i] == b'"' {
                self.i = i + 1;
                return Ok(out);
            }
            // escape
            i += 1;
            if i >= len {
                return Err(self.fail("Unterminated string starting at", begin));
            }
            let e = b[i];
            if e != b'u' {
                let ch = match e {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    _ => return Err(self.fail("Invalid \\escape", i - 1)),
                };
                out.push(ch);
                i += 1;
                continue;
            }
            let upos = i;
            i += 1;
            let end = i + 4;
            if end >= len {
                return Err(self.fail("Invalid \\uXXXX escape", upos));
            }
            let mut cp =
                hex4(&b[i..end]).ok_or_else(|| self.fail("Invalid \\uXXXX escape", upos))?;
            i = end;
            // surrogate pair
            if (0xd800..0xdc00).contains(&cp)
                && end + 6 < len
                && b[end] == b'\\'
                && b[end + 1] == b'u'
            {
                let end2 = end + 6;
                let c2 = hex4(&b[end + 2..end2])
                    .ok_or_else(|| self.fail("Invalid \\uXXXX escape", end + 1))?;
                if (0xdc00..0xe000).contains(&c2) {
                    cp = 0x10000 + ((cp - 0xd800) << 10) + (c2 - 0xdc00);
                    i = end2;
                }
            }
            // A lone surrogate is a legal Python `str` but not a Rust one.
            out.push(char::from_u32(cp).unwrap_or('\u{fffd}'));
        }
    }

    /// `_parse_object_unicode`
    fn object(&mut self) -> Result<Arg> {
        self.i += 1; // '{'
        let mut map: PyMap<Arg, Arg> = PyMap::default();
        self.ws();
        if self.peek() != Some(b'}') {
            loop {
                if self.peek() != Some(b'"') {
                    return Err(
                        self.fail("Expecting property name enclosed in double quotes", self.i)
                    );
                }
                let begin = self.i;
                self.i += 1;
                let key = self.string(begin)?;
                self.ws();
                if self.peek() != Some(b':') {
                    return Err(self.fail("Expecting ':' delimiter", self.i));
                }
                self.i += 1;
                self.ws();
                let val = self.value()?;
                // later duplicates overwrite earlier ones (position of first kept)
                map.insert(Arg::Str(key.into()), val);
                self.ws();
                if self.peek() == Some(b'}') {
                    break;
                }
                if self.peek() != Some(b',') {
                    return Err(self.fail("Expecting ',' delimiter", self.i));
                }
                let comma = self.i;
                self.i += 1;
                self.ws();
                if self.peek() == Some(b'}') {
                    return Err(self.fail("Illegal trailing comma before end of object", comma));
                }
            }
        }
        self.i += 1; // '}'
        Ok(Arg::Dict(Arc::new(RwLock::new(map))))
    }

    /// `_parse_array_unicode`
    fn array(&mut self) -> Result<Arg> {
        self.i += 1; // '['
        let mut items: Vec<Arg> = Vec::new();
        self.ws();
        if self.peek() != Some(b']') {
            loop {
                items.push(self.value()?);
                self.ws();
                if self.peek() == Some(b']') {
                    break;
                }
                if self.peek() != Some(b',') {
                    return Err(self.fail("Expecting ',' delimiter", self.i));
                }
                let comma = self.i;
                self.i += 1;
                self.ws();
                if self.peek() == Some(b']') {
                    return Err(self.fail("Illegal trailing comma before end of array", comma));
                }
            }
        }
        self.i += 1; // ']'
        Ok(Arg::List(Arc::new(RwLock::new(items))))
    }
}

fn hex4(b: &[u8]) -> Option<u32> {
    let mut v = 0u32;
    for &c in b {
        v = (v << 4) | (c as char).to_digit(16)?;
    }
    Some(v)
}
