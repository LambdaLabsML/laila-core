//! Python `base64` / `binascii` semantics on byte slices.
//!
//! ```text
//! b64encode(s, altchars=None) -> bytes
//! b64decode(s, altchars=None, validate=False) -> bytes
//! urlsafe_b64encode / urlsafe_b64decode, standard_b64encode / standard_b64decode
//! ```
//!
//! `b64decode` with `validate=False` discards non-alphabet characters
//! (`binascii.a2b_base64` non-strict) and raises `binascii.Error` on bad
//! padding; `validate=True` rejects any non-alphabet character up front.
//! `str` inputs must be ASCII (`ValueError` otherwise), as in Python.

use alloc::string::String;
use alloc::vec::Vec;

use crate::python::errors::ExcKind;
use crate::python::object::Arg;
use crate::python::{LailaError, Result};

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn binascii_error(msg: impl Into<String>) -> LailaError {
    LailaError::new(ExcKind::BinasciiError, msg)
}

/// `binascii`'s "bytes-like object or ASCII string" coercion.
fn bytes_from_decode_data(s: &Arg) -> Result<Vec<u8>> {
    match s {
        Arg::Str(st) => {
            if !st.is_ascii() {
                return Err(LailaError::value_error(
                    "string argument should contain only ASCII characters",
                ));
            }
            Ok(st.as_bytes().to_vec())
        }
        other => other.as_bytes_like().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "argument should be a bytes-like object or ASCII string, not '{}'",
                other.type_name()
            ))
        }),
    }
}

fn altchars_pair(alt: &[u8]) -> Result<(u8, u8)> {
    if alt.len() != 2 {
        // CPython: `assert len(altchars) == 2`
        return Err(LailaError::new(
            ExcKind::AssertionError,
            crate::python::pyrepr::bytes_repr(alt),
        ));
    }
    Ok((alt[0], alt[1]))
}

/// `binascii.b2a_base64(data, newline=False)` core.
pub fn b2a_base64(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(STD[(n >> 18) as usize & 63]);
        out.push(STD[(n >> 12) as usize & 63]);
        out.push(if chunk.len() > 1 {
            STD[(n >> 6) as usize & 63]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            STD[n as usize & 63]
        } else {
            b'='
        });
    }
    out
}

/// `base64.b64encode(s, altchars=None)`.
pub fn b64encode(s: &[u8], altchars: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut enc = b2a_base64(s);
    if let Some(alt) = altchars {
        let (a, b) = altchars_pair(alt)?;
        for c in enc.iter_mut() {
            if *c == b'+' {
                *c = a;
            } else if *c == b'/' {
                *c = b;
            }
        }
    }
    Ok(enc)
}

/// `base64.b64decode(s, altchars=None, validate=False)`.
pub fn b64decode(s: &Arg, altchars: Option<&[u8]>, validate: bool) -> Result<Vec<u8>> {
    let mut data = bytes_from_decode_data(s)?;
    if let Some(alt) = altchars {
        let (a, b) = altchars_pair(alt)?;
        for c in data.iter_mut() {
            if *c == a {
                *c = b'+';
            } else if *c == b {
                *c = b'/';
            }
        }
    }
    a2b_base64(&data, validate)
}

/// `binascii.a2b_base64(data, strict_mode=False)` -- faithful port of the
/// CPython decoder including its error messages.
pub fn a2b_base64(buf: &[u8], strict_mode: bool) -> Result<Vec<u8>> {
    let n = buf.len();
    let mut out: Vec<u8> = Vec::with_capacity(n / 4 * 3);
    let mut quad_pos = 0u8;
    let mut leftchar: u32 = 0;
    let mut pads = 0u8;
    let mut padding_started = false;
    if strict_mode && n > 0 && buf[0] == b'=' {
        return Err(binascii_error("Leading padding not allowed"));
    }
    for (i, &c) in buf.iter().enumerate() {
        if c == b'=' {
            padding_started = true;
            if strict_mode && quad_pos == 0 {
                return Err(binascii_error(if i == 0 {
                    "Leading padding not allowed"
                } else {
                    "Excess padding not allowed"
                }));
            }
            if quad_pos >= 2 {
                pads += 1;
                if quad_pos + pads >= 4 {
                    if strict_mode && i + 1 < n {
                        return Err(binascii_error("Excess data after padding"));
                    }
                    return Ok(out);
                }
            }
            continue;
        }
        let v = match STD.iter().position(|&x| x == c) {
            Some(v) => v as u32,
            None => {
                if strict_mode {
                    return Err(binascii_error("Only base64 data is allowed"));
                }
                continue;
            }
        };
        if strict_mode && padding_started {
            return Err(binascii_error("Discontinuous padding not allowed"));
        }
        pads = 0;
        match quad_pos {
            0 => {
                quad_pos = 1;
                leftchar = v;
            }
            1 => {
                quad_pos = 2;
                out.push(((leftchar << 2) | (v >> 4)) as u8);
                leftchar = v & 0x0f;
            }
            2 => {
                quad_pos = 3;
                out.push(((leftchar << 4) | (v >> 2)) as u8);
                leftchar = v & 0x03;
            }
            _ => {
                quad_pos = 0;
                out.push(((leftchar << 6) | v) as u8);
                leftchar = 0;
            }
        }
    }
    if quad_pos != 0 {
        if quad_pos == 1 {
            let count = out.len();
            return Err(binascii_error(alloc::format!(
                "Invalid base64-encoded string: number of data characters ({}) cannot be 1 more than a multiple of 4",
                count / 3 * 4 + 1
            )));
        }
        return Err(binascii_error("Incorrect padding"));
    }
    Ok(out)
}

pub fn standard_b64encode(s: &[u8]) -> Vec<u8> {
    b2a_base64(s)
}
pub fn standard_b64decode(s: &Arg) -> Result<Vec<u8>> {
    b64decode(s, None, false)
}
pub fn urlsafe_b64encode(s: &[u8]) -> Vec<u8> {
    b64encode(s, Some(b"-_")).expect("two altchars")
}
pub fn urlsafe_b64decode(s: &Arg) -> Result<Vec<u8>> {
    b64decode(s, Some(b"-_"), false)
}

/// `b64encode(..).decode("ascii")`.
pub fn b64encode_str(s: &[u8], altchars: Option<&[u8]>) -> Result<String> {
    Ok(String::from_utf8(b64encode(s, altchars)?).expect("base64 is ASCII"))
}
