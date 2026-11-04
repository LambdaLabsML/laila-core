//! Python `repr()` text for scalars (Phase 0 subset; Phase 1 `_compat/pyrepr`
//! replaces `float_repr` with the exact shortest-roundtrip algorithm).

use alloc::string::String;

/// `repr(float)`: `1.0`, `0.5`, `1e+100`, `nan`, `inf`.
pub fn float_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if crate::_compat::fmath::fract(f) == 0.0 && f.abs() < 1e16 {
        return alloc::format!("{f:.1}");
    }
    // Rust's `{:?}` is shortest-roundtrip; normalise the exponent spelling.
    let s = alloc::format!("{f:?}");
    if let Some((m, e)) = s.split_once('e') {
        let (sign, digits) = match e.strip_prefix('-') {
            Some(d) => ("-", d),
            None => ("+", e),
        };
        alloc::format!("{m}e{sign}{digits:0>2}")
    } else {
        s
    }
}

/// `repr(str)`: single quotes unless the text contains `'` and no `"`.
pub fn str_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&alloc::format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// `repr(bytes)`: `b'...'`.
pub fn bytes_repr(b: &[u8]) -> String {
    let quote = if b.contains(&b'\'') && !b.contains(&b'"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::from("b");
    out.push(quote);
    for &c in b {
        match c {
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            c if c as char == quote => {
                out.push('\\');
                out.push(c as char);
            }
            0x20..=0x7e => out.push(c as char),
            c => out.push_str(&alloc::format!("\\x{c:02x}")),
        }
    }
    out.push(quote);
    out
}
