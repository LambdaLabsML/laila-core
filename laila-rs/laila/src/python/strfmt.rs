//! Python string formatting at run time: `str.format(*args, **kwargs)`
//! and printf-style `str % values`.
//!
//! The `laila::python!` rewriter lowers f-strings at compile time (the
//! field expressions are Rust); a *template* that is formatted later --
//! `_SCRIPT.format(path=p)`, `"k%d" % i` -- goes through here with the
//! same replacement-field grammar: `{}` / `{0}` / `{name}` with optional
//! `!r` / `!s` conversion and `:spec` (`fill`, `align`, sign, width,
//! `,`, `.precision`, `d` / `f` / `e` / `x` / `s` / `%`), and `%s` /
//! `%d` / `%r` / `%f` / `%x` / `%%` with optional flags, width and
//! precision.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use super::args::Args;
use super::errors::{ExcKind, LailaError, Result};
use super::object::Arg;
use super::pyrepr::float_repr;

/// `template.format(*args, **kwargs)`.
pub fn str_format(template: &str, args: &Args) -> Result<String> {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let mut auto_index = 0usize;
    let mut manual = false;
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if chars.peek() == Some(&'{') {
                    chars.next();
                    out.push('{');
                    continue;
                }
                let mut field = String::new();
                let mut depth = 1;
                for ch in chars.by_ref() {
                    if ch == '{' {
                        depth += 1;
                    } else if ch == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    field.push(ch);
                }
                if depth != 0 {
                    return Err(LailaError::new(
                        ExcKind::ValueError,
                        "Single '{' encountered in format string",
                    ));
                }
                let (name, conv, spec) = split_field(&field);
                // nested fields inside the spec (`{:{width}}`)
                let spec = if spec.contains('{') {
                    str_format(&spec, args)?
                } else {
                    spec
                };
                let value = if name.is_empty() {
                    if manual {
                        return Err(LailaError::new(
                            ExcKind::ValueError,
                            "cannot switch from manual field specification to automatic field numbering",
                        ));
                    }
                    let v = positional(args, auto_index)?;
                    auto_index += 1;
                    v
                } else if let Ok(i) = name.parse::<usize>() {
                    manual = true;
                    positional(args, i)?
                } else {
                    lookup(args, &name)?
                };
                let text = match conv.as_deref() {
                    Some("r") => value.__repr__(),
                    Some("s") | None if spec.is_empty() => value.__str__(),
                    Some("s") => value.__str__(),
                    Some("a") => value.__repr__(),
                    Some(other) => {
                        return Err(LailaError::new(
                            ExcKind::ValueError,
                            alloc::format!("Unknown conversion specifier {other}"),
                        ))
                    }
                    None => format_spec(&value, &spec)?,
                };
                let text = if conv.is_some() && !spec.is_empty() {
                    format_spec(&Arg::Str(text.into()), &spec)?
                } else {
                    text
                };
                out.push_str(&text);
            }
            '}' => {
                if chars.peek() == Some(&'}') {
                    chars.next();
                    out.push('}');
                } else {
                    return Err(LailaError::new(
                        ExcKind::ValueError,
                        "Single '}' encountered in format string",
                    ));
                }
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

fn positional(args: &Args, i: usize) -> Result<Arg> {
    args.positional.get(i).cloned().ok_or_else(|| {
        LailaError::new(
            ExcKind::IndexError,
            alloc::format!("Replacement index {i} out of range for positional args tuple"),
        )
    })
}

/// `{name}`, `{name.attr}`, `{name[key]}`.
fn lookup(args: &Args, name: &str) -> Result<Arg> {
    let end = name.find(['.', '[']).unwrap_or(name.len());
    let (head, rest) = name.split_at(end);
    let mut value = if let Ok(i) = head.parse::<usize>() {
        positional(args, i)?
    } else {
        args.keywords
            .iter()
            .find(|(k, _)| k == head)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| LailaError::key_error(alloc::format!("'{head}'")))?
    };
    let mut rest = rest;
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[']).unwrap_or(r.len());
            value = value.__getattr__(&r[..end])?;
            rest = &r[end..];
        } else if let Some(r) = rest.strip_prefix('[') {
            let close = r.find(']').ok_or_else(|| {
                LailaError::new(ExcKind::ValueError, "Missing ']' in format string")
            })?;
            let key = &r[..close];
            let key = match key.parse::<i64>() {
                Ok(i) => Arg::Int(i),
                Err(_) => Arg::Str(key.into()),
            };
            value = value.__getitem__(&key)?;
            rest = &r[close + 1..];
        } else {
            return Err(LailaError::new(
                ExcKind::ValueError,
                "Only '.' or '[' may follow ']' in format field specifier",
            ));
        }
    }
    Ok(value)
}

/// `name!conv:spec` -> (name, conv, spec); `!`/`:` inside brackets are
/// part of the name.
fn split_field(field: &str) -> (String, Option<String>, String) {
    let mut depth = 0;
    let mut conv_at = None;
    let mut spec_at = None;
    for (i, c) in field.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            '!' if depth == 0 && conv_at.is_none() && spec_at.is_none() => conv_at = Some(i),
            ':' if depth == 0 && spec_at.is_none() => {
                spec_at = Some(i);
                break;
            }
            _ => {}
        }
    }
    let spec = spec_at
        .map(|i| field[i + 1..].to_string())
        .unwrap_or_default();
    let name_end = conv_at.or(spec_at).unwrap_or(field.len());
    let conv = conv_at.map(|i| field[i + 1..spec_at.unwrap_or(field.len())].to_string());
    (field[..name_end].to_string(), conv, spec)
}

/// An f-string field with a format spec, `f"{x:>12}"` / `f"{n:,.2f}"`:
/// what the `laila::python!` rewriter emits for the placeholder. The spec
/// is a source literal, so a malformed one is a programming error and
/// surfaces as the uncaught `ValueError` it would be in Python.
pub fn fstr_spec(value: impl super::object::IntoArg, spec: &str) -> String {
    match format_spec(&value.into_arg(), spec) {
        Ok(s) => s,
        Err(e) => panic!("{}", e),
    }
}

/// `format(value, spec)` -- the format-spec mini-language.
pub fn format_spec(value: &Arg, spec: &str) -> Result<String> {
    if spec.is_empty() {
        return Ok(value.__str__());
    }
    let chars: Vec<char> = spec.chars().collect();
    let mut i = 0;
    let (mut fill, mut align) = (' ', None);
    if chars.len() >= 2 && matches!(chars[1], '<' | '>' | '^' | '=') {
        fill = chars[0];
        align = Some(chars[1]);
        i = 2;
    } else if !chars.is_empty() && matches!(chars[0], '<' | '>' | '^' | '=') {
        align = Some(chars[0]);
        i = 1;
    }
    let mut sign = '-';
    if i < chars.len() && matches!(chars[i], '+' | '-' | ' ') {
        sign = chars[i];
        i += 1;
    }
    let mut alternate = false;
    if i < chars.len() && chars[i] == '#' {
        alternate = true;
        i += 1;
    }
    if i < chars.len() && chars[i] == '0' && align.is_none() {
        fill = '0';
        align = Some('=');
        i += 1;
    }
    let mut width = String::new();
    while i < chars.len() && chars[i].is_ascii_digit() {
        width.push(chars[i]);
        i += 1;
    }
    let width: usize = width.parse().unwrap_or(0);
    let mut grouping = None;
    if i < chars.len() && matches!(chars[i], ',' | '_') {
        grouping = Some(chars[i]);
        i += 1;
    }
    let mut precision = None;
    if i < chars.len() && chars[i] == '.' {
        i += 1;
        let mut p = String::new();
        while i < chars.len() && chars[i].is_ascii_digit() {
            p.push(chars[i]);
            i += 1;
        }
        precision = Some(p.parse::<usize>().map_err(|_| {
            LailaError::new(ExcKind::ValueError, "Format specifier missing precision")
        })?);
    }
    let ty = if i < chars.len() {
        let t = chars[i];
        i += 1;
        Some(t)
    } else {
        None
    };
    if i != chars.len() {
        return Err(LailaError::new(
            ExcKind::ValueError,
            "Invalid format specifier",
        ));
    }

    let is_number = matches!(value, Arg::Int(_) | Arg::Float(_) | Arg::Bool(_));
    let (body, negative): (String, bool) = match (ty, value) {
        (None | Some('s'), _) if !is_number || ty == Some('s') => {
            if is_number && ty == Some('s') {
                return Err(LailaError::new(
                    ExcKind::ValueError,
                    alloc::format!(
                        "Unknown format code 's' for object of type '{}'",
                        value.type_name()
                    ),
                ));
            }
            let mut s = value.__str__();
            if let Some(p) = precision {
                s = s.chars().take(p).collect();
            }
            (s, false)
        }
        (None | Some('d'), v) if matches!(v, Arg::Int(_) | Arg::Bool(_)) => {
            let n = v.as_i64().unwrap_or(0);
            (group(&n.unsigned_abs().to_string(), grouping), n < 0)
        }
        (Some('x') | Some('X') | Some('o') | Some('b'), v)
            if matches!(v, Arg::Int(_) | Arg::Bool(_)) =>
        {
            let n = v.as_i64().unwrap_or(0);
            let m = n.unsigned_abs();
            let (digits, prefix) = match ty {
                Some('x') => (alloc::format!("{m:x}"), "0x"),
                Some('X') => (alloc::format!("{m:X}"), "0X"),
                Some('o') => (alloc::format!("{m:o}"), "0o"),
                _ => (alloc::format!("{m:b}"), "0b"),
            };
            let digits = if alternate {
                alloc::format!("{prefix}{digits}")
            } else {
                digits
            };
            (digits, n < 0)
        }
        (None, Arg::Float(f)) => (
            group(&float_repr(f.abs()), grouping),
            f.is_sign_negative() && *f != 0.0,
        ),
        (Some('f') | Some('F'), v) if is_number => {
            let f = v.as_number().unwrap_or(0.0);
            let p = precision.unwrap_or(6);
            (
                group(&alloc::format!("{:.*}", p, f.abs()), grouping),
                f < 0.0,
            )
        }
        (Some('e') | Some('E'), v) if is_number => {
            let f = v.as_number().unwrap_or(0.0);
            let p = precision.unwrap_or(6);
            let mut s = alloc::format!("{:.*e}", p, f.abs());
            // Rust prints `1.5e2`; Python `1.500000e+02`.
            if let Some(pos) = s.find('e') {
                let (m, e) = s.split_at(pos);
                let e: i32 = e[1..].parse().unwrap_or(0);
                s = alloc::format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs());
            }
            if ty == Some('E') {
                s = s.to_uppercase();
            }
            (s, f < 0.0)
        }
        (Some('g') | Some('G'), v) if is_number => {
            let f = v.as_number().unwrap_or(0.0);
            (float_repr(f.abs()), f < 0.0)
        }
        (Some('%'), v) if is_number => {
            let f = v.as_number().unwrap_or(0.0) * 100.0;
            let p = precision.unwrap_or(6);
            (alloc::format!("{:.*}%", p, f.abs()), f < 0.0)
        }
        (Some(t), v) => {
            return Err(LailaError::new(
                ExcKind::ValueError,
                alloc::format!(
                    "Unknown format code '{t}' for object of type '{}'",
                    v.type_name()
                ),
            ))
        }
        (None, v) => (v.__str__(), false),
    };
    let sign_str = if negative {
        "-"
    } else {
        match sign {
            '+' if is_number => "+",
            ' ' if is_number => " ",
            _ => "",
        }
    };
    let align = align.unwrap_or(if is_number { '>' } else { '<' });
    let content_len = sign_str.chars().count() + body.chars().count();
    let pad = width.saturating_sub(content_len);
    let fill_str = |n: usize| fill.to_string().repeat(n);
    Ok(match align {
        '<' => alloc::format!("{sign_str}{body}{}", fill_str(pad)),
        '>' => alloc::format!("{}{sign_str}{body}", fill_str(pad)),
        '^' => alloc::format!(
            "{}{sign_str}{body}{}",
            fill_str(pad / 2),
            fill_str(pad - pad / 2)
        ),
        _ => alloc::format!("{sign_str}{}{body}", fill_str(pad)),
    })
}

fn group(digits: &str, sep: Option<char>) -> String {
    let Some(sep) = sep else {
        return digits.to_string();
    };
    let (int_part, frac) = match digits.find('.') {
        Some(p) => (&digits[..p], &digits[p..]),
        None => (digits, ""),
    };
    let mut out = String::new();
    let n = int_part.len();
    for (i, c) in int_part.chars().enumerate() {
        if i > 0 && (n - i) % 3 == 0 {
            out.push(sep);
        }
        out.push(c);
    }
    out.push_str(frac);
    out
}

/// `template % values` -- `values` is a tuple, a dict (for `%(name)s`) or
/// one value.
pub fn percent_format(template: &str, values: &Arg) -> Result<String> {
    let items: Vec<Arg> = match values {
        Arg::Tuple(t) => t.to_vec(),
        other => alloc::vec![other.clone()],
    };
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let mut next = 0usize;
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        // %(name)
        let mut key = None;
        if chars.peek() == Some(&'(') {
            chars.next();
            let mut k = String::new();
            for ch in chars.by_ref() {
                if ch == ')' {
                    break;
                }
                k.push(ch);
            }
            key = Some(k);
        }
        let mut flags = String::new();
        while let Some(&f) = chars.peek() {
            if matches!(f, '-' | '+' | ' ' | '#' | '0') {
                flags.push(f);
                chars.next();
            } else {
                break;
            }
        }
        let mut width = String::new();
        while let Some(&d) = chars.peek() {
            if d.is_ascii_digit() {
                width.push(d);
                chars.next();
            } else {
                break;
            }
        }
        let mut precision = None;
        if chars.peek() == Some(&'.') {
            chars.next();
            let mut p = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_ascii_digit() {
                    p.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            precision = Some(p);
        }
        let ty = chars
            .next()
            .ok_or_else(|| LailaError::new(ExcKind::ValueError, "incomplete format"))?;
        let value = match &key {
            Some(k) => values.__getitem__(Arg::Str(k.as_str().into()))?,
            None => {
                let v = items.get(next).cloned().ok_or_else(|| {
                    LailaError::type_error("not enough arguments for format string")
                })?;
                next += 1;
                v
            }
        };
        // Translate to a format-spec: flags/width/precision/type.
        let mut spec = String::new();
        if flags.contains('-') {
            spec.push('<');
        } else if flags.contains('0') && !matches!(ty, 's' | 'r' | 'a') {
            spec.push('0');
        }
        if flags.contains('+') {
            spec.push('+');
        } else if flags.contains(' ') {
            spec.push(' ');
        }
        if flags.contains('#') {
            spec.push('#');
        }
        spec.push_str(&width);
        if let Some(p) = &precision {
            spec.push('.');
            spec.push_str(if p.is_empty() { "0" } else { p });
        }
        let text = match ty {
            's' => format_spec(
                &Arg::Str(value.__str__().into()),
                &strip_precision(&spec, precision.is_some()),
            )?,
            'r' | 'a' => format_spec(
                &Arg::Str(value.__repr__().into()),
                &strip_precision(&spec, precision.is_some()),
            )?,
            'd' | 'i' | 'u' => {
                let n = match &value {
                    Arg::Float(f) => Arg::Int(crate::_compat::fmath::trunc(*f) as i64),
                    Arg::Int(_) | Arg::Bool(_) => value.clone(),
                    other => {
                        return Err(LailaError::type_error(alloc::format!(
                            "%d format: a real number is required, not {}",
                            other.type_name()
                        )))
                    }
                };
                spec.push('d');
                format_spec(&n, &spec)?
            }
            'f' | 'F' | 'e' | 'E' | 'g' | 'G' => {
                if !matches!(value, Arg::Int(_) | Arg::Float(_) | Arg::Bool(_)) {
                    return Err(LailaError::type_error(alloc::format!(
                        "must be real number, not {}",
                        value.type_name()
                    )));
                }
                spec.push(ty);
                format_spec(&value, &spec)?
            }
            'x' | 'X' | 'o' => {
                spec.push(ty);
                format_spec(&value, &spec)?
            }
            'c' => value.__str__(),
            other => {
                return Err(LailaError::new(
                    ExcKind::ValueError,
                    alloc::format!(
                        "unsupported format character '{other}' (0x{:x})",
                        other as u32
                    ),
                ))
            }
        };
        out.push_str(&text);
    }
    if next < items.len() && matches!(values, Arg::Tuple(_)) {
        return Err(LailaError::type_error(
            "not all arguments converted during string formatting",
        ));
    }
    Ok(out)
}

/// `%.3s` truncates: keep the precision for strings, drop numeric-only flags.
fn strip_precision(spec: &str, _has_precision: bool) -> String {
    spec.replace('0', "").replace(['+', ' ', '#'], "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::python::args::Args;

    fn fmt(t: &str, a: Args) -> String {
        str_format(t, &a).unwrap()
    }

    #[test]
    fn format_fields() {
        let mut a = Args::new();
        a.push_positional(3);
        a.push_positional("x");
        assert_eq!(fmt("{} {}", a.clone()), "3 x");
        assert_eq!(fmt("{1}{0}", a.clone()), "x3");
        assert_eq!(fmt("{{{0}}}", a.clone()), "{3}");
        assert_eq!(fmt("{1!r}", a), "'x'");
        let mut kw = Args::new();
        kw.push_keyword("path", "/tmp/a b");
        kw.push_keyword("n", -5);
        assert_eq!(
            fmt("p={path!r} n={n:>4}", kw.clone()),
            "p='/tmp/a b' n=  -5"
        );
        assert_eq!(fmt("{n:+d}|{n:05d}|{n:x}", kw), "-5|-0005|-5");
    }

    #[test]
    fn format_specs() {
        assert_eq!(format_spec(&Arg::Float(1.23456), ".2f").unwrap(), "1.23");
        assert_eq!(format_spec(&Arg::Int(1234567), ",").unwrap(), "1,234,567");
        assert_eq!(
            format_spec(&Arg::Str("ab".into()), "*^6").unwrap(),
            "**ab**"
        );
        assert_eq!(
            format_spec(&Arg::Float(1234.5), "e").unwrap(),
            "1.234500e+03"
        );
        assert_eq!(format_spec(&Arg::Float(0.256), ".1%").unwrap(), "25.6%");
        assert_eq!(format_spec(&Arg::Int(255), "#x").unwrap(), "0xff");
    }

    #[test]
    fn percent() {
        assert_eq!(percent_format("k%d", &Arg::Int(7)).unwrap(), "k7");
        let t = Arg::Tuple(alloc::vec![Arg::Str("a".into()), Arg::Float(2.5)].into());
        assert_eq!(percent_format("%s=%.1f%%", &t).unwrap(), "a=2.5%");
        assert_eq!(
            percent_format(
                "%5d|%-3s|%r",
                &Arg::Tuple(
                    alloc::vec![Arg::Int(42), Arg::Str("x".into()), Arg::Str("y".into())].into()
                )
            )
            .unwrap(),
            "   42|x  |'y'"
        );
        assert!(percent_format("%d %d", &Arg::Int(1)).is_err());
    }
}
