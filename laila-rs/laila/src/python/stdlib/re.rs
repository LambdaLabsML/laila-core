//! `import re` -- `re.compile` / `match` / `search` / `fullmatch` /
//! `findall` / `sub` over the in-tree engine (`_compat::re_lite`): a
//! compiled `Pattern` and `Match` objects with Python's `group` /
//! `groups` / `groupdict` / `start` / `end` / `span`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::re_lite::{Match as RawMatch, Regex};
use crate::python::object::PyObject;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `import re`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct re_mod;
#[allow(non_upper_case_globals)]
pub static re: re_mod = re_mod;

impl PyObject for re_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 're'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<re_mod>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x72_65)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "compile" => self.compile(args).map(IntoArg::into_arg),
            "match" => self.r#match(args),
            "search" => self.search(args),
            "fullmatch" => self.fullmatch(args),
            "findall" => self.findall(args),
            "sub" => self.sub(args),
            "escape" => self.escape(args),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 're' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for re_mod {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for re_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl crate::python::builtins::ModuleValue for re_mod {
    fn module_value(self) -> Arg {
        self.into_arg()
    }
}

/// The pattern argument: a `str` / `bytes` pattern or a compiled `Pattern`.
fn pattern_of(p: &Arg) -> Result<Pattern> {
    if let Some(c) = p.downcast_ref::<Pattern>() {
        return Ok(c.clone());
    }
    let text = match p {
        Arg::Str(s) => String::from(&**s),
        Arg::Bytes(_) | Arg::ByteArray(_) => {
            // Byte patterns are matched byte-wise; the pattern text is the
            // Latin-1 view of the bytes (escapes are ASCII).
            p.as_bytes_like()
                .unwrap_or_default()
                .iter()
                .map(|&b| b as char)
                .collect()
        }
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "first argument must be string or compiled pattern, not {}",
                other.type_name()
            )))
        }
    };
    Pattern::compile(&text, matches!(p, Arg::Bytes(_) | Arg::ByteArray(_)))
}

/// The subject text as bytes (a `str` or a bytes-like).
fn subject(t: &Arg) -> Result<(Vec<u8>, bool)> {
    match t {
        Arg::Str(s) => Ok((s.as_bytes().to_vec(), false)),
        Arg::Bytes(_) | Arg::ByteArray(_) => Ok((t.as_bytes_like().unwrap_or_default(), true)),
        other => Err(LailaError::type_error(alloc::format!(
            "expected string or bytes-like object, got '{}'",
            other.type_name()
        ))),
    }
}

impl re_mod {
    /// `re.compile(pattern, flags=0)`
    pub fn compile(&self, args: impl Into<Args>) -> Result<Pattern> {
        let a: Args = args.into();
        let p = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("pattern"));
        pattern_of(&p)
    }
    fn two(a: &Args) -> Result<(Pattern, Arg)> {
        if a.positional.len() < 2 {
            return Err(LailaError::type_error(
                "missing required positional arguments: 'pattern' and 'string'",
            ));
        }
        Ok((pattern_of(&a.positional[0])?, a.positional[1].clone()))
    }
    /// `re.match(pattern, string)`
    pub fn r#match(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let (p, s) = Self::two(&a)?;
        p.r#match(s)
    }
    /// `re.search(pattern, string)`
    pub fn search(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let (p, s) = Self::two(&a)?;
        p.search(s)
    }
    /// `re.fullmatch(pattern, string)`
    pub fn fullmatch(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let (p, s) = Self::two(&a)?;
        p.fullmatch(s)
    }
    /// `re.findall(pattern, string)`
    pub fn findall(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let (p, s) = Self::two(&a)?;
        p.findall(s)
    }
    /// `re.sub(pattern, repl, string)` (a literal replacement).
    pub fn sub(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if a.positional.len() < 3 {
            return Err(LailaError::type_error(
                "missing required positional arguments: 'pattern', 'repl' and 'string'",
            ));
        }
        let p = pattern_of(&a.positional[0])?;
        p.sub(Args {
            positional: alloc::vec![a.positional[1].clone(), a.positional[2].clone()],
            keywords: Vec::new(),
        })
    }
    /// `re.escape(s)`
    pub fn escape(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let s = a
            .positional
            .first()
            .map(|s| s.__str__())
            .unwrap_or_default();
        let mut out = String::new();
        for c in s.chars() {
            if c.is_ascii_alphanumeric() || c == '_' {
                out.push(c);
            } else {
                out.push('\\');
                out.push(c);
            }
        }
        Ok(Arg::Str(out.into()))
    }
}

/// `re.Pattern`
#[derive(Clone, Debug)]
pub struct Pattern {
    inner: Arc<Regex>,
    bytes: bool,
}

impl Pattern {
    fn compile(text: &str, bytes: bool) -> Result<Pattern> {
        Regex::new(text)
            .map(|r| Pattern {
                inner: Arc::new(r),
                bytes,
            })
            .map_err(|e| {
                LailaError::new(
                    crate::python::errors::ExcKind::ValueError,
                    alloc::format!("re.error: {e}"),
                )
            })
    }
    /// `pattern.pattern`
    pub fn pattern(&self) -> String {
        self.inner.pattern.clone()
    }
    /// `repr(pattern.pattern)`
    pub fn pattern_repr(&self) -> String {
        crate::python::pyrepr::str_repr(&self.inner.pattern)
    }
    fn wrap(&self, text: &Arg, m: Option<RawMatch>) -> Arg {
        match m {
            Some(m) => Match {
                raw: m,
                text: text.clone(),
                bytes: self.bytes,
            }
            .into_arg(),
            None => Arg::None,
        }
    }
    /// `pattern.match(string)`
    pub fn r#match(&self, string: impl IntoArg) -> Result<Arg> {
        let t = string.into_arg();
        let (b, _) = subject(&t)?;
        Ok(self.wrap(&t, self.inner.match_at_start(&b)))
    }
    /// `pattern.search(string)`
    pub fn search(&self, string: impl IntoArg) -> Result<Arg> {
        let t = string.into_arg();
        let (b, _) = subject(&t)?;
        Ok(self.wrap(&t, self.inner.search(&b)))
    }
    /// `pattern.fullmatch(string)`
    pub fn fullmatch(&self, string: impl IntoArg) -> Result<Arg> {
        let t = string.into_arg();
        let (b, _) = subject(&t)?;
        Ok(self.wrap(&t, self.inner.fullmatch(&b)))
    }
    /// `pattern.findall(string)`: the whole matches (no groups) or the
    /// first group / a tuple of groups, as Python.
    pub fn findall(&self, string: impl IntoArg) -> Result<Arg> {
        let t = string.into_arg();
        let (b, is_bytes) = subject(&t)?;
        let slice = |s: usize, e: usize| -> Arg {
            if is_bytes {
                Arg::Bytes(b[s..e].to_vec().into())
            } else {
                Arg::Str(String::from_utf8_lossy(&b[s..e]).into_owned().into())
            }
        };
        let out: Vec<Arg> = self
            .inner
            .find_all(&b)
            .into_iter()
            .map(|m| match m.groups.len() {
                1 => slice(m.start, m.end),
                2 => m.groups[1]
                    .map(|(s, e)| slice(s, e))
                    .unwrap_or_else(|| slice(0, 0)),
                _ => Arg::Tuple(
                    m.groups[1..]
                        .iter()
                        .map(|g| g.map(|(s, e)| slice(s, e)).unwrap_or_else(|| slice(0, 0)))
                        .collect::<Vec<_>>()
                        .into(),
                ),
            })
            .collect();
        Ok(out.into_arg())
    }
    /// `pattern.sub(repl, string)` with a literal `repl`.
    pub fn sub(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if a.positional.len() < 2 {
            return Err(LailaError::type_error(
                "missing required positional arguments: 'repl' and 'string'",
            ));
        }
        let repl = a.positional[0].clone();
        let t = a.positional[1].clone();
        let (b, is_bytes) = subject(&t)?;
        let (r, _) = subject(&repl)?;
        let mut out: Vec<u8> = Vec::new();
        let mut last = 0;
        for m in self.inner.find_all(&b) {
            out.extend_from_slice(&b[last..m.start]);
            out.extend_from_slice(&r);
            last = m.end;
        }
        out.extend_from_slice(&b[last..]);
        Ok(if is_bytes {
            Arg::Bytes(out.into())
        } else {
            Arg::Str(String::from_utf8_lossy(&out).into_owned().into())
        })
    }
}

impl PyObject for Pattern {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "re.Pattern"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "re.compile({})",
            crate::python::pyrepr::str_repr(&self.inner.pattern)
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Pattern>()
            .is_some_and(|o| o.inner.pattern == self.inner.pattern && o.bytes == self.bytes)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.inner.pattern.as_str().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "pattern" => Ok(Arg::Str(self.inner.pattern.as_str().into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'re.Pattern' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "match" => self.r#match(first),
            "search" => self.search(first),
            "fullmatch" => self.fullmatch(first),
            "findall" => self.findall(first),
            "sub" => self.sub(args),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'re.Pattern' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Pattern {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl IntoArg for &Pattern {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `re.Match`
#[derive(Clone, Debug)]
pub struct Match {
    raw: RawMatch,
    text: Arg,
    bytes: bool,
}

impl Match {
    fn slice(&self, span: Option<(usize, usize)>) -> Arg {
        let Some((s, e)) = span else {
            return Arg::None;
        };
        let b = self
            .text
            .as_bytes_like()
            .unwrap_or_else(|| self.text.__str__().into_bytes());
        if self.bytes {
            Arg::Bytes(b[s..e].to_vec().into())
        } else {
            Arg::Str(String::from_utf8_lossy(&b[s..e]).into_owned().into())
        }
    }
    fn group_index(&self, key: &Arg) -> Result<usize> {
        match key {
            Arg::Int(i) if *i >= 0 && (*i as usize) < self.raw.groups.len() => Ok(*i as usize),
            Arg::Str(s) => self
                .raw
                .names_with_index()
                .into_iter()
                .find(|(n, _)| n == &**s)
                .map(|(_, i)| i)
                .ok_or_else(|| {
                    LailaError::new(crate::python::errors::ExcKind::IndexError, "no such group")
                }),
            _ => Err(LailaError::new(
                crate::python::errors::ExcKind::IndexError,
                "no such group",
            )),
        }
    }
    /// `m.group(i=0)` / `m.group("name")`
    pub fn group(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if a.positional.is_empty() {
            return Ok(self.slice(self.raw.groups[0]));
        }
        if a.positional.len() == 1 {
            let i = self.group_index(&a.positional[0])?;
            return Ok(self.slice(self.raw.groups[i]));
        }
        let mut out = Vec::new();
        for k in &a.positional {
            let i = self.group_index(k)?;
            out.push(self.slice(self.raw.groups[i]));
        }
        Ok(Arg::Tuple(out.into()))
    }
    /// `m.groups()`
    pub fn groups(&self) -> Arg {
        Arg::Tuple(
            self.raw.groups[1..]
                .iter()
                .map(|g| self.slice(*g))
                .collect::<Vec<_>>()
                .into(),
        )
    }
    /// `m.groupdict()`
    pub fn groupdict(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (n, i) in self.raw.names_with_index() {
            d.insert(n, self.slice(self.raw.groups[i]));
        }
        d.finish()
    }
    /// `m.start()` / `m.end()` / `m.span()`
    pub fn start(&self) -> i64 {
        self.raw.start as i64
    }
    pub fn end(&self) -> i64 {
        self.raw.end as i64
    }
    pub fn span(&self) -> Arg {
        Arg::Tuple(alloc::vec![Arg::Int(self.start()), Arg::Int(self.end())].into())
    }
}

impl PyObject for Match {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "re.Match"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<re.Match object; span=({}, {}), match={}>",
            self.raw.start,
            self.raw.end,
            self.slice(self.raw.groups[0]).__repr__()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Match>()
            .is_some_and(|o| o.raw == self.raw && o.text == self.text)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self.raw.start as i64) << 32 | self.raw.end as i64)
    }
    fn __bool__(&self) -> bool {
        true
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "string" => Ok(self.text.clone()),
            "pos" => Ok(Arg::Int(0)),
            "lastindex" => Ok(self
                .raw
                .groups
                .iter()
                .enumerate()
                .skip(1)
                .filter(|(_, g)| g.is_some())
                .map(|(i, _)| Arg::Int(i as i64))
                .next_back()
                .unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'re.Match' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "group" => self.group(args),
            "groups" => Ok(self.groups()),
            "groupdict" => Ok(self.groupdict()),
            "start" => Ok(Arg::Int(self.start())),
            "end" => Ok(Arg::Int(self.end())),
            "span" => Ok(self.span()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'re.Match' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Match {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
