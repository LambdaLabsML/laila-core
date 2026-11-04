//! Runtime argument loading from multiple file formats and the terminal.
//!
//! Defines [`ArgReader`], the loader behind `laila.args`. The reader accepts
//! arguments from any of the following formats and merges them into a
//! single attribute-accessible mapping (defaulting to `laila.args`):
//!
//! - `.json` -- JSON object at the top level (`from_json`).
//! - `.toml` -- TOML table at the top level (`from_toml`).
//! - `.env` -- `KEY=VALUE` lines, `#` comments allowed (`from_env`). A
//!   file literally named `.env` works too.
//! - `.xml` -- one level of nesting allowed; siblings become nested dicts
//!   (`from_xml`).
//! - `terminal` -- `key=value`, `--key=value`, `--key value` and bare
//!   `--flag` (-> `True`) tokens parsed from `sys.argv` (`from_terminal`).
//! - `load(source)` -- auto-detects format from the file suffix.
//!
//! Values are coerced from strings to the most specific Python scalar they
//! fit (bool, None, int, float, JSON-shaped collection, quoted string,
//! fall-back raw string) by [`ArgReader::_coerce_scalar`]. Nested dicts in
//! JSON / TOML files are applied twice: flattened *one level* with
//! underscore-joined keys (`s3_bucket_name`) for the flat, shell-style
//! surface, **and** deep-merged into the nested tree (`laila.args.s3.bucket_name`)
//! so a config file can drive the CLI-capable defaults that live under
//! `laila.args.policy...` / `laila.args.logger`. Merging never replaces an
//! existing nested mapping wholesale: keys set earlier survive unless the
//! new file sets the same leaf.
//!
//! File access needs the `std` feature; the `*_str` variants parse text and
//! work everywhere.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::_compat::dotmap::DotMap;
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyDictBuilder, Result};

/// `True` for tokens such as `-3` / `-0.5` (values, not flags) -- `float(token)` succeeds.
pub fn _looks_numeric(token: &str) -> bool {
    py_float(token).is_some()
}

/// Python's `float(s)` acceptance (whitespace, sign, `inf`/`nan`, digit underscores).
fn py_float(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    let cleaned = strip_digit_underscores(t)?;
    let lower = cleaned.to_ascii_lowercase();
    let body = lower.trim_start_matches(['+', '-']);
    if matches!(body, "inf" | "infinity" | "nan") {
        return cleaned.parse::<f64>().ok().or_else(|| {
            let neg = lower.starts_with('-');
            Some(match (body, neg) {
                ("nan", _) => f64::NAN,
                (_, true) => f64::NEG_INFINITY,
                _ => f64::INFINITY,
            })
        });
    }
    if !cleaned
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '+' | '-' | '.' | 'e' | 'E'))
    {
        return None;
    }
    cleaned.parse::<f64>().ok()
}

/// Python's `int(s)` acceptance (whitespace, sign, digit underscores, any size).
fn py_int(s: &str) -> Option<Arg> {
    let t = s.trim();
    let cleaned = strip_digit_underscores(t)?;
    let digits = cleaned.strip_prefix(['+', '-']).unwrap_or(&cleaned);
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    match cleaned.parse::<i64>() {
        Ok(i) => Some(Arg::Int(i)),
        Err(_) => cleaned
            .parse::<num_bigint::BigInt>()
            .ok()
            .map(Arg::from_bigint),
    }
}

/// `1_000` -> `1000`; a misplaced underscore (`_1`, `1__0`, `1_`) is a parse error.
fn strip_digit_underscores(s: &str) -> Option<String> {
    if !s.contains('_') {
        return Some(s.to_string());
    }
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        if *c == '_' {
            let prev_ok = i > 0 && chars[i - 1].is_ascii_alphanumeric();
            let next_ok = i + 1 < chars.len() && chars[i + 1].is_ascii_alphanumeric();
            if !prev_ok || !next_ok {
                return None;
            }
            continue;
        }
        out.push(*c);
    }
    Some(out)
}

#[cfg(not(feature = "std"))]
fn not_std() -> LailaError {
    LailaError::new(
        ExcKind::OSError,
        "file access needs the `std` feature (use the *_str variants)",
    )
}

#[cfg(feature = "std")]
fn read_file(path: &str) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| {
        let kind = match e.kind() {
            std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
            std::io::ErrorKind::PermissionDenied => ExcKind::PermissionError,
            _ => ExcKind::OSError,
        };
        let msg = match e.kind() {
            std::io::ErrorKind::NotFound => {
                alloc::format!("[Errno 2] No such file or directory: '{path}'")
            }
            std::io::ErrorKind::PermissionDenied => {
                alloc::format!("[Errno 13] Permission denied: '{path}'")
            }
            _ => alloc::format!("{e}: '{path}'"),
        };
        LailaError::new(kind, msg)
    })
}

/// Loader of runtime args into an attribute-accessible target mapping.
///
/// All `load` / `from_*` methods mutate the target in place. The target
/// defaults to `laila.args` when not specified; a custom target lets test
/// suites or library callers run with a private `DotMap`-like object.
///
/// The reader is intentionally tolerant: malformed lines in `.env`-style
/// files are skipped, JSON-coerced collections that fail to parse fall back
/// to the original string, etc.
#[derive(Clone, Default)]
pub struct ArgReader {
    _target: Option<Arg>,
}

/// The target of `laila.arg_reader` (`None`: the live `laila.args`).
static PROCESS_WIDE_TARGET: crate::python::sync::RwLock<Option<Arg>> =
    crate::python::sync::RwLock::new(None);

impl ArgReader {
    /// `ArgReader(target=None)`
    #[pysig("ArgReader(target=None)")]
    pub fn new(args: impl Into<Args>) -> Result<ArgReader> {
        Ok(ArgReader {
            _target: if target.is_none() { None } else { Some(target) },
        })
    }

    /// A reader bound to `target`.
    pub fn with_target(target: Arg) -> ArgReader {
        ArgReader {
            _target: Some(target),
        }
    }

    /// `laila.arg_reader` -- the process-wide reader `laila.read_args`
    /// loads through (`ArgReader(target=args)` in Python; here the target
    /// is the live `laila.args` until `laila.arg_reader.target = x`).
    pub fn process_wide() -> ArgReader {
        ArgReader {
            _target: PROCESS_WIDE_TARGET.read().clone(),
        }
    }

    /// `reader.target`
    pub fn target(&self) -> Arg {
        self._target_map()
    }

    /// `laila.arg_reader.target = laila.args` (a test's `setUp` after
    /// swapping in a fresh `_LailaArgs`): rebind the process-wide reader.
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "target" => {
                let value = value.into_arg();
                *PROCESS_WIDE_TARGET.write() = if value.is_none() { None } else { Some(value) };
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ArgReader' object has no attribute '{other}'"
            ))),
        }
    }

    /// Return the target mapping, falling back to `laila.args`.
    pub fn _target_map(&self) -> Arg {
        match &self._target {
            Some(t) => t.clone(),
            None => crate::basics::definitions::cli_capable::args(),
        }
    }

    /// Coerce a string value to its most specific Python scalar type.
    pub fn _coerce_scalar(value: impl IntoArg) -> Arg {
        let value = value.into_arg();
        let s = match &value {
            Arg::Str(s) => s.clone(),
            _ => return value,
        };
        let stripped = s.trim();
        let lowered = stripped.to_lowercase();
        if lowered == "true" || lowered == "false" {
            return Arg::Bool(lowered == "true");
        }
        if lowered == "none" || lowered == "null" {
            return Arg::None;
        }
        if let Some(i) = py_int(stripped) {
            return i;
        }
        if let Some(f) = py_float(stripped) {
            return Arg::Float(f);
        }
        if (stripped.starts_with('{') && stripped.ends_with('}'))
            || (stripped.starts_with('[') && stripped.ends_with(']'))
        {
            return match crate::_compat::pyjson::loads(stripped) {
                Ok(v) => v,
                Err(_) => value,
            };
        }
        if ((stripped.starts_with('"') && stripped.ends_with('"'))
            || (stripped.starts_with('\'') && stripped.ends_with('\'')))
            && stripped.chars().count() >= 2
        {
            let chars: Vec<char> = stripped.chars().collect();
            return Arg::Str(chars[1..chars.len() - 1].iter().collect::<String>().into());
        }
        value
    }

    /// Flatten nested dicts one level, joining keys with `_`.
    pub fn _flatten_one_level(payload: impl IntoArg) -> Result<Arg> {
        let payload = &payload.into_arg();
        let out = crate::python::builtins::dict(Args::new())?;
        for key in payload.__iter__()? {
            let value = payload.__getitem__(key.clone())?;
            if let Arg::Dict(_) = &value {
                for sub_key in value.__iter__()? {
                    let sub_value = value.__getitem__(sub_key.clone())?;
                    out.__setitem__(
                        alloc::format!("{}_{}", key.__str__(), sub_key.__str__()),
                        Self::_coerce_scalar(sub_value),
                    )?;
                }
            } else {
                out.__setitem__(key, Self::_coerce_scalar(value))?;
            }
        }
        Ok(out)
    }

    /// Coerce scalars recursively inside nested dicts / lists.
    pub fn _coerce_tree(value: &Arg) -> Result<Arg> {
        match value {
            Arg::Dict(_) => {
                let out = crate::python::builtins::dict(Args::new())?;
                for k in value.__iter__()? {
                    out.__setitem__(k.clone(), Self::_coerce_tree(&value.__getitem__(k)?)?)?;
                }
                Ok(out)
            }
            Arg::List(_) => Ok(value
                .__iter__()?
                .iter()
                .map(Self::_coerce_tree)
                .collect::<Result<Vec<_>>>()?
                .into_arg()),
            other => Ok(Self::_coerce_scalar(other.clone())),
        }
    }

    fn _lookup(container: &Arg, k: &str) -> Arg {
        // Resolve ``get`` through the *type* before trusting a callable.
        match container {
            Arg::Dict(_) => container.get(Args::from(k)).unwrap_or(Arg::None),
            Arg::Object(_) => match container.__getattr__("get") {
                Ok(getter) if matches!(getter, Arg::Callable(_)) => {
                    getter.__call__(Args::from(k)).unwrap_or(Arg::None)
                }
                _ => container.__getattr__(k).unwrap_or(Arg::None),
            },
            _ => Arg::None,
        }
    }

    fn _store(container: &Arg, k: &str, v: Arg) -> Result<()> {
        if DotMap::downcast(container).is_some() {
            let v = match &v {
                Arg::Dict(_) => DotMap::new(Args::from(v.clone()))?.into_arg(),
                _ => v,
            };
            return container.__setattr__(k, v);
        }
        if let Arg::Dict(_) = container {
            return container.__setitem__(k, v);
        }
        container.__setattr__(k, v)
    }

    /// Merge the nested dict `value` into `target[key]` leaf by leaf: an
    /// existing mapping under `key` is updated in place; missing branches are
    /// created; non-mapping values at either side are replaced by the leaf.
    pub fn _deep_merge(target: &Arg, key: &str, value: &Arg) -> Result<()> {
        let existing = Self::_lookup(target, key);
        let mapping_like = matches!(existing, Arg::Dict(_))
            || DotMap::downcast(&existing).is_some()
            || (!existing.is_none()
                && matches!(existing, Arg::Object(_))
                && existing.__getattr__("__setitem__").is_ok());
        if !mapping_like {
            // ``DotMap(dict)`` converts nested dicts recursively so the
            // branch stays attribute-accessible all the way down.
            return Self::_store(target, key, value.clone());
        }
        for sub_key in value.__iter__()? {
            let sub_value = value.__getitem__(sub_key.clone())?;
            if let Arg::Dict(_) = &sub_value {
                Self::_deep_merge(&existing, &sub_key.__str__(), &sub_value)?;
            } else {
                dyn_setitem(&existing, sub_key, sub_value)?;
            }
        }
        Ok(())
    }

    /// Apply `payload` to the target: flat `a_b` keys plus the nested tree.
    /// The `environment` key is applied last so an environment load sees
    /// every other key already in place.
    pub fn _apply(&self, payload: impl IntoArg) -> Result<()> {
        let payload = &payload.into_arg();
        let flat = Self::_flatten_one_level(payload)?;
        let target = self._target_map();
        for key in flat.__iter__()? {
            target.__setattr__(&key.__str__(), flat.__getitem__(key.clone())?)?;
        }
        let mut nested: Vec<(String, Arg)> = Vec::new();
        for k in payload.__iter__()? {
            let v = payload.__getitem__(k.clone())?;
            if let Arg::Dict(_) = &v {
                nested.push((k.__str__(), v));
            }
        }
        // stable: ``environment`` sorts last
        nested.sort_by_key(|(k, _)| k == "environment");
        for (key, value) in nested {
            Self::_deep_merge(&target, &key, &Self::_coerce_tree(&value)?)?;
        }
        Ok(())
    }

    /// Remove all keys from the target mapping.
    #[pysig("clear(self)")]
    pub fn clear(&self, _args: impl Into<Args>) -> Result<()> {
        let target = self._target_map();
        let keys: Vec<Arg> = match target.__getattr__("keys") {
            Ok(k) => k.__call__(Args::new())?.__iter__()?,
            Err(_) => match &target {
                Arg::Dict(_) => target.__iter__()?,
                _ => return Ok(()),
            },
        };
        for key in keys {
            let k = key.__str__();
            if let Some(dm) = DotMap::downcast(&target) {
                let _ = dm.__delattr__(&k);
            } else if let Some(la) = target.downcast_ref::<crate::_LailaArgs>() {
                let _ = la.dotmap().__delattr__(&k);
            } else if let Arg::Dict(_) = &target {
                let _ = target.__delitem__(key);
            } else {
                // Any other mapping-like target (`AtomicDotMap`, ..): Python's
                // `delattr(target, key)`, swallowing the failure like the original.
                let _ = target.__delattr__(&k);
            }
        }
        Ok(())
    }

    /// Load arguments from a JSON file.
    pub fn from_json(&self, path: impl IntoArg) -> Result<()> {
        #[cfg(feature = "std")]
        {
            let text = read_file(&path.into_arg().__str__())?;
            self.from_json_str(&text)
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = path;
            Err(not_std())
        }
    }

    /// `from_json` on the file's text.
    pub fn from_json_str(&self, text: &str) -> Result<()> {
        let data = crate::_compat::pyjson::loads(text)?;
        if !matches!(data, Arg::Dict(_)) {
            return Err(LailaError::value_error(
                "JSON args file must be a key/value object.",
            ));
        }
        self._apply(&data)
    }

    /// Load arguments from a TOML file.
    pub fn from_toml(&self, path: impl IntoArg) -> Result<()> {
        #[cfg(feature = "std")]
        {
            let text = read_file(&path.into_arg().__str__())?;
            self.from_toml_str(&text)
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = path;
            Err(not_std())
        }
    }

    /// `from_toml` on the file's text.
    pub fn from_toml_str(&self, text: &str) -> Result<()> {
        let data = toml::parse(text)?;
        if !matches!(data, Arg::Dict(_)) {
            return Err(LailaError::value_error(
                "TOML args file must be a key/value table.",
            ));
        }
        self._apply(&data)
    }

    /// Load arguments from a `.env`-style file.
    pub fn from_env(&self, path: impl IntoArg) -> Result<()> {
        #[cfg(feature = "std")]
        {
            let text = read_file(&path.into_arg().__str__())?;
            self.from_env_str(&text)
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = path;
            Err(not_std())
        }
    }

    /// `from_env` on the file's text.
    pub fn from_env_str(&self, text: &str) -> Result<()> {
        let parsed = crate::python::builtins::dict(Args::new())?;
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            parsed.__setitem__(k.trim(), Self::_coerce_scalar(v.trim()))?;
        }
        self._apply(&parsed)
    }

    /// Load arguments from an XML file.
    pub fn from_xml(&self, path: impl IntoArg) -> Result<()> {
        #[cfg(feature = "std")]
        {
            let text = read_file(&path.into_arg().__str__())?;
            self.from_xml_str(&text)
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = path;
            Err(not_std())
        }
    }

    /// `from_xml` on the file's text: one level of nesting; siblings become
    /// nested dicts; element text (or `""`) is the value.
    pub fn from_xml_str(&self, text: &str) -> Result<()> {
        let root = xml::parse(text)?;
        let parsed = crate::python::builtins::dict(Args::new())?;
        for child in &root.children {
            if !child.children.is_empty() {
                let sub = crate::python::builtins::dict(Args::new())?;
                for gc in &child.children {
                    sub.__setitem__(gc.tag.as_str(), gc.text.clone().unwrap_or_default())?;
                }
                parsed.__setitem__(child.tag.as_str(), sub)?;
            } else {
                parsed.__setitem__(child.tag.as_str(), child.text.clone().unwrap_or_default())?;
            }
        }
        self._apply(&parsed)
    }

    /// Load arguments from command-line tokens (`None` -> `sys.argv[1:]`).
    ///
    /// Accepted forms (dashes are stripped from the key): `key=value` /
    /// `--key=value` / `-key=value`; `--key value` (the next token, unless it
    /// is itself a flag); `--flag` (no value) -> `True`. Bare tokens without
    /// `=` or a leading dash are ignored.
    pub fn from_terminal(&self, args: impl IntoArg) -> Result<()> {
        let args = args.into_arg();
        let tokens: Vec<String> = if args.is_none() {
            sys_argv()
        } else {
            args.__iter__()?.iter().map(|t| t.__str__()).collect()
        };
        let parsed = crate::python::builtins::dict(Args::new())?;
        let mut i = 0;
        while i < tokens.len() {
            let token = &tokens[i];
            i += 1;
            let dashed = token.starts_with('-') && !_looks_numeric(token);
            if let Some((k, v)) = token.split_once('=') {
                let key = if dashed {
                    k.trim().trim_start_matches('-')
                } else {
                    k.trim()
                };
                if key.is_empty() {
                    continue;
                }
                parsed.__setitem__(key, Self::_coerce_scalar(v.trim()))?;
                continue;
            }
            if !dashed {
                continue;
            }
            let key = token.trim().trim_start_matches('-');
            if key.is_empty() {
                continue;
            }
            if i < tokens.len() {
                let nxt = &tokens[i];
                if !nxt.contains('=') && !(nxt.starts_with('-') && !_looks_numeric(nxt)) {
                    parsed.__setitem__(key, Self::_coerce_scalar(nxt.trim()))?;
                    i += 1;
                    continue;
                }
            }
            parsed.__setitem__(key, Arg::Bool(true))?;
        }
        self._apply(&parsed)
    }

    /// Auto-detect format and load arguments: a file path (suffix selects
    /// the format) or the literal `"terminal"`; `ValueError` otherwise.
    #[pysig("load(source, *, terminal_args=None)")]
    pub fn load(&self, args: impl Into<Args>) -> Result<()> {
        let source_text = source.__str__();
        let name = source_text
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(&source_text)
            .to_string();
        let mut suffix = match name.rfind('.') {
            Some(0) | None => String::new(),
            Some(i) => name[i..].to_lowercase(),
        };
        if suffix.is_empty() && name.to_lowercase() == ".env" {
            suffix = ".env".to_string(); // ``Path(".env").suffix`` is ``""``
        }
        match suffix.as_str() {
            ".json" => return self.from_json(source),
            ".toml" => return self.from_toml(source),
            ".env" => return self.from_env(source),
            ".xml" => return self.from_xml(source),
            _ => {}
        }
        if source_text.to_lowercase() == "terminal" {
            return self.from_terminal(terminal_args);
        }
        Err(LailaError::value_error(alloc::format!(
            "Unsupported args source: {source_text}"
        )))
    }
}

impl core::fmt::Debug for ArgReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ArgReader(..)")
    }
}

/// The reader as a Python value (`self.reader = ArgReader(target=laila.args)`).
impl crate::python::PyObject for ArgReader {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "ArgReader"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<laila.utils.args.ArgReader object at {:#x}>",
            self as *const _ as usize
        )
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        core::ptr::eq(
            other
                .as_any()
                .downcast_ref::<ArgReader>()
                .map_or(core::ptr::null(), |o| o as *const _),
            self as *const _,
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "load" => self.load(args)?,
            "clear" => self.clear(args)?,
            "from_json" => self.from_json(first())?,
            "from_toml" => self.from_toml(first())?,
            "from_env" => self.from_env(first())?,
            "from_xml" => self.from_xml(first())?,
            "from_terminal" => self.from_terminal(first())?,
            "_apply" => self._apply(first())?,
            "_target_map" => return Ok(self._target_map()),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'ArgReader' object has no attribute '{name}'"
                )))
            }
        }
        Ok(Arg::None)
    }
}
impl IntoArg for ArgReader {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

/// `container[k] = v` through the dynamic protocol (`DotMap`, `_LailaArgs`, `dict`).
fn dyn_setitem(container: &Arg, k: Arg, v: Arg) -> Result<()> {
    if let Some(dm) = DotMap::downcast(container) {
        return dm.__setitem__(k, v);
    }
    if let Some(la) = container.downcast_ref::<crate::_LailaArgs>() {
        return la.__setitem__(k, v);
    }
    container.__setitem__(k, v)
}

/// `sys.argv[1:]`
fn sys_argv() -> Vec<String> {
    #[cfg(feature = "std")]
    {
        crate::python::stdlib::sys_argv()
            .into_iter()
            .skip(1)
            .collect()
    }
    #[cfg(not(feature = "std"))]
    {
        Vec::new()
    }
}

/// Builds the `{key: value}` payload dict for callers assembling one by hand.
pub fn payload(pairs: Vec<(&str, Arg)>) -> Arg {
    let mut d = PyDictBuilder::new();
    for (k, v) in pairs {
        d.insert(k, v);
    }
    d.finish()
}

// ---------------------------------------------------------------------------
// Minimal TOML reader (the subset `tomllib.load` needs for config files)

mod toml {
    use super::*;

    struct P<'a> {
        s: &'a [u8],
        i: usize,
    }

    /// `tomllib.TOMLDecodeError` (a `ValueError`).
    fn err(msg: &str) -> LailaError {
        LailaError::new(
            ExcKind::TOMLDecodeError,
            alloc::format!("Invalid TOML: {msg}"),
        )
    }

    impl P<'_> {
        fn peek(&self) -> Option<u8> {
            self.s.get(self.i).copied()
        }
        fn skip_ws(&mut self) {
            while matches!(self.peek(), Some(b' ' | b'\t')) {
                self.i += 1;
            }
        }
        fn skip_ws_nl(&mut self) {
            loop {
                self.skip_ws();
                match self.peek() {
                    Some(b'\n' | b'\r') => self.i += 1,
                    Some(b'#') => self.skip_comment(),
                    _ => break,
                }
            }
        }
        fn skip_comment(&mut self) {
            while let Some(c) = self.peek() {
                if c == b'\n' {
                    break;
                }
                self.i += 1;
            }
        }
        fn expect_eol(&mut self) -> Result<()> {
            self.skip_ws();
            match self.peek() {
                None | Some(b'\n' | b'\r') => Ok(()),
                Some(b'#') => {
                    self.skip_comment();
                    Ok(())
                }
                Some(c) => Err(err(&alloc::format!(
                    "expected end of line, found {:?}",
                    c as char
                ))),
            }
        }
        fn key_part(&mut self) -> Result<String> {
            self.skip_ws();
            match self.peek() {
                Some(b'"') => self.basic_string(),
                Some(b'\'') => self.literal_string(),
                _ => {
                    let start = self.i;
                    while let Some(c) = self.peek() {
                        if c.is_ascii_alphanumeric() || c == b'_' || c == b'-' {
                            self.i += 1;
                        } else {
                            break;
                        }
                    }
                    if start == self.i {
                        return Err(err("expected key"));
                    }
                    Ok(String::from_utf8_lossy(&self.s[start..self.i]).into_owned())
                }
            }
        }
        fn dotted_key(&mut self) -> Result<Vec<String>> {
            let mut parts = alloc::vec![self.key_part()?];
            loop {
                self.skip_ws();
                if self.peek() == Some(b'.') {
                    self.i += 1;
                    parts.push(self.key_part()?);
                } else {
                    break;
                }
            }
            Ok(parts)
        }
        fn basic_string(&mut self) -> Result<String> {
            // "..." or """..."""
            let triple = self.s[self.i..].starts_with(b"\"\"\"");
            self.i += if triple { 3 } else { 1 };
            if triple && matches!(self.peek(), Some(b'\n')) {
                self.i += 1;
            }
            let mut out = String::new();
            loop {
                let c = self.peek().ok_or_else(|| err("unterminated string"))?;
                if triple && self.s[self.i..].starts_with(b"\"\"\"") {
                    self.i += 3;
                    break;
                }
                if !triple && c == b'"' {
                    self.i += 1;
                    break;
                }
                if c == b'\\' {
                    self.i += 1;
                    let e = self.peek().ok_or_else(|| err("bad escape"))?;
                    self.i += 1;
                    match e {
                        b'n' => out.push('\n'),
                        b't' => out.push('\t'),
                        b'r' => out.push('\r'),
                        b'\\' => out.push('\\'),
                        b'"' => out.push('"'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'u' | b'U' => {
                            let n = if e == b'u' { 4 } else { 8 };
                            let hex = core::str::from_utf8(&self.s[self.i..self.i + n])
                                .map_err(|_| err("bad unicode escape"))?;
                            self.i += n;
                            let cp = u32::from_str_radix(hex, 16)
                                .map_err(|_| err("bad unicode escape"))?;
                            out.push(char::from_u32(cp).ok_or_else(|| err("bad unicode escape"))?);
                        }
                        b'\n' if triple => {
                            self.skip_ws_nl();
                        }
                        _ => return Err(err("bad escape")),
                    }
                    continue;
                }
                let ch_len = utf8_len(c);
                out.push_str(
                    core::str::from_utf8(&self.s[self.i..self.i + ch_len])
                        .map_err(|_| err("invalid utf-8"))?,
                );
                self.i += ch_len;
            }
            Ok(out)
        }
        fn literal_string(&mut self) -> Result<String> {
            let triple = self.s[self.i..].starts_with(b"'''");
            self.i += if triple { 3 } else { 1 };
            if triple && matches!(self.peek(), Some(b'\n')) {
                self.i += 1;
            }
            let start = self.i;
            loop {
                let c = self.peek().ok_or_else(|| err("unterminated string"))?;
                if triple && self.s[self.i..].starts_with(b"'''") {
                    let out = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
                    self.i += 3;
                    return Ok(out);
                }
                if !triple && c == b'\'' {
                    let out = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
                    self.i += 1;
                    return Ok(out);
                }
                self.i += 1;
            }
        }
        fn value(&mut self) -> Result<Arg> {
            self.skip_ws();
            match self.peek().ok_or_else(|| err("expected value"))? {
                b'"' => Ok(Arg::Str(self.basic_string()?.into())),
                b'\'' => Ok(Arg::Str(self.literal_string()?.into())),
                b'[' => {
                    self.i += 1;
                    let mut items = Vec::new();
                    loop {
                        self.skip_ws_nl();
                        if self.peek() == Some(b']') {
                            self.i += 1;
                            break;
                        }
                        items.push(self.value()?);
                        self.skip_ws_nl();
                        match self.peek() {
                            Some(b',') => self.i += 1,
                            Some(b']') => {}
                            _ => return Err(err("expected ',' or ']' in array")),
                        }
                    }
                    Ok(items.into_arg())
                }
                b'{' => {
                    self.i += 1;
                    let table = crate::python::builtins::dict(Args::new())?;
                    self.skip_ws();
                    if self.peek() == Some(b'}') {
                        self.i += 1;
                        return Ok(table);
                    }
                    loop {
                        let key = self.dotted_key()?;
                        self.skip_ws();
                        if self.peek() != Some(b'=') {
                            return Err(err("expected '=' in inline table"));
                        }
                        self.i += 1;
                        let v = self.value()?;
                        insert_path(&table, &key, v)?;
                        self.skip_ws();
                        match self.peek() {
                            Some(b',') => self.i += 1,
                            Some(b'}') => {
                                self.i += 1;
                                break;
                            }
                            _ => return Err(err("expected ',' or '}' in inline table")),
                        }
                    }
                    Ok(table)
                }
                _ => {
                    let start = self.i;
                    while let Some(c) = self.peek() {
                        if matches!(c, b',' | b']' | b'}' | b'#' | b'\n' | b'\r') {
                            break;
                        }
                        self.i += 1;
                    }
                    let raw = String::from_utf8_lossy(&self.s[start..self.i])
                        .trim()
                        .to_string();
                    scalar(&raw)
                }
            }
        }
    }

    fn utf8_len(first: u8) -> usize {
        match first {
            0x00..=0x7f => 1,
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            _ => 4,
        }
    }

    fn scalar(raw: &str) -> Result<Arg> {
        match raw {
            "true" => return Ok(Arg::Bool(true)),
            "false" => return Ok(Arg::Bool(false)),
            "inf" | "+inf" => return Ok(Arg::Float(f64::INFINITY)),
            "-inf" => return Ok(Arg::Float(f64::NEG_INFINITY)),
            "nan" | "+nan" | "-nan" => return Ok(Arg::Float(f64::NAN)),
            _ => {}
        }
        let cleaned = raw.replace('_', "");
        if let Some(hex) = cleaned.strip_prefix("0x") {
            return i64::from_str_radix(hex, 16)
                .map(Arg::Int)
                .map_err(|_| err("bad hex integer"));
        }
        if let Some(oct) = cleaned.strip_prefix("0o") {
            return i64::from_str_radix(oct, 8)
                .map(Arg::Int)
                .map_err(|_| err("bad octal integer"));
        }
        if let Some(bin) = cleaned.strip_prefix("0b") {
            return i64::from_str_radix(bin, 2)
                .map(Arg::Int)
                .map_err(|_| err("bad binary integer"));
        }
        if let Ok(i) = cleaned.parse::<i64>() {
            return Ok(Arg::Int(i));
        }
        if cleaned
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '+' | '-' | '.' | 'e' | 'E'))
        {
            if let Ok(f) = cleaned.parse::<f64>() {
                return Ok(Arg::Float(f));
            }
        }
        // Dates / times keep their text (``datetime`` objects in tomllib).
        if raw.chars().all(|c| {
            c.is_ascii_digit() || matches!(c, '-' | ':' | 'T' | 'Z' | '.' | '+' | ' ' | 't' | 'z')
        }) && raw.chars().any(|c| c.is_ascii_digit())
        {
            return Ok(Arg::Str(raw.into()));
        }
        Err(err(&alloc::format!("invalid value {raw:?}")))
    }

    fn insert_path(table: &Arg, key: &[String], v: Arg) -> Result<()> {
        let mut cur = table.clone();
        for part in &key[..key.len() - 1] {
            let next = cur.get(Args::from(part.as_str()))?;
            let next = if matches!(next, Arg::Dict(_)) {
                next
            } else {
                let fresh = crate::python::builtins::dict(Args::new())?;
                cur.__setitem__(part.as_str(), fresh.clone())?;
                fresh
            };
            cur = next;
        }
        let last = key.last().map(String::as_str).unwrap_or("");
        if cur.__contains__(last)? {
            return Err(err(&alloc::format!("duplicate key {last:?}")));
        }
        cur.__setitem__(last, v)
    }

    fn table_at(root: &Arg, path: &[String], array_item: bool) -> Result<Arg> {
        let mut cur = root.clone();
        for (idx, part) in path.iter().enumerate() {
            let last = idx + 1 == path.len();
            let existing = cur.get(Args::from(part.as_str()))?;
            let next = match &existing {
                Arg::Dict(_) if !(last && array_item) => existing,
                Arg::List(_) => {
                    if last && array_item {
                        let fresh = crate::python::builtins::dict(Args::new())?;
                        existing.append(fresh.clone())?;
                        fresh
                    } else {
                        let items = existing.__iter__()?;
                        items
                            .last()
                            .cloned()
                            .ok_or_else(|| err("empty array of tables"))?
                    }
                }
                Arg::None => {
                    if last && array_item {
                        let fresh = crate::python::builtins::dict(Args::new())?;
                        let list = alloc::vec![fresh.clone()].into_arg();
                        cur.__setitem__(part.as_str(), list)?;
                        fresh
                    } else {
                        let fresh = crate::python::builtins::dict(Args::new())?;
                        cur.__setitem__(part.as_str(), fresh.clone())?;
                        fresh
                    }
                }
                _ => return Err(err(&alloc::format!("key {part:?} is not a table"))),
            };
            cur = next;
        }
        Ok(cur)
    }

    /// `tomllib.loads(text)` for the config subset.
    pub fn parse(text: &str) -> Result<Arg> {
        let root = crate::python::builtins::dict(Args::new())?;
        let mut current = root.clone();
        let mut p = P {
            s: text.as_bytes(),
            i: 0,
        };
        loop {
            p.skip_ws_nl();
            match p.peek() {
                None => break,
                Some(b'[') => {
                    let array = p.s[p.i..].starts_with(b"[[");
                    p.i += if array { 2 } else { 1 };
                    let path = p.dotted_key()?;
                    p.skip_ws();
                    let close: &[u8] = if array { b"]]" } else { b"]" };
                    if !p.s[p.i..].starts_with(close) {
                        return Err(err("expected ']' after table header"));
                    }
                    p.i += close.len();
                    p.expect_eol()?;
                    current = table_at(&root, &path, array)?;
                }
                Some(_) => {
                    let key = p.dotted_key()?;
                    p.skip_ws();
                    if p.peek() != Some(b'=') {
                        return Err(err("expected '=' after key"));
                    }
                    p.i += 1;
                    let v = p.value()?;
                    insert_path(&current, &key, v)?;
                    p.expect_eol()?;
                }
            }
        }
        Ok(root)
    }
}

// ---------------------------------------------------------------------------
// Minimal XML reader (elements, text; attributes / comments / declarations skipped)

mod xml {
    use super::*;

    pub struct Element {
        pub tag: String,
        pub text: Option<String>,
        pub children: Vec<Element>,
    }

    /// `xml.etree.ElementTree.ParseError` (a `SyntaxError`).
    fn err(msg: &str) -> LailaError {
        LailaError::new(
            ExcKind::ParseError,
            alloc::format!("XML parse error: {msg}"),
        )
    }

    fn unescape(s: &str) -> String {
        s.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&")
    }

    struct P<'a> {
        s: &'a str,
        i: usize,
    }

    impl P<'_> {
        fn rest(&self) -> &str {
            &self.s[self.i..]
        }
        fn skip_misc(&mut self) {
            loop {
                let r = self.rest();
                let trimmed = r.trim_start();
                self.i += r.len() - trimmed.len();
                let r = self.rest();
                if r.starts_with("<?") {
                    match r.find("?>") {
                        Some(n) => self.i += n + 2,
                        None => return,
                    }
                } else if r.starts_with("<!--") {
                    match r.find("-->") {
                        Some(n) => self.i += n + 3,
                        None => return,
                    }
                } else if r.starts_with("<!") {
                    match r.find('>') {
                        Some(n) => self.i += n + 1,
                        None => return,
                    }
                } else {
                    return;
                }
            }
        }
        fn element(&mut self) -> Result<Element> {
            self.skip_misc();
            let r = self.rest();
            if !r.starts_with('<') {
                return Err(err("expected '<'"));
            }
            let end = r.find('>').ok_or_else(|| err("unterminated tag"))?;
            let head = &r[1..end];
            let self_closing = head.ends_with('/');
            let head = head.trim_end_matches('/').trim();
            let tag = head.split_whitespace().next().unwrap_or("").to_string();
            if tag.is_empty() {
                return Err(err("empty tag"));
            }
            self.i += end + 1;
            let mut el = Element {
                tag: tag.clone(),
                text: None,
                children: Vec::new(),
            };
            if self_closing {
                return Ok(el);
            }
            let mut text = String::new();
            loop {
                let r = self.rest();
                let Some(lt) = r.find('<') else {
                    return Err(err(&alloc::format!("unclosed element '{tag}'")));
                };
                text.push_str(&r[..lt]);
                self.i += lt;
                let r = self.rest();
                if r.starts_with("</") {
                    let close_end = r.find('>').ok_or_else(|| err("unterminated end tag"))?;
                    let closing = r[2..close_end].trim();
                    if closing != tag {
                        return Err(err(&alloc::format!(
                            "mismatched tag: expected '</{tag}>', found '</{closing}>'"
                        )));
                    }
                    self.i += close_end + 1;
                    break;
                }
                if r.starts_with("<!--") {
                    let n = r.find("-->").ok_or_else(|| err("unterminated comment"))?;
                    self.i += n + 3;
                    continue;
                }
                if r.starts_with("<![CDATA[") {
                    let n = r.find("]]>").ok_or_else(|| err("unterminated CDATA"))?;
                    text.push_str(&r[9..n]);
                    self.i += n + 3;
                    continue;
                }
                let child = self.element()?;
                el.children.push(child);
            }
            // ElementTree: ``text`` is the content before the first child (None when empty).
            if el.children.is_empty() {
                el.text = if text.is_empty() {
                    None
                } else {
                    Some(unescape(&text))
                };
            } else {
                el.text = None;
            }
            Ok(el)
        }
    }

    /// `ET.parse(path).getroot()` for the text.
    pub fn parse(text: &str) -> Result<Element> {
        let mut p = P { s: text, i: 0 };
        let root = p.element()?;
        p.skip_misc();
        if !p.rest().trim().is_empty() {
            return Err(err("junk after document element"));
        }
        Ok(root)
    }
}
