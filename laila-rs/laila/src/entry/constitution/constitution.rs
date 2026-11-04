//! Abstract [`Constitution`] base class -- recipe for materializing an
//! `Entry`'s payload.
//!
//! A *constitution* tells the build pipeline how to recover (or compute) an
//! entry's payload from upstream input. Constitutions are first-class,
//! serializable objects so an entry can be persisted with its build recipe
//! intact and re-executed in another process / on another machine.
//!
//! Two concrete subclasses cover the bulk of the design space:
//!
//! - `SimpleConstitution` -- an ordered list of Python source strings. Each
//!   source string defines exactly one top-level callable, and the build
//!   threads `payload_input` through the chain in order:
//!   `code[0](input) -> code[1](output) -> ...`. The canonical use case is
//!   the *inverse* transformation chain emitted by `Entry.serialize`.
//! - `ComplexConstitution` -- a single source string defining
//!   `f(manifest) -> payload` plus a `Manifest` (or its `global_id`). At
//!   build time the manifest is resolved from the active policy's memory if
//!   necessary, its referenced entries are forced to materialize, and the
//!   callable is applied to the live manifest.
//!
//! Both implement `build` and `as_dict`. The dispatcher
//! [`Constitution::from_dict`] selects the right subclass by reading the
//! `_kind` tag baked into every serialized constitution (`"simple"` or
//! `"complex"`).
//!
//! # Rust design decision (do not deviate)
//! Rust cannot `exec` Python source. Recovery snippets are recognised by
//! template and dispatched to native code (`_codecs::recovery_codes`); a
//! complex constitution's `code` is a *builder token* -- the same string on
//! both sides -- looked up in a process-wide registry
//! ([`register_builder_token`]). Anything else raises `NotImplementedError`.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_codecs::recovery_codes;
use crate::python::builtins::IsInstance;
use crate::python::pyrepr::str_repr;
use crate::python::sync::{Mutex, RwLock};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::complex_constitution::ComplexConstitution;
use super::simple_constitution::SimpleConstitution;

/// The single callable a constitution slot defines: `f(value) -> value`
/// for simple codes, `f(manifest) -> payload` for complex ones.
///
/// The complex builder receives the *manifest* as an `Arg` (`Arg::Entry`
/// tagged `Manifest`; see `policy::central::memory::schema::manifest`).
pub type ConstitutionFn = Arc<dyn Fn(&Arg) -> Result<Arg> + Send + Sync>;

/// `_from_dict` hook of a registered subclass.
pub type FromDictFn = fn(&Arg) -> Result<Constitution>;

/// `_REGISTRY`: kind -> `_from_dict` hook of each registered subclass.
pub static _REGISTRY: RwLock<Vec<(String, FromDictFn)>> = RwLock::new(Vec::new());
static BUILDER_TOKENS: RwLock<Vec<(String, ConstitutionFn)>> = RwLock::new(Vec::new());
static SEEDED: Mutex<bool> = Mutex::new(false);

fn ensure_seeded() {
    let mut g = SEEDED.lock();
    if *g {
        return;
    }
    *g = true;
    // ``constitution/__init__.py`` imports ``complex_constitution`` first,
    // then ``simple_constitution``: that is the ``_REGISTRY`` key order.
    _register_kind("complex", |d| {
        Ok(Constitution::Complex(ComplexConstitution::_from_dict(d)?))
    });
    _register_kind("simple", |d| {
        Ok(Constitution::Simple(SimpleConstitution::_from_dict(d)?))
    });
}

/// `@_register_kind(kind)`: register a `Constitution` subclass under a
/// string `_kind` tag. The dispatcher [`Constitution::from_dict`] uses the
/// registry to map serialized `_kind` values back to the right subclass.
pub fn _register_kind(kind: &str, from_dict: FromDictFn) {
    let mut g = _REGISTRY.write();
    if let Some(slot) = g.iter_mut().find(|(k, _)| k == kind) {
        slot.1 = from_dict;
    } else {
        g.push((kind.to_string(), from_dict));
    }
}

/// `list(_REGISTRY.keys())`
pub fn registered_kinds() -> Vec<String> {
    ensure_seeded();
    _REGISTRY.read().iter().map(|(k, _)| k.clone()).collect()
}

/// Register the native builder for a complex-constitution *token*: the
/// `code` string stored in the constitution (identical on the Python side)
/// maps to `builder(manifest) -> payload`.
pub fn register_builder_token(token: &str, builder: ConstitutionFn) {
    let mut g = BUILDER_TOKENS.write();
    if let Some(slot) = g.iter_mut().find(|(t, _)| t == token) {
        slot.1 = builder;
    } else {
        g.push((token.to_string(), builder));
    }
}

/// A constitution source literal *as a value*: registers `builder` as the
/// native twin of `token` and yields the token text, so
/// `Entry.variable(constitution=builder_token("def build(m): ..", f), ..)`
/// reads like the Python while the serialized `code` stays byte-identical
/// (rule P31b).
pub fn builder_token(token: impl IntoArg, builder: fn(&Arg) -> Result<Arg>) -> Arg {
    let token = token.into_arg();
    register_builder_token(&token.__str__(), Arc::new(builder));
    token
}

/// A *computed* constitution source as a value: registers `builder` under
/// the token's exact text and yields the text. The builder receives the
/// text it was registered under (so one Rust twin serves a family of
/// generated bodies, reading its parameters back from the source) and
/// the manifest (rule P31c).
pub fn builder_token_dyn(token: impl IntoArg, builder: fn(&str, &Arg) -> Result<Arg>) -> Arg {
    let token = token.into_arg();
    let text = token.__str__();
    let owned = text.clone();
    register_builder_token(
        &text,
        Arc::new(move |manifest: &Arg| builder(&owned, manifest)),
    );
    token
}

fn registered_builder(code: &str) -> Option<ConstitutionFn> {
    BUILDER_TOKENS
        .read()
        .iter()
        .find(|(t, _)| t == code)
        .map(|(_, b)| b.clone())
}

/// Compile a constitution source string and return its single defined callable.
///
/// Python `exec`s *code* in an empty namespace and requires exactly one
/// top-level callable. laila-rs resolves the string instead:
///
/// 1. a recognised laila recovery-code template -> its native `backward`;
/// 2. a registered builder token -> the registered builder;
/// 3. anything else (including Python source -- `def ...`) raises
///    `NotImplementedError`.
pub fn _exec_one_fn(code: &str) -> Result<ConstitutionFn> {
    // Make sure the built-in transformations (and the ``fernet`` backend)
    // are registered before compiling.
    let _ = super::super::compdata::transformation::REGISTRY();
    if recovery_codes::recognize(code)?.is_some() {
        return recovery_codes::compile_backward(code);
    }
    if let Some(b) = registered_builder(code) {
        return Ok(b);
    }
    // Python's "exactly one callable" rule, applied to the source text
    // (top-level `def` / `class` / `name = lambda` statements, private
    // `__`-prefixed names tolerated) before admitting that the body itself
    // cannot be executed here. A bare builder token (`"combine"`: one
    // word, no statement syntax) is not source: it is simply unregistered.
    if _looks_like_source(code) && _count_top_level_callables(code) != 1 {
        return Err(LailaError::value_error(
            "constitution code must define exactly one callable function",
        ));
    }
    Err(LailaError::not_implemented(
        "laila-rs cannot execute Python constitution source; register a builder for this token",
    ))
}

/// Is *code* Python source rather than a bare builder token? Source has
/// statement syntax: a newline, whitespace, or one of `=`, `:`, `(`.
fn _looks_like_source(code: &str) -> bool {
    let code = code.trim();
    code.contains(char::is_whitespace) || code.contains(['=', ':', '('])
}

/// Count the callables a Python source string binds at module level, as
/// `_exec_one_fn` would see them in its namespace after `exec`.
fn _count_top_level_callables(code: &str) -> usize {
    code.lines()
        .filter(|line| !line.starts_with([' ', '\t']))
        .filter_map(|line| {
            let line = line.trim_end();
            let name = if let Some(rest) = line
                .strip_prefix("def ")
                .or_else(|| line.strip_prefix("async def "))
                .or_else(|| line.strip_prefix("class "))
            {
                rest.split(|c: char| c == '(' || c == ':' || c.is_whitespace())
                    .next()
                    .unwrap_or("")
            } else if let Some((lhs, rhs)) = line.split_once('=') {
                if rhs.trim_start().starts_with("lambda") {
                    lhs.trim()
                } else {
                    return None;
                }
            } else {
                return None;
            };
            (!name.starts_with("__")).then_some(name)
        })
        .count()
}

/// Abstract base for entry constitutions (the closed set of registered
/// subclasses, as a Rust enum).
///
/// A constitution knows how to produce an entry's payload value. The
/// `payload_input` argument to [`build`](Constitution::build) is used by
/// simple constitutions to receive the serialized-blob bytes that need to be
/// decoded; complex constitutions ignore it and read from their bound
/// manifest instead.
#[derive(Clone, Debug)]
pub enum Constitution {
    Simple(SimpleConstitution),
    Complex(ComplexConstitution),
}

impl Constitution {
    /// `self._kind`
    pub fn _kind(&self) -> &'static str {
        match self {
            Constitution::Simple(_) => SimpleConstitution::_KIND,
            Constitution::Complex(_) => ComplexConstitution::_KIND,
        }
    }

    /// Produce and return the entry's payload value.
    ///
    /// `payload_input`: for `SimpleConstitution` the serialized blob to be
    /// threaded through the inverse-transformation chain; for
    /// `ComplexConstitution` ignored (input comes from the bound manifest).
    pub fn build(&self, payload_input: impl Into<Args>) -> Result<Arg> {
        match self {
            Constitution::Simple(s) => s.build(payload_input),
            Constitution::Complex(c) => c.build(payload_input),
        }
    }

    /// The JSON-friendly serialized representation (includes `_kind`).
    pub fn as_dict(&self) -> Result<Arg> {
        match self {
            Constitution::Simple(s) => s.as_dict(),
            Constitution::Complex(c) => c.as_dict(),
        }
    }

    /// Reconstruct a `Constitution` from its serialized dict.
    ///
    /// Reads the `_kind` tag, looks up the registered subclass, and
    /// delegates to that subclass's `_from_dict`. `None` short-circuits to
    /// `None`.
    ///
    /// `KeyError` if *in_dict* is missing the `_kind` tag; `ValueError` if
    /// the `_kind` is not registered.
    pub fn from_dict(in_dict: impl IntoArg) -> Result<Option<Constitution>> {
        let in_dict = in_dict.into_arg();
        if in_dict.is_none() {
            return Ok(None);
        }
        if !in_dict.__contains__("_kind")? {
            return Err(LailaError::key_error(str_repr(
                "serialized constitution is missing the '_kind' tag",
            )));
        }
        let kind = in_dict.__getitem__("_kind")?;
        ensure_seeded();
        // ``_REGISTRY.get(kind)`` -- an unhashable key raises TypeError.
        kind.__hash__()?;
        let sub = kind.as_str().and_then(|k| {
            _REGISTRY
                .read()
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, f)| *f)
        });
        let Some(sub_cls) = sub else {
            return Err(LailaError::value_error(alloc::format!(
                "no Constitution subclass registered for kind '{}'. Registered: {}",
                kind.__str__(),
                registered_kinds()
                    .into_iter()
                    .map(|k| Arg::Str(k.into()))
                    .collect::<Vec<_>>()
                    .into_arg()
                    .__repr__()
            )));
        };
        Ok(Some(sub_cls(&in_dict)?))
    }

    /// `isinstance(x, Constitution)` -> the value.
    pub fn from_arg(a: &Arg) -> Option<Constitution> {
        if let Some(s) = a.downcast_ref::<SimpleConstitution>() {
            return Some(Constitution::Simple(s.clone()));
        }
        if let Some(c) = a.downcast_ref::<ComplexConstitution>() {
            return Some(Constitution::Complex(c.clone()));
        }
        None
    }

    pub fn as_simple(&self) -> Option<&SimpleConstitution> {
        match self {
            Constitution::Simple(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_complex(&self) -> Option<&ComplexConstitution> {
        match self {
            Constitution::Complex(c) => Some(c),
            _ => None,
        }
    }

    /// Python `is`.
    pub fn is(&self, other: &Constitution) -> bool {
        match (self, other) {
            (Constitution::Simple(a), Constitution::Simple(b)) => a.is(b),
            (Constitution::Complex(a), Constitution::Complex(b)) => a.is(b),
            _ => false,
        }
    }

    pub fn __repr__(&self) -> String {
        match self {
            Constitution::Simple(s) => s.__repr__(),
            Constitution::Complex(c) => c.__repr__(),
        }
    }
}

impl PartialEq for Constitution {
    fn eq(&self, other: &Constitution) -> bool {
        match (self, other) {
            (Constitution::Simple(a), Constitution::Simple(b)) => a == b,
            (Constitution::Complex(a), Constitution::Complex(b)) => a == b,
            _ => false,
        }
    }
}

/// A `Constitution` travels as the `Arg::Object` of its concrete subclass.
impl IntoArg for Constitution {
    fn into_arg(self) -> Arg {
        match self {
            Constitution::Simple(s) => s.into_arg(),
            Constitution::Complex(c) => c.into_arg(),
        }
    }
}
impl PyArg for Constitution {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}
impl IsInstance for Constitution {
    fn isinstance(x: &Arg) -> bool {
        Constitution::from_arg(x).is_some()
    }
}

/// Shared `PyObject` plumbing for the two subclasses: the attribute names
/// Python exposes and the methods callable through the dynamic layer.
pub(crate) fn call_method(c: &Constitution, name: &str, args: Args) -> Result<Arg> {
    match name {
        "build" => {
            let payload_input = match args.positional.first() {
                Some(v) => v.clone(),
                None => args.get("payload_input"),
            };
            c.build(payload_input)
        }
        "as_dict" => c.as_dict(),
        other => Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute '{other}'",
            match c {
                Constitution::Simple(_) => "SimpleConstitution",
                Constitution::Complex(_) => "ComplexConstitution",
            }
        ))),
    }
}
