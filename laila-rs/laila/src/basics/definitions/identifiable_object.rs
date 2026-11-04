//! Identifiable-object base class with UUID and global-ID support.
//!
//! `_LAILA_IDENTIFIABLE_OBJECT` is the foundation every *identifiable* laila
//! object inherits from (it itself derives from `_LAILA_OBJECT`, the shared
//! root that carries the creation timestamp). It pins down the *identity*
//! contract the rest of the codebase relies on:
//!
//! - A UUID (defaulting to a fresh `uuid4`).
//! - A list of hierarchical *scopes* (e.g. `["POLICY"]`, `["FUTURE"]`).
//! - An optional *evolution* counter that distinguishes successive versions
//!   of the same logical object (`None` for "constant" / unversioned
//!   objects, an integer for "variable" / mutable ones).
//!
//! These three components combine into the canonical *global ID* string:
//!
//! ```text
//! LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]
//! ```
//!
//! Everything after `@` is a comma-separated `key=value` list of
//! *attributes*. `evolution` is the only attribute that is part of the
//! identity and the only one `global_id` ever emits; any other key
//! (`creation_timestamp=...`) is accepted by the parser as a *search
//! argument*, exposed through `get_attributes_from_global_id`, and consumed
//! by `laila.remember` -- it is never folded into identity.
//!
//! The Rust port exposes the identity as a plain value, [`Identity`], that
//! typed objects (`Entry`, pools, policies) embed, plus the class itself,
//! [`_LAILA_IDENTIFIABLE_OBJECT`], as a pydantic-style model base whose
//! hooks stage the identity through the per-construction `_INIT_PENDING`
//! slot exactly like the Python class does. Every static / class method of
//! the Python class is a free function here *and* an associated function of
//! the unit struct, so `_LAILA_IDENTIFIABLE_OBJECT.process_global_id(x)`
//! keeps its spelling.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    InitState, Kwargs, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance, PrivateAttrInfo,
};
use crate::macros::strings::{_ENTRY_SCOPE, _OBJECT_SCOPE, _TOPMOST_SCOPE};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyDictBuilder, Result};

use super::laila_object::_LAILA_OBJECT;

// ---------------------------------------------------------------------------
// Patterns

/// `key=value` attribute pair. Keys are identifiers; values may contain
/// anything except the pair separator `,` and the attribute marker `@`
/// (ISO timestamps with `:` / `+` / `.` are therefore fine).
pub const _ATTRIBUTE_PAIR: &str = r"[A-Za-z_][A-Za-z0-9_]*=[^,@]*";

/// The single source of truth for parsing global ids (as regex text; the
/// matcher is [`match_global_id`]).
pub const GLOBAL_ID_REGEX_PATTERN: &str = r"^(?P<scopes>(?:[A-Za-z0-9_]+:)+)(?P<uuid>[0-9a-fA-F-]{36})(?:@(?P<attributes>[A-Za-z_][A-Za-z0-9_]*=[^,@]*(?:,[A-Za-z_][A-Za-z0-9_]*=[^,@]*)*))?$";

/// `^[0-9a-fA-F-]{36}$`
pub const _UUID_RE: &str = r"^[0-9a-fA-F-]{36}$";
/// Hex digits and dashes in the neighbourhood of a uuid's length: what a
/// truncated / corrupted uuid (or a uuid with junk appended) looks like.
pub const _UUID_LIKE_RE: &str = r"^[0-9a-fA-F-]{30,48}$";
/// `^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,@]*)$`
pub const _ATTRIBUTE_RE: &str = r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,@]*)$";
/// Evolution as written in a *reference*: negatives (`-1` = latest) are
/// search arguments resolved by `laila.remember`; identity is non-negative.
pub const _REFERENCE_EVOLUTION_RE: &str = r"^-?\d+$";

/// Name of the single attribute that is part of an object's identity.
pub const EVOLUTION_ATTRIBUTE: &str = "evolution";

/// Scopes assumed for a scope-less *reference* (`"run-3"`,
/// `"counter@evolution=3"`) when the resolving class is the generic base.
pub const _DEFAULT_REFERENCE_SCOPES: &[&str] = &[_ENTRY_SCOPE];

fn is_uuid_char(c: char) -> bool {
    c.is_ascii_hexdigit() || c == '-'
}

fn is_scope_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

/// `_UUID_RE.match(s)`
pub fn is_valid_uuid(s: &str) -> bool {
    s.chars().count() == 36 && s.chars().all(is_uuid_char)
}

/// `_UUID_LIKE_RE.match(s)`
pub fn is_uuid_like(s: &str) -> bool {
    let n = s.chars().count();
    (30..=48).contains(&n) && s.chars().all(is_uuid_char)
}

/// `_REFERENCE_EVOLUTION_RE.match(s)`
pub fn is_reference_evolution(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// `_ATTRIBUTE_RE.match(pair)` -> `(key, value)`.
pub fn match_attribute(pair: &str) -> Option<(&str, &str)> {
    let (key, value) = pair.split_once('=')?;
    let mut chars = key.chars();
    let first = chars.next()?;
    if !is_ident_start(first) || !chars.all(is_scope_char) {
        return None;
    }
    if value.contains(',') || value.contains('@') {
        return None;
    }
    Some((key, value))
}

/// The named groups of `GLOBAL_ID_REGEX_PATTERN`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GidMatch<'a> {
    /// `(?P<scopes>...)`: `"LAILA:ENTRY:"` (trailing colon included).
    pub scopes: &'a str,
    /// `(?P<uuid>...)`
    pub uuid: &'a str,
    /// `(?P<attributes>...)` -- the text after `@`, if any.
    pub attributes: Option<&'a str>,
}

/// `GLOBAL_ID_REGEX_PATTERN.match(s)`.
pub fn match_global_id(s: &str) -> Option<GidMatch<'_>> {
    let (head, attributes) = match s.split_once('@') {
        Some((h, a)) => (h, Some(a)),
        None => (s, None),
    };
    if let Some(a) = attributes {
        if a.is_empty() || a.split(',').any(|p| match_attribute(p).is_none()) {
            return None;
        }
    }
    // The uuid is the 36 characters right before the `@` / the end.
    let bytes = head.len();
    if bytes < 36 || !head.is_char_boundary(bytes - 36) {
        return None;
    }
    let (scopes, uuid) = head.split_at(bytes - 36);
    if !uuid.chars().all(is_uuid_char) || uuid.chars().count() != 36 {
        return None;
    }
    if !scopes.ends_with(':') {
        return None;
    }
    let segs = &scopes[..scopes.len() - 1];
    if segs
        .split(':')
        .any(|seg| seg.is_empty() || !seg.chars().all(is_scope_char))
    {
        return None;
    }
    Some(GidMatch {
        scopes,
        uuid,
        attributes,
    })
}

// ---------------------------------------------------------------------------
// Attribute helpers

/// Parse the `@`-suffix of a global id into an ordered `[(key, value)]` list.
///
/// `attributes` is the text after `@` (`"evolution=3,creation_timestamp=..."`),
/// or `None` / `""` for no attributes. Raises `ValueError` on a malformed
/// pair (missing `=`, empty key, illegal key characters, trailing comma) or
/// a duplicated key.
pub fn parse_global_id_attributes(attributes: Option<&str>) -> Result<Vec<(String, String)>> {
    let attributes = match attributes {
        None | Some("") => return Ok(Vec::new()),
        Some(a) => a,
    };
    let mut out: Vec<(String, String)> = Vec::new();
    for pair in attributes.split(',') {
        let (key, value) = match_attribute(pair).ok_or_else(|| {
            LailaError::value_error(alloc::format!(
                "Invalid global id attribute: {}",
                crate::python::pyrepr::str_repr(pair)
            ))
        })?;
        if out.iter().any(|(k, _)| k == key) {
            return Err(LailaError::value_error(alloc::format!(
                "Duplicate global id attribute: {}",
                crate::python::pyrepr::str_repr(key)
            )));
        }
        out.push((key.to_string(), value.to_string()));
    }
    Ok(out)
}

/// [`parse_global_id_attributes`] as a Python dict `{key: value}`.
pub fn parse_global_id_attributes_dict(attributes: Option<&str>) -> Result<Arg> {
    let mut d = PyDictBuilder::new();
    for (k, v) in parse_global_id_attributes(attributes)? {
        d.insert(k, v);
    }
    Ok(d.finish())
}

/// Inverse of [`parse_global_id_attributes`] (without the leading `@`).
///
/// `None` values are skipped so callers can pass `("evolution", None)` for
/// constants; an empty result means "no suffix".
pub fn format_global_id_attributes(attributes: &[(String, Option<String>)]) -> String {
    attributes
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| alloc::format!("{k}={v}")))
        .collect::<Vec<_>>()
        .join(",")
}

/// Split `"<head>@<attributes>"` into `(head, [(key, value)])`.
///
/// *head* is everything before the first `@` (scopes plus uuid or nickname).
/// Works on full global ids and on shorthand references alike; a reference
/// without `@` yields `(ref, [])`.
pub fn split_global_id_attributes(reference: &str) -> Result<(String, Vec<(String, String)>)> {
    match reference.split_once('@') {
        None => Ok((reference.to_string(), Vec::new())),
        Some((_, "")) => Err(LailaError::value_error(alloc::format!(
            "Invalid GID format: {reference}"
        ))),
        Some((head, tail)) => Ok((head.to_string(), parse_global_id_attributes(Some(tail))?)),
    }
}

// ---------------------------------------------------------------------------
// Staging slot (`_INIT_PENDING`)

/// The identity staged by `__init__` for `model_post_init` (Python's
/// thread-local `_INIT_PENDING`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingIdentity {
    pub uuid: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub evolution: Option<Arg>,
    pub scopes_explicit: bool,
}

#[cfg(feature = "std")]
mod pending {
    use super::PendingIdentity;
    use core::cell::RefCell;
    std::thread_local! {
        static INIT_PENDING: RefCell<PendingIdentity> = RefCell::new(PendingIdentity::default());
    }
    pub fn get() -> PendingIdentity {
        INIT_PENDING.with(|p| p.borrow().clone())
    }
    pub fn set(v: PendingIdentity) {
        INIT_PENDING.with(|p| *p.borrow_mut() = v);
    }
}
#[cfg(not(feature = "std"))]
mod pending {
    use super::PendingIdentity;
    use crate::python::sync::Mutex;
    static INIT_PENDING: Mutex<PendingIdentity> = Mutex::new(PendingIdentity {
        uuid: None,
        scopes: None,
        evolution: None,
        scopes_explicit: false,
    });
    pub fn get() -> PendingIdentity {
        INIT_PENDING.lock().clone()
    }
    pub fn set(v: PendingIdentity) {
        *INIT_PENDING.lock() = v;
    }
}

/// The current `_INIT_PENDING` staging values (read by
/// `cli_capable._resolve_global_id_for_path` while a model is validating).
pub fn _init_pending() -> PendingIdentity {
    pending::get()
}

/// Overwrite the `_INIT_PENDING` staging values.
pub fn _set_init_pending(v: PendingIdentity) {
    pending::set(v)
}

// ---------------------------------------------------------------------------
// Identity value

/// The `(uuid, scopes, evolution)` triple of a laila object -- what
/// `_LAILA_IDENTIFIABLE_OBJECT` keeps in `_uuid` / `_scopes` / `_evolution`,
/// as a plain value that typed objects embed. `nickname` is the alias the
/// uuid was derived from, if any (`Entry` keeps it; the base class does
/// not), and `creation_timestamp` is the `_LAILA_OBJECT` stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub uuid: String,
    pub scopes: Vec<String>,
    pub evolution: Option<i64>,
    pub nickname: Option<String>,
    pub creation_timestamp: Option<String>,
}

/// The parsed parts of a global id: `{"uuid", "scopes", "evolution"}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedGlobalId {
    pub uuid: String,
    pub scopes: Vec<String>,
    pub evolution: Option<i64>,
}

impl ProcessedGlobalId {
    /// The Python return value: `{"uuid": str, "scopes": list[str], "evolution": int | None}`.
    pub fn to_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        d.insert("uuid", self.uuid.as_str());
        d.insert("scopes", strs_to_arg(&self.scopes));
        d.insert("evolution", self.evolution);
        d.finish()
    }
    /// `(uuid, evolution, scopes)`.
    pub fn into_tuple(self) -> (String, Option<i64>, Vec<String>) {
        (self.uuid, self.evolution, self.scopes)
    }
}

impl IntoArg for ProcessedGlobalId {
    fn into_arg(self) -> Arg {
        self.to_dict()
    }
}

fn strs_to_arg(v: &[String]) -> Arg {
    v.iter()
        .map(|s| Arg::Str(s.as_str().into()))
        .collect::<Vec<_>>()
        .into_arg()
}

/// `list(str(x) for x in a)` for a `scopes` argument (`TypeError` for a non-sequence).
pub fn arg_to_strs(a: &Arg) -> Result<Vec<String>> {
    Ok(a.__iter__()?.iter().map(|x| x.__str__()).collect())
}

fn opt_str(a: &Arg) -> Option<String> {
    match a {
        Arg::None => None,
        other => Some(other.__str__()),
    }
}

impl Identity {
    /// The effective identity for `(uuid=, scopes=, evolution=, nickname=)`
    /// constructor arguments: a nickname is hashed to a UUID-5 under the
    /// active namespace, an explicit uuid is kept, otherwise a fresh uuid4;
    /// missing scopes fall back to `default_scopes` (the class's
    /// `_DEFAULT_SCOPES`).
    pub fn new(
        uuid: Option<&str>,
        scopes: Option<Vec<String>>,
        evolution: Option<i64>,
        nickname: Option<&str>,
        default_scopes: &[&str],
    ) -> Identity {
        let uuid = match (nickname, uuid) {
            (Some(n), _) => generate_uuid_from_nickname(n),
            (None, Some(u)) => u.to_string(),
            (None, None) => crate::_compat::uuid::uuid4(),
        };
        Identity {
            uuid,
            scopes: scopes
                .unwrap_or_else(|| default_scopes.iter().map(|s| s.to_string()).collect()),
            evolution,
            nickname: nickname.map(str::to_string),
            creation_timestamp: Some(super::laila_object::_now_creation_timestamp()),
        }
    }

    /// `Identity::new` with `["OBJECT"]` as the default scopes (the base class).
    pub fn object(
        uuid: Option<&str>,
        scopes: Option<Vec<String>>,
        evolution: Option<i64>,
        nickname: Option<&str>,
    ) -> Identity {
        Identity::new(uuid, scopes, evolution, nickname, &[_OBJECT_SCOPE])
    }

    /// `_LAILA_IDENTIFIABLE_OBJECT(uuid=None, scopes=None, evolution=None, nickname=None)`
    /// from Python-style arguments (a `global_id=` keyword is also accepted
    /// and parsed; `scopes` is any iterable of strings).
    #[pysig("Identity(uuid=None, scopes=None, evolution=None, nickname=None, global_id=None, default_scopes=None)")]
    pub fn from_args(args: impl Into<Args>) -> Result<Identity> {
        let defaults: Vec<String> = match &default_scopes {
            Arg::None => alloc::vec![_OBJECT_SCOPE.to_string()],
            s => arg_to_strs(s)?,
        };
        let defaults_ref: Vec<&str> = defaults.iter().map(String::as_str).collect();
        if !global_id.is_none() {
            let parsed = process_global_id_arg(&global_id)?;
            return Ok(Identity::new(
                Some(&parsed.uuid),
                Some(parsed.scopes),
                parsed.evolution,
                None,
                &defaults_ref,
            ));
        }
        let scopes = match &scopes {
            Arg::None => None,
            s => Some(arg_to_strs(s)?),
        };
        Ok(Identity::new(
            opt_str(&uuid).as_deref(),
            scopes,
            evolution_of(&evolution)?,
            opt_str(&nickname).as_deref(),
            &defaults_ref,
        ))
    }

    /// `cls.from_global_id(global_id)`: identity parsed from a global id or
    /// a scoped shorthand (see [`resolve_global_id`]).
    pub fn from_global_id(global_id: &str) -> Result<Identity> {
        let p = process_global_id(global_id)?;
        Ok(Identity {
            uuid: p.uuid,
            scopes: p.scopes,
            evolution: p.evolution,
            nickname: None,
            creation_timestamp: Some(super::laila_object::_now_creation_timestamp()),
        })
    }

    /// `LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]`
    pub fn global_id(&self) -> String {
        to_global_id(Some(&self.uuid), Some(&self.scopes), self.evolution, None)
    }

    /// The `global_id` setter: re-parse and re-assign uuid / scopes / evolution.
    pub fn set_global_id(&mut self, value: &str) -> Result<()> {
        let p = process_global_id(value)?;
        self.uuid = p.uuid;
        self.scopes = p.scopes;
        self.evolution = p.evolution;
        Ok(())
    }

    /// `self.evolution is not None`
    pub fn has_evolution(&self) -> bool {
        self.evolution.is_some()
    }

    /// `identity()`: `{"uuid": ..}` plus `"scopes"` when non-empty and
    /// `"evolution"` when not `None`.
    pub fn identity(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        d.insert("uuid", self.uuid.as_str());
        if !self.scopes.is_empty() {
            d.insert("scopes", strs_to_arg(&self.scopes));
        }
        if let Some(e) = self.evolution {
            d.insert("evolution", e);
        }
        d.finish()
    }

    /// `json.dumps(self.identity())`
    pub fn identity_as_json(&self) -> Result<String> {
        crate::python::stdlib::json.dumps(Args::from(self.identity()))
    }

    /// `str(obj)` / `repr(obj)` -> the global id.
    pub fn __str__(&self) -> String {
        self.global_id()
    }
    pub fn __repr__(&self) -> String {
        self.global_id()
    }
    /// `hash(obj)` -> `hash(global_id)`.
    pub fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.global_id().into()).__hash__()
    }
    /// Equality follows identity: same global id means the same object.
    pub fn __eq__(&self, other: &Identity) -> bool {
        self.global_id() == other.global_id()
    }
    /// `obj != other` -- the negation of [`__eq__`](Self::__eq__).
    pub fn __ne__(&self, other: &Identity) -> bool {
        !self.__eq__(other)
    }
    /// `"variable"` when the identity has an evolution, else `"constant"`.
    pub fn kind(&self) -> &'static str {
        if self.evolution.is_some() {
            "variable"
        } else {
            "constant"
        }
    }
    /// `scopes` as a Python list.
    pub fn scopes_arg(&self) -> Arg {
        strs_to_arg(&self.scopes)
    }
    /// `evolution` as a Python value.
    pub fn evolution_arg(&self) -> Arg {
        self.evolution.into_arg()
    }
}

impl core::fmt::Display for Identity {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.global_id())
    }
}

fn evolution_of(a: &Arg) -> Result<Option<i64>> {
    match a {
        Arg::None => Ok(None),
        Arg::Int(i) => Ok(Some(*i)),
        Arg::Bool(b) => Ok(Some(i64::from(*b))),
        Arg::Str(s) => s.parse::<i64>().map(Some).map_err(|_| {
            LailaError::value_error(alloc::format!(
                "invalid literal for int() with base 10: {}",
                crate::python::pyrepr::str_repr(s)
            ))
        }),
        other => Err(LailaError::type_error(alloc::format!(
            "'{}' object cannot be interpreted as an integer",
            other.type_name()
        ))),
    }
}

// ---------------------------------------------------------------------------
// Free functions (the static / class methods of the Python class)

/// Options of [`resolve_global_id`] (the keyword-only parameters).
#[derive(Clone, Debug, Default)]
pub struct ResolveOptions<'a> {
    /// Explicit evolution counter; overrides an `evolution=` attribute in the reference.
    pub evolution: Option<i64>,
    /// Scopes placed before the user-supplied ones (default `["LAILA"]`).
    pub prefix_scopes: Option<&'a [&'a str]>,
    /// Scopes used when the reference has no scope segment (default: the class's `_DEFAULT_SCOPES`).
    pub default_scopes: Option<&'a [&'a str]>,
    /// Whether an `@...` suffix is interpreted (`false`: the whole tail is the nickname).
    pub parse_evolution: bool,
}

impl ResolveOptions<'_> {
    /// The Python defaults (`evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True`).
    pub fn new() -> Self {
        ResolveOptions {
            evolution: None,
            prefix_scopes: None,
            default_scopes: None,
            parse_evolution: true,
        }
    }
}

/// `cls.resolve_global_id(ref, *, evolution=None, prefix_scopes=None,
/// default_scopes=None, parse_evolution=True)` with Python-shaped
/// (already bound) arguments, for a class whose `_DEFAULT_SCOPES` is
/// `class_default_scopes` -- the body shared by the subclasses' classmethods.
pub fn resolve_global_id_bound(
    class_default_scopes: &[&str],
    r#ref: &Arg,
    evolution: &Arg,
    prefix_scopes: &Arg,
    default_scopes: &Arg,
    parse_evolution: &Arg,
) -> Result<String> {
    let reference = match r#ref {
        Arg::Str(s) => s.to_string(),
        other => {
            return Err(LailaError::value_error(alloc::format!(
                "global id reference must be a string, got {}",
                other.type_name()
            )))
        }
    };
    let evolution = match evolution {
        Arg::None => None,
        e => Some(
            e.as_i64()
                .ok_or_else(|| LailaError::type_error("evolution must be an integer or None"))?,
        ),
    };
    let to_strs = |a: &Arg| -> Result<Option<Vec<String>>> {
        match a {
            Arg::None => Ok(None),
            s => Ok(Some(s.__iter__()?.iter().map(|x| x.__str__()).collect())),
        }
    };
    let prefix = to_strs(prefix_scopes)?;
    let defaults = to_strs(default_scopes)?;
    let prefix_ref: Option<Vec<&str>> = prefix
        .as_ref()
        .map(|p| p.iter().map(String::as_str).collect());
    let defaults_ref: Option<Vec<&str>> = defaults
        .as_ref()
        .map(|p| p.iter().map(String::as_str).collect());
    resolve_global_id_for(
        class_default_scopes,
        &reference,
        &ResolveOptions {
            evolution,
            prefix_scopes: prefix_ref.as_deref(),
            default_scopes: defaults_ref.as_deref(),
            parse_evolution: match parse_evolution {
                Arg::None => true,
                v => v.__bool__(),
            },
        },
    )
}

/// Decode a global-id *reference* into a full global ID, for a class whose
/// `_DEFAULT_SCOPES` is `class_default_scopes`.
///
/// Accepted forms: a complete global id (returned unchanged), a scoped
/// shorthand `SCOPE[:SCOPE...]:<uuid | nickname>[@attrs]`, or a scope-less
/// `<uuid | nickname>[@attrs]` (scoped with the defaults; on the generic base
/// class a scope-less reference means an `ENTRY`). A tail that looks like a
/// truncated / corrupted uuid is rejected rather than hashed as a nickname.
pub fn resolve_global_id_for(
    class_default_scopes: &[&str],
    reference: &str,
    opts: &ResolveOptions<'_>,
) -> Result<String> {
    let prefix: Vec<String> = match opts.prefix_scopes {
        Some(p) => p.iter().map(|s| s.to_string()).collect(),
        None => alloc::vec![_TOPMOST_SCOPE.to_string()],
    };

    let (head, mut attrs) = if opts.parse_evolution {
        split_global_id_attributes(reference)?
    } else {
        (reference.to_string(), Vec::new())
    };

    let mut parts: Vec<&str> = head.split(':').collect();
    let tail = parts.pop().unwrap_or("");
    let mut scopes: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
    if tail.is_empty() || scopes.iter().any(String::is_empty) {
        return Err(LailaError::value_error(alloc::format!(
            "Invalid GID format: {reference}"
        )));
    }

    // Already a full global id (well-formed *and* framed by the prefix)?
    // ``POLICY:<uuid>`` also matches the regex but lacks the frame, so it
    // falls through and is expanded like any other shorthand.
    let attr_evolution = attrs
        .iter()
        .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
        .map(|(_, v)| v.clone());
    if match_global_id(reference).is_some()
        && scopes.len() > prefix.len()
        && scopes[..prefix.len()] == prefix[..]
        && (opts.evolution.is_none()
            || attr_evolution.as_deref() == Some(opts.evolution.unwrap().to_string().as_str()))
    {
        return Ok(reference.to_string());
    }

    if let Some(e) = opts.evolution {
        match attrs.iter_mut().find(|(k, _)| k == EVOLUTION_ATTRIBUTE) {
            Some(slot) => slot.1 = e.to_string(),
            None => attrs.push((EVOLUTION_ATTRIBUTE.to_string(), e.to_string())),
        }
    } else if let Some(v) = &attr_evolution {
        if !is_reference_evolution(v) {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid evolution in GID: {reference}"
            )));
        }
    }

    let uid = if is_valid_uuid(tail) {
        tail.to_string()
    } else if is_uuid_like(tail) {
        // Hex-and-dashes that is not a well-formed uuid: a truncated or
        // corrupted uuid, or the legacy ``<uuid>-<evolution>`` form. A
        // malformed id, not a nickname -- hashing it would silently
        // produce a *different* valid id.
        return Err(LailaError::value_error(alloc::format!(
            "Invalid GID format: {reference}"
        )));
    } else {
        generate_uuid_from_nickname(tail)
    };

    // Tolerate a spelled-out prefix.
    if !prefix.is_empty() && scopes.len() >= prefix.len() && scopes[..prefix.len()] == prefix[..] {
        scopes = scopes[prefix.len()..].to_vec();
    }
    if scopes.is_empty() {
        scopes = if let Some(d) = opts.default_scopes {
            d.iter().map(|s| s.to_string()).collect()
        } else if class_default_scopes == [_OBJECT_SCOPE] {
            // The base class has no domain of its own: a scope-less
            // *reference* means an entry (``laila.remember("run-3")``).
            _DEFAULT_REFERENCE_SCOPES
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            class_default_scopes.iter().map(|s| s.to_string()).collect()
        };
    }

    let mut all: Vec<String> = prefix;
    all.extend(scopes);
    let gid = alloc::format!("{}:{uid}", all.join(":"));
    let suffix = format_global_id_attributes(
        &attrs
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect::<Vec<_>>(),
    );
    Ok(if suffix.is_empty() {
        gid
    } else {
        alloc::format!("{gid}@{suffix}")
    })
}

/// [`resolve_global_id_for`] on the generic base class (`_DEFAULT_SCOPES = ["OBJECT"]`).
pub fn resolve_global_id(reference: &str, opts: &ResolveOptions<'_>) -> Result<String> {
    resolve_global_id_for(&[_OBJECT_SCOPE], reference, opts)
}

/// Build a global ID string from its constituent parts:
/// `LAILA:<scopes>:<uuid>[@evolution=<n>]`. A `nickname` is converted to a
/// deterministic UUID-5 first; empty / missing scopes become `["OBJECT"]`.
pub fn to_global_id(
    uuid: Option<&str>,
    scopes: Option<&[String]>,
    evolution: Option<i64>,
    nickname: Option<&str>,
) -> String {
    let uuid = match nickname {
        Some(n) => generate_uuid_from_nickname(n),
        None => uuid
            .map(str::to_string)
            .unwrap_or_else(|| "None".to_string()),
    };
    let scopes: Vec<&str> = match scopes {
        Some(s) if !s.is_empty() => s.iter().map(String::as_str).collect(),
        _ => alloc::vec![_OBJECT_SCOPE],
    };
    let mut all: Vec<&str> = alloc::vec![_TOPMOST_SCOPE];
    all.extend(scopes);
    let base = alloc::format!("{}:{uuid}", all.join(":"));
    match evolution {
        None => base,
        Some(e) => alloc::format!("{base}@{EVOLUTION_ATTRIBUTE}={e}"),
    }
}

/// `True` if `global_id` parses as a laila global-ID string.
pub fn is_laila_resource(global_id: &str) -> bool {
    match_global_id(global_id).is_some()
}

/// Return every `@key=value` attribute of `global_id` as raw strings
/// (scoped shorthands are expanded first; `evolution` is a string too).
pub fn get_attributes_from_global_id(global_id: &str) -> Result<Vec<(String, String)>> {
    let full = resolve_global_id(global_id, &ResolveOptions::new())?;
    let m = match_global_id(&full)
        .ok_or_else(|| LailaError::value_error(alloc::format!("Invalid GID format: {full}")))?;
    parse_global_id_attributes(m.attributes)
}

/// `global_id` without its `@...` suffix (`LAILA:scopes:<uuid>`).
pub fn strip_global_id_attributes(global_id: &str) -> String {
    global_id.split('@').next().unwrap_or("").to_string()
}

/// Parse a global ID into its `uuid`, `scopes`, and `evolution` parts.
///
/// A full global-ID string, or a *scoped* shorthand such as
/// `"POLICY:trainer"` / `"ENTRY:counter@evolution=3"` which is first
/// expanded through [`resolve_global_id`]. A bare string with no scope
/// segment (`"counter"`) is an `ENTRY` reference. Attributes other than
/// `evolution` are ignored; a *negative* evolution is a search argument,
/// not an identity, and is rejected here.
pub fn process_global_id(global_id: &str) -> Result<ProcessedGlobalId> {
    if global_id.is_empty() {
        return Err(LailaError::value_error(alloc::format!(
            "Invalid GID format: {global_id}"
        )));
    }
    // Framed full gids pass through unchanged; scoped shorthands
    // ("POLICY:trainer", "POOL:<uuid>") are expanded.
    let full = resolve_global_id(global_id, &ResolveOptions::new())?;
    let m = match_global_id(&full)
        .ok_or_else(|| LailaError::value_error(alloc::format!("Invalid GID format: {full}")))?;
    let attrs = parse_global_id_attributes(m.attributes)?;
    let evolution_raw = attrs
        .iter()
        .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
        .map(|(_, v)| v.as_str());
    let evolution = match evolution_raw {
        None => None,
        Some(v) if !v.is_empty() && v.chars().all(|c| c.is_ascii_digit()) => {
            Some(v.parse::<i64>().map_err(|_| {
                LailaError::value_error(alloc::format!("Invalid evolution in GID: {full}"))
            })?)
        }
        Some(_) => {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid evolution in GID: {full}"
            )))
        }
    };
    // The scopes group is "LAILA:<mid...>:"; drop the leading TOPMOST and
    // the empty string after the final ':' so that `to_global_id(**parsed)`
    // round-trips exactly.
    let mut segs: Vec<&str> = m.scopes.split(':').collect();
    segs.pop(); // the empty string after the final ':'
    let scopes: Vec<String> = segs.iter().skip(1).map(|s| s.to_string()).collect();
    Ok(ProcessedGlobalId {
        uuid: m.uuid.to_string(),
        scopes,
        evolution,
    })
}

/// [`process_global_id`] on a Python value: a non-string is
/// `ValueError("Invalid GID format: <str(x)>")`.
pub fn process_global_id_arg(global_id: &Arg) -> Result<ProcessedGlobalId> {
    match global_id {
        Arg::Str(s) if !s.is_empty() => process_global_id(s),
        other => Err(LailaError::value_error(alloc::format!(
            "Invalid GID format: {}",
            other.__str__()
        ))),
    }
}

/// Classify a global id as `"variable"` (has evolution) or `"constant"`.
pub fn gid_type(x: &str) -> Result<&'static str> {
    Ok(if process_global_id(x)?.evolution.is_some() {
        "variable"
    } else {
        "constant"
    })
}

/// Deterministically derive a UUID from a human-readable nickname:
/// `str(uuid.uuid5(laila.get_active_namespace(), nickname))`.
pub fn generate_uuid_from_nickname(nickname: &str) -> String {
    crate::_compat::uuid::uuid5(&crate::_compat::uuid_ns::active_bytes(), nickname)
}

// ---------------------------------------------------------------------------
// The class

/// Pydantic base model providing UUID-based identity and global-ID encoding.
///
/// As a Rust value this unit struct is the *class*: the static / class
/// methods of the Python class are associated functions
/// (`_LAILA_IDENTIFIABLE_OBJECT.process_global_id(..)`), `__class__()` is
/// the model descriptor, `new(args)` constructs an instance and
/// `py_model!(X: _LAILA_IDENTIFIABLE_OBJECT)` derives from it. Typed objects
/// embed an [`Identity`] instead.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_OBJECT;

const UUID_ATTR: &str = "_uuid";
const SCOPES_ATTR: &str = "_scopes";
const EVOLUTION_ATTR: &str = "_evolution";

/// `type(self)._DEFAULT_SCOPES` for a model class.
pub fn default_scopes_of(cls: &'static ModelClass) -> Vec<String> {
    match cls.class_var("_DEFAULT_SCOPES") {
        Some(v) => arg_to_strs(&v).unwrap_or_else(|_| alloc::vec![_OBJECT_SCOPE.to_string()]),
        None => alloc::vec![_OBJECT_SCOPE.to_string()],
    }
}

/// The scope list a model class's `_scopes` PrivateAttr default produces
/// (`cls.__private_attributes__["_scopes"].default_factory()`), if any.
pub fn scopes_default_of(cls: &'static ModelClass) -> Option<Vec<String>> {
    let attr = cls.private_attr(SCOPES_ATTR)?;
    let v = attr.get_default().ok()??;
    if v.is_none() {
        return None;
    }
    arg_to_strs(&v).ok()
}

/// Read an identifiable model instance's identity triple.
pub fn identity_of(inst: &ModelInstance) -> Identity {
    let uuid = inst
        .private(UUID_ATTR)
        .map(|u| u.__str__())
        .unwrap_or_default();
    let scopes = inst
        .private(SCOPES_ATTR)
        .and_then(|s| arg_to_strs(&s).ok())
        .unwrap_or_default();
    let evolution = inst.private(EVOLUTION_ATTR).and_then(|e| e.as_i64());
    Identity {
        uuid,
        scopes,
        evolution,
        nickname: None,
        creation_timestamp: opt_str(&_LAILA_OBJECT::creation_timestamp(inst)),
    }
}

fn global_id_of(inst: &ModelInstance) -> String {
    let uuid = inst.private(UUID_ATTR).unwrap_or(Arg::None);
    let scopes = inst.private(SCOPES_ATTR).and_then(|s| arg_to_strs(&s).ok());
    let evolution = inst.private(EVOLUTION_ATTR).and_then(|e| e.as_i64());
    to_global_id(Some(&uuid.__str__()), scopes.as_deref(), evolution, None)
}

struct Hooks;

impl ModelHooks for Hooks {
    /// Stash identity fields in the staging slot and delegate to Pydantic.
    fn init_head(
        &self,
        cls: &'static ModelClass,
        data: &mut Kwargs,
        st: &mut InitState,
    ) -> Result<()> {
        let uuid_input = data.shift_remove("uuid").unwrap_or(Arg::None);
        let scopes_input = data.shift_remove("scopes").unwrap_or(Arg::None);
        let evolution_input = data.shift_remove("evolution").unwrap_or(Arg::None);
        let nickname_input = data.shift_remove("nickname").unwrap_or(Arg::None);

        let pending_uuid = if !nickname_input.is_none() {
            generate_uuid_from_nickname(&nickname_input.__str__())
        } else if !uuid_input.is_none() {
            uuid_input.__str__()
        } else {
            crate::_compat::uuid::uuid4()
        };

        let scopes_explicit = !scopes_input.is_none();
        let pending_scopes: Vec<String> = if scopes_explicit {
            arg_to_strs(&scopes_input)?
        } else {
            default_scopes_of(cls)
        };

        // Save the enclosing construction's staged identity (if any): a
        // nested identifiable object may be built while Pydantic validates
        // our fields, and it must not wipe our values before our own
        // ``model_post_init`` has consumed them.
        let prev = _init_pending();
        st.scratch
            .insert("prev_uuid".into(), prev.uuid.clone().into_arg());
        st.scratch.insert(
            "prev_scopes".into(),
            prev.scopes.as_deref().map(strs_to_arg).unwrap_or(Arg::None),
        );
        st.scratch.insert(
            "prev_evolution".into(),
            prev.evolution.clone().unwrap_or(Arg::None),
        );
        st.scratch.insert(
            "prev_scopes_explicit".into(),
            Arg::Bool(prev.scopes_explicit),
        );
        st.scratch.insert(
            "pending_uuid".into(),
            Arg::Str(pending_uuid.as_str().into()),
        );
        st.scratch
            .insert("pending_scopes".into(), strs_to_arg(&pending_scopes));
        st.scratch
            .insert("evolution_input".into(), evolution_input.clone());
        st.scratch
            .insert("scopes_explicit".into(), Arg::Bool(scopes_explicit));

        _set_init_pending(PendingIdentity {
            uuid: Some(pending_uuid),
            scopes: Some(pending_scopes),
            evolution: if evolution_input.is_none() {
                None
            } else {
                Some(evolution_input)
            },
            scopes_explicit,
        });
        Ok(())
    }

    /// Copy staged identity values from the staging slot onto private attrs.
    fn model_post_init(&self, inst: &ModelInstance, _st: &InitState) -> Result<()> {
        let pending = _init_pending();
        if let Some(u) = pending.uuid {
            inst.set_private(UUID_ATTR, Arg::Str(u.into()));
        }
        if let Some(s) = pending.scopes {
            // A subclass may still declare ``_scopes`` with its own
            // default_factory; an explicit ``scopes=`` argument always
            // wins, but a generated default must not clobber it.
            let current = inst.private(SCOPES_ATTR).unwrap_or(Arg::None);
            if current.is_none() || pending.scopes_explicit {
                inst.set_private(SCOPES_ATTR, strs_to_arg(&s));
            }
        }
        if let Some(e) = pending.evolution {
            inst.set_private(EVOLUTION_ATTR, e);
        }
        Ok(())
    }

    /// Restore the enclosing construction's staging values, then the
    /// fallback for subclasses whose ``model_post_init`` does not chain.
    fn init_tail(&self, inst: &ModelInstance, st: &mut InitState) -> Result<()> {
        let take = |st: &InitState, k: &str| st.scratch.get(k).cloned().unwrap_or(Arg::None);
        let prev = PendingIdentity {
            uuid: opt_str(&take(st, "prev_uuid")),
            scopes: match take(st, "prev_scopes") {
                Arg::None => None,
                s => Some(arg_to_strs(&s)?),
            },
            evolution: match take(st, "prev_evolution") {
                Arg::None => None,
                e => Some(e),
            },
            scopes_explicit: take(st, "prev_scopes_explicit").__bool__(),
        };
        _set_init_pending(prev);

        let pending_uuid = take(st, "pending_uuid");
        let pending_scopes = take(st, "pending_scopes");
        let evolution_input = take(st, "evolution_input");
        let scopes_explicit = take(st, "scopes_explicit").__bool__();

        if inst.private(UUID_ATTR).unwrap_or(Arg::None).is_none() {
            inst.set_private(UUID_ATTR, pending_uuid);
        }
        let current_scopes = inst.private(SCOPES_ATTR).unwrap_or(Arg::None);
        if current_scopes.is_none() || (scopes_explicit && current_scopes != pending_scopes) {
            inst.set_private(SCOPES_ATTR, pending_scopes);
        }
        if !evolution_input.is_none() && inst.private(EVOLUTION_ATTR).unwrap_or(Arg::None).is_none()
        {
            inst.set_private(EVOLUTION_ATTR, evolution_input);
        }
        Ok(())
    }

    fn getattr(&self, inst: &ModelInstance, name: &str) -> Option<Result<Arg>> {
        Some(Ok(match name {
            "global_id" => Arg::Str(global_id_of(inst).into()),
            "uuid" => inst.private(UUID_ATTR).unwrap_or(Arg::None),
            "scopes" => inst.private(SCOPES_ATTR).unwrap_or(Arg::None),
            "evolution" => inst.private(EVOLUTION_ATTR).unwrap_or(Arg::None),
            "_DEFAULT_SCOPES" => strs_to_arg(&default_scopes_of(inst.class)),
            _ => return None,
        }))
    }

    fn setattr(&self, inst: &ModelInstance, name: &str, value: Arg) -> Option<Result<()>> {
        match name {
            "global_id" => Some((|| {
                let parsed = process_global_id_arg(&value)?;
                inst.set_private(UUID_ATTR, Arg::Str(parsed.uuid.into()));
                inst.set_private(SCOPES_ATTR, strs_to_arg(&parsed.scopes));
                inst.set_private(EVOLUTION_ATTR, parsed.evolution.into_arg());
                Ok(())
            })()),
            "uuid" => {
                inst.set_private(UUID_ATTR, value);
                Some(Ok(()))
            }
            "scopes" => {
                inst.set_private(SCOPES_ATTR, value);
                Some(Ok(()))
            }
            "evolution" => {
                inst.set_private(EVOLUTION_ATTR, value);
                Some(Ok(()))
            }
            _ => None,
        }
    }

    fn call_method(&self, inst: &ModelInstance, name: &str, args: Args) -> Option<Result<Arg>> {
        let id = identity_of(inst);
        Some(match name {
            "has_evolution" => Ok(Arg::Bool(id.has_evolution())),
            "identity" => Ok(id.identity()),
            "identity_as_json" => id.identity_as_json().map(|s| Arg::Str(s.into())),
            "from_global_id" => {
                let gid = args.positional.first().cloned().unwrap_or(Arg::None);
                _LAILA_IDENTIFIABLE_OBJECT::from_global_id_on(inst.class, gid)
            }
            "resolve_global_id" => {
                _LAILA_IDENTIFIABLE_OBJECT::resolve_global_id_on(inst.class, args)
                    .map(|s| Arg::Str(s.into()))
            }
            "to_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .to_global_id(args)
                .map(|s| Arg::Str(s.into())),
            "is_laila_resource" => _LAILA_IDENTIFIABLE_OBJECT
                .is_laila_resource(args.positional.first().cloned().unwrap_or(Arg::None))
                .map(Arg::Bool),
            "get_scopes_from_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .get_scopes_from_global_id(args.positional.first().cloned().unwrap_or(Arg::None)),
            "get_uuid_from_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .get_uuid_from_global_id(args.positional.first().cloned().unwrap_or(Arg::None)),
            "get_evolution_from_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .get_evolution_from_global_id(
                    args.positional.first().cloned().unwrap_or(Arg::None),
                ),
            "get_attributes_from_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .get_attributes_from_global_id(
                    args.positional.first().cloned().unwrap_or(Arg::None),
                ),
            "strip_global_id_attributes" => _LAILA_IDENTIFIABLE_OBJECT
                .strip_global_id_attributes(args.positional.first().cloned().unwrap_or(Arg::None)),
            "process_global_id" => _LAILA_IDENTIFIABLE_OBJECT
                .process_global_id(args.positional.first().cloned().unwrap_or(Arg::None)),
            "type" => _LAILA_IDENTIFIABLE_OBJECT
                .r#type(args.positional.first().cloned().unwrap_or(Arg::None)),
            "generate_uuid_from_nickname" => _LAILA_IDENTIFIABLE_OBJECT
                .generate_uuid_from_nickname(args.positional.first().cloned().unwrap_or(Arg::None))
                .map(|s| Arg::Str(s.into())),
            _ => return None,
        })
    }

    fn property_setters(&self) -> &'static [&'static str] {
        &["evolution", "global_id", "scopes", "uuid"]
    }
    fn method_names(&self) -> &'static [&'static str] {
        &[
            "from_global_id",
            "resolve_global_id",
            "to_global_id",
            "is_laila_resource",
            "get_scopes_from_global_id",
            "get_uuid_from_global_id",
            "get_evolution_from_global_id",
            "get_attributes_from_global_id",
            "strip_global_id_attributes",
            "has_evolution",
            "process_global_id",
            "type",
            "identity",
            "identity_as_json",
            "generate_uuid_from_nickname",
        ]
    }
    fn str(&self, inst: &ModelInstance) -> Option<String> {
        Some(global_id_of(inst))
    }
    fn repr(&self, inst: &ModelInstance) -> Option<String> {
        Some(global_id_of(inst))
    }
    fn eq(&self, inst: &ModelInstance, other: &Arg) -> Option<bool> {
        let o = ModelInstance::downcast(other)?;
        if !o.class.is_subclass("_LAILA_IDENTIFIABLE_OBJECT") {
            // ``NotImplemented`` -> Python falls back to identity comparison.
            return Some(core::ptr::eq(inst, o));
        }
        Some(global_id_of(inst) == global_id_of(o))
    }
    fn hash(&self, inst: &ModelInstance) -> Option<Result<i64>> {
        Some(Arg::Str(global_id_of(inst).into()).__hash__())
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_IDENTIFIABLE_OBJECT",
    module: "laila.basics.definitions.identifiable_object",
    bases: alloc::vec![_LAILA_OBJECT::__class__()],
    fields: Vec::new(),
    private_attrs: alloc::vec![
        (
            String::from(UUID_ATTR),
            PrivateAttrInfo {
                default: Some(Arg::None),
                default_factory: None,
            }
        ),
        (
            String::from(SCOPES_ATTR),
            PrivateAttrInfo {
                default: Some(Arg::None),
                default_factory: None,
            }
        ),
        (
            String::from(EVOLUTION_ATTR),
            PrivateAttrInfo {
                default: Some(Arg::None),
                default_factory: None,
            }
        ),
    ],
    class_vars: alloc::vec![(
        String::from("_DEFAULT_SCOPES"),
        alloc::vec![Arg::Str(_OBJECT_SCOPE.into())].into_arg()
    )],
    hooks: Some(&HOOKS),
});

impl ModelBase for _LAILA_IDENTIFIABLE_OBJECT {
    fn __class__() -> &'static ModelClass {
        &CLASS
    }
}

fn str_arg(a: &Arg, what: &str) -> Result<String> {
    match a {
        Arg::Str(s) => Ok(s.to_string()),
        other => Err(LailaError::type_error(alloc::format!(
            "{what} must be a string, got {}",
            other.type_name()
        ))),
    }
}

impl _LAILA_IDENTIFIABLE_OBJECT {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }

    /// `_LAILA_IDENTIFIABLE_OBJECT(uuid=None, scopes=None, evolution=None, nickname=None)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args.into())
    }

    /// `cls.from_global_id(global_id)` on the base class.
    pub fn from_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        Self::from_global_id_on(&CLASS, global_id.into_arg())
    }

    /// `cls.from_global_id(global_id)` for any identifiable model class.
    pub fn from_global_id_on(cls: &'static ModelClass, global_id: Arg) -> Result<Arg> {
        let parsed = process_global_id_arg(&global_id)?;
        let mut a = Args::new();
        a.push_keyword("uuid", parsed.uuid);
        a.push_keyword("scopes", strs_to_arg(&parsed.scopes));
        a.push_keyword("evolution", parsed.evolution);
        ModelInstance::construct(cls, a)
    }

    /// `cls.resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)`.
    #[pysig("resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)")]
    pub fn resolve_global_id(&self, args: impl Into<Args>) -> Result<String> {
        let _ = (
            &r#ref,
            &evolution,
            &prefix_scopes,
            &default_scopes,
            &parse_evolution,
        );
        let mut a = Args::new();
        a.push_positional(r#ref);
        a.push_keyword("evolution", evolution);
        a.push_keyword("prefix_scopes", prefix_scopes);
        a.push_keyword("default_scopes", default_scopes);
        a.push_keyword("parse_evolution", parse_evolution);
        Self::resolve_global_id_on(&CLASS, a)
    }

    /// `cls.resolve_global_id(..)` for any identifiable model class (its
    /// `_DEFAULT_SCOPES` decides the scope-less fallback).
    pub fn resolve_global_id_on(cls: &'static ModelClass, args: Args) -> Result<String> {
        let reference = args
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| args.get("ref"));
        let reference = match &reference {
            Arg::Str(s) => s.to_string(),
            other => {
                return Err(LailaError::value_error(alloc::format!(
                    "global id reference must be a string, got {}",
                    other.type_name()
                )))
            }
        };
        let evolution = evolution_of(&args.get("evolution"))?;
        let prefix: Option<Vec<String>> = match args.get("prefix_scopes") {
            Arg::None => None,
            s => Some(arg_to_strs(&s)?),
        };
        let defaults: Option<Vec<String>> = match args.get("default_scopes") {
            Arg::None => None,
            s => Some(arg_to_strs(&s)?),
        };
        let parse_evolution = match args.get("parse_evolution") {
            Arg::None => true,
            v => v.__bool__(),
        };
        let prefix_ref: Option<Vec<&str>> = prefix
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        let defaults_ref: Option<Vec<&str>> = defaults
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        let class_defaults = default_scopes_of(cls);
        let class_defaults_ref: Vec<&str> = class_defaults.iter().map(String::as_str).collect();
        resolve_global_id_for(
            &class_defaults_ref,
            &reference,
            &ResolveOptions {
                evolution,
                prefix_scopes: prefix_ref.as_deref(),
                default_scopes: defaults_ref.as_deref(),
                parse_evolution,
            },
        )
    }

    /// `to_global_id(uuid=None, scopes=None, evolution=None, nickname=None)`.
    #[pysig("to_global_id(uuid=None, scopes=None, evolution=None, nickname=None)")]
    pub fn to_global_id(&self, args: impl Into<Args>) -> Result<String> {
        let scopes: Option<Vec<String>> = match &scopes {
            Arg::None => None,
            s => Some(arg_to_strs(s)?),
        };
        let evolution_text = match &evolution {
            Arg::None => None,
            e => Some(e.__str__()),
        };
        let uuid_text = match &uuid {
            Arg::None => None,
            u => Some(u.__str__()),
        };
        let base = to_global_id(
            uuid_text.as_deref(),
            scopes.as_deref(),
            None,
            opt_str(&nickname).as_deref(),
        );
        Ok(match evolution_text {
            None => base,
            Some(e) => alloc::format!("{base}@{EVOLUTION_ATTRIBUTE}={e}"),
        })
    }

    /// `is_laila_resource(global_id)`
    pub fn is_laila_resource(&self, global_id: impl IntoArg) -> Result<bool> {
        let g = str_arg(
            &global_id.into_arg(),
            "expected string or bytes-like object",
        )?;
        Ok(is_laila_resource(&g))
    }

    /// `get_scopes_from_global_id(global_id)`
    pub fn get_scopes_from_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        Ok(strs_to_arg(
            &process_global_id_arg(&global_id.into_arg())?.scopes,
        ))
    }

    /// `get_uuid_from_global_id(global_id)`
    pub fn get_uuid_from_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        Ok(Arg::Str(
            process_global_id_arg(&global_id.into_arg())?.uuid.into(),
        ))
    }

    /// `get_evolution_from_global_id(global_id)`
    pub fn get_evolution_from_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        Ok(process_global_id_arg(&global_id.into_arg())?
            .evolution
            .into_arg())
    }

    /// `get_attributes_from_global_id(global_id)` -> `{key: value}`
    pub fn get_attributes_from_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        let g = global_id.into_arg();
        let g = match &g {
            Arg::Str(s) => s.to_string(),
            other => {
                return Err(LailaError::value_error(alloc::format!(
                    "global id reference must be a string, got {}",
                    other.type_name()
                )))
            }
        };
        let mut d = PyDictBuilder::new();
        for (k, v) in get_attributes_from_global_id(&g)? {
            d.insert(k, v);
        }
        Ok(d.finish())
    }

    /// `strip_global_id_attributes(global_id)`
    pub fn strip_global_id_attributes(&self, global_id: impl IntoArg) -> Result<Arg> {
        let g = str_arg(&global_id.into_arg(), "global id")?;
        Ok(Arg::Str(strip_global_id_attributes(&g).into()))
    }

    /// `process_global_id(global_id)` -> `{"uuid", "scopes", "evolution"}`.
    pub fn process_global_id(&self, global_id: impl IntoArg) -> Result<Arg> {
        Ok(process_global_id_arg(&global_id.into_arg())?.to_dict())
    }

    /// `type(x)` -> `"variable"` / `"constant"`.
    #[allow(non_snake_case)]
    pub fn r#type(&self, x: impl IntoArg) -> Result<Arg> {
        let p = process_global_id_arg(&x.into_arg())?;
        Ok(Arg::Str(
            if p.evolution.is_some() {
                "variable"
            } else {
                "constant"
            }
            .into(),
        ))
    }

    /// `generate_uuid_from_nickname(nickname)`
    pub fn generate_uuid_from_nickname(&self, nickname: impl IntoArg) -> Result<String> {
        // `uuid.uuid5(ns, None)` is Python's `TypeError`.
        match nickname.into_arg() {
            Arg::Str(s) => Ok(generate_uuid_from_nickname(&s)),
            other => Err(LailaError::type_error(alloc::format!(
                "nickname must be a string, got {}",
                other.type_name()
            ))),
        }
    }
}

impl _LAILA_IDENTIFIABLE_OBJECT {
    /// `R = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id`: a classmethod
    /// read as a value is a bound callable.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        use crate::python::object::callable;
        let this = *self;
        Ok(match name {
            "resolve_global_id" => callable(move |a: Args| this.resolve_global_id(a)),
            "to_global_id" => callable(move |a: Args| this.to_global_id(a)),
            "from_global_id" => callable(move |a: Args| {
                this.from_global_id(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            "generate_uuid_from_nickname" => callable(move |a: Args| {
                this.generate_uuid_from_nickname(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            "is_laila_resource" => callable(move |a: Args| {
                this.is_laila_resource(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            "process_global_id" => callable(move |a: Args| {
                this.process_global_id(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            "strip_global_id_attributes" => callable(move |a: Args| {
                this.strip_global_id_attributes(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            "type" => callable(move |a: Args| {
                this.r#type(a.positional.first().cloned().unwrap_or(Arg::None))
            }),
            other => CLASS.as_pyclass().into_arg().__getattr__(other)?,
        })
    }
}

impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_OBJECT {
    /// `_LAILA_IDENTIFIABLE_OBJECT(**data)` as a call on the class value.
    fn call_value(&self, args: Args) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args)
    }
}

impl crate::python::PyArg for _LAILA_IDENTIFIABLE_OBJECT {
    fn to_arg(&self) -> Arg {
        CLASS.as_pyclass().into_arg()
    }
}
impl crate::python::builtins::IsInstance for _LAILA_IDENTIFIABLE_OBJECT {
    fn isinstance(x: &Arg) -> bool {
        crate::_compat::pydantic::isinstance_model(x, "_LAILA_IDENTIFIABLE_OBJECT")
    }
}
