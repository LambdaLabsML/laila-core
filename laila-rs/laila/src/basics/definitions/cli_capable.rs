//! CLI-capable base class with 4-tier parameter resolution.
//!
//! Implements the *configurability* contract that every "first-class" laila
//! object (policies, taskforces, pools, comm protocols, the logger) opts
//! into by inheriting from `_LAILA_CLI_CAPABLE_CLASS`. The class hooks into
//! the model `before`-validator so that, during construction, missing
//! constructor arguments are auto-filled from `laila.args` in this order:
//!
//! 1. **Explicit kwarg** -- `MyClass(param=value)` always wins.
//! 2. **laila.args lookup** -- a value found at the corresponding
//!    `laila.args.policy.central.command.taskforces.<gid>.<param>`-style
//!    path is used. Paths are derived automatically from the class's
//!    `_scopes` PrivateAttr; see [`_SCOPE_TO_ARGS_PATH`] for the mapping.
//! 3. **Pydantic default** -- the field's `default=` / `default_factory`.
//! 4. **Required-field check** -- fields listed in `_cli_required_fields`
//!    that are still `None` after the above raise `RuntimeError`.
//!
//! Mark a field as [`CLIExempt`] to keep it out of both the `laila.args`
//! injection step and the environment mirror produced by
//! [`build_environment`].
//!
//! The environment mirror lives at `laila.args.environment.policies[<gid>]`
//! and is kept current *lazily*: every successful CLI-capable construction
//! marks its owning policy dirty ([`_mark_environment_dirty`], O(1)) and the
//! dump is produced by [`_flush_environment_mirror`] the next time
//! `laila.args.environment` is read, or by an explicit
//! [`_refresh_args_environment`].
//!
//! `laila.args` itself (`_LailaArgs`, the DotMap with the `environment`
//! hooks) and the process-wide policy registry this module consults
//! (`laila._local_policies`, `laila._active_policy_gid`) live here as well
//! until the policy phase lands: [`args`], [`_local_policies`],
//! [`_active_policy_gid`]. The policy rebuild of [`_load_environment`]
//! (`DefaultPolicy()`, taskforce / pool / protocol class lookup) is behind
//! the [`EnvironmentBackend`] hook the policy port registers; the
//! validation front half is ported here.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::dotmap::DotMap;
use crate::_compat::pydantic::{
    FieldInfo, Kwargs, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance,
};
use crate::python::sync::{Mutex, RwLock};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyDictBuilder, PyObject, Result};

use super::identifiable_object::{_init_pending, to_global_id};

// ---------------------------------------------------------------------------
// Process state: ``laila.args``, dirty set, policy registry

static ARGS: RwLock<Option<Arg>> = RwLock::new(None);

/// Owning-policy gids whose mirror entry is stale (see module docstring).
static DIRTY_POLICY_GIDS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Re-entrancy flag of the mirror flush (`_FLUSH_STATE.active`).
static _FLUSH_ACTIVE: RwLock<bool> = RwLock::new(false);

/// `laila.args`: the process-wide `_LailaArgs` (created on first access).
pub fn args() -> Arg {
    if let Some(a) = ARGS.read().as_ref() {
        return a.clone();
    }
    let fresh = crate::_LailaArgs::default().into_arg();
    let mut g = ARGS.write();
    if g.is_none() {
        *g = Some(fresh);
    }
    g.as_ref().cloned().unwrap_or(Arg::None)
}

/// `laila.args = value` (also what `unittest.mock.patch("laila.args", ..)`
/// does); returns the previous value.
pub fn _replace_args(new: Arg) -> Arg {
    let old = args();
    *ARGS.write() = Some(new);
    old
}

/// `laila._local_policies` snapshot (gid -> policy). The registry itself
/// lives in `crate::policy` (`activate_policy` / `terminate` own it).
pub fn _local_policies() -> Vec<(String, Arg)> {
    crate::policy::_local_policies()
        .into_iter()
        .map(|(g, p)| (g, p.into_arg()))
        .collect()
}

/// `laila._local_policies.get(gid)`.
pub fn _local_policy(gid: &str) -> Option<Arg> {
    crate::policy::_local_policies()
        .into_iter()
        .find(|(g, _)| g == gid)
        .map(|(_, p)| p.into_arg())
}

/// `laila._local_policies[gid] = policy` (registered, not activated).
pub fn _register_local_policy(gid: &str, policy: Arg) {
    match policy.downcast_ref::<crate::policy::Policy>() {
        Some(p) => crate::policy::_register_local_policy(p),
        None => {
            let _ = gid;
        }
    }
}

/// `laila._live_policies.get(gid)`.
pub fn _live_policy(gid: &str) -> Option<Arg> {
    crate::policy::_live_policies()
        .into_iter()
        .find(|(g, _)| g == gid)
        .map(|(_, p)| p.into_arg())
}

/// `laila._active_policy_gid`.
pub fn _active_policy_gid() -> Option<String> {
    crate::policy::_active_policy_gid()
}

// ---------------------------------------------------------------------------
// CLIExempt

/// Drop-in replacement for `pydantic.Field` that marks a field CLI-exempt.
///
/// Exempt fields are skipped by both the `laila.args` auto-injection step
/// and the environment mirror produced by [`build_environment`]. The marker
/// is `cli_exempt=True` inside `json_schema_extra`.
#[allow(non_snake_case)]
pub fn CLIExempt(args: impl Into<Args>) -> Result<FieldInfo> {
    let mut a: Args = args.into();
    let extra = a.get("json_schema_extra");
    let extra = if extra.__bool__() {
        extra
    } else {
        crate::python::builtins::dict(Args::new())?
    };
    extra.__setitem__("cli_exempt", Arg::Bool(true))?;
    a.keywords.retain(|(k, _)| k != "json_schema_extra");
    a.push_keyword("json_schema_extra", extra);
    crate::_compat::pydantic::Field(a)
}

/// `CLIExempt(..)` spelled as a class (a `laila::python!` body lowers it to `CLIExempt::new(..)`).
#[allow(non_camel_case_types)]
pub struct CLIExempt {}
impl CLIExempt {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<FieldInfo> {
        self::CLIExempt(args)
    }
}

/// `True` if a `FieldInfo` carries the CLI-exempt marker (typed form).
pub fn is_cli_exempt(field_info: &FieldInfo) -> bool {
    if let Arg::Dict(_) = &field_info.json_schema_extra {
        return field_info
            .json_schema_extra
            .get(Args::from("cli_exempt"))
            .map(|v| v.__bool__())
            .unwrap_or(false);
    }
    false
}

/// `_is_cli_exempt(field_info)` -> `True` if a `FieldInfo` carries the
/// CLI-exempt marker (reads the `cli_exempt` flag stashed by [`CLIExempt`]
/// on the field's `json_schema_extra` dict).
#[pysig("_is_cli_exempt(field_info)")]
pub fn _is_cli_exempt(args: impl Into<Args>) -> Result<Arg> {
    let extra = field_info.__getattr__("json_schema_extra")?;
    if let Arg::Dict(_) = &extra {
        return Ok(Arg::Bool(extra.get(Args::from("cli_exempt"))?.__bool__()));
    }
    Ok(Arg::Bool(false))
}

/// Scope -> `laila.args` path template.
pub const _SCOPE_TO_ARGS_PATH: &[(&str, &str)] = &[
    ("POLICY", "policy"),
    ("CENTRAL_COMMAND", "policy.central.command"),
    ("CENTRAL_MEMORY", "policy.central.memory"),
    ("CENTRAL_COMMUNICATION", "policy.central.communication"),
    ("POOL", "policy.central.memory.pools.{global_id}"),
    ("POOL_ROUTER", "policy.central.memory.pool_router"),
    (
        "TASK_FORCE",
        "policy.central.command.taskforces.{global_id}",
    ),
    (
        "COMM_PROTOCOL",
        "policy.central.communication.connections.{global_id}",
    ),
    ("LOGGER", "logger"),
];

/// `_SCOPE_TO_ARGS_PATH.get(scope)`.
pub fn scope_args_path(scope: &str) -> Option<&'static str> {
    _SCOPE_TO_ARGS_PATH
        .iter()
        .find(|(s, _)| *s == scope)
        .map(|(_, p)| *p)
}

/// `subtree.get(part) if hasattr(subtree, "get") else None`.
fn dyn_get(subtree: &Arg, key: &str) -> Arg {
    match subtree {
        Arg::Dict(_) => subtree.get(Args::from(key)).unwrap_or(Arg::None),
        Arg::Object(_) => match subtree.__getattr__("get") {
            Ok(getter) => getter.__call__(Args::from(key)).unwrap_or(Arg::None),
            Err(_) => Arg::None,
        },
        _ => Arg::None,
    }
}

/// `hasattr(x, "keys") and len(x) == 0`
fn is_empty_mapping(x: &Arg) -> bool {
    match x {
        Arg::Dict(_) => x.__len__().map(|n| n == 0).unwrap_or(false),
        Arg::Object(_) => {
            x.__getattr__("keys").is_ok() && x.__len__().map(|n| n == 0).unwrap_or(false)
        }
        _ => false,
    }
}

/// `len(x)` through the dynamic protocol (`DotMap`, `dict`, ...).
fn dyn_len(x: &Arg) -> Option<usize> {
    match x {
        Arg::Dict(_) | Arg::List(_) | Arg::Tuple(_) => x.__len__().ok(),
        Arg::Object(_) => {
            if let Some(dm) = DotMap::downcast(x) {
                return Some(dm.__len__());
            }
            if let Some(la) = x.downcast_ref::<crate::_LailaArgs>() {
                return Some(la.0.__len__());
            }
            None
        }
        _ => None,
    }
}

/// Traverse `laila.args` using `.get()` to avoid DotMap auto-creation.
pub fn _get_args_subtree(path: &str) -> Option<Arg> {
    let mut subtree = args();
    for part in path.split('.') {
        if subtree.is_none() {
            return None;
        }
        subtree = dyn_get(&subtree, part);
    }
    if !subtree.is_none() && is_empty_mapping(&subtree) {
        return None;
    }
    if subtree.is_none() {
        None
    } else {
        Some(subtree)
    }
}

/// Compute a global_id from constructor data for `{global_id}` path
/// templates: explicit `data["global_id"]`, else `data["uuid"]`, else the
/// uuid staged by `_LAILA_IDENTIFIABLE_OBJECT.__init__`, framed with the
/// class's `_scopes` default.
pub fn _resolve_global_id_for_path(cls: &'static ModelClass, data: &Kwargs) -> Option<String> {
    if let Some(g) = data.get("global_id") {
        return Some(g.__str__());
    }
    let mut uuid_val = data.get("uuid").cloned().filter(|u| !u.is_none());
    if uuid_val.is_none() {
        uuid_val = _init_pending().uuid.map(|u| Arg::Str(u.into()));
    }
    let uuid_val = uuid_val?;
    let scopes_attr = cls.private_attr("_scopes")?;
    let scopes: Vec<String> = match scopes_attr.default_factory {
        Some(_) => scopes_attr
            .get_default()
            .ok()
            .flatten()
            .and_then(|v| v.__iter__().ok())
            .map(|v| v.iter().map(|s| s.__str__()).collect())
            .unwrap_or_else(|| alloc::vec!["OBJECT".to_string()]),
        None => alloc::vec!["OBJECT".to_string()],
    };
    Some(to_global_id(
        Some(&uuid_val.__str__()),
        Some(&scopes),
        None,
        None,
    ))
}

/// The before-validator for a CLI-capable class that is a Rust handle
/// rather than a `ModelInstance` (a taskforce, a transport): fill the
/// constructor keywords *not given explicitly* from the `laila.args`
/// subtree of the class -- `scope` picks the path template, a
/// `{global_id}` template is keyed by `global_id=` / `uuid=` (so a
/// rebuilt object with the same uuid re-reads its dumped config).
/// `eligible` is the class's public, non-exempt field list.
pub fn resolve_cli_fields_into(scope: &str, eligible: &[&str], args: &mut Args) {
    let Some(template) = scope_args_path(scope) else {
        return;
    };
    let path = if template.contains("{global_id}") {
        let gid = match args.get("global_id") {
            Arg::None => match args.get("uuid") {
                Arg::None => return,
                u => to_global_id(Some(&u.__str__()), Some(&[String::from(scope)]), None, None),
            },
            g => g.__str__(),
        };
        template.replace("{global_id}", &gid)
    } else {
        String::from(template)
    };
    let Some(subtree) = _get_args_subtree(&path) else {
        return;
    };
    for field in eligible {
        if args.keywords.iter().any(|(k, _)| k == field) {
            continue;
        }
        let val = dyn_get(&subtree, field);
        if val.is_none() || DotMap::downcast(&val).is_some_and(|d| d.__len__() == 0) {
            continue;
        }
        args.push_keyword(field, val);
    }
}

/// Read the first scope from a class's `_scopes` PrivateAttr default.
pub fn _scope_for_class(cls: &'static ModelClass) -> Option<String> {
    let scopes_attr = cls.private_attr("_scopes")?;
    scopes_attr.default_factory.as_ref()?;
    let v = scopes_attr.get_default().ok()??;
    let items = v.__iter__().ok()?;
    items.first().map(|s| s.__str__())
}

/// Determine the `laila.args` path for a CLI-capable class instance.
pub fn _resolve_path_for_class(cls: &'static ModelClass, data: &Kwargs) -> Option<String> {
    let scope = _scope_for_class(cls)?;
    let template = scope_args_path(&scope)?;
    if template.contains("{global_id}") {
        let gid = _resolve_global_id_for_path(cls, data)?;
        return Some(template.replace("{global_id}", &gid));
    }
    Some(template.to_string())
}

fn class_of(cls: &Arg) -> Result<&'static ModelClass> {
    if let Some(c) = crate::python::PyClass::downcast(cls) {
        if let Some(m) = c.model {
            return Ok(m);
        }
        return Err(LailaError::attribute_error(alloc::format!(
            "type object '{}' has no attribute 'model_fields'",
            c.name
        )));
    }
    if let Some(inst) = ModelInstance::downcast(cls) {
        return Ok(inst.class);
    }
    Err(LailaError::attribute_error(alloc::format!(
        "'{}' object has no attribute 'model_fields'",
        cls.type_name()
    )))
}

/// Public, non-exempt Pydantic Field names for `cls` (typed form).
pub fn _eligible_model_fields_of(cls: &'static ModelClass) -> Vec<String> {
    cls.all_fields()
        .into_iter()
        .filter(|(name, info)| !name.starts_with('_') && !is_cli_exempt(info))
        .map(|(name, _)| name)
        .collect()
}

/// `_eligible_model_fields(cls)` -> `list[str]`: public, non-exempt Pydantic
/// Field names (accepts a class object or an instance).
#[pysig("_eligible_model_fields(klass)")]
pub fn _eligible_model_fields(args: impl Into<Args>) -> Result<Arg> {
    let names: Vec<String> = match class_of(&klass) {
        Ok(c) => _eligible_model_fields_of(c),
        // A class whose `model_fields` is answered dynamically (a transport
        // class object; an instance of one): the same filter over that dict.
        Err(e) => {
            let fields = klass
                .__getattr__("model_fields")
                .or_else(|_| klass.__getattr__("__class__")?.__getattr__("model_fields"))
                .map_err(|_| e)?;
            let mut out = Vec::new();
            for k in fields.__iter__()? {
                let name = k.__str__();
                if name.starts_with('_') {
                    continue;
                }
                let info = fields.__getitem__(k)?;
                let exempt = info
                    .downcast_ref::<FieldInfo>()
                    .map(is_cli_exempt)
                    .unwrap_or(false);
                if !exempt {
                    out.push(name);
                }
            }
            out
        }
    };
    Ok(names
        .into_iter()
        .map(|n| Arg::Str(n.into()))
        .collect::<Vec<_>>()
        .into_arg())
}

/// Names of public property descriptors that have a setter and are not
/// model fields (`dir()` order, i.e. sorted).
pub fn _eligible_property_setters_of(cls: &'static ModelClass) -> Vec<String> {
    let fields: Vec<String> = cls.all_fields().into_iter().map(|(n, _)| n).collect();
    cls.property_setters()
        .into_iter()
        .filter(|p| !p.starts_with('_') && !fields.iter().any(|f| f == p))
        .map(str::to_string)
        .collect()
}

/// `_eligible_property_setters(cls)` -> `list[str]`.
#[pysig("_eligible_property_setters(klass)")]
pub fn _eligible_property_setters(args: impl Into<Args>) -> Result<Arg> {
    let c = class_of(&klass)?;
    Ok(_eligible_property_setters_of(c)
        .into_iter()
        .map(|n| Arg::Str(n.into()))
        .collect::<Vec<_>>()
        .into_arg())
}

// ---------------------------------------------------------------------------
// Subclass registry (``__subclasses__()`` walk)

static SUBCLASSES: RwLock<Vec<&'static ModelClass>> = RwLock::new(Vec::new());

/// Register a model class so [`_all_subclasses`] / [`_resolve_class`] can
/// find it (Python walks `__subclasses__()`; Rust has no class registry, so
/// `py_model!` and the typed laila classes register themselves).
pub fn register_class(cls: &'static ModelClass) {
    let mut g = SUBCLASSES.write();
    if !g.iter().any(|c| core::ptr::eq(*c, cls)) {
        g.push(cls);
    }
}

/// A flat `{class_name: class}` map of `root` and every registered subclass
/// (last registration wins on a shared name).
pub fn _all_subclasses(root: &'static ModelClass) -> Vec<(String, &'static ModelClass)> {
    let mut out: Vec<(String, &'static ModelClass)> = alloc::vec![(root.name.to_string(), root)];
    for c in SUBCLASSES.read().iter() {
        if !core::ptr::eq(*c, root) && c.is_subclass(root.name) {
            if let Some(slot) = out.iter_mut().find(|(n, _)| n == c.name) {
                slot.1 = c;
            } else {
                out.push((c.name.to_string(), c));
            }
        }
    }
    out
}

/// Look up a subclass of `root` by `__name__` token; `ValueError` listing
/// the known tokens when unknown.
pub fn _resolve_class(token: &str, root: &'static ModelClass) -> Result<&'static ModelClass> {
    let classes = _all_subclasses(root);
    match classes.iter().find(|(n, _)| n == token) {
        Some((_, c)) => Ok(c),
        None => {
            let mut known: Vec<&str> = classes.iter().map(|(n, _)| n.as_str()).collect();
            known.sort_unstable();
            Err(LailaError::value_error(alloc::format!(
                "unknown {} subclass token: {} (known: {})",
                root.name,
                crate::python::pyrepr::str_repr(token),
                known.join(", ")
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// build_environment

/// The pydantic model behind `v`: a model instance, or a pool (`Arg::Pool`
/// carries its model inside).
fn model_of(v: &Arg) -> Option<&ModelInstance> {
    match v {
        Arg::Pool(p) => Some(p.inst()),
        other => ModelInstance::downcast(other),
    }
}

/// The CLI-eligible fields of a typed (non-pydantic) laila object --
/// policy, command, taskforce, memory, router -- via `__cli_fields__`.
fn typed_cli_fields(v: &Arg) -> Option<Vec<(String, Arg)>> {
    match v {
        Arg::Object(o) => o.__cli_fields__(),
        _ => None,
    }
}

/// `hasattr(type(val), "model_fields")`
fn is_model(v: &Arg) -> bool {
    model_of(v).is_some() || typed_cli_fields(v).is_some()
}

/// `_dump_obj(obj)`: the CLI-eligible fields of a model instance (nested
/// models recurse, dict values dump their model items), plus the property
/// setters.
fn _dump_obj(obj: &Arg) -> Result<Arg> {
    let result = crate::python::builtins::dict(Args::new())?;
    let inst = match model_of(obj) {
        Some(i) => i,
        None => {
            if let Some(fields) = typed_cli_fields(obj) {
                for (name, val) in fields {
                    result.__setitem__(name.as_str(), val)?;
                }
            }
            return Ok(result);
        }
    };
    for name in _eligible_model_fields_of(inst.class) {
        let val = obj.__getattr__(&name).unwrap_or(Arg::None);
        if !val.is_none() && is_model(&val) {
            result.__setitem__(name.as_str(), _dump_obj(&val)?)?;
        } else if let Arg::Dict(_) = &val {
            let nested = crate::python::builtins::dict(Args::new())?;
            for k in val.__iter__()? {
                let v = val.__getitem__(k.clone())?;
                if is_model(&v) {
                    nested.__setitem__(k, _dump_obj(&v)?)?;
                } else {
                    nested.__setitem__(k, v)?;
                }
            }
            result.__setitem__(name.as_str(), nested)?;
        } else {
            result.__setitem__(name.as_str(), val)?;
        }
    }
    for name in _eligible_property_setters_of(inst.class) {
        if let Ok(v) = obj.__getattr__(&name) {
            result.__setitem__(name.as_str(), v)?;
        }
    }
    Ok(result)
}

fn add_props(target: &Arg, obj: &Arg) -> Result<()> {
    if let Some(inst) = model_of(obj) {
        for name in _eligible_property_setters_of(inst.class) {
            if let Ok(v) = obj.__getattr__(&name) {
                target.__setitem__(name.as_str(), v)?;
            }
        }
    }
    Ok(())
}

fn getattr_opt(obj: &Arg, name: &str) -> Option<Arg> {
    match obj.__getattr__(name) {
        Ok(v) if !v.is_none() => Some(v),
        _ => None,
    }
}

fn type_name_of(v: &Arg) -> String {
    match model_of(v) {
        Some(i) => i.class_name().to_string(),
        None => v.type_name().to_string(),
    }
}

fn dump_collection(items: &Arg) -> Result<Arg> {
    let out = crate::python::builtins::dict(Args::new())?;
    for k in items.__iter__()? {
        let item = items.__getitem__(k.clone())?;
        let dump = _dump_obj(&item)?;
        add_props(&dump, &item)?;
        dump.__setitem__("class_token", Arg::Str(type_name_of(&item).into()))?;
        out.__setitem__(k, dump)?;
    }
    Ok(out)
}

/// Build a JSON-serializable environment dict from a live policy: walks the
/// policy tree and collects only CLI-eligible fields (public, non-exempt,
/// non-private). This is the same structure `laila.args` accepts.
#[pysig("build_environment(policy)")]
pub fn build_environment(args: impl Into<Args>) -> Result<Arg> {
    let env = crate::python::builtins::dict(Args::new())?;
    let policy_data = _dump_obj(&policy)?;
    add_props(&policy_data, &policy)?;
    env.__setitem__("policy", policy_data.clone())?;

    if let Some(central) = getattr_opt(&policy, "central") {
        if !policy_data.get(Args::from("central"))?.__bool__() {
            policy_data.__setitem__("central", crate::python::builtins::dict(Args::new())?)?;
        }
        let central_env = policy_data.__getitem__("central")?;

        if let Some(cmd) = getattr_opt(&central, "command") {
            let cmd_data = _dump_obj(&cmd)?;
            add_props(&cmd_data, &cmd)?;
            if let Ok(taskforces) = cmd.__getattr__("taskforces") {
                cmd_data.__setitem__("taskforces", dump_collection(&taskforces)?)?;
            }
            central_env.__setitem__("command", cmd_data)?;
        }

        if let Some(mem) = getattr_opt(&central, "memory") {
            let mem_data = _dump_obj(&mem)?;
            add_props(&mem_data, &mem)?;
            if let Some(router) = getattr_opt(&mem, "pool_router") {
                let router_data = _dump_obj(&router)?;
                add_props(&router_data, &router)?;
                if let Ok(pools) = router.__getattr__("pools") {
                    router_data.__setitem__("pools", dump_collection(&pools)?)?;
                }
                mem_data.__setitem__("pool_router", router_data)?;
            }
            central_env.__setitem__("memory", mem_data)?;
        }

        if let Some(comm) = getattr_opt(&central, "communication") {
            let comm_data = _dump_obj(&comm)?;
            add_props(&comm_data, &comm)?;
            if let Ok(connections) = comm.__getattr__("connections") {
                comm_data.__setitem__("connections", dump_collection(&connections)?)?;
            }
            central_env.__setitem__("communication", comm_data)?;
        }
    }
    Ok(env)
}

// ---------------------------------------------------------------------------
// The class

/// Base class enabling 4-tier parameter resolution from `laila.args`.
///
/// Inherit from this alongside `_LAILA_IDENTIFIABLE_OBJECT` to opt into
/// auto-population of constructor kwargs from the matching subtree of
/// `laila.args`, the lazy `laila.args.environment.policies[<gid>]` mirror,
/// and mandatory-field enforcement via `_cli_required_fields`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_CLI_CAPABLE_CLASS;

struct Hooks;

impl ModelHooks for Hooks {
    /// Before-validator that injects values from `laila.args` for the
    /// eligible fields the caller did not pass (empty `DotMap` nodes count
    /// as absent).
    fn before_validate(&self, cls: &'static ModelClass, data: &mut Kwargs) -> Result<()> {
        let path = match _resolve_path_for_class(cls, data) {
            Some(p) => p,
            None => return Ok(()),
        };
        let subtree = match _get_args_subtree(&path) {
            Some(s) => s,
            None => return Ok(()),
        };
        for field_name in _eligible_model_fields_of(cls) {
            if data.contains_key(&field_name) {
                continue;
            }
            let val = dyn_get(&subtree, &field_name);
            if !val.is_none() {
                if DotMap::downcast(&val).is_some_and(|d| d.__len__() == 0) {
                    continue;
                }
                data.insert(field_name, val);
            }
        }
        Ok(())
    }

    /// Post-init: backfill property setters from the same args subtree and
    /// enforce `_cli_required_fields`.
    fn model_post_init(
        &self,
        inst: &ModelInstance,
        _st: &crate::_compat::pydantic::InitState,
    ) -> Result<()> {
        let path = _resolve_path_for_class(inst.class, &Kwargs::default());
        let subtree = path.and_then(|p| _get_args_subtree(&p));
        if let Some(subtree) = subtree {
            for prop_name in _eligible_property_setters_of(inst.class) {
                if prop_name.starts_with('_') {
                    continue;
                }
                let val = dyn_get(&subtree, &prop_name);
                if !val.is_none() {
                    if DotMap::downcast(&val).is_some_and(|d| d.__len__() == 0) {
                        continue;
                    }
                    let _ = PyObject::__setattr__(inst, &prop_name, val);
                }
            }
        }
        let required = inst
            .class
            .class_var("_cli_required_fields")
            .unwrap_or(Arg::None);
        if !required.is_none() {
            for field_name in required.__iter__()? {
                let name = field_name.__str__();
                let current = PyObject::__getattr__(inst, &name).unwrap_or(Arg::None);
                if current.is_none() {
                    return Err(LailaError::runtime_error(alloc::format!(
                        "{}.{name} is required but was not provided explicitly, via laila.args, or as a default.",
                        inst.class_name()
                    )));
                }
            }
        }
        Ok(())
    }

    /// After-validator: mark the owning policy dirty (best-effort).
    fn after_validate(&self, inst: &ModelInstance) -> Result<()> {
        let _ = _mark_environment_dirty_inst(inst);
        Ok(())
    }

    fn method_names(&self) -> &'static [&'static str] {
        &["model_post_init"]
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_CLI_CAPABLE_CLASS",
    module: "laila.basics.definitions.cli_capable",
    bases: Vec::new(),
    fields: Vec::new(),
    private_attrs: Vec::new(),
    class_vars: alloc::vec![(
        String::from("_cli_required_fields"),
        crate::python::builtins::set(Args::new()).unwrap_or(Arg::None)
    )],
    hooks: Some(&HOOKS),
});

impl ModelBase for _LAILA_CLI_CAPABLE_CLASS {
    fn __class__() -> &'static ModelClass {
        &CLASS
    }
}

impl _LAILA_CLI_CAPABLE_CLASS {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }
    /// `_LAILA_CLI_CAPABLE_CLASS(**data)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args.into())
    }
    /// `cls._resolve_from_cli_args(data)` -- the before-validator as a function.
    pub fn _resolve_from_cli_args(cls: &'static ModelClass, data: &mut Kwargs) -> Result<()> {
        HOOKS.before_validate(cls, data)
    }
}

impl crate::python::builtins::CallValue for _LAILA_CLI_CAPABLE_CLASS {
    fn call_value(&self, args: Args) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args)
    }
}

impl crate::python::PyArg for _LAILA_CLI_CAPABLE_CLASS {
    fn to_arg(&self) -> Arg {
        CLASS.as_pyclass().into_arg()
    }
}
impl crate::python::builtins::IsInstance for _LAILA_CLI_CAPABLE_CLASS {
    fn isinstance(x: &Arg) -> bool {
        crate::_compat::pydantic::isinstance_model(x, "_LAILA_CLI_CAPABLE_CLASS")
    }
}

// ---------------------------------------------------------------------------
// Owning policy / environment mirror

/// `isinstance(x, _LAILA_IDENTIFIABLE_POLICY)`.
fn is_policy(x: &Arg) -> bool {
    x.downcast_ref::<crate::policy::Policy>().is_some()
        || crate::_compat::pydantic::isinstance_model(x, "_LAILA_IDENTIFIABLE_POLICY")
}

/// Return the policy that `instance` belongs to, or `None`: the instance
/// itself when it is a policy; else its `policy_id` looked up in
/// `laila._local_policies`; else the active local policy.
pub fn _find_owning_policy(instance: &Arg) -> Option<Arg> {
    if is_policy(instance) {
        return Some(instance.clone());
    }
    if let Some(policy_id) = getattr_opt(instance, "policy_id") {
        let policy_id =
            if crate::_compat::pydantic::isinstance_model(&policy_id, "_LAILA_IDENTIFIABLE_OBJECT")
                || is_policy(&policy_id)
            {
                policy_id.__getattr__("global_id").ok()?.__str__()
            } else {
                policy_id.__str__()
            };
        if let Some(p) = _local_policy(&policy_id) {
            return Some(p);
        }
    }
    if let Some(gid) = _active_policy_gid() {
        if let Some(p) = _local_policy(&gid) {
            return Some(p);
        }
    }
    None
}

/// A shallow copy of `data` (dict-like) without `class_token`.
#[pysig("_strip_class_token(data)")]
pub fn _strip_class_token(args: impl Into<Args>) -> Result<Arg> {
    let data = &data;
    let out = crate::python::builtins::dict(Args::new())?;
    if data.is_none() {
        return Ok(out);
    }
    let data = match DotMap::downcast(data) {
        Some(d) => d.to_dict(),
        None => data.clone(),
    };
    let data = match &data {
        Arg::Dict(_) => data,
        other => match crate::python::builtins::dict(Args::from(other.clone())) {
            Ok(d) => d,
            Err(_) => return Ok(out),
        },
    };
    for k in data.__iter__()? {
        if k == "class_token" {
            continue;
        }
        out.__setitem__(k.clone(), data.__getitem__(k)?)?;
    }
    Ok(out)
}

/// Best-effort conversion of DotMap / nested dicts to plain dicts.
pub fn _coerce_to_plain_dict(value: &Arg) -> Result<Arg> {
    coerce_to_plain_dict(value)
}

fn coerce_to_plain_dict(value: &Arg) -> Result<Arg> {
    if let Some(d) = DotMap::downcast(value) {
        return Ok(d.to_dict());
    }
    if let Some(la) = value.downcast_ref::<crate::_LailaArgs>() {
        return Ok(la.toDict());
    }
    if let Arg::Dict(_) = value {
        let out = crate::python::builtins::dict(Args::new())?;
        for k in value.__iter__()? {
            out.__setitem__(k.clone(), coerce_to_plain_dict(&value.__getitem__(k)?)?)?;
        }
        return Ok(out);
    }
    Ok(value.clone())
}

/// The policy-side half of [`_load_environment`]: what the policy port
/// provides once `DefaultPolicy`, taskforce / pool / protocol classes and
/// `laila.activate_policy` exist. `terminate` is `laila.terminate(wait=True,
/// cancel_pending=False)`; `default_policy` is `DefaultPolicy()`;
/// `build_policy_from_dump` is `_build_policy_from_dump(policy_uuid=..,
/// policy_data=.., seen_proto_gids=..)` (it registers the policy in
/// `laila._local_policies`); `activate_policy` is `laila.activate_policy(p)`.
#[derive(Clone, Copy)]
pub struct EnvironmentBackend {
    pub terminate: fn() -> Result<()>,
    pub default_policy: fn() -> Result<Arg>,
    pub build_policy_from_dump: fn(&str, &Arg, &mut Vec<String>) -> Result<Arg>,
    pub activate_policy: fn(&Arg) -> Result<()>,
    pub shutdown_orphan_policy: fn(&Arg),
}

static BACKEND: RwLock<Option<EnvironmentBackend>> = RwLock::new(None);

/// Register the policy-side operations [`_load_environment`] needs.
pub fn set_environment_backend(backend: EnvironmentBackend) {
    *BACKEND.write() = Some(backend);
}

/// The registered backend, else the crate's own (`crate::policy::environment`).
fn backend_or_err() -> Result<EnvironmentBackend> {
    Ok((*BACKEND.read()).unwrap_or_else(crate::policy::environment::backend))
}

/// Replace the entire laila runtime with the policies described by `env`.
///
/// 1. `env.active_gid` set: must appear in `env.policies`; activate it.
/// 2. `env.policies` empty: create a fresh `DefaultPolicy`, activate it.
/// 3. `env.policies` has exactly one entry: activate it.
/// 4. Otherwise (>=2 policies, no `active_gid`): `ValueError`.
#[pysig("_load_environment(env)")]
pub fn _load_environment(args: impl Into<Args>) -> Result<()> {
    let env = _coerce_to_plain_dict(&env)?;
    if !matches!(env, Arg::Dict(_)) {
        return Err(LailaError::value_error(alloc::format!(
            "environment must be a dict, got {}",
            env.type_name()
        )));
    }
    if !env.__contains__("policies")? {
        return Err(LailaError::value_error(
            "environment must contain a 'policies' key",
        ));
    }
    let policies_dump = env.get(Args::from("policies"))?;
    let policies_dump = if policies_dump.__bool__() {
        policies_dump
    } else {
        crate::python::builtins::dict(Args::new())?
    };
    if !matches!(policies_dump, Arg::Dict(_)) {
        return Err(LailaError::value_error(alloc::format!(
            "environment.policies must be a dict, got {}",
            policies_dump.type_name()
        )));
    }
    let active_gid = env.get(Args::from("active_gid"))?;
    if !active_gid.is_none() && policies_dump.__len__()? == 0 {
        return Err(LailaError::value_error(
            "active_gid is set but environment.policies is empty",
        ));
    }

    let backend = backend_or_err()?;
    (backend.terminate)()?;

    if policies_dump.__len__()? == 0 {
        let new_policy = (backend.default_policy)()?;
        return (backend.activate_policy)(&new_policy);
    }

    let mut seen_uuids: Vec<String> = Vec::new();
    let mut seen_proto_gids: Vec<String> = Vec::new();
    let mut built_policies: Vec<Arg> = Vec::new();

    let mut build = || -> Result<()> {
        for policy_gid in policies_dump.__iter__()? {
            let policy_data =
                _coerce_to_plain_dict(&policies_dump.__getitem__(policy_gid.clone())?)?;
            if !matches!(policy_data, Arg::Dict(_)) {
                return Err(LailaError::value_error(alloc::format!(
                    "policy entry for {} must be a dict, got {}",
                    policy_gid.__repr__(),
                    policy_data.type_name()
                )));
            }
            let ident =
                super::identifiable_object::process_global_id_arg(&policy_gid).map_err(|e| {
                    LailaError::value_error(alloc::format!(
                        "invalid policy gid {}: {}",
                        policy_gid.__repr__(),
                        e.__str__()
                    ))
                })?;
            if seen_uuids.contains(&ident.uuid) {
                return Err(LailaError::value_error(alloc::format!(
                    "duplicate policy uuid {} in environment.policies",
                    crate::python::pyrepr::str_repr(&ident.uuid)
                )));
            }
            seen_uuids.push(ident.uuid.clone());
            let policy =
                (backend.build_policy_from_dump)(&ident.uuid, &policy_data, &mut seen_proto_gids)?;
            let built_gid = policy.__getattr__("global_id")?.__str__();
            if built_gid != policy_gid.__str__() {
                (backend.shutdown_orphan_policy)(&policy);
                return Err(LailaError::value_error(alloc::format!(
                    "reconstructed policy gid {} does not match dumped key {}",
                    crate::python::pyrepr::str_repr(&built_gid),
                    policy_gid.__repr__()
                )));
            }
            _register_local_policy(&built_gid, policy.clone());
            built_policies.push(policy);
        }
        Ok(())
    };
    if let Err(e) = build() {
        let _ = (backend.terminate)();
        return Err(e);
    }

    let chosen = if !active_gid.is_none() {
        let active_gid = active_gid.__str__();
        if !policies_dump.__contains__(active_gid.as_str())? {
            return Err(LailaError::value_error(alloc::format!(
                "active_gid {} not present in environment.policies",
                crate::python::pyrepr::str_repr(&active_gid)
            )));
        }
        _local_policy(&active_gid)
            .ok_or_else(|| LailaError::key_error(crate::python::pyrepr::str_repr(&active_gid)))?
    } else if built_policies.len() == 1 {
        built_policies[0].clone()
    } else {
        return Err(LailaError::value_error(
            "active_gid required when env contains multiple policies",
        ));
    };
    (backend.activate_policy)(&chosen)
}

/// Best-effort tear-down of `policy` and all its sub-resources through the
/// dynamic protocol (`central.communication.stop()`,
/// `central.command.shutdown(wait=True, cancel_pending=False)`, every
/// pool's `close()`).
pub fn _shutdown_orphan_policy(policy: &Arg) {
    let central = getattr_opt(policy, "central");
    if let Some(central) = &central {
        if let Some(comm) = getattr_opt(central, "communication") {
            let _ = comm.__call_method__("stop", Args::new());
        }
        if let Some(cmd) = getattr_opt(central, "command") {
            let mut a = Args::new();
            a.push_keyword("wait", true);
            a.push_keyword("cancel_pending", false);
            let _ = cmd.__call_method__("shutdown", a);
        }
        if let Some(mem) = getattr_opt(central, "memory") {
            if let Some(router) = getattr_opt(&mem, "pool_router") {
                if let Some(pools) = getattr_opt(&router, "pools") {
                    if let Ok(values) = pools.values() {
                        if let Ok(items) = values.__iter__() {
                            for pool in items {
                                let _ = pool.__call_method__("close", Args::new());
                            }
                        }
                    }
                }
            }
        }
    }
}

/// `laila.basics.definitions.cli_capable._DIRTY_POLICY_GIDS` as a value: a
/// snapshot `set` of the owning-policy gids whose mirror entry is stale.
#[allow(non_snake_case)]
pub fn _DIRTY_POLICY_GIDS() -> Arg {
    let g = DIRTY_POLICY_GIDS.lock();
    let mut s = crate::python::object::PySetInner::default();
    for gid in g.iter() {
        s.insert(Arg::Str(gid.as_str().into()));
    }
    Arg::Set(Arc::new(RwLock::new(s)))
}

fn mark_dirty(gid: String) {
    let mut g = DIRTY_POLICY_GIDS.lock();
    if !g.contains(&gid) {
        g.push(gid);
    }
}

fn _mark_environment_dirty_inst(inst: &ModelInstance) -> Result<()> {
    if _scope_for_class(inst.class).as_deref() == Some("LOGGER") {
        return _refresh_args_environment_inst(inst);
    }
    if *_FLUSH_ACTIVE.read() {
        return Ok(());
    }
    // The instance is still being constructed (not yet behind an `Arc`):
    // resolve the owning policy through its identity / policy_id only.
    let policy_id = inst.field("policy_id").filter(|p| !p.is_none());
    let owner = if inst.class.is_subclass("_LAILA_IDENTIFIABLE_POLICY") {
        Some(super::identifiable_object::identity_of(inst).global_id())
    } else if let Some(pid) = policy_id {
        let pid = pid.__str__();
        _local_policy(&pid).map(|_| pid).or_else(_active_policy_gid)
    } else {
        _active_policy_gid()
    };
    if let Some(gid) = owner {
        if _local_policy(&gid).is_some() || inst.class.is_subclass("_LAILA_IDENTIFIABLE_POLICY") {
            mark_dirty(gid);
        }
    }
    Ok(())
}

/// Record that `instance`'s owning policy needs a fresh mirror entry
/// (O(1); the dump is deferred to [`_flush_environment_mirror`]). The
/// logger branch has no owning policy and is written immediately.
#[pysig("_mark_environment_dirty(instance)")]
pub fn _mark_environment_dirty(args: impl Into<Args>) -> Result<Arg> {
    if let Some(inst) = ModelInstance::downcast(&instance) {
        if _scope_for_class(inst.class).as_deref() == Some("LOGGER") {
            return _refresh_args_environment(Args::from(instance.clone()));
        }
    }
    if *_FLUSH_ACTIVE.read() {
        return Ok(Arg::None);
    }
    if let Some(policy) = _find_owning_policy(&instance) {
        mark_dirty(policy.__getattr__("global_id")?.__str__());
    }
    Ok(Arg::None)
}

/// Write the mirror entry of every dirty policy (at most once each).
/// Re-entrant reads from inside the flush are no-ops; the dirty set is
/// drained before dumping.
pub fn _flush_environment_mirror() -> Result<()> {
    if *_FLUSH_ACTIVE.read() {
        return Ok(());
    }
    let gids: Vec<String> = {
        let mut g = DIRTY_POLICY_GIDS.lock();
        if g.is_empty() {
            return Ok(());
        }
        core::mem::take(&mut *g)
    };
    *_FLUSH_ACTIVE.write() = true;
    for gid in gids {
        let policy = _local_policy(&gid).or_else(|| _live_policy(&gid));
        if let Some(policy) = policy {
            let _ = _refresh_args_environment(Args::from(policy));
        }
    }
    *_FLUSH_ACTIVE.write() = false;
    Ok(())
}

/// Forget every pending mirror update (`terminate`).
pub fn _clear_environment_dirty() {
    DIRTY_POLICY_GIDS.lock().clear();
}

fn ensure_dotmap_slot(parent: &Arg, key: &str) -> Result<Arg> {
    let existing = dyn_get(parent, key);
    if DotMap::downcast(&existing).is_some() {
        return Ok(existing);
    }
    let fresh = DotMap::default().into_arg();
    parent.__setattr__(key, fresh.clone())?;
    Ok(fresh)
}

fn _refresh_args_environment_inst(inst: &ModelInstance) -> Result<()> {
    // logger branch only (an instance mid-construction has no owning policy lookup)
    let was_active = *_FLUSH_ACTIVE.read();
    *_FLUSH_ACTIVE.write() = true;
    let r = (|| -> Result<()> {
        let args = args();
        let env = ensure_dotmap_slot(&args, "environment")?;
        let dump = crate::python::builtins::dict(Args::new())?;
        dump.__setitem__("class_token", Arg::Str(inst.class_name().into()))?;
        for name in _eligible_model_fields_of(inst.class) {
            if let Ok(v) = PyObject::__getattr__(inst, &name) {
                dump.__setitem__(name.as_str(), v)?;
            }
        }
        for name in _eligible_property_setters_of(inst.class) {
            if let Ok(v) = PyObject::__getattr__(inst, &name) {
                dump.__setitem__(name.as_str(), v)?;
            }
        }
        env.__setattr__("logger", DotMap::new(Args::from(dump))?.into_arg())
    })();
    *_FLUSH_ACTIVE.write() = was_active;
    r
}

/// Write the owning policy's [`build_environment`] dump under
/// `laila.args.environment.policies[<gid>]` *now*. For a top-level
/// singleton like the logger, which has no owning policy, the dump is
/// written under `laila.args.environment.logger` instead. The mirror maps
/// are mutated in place so the write cannot trip the environment-load hook.
#[pysig("_refresh_args_environment(instance)")]
pub fn _refresh_args_environment(args: impl Into<Args>) -> Result<Arg> {
    let was_active = *_FLUSH_ACTIVE.read();
    *_FLUSH_ACTIVE.write() = true;
    let r = (|| -> Result<()> {
        let laila_args = self::args();
        let env = ensure_dotmap_slot(&laila_args, "environment")?;
        if let Some(inst) = ModelInstance::downcast(&instance) {
            if _scope_for_class(inst.class).as_deref() == Some("LOGGER") {
                let dump = crate::python::builtins::dict(Args::new())?;
                dump.__setitem__("class_token", Arg::Str(inst.class_name().into()))?;
                for name in _eligible_model_fields_of(inst.class) {
                    if let Ok(v) = instance.__getattr__(&name) {
                        dump.__setitem__(name.as_str(), v)?;
                    }
                }
                for name in _eligible_property_setters_of(inst.class) {
                    if let Ok(v) = instance.__getattr__(&name) {
                        dump.__setitem__(name.as_str(), v)?;
                    }
                }
                return env.__setattr__("logger", DotMap::new(Args::from(dump))?.into_arg());
            }
        }
        let policy = match _find_owning_policy(&instance) {
            Some(p) => p,
            None => return Ok(()),
        };
        let policies = ensure_dotmap_slot(&env, "policies")?;
        let dump = build_environment(Args::from(policy.clone()))?;
        let gid = policy.__getattr__("global_id")?.__str__();
        policies.__setitem__(
            gid.as_str(),
            DotMap::new(Args::from(dump.__getitem__("policy")?))?.into_arg(),
        )
    })();
    *_FLUSH_ACTIVE.write() = was_active;
    r.map(|()| Arg::None)
}

/// `dyn_len` is part of the dynamic protocol helpers used by `read_args`.
pub fn _dyn_len(x: &Arg) -> Option<usize> {
    dyn_len(x)
}

/// A `PyDictBuilder` convenience for callers assembling args subtrees.
pub fn _dict(pairs: Vec<(&str, Arg)>) -> Arg {
    let mut d = PyDictBuilder::new();
    for (k, v) in pairs {
        d.insert(k, v);
    }
    d.finish()
}

/// `_LAILA_CLI_CAPABLE_CLASS._resolve_from_cli_args(data)` on a class
/// object, Python-spelled (`cls` is a class object, `data` a dict).
pub fn _resolve_from_cli_args(cls: impl IntoArg, data: impl IntoArg) -> Result<Arg> {
    let (cls, data) = (cls.into_arg(), data.into_arg());
    let c = class_of(&cls)?;
    if !matches!(data, Arg::Dict(_)) {
        return Ok(data);
    }
    let mut kw = Kwargs::default();
    for k in data.__iter__()? {
        kw.insert(k.__str__(), data.__getitem__(k)?);
    }
    _LAILA_CLI_CAPABLE_CLASS::_resolve_from_cli_args(c, &mut kw)?;
    let out = crate::python::builtins::dict(Args::new())?;
    for (k, v) in kw {
        out.__setitem__(k, v)?;
    }
    Ok(out)
}
