//! The policy-side half of `laila.args.environment` loading
//! (`cli_capable::_load_environment`): `_build_policy_from_dump` and the
//! `EnvironmentBackend` that hands it, `DefaultPolicy()`, `terminate` and
//! `activate_policy` to the loader.
//!
//! Python's `_build_policy_from_dump` lives in `cli_capable.py` and reaches
//! the policy classes through late imports; here the loader is generic
//! over a backend (so `basics` does not depend on `policy`) and this
//! module is the one implementation.

use alloc::string::String;
use alloc::vec::Vec;

use crate::basics::definitions::cli_capable::{
    _coerce_to_plain_dict, _resolve_class, _strip_class_token, EnvironmentBackend,
};
use crate::basics::definitions::identifiable_object::process_global_id_arg;
use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use super::central::command::taskforce::TaskForce;
#[cfg(feature = "std")]
use super::central::communication::protocols::{self, Protocol, ProtocolSpec};
use super::Policy;
#[cfg(feature = "std")]
use crate::python::PyObject;

/// `laila.terminate(wait=True, cancel_pending=False)`.
fn terminate() -> Result<()> {
    let _ = super::terminate(true, false);
    Ok(())
}

/// `DefaultPolicy()`.
fn default_policy() -> Result<Arg> {
    Ok(Policy::new(Args::new())?.into_arg())
}

/// `laila.activate_policy(policy)`.
fn activate_policy(policy: &Arg) -> Result<()> {
    let p = Policy::of(policy).ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "activate_policy expects a policy, got {}",
            policy.type_name()
        ))
    })?;
    super::activate_policy(&p);
    Ok(())
}

fn shutdown_orphan_policy(policy: &Arg) {
    crate::basics::definitions::cli_capable::_shutdown_orphan_policy(policy)
}

/// `data.get(key) or {}` coerced to a plain dict (`{}` when not a mapping).
fn sub_dict(data: &Arg, key: &str) -> Result<Arg> {
    let v = if matches!(data, Arg::Dict(_)) {
        data.get(Args::from(key))?
    } else {
        Arg::None
    };
    let v = _coerce_to_plain_dict(&v)?;
    if matches!(v, Arg::Dict(_)) {
        Ok(v)
    } else {
        crate::python::builtins::dict(Args::new())
    }
}

/// `uuid` of a dumped gid key (`ValueError("invalid <what> gid ..")`).
fn uuid_of(gid: &Arg, what: &str) -> Result<String> {
    process_global_id_arg(gid).map(|p| p.uuid).map_err(|e| {
        LailaError::value_error(alloc::format!(
            "invalid {what} gid {}: {}",
            gid.__repr__(),
            e.__str__()
        ))
    })
}

/// The `class_token` of a dumped entry (`ValueError` when absent).
fn class_token(entry: &Arg, gid: &Arg, what: &str) -> Result<String> {
    if !matches!(entry, Arg::Dict(_)) {
        return Err(LailaError::value_error(alloc::format!(
            "{what} entry {} must be a dict",
            gid.__repr__()
        )));
    }
    let token = entry.get(Args::from("class_token"))?;
    if token.is_none() {
        return Err(LailaError::value_error(alloc::format!(
            "{what} entry {} is missing 'class_token'",
            gid.__repr__()
        )));
    }
    Ok(token.__str__())
}

/// `_LAILA_IDENTIFIABLE_TASK_FORCE` subclasses by `__name__` (the
/// taskforces are not pydantic models, so `_resolve_class` cannot see them).
const TASKFORCE_TOKENS: &[&str] = &[
    "PythonAsyncThreadPoolTaskForce",
    "PythonProcessPoolTaskForce",
];

/// `_resolve_class(token, _LAILA_IDENTIFIABLE_COMM_PROTOCOL)`: the comm
/// protocols are spec-backed classes (not pydantic models), looked up by
/// `__name__`; `ValueError` listing the known tokens when unknown.
#[cfg(feature = "std")]
fn resolve_protocol_class(token: &str) -> Result<&'static ProtocolSpec> {
    if let Some(spec) = protocols::spec_by_class_name(token) {
        return Ok(spec);
    }
    let mut known: Vec<&str> = protocols::iter_comm_protocol_specs()
        .into_iter()
        .map(|s| s.class_name)
        .collect();
    known.push(protocols::base::BASE_SPEC.class_name);
    known.sort_unstable();
    known.dedup();
    Err(LailaError::value_error(alloc::format!(
        "unknown _LAILA_IDENTIFIABLE_COMM_PROTOCOL subclass token: {} (known: {})",
        crate::python::pyrepr::str_repr(token),
        known.join(", ")
    )))
}

fn construct_taskforce(token: &str, kwargs: Args) -> Result<TaskForce> {
    match token {
        "PythonAsyncThreadPoolTaskForce" => TaskForce::new(kwargs),
        "PythonProcessPoolTaskForce" => {
            super::central::command::taskforce::PythonProcessPoolTaskForce::new(kwargs)
        }
        _ => {
            let mut known: Vec<&str> = TASKFORCE_TOKENS.to_vec();
            known.sort_unstable();
            Err(LailaError::value_error(alloc::format!(
                "unknown _LAILA_IDENTIFIABLE_TASK_FORCE subclass token: {} (known: {})",
                crate::python::pyrepr::str_repr(token),
                known.join(", ")
            )))
        }
    }
}

/// Build one `DefaultPolicy` from a *policy_data* dump branch.
///
/// Re-creates taskforces, pools, and comm-protocols using `class_token`
/// to dispatch to the right subclass. Protocols are registered without
/// calling `start()`.
pub fn _build_policy_from_dump(
    policy_uuid: &str,
    policy_data: &Arg,
    #[cfg_attr(not(feature = "std"), allow(unused_variables, clippy::ptr_arg))]
    seen_proto_gids: &mut Vec<String>,
) -> Result<Arg> {
    // `DefaultPolicy()` then `policy._uuid = policy_uuid`: the identity is
    // immutable here, so the policy is built with that uuid (its command
    // and taskforces are stamped with the final gid by `model_post_init`).
    let mut a = Args::new();
    a.push_keyword("uuid", Arg::Str(policy_uuid.into()));
    let policy = Policy::new(a)?;
    super::_register_local_policy(&policy);

    let central_data = sub_dict(policy_data, "central")?;

    // ---- command / taskforces --------------------------------------
    let cmd_data = sub_dict(&central_data, "command")?;
    let tf_dump = sub_dict(&cmd_data, "taskforces")?;
    if tf_dump.__len__()? > 0 {
        let command = policy.command();
        for v in command.taskforces_snapshot().values()?.__iter__()? {
            if let Some(tf) = TaskForce::of(&v) {
                let mut a = Args::new();
                a.push_keyword("wait", true);
                a.push_keyword("cancel_pending", true);
                let _ = tf.shutdown(a);
            }
        }
        command.taskforces().clear(Args::new())?;

        for tf_gid in tf_dump.__iter__()? {
            let tf_data = _coerce_to_plain_dict(&tf_dump.__getitem__(tf_gid.clone())?)?;
            let token = class_token(&tf_data, &tf_gid, "taskforce")?;
            let tf_kwargs = _strip_class_token(Args::from(tf_data))?;
            for k in ["status", "queue_len"] {
                let mut p = Args::from(k);
                p.push_positional(Arg::None);
                let _ = tf_kwargs.pop(p);
            }
            let tf_uuid = uuid_of(&tf_gid, "taskforce")?;
            tf_kwargs.__setitem__("uuid", Arg::Str(tf_uuid.as_str().into()))?;
            tf_kwargs.__setitem__("policy_id", Arg::Str(policy.global_id().into()))?;
            let mut kw = Args::new();
            kw.extend_keywords(tf_kwargs)?;
            let tf = construct_taskforce(&token, kw)?;
            command.add_taskforce(tf)?;
        }

        let registered = command.taskforces_snapshot();
        let alpha_tf = cmd_data.get(Args::from("alpha_taskforce"))?;
        if !alpha_tf.is_none() && registered.__contains__(alpha_tf.__str__().as_str())? {
            command.__setattr__("alpha_taskforce", Arg::Str(alpha_tf.__str__().into()))?;
        } else {
            let first = registered
                .__iter__()?
                .into_iter()
                .next()
                .unwrap_or(Arg::None);
            command.__setattr__("alpha_taskforce", first)?;
        }
        let internal_tf = cmd_data.get(Args::from("internal_taskforce"))?;
        if !internal_tf.is_none() && registered.__contains__(internal_tf.__str__().as_str())? {
            command.__setattr__("internal_taskforce", Arg::Str(internal_tf.__str__().into()))?;
        } else {
            // Environments dumped before the internal/alpha split (or with a
            // stale pointer) run laila's internals on the alpha taskforce.
            command.__setattr__("internal_taskforce", command.alpha_taskforce())?;
        }
    }

    // ---- memory / pools --------------------------------------------
    let mem_data = sub_dict(&central_data, "memory")?;
    let router_data = sub_dict(&mem_data, "pool_router")?;
    let pool_dump = sub_dict(&router_data, "pools")?;
    if pool_dump.__len__()? > 0 {
        crate::data::register_pool_classes();
        let router = policy.pool_router();
        let pools = router.pools();
        if let Ok(values) = pools.values() {
            for old_pool in values.__iter__().unwrap_or_default() {
                let _ = old_pool.__call_method__("close", Args::new());
            }
        }
        pools.clear(Args::new())?;
        router.__setattr__(
            "pools_pq",
            crate::python::stdlib::queue.PriorityQueue(Args::new())?,
        )?;
        let nicks_in = sub_dict(&router_data, "pools_nicknames")?;
        let mut gid_to_nick: Vec<(String, Arg)> = Vec::new();
        for nick in nicks_in.__iter__()? {
            let gid = nicks_in.__getitem__(nick.clone())?;
            gid_to_nick.push((gid.__str__(), nick));
        }
        router.pools_nicknames().clear(Args::new())?;

        let mut first_pool_gid: Option<String> = None;
        for pool_gid in pool_dump.__iter__()? {
            let pool_data = _coerce_to_plain_dict(&pool_dump.__getitem__(pool_gid.clone())?)?;
            let token = class_token(&pool_data, &pool_gid, "pool")?;
            // `_resolve_class(token, _LAILA_IDENTIFIABLE_POOL)`: the
            // `ValueError` listing the known tokens when unknown.
            _resolve_class(&token, crate::data::_LAILA_IDENTIFIABLE_POOL::__class__())?;
            let pool_kwargs = _strip_class_token(Args::from(pool_data))?;
            let mut p = Args::from("pool_id");
            p.push_positional(Arg::None);
            let _ = pool_kwargs.pop(p);
            let pool_uuid = uuid_of(&pool_gid, "pool")?;
            pool_kwargs.__setitem__("uuid", Arg::Str(pool_uuid.as_str().into()))?;
            let mut kw = Args::new();
            kw.extend_keywords(pool_kwargs)?;
            let pool = crate::data::construct_pool(&token, kw).ok_or_else(|| {
                LailaError::value_error(alloc::format!(
                    "unknown _LAILA_IDENTIFIABLE_POOL subclass token: {}",
                    crate::python::pyrepr::str_repr(&token)
                ))
            })??;
            let gid = pool.global_id();
            let nickname = gid_to_nick
                .iter()
                .find(|(g, _)| *g == gid)
                .map(|(_, n)| n.clone())
                .unwrap_or(Arg::None);
            let mut a = Args::new();
            a.push_positional(Arg::Pool(pool));
            a.push_keyword("affinity", Arg::None);
            a.push_keyword("pool_nickname", nickname);
            router.extend(a)?;
            if first_pool_gid.is_none() {
                first_pool_gid = Some(gid);
            }
        }

        let alpha = mem_data.get(Args::from("alpha_pool"))?;
        let state = policy.memory_state();
        if !alpha.is_none() && router.pools().__contains__(alpha.__str__().as_str())? {
            *state.alpha_pool.write() = alpha.__str__();
        } else if let Some(first) = first_pool_gid {
            *state.alpha_pool.write() = first;
        }
    }

    // ---- communication / protocols --------------------------------
    // (transports are `std`-only; a no_std core restores no connections)
    #[cfg(feature = "std")]
    let comm_data = sub_dict(&central_data, "communication")?;
    #[cfg(feature = "std")]
    let conn_dump = sub_dict(&comm_data, "connections")?;
    #[cfg(feature = "std")]
    if conn_dump.__len__()? > 0 {
        let comm = policy.communication();
        for proto_gid in conn_dump.__iter__()? {
            let proto_data = _coerce_to_plain_dict(&conn_dump.__getitem__(proto_gid.clone())?)?;
            let token = class_token(&proto_data, &proto_gid, "protocol")?;
            // `_resolve_class(token, _LAILA_IDENTIFIABLE_COMM_PROTOCOL)`: the
            // transports are spec-backed classes, looked up by `__name__`.
            let spec = resolve_protocol_class(&token)?;
            let proto_kwargs = _strip_class_token(Args::from(proto_data))?;
            let proto_uuid = uuid_of(&proto_gid, "protocol")?;
            proto_kwargs.__setitem__("uuid", Arg::Str(proto_uuid.as_str().into()))?;
            let mut kw = Args::new();
            kw.extend_keywords(proto_kwargs)?;
            let proto = Protocol::new(spec, kw)?;
            let full_gid = proto.global_id();
            if seen_proto_gids.contains(&full_gid) {
                return Err(LailaError::value_error(alloc::format!(
                    "duplicate protocol gid {} across policies",
                    crate::python::pyrepr::str_repr(&full_gid)
                )));
            }
            seen_proto_gids.push(full_gid.clone());
            // `proto._communication = policy.central.communication` and
            // `connections[full_gid] = proto`: registered, not started.
            proto.set_communication(Some(&comm));
            PyObject::__setitem__(
                &comm.connections(),
                &Arg::Str(full_gid.as_str().into()),
                proto.into_arg(),
            )?;
        }
    }

    Ok(policy.into_arg())
}

/// The backend `cli_capable::_load_environment` runs on.
pub fn backend() -> EnvironmentBackend {
    EnvironmentBackend {
        terminate,
        default_policy,
        build_policy_from_dump: _build_policy_from_dump,
        activate_policy,
        shutdown_orphan_policy,
    }
}
