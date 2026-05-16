"""CLI-capable base class with 4-tier parameter resolution.

Implements the *configurability* contract that every "first-class"
laila object (policies, taskforces, pools, comm protocols, the
logger) opts into by inheriting from :class:`_LAILA_CLI_CAPABLE_CLASS`.
The class hooks into Pydantic's ``model_validator`` machinery so that,
during construction, missing constructor arguments are auto-filled
from :data:`laila.args` according to the following resolution order:

1. **Explicit kwarg** -- ``MyClass(param=value)`` always wins.
2. **laila.args lookup** -- a value found at the corresponding
   ``laila.args.policy.central.command.taskforces.<gid>.<param>``-style
   path is used. Paths are derived automatically from the class's
   ``_scopes`` PrivateAttr; see :data:`_SCOPE_TO_ARGS_PATH` for the
   mapping.
3. **Pydantic default** -- the field's ``default=`` / ``default_factory``
   is used if neither of the above produced a value.
4. **Required-field check** -- fields listed in ``_cli_required_fields``
   that are still ``None`` after the above raise :class:`RuntimeError`.

Exemptions
----------
Mark a field as :func:`CLIExempt` to keep it out of both the
``laila.args`` injection step and the environment mirror produced by
:func:`build_environment`. Use this for private bookkeeping, runtime
caches, or any state that should not appear on a CLI surface.

Environment mirror
------------------
This module also exposes :func:`build_environment` and
:func:`_load_environment` -- the round-trip used by
:func:`laila.environment_to_s3` to snapshot a live policy graph,
ship it elsewhere, and reconstruct the same hierarchy on the
receiving side. The mirror lives at
``laila.args.environment.policies[<gid>]`` and is refreshed on
every successful CLI-capable construction via
:func:`_refresh_args_environment` (see :class:`model_validator`
hook in :class:`_LAILA_CLI_CAPABLE_CLASS`).
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, Set

from pydantic import BaseModel, Field, model_validator


def CLIExempt(*args: Any, **kwargs: Any) -> Any:
    """Drop-in replacement for :func:`pydantic.Field` that marks a field CLI-exempt.

    Exempt fields are skipped by both the ``laila.args`` auto-injection
    step in :meth:`_LAILA_CLI_CAPABLE_CLASS._resolve_from_cli_args`
    and the environment mirror produced by
    :func:`build_environment`. Use this for fields that should never
    end up on a command-line surface or in a serialised environment
    dump -- typically internal caches, mutable runtime state, or
    fields whose values are derived rather than configured.

    Implementation detail: marks the field by stashing
    ``cli_exempt=True`` inside ``json_schema_extra``, which is the
    Pydantic-blessed extension point and survives JSON-schema dumps.
    """
    extra = kwargs.get("json_schema_extra") or {}
    extra["cli_exempt"] = True
    kwargs["json_schema_extra"] = extra
    return Field(*args, **kwargs)


def _is_cli_exempt(field_info: Any) -> bool:
    """Return ``True`` if a ``FieldInfo`` carries the CLI-exempt marker.

    Reads the ``cli_exempt`` flag stashed by :func:`CLIExempt` on the
    field's ``json_schema_extra`` dict.
    """
    extra = field_info.json_schema_extra
    if isinstance(extra, dict):
        return extra.get("cli_exempt", False)
    return False


_SCOPE_TO_ARGS_PATH: dict[str, str] = {
    "POLICY":                "policy",
    "CENTRAL_COMMAND":       "policy.central.command",
    "CENTRAL_MEMORY":        "policy.central.memory",
    "CENTRAL_COMMUNICATION": "policy.central.communication",
    "POOL":                  "policy.central.memory.pools.{global_id}",
    "POOL_ROUTER":           "policy.central.memory.pool_router",
    "TASK_FORCE":            "policy.central.command.taskforces.{global_id}",
    "COMM_PROTOCOL":         "policy.central.communication.connections.{global_id}",
    "LOGGER":                "logger",
}


def _get_args_subtree(path: str) -> Any:
    """Traverse ``laila.args`` using ``.get()`` to avoid DotMap auto-creation."""
    try:
        import laila
        subtree: Any = laila.args
        for part in path.split("."):
            if subtree is None:
                return None
            subtree = subtree.get(part) if hasattr(subtree, "get") else None
        if subtree is not None and hasattr(subtree, "keys") and len(subtree) == 0:
            return None
        return subtree
    except (ImportError, AttributeError):
        return None


def _resolve_global_id_for_path(cls: type, data: dict) -> Optional[str]:
    """Compute a global_id from constructor data for ``{global_id}`` path templates.

    Checks explicit ``data`` first, then falls back to the thread-local
    ``_INIT_PENDING`` staging area used by ``_LAILA_IDENTIFIABLE_OBJECT.__init__``
    (which pops ``uuid`` from data before Pydantic validation runs).
    """
    if "global_id" in data:
        return str(data["global_id"])

    uuid_val = data.get("uuid")
    if uuid_val is None:
        try:
            from .identifiable_object import _INIT_PENDING
            uuid_val = getattr(_INIT_PENDING, "uuid", None)
        except ImportError:
            pass
    if uuid_val is None:
        return None

    scopes_attr = cls.__private_attributes__.get("_scopes")
    if scopes_attr is None:
        return None
    scopes = scopes_attr.default_factory() if scopes_attr.default_factory else ["OBJECT"]

    from .identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
    return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(uuid=uuid_val, scopes=scopes)


def _scope_for_class(cls: type) -> Optional[str]:
    """Read the first scope from a class's ``_scopes`` PrivateAttr default."""
    scopes_attr = cls.__private_attributes__.get("_scopes")
    if scopes_attr is None:
        return None
    scopes = scopes_attr.default_factory() if scopes_attr.default_factory else None
    if scopes and len(scopes) > 0:
        return scopes[0]
    return None


def _resolve_path_for_class(cls: type, data: dict) -> Optional[str]:
    """Determine the ``laila.args`` path for a CLI-capable class instance."""
    scope = _scope_for_class(cls)
    if scope is None:
        return None
    path_template = _SCOPE_TO_ARGS_PATH.get(scope)
    if path_template is None:
        return None
    if "{global_id}" in path_template:
        gid = _resolve_global_id_for_path(cls, data)
        if gid is None:
            return None
        path_template = path_template.replace("{global_id}", gid)
    return path_template


def _eligible_model_fields(cls: type) -> list[str]:
    """Return public, non-exempt Pydantic Field names for *cls*."""
    names = []
    for name, field_info in cls.model_fields.items():
        if name.startswith("_"):
            continue
        if _is_cli_exempt(field_info):
            continue
        names.append(name)
    return names


def _eligible_property_setters(cls: type) -> list[str]:
    """Return names of public property descriptors that have a setter."""
    names = []
    for name in dir(cls):
        if name.startswith("_"):
            continue
        attr = getattr(cls, name, None)
        if isinstance(attr, property) and attr.fset is not None:
            if name not in cls.model_fields:
                names.append(name)
    return names


def _all_subclasses(root: type) -> dict[str, type]:
    """Return a flat ``{class_name: class}`` map of *root* and every subclass.

    Walks ``__subclasses__()`` recursively, so it only sees classes that
    have already been imported in this process.  Sub-classes that share
    a name shadow each other (last wins) -- callers should ensure their
    registry uses globally-unique class names.
    """
    out: dict[str, type] = {root.__name__: root}
    for sub in root.__subclasses__():
        out.update(_all_subclasses(sub))
    return out


def _resolve_class(token: str, root: type) -> type:
    """Look up a subclass of *root* by ``__name__`` token.

    Raises ValueError listing the known tokens when *token* is unknown.
    """
    classes = _all_subclasses(root)
    if token not in classes:
        known = ", ".join(sorted(classes))
        raise ValueError(
            f"unknown {root.__name__} subclass token: {token!r} "
            f"(known: {known})"
        )
    return classes[token]


def build_environment(policy: Any) -> dict:
    """Build a JSON-serializable environment dict from a live policy.

    Walks the policy tree and collects only CLI-eligible fields (public,
    non-exempt, non-private).  This is the same structure ``laila.args``
    accepts.
    """
    def _dump_obj(obj: Any) -> dict:
        result: dict[str, Any] = {}
        cls = type(obj)
        for name in _eligible_model_fields(cls):
            val = getattr(obj, name, None)
            if val is not None and hasattr(type(val), "model_fields"):
                result[name] = _dump_obj(val)
            elif isinstance(val, dict):
                nested: dict[str, Any] = {}
                for k, v in val.items():
                    if hasattr(type(v), "model_fields"):
                        nested[k] = _dump_obj(v)
                    else:
                        nested[k] = v
                result[name] = nested
            else:
                result[name] = val
        for name in _eligible_property_setters(cls):
            try:
                result[name] = getattr(obj, name, None)
            except Exception:
                pass
        return result

    env: dict[str, Any] = {"policy": {}}
    policy_data = _dump_obj(policy)
    for prop_name in _eligible_property_setters(type(policy)):
        try:
            policy_data[prop_name] = getattr(policy, prop_name, None)
        except Exception:
            pass

    env["policy"] = policy_data

    if hasattr(policy, "central"):
        central = policy.central
        if not env["policy"].get("central"):
            env["policy"]["central"] = {}
        central_env = env["policy"]["central"]

        if hasattr(central, "command") and central.command is not None:
            cmd = central.command
            cmd_data = _dump_obj(cmd)
            for prop_name in _eligible_property_setters(type(cmd)):
                try:
                    cmd_data[prop_name] = getattr(cmd, prop_name, None)
                except Exception:
                    pass
            if hasattr(cmd, "taskforces"):
                tf_data: dict[str, Any] = {}
                for tf_id, tf in cmd.taskforces.items():
                    tf_dump = _dump_obj(tf)
                    for prop_name in _eligible_property_setters(type(tf)):
                        try:
                            tf_dump[prop_name] = getattr(tf, prop_name, None)
                        except Exception:
                            pass
                    tf_dump["class_token"] = type(tf).__name__
                    tf_data[tf_id] = tf_dump
                cmd_data["taskforces"] = tf_data
            central_env["command"] = cmd_data

        if hasattr(central, "memory") and central.memory is not None:
            mem = central.memory
            mem_data = _dump_obj(mem)
            for prop_name in _eligible_property_setters(type(mem)):
                try:
                    mem_data[prop_name] = getattr(mem, prop_name, None)
                except Exception:
                    pass
            if hasattr(mem, "pool_router") and mem.pool_router is not None:
                router = mem.pool_router
                router_data = _dump_obj(router)
                for prop_name in _eligible_property_setters(type(router)):
                    try:
                        router_data[prop_name] = getattr(router, prop_name, None)
                    except Exception:
                        pass
                if hasattr(router, "pools"):
                    pool_data: dict[str, Any] = {}
                    for pool_id, pool in router.pools.items():
                        pool_dump = _dump_obj(pool)
                        for prop_name in _eligible_property_setters(type(pool)):
                            try:
                                pool_dump[prop_name] = getattr(pool, prop_name, None)
                            except Exception:
                                pass
                        pool_dump["class_token"] = type(pool).__name__
                        pool_data[pool_id] = pool_dump
                    router_data["pools"] = pool_data
                mem_data["pool_router"] = router_data
            central_env["memory"] = mem_data

        if hasattr(central, "communication") and central.communication is not None:
            comm = central.communication
            comm_data = _dump_obj(comm)
            for prop_name in _eligible_property_setters(type(comm)):
                try:
                    comm_data[prop_name] = getattr(comm, prop_name, None)
                except Exception:
                    pass
            if hasattr(comm, "connections"):
                conn_data: dict[str, Any] = {}
                for proto_id, proto in comm.connections.items():
                    proto_dump = _dump_obj(proto)
                    for prop_name in _eligible_property_setters(type(proto)):
                        try:
                            proto_dump[prop_name] = getattr(proto, prop_name, None)
                        except Exception:
                            pass
                    proto_dump["class_token"] = type(proto).__name__
                    conn_data[proto_id] = proto_dump
                comm_data["connections"] = conn_data
            central_env["communication"] = comm_data

    return env


class _LAILA_CLI_CAPABLE_CLASS(BaseModel):
    """Base class enabling 4-tier parameter resolution from ``laila.args``.

    Inherit from this alongside :class:`_LAILA_IDENTIFIABLE_OBJECT`
    (or one of its subclasses) to opt into:

    - Auto-population of constructor kwargs from a matching subtree
      of :data:`laila.args` (see module docstring for the exact
      resolution order).
    - Auto-refresh of the corresponding entry under
      ``laila.args.environment.policies[<gid>]`` after every successful
      construction, so the live policy graph and the CLI mirror stay
      in sync.
    - Mandatory-field enforcement via :attr:`_cli_required_fields` --
      fields named here that remain ``None`` after the four-tier
      resolution raise :class:`RuntimeError`.

    No additional configuration is needed in the subclass: the args
    path is derived automatically from the class's ``_scopes``
    PrivateAttr (see :data:`_SCOPE_TO_ARGS_PATH`). Only the rare
    cases where a field must always come from somewhere need to set
    ``_cli_required_fields``.

    Attributes
    ----------
    _cli_required_fields : ClassVar[set[str]]
        Names of fields that are mandatory after resolution. Override
        in subclasses that need to fail loudly instead of carrying a
        ``None`` default through to runtime.
    """

    _cli_required_fields: ClassVar[Set[str]] = set()

    @model_validator(mode="before")
    @classmethod
    def _resolve_from_cli_args(cls, data: Any) -> Any:
        """Pydantic before-validator that injects values from ``laila.args``.

        Skipped silently when *data* is not a dict (e.g. when Pydantic
        is round-tripping an existing model). When the class's
        ``_scopes`` does not map to a known args path, also a no-op.
        Otherwise: walk the eligible model fields, and for any that
        the caller did not pass explicitly, pull the matching value
        out of the args subtree (treating empty :class:`DotMap` nodes
        as "absent" so user code can write ``laila.args.foo = {}``
        without unintentionally clearing defaults).
        """
        if not isinstance(data, dict):
            return data

        path = _resolve_path_for_class(cls, data)
        if path is None:
            return data

        subtree = _get_args_subtree(path)
        if subtree is None:
            return data

        for field_name in _eligible_model_fields(cls):
            if field_name in data:
                continue
            val = subtree.get(field_name) if hasattr(subtree, "get") else None
            if val is not None:
                from dotmap import DotMap
                if isinstance(val, DotMap) and len(val) == 0:
                    continue
                data[field_name] = val

        return data

    def model_post_init(self, __context: Any) -> None:
        """Pydantic post-init hook that backfills property setters and enforces required fields.

        Runs after :meth:`_resolve_from_cli_args`. Two responsibilities:

        - For each property on the class that has a setter (so users
          can reasonably configure it via ``laila.args``), look up
          and assign the matching value from the same args subtree
          used during field resolution. Failures are swallowed so a
          mis-typed property name does not break construction.
        - Re-check :attr:`_cli_required_fields`: any name that is
          still ``None`` after both passes triggers a
          :class:`RuntimeError` with a message pointing the user at
          the explicit / args / default escape hatches.
        """
        super().model_post_init(__context)

        path = _resolve_path_for_class(type(self), {})
        subtree = _get_args_subtree(path) if path else None

        if subtree is not None:
            for prop_name in _eligible_property_setters(type(self)):
                if prop_name.startswith("_"):
                    continue
                val = subtree.get(prop_name) if hasattr(subtree, "get") else None
                if val is not None:
                    from dotmap import DotMap
                    if isinstance(val, DotMap) and len(val) == 0:
                        continue
                    try:
                        setattr(self, prop_name, val)
                    except Exception:
                        pass

        for field_name in self._cli_required_fields:
            if getattr(self, field_name, None) is None:
                raise RuntimeError(
                    f"{type(self).__name__}.{field_name} is required but was not "
                    f"provided explicitly, via laila.args, or as a default."
                )

    @model_validator(mode="after")
    def _refresh_environment_mirror(self):
        """Update ``laila.args.environment.policies[<gid>]`` for this instance's owning policy.

        Best-effort: silently no-ops if ``laila`` is mid-import, if no
        owning policy can be resolved, or if anything else goes wrong.
        """
        try:
            _refresh_args_environment(self)
        except Exception:
            pass
        return self


def _find_owning_policy(instance: Any) -> Any:
    """Return the policy that *instance* belongs to, or ``None``.

    Resolution order:
    1. *instance* is itself a ``_LAILA_IDENTIFIABLE_POLICY``.
    2. *instance* has a non-None ``policy_id`` registered in
       ``laila._local_policies``.
    3. The currently active local policy (``laila._active_policy_gid``).
    """
    try:
        import laila
        from ..definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
    except Exception:
        return None

    try:
        from ...policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
    except Exception:
        _LAILA_IDENTIFIABLE_POLICY = None

    if _LAILA_IDENTIFIABLE_POLICY is not None and isinstance(instance, _LAILA_IDENTIFIABLE_POLICY):
        return instance

    policy_id = getattr(instance, "policy_id", None)
    if policy_id is not None:
        if isinstance(policy_id, _LAILA_IDENTIFIABLE_OBJECT):
            policy_id = policy_id.global_id
        policy_id = str(policy_id)
        local = getattr(laila, "_local_policies", {})
        policy = local.get(policy_id)
        if policy is not None:
            return policy

    active_gid = getattr(laila, "_active_policy_gid", None)
    if active_gid is not None:
        local = getattr(laila, "_local_policies", {})
        policy = local.get(active_gid)
        if policy is not None:
            return policy

    return None


def _strip_class_token(data: Any) -> dict:
    """Return a shallow copy of *data* (dict-like) without ``class_token``."""
    if data is None:
        return {}
    if hasattr(data, "toDict"):
        try:
            data = data.toDict()
        except Exception:
            pass
    if not isinstance(data, dict):
        try:
            data = dict(data)
        except Exception:
            return {}
    out = {k: v for k, v in data.items() if k != "class_token"}
    return out


def _coerce_to_plain_dict(value: Any) -> Any:
    """Best-effort conversion of DotMap / nested dicts to plain dicts."""
    if hasattr(value, "toDict"):
        try:
            return value.toDict()
        except Exception:
            pass
    if isinstance(value, dict):
        return {k: _coerce_to_plain_dict(v) for k, v in value.items()}
    return value


def _load_environment(env: Any) -> None:
    """Replace the entire laila runtime with the policies described by *env*.

    Implements the 4-rule active-policy resolution:

    1. ``env.active_gid`` set: must appear in ``env.policies``; activate it.
    2. ``env.policies`` empty: create a fresh ``DefaultPolicy``, activate it.
    3. ``env.policies`` has exactly one entry: activate it.
    4. Otherwise (>=2 policies, no ``active_gid``): raise ``ValueError``.

    Reconstructs concrete subclasses for taskforces, pools, and
    communication protocols using the ``class_token`` embedded by
    ``build_environment``. Protocols are registered but **not** started --
    callers must call ``protocol.start()`` themselves.
    """
    import laila
    from .identifiable_object import _LAILA_IDENTIFIABLE_OBJECT

    env = _coerce_to_plain_dict(env)
    if not isinstance(env, dict):
        raise ValueError(f"environment must be a dict, got {type(env).__name__}")

    if "policies" not in env:
        raise ValueError("environment must contain a 'policies' key")

    policies_dump = env.get("policies") or {}
    if not isinstance(policies_dump, dict):
        raise ValueError(
            f"environment.policies must be a dict, got {type(policies_dump).__name__}"
        )

    active_gid = env.get("active_gid")
    if active_gid is not None and len(policies_dump) == 0:
        raise ValueError(
            "active_gid is set but environment.policies is empty"
        )

    laila.terminate(wait=True, cancel_pending=False)

    if len(policies_dump) == 0:
        from ...macros.defaults import DefaultPolicy
        new_policy = DefaultPolicy()
        laila.activate_policy(new_policy)
        return

    seen_uuids: set = set()
    seen_proto_gids: set = set()
    built_policies: list = []

    try:
        for policy_gid, policy_data in policies_dump.items():
            policy_data = _coerce_to_plain_dict(policy_data)
            if not isinstance(policy_data, dict):
                raise ValueError(
                    f"policy entry for {policy_gid!r} must be a dict, "
                    f"got {type(policy_data).__name__}"
                )

            try:
                ident = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(policy_gid)
            except ValueError as e:
                raise ValueError(
                    f"invalid policy gid {policy_gid!r}: {e}"
                ) from e

            policy_uuid = ident["uuid"]
            if policy_uuid in seen_uuids:
                raise ValueError(
                    f"duplicate policy uuid {policy_uuid!r} in environment.policies"
                )
            seen_uuids.add(policy_uuid)

            policy = _build_policy_from_dump(
                policy_uuid=policy_uuid,
                policy_data=policy_data,
                seen_proto_gids=seen_proto_gids,
            )

            if str(policy.global_id) != str(policy_gid):
                try:
                    _shutdown_orphan_policy(policy)
                except Exception:
                    pass
                raise ValueError(
                    f"reconstructed policy gid {policy.global_id!r} does not "
                    f"match dumped key {policy_gid!r}"
                )

            laila._local_policies[str(policy.global_id)] = policy
            built_policies.append(policy)
    except Exception:
        try:
            laila.terminate(wait=True, cancel_pending=False)
        finally:
            raise

    if active_gid is not None:
        active_gid = str(active_gid)
        if active_gid not in policies_dump:
            raise ValueError(
                f"active_gid {active_gid!r} not present in environment.policies"
            )
        chosen = laila._local_policies[active_gid]
    elif len(built_policies) == 1:
        chosen = built_policies[0]
    else:
        raise ValueError(
            "active_gid required when env contains multiple policies"
        )

    laila.activate_policy(chosen)


def _shutdown_orphan_policy(policy: Any) -> None:
    """Best-effort tear-down of *policy* and all its sub-resources.

    Used when a policy was partially built but cannot be added to
    ``laila._local_policies`` (e.g. its reconstructed global_id doesn't
    match the dumped key, or one of its sub-instances failed to build).
    """
    try:
        comm = getattr(getattr(policy, "central", None), "communication", None)
        if comm is not None:
            try:
                comm.stop()
            except Exception:
                pass
    except Exception:
        pass
    try:
        cmd = getattr(getattr(policy, "central", None), "command", None)
        if cmd is not None:
            try:
                cmd.shutdown(wait=True, cancel_pending=False)
            except Exception:
                pass
    except Exception:
        pass
    try:
        mem = getattr(getattr(policy, "central", None), "memory", None)
        router = getattr(mem, "pool_router", None) if mem is not None else None
        if router is not None:
            for pool in list(getattr(router, "pools", {}).values()):
                try:
                    close = getattr(pool, "close", None)
                    if callable(close):
                        close()
                except Exception:
                    pass
    except Exception:
        pass


def _build_policy_from_dump(
    *,
    policy_uuid: str,
    policy_data: dict,
    seen_proto_gids: set,
) -> Any:
    """Build one ``DefaultPolicy`` from a *policy_data* dump branch.

    Re-creates taskforces, pools, and comm-protocols using
    ``class_token`` to dispatch to the right subclass. Protocols are
    registered without calling ``start()``.
    """
    from ...macros.defaults import DefaultPolicy
    from ..definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
    from ...pool.schema.base import _LAILA_IDENTIFIABLE_POOL
    from ...policy.central.command.taskforce.base import _LAILA_IDENTIFIABLE_TASK_FORCE
    from ...policy.central.communication.protocols.base import (
        _LAILA_IDENTIFIABLE_COMM_PROTOCOL,
    )

    policy = DefaultPolicy()
    if policy_uuid != policy.uuid:
        policy._uuid = policy_uuid
        new_gid = policy.global_id
        if policy.central.command is not None:
            try:
                policy.central.command.policy_id = new_gid
            except Exception:
                pass
            for default_tf in policy.central.command.taskforces.values():
                try:
                    default_tf.policy_id = new_gid
                except Exception:
                    pass
        if policy.central.communication is not None:
            try:
                policy.central.communication.policy_id = new_gid
            except Exception:
                pass

    import laila as _laila

    _laila._local_policies[str(policy.global_id)] = policy

    central_data = policy_data.get("central") or {}
    if hasattr(central_data, "toDict"):
        central_data = central_data.toDict()
    if not isinstance(central_data, dict):
        central_data = {}

    cmd_data = central_data.get("command") or {}
    if hasattr(cmd_data, "toDict"):
        cmd_data = cmd_data.toDict()
    if not isinstance(cmd_data, dict):
        cmd_data = {}

    tf_dump = cmd_data.get("taskforces") or {}
    if hasattr(tf_dump, "toDict"):
        tf_dump = tf_dump.toDict()

    if isinstance(tf_dump, dict) and len(tf_dump) > 0:
        for default_tf in list(policy.central.command.taskforces.values()):
            try:
                if hasattr(default_tf, "shutdown"):
                    default_tf.shutdown(wait=True, cancel_pending=True)
            except Exception:
                pass
        policy.central.command.taskforces.clear()

        for tf_gid, tf_data in tf_dump.items():
            tf_data_plain = _coerce_to_plain_dict(tf_data)
            if not isinstance(tf_data_plain, dict):
                raise ValueError(
                    f"taskforce entry {tf_gid!r} must be a dict"
                )
            token = tf_data_plain.get("class_token")
            if token is None:
                raise ValueError(
                    f"taskforce entry {tf_gid!r} is missing 'class_token'"
                )
            cls = _resolve_class(token, _LAILA_IDENTIFIABLE_TASK_FORCE)
            tf_kwargs = _strip_class_token(tf_data_plain)
            tf_kwargs.pop("status", None)
            tf_kwargs.pop("queue_len", None)
            try:
                tf_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(tf_gid)["uuid"]
            except ValueError as e:
                raise ValueError(f"invalid taskforce gid {tf_gid!r}: {e}") from e
            tf_kwargs["uuid"] = tf_uuid
            tf_kwargs["policy_id"] = policy.global_id
            tf = cls(**tf_kwargs)
            if tf.uuid != tf_uuid:
                tf._uuid = tf_uuid
            policy.central.command.add_taskforce(tf)

        alpha_tf = cmd_data.get("alpha_taskforce")
        if alpha_tf is not None and str(alpha_tf) in policy.central.command.taskforces:
            policy.central.command.alpha_taskforce = str(alpha_tf)
        else:
            policy.central.command.alpha_taskforce = next(
                iter(policy.central.command.taskforces)
            )

    mem_data = central_data.get("memory") or {}
    if hasattr(mem_data, "toDict"):
        mem_data = mem_data.toDict()
    if not isinstance(mem_data, dict):
        mem_data = {}

    router_data = mem_data.get("pool_router") or {}
    if hasattr(router_data, "toDict"):
        router_data = router_data.toDict()

    pool_dump = router_data.get("pools") if isinstance(router_data, dict) else None
    if hasattr(pool_dump, "toDict"):
        pool_dump = pool_dump.toDict()

    if isinstance(pool_dump, dict) and len(pool_dump) > 0:
        try:
            for old_pool in list(policy.central.memory.pool_router.pools.values()):
                close = getattr(old_pool, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
        except Exception:
            pass

        policy.central.memory.pool_router.pools.clear()
        try:
            from queue import PriorityQueue
            policy.central.memory.pool_router.pools_pq = PriorityQueue()
        except Exception:
            pass
        nicks_in = router_data.get("pools_nicknames") or {}
        if hasattr(nicks_in, "toDict"):
            nicks_in = nicks_in.toDict()
        nicks_in = nicks_in if isinstance(nicks_in, dict) else {}
        gid_to_nick: dict[str, str] = {
            str(gid): nick for nick, gid in nicks_in.items()
        }
        policy.central.memory.pool_router.pools_nicknames.clear()

        first_pool_gid: Optional[str] = None
        for pool_gid, pool_data in pool_dump.items():
            pool_data_plain = _coerce_to_plain_dict(pool_data)
            if not isinstance(pool_data_plain, dict):
                raise ValueError(f"pool entry {pool_gid!r} must be a dict")
            token = pool_data_plain.get("class_token")
            if token is None:
                raise ValueError(
                    f"pool entry {pool_gid!r} is missing 'class_token'"
                )
            cls = _resolve_class(token, _LAILA_IDENTIFIABLE_POOL)
            pool_kwargs = _strip_class_token(pool_data_plain)
            pool_kwargs.pop("pool_id", None)
            try:
                pool_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(pool_gid)["uuid"]
            except ValueError as e:
                raise ValueError(f"invalid pool gid {pool_gid!r}: {e}") from e
            pool_kwargs["uuid"] = pool_uuid
            pool = cls(**pool_kwargs)
            if pool.uuid != pool_uuid:
                pool._uuid = pool_uuid
            nickname = gid_to_nick.get(str(pool.global_id))
            policy.central.memory.pool_router.extend(
                pool, affinity=None, pool_nickname=nickname
            )
            if first_pool_gid is None:
                first_pool_gid = str(pool.global_id)

        alpha = mem_data.get("alpha_pool")
        if alpha is not None and str(alpha) in policy.central.memory.pool_router.pools:
            policy.central.memory.alpha_pool = str(alpha)
        elif first_pool_gid is not None:
            policy.central.memory.alpha_pool = first_pool_gid

    comm_data = central_data.get("communication") or {}
    if hasattr(comm_data, "toDict"):
        comm_data = comm_data.toDict()
    if not isinstance(comm_data, dict):
        comm_data = {}

    conn_dump = comm_data.get("connections") or {}
    if hasattr(conn_dump, "toDict"):
        conn_dump = conn_dump.toDict()
    if isinstance(conn_dump, dict) and len(conn_dump) > 0:
        for proto_gid, proto_data in conn_dump.items():
            proto_data_plain = _coerce_to_plain_dict(proto_data)
            if not isinstance(proto_data_plain, dict):
                raise ValueError(
                    f"protocol entry {proto_gid!r} must be a dict"
                )
            token = proto_data_plain.get("class_token")
            if token is None:
                raise ValueError(
                    f"protocol entry {proto_gid!r} is missing 'class_token'"
                )
            cls = _resolve_class(token, _LAILA_IDENTIFIABLE_COMM_PROTOCOL)
            proto_kwargs = _strip_class_token(proto_data_plain)
            try:
                proto_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(proto_gid)["uuid"]
            except ValueError as e:
                raise ValueError(
                    f"invalid protocol gid {proto_gid!r}: {e}"
                ) from e
            proto_kwargs["uuid"] = proto_uuid
            proto = cls(**proto_kwargs)
            if proto.uuid != proto_uuid:
                proto._uuid = proto_uuid
            full_gid = str(proto.global_id)
            if full_gid in seen_proto_gids:
                raise ValueError(
                    f"duplicate protocol gid {full_gid!r} across policies"
                )
            seen_proto_gids.add(full_gid)
            proto._communication = policy.central.communication
            policy.central.communication.connections[full_gid] = proto

    return policy


def _refresh_args_environment(instance: Any) -> None:
    """Write the owning policy's ``build_environment`` dump under
    ``laila.args.environment.policies[<gid>]``.

    For a top-level singleton like the :class:`laila.logger.Logger`, which
    has no owning policy, the dump is written under
    ``laila.args.environment.logger`` instead.
    """
    import laila
    from dotmap import DotMap

    if _scope_for_class(type(instance)) == "LOGGER":
        args = laila.args
        env = args.get("environment") if hasattr(args, "get") else None
        if not isinstance(env, DotMap):
            env = DotMap()
            args.environment = env

        dump: dict[str, Any] = {"class_token": type(instance).__name__}
        for name in _eligible_model_fields(type(instance)):
            try:
                dump[name] = getattr(instance, name, None)
            except Exception:
                pass
        for name in _eligible_property_setters(type(instance)):
            try:
                dump[name] = getattr(instance, name, None)
            except Exception:
                pass
        env.logger = DotMap(dump)
        return

    policy = _find_owning_policy(instance)
    if policy is None:
        return

    args = laila.args
    env = args.get("environment") if hasattr(args, "get") else None
    if not isinstance(env, DotMap):
        env = DotMap()
        args.environment = env

    policies = env.get("policies") if hasattr(env, "get") else None
    if not isinstance(policies, DotMap):
        policies = DotMap()
        env.policies = policies

    dump = build_environment(policy)
    policies[str(policy.global_id)] = DotMap(dump["policy"])
