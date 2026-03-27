"""CLI-capable base class with 4-tier parameter resolution.

Provides automatic parameter injection from ``laila.args`` for any Pydantic
model that inherits from ``_LAILA_CLI_CAPABLE_CLASS``.  Resolution order:

1. Explicitly passed in code (``param=value``)
2. Found at the matching path in ``laila.args``
3. Pydantic ``Field(default=...)`` or ``default_factory``
4. ``RuntimeError`` if still uninitialized
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, Set

from pydantic import BaseModel, Field, model_validator


def CLIExempt(*args: Any, **kwargs: Any) -> Any:
    """Drop-in replacement for ``Field()`` that marks a field as CLI-exempt.

    Exempt fields are never injected from ``laila.args`` and are excluded
    from the ``laila.environment`` dump.
    """
    extra = kwargs.get("json_schema_extra") or {}
    extra["cli_exempt"] = True
    kwargs["json_schema_extra"] = extra
    return Field(*args, **kwargs)


def _is_cli_exempt(field_info: Any) -> bool:
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
                    conn_data[proto_id] = proto_dump
                comm_data["connections"] = conn_data
            central_env["communication"] = comm_data

    return env


class _LAILA_CLI_CAPABLE_CLASS(BaseModel):
    """Base class enabling 4-tier parameter resolution from ``laila.args``.

    Inherit from this alongside ``_LAILA_IDENTIFIABLE_OBJECT`` (or its
    subclasses).  No configuration is needed in the child class — the
    resolution path is derived automatically from the child's ``_scopes``
    private attribute.

    Resolution order
    ----------------
    1. Explicitly passed ``param=value`` in code
    2. Value found at the matching ``laila.args`` path
    3. Pydantic ``Field(default=...)`` / ``default_factory``
    4. ``RuntimeError`` for required fields still unset
    """

    _cli_required_fields: ClassVar[Set[str]] = set()

    @model_validator(mode="before")
    @classmethod
    def _resolve_from_cli_args(cls, data: Any) -> Any:
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
