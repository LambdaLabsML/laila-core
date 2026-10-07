/**
 * CLI-capable base class with 4-tier parameter resolution.
 *
 * Implements the *configurability* contract that every "first-class" laila
 * object (policies, taskforces, pools, comm protocols, the logger) opts into
 * by inheriting from ``_LAILA_CLI_CAPABLE_CLASS``. The class hooks into
 * Pydantic's ``model_validator`` machinery so that, during construction,
 * missing constructor arguments are auto-filled from ``laila.args`` according
 * to the following resolution order:
 *
 * 1. **Explicit kwarg** -- ``new MyClass({param: value})`` always wins.
 * 2. **laila.args lookup** -- a value found at the corresponding
 *    ``laila.args.policy.central.command.taskforces.<gid>.<param>``-style
 *    path is used. Paths are derived automatically from the class's
 *    ``_scopes`` PrivateAttr; see ``_SCOPE_TO_ARGS_PATH`` for the mapping.
 * 3. **Pydantic default** -- the field's ``default`` / ``default_factory`` is
 *    used if neither of the above produced a value.
 * 4. **Required-field check** -- fields listed in ``_cli_required_fields``
 *    that are still ``null`` after the above raise ``RuntimeError``.
 *
 * Exemptions
 * ----------
 * Mark a field as ``CLIExempt`` to keep it out of both the ``laila.args``
 * injection step and the environment mirror produced by
 * ``build_environment``. Use this for private bookkeeping, runtime caches, or
 * any state that should not appear on a CLI surface.
 *
 * Environment mirror
 * ------------------
 * This module also exposes ``build_environment`` and ``_load_environment`` --
 * the round-trip used by ``laila.environment_to_s3`` to snapshot a live
 * policy graph, ship it elsewhere, and reconstruct the same hierarchy on the
 * receiving side. The mirror lives at
 * ``laila.args.environment.policies[<gid>]`` and is refreshed on every
 * successful CLI-capable construction via ``_refresh_args_environment`` (see
 * the ``model_validator`` hook in ``_LAILA_CLI_CAPABLE_CLASS``).
 *
 * Multiple inheritance: Python declares ``class Logger(_LAILA_CLI_CAPABLE_CLASS,
 * _LAILA_IDENTIFIABLE_OBJECT)``. The JS equivalent is
 * ``class Logger extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT)`` -- the mixin
 * inserts the CLI-capable behaviour at the same MRO position, and
 * ``instanceof _LAILA_CLI_CAPABLE_CLASS`` keeps working for every instance.
 */
import { BaseModel, Field, model_validator, finalize_model, private_attributes } from "../../_compat/pydantic.js";
import { RuntimeError, ValueError, ImportError } from "../../_compat/errors.js";
import { DotMap, is_dotmap } from "../../_compat/dotmap.js";
import { lazy } from "../../_compat/lazy.js";
import { repr } from "../../_compat/pyrepr.js";
import { isdict, is_plain_object, dict_items, dict_keys, dict_values, dict_get, dict_has, dict_set, dict_clear, len, type_name } from "../../_compat/pytypes.js";
import { PriorityQueue } from "../../_compat/queue.js";

/**
 * Drop-in replacement for ``Field`` that marks a field CLI-exempt.
 *
 * Exempt fields are skipped by both the ``laila.args`` auto-injection step in
 * ``_LAILA_CLI_CAPABLE_CLASS._resolve_from_cli_args`` and the environment
 * mirror produced by ``build_environment``. Use this for fields that should
 * never end up on a command-line surface or in a serialised environment dump
 * -- typically internal caches, mutable runtime state, or fields whose values
 * are derived rather than configured.
 *
 * Implementation detail: marks the field by stashing ``cli_exempt: true``
 * inside ``json_schema_extra``, which is the Pydantic-blessed extension point
 * and survives JSON-schema dumps.
 * @param {object} [opts] ``Field`` options
 */
export function CLIExempt(opts = {}) {
  const extra = { ...(opts.json_schema_extra ?? {}) };
  extra.cli_exempt = true;
  return Field({ ...opts, json_schema_extra: extra });
}

/**
 * Return ``true`` if a ``FieldInfo`` carries the CLI-exempt marker.
 *
 * Reads the ``cli_exempt`` flag stashed by ``CLIExempt`` on the field's
 * ``json_schema_extra`` dict.
 */
export function _is_cli_exempt(field_info) {
  const extra = field_info.json_schema_extra;
  if (isdict(extra)) return dict_get(extra, "cli_exempt", false);
  return false;
}

export const _SCOPE_TO_ARGS_PATH = {
  POLICY: "policy",
  CENTRAL_COMMAND: "policy.central.command",
  CENTRAL_MEMORY: "policy.central.memory",
  CENTRAL_COMMUNICATION: "policy.central.communication",
  POOL: "policy.central.memory.pools.{global_id}",
  POOL_ROUTER: "policy.central.memory.pool_router",
  TASK_FORCE: "policy.central.command.taskforces.{global_id}",
  COMM_PROTOCOL: "policy.central.communication.connections.{global_id}",
  LOGGER: "logger",
};

// --------------------------------------------------------------------------
// Mapping helpers (Python duck-typing: ``hasattr(x, "get")`` / ``isinstance(x, dict)``)
// --------------------------------------------------------------------------

/** Python ``isinstance(x, dict)`` -- plain object, Map, or DotMap (a dict subclass). */
function _isdict(x) {
  return isdict(x) || is_dotmap(x);
}

/** ``x.get(key) if hasattr(x, "get") else None`` for every dict-like shape. */
function _mget(x, key) {
  if (x === null || x === undefined) return null;
  if (is_dotmap(x) || x instanceof Map) return x.get(key) ?? null;
  if (is_plain_object(x)) return Object.prototype.hasOwnProperty.call(x, key) ? x[key] : null;
  if (typeof x.get === "function") return x.get(key) ?? null;
  return null;
}

/** ``hasattr(x, "keys")`` */
function _has_keys(x) {
  return x !== null && x !== undefined && (is_plain_object(x) || x instanceof Map || typeof x.keys === "function");
}

/** ``hasattr(x, "toDict")`` -> ``x.toDict()`` (best effort) */
function _to_dict_if_possible(x) {
  if (x !== null && x !== undefined && typeof x.toDict === "function") {
    try {
      return x.toDict();
    } catch {
      return x;
    }
  }
  return x;
}

/** Traverse ``laila.args`` using ``.get()`` to avoid DotMap auto-creation. */
export function _get_args_subtree(path) {
  try {
    const laila = lazy("laila");
    let subtree = laila.args;
    for (const part of path.split(".")) {
      if (subtree === null || subtree === undefined) return null;
      subtree = _mget(subtree, part);
    }
    if (subtree !== null && subtree !== undefined && _has_keys(subtree) && len(subtree) === 0) return null;
    return subtree ?? null;
  } catch (e) {
    if (e instanceof ImportError || e instanceof globalThis.TypeError || (e && e.name === "AttributeError")) return null;
    throw e;
  }
}

/**
 * Compute a global_id from constructor data for ``{global_id}`` path templates.
 *
 * Checks explicit ``data`` first, then falls back to the thread-local
 * ``_INIT_PENDING`` staging area used by the
 * ``_LAILA_IDENTIFIABLE_OBJECT`` constructor (which pops ``uuid`` from data
 * before Pydantic validation runs).
 */
export function _resolve_global_id_for_path(cls, data) {
  if (dict_has(data, "global_id")) return String(dict_get(data, "global_id"));

  let uuid_val = dict_get(data, "uuid", null);
  if (uuid_val === null) {
    try {
      const { _INIT_PENDING } = lazy("laila.basics.definitions.identifiable_object");
      uuid_val = _INIT_PENDING.uuid ?? null;
    } catch (e) {
      if (!(e instanceof ImportError)) throw e;
    }
  }
  if (uuid_val === null) return null;

  const scopes_attr = private_attributes(cls).get("_scopes");
  if (scopes_attr === undefined) return null;
  const scopes = scopes_attr.default_factory ? scopes_attr.default_factory() : ["OBJECT"];

  const { _LAILA_IDENTIFIABLE_OBJECT } = lazy("laila.basics.definitions.identifiable_object");
  return _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uuid_val, scopes });
}

/** Read the first scope from a class's ``_scopes`` PrivateAttr default. */
export function _scope_for_class(cls) {
  const scopes_attr = private_attributes(cls).get("_scopes");
  if (scopes_attr === undefined) return null;
  const scopes = scopes_attr.default_factory ? scopes_attr.default_factory() : null;
  if (scopes && scopes.length > 0) return scopes[0];
  return null;
}

/** Determine the ``laila.args`` path for a CLI-capable class instance. */
export function _resolve_path_for_class(cls, data) {
  const scope = _scope_for_class(cls);
  if (scope === null) return null;
  let path_template = _SCOPE_TO_ARGS_PATH[scope];
  if (path_template === undefined) return null;
  if (path_template.includes("{global_id}")) {
    const gid = _resolve_global_id_for_path(cls, data);
    if (gid === null) return null;
    path_template = path_template.replace("{global_id}", gid);
  }
  return path_template;
}

/** Return public, non-exempt Pydantic Field names for *cls*. */
export function _eligible_model_fields(cls) {
  const names = [];
  for (const [name, field_info] of Object.entries(cls.model_fields)) {
    if (name.startsWith("_")) continue;
    if (_is_cli_exempt(field_info)) continue;
    names.push(name);
  }
  return names;
}

/**
 * Return names of public property descriptors that have a setter.
 *
 * Mirrors ``dir(cls)``: every accessor along the prototype chain, sorted by
 * name.
 */
export function _eligible_property_setters(cls) {
  const names = new Set();
  const fields = cls.model_fields;
  let proto = cls.prototype;
  while (proto && proto !== Object.prototype) {
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (name.startsWith("_")) continue;
      if (names.has(name)) continue;
      const desc = Object.getOwnPropertyDescriptor(proto, name);
      if (desc && typeof desc.get === "function" && typeof desc.set === "function") {
        if (!(name in fields)) names.add(name);
      }
    }
    proto = Object.getPrototypeOf(proto);
  }
  return [...names].sort();
}

/**
 * Return a flat ``{class_name: class}`` map of *root* and every subclass.
 *
 * Walks ``__subclasses__()`` recursively, so it only sees classes that have
 * already been loaded in this process. Sub-classes that share a name shadow
 * each other (last wins) -- callers should ensure their registry uses
 * globally-unique class names.
 */
export function _all_subclasses(root) {
  const out = { [root.name]: root };
  for (const sub of root.__subclasses__()) Object.assign(out, _all_subclasses(sub));
  return out;
}

/**
 * Look up a subclass of *root* by ``__name__`` token.
 *
 * Raises ValueError listing the known tokens when *token* is unknown.
 */
export function _resolve_class(token, root) {
  const classes = _all_subclasses(root);
  if (!Object.prototype.hasOwnProperty.call(classes, token)) {
    const known = Object.keys(classes).sort().join(", ");
    throw new ValueError(`unknown ${root.name} subclass token: ${repr(token)} (known: ${known})`);
  }
  return classes[token];
}

function _getattr_or_null(obj, name) {
  try {
    const v = obj[name];
    return v === undefined ? null : v;
  } catch {
    return null;
  }
}

/**
 * Build a JSON-serializable environment dict from a live policy.
 *
 * Walks the policy tree and collects only CLI-eligible fields (public,
 * non-exempt, non-private). This is the same structure ``laila.args``
 * accepts.
 */
export function build_environment(policy) {
  function _dump_obj(obj) {
    const result = {};
    const cls = obj.constructor;
    for (const name of _eligible_model_fields(cls)) {
      const val = _getattr_or_null(obj, name);
      if (val !== null && val instanceof BaseModel) {
        result[name] = _dump_obj(val);
      } else if (_isdict(val)) {
        const nested = {};
        for (const [k, v] of dict_items(val)) {
          if (v instanceof BaseModel) nested[k] = _dump_obj(v);
          else nested[k] = v;
        }
        result[name] = nested;
      } else {
        result[name] = val;
      }
    }
    for (const name of _eligible_property_setters(cls)) {
      try {
        result[name] = _getattr_or_null(obj, name);
      } catch {
        /* ignore */
      }
    }
    return result;
  }

  function _dump_with_props(obj) {
    const data = _dump_obj(obj);
    for (const prop_name of _eligible_property_setters(obj.constructor)) {
      try {
        data[prop_name] = _getattr_or_null(obj, prop_name);
      } catch {
        /* ignore */
      }
    }
    return data;
  }

  const env = { policy: {} };
  env.policy = _dump_with_props(policy);

  if ("central" in policy) {
    const central = policy.central;
    if (!env.policy.central) env.policy.central = {};
    const central_env = env.policy.central;

    if (central && "command" in central && central.command !== null && central.command !== undefined) {
      const cmd = central.command;
      const cmd_data = _dump_with_props(cmd);
      if ("taskforces" in cmd) {
        const tf_data = {};
        for (const [tf_id, tf] of dict_items(cmd.taskforces)) {
          const tf_dump = _dump_with_props(tf);
          tf_dump.class_token = tf.constructor.name;
          tf_data[tf_id] = tf_dump;
        }
        cmd_data.taskforces = tf_data;
      }
      central_env.command = cmd_data;
    }

    if (central && "memory" in central && central.memory !== null && central.memory !== undefined) {
      const mem = central.memory;
      const mem_data = _dump_with_props(mem);
      if ("pool_router" in mem && mem.pool_router !== null && mem.pool_router !== undefined) {
        const router = mem.pool_router;
        const router_data = _dump_with_props(router);
        if ("pools" in router) {
          const pool_data = {};
          for (const [pool_id, pool] of dict_items(router.pools)) {
            const pool_dump = _dump_with_props(pool);
            pool_dump.class_token = pool.constructor.name;
            pool_data[pool_id] = pool_dump;
          }
          router_data.pools = pool_data;
        }
        mem_data.pool_router = router_data;
      }
      central_env.memory = mem_data;
    }

    if (central && "communication" in central && central.communication !== null && central.communication !== undefined) {
      const comm = central.communication;
      const comm_data = _dump_with_props(comm);
      if ("connections" in comm) {
        const conn_data = {};
        for (const [proto_id, proto] of dict_items(comm.connections)) {
          const proto_dump = _dump_with_props(proto);
          proto_dump.class_token = proto.constructor.name;
          conn_data[proto_id] = proto_dump;
        }
        comm_data.connections = conn_data;
      }
      central_env.communication = comm_data;
    }
  }

  return env;
}

// --------------------------------------------------------------------------
// The mixin
// --------------------------------------------------------------------------

const kCLICapable = Symbol("laila.cli_capable");

/**
 * Pydantic before-validator that injects values from ``laila.args``.
 *
 * Skipped silently when *data* is not a dict (e.g. when Pydantic is
 * round-tripping an existing model). When the class's ``_scopes`` does not
 * map to a known args path, also a no-op. Otherwise: walk the eligible model
 * fields, and for any that the caller did not pass explicitly, pull the
 * matching value out of the args subtree (treating empty ``DotMap`` nodes as
 * "absent" so user code can write ``laila.args.foo = {}`` without
 * unintentionally clearing defaults).
 */
function _resolve_from_cli_args(cls, data) {
  if (!_isdict(data)) return data;

  const path = _resolve_path_for_class(cls, data);
  if (path === null) return data;

  const subtree = _get_args_subtree(path);
  if (subtree === null) return data;

  for (const field_name of _eligible_model_fields(cls)) {
    if (dict_has(data, field_name)) continue;
    const val = _mget(subtree, field_name);
    if (val !== null) {
      if (is_dotmap(val) && len(val) === 0) continue;
      dict_set(data, field_name, val);
    }
  }

  return data;
}

/**
 * Update ``laila.args.environment.policies[<gid>]`` for this instance's owning policy.
 *
 * Best-effort: silently no-ops if ``laila`` is mid-import, if no owning
 * policy can be resolved, or if anything else goes wrong.
 */
function _refresh_environment_mirror(self) {
  try {
    _refresh_args_environment(self);
  } catch {
    /* best effort */
  }
  return self;
}

/**
 * Mixin producing the ``_LAILA_CLI_CAPABLE_CLASS`` behaviour on top of
 * ``Base`` (a ``BaseModel`` subclass). ``CLICapable(X)`` is the JS spelling of
 * Python's ``class Y(_LAILA_CLI_CAPABLE_CLASS, X)``.
 *
 * Inheriting opts into:
 *
 * - Auto-population of constructor kwargs from a matching subtree of
 *   ``laila.args`` (see module docstring for the exact resolution order).
 * - Auto-refresh of the corresponding entry under
 *   ``laila.args.environment.policies[<gid>]`` after every successful
 *   construction, so the live policy graph and the CLI mirror stay in sync.
 * - Mandatory-field enforcement via ``_cli_required_fields`` -- fields named
 *   there that remain ``null`` after the four-tier resolution raise
 *   ``RuntimeError``.
 *
 * No additional configuration is needed in the subclass: the args path is
 * derived automatically from the class's ``_scopes`` PrivateAttr (see
 * ``_SCOPE_TO_ARGS_PATH``). Only the rare cases where a field must always
 * come from somewhere need to set ``_cli_required_fields``.
 *
 * Class attributes
 * ----------------
 * ``static _cli_required_fields = new Set()`` -- names of fields that are
 * mandatory after resolution. Override in subclasses that need to fail loudly
 * instead of carrying a ``null`` default through to runtime.
 * @template {typeof BaseModel} B
 * @param {B} Base
 */
export function CLICapable(Base) {
  class _LAILA_CLI_CAPABLE_MIXIN extends Base {
    static _cli_required_fields = new Set();

    static {
      finalize_model(this);
      Object.defineProperty(this, "name", { value: `_LAILA_CLI_CAPABLE_CLASS(${Base.name})` });
      Object.defineProperty(this.prototype, kCLICapable, { value: true });
      model_validator(this, "before", _resolve_from_cli_args);
      model_validator(this, "after", _refresh_environment_mirror);
    }

    static [Symbol.hasInstance](x) {
      if (this === _LAILA_CLI_CAPABLE_CLASS) return !!(x !== null && x !== undefined && typeof x === "object" && x[kCLICapable] === true);
      return Function.prototype[Symbol.hasInstance].call(this, x);
    }

    /**
     * Pydantic post-init hook that backfills property setters and enforces
     * required fields.
     *
     * Runs after ``_resolve_from_cli_args``. Two responsibilities:
     *
     * - For each property on the class that has a setter (so users can
     *   reasonably configure it via ``laila.args``), look up and assign the
     *   matching value from the same args subtree used during field
     *   resolution. Failures are swallowed so a mis-typed property name does
     *   not break construction.
     * - Re-check ``_cli_required_fields``: any name that is still ``null``
     *   after both passes triggers a ``RuntimeError`` with a message pointing
     *   the user at the explicit / args / default escape hatches.
     */
    model_post_init(_context) {
      super.model_post_init(_context);

      const cls = this.constructor;
      const path = _resolve_path_for_class(cls, {});
      const subtree = path ? _get_args_subtree(path) : null;

      if (subtree !== null) {
        for (const prop_name of _eligible_property_setters(cls)) {
          if (prop_name.startsWith("_")) continue;
          const val = _mget(subtree, prop_name);
          if (val !== null) {
            if (is_dotmap(val) && len(val) === 0) continue;
            try {
              this[prop_name] = val;
            } catch {
              /* swallowed */
            }
          }
        }
      }

      for (const field_name of cls._cli_required_fields) {
        if ((this[field_name] ?? null) === null) {
          throw new RuntimeError(`${cls.name}.${field_name} is required but was not provided explicitly, via laila.args, or as a default.`);
        }
      }
    }
  }
  return _LAILA_CLI_CAPABLE_MIXIN;
}

/**
 * Base class enabling 4-tier parameter resolution from ``laila.args``.
 *
 * ``CLICapable(BaseModel)``: the plain-``BaseModel`` flavour, exactly what
 * Python's ``class _LAILA_CLI_CAPABLE_CLASS(BaseModel)`` is. ``instanceof``
 * against this class is true for every object produced by any ``CLICapable``
 * mixin, mirroring Python's multiple-inheritance ``isinstance``.
 */
export const _LAILA_CLI_CAPABLE_CLASS = CLICapable(BaseModel);
Object.defineProperty(_LAILA_CLI_CAPABLE_CLASS, "name", { value: "_LAILA_CLI_CAPABLE_CLASS" });

// --------------------------------------------------------------------------
// Environment round-trip
// --------------------------------------------------------------------------

/**
 * Return the policy that *instance* belongs to, or ``null``.
 *
 * Resolution order:
 * 1. *instance* is itself a ``_LAILA_IDENTIFIABLE_POLICY``.
 * 2. *instance* has a non-null ``policy_id`` registered in
 *    ``laila._local_policies``.
 * 3. The currently active local policy (``laila._active_policy_gid``).
 */
export function _find_owning_policy(instance) {
  let laila, _LAILA_IDENTIFIABLE_OBJECT;
  try {
    laila = lazy("laila");
    laila.args; // force resolution (ImportError when laila is not loaded)
    ({ _LAILA_IDENTIFIABLE_OBJECT } = lazy("laila.basics.definitions.identifiable_object"));
  } catch {
    return null;
  }

  let _LAILA_IDENTIFIABLE_POLICY = null;
  try {
    ({ _LAILA_IDENTIFIABLE_POLICY } = lazy("laila.policy.schema.base"));
  } catch {
    _LAILA_IDENTIFIABLE_POLICY = null;
  }

  if (_LAILA_IDENTIFIABLE_POLICY !== null && instance instanceof _LAILA_IDENTIFIABLE_POLICY) return instance;

  let policy_id = _getattr_or_null(instance, "policy_id");
  if (policy_id !== null) {
    if (policy_id instanceof _LAILA_IDENTIFIABLE_OBJECT) policy_id = policy_id.global_id;
    policy_id = String(policy_id);
    const local = laila._local_policies ?? {};
    const policy = dict_get(local, policy_id, null);
    if (policy !== null) return policy;
  }

  const active_gid = laila._active_policy_gid ?? null;
  if (active_gid !== null) {
    const local = laila._local_policies ?? {};
    const policy = dict_get(local, active_gid, null);
    if (policy !== null) return policy;
  }

  return null;
}

/** Return a shallow copy of *data* (dict-like) without ``class_token``. */
export function _strip_class_token(data) {
  if (data === null || data === undefined) return {};
  data = _to_dict_if_possible(data);
  if (!_isdict(data)) {
    try {
      data = Object.fromEntries(dict_items(data));
    } catch {
      return {};
    }
  }
  const out = {};
  for (const [k, v] of dict_items(data)) if (k !== "class_token") out[k] = v;
  return out;
}

/** Best-effort conversion of DotMap / nested dicts to plain dicts. */
export function _coerce_to_plain_dict(value) {
  if (value !== null && value !== undefined && typeof value.toDict === "function") {
    try {
      return value.toDict();
    } catch {
      /* fall through */
    }
  }
  if (_isdict(value)) {
    const out = {};
    for (const [k, v] of dict_items(value)) out[k] = _coerce_to_plain_dict(v);
    return out;
  }
  return value;
}

/**
 * Replace the entire laila runtime with the policies described by *env*.
 *
 * Implements the 4-rule active-policy resolution:
 *
 * 1. ``env.active_gid`` set: must appear in ``env.policies``; activate it.
 * 2. ``env.policies`` empty: create a fresh ``DefaultPolicy``, activate it.
 * 3. ``env.policies`` has exactly one entry: activate it.
 * 4. Otherwise (>=2 policies, no ``active_gid``): raise ``ValueError``.
 *
 * Reconstructs concrete subclasses for taskforces, pools, and communication
 * protocols using the ``class_token`` embedded by ``build_environment``.
 * Protocols are registered but **not** started -- callers must call
 * ``protocol.start()`` themselves.
 */
export function _load_environment(env) {
  const laila = lazy("laila");
  const { _LAILA_IDENTIFIABLE_OBJECT } = lazy("laila.basics.definitions.identifiable_object");

  env = _coerce_to_plain_dict(env);
  if (!_isdict(env)) throw new ValueError(`environment must be a dict, got ${type_name(env)}`);

  if (!dict_has(env, "policies")) throw new ValueError("environment must contain a 'policies' key");

  const policies_dump = dict_get(env, "policies", null) || {};
  if (!_isdict(policies_dump)) throw new ValueError(`environment.policies must be a dict, got ${type_name(policies_dump)}`);

  let active_gid = dict_get(env, "active_gid", null);
  if (active_gid !== null && len(policies_dump) === 0) throw new ValueError("active_gid is set but environment.policies is empty");

  laila.terminate({ wait: true, cancel_pending: false });

  if (len(policies_dump) === 0) {
    const { DefaultPolicy } = lazy("laila.macros.defaults");
    const new_policy = new DefaultPolicy();
    laila.activate_policy(new_policy);
    return;
  }

  const seen_uuids = new Set();
  const seen_proto_gids = new Set();
  const built_policies = [];

  try {
    for (let [policy_gid, policy_data] of dict_items(policies_dump)) {
      policy_data = _coerce_to_plain_dict(policy_data);
      if (!_isdict(policy_data)) throw new ValueError(`policy entry for ${repr(policy_gid)} must be a dict, got ${type_name(policy_data)}`);

      let ident;
      try {
        ident = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(policy_gid);
      } catch (e) {
        if (e instanceof ValueError) throw new ValueError(`invalid policy gid ${repr(policy_gid)}: ${e.message}`, { cause: e });
        throw e;
      }

      const policy_uuid = ident.uuid;
      if (seen_uuids.has(policy_uuid)) throw new ValueError(`duplicate policy uuid ${repr(policy_uuid)} in environment.policies`);
      seen_uuids.add(policy_uuid);

      const policy = _build_policy_from_dump({ policy_uuid, policy_data, seen_proto_gids });

      if (String(policy.global_id) !== String(policy_gid)) {
        try {
          _shutdown_orphan_policy(policy);
        } catch {
          /* ignore */
        }
        throw new ValueError(`reconstructed policy gid ${repr(policy.global_id)} does not match dumped key ${repr(policy_gid)}`);
      }

      dict_set(laila._local_policies, String(policy.global_id), policy);
      built_policies.push(policy);
    }
  } catch (e) {
    try {
      laila.terminate({ wait: true, cancel_pending: false });
    } finally {
      // eslint-disable-next-line no-unsafe-finally
      throw e;
    }
  }

  let chosen;
  if (active_gid !== null) {
    active_gid = String(active_gid);
    if (!dict_has(policies_dump, active_gid)) throw new ValueError(`active_gid ${repr(active_gid)} not present in environment.policies`);
    chosen = dict_get(laila._local_policies, active_gid);
  } else if (built_policies.length === 1) {
    chosen = built_policies[0];
  } else {
    throw new ValueError("active_gid required when env contains multiple policies");
  }

  laila.activate_policy(chosen);
}

/**
 * Best-effort tear-down of *policy* and all its sub-resources.
 *
 * Used when a policy was partially built but cannot be added to
 * ``laila._local_policies`` (e.g. its reconstructed global_id doesn't match
 * the dumped key, or one of its sub-instances failed to build).
 */
export function _shutdown_orphan_policy(policy) {
  try {
    const comm = policy?.central?.communication ?? null;
    if (comm !== null) {
      try {
        comm.stop();
      } catch {
        /* ignore */
      }
    }
  } catch {
    /* ignore */
  }
  try {
    const cmd = policy?.central?.command ?? null;
    if (cmd !== null) {
      try {
        cmd.shutdown({ wait: true, cancel_pending: false });
      } catch {
        /* ignore */
      }
    }
  } catch {
    /* ignore */
  }
  try {
    const mem = policy?.central?.memory ?? null;
    const router = mem !== null ? (mem.pool_router ?? null) : null;
    if (router !== null) {
      for (const pool of dict_values(router.pools ?? {})) {
        try {
          const close = pool.close;
          if (typeof close === "function") pool.close();
        } catch {
          /* ignore */
        }
      }
    }
  } catch {
    /* ignore */
  }
}

/**
 * Build one ``DefaultPolicy`` from a *policy_data* dump branch.
 *
 * Re-creates taskforces, pools, and comm-protocols using ``class_token`` to
 * dispatch to the right subclass. Protocols are registered without calling
 * ``start()``.
 * @param {{policy_uuid: string, policy_data: object, seen_proto_gids: Set<string>}} opts
 */
export function _build_policy_from_dump({ policy_uuid, policy_data, seen_proto_gids }) {
  const { _LAILA_IDENTIFIABLE_POOL } = lazy("laila.data.schema.base");
  const { DefaultPolicy } = lazy("laila.macros.defaults");
  const { _LAILA_IDENTIFIABLE_TASK_FORCE } = lazy("laila.policy.central.command.taskforce.base");
  const { _LAILA_IDENTIFIABLE_COMM_PROTOCOL } = lazy("laila.policy.central.communication.protocols.base");
  const { _LAILA_IDENTIFIABLE_OBJECT } = lazy("laila.basics.definitions.identifiable_object");

  const policy = new DefaultPolicy();
  if (policy_uuid !== policy.uuid) {
    policy._uuid = policy_uuid;
    const new_gid = policy.global_id;
    if (policy.central.command !== null && policy.central.command !== undefined) {
      try {
        policy.central.command.policy_id = new_gid;
      } catch {
        /* ignore */
      }
      for (const default_tf of dict_values(policy.central.command.taskforces)) {
        try {
          default_tf.policy_id = new_gid;
        } catch {
          /* ignore */
        }
      }
    }
    if (policy.central.communication !== null && policy.central.communication !== undefined) {
      try {
        policy.central.communication.policy_id = new_gid;
      } catch {
        /* ignore */
      }
    }
  }

  const _laila = lazy("laila");

  dict_set(_laila._local_policies, String(policy.global_id), policy);

  let central_data = dict_get(policy_data, "central", null) || {};
  central_data = _to_dict_if_possible(central_data);
  if (!_isdict(central_data)) central_data = {};

  let cmd_data = dict_get(central_data, "command", null) || {};
  cmd_data = _to_dict_if_possible(cmd_data);
  if (!_isdict(cmd_data)) cmd_data = {};

  let tf_dump = dict_get(cmd_data, "taskforces", null) || {};
  tf_dump = _to_dict_if_possible(tf_dump);

  if (_isdict(tf_dump) && len(tf_dump) > 0) {
    for (const default_tf of dict_values(policy.central.command.taskforces)) {
      try {
        if (typeof default_tf.shutdown === "function") default_tf.shutdown({ wait: true, cancel_pending: true });
      } catch {
        /* ignore */
      }
    }
    dict_clear(policy.central.command.taskforces);

    for (const [tf_gid, tf_data] of dict_items(tf_dump)) {
      const tf_data_plain = _coerce_to_plain_dict(tf_data);
      if (!_isdict(tf_data_plain)) throw new ValueError(`taskforce entry ${repr(tf_gid)} must be a dict`);
      const token = dict_get(tf_data_plain, "class_token", null);
      if (token === null) throw new ValueError(`taskforce entry ${repr(tf_gid)} is missing 'class_token'`);
      const cls = _resolve_class(token, _LAILA_IDENTIFIABLE_TASK_FORCE);
      const tf_kwargs = _strip_class_token(tf_data_plain);
      delete tf_kwargs.status;
      delete tf_kwargs.queue_len;
      let tf_uuid;
      try {
        tf_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(tf_gid).uuid;
      } catch (e) {
        if (e instanceof ValueError) throw new ValueError(`invalid taskforce gid ${repr(tf_gid)}: ${e.message}`, { cause: e });
        throw e;
      }
      tf_kwargs.uuid = tf_uuid;
      tf_kwargs.policy_id = policy.global_id;
      const tf = new cls(tf_kwargs);
      if (tf.uuid !== tf_uuid) tf._uuid = tf_uuid;
      policy.central.command.add_taskforce(tf);
    }

    const alpha_tf = dict_get(cmd_data, "alpha_taskforce", null);
    if (alpha_tf !== null && dict_has(policy.central.command.taskforces, String(alpha_tf))) {
      policy.central.command.alpha_taskforce = String(alpha_tf);
    } else {
      policy.central.command.alpha_taskforce = dict_keys(policy.central.command.taskforces)[0];
    }

    const internal_tf = dict_get(cmd_data, "internal_taskforce", null);
    if (internal_tf !== null && dict_has(policy.central.command.taskforces, String(internal_tf))) {
      policy.central.command.internal_taskforce = String(internal_tf);
    } else {
      // Environments dumped before the internal/alpha split (or with a
      // stale pointer) run laila's internals on the alpha taskforce.
      policy.central.command.internal_taskforce = policy.central.command.alpha_taskforce;
    }
  }

  let mem_data = dict_get(central_data, "memory", null) || {};
  mem_data = _to_dict_if_possible(mem_data);
  if (!_isdict(mem_data)) mem_data = {};

  let router_data = dict_get(mem_data, "pool_router", null) || {};
  router_data = _to_dict_if_possible(router_data);

  let pool_dump = _isdict(router_data) ? dict_get(router_data, "pools", null) : null;
  pool_dump = _to_dict_if_possible(pool_dump);

  if (_isdict(pool_dump) && len(pool_dump) > 0) {
    try {
      for (const old_pool of dict_values(policy.central.memory.pool_router.pools)) {
        const close = old_pool.close;
        if (typeof close === "function") {
          try {
            old_pool.close();
          } catch {
            /* ignore */
          }
        }
      }
    } catch {
      /* ignore */
    }

    dict_clear(policy.central.memory.pool_router.pools);
    try {
      policy.central.memory.pool_router.pools_pq = new PriorityQueue();
    } catch {
      /* ignore */
    }
    let nicks_in = dict_get(router_data, "pools_nicknames", null) || {};
    nicks_in = _to_dict_if_possible(nicks_in);
    nicks_in = _isdict(nicks_in) ? nicks_in : {};
    const gid_to_nick = {};
    for (const [nick, gid] of dict_items(nicks_in)) gid_to_nick[String(gid)] = nick;
    dict_clear(policy.central.memory.pool_router.pools_nicknames);

    let first_pool_gid = null;
    for (const [pool_gid, pool_data] of dict_items(pool_dump)) {
      const pool_data_plain = _coerce_to_plain_dict(pool_data);
      if (!_isdict(pool_data_plain)) throw new ValueError(`pool entry ${repr(pool_gid)} must be a dict`);
      const token = dict_get(pool_data_plain, "class_token", null);
      if (token === null) throw new ValueError(`pool entry ${repr(pool_gid)} is missing 'class_token'`);
      const cls = _resolve_class(token, _LAILA_IDENTIFIABLE_POOL);
      const pool_kwargs = _strip_class_token(pool_data_plain);
      delete pool_kwargs.pool_id;
      let pool_uuid;
      try {
        pool_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(pool_gid).uuid;
      } catch (e) {
        if (e instanceof ValueError) throw new ValueError(`invalid pool gid ${repr(pool_gid)}: ${e.message}`, { cause: e });
        throw e;
      }
      pool_kwargs.uuid = pool_uuid;
      const pool = new cls(pool_kwargs);
      if (pool.uuid !== pool_uuid) pool._uuid = pool_uuid;
      const nickname = dict_get(gid_to_nick, String(pool.global_id), null);
      policy.central.memory.pool_router.extend(pool, { affinity: null, pool_nickname: nickname });
      if (first_pool_gid === null) first_pool_gid = String(pool.global_id);
    }

    const alpha = dict_get(mem_data, "alpha_pool", null);
    if (alpha !== null && dict_has(policy.central.memory.pool_router.pools, String(alpha))) {
      policy.central.memory.alpha_pool = String(alpha);
    } else if (first_pool_gid !== null) {
      policy.central.memory.alpha_pool = first_pool_gid;
    }
  }

  let comm_data = dict_get(central_data, "communication", null) || {};
  comm_data = _to_dict_if_possible(comm_data);
  if (!_isdict(comm_data)) comm_data = {};

  let conn_dump = dict_get(comm_data, "connections", null) || {};
  conn_dump = _to_dict_if_possible(conn_dump);
  if (_isdict(conn_dump) && len(conn_dump) > 0) {
    for (const [proto_gid, proto_data] of dict_items(conn_dump)) {
      const proto_data_plain = _coerce_to_plain_dict(proto_data);
      if (!_isdict(proto_data_plain)) throw new ValueError(`protocol entry ${repr(proto_gid)} must be a dict`);
      const token = dict_get(proto_data_plain, "class_token", null);
      if (token === null) throw new ValueError(`protocol entry ${repr(proto_gid)} is missing 'class_token'`);
      const cls = _resolve_class(token, _LAILA_IDENTIFIABLE_COMM_PROTOCOL);
      const proto_kwargs = _strip_class_token(proto_data_plain);
      let proto_uuid;
      try {
        proto_uuid = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(proto_gid).uuid;
      } catch (e) {
        if (e instanceof ValueError) throw new ValueError(`invalid protocol gid ${repr(proto_gid)}: ${e.message}`, { cause: e });
        throw e;
      }
      proto_kwargs.uuid = proto_uuid;
      const proto = new cls(proto_kwargs);
      if (proto.uuid !== proto_uuid) proto._uuid = proto_uuid;
      const full_gid = String(proto.global_id);
      if (seen_proto_gids.has(full_gid)) throw new ValueError(`duplicate protocol gid ${repr(full_gid)} across policies`);
      seen_proto_gids.add(full_gid);
      proto._communication = policy.central.communication;
      dict_set(policy.central.communication.connections, full_gid, proto);
    }
  }

  return policy;
}

/**
 * Write the owning policy's ``build_environment`` dump under
 * ``laila.args.environment.policies[<gid>]``.
 *
 * For a top-level singleton like the ``laila.logger.Logger``, which has no
 * owning policy, the dump is written under ``laila.args.environment.logger``
 * instead.
 */
export function _refresh_args_environment(instance) {
  const laila = lazy("laila");

  if (_scope_for_class(instance.constructor) === "LOGGER") {
    const args = laila.args;
    let env = typeof args.get === "function" ? args.get("environment") : null;
    if (!is_dotmap(env)) {
      env = new DotMap();
      args.environment = env;
    }

    const dump = { class_token: instance.constructor.name };
    for (const name of _eligible_model_fields(instance.constructor)) {
      try {
        dump[name] = _getattr_or_null(instance, name);
      } catch {
        /* ignore */
      }
    }
    for (const name of _eligible_property_setters(instance.constructor)) {
      try {
        dump[name] = _getattr_or_null(instance, name);
      } catch {
        /* ignore */
      }
    }
    env.logger = new DotMap(dump);
    return;
  }

  const policy = _find_owning_policy(instance);
  if (policy === null) return;

  const args = laila.args;
  let env = typeof args.get === "function" ? args.get("environment") : null;
  if (!is_dotmap(env)) {
    env = new DotMap();
    args.environment = env;
  }

  let policies = typeof env.get === "function" ? env.get("policies") : null;
  if (!is_dotmap(policies)) {
    policies = new DotMap();
    env.policies = policies;
  }

  const dump = build_environment(policy);
  policies[String(policy.global_id)] = new DotMap(dump.policy);
}
