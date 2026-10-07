/**
 * Laila top-level package -- Lambda's Interdisciplinary Large Atlas.
 *
 * This module is the single, opinionated entry point that user code imports
 * as ``import laila from "laila-core"``. Every other capability -- storage
 * pools, task-forces, peer-to-peer communication, futures, manifests,
 * constitutions, and the CLI/TOML environment loader -- is reachable either
 * as a public attribute on the default export or as a named export defined
 * below.
 *
 * Major surface
 * -------------
 * **Subsystem shortcuts** (resolved against the *active* policy):
 *
 * - ``laila.memory`` -> ``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``
 * - ``laila.command`` -> ``_LAILA_IDENTIFIABLE_CENTRAL_COMMAND``
 * - ``laila.communication`` -> ``_LAILA_IDENTIFIABLE_COMMUNICATION``
 * - ``laila.peers`` -> ``PeerRegistry`` (a dict) of ``PeerProxy`` keyed by
 *   ``global_id``; ``laila.peers[gid].channel(name)`` is a stream ``Channel``
 *   consumed with ``relay``
 * - ``laila.alpha_pool`` -> the active policy's default storage pool
 * - ``laila.runtime`` -> the ``laila/runtime`` module (futures introspection)
 * - ``laila.logger`` -> the process-wide ``Logger`` singleton
 *
 * **Active policy management**:
 *
 * - ``laila.active_policy`` (read/write) -- get or set the active policy.
 *   Setting accepts both local policies and remote ``RemotePolicyProxy``
 *   objects, enabling "morph" mode where local code transparently routes
 *   through a peer.
 * - ``activate_policy`` / ``get_active_policy`` -- explicit forms.
 * - ``get_active_namespace`` / ``set_active_namespace`` -- control the UUID-5
 *   namespace used to derive deterministic IDs from nicknames.
 * - ``laila.local_policies`` / ``laila.remote_policies`` / ``laila.universe``
 *   -- enumerate every policy reachable from this process.
 *
 * **High-level memory operations** (delegate to the active policy's central
 * memory; ``memorize`` / ``remember`` also accept ``policy_id`` to target a
 * connected peer's memory):
 *
 * - ``memorize`` -- write entries to the routed pool (or a peer's pool via
 *   ``policy_id``).
 * - ``remember`` -- read entries (optionally caching into the alpha pool;
 *   read from a peer via ``policy_id``).
 * - ``forget``  -- delete entries.
 * - ``build``   -- materialize an entry by running its constitution.
 *
 * **Lifecycle and configuration**:
 *
 * - ``terminate`` -- best-effort, idempotent tear-down of every subsystem
 *   this process owns.
 * - ``read_args`` -- load TOML/JSON/.env/.xml/CLI args into ``laila.args``.
 * - ``laila.args`` -- a ``_LailaArgs`` (DotMap subclass) used by every
 *   ``_LAILA_CLI_CAPABLE_CLASS`` for 4-tier parameter resolution. Assigning
 *   to ``laila.args.environment`` triggers a full environment reload.
 * - ``set_default_directory`` -- relocate the on-disk root used by pools,
 *   logs, and secrets.
 *
 * **Networking helpers**:
 *
 * - ``add_peer`` -- handshake with a remote policy and return its gid.
 *
 * Implementation notes
 * --------------------
 * Python swaps the module's ``__class__`` so that ``laila.memory`` /
 * ``laila.active_policy`` are backed by ``property`` descriptors. The JS
 * default export is a plain object whose accessor properties play the same
 * role: every read of ``laila.memory`` re-resolves through the *current*
 * active policy, and ``laila.active_policy = p`` routes through
 * ``activate_policy``. Keyword arguments travel as a trailing options
 * object (``laila.memorize(entry, { dst_pool: "cold" })``).
 *
 * The module also installs a null logging handler at import time so that
 * applications which never call ``laila.enable_logging`` do not see
 * "No handlers could be found" warnings.
 */
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

import { DotMap } from "./_compat/dotmap.js";
import { ConnectionError, RuntimeError, TypeError as PyTypeError, ValueError, KeyError } from "./_compat/errors.js";
import { register } from "./_compat/lazy.js";
import { dict_get, dict_has, dict_items, hasattr, isdict, is_plain_object, len, str, PyTuple } from "./_compat/pytypes.js";
import { NAMESPACE_DNS, uuid5 } from "./_compat/uuid.js";
import { to_thread } from "./_compat/asyncio.js";

import * as atomic from "./atomic/index.js";
import * as basics from "./basics/index.js";
import * as data from "./data/index.js";
import * as entry from "./entry/index.js";
import * as macros from "./macros/index.js";
import * as aliases from "./macros/aliases.js";
import * as defaults from "./macros/defaults.js";
import * as policy from "./policy/index.js";
import * as runtime from "./runtime/index.js";
import * as utils from "./utils/index.js";
import * as TaskForce from "./policy/central/command/taskforce/index.js";

import { Entry, transformation_base64 } from "./entry/index.js";
import { build_by_scope } from "./entry/constitution/build_maps.js";
import { Manifest } from "./policy/central/memory/schema/manifest.js";
import { Logger, _install_null_handler as _install_logger_null_handler, disable_logging, enable_logging, get_logger, set_log_level } from "./logger/index.js";
import { _ENTRY_SCOPE, _POLICY_SCOPE } from "./macros/strings.js";
import { _LAILA_IDENTIFIABLE_POLICY } from "./policy/schema/base.js";
import { guarantee, guarantee_async } from "./utils/guarantee.js";
import { ArgReader } from "./utils/args/args.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "./basics/definitions/identifiable_object.js";
import { _load_environment, _refresh_args_environment } from "./basics/definitions/cli_capable.js";
import { _live_taskforces_snapshot } from "./policy/central/command/taskforce/base.js";
import { _RESOLVE_CHAIN, check_resolve_cycle } from "./policy/central/command/schema/parking.js";
import { Future } from "./policy/central/command/schema/future/future/future.js";
import { _LAILA_IDENTIFIABLE_FUTURE } from "./policy/central/command/schema/future/future/future_identity.js";
import { GroupFuture } from "./policy/central/command/schema/future/future/group_future.js";
import { RemoteFuture } from "./policy/central/command/schema/future/future/remote_future.js";
import { Channel as _Channel } from "./policy/central/communication/channel.js";
import { RemotePolicyProxy } from "./policy/central/communication/proxy.js";

export { atomic, basics, data, entry, macros, policy, runtime, utils, TaskForce };
export { Entry, Manifest, Logger, disable_logging, enable_logging, get_logger, set_log_level, guarantee, guarantee_async, ArgReader, _LAILA_IDENTIFIABLE_POLICY, _ENTRY_SCOPE };
export const manifest = Manifest;

// ``from .macros.aliases import *`` / ``from .macros.defaults import *``
export const { constant, variable, contingent, future } = aliases;
export * from "./macros/defaults.js";

const _require = createRequire(import.meta.url);
let __version__;
try {
  __version__ = _require("../package.json").version;
} catch {
  __version__ = "0.0.0+local";
}
export { __version__ };

_install_logger_null_handler();


/**
 * Decide whether assigning *value* to ``laila.args.environment`` should
 * trigger a full ``_load_environment`` cycle.
 *
 * The ``args.environment`` slot is overloaded -- it serves two roles that
 * share the same key, and we must distinguish them at write time to avoid an
 * infinite loop:
 *
 * 1. **User-driven full reload.** A user (or a CLI / TOML loader) assigns a
 *    *fully-populated* environment payload to ``laila.args.environment``. We
 *    must call ``_load_environment`` which tears down the current process
 *    state and rebuilds every policy described by the payload.
 * 2. **Internal mirror update.** Whenever a CLI-capable class is constructed
 *    or mutated, ``_refresh_args_environment`` writes a snapshot of the
 *    owning policy back to ``laila.args.environment.policies[<gid>]``. That
 *    write must NOT re-enter ``_load_environment``.
 *
 * The two cases are distinguished as follows:
 *
 * - A *plain* dict with at least one key is always treated as a user-driven
 *   payload. (``_refresh_args_environment`` only ever writes ``DotMap``
 *   instances, never plain dicts.)
 * - A ``DotMap`` is treated as a load trigger only when it carries a
 *   non-empty ``policies`` mapping or an ``active_gid``.
 * - Anything else (non-mapping, ``null``, empty dict) is inert and silently
 *   passes through to ``DotMap``'s normal setter.
 *
 * @param {any} value The proposed new value for ``laila.args.environment``.
 * @returns {boolean}
 */
export function _is_env_load_trigger(value) {
  if (value instanceof DotMap) {
    let policies, active;
    try {
      policies = value.get("policies");
      active = value.get("active_gid");
    } catch {
      return false;
    }
    if (active !== null && active !== undefined) return true;
    if (policies === null || policies === undefined) return false;
    try {
      return len(policies) > 0;
    } catch {
      return false;
    }
  }
  if (isdict(value)) return len(value) > 0;
  return false;
}


/**
 * ``DotMap`` subclass that watches for assignments to ``environment`` and
 * triggers a full process-wide reload when one arrives.
 *
 * ``laila.args`` is a single instance of this class created at import time.
 * Almost every CLI-capable model in the package walks ``laila.args`` during
 * validation to fill in defaults (see ``_LAILA_CLI_CAPABLE_CLASS``). The
 * ``environment`` key, however, is special: it stores a machine-readable
 * snapshot of the *current* runtime and is also the input format for
 * restoring that runtime on another process or after ``terminate``.
 *
 * Setting ``laila.args.environment = my_env`` where ``my_env`` carries a
 * non-empty ``policies`` mapping (or an ``active_gid``) triggers a cascade:
 *
 * 1. ``terminate`` is invoked to shut down every existing policy.
 * 2. ``_load_environment`` walks ``my_env.policies`` and, using the
 *    ``class_token`` recorded for each taskforce, pool, and protocol,
 *    instantiates the appropriate concrete subclasses with the recorded
 *    UUIDs preserved.
 * 3. The chosen policy is activated.
 *
 * Plain dict -> ``DotMap`` coercion is also performed for any other key, so
 * users can write ``laila.args.foo = { bar: 1 }`` and immediately access
 * ``laila.args.foo.bar``.
 */
export class _LailaArgs extends DotMap {
  __setattr__(key, value) {
    if (key === "environment" && _is_env_load_trigger(value)) {
      _load_environment(value);
      return;
    }
    if (isdict(value) && !(value instanceof DotMap)) value = new DotMap(value);
    super.__setattr__(key, value);
  }

  __setitem__(key, value) {
    if (key === "environment" && _is_env_load_trigger(value)) {
      _load_environment(value);
      return;
    }
    if (isdict(value) && !(value instanceof DotMap)) value = new DotMap(value);
    super.__setitem__(key, value);
  }
}


/**
 * The process-wide ``_LailaArgs`` instance. A live binding: ``laila.args =
 * new _LailaArgs()`` (as the Python test-suites do to isolate state)
 * rebinds this export too.
 */
export let args = new _LailaArgs();

export const arg_reader = new ArgReader(args);

export const _local_policies = {};
export const _remote_policies = {};

let _active_policy_gid = null;
let _active_namespace = null;


/**
 * Gracefully tear down everything ``laila`` has spawned in this process.
 *
 * This is the canonical "go back to a clean slate" call. It is safe to call
 * from anywhere -- including inside an exception handler -- and is
 * automatically invoked at the start of every full environment reload.
 *
 * Order of operations, for each policy in ``_local_policies`` (a snapshot is
 * taken first so concurrent mutation is harmless):
 *
 * 1. ``policy.central.communication.stop()``
 * 2. ``policy.central.command.shutdown({ wait, cancel_pending })``
 * 3. ``pool.close()`` for each pool in ``policy.central.memory.pool_router.pools``
 *
 * After the per-policy sweep:
 *
 * 4. ``_local_policies``, ``_remote_policies`` and ``_active_policy_gid`` are
 *    cleared.
 * 5. *Orphan* taskforces are looked up through ``_live_taskforces_snapshot``
 *    and shut down.
 * 6. The ``Logger`` singleton is reset.
 * 7. The ``laila.args.environment.policies`` / ``.logger`` mirrors are
 *    cleared.
 *
 * Each step is wrapped in its own ``try``/``catch`` so that a failure in one
 * subsystem does not prevent the others from being torn down. Failures are
 * recorded in the returned list as short ``"<step>[<id>]: <repr>"`` strings;
 * this function never raises directly.
 *
 * @param {{wait?: boolean, cancel_pending?: boolean}} [opts]
 * @returns {string[]} One short error string per failed step.
 */
export function terminate(opts = {}) {
  const { wait = true, cancel_pending = false } = opts;
  const errors = [];

  for (const [gid, pol] of Object.entries({ ..._local_policies })) {
    try {
      const comm = pol?.central?.communication ?? null;
      if (comm !== null) comm.stop();
    } catch (e) {
      errors.push(`communication.stop[${gid}]: ${_repr_exc(e)}`);
    }

    try {
      const cmd = pol?.central?.command ?? null;
      if (cmd !== null) cmd.shutdown({ wait, cancel_pending });
    } catch (e) {
      errors.push(`command.shutdown[${gid}]: ${_repr_exc(e)}`);
    }

    try {
      const mem = pol?.central?.memory ?? null;
      const router = mem !== null ? (mem.pool_router ?? null) : null;
      if (router !== null) {
        for (const [pool_id, pool] of dict_items(router.pools ?? {})) {
          try {
            const close = pool?.close;
            if (typeof close === "function") close.call(pool);
          } catch (e) {
            errors.push(`pool.close[${pool_id}]: ${_repr_exc(e)}`);
          }
        }
      }
    } catch (e) {
      errors.push(`memory.pools[${gid}]: ${_repr_exc(e)}`);
    }
  }

  for (const k of Object.keys(_local_policies)) delete _local_policies[k];
  for (const k of Object.keys(_remote_policies)) delete _remote_policies[k];
  _active_policy_gid = null;

  try {
    for (const tf of _live_taskforces_snapshot()) {
      try {
        tf.shutdown({ wait, cancel_pending });
      } catch (e) {
        errors.push(`orphan_taskforce.shutdown[${tf?.global_id ?? "?"}]: ${_repr_exc(e)}`);
      }
    }
  } catch (e) {
    errors.push(`orphan_taskforce sweep: ${_repr_exc(e)}`);
  }

  try {
    Logger.reset_singleton();
  } catch (e) {
    errors.push(`logger.reset_singleton: ${_repr_exc(e)}`);
  }

  try {
    const env = typeof args.get === "function" ? args.get("environment") : null;
    if (env !== null && env !== undefined && typeof env.get === "function") {
      const policies = env.get("policies");
      if (policies !== null && policies !== undefined && typeof policies.clear === "function") policies.clear();
      try {
        if (typeof env.pop === "function") env.pop("logger", null);
        else if ("logger" in env) delete env.logger;
      } catch {
        /* best effort */
      }
    }
  } catch (e) {
    errors.push(`clear environment mirror: ${_repr_exc(e)}`);
  }

  return errors;
}

/** ``f"{e!r}"`` for the best-effort error strings above. */
function _repr_exc(e) {
  if (e !== null && typeof e === "object" && typeof e.__repr__ === "function") return e.__repr__();
  if (e instanceof Error) return `${e.name}(${JSON.stringify(e.message)})`;
  return String(e);
}


/**
 * Load user arguments from a file or terminal flags into ``laila.args``.
 *
 * A thin wrapper around the singleton ``ArgReader`` bound to the live
 * ``laila.args`` instance. Supported sources: ``.toml``, ``.json``, ``.env``,
 * ``.xml`` files, or the literal string ``"terminal"`` (consume
 * ``process.argv`` or *terminal_args* as ``--key value`` / ``--key=value``
 * pairs).
 *
 * Mutates ``laila.args`` in place (existing keys are *merged*). If the loaded
 * payload contains an ``environment`` key with a non-empty ``policies``
 * mapping, the assignment hook fires and ``_load_environment`` rebuilds the
 * entire policy graph.
 *
 * @param {string} source
 * @param {{terminal_args?: string[]|null}} [opts]
 */
export function read_args(source, opts = {}) {
  const { terminal_args = null } = opts;
  arg_reader.load(source, { terminal_args });
}


/**
 * Return the UUID-5 namespace used to derive deterministic IDs from nicknames.
 *
 * On first access the namespace is initialized to
 * ``LAILA_UNIVERSAL_NAMESPACE``. Override it via ``set_active_namespace``.
 */
export function get_active_namespace() {
  if (_active_namespace === null) _active_namespace = defaults.LAILA_UNIVERSAL_NAMESPACE;
  return _active_namespace;
}


/**
 * Replace the active UUID-5 namespace with one derived from *namespace_key*
 * (``uuid5(NAMESPACE_DNS, namespace_key)``).
 * @param {string} namespace_key
 */
export function set_active_namespace(namespace_key) {
  _active_namespace = uuid5(NAMESPACE_DNS, namespace_key);
}


/**
 * Return the active policy, lazily creating a ``DefaultPolicy`` on first
 * access.
 *
 * 1. If no policy has been activated yet, instantiate a fresh
 *    ``DefaultPolicy`` and activate it.
 * 2. If the active gid maps to a *local* policy, return that instance.
 * 3. Otherwise the active gid maps to a *remote* peer; return the proxy
 *    ("morph mode").
 */
export function get_active_policy() {
  if (_active_policy_gid === null) activate_policy(new defaults.DefaultPolicy());
  if (_active_policy_gid in _local_policies) return _local_policies[_active_policy_gid];
  return _remote_policies[_active_policy_gid];
}


/**
 * Replace the active policy with *policy*.
 *
 * Mutates ``_active_policy_gid`` and, when *policy* is a *local* policy,
 * ensures it is registered in ``_local_policies``. Equivalent to
 * ``laila.active_policy = policy``. The args-environment refresh is
 * best-effort (non-fatal).
 *
 * @param {any} policy ``_LAILA_IDENTIFIABLE_POLICY`` or ``RemotePolicyProxy``.
 */
export function activate_policy(policy) {
  const new_gid = str(policy.global_id);
  _active_policy_gid = new_gid;

  if (policy instanceof _LAILA_IDENTIFIABLE_POLICY) _local_policies[new_gid] = policy;

  try {
    _refresh_args_environment(policy);
  } catch {
    /* non-fatal: the in-memory policy state is the source of truth */
  }
}


/**
 * Return the local policy that should own newly-created futures.
 *
 * 1. If no policy is active yet, lazily activate a ``DefaultPolicy``.
 * 2. If the active gid maps to a *local* policy, return it.
 * 3. Otherwise (active policy is a remote proxy): if exactly one local
 *    policy exists, return it; else raise ``RuntimeError``.
 */
export function _get_active_local_policy() {
  if (_active_policy_gid === null) get_active_policy();
  if (_active_policy_gid in _local_policies) return _local_policies[_active_policy_gid];
  const locals = Object.values(_local_policies);
  if (locals.length === 1) return locals[0];
  throw new RuntimeError(
    "No local policy available for future registration; set `laila.active_policy` to a local policy first.",
  );
}


/**
 * Materialize *entry* by running its constitution on a taskforce.
 *
 * Always submits the entry's ``_build_async`` coroutine to the chosen
 * taskforce (alpha by default). On completion the entry is mutated in place
 * (``_payload`` populated, ``_constitution`` cleared, ``_state`` -> READY)
 * and the future also resolves to the same entry instance.
 *
 * Raises ``CyclicDependencyError`` *synchronously* when this call is made
 * from inside a resolution (build / remember) of the same entry.
 *
 * @param {Entry} entry
 * @param {{taskforce_id?: string|null}} [opts]
 * @returns {any} Future identity that resolves to the (now-built) entry.
 */
export function build(entry_, opts = {}) {
  const { taskforce_id = null } = opts;

  // A build that (transitively, through its constitution body) triggers a
  // build of the same entry can never finish; fail fast instead.
  check_resolve_cycle(entry_.global_id);

  const _build_on_chain = async () => {
    const token = _RESOLVE_CHAIN.set([..._RESOLVE_CHAIN.get(), entry_.global_id]);
    try {
      return await entry_._build_async();
    } finally {
      _RESOLVE_CHAIN.reset(token);
    }
  };

  const command = get_active_policy().central.command;
  return command.submit([_build_on_chain], { taskforce_id });
}


/**
 * Route a memory op to a remote peer as a LOCAL (A-owned) Future.
 *
 * With the *current* active policy A (not morphed), asking peer B to
 * memorize/remember produces a normal local ``Future`` / ``GroupFuture``
 * owned by A -- not a ``RemoteFuture``. The over-the-wire transfer runs
 * inside one A-side task per entry; each task offloads the blocking wire RPC
 * with ``to_thread`` so it never blocks the taskforce event loop.
 *
 * Entries cross the wire as their canonical, self-describing
 * ``Entry.serialize(transformation_base64)`` blob and are rebuilt via
 * ``build_by_scope``.
 */
function _route_memory_to_peer(proxy, op, call_args, kwargs) {
  const pool = kwargs.pool ?? null;
  if (pool !== null && typeof pool !== "string") {
    throw new PyTypeError(
      "Remote memory ops need a pool gid or nickname *string* for the " +
        "peer-side pool; a standalone pool object cannot be shipped to a peer.",
    );
  }
  const mem_kwargs = pool !== null ? { pool } : {};
  const command = get_active_policy().central.command;

  if (op === "memorize") {
    const entries = call_args.length ? call_args[0] : kwargs.entries;
    const entries_list = Array.isArray(entries) ? entries : [entries];

    const _make_store = (e) => {
      const blob = e.serialize(transformation_base64);
      return async function _store() {
        const gids = await to_thread(() => proxy.central.memory._remote_memorize([blob], mem_kwargs));
        return gids[0];
      };
    };

    return command.submit(entries_list.map(_make_store), { taskforce_id: command.internal_taskforce });
  }

  if (op === "remember") {
    const entry_ids = call_args.length ? call_args[0] : kwargs.entry_ids;
    const ids_list = Array.isArray(entry_ids) ? entry_ids : [entry_ids];
    const ids = ids_list.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));

    const _make_fetch = (eid) =>
      async function _fetch() {
        const blobs = await to_thread(() => proxy.central.memory._remote_remember([eid], mem_kwargs));
        return build_by_scope(blobs[0], { asynchronous: false });
      };

    return command.submit(ids.map(_make_fetch), { taskforce_id: command.internal_taskforce });
  }

  if (op === "forget") {
    const entry_ids = call_args.length ? call_args[0] : kwargs.entry_ids;
    const ids_list = Array.isArray(entry_ids) ? entry_ids : [entry_ids];
    const ids = ids_list.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));

    const _delete = () => proxy.central.memory._remote_forget(ids, mem_kwargs);

    async function _delete_async() {
      return await to_thread(_delete);
    }

    return command.submit([_delete_async], { taskforce_id: command.internal_taskforce });
  }

  // Any other op: pass straight through (gids are JSON-safe).
  return proxy.central.memory[op](...call_args, kwargs);
}


/**
 * Run a central-memory operation against an arbitrary policy by ``global_id``.
 *
 * The engine behind the ``policy_id`` argument on ``memorize`` / ``remember``.
 * The target may be a *remote* peer (dispatched via the proxy's attribute
 * chain, local active policy unchanged) or another *local* policy in this
 * process (the active policy is *transiently* morphed into the target and
 * restored afterwards).
 *
 * @throws {ConnectionError} If *policy_id* does not name any known policy.
 */
function _route_to_policy(policy_id, op, call_args, kwargs, comm = null) {
  const pid = str(policy_id);
  const target = { ..._remote_policies, ..._local_policies }[pid] ?? null;
  if (target === null) {
    throw new ConnectionError(
      `Unknown policy_id ${JSON.stringify(pid)}: not a local policy or a connected peer. ` +
        "Connect to the peer first with laila.add_peer()/add_tcpip_peer().",
    );
  }

  if (target instanceof RemotePolicyProxy) {
    const t = comm !== null ? target.via(comm) : target;
    return _route_memory_to_peer(t, op, call_args, kwargs);
  }

  const previous_gid = _active_policy_gid;
  activate_policy(target);
  try {
    const memory = get_active_policy().central.memory;
    return memory[op](...call_args, kwargs);
  } finally {
    _active_policy_gid = previous_gid;
  }
}


/**
 * Resolve a policy reference to a ``global_id`` string, or ``null``.
 *
 * Accepts ``null``; a live policy / ``RemotePolicyProxy`` (anything exposing
 * ``global_id``); a global-id string; a scoped shorthand such as
 * ``"POLICY:trainer"``; or any other string, treated as a policy *nickname*
 * and turned into a deterministic ``LAILA:POLICY:<uuid5>``.
 */
export function _resolve_policy_ref(ref) {
  if (ref === null || ref === undefined) return null;
  const gid = ref !== null && (typeof ref === "object" || typeof ref === "function") ? ref.global_id : undefined;
  if (typeof gid === "string") return gid;
  return _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(str(ref), { default_scopes: [_POLICY_SCOPE], parse_evolution: false });
}


/** Pull a list of entry global_ids out of a ``(entry_ids,)`` arg shape. */
function _extract_gids(call_args, kwargs) {
  const val = call_args.length ? call_args[0] : (kwargs.entry_ids ?? kwargs.entries);
  const items = Array.isArray(val) ? val : [val];
  return items.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));
}


/**
 * Drive a 3-party transfer by commanding the *source* policy.
 *
 * With active policy A, ``src_gid`` = B, ``dst_gid`` = C: A reaches B (A must
 * be peered to B) and asks B to move the entries to/from C over B's *own*
 * B<->C link. A never brokers the B<->C connection.
 */
function _relay_transfer(verb, src_gid, call_args, kwargs, opts) {
  const { src_pool, dst_gid, dst_pool, comm, persist = true } = opts;

  const src = { ..._remote_policies, ..._local_policies }[src_gid] ?? null;
  if (src === null) {
    throw new ConnectionError(
      `src_policy ${JSON.stringify(src_gid)} is not a connected peer or a local policy. ` +
        "The active policy must be peered to the source policy " +
        "(connect first with laila.add_peer()).",
    );
  }
  const gids = _extract_gids(call_args, kwargs);

  if (src instanceof RemotePolicyProxy) {
    const relay_ = (comm !== null ? src.via(comm) : src).central.memory;
    if (verb === "memorize") {
      return relay_._relay_memorize(gids, { src_pool, dst_policy: dst_gid, dst_pool, comm });
    }
    return relay_._relay_remember(gids, { dst_policy: dst_gid, dst_pool, src_pool, comm, persist });
  }

  const previous_gid = _active_policy_gid;
  activate_policy(src);
  try {
    const memory = get_active_policy().central.memory;
    if (verb === "memorize") {
      return memory._relay_memorize(gids, { src_pool, dst_policy: dst_gid, dst_pool, comm });
    }
    return memory._relay_remember(gids, { dst_policy: dst_gid, dst_pool, src_pool, comm, persist });
  } finally {
    _active_policy_gid = previous_gid;
  }
}


/**
 * Return the ``Manifest`` when *args* is exactly one bare manifest, else
 * ``null``. A manifest inside a list is treated as a regular entry.
 */
export function _lone_manifest(call_args) {
  if (call_args.length === 1 && call_args[0] instanceof Manifest) return call_args[0];
  return null;
}


/**
 * Split a JS call list into Python ``(*args, **kwargs)``: a trailing plain
 * object is the keyword block.
 */
function _split_kwargs(call) {
  if (call.length && is_plain_object(call[call.length - 1])) {
    return [call.slice(0, -1), { ...call[call.length - 1] }];
  }
  return [call, {}];
}


/**
 * Persist one or more entries into a policy's memory.
 *
 * Thin top-level shim that forwards to ``policy.central.memory.memorize``.
 * Returns a *future identity* you can ``await`` or ``.wait()``: one entry ->
 * ``Future``; many entries -> ``GroupFuture``.
 *
 * Passing a single ``Manifest`` is equivalent to ``Manifest.memorize``:
 * every pending entry the manifest was built from *and* the manifest itself
 * are stored. A manifest inside a list is stored as a plain entry.
 *
 * Keyword block (trailing object): ``src_policy``, ``src_pool``,
 * ``dst_policy``, ``dst_pool``, ``comm``, ``policy_id``, ``pool_id``,
 * ``pool_nickname``, ``affinity``, ``entries`` plus any extra forwarded
 * kwargs.
 *
 * @param {...any} call ``entries`` (Entry | Manifest | Entry[]) then the keyword block.
 */
export function memorize(...call) {
  let [call_args, kwargs] = _split_kwargs(call);
  let {
    src_policy = null,
    src_pool = null,
    dst_policy = null,
    dst_pool = null,
    comm = null,
    policy_id = null,
    pool_id = null,
    pool_nickname = null,
    affinity = null,
    ...rest
  } = kwargs;
  kwargs = rest;

  // Accept the leading positional via the ``entries=`` keyword too.
  if (!call_args.length && "entries" in kwargs) {
    call_args = [kwargs.entries];
    delete kwargs.entries;
  }

  // Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
  if (dst_policy === null) dst_policy = policy_id;
  if (dst_pool === null) dst_pool = pool_id !== null ? pool_id : pool_nickname;

  const src_gid = _resolve_policy_ref(src_policy);
  const dst_gid = _resolve_policy_ref(dst_policy);
  const active_gid = get_active_policy().global_id;

  // Source is another policy -> 3-party relay (B pushes src->dst).
  if (src_gid !== null && src_gid !== active_gid) {
    return _relay_transfer("memorize", src_gid, call_args, kwargs, { src_pool, dst_gid, dst_pool, comm });
  }

  // Source is the active policy.
  if (dst_gid !== null && dst_gid !== active_gid) {
    // active -> peer push (2-party).
    return _route_to_policy(dst_gid, "memorize", call_args, { pool: dst_pool }, comm);
  }

  // A bare Manifest -> its bulk operation (pending entries + itself).
  const man = _lone_manifest(call_args);
  if (man !== null) return man.memorize({ pool: dst_pool, ...kwargs });

  // Purely local / standalone write into the active policy's pool.
  return get_active_policy().central.memory.memorize(...call_args, { pool: dst_pool, affinity, ...kwargs });
}


/**
 * Expand an *entry* reference into a full ``global_id``.
 *
 * Thin alias for ``Entry.resolve_global_id``. Accepts a full global id
 * (returned as-is), a scoped shorthand ``SCOPE[:SCOPE...]:<uuid | nickname>[@attrs]``
 * such as ``"MANIFEST:my_dataset"`` or ``"ENTRY:counter@evolution=3"``, or a
 * bare ``<uuid | nickname>[@attrs]`` which is taken to be an ``ENTRY``.
 *
 * @param {string} ref
 * @param {{evolution?: number|null, prefix_scopes?: string[]|null, parse_evolution?: boolean}} [opts]
 * @returns {string}
 */
export function resolve_global_id(ref, opts = {}) {
  const { evolution = null, prefix_scopes = null, parse_evolution = true } = opts;
  return Entry.resolve_global_id(ref, { evolution, prefix_scopes, parse_evolution });
}


/**
 * Expand nickname shorthands in the leading ``entry_ids`` positional.
 * Strings (and strings inside a list) go through ``resolve_global_id``;
 * anything else (``Manifest``, ``Entry``, ...) is passed through untouched.
 */
function _resolve_entry_refs(call_args, prefix_scopes) {
  if (!call_args.length) return call_args;
  let [head, ...rest] = call_args;

  const one = (x) => (typeof x === "string" ? resolve_global_id(x, { prefix_scopes }) : x);

  if (typeof head === "string") head = one(head);
  else if (Array.isArray(head)) head = head instanceof PyTuple ? PyTuple.from_iterable(head.map(one)) : head.map(one);
  return [head, ...rest];
}


/**
 * Translate a ``nickname`` (and optional ``evolution``) kwarg pair into the
 * canonical ``[global_id]`` shape consumed by ``memory.remember`` /
 * ``memory.forget``.
 * @throws {ValueError} If ``kwargs.nickname`` is not a string.
 */
function __resolve_nickname(kwargs, prefix_scopes = null) {
  if (typeof kwargs.nickname !== "string") throw new ValueError("nickname must be a string");
  return [
    resolve_global_id(kwargs.nickname, {
      evolution: kwargs.evolution ?? null,
      prefix_scopes,
      parse_evolution: false,
    }),
  ];
}


/**
 * Retrieve one or more entries from a policy's memory.
 *
 * Thin top-level shim that forwards to ``policy.central.memory.remember``.
 * By default (``persist: true``), if the routed source pool is *not* the
 * alpha pool, the fetched entries are additionally memorized into the alpha
 * pool and the returned future only resolves once that write completes.
 *
 * Entries are identified by full ``global_id`` strings or nickname
 * shorthands (``"MANIFEST:my_dataset"``, ``"ENTRY:counter@evolution=3"``,
 * ``"counter"``); the keyword form ``nickname`` (+ optional ``evolution``) is
 * also accepted. Passing a single ``Manifest`` is equivalent to
 * ``Manifest.remember``.
 *
 * Keyword block (trailing object): ``persist``, ``src_policy``, ``src_pool``,
 * ``dst_policy``, ``dst_pool``, ``comm``, ``prefix_scopes``, ``policy_id``,
 * ``pool_id``, ``pool_nickname``, ``nickname``, ``evolution``, ``entry_ids``
 * plus any extra forwarded kwargs.
 *
 * @param {...any} call ``entry_ids`` (string | Manifest | string[]) then the keyword block.
 */
export function remember(...call) {
  let [call_args, kwargs] = _split_kwargs(call);
  let {
    persist = true,
    src_policy = null,
    src_pool = null,
    dst_policy = null,
    dst_pool = null,
    comm = null,
    prefix_scopes = null,
    policy_id = null,
    pool_id = null,
    pool_nickname = null,
    ...rest
  } = kwargs;
  kwargs = rest;

  if ("nickname" in kwargs) {
    call_args = [];
    kwargs.entry_ids = __resolve_nickname(kwargs, prefix_scopes);
    delete kwargs.nickname;
    delete kwargs.evolution;
  }

  // Accept the leading positional via the ``entry_ids=`` keyword too.
  if (!call_args.length && "entry_ids" in kwargs) {
    call_args = [kwargs.entry_ids];
    delete kwargs.entry_ids;
  }

  // Expand nickname shorthands ("MANIFEST:my_dataset") into full gids
  // before any routing so peers always receive canonical ids.
  call_args = _resolve_entry_refs(call_args, prefix_scopes);

  // Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
  if (dst_policy === null) dst_policy = policy_id;
  if (dst_pool === null) dst_pool = pool_id !== null ? pool_id : pool_nickname;

  const src_gid = _resolve_policy_ref(src_policy);
  const dst_gid = _resolve_policy_ref(dst_policy);
  const active_gid = get_active_policy().global_id;

  // Source policy is another policy -> 3-party relay (B pulls from dst).
  if (src_gid !== null && src_gid !== active_gid) {
    return _relay_transfer("remember", src_gid, call_args, kwargs, { src_pool, dst_gid, dst_pool, comm, persist });
  }

  // Active policy pulls from a peer (2-party).
  if (dst_gid !== null && dst_gid !== active_gid) {
    return _route_to_policy(dst_gid, "remember", call_args, { persist, pool: dst_pool }, comm);
  }

  // A bare Manifest -> fetch every entry it references.
  const man = _lone_manifest(call_args);
  if (man !== null) return man.remember({ pool: dst_pool, persist, ...kwargs });

  // Purely local / standalone read from the active policy's pool.
  return get_active_policy().central.memory.remember(...call_args, { persist, pool: dst_pool, ...kwargs });
}


/**
 * Delete one or more entries from the active policy's memory.
 *
 * Thin top-level shim that forwards to ``policy.central.memory.forget``.
 * Forgetting is *pool-local* and an exact-key operation. Identifying entries
 * follows the same rules as ``remember``. Passing a single ``Manifest`` is
 * equivalent to ``Manifest.forget``.
 *
 * Keyword block (trailing object): ``policy``, ``pool``, ``comm``,
 * ``prefix_scopes``, ``policy_id``, ``pool_id``, ``pool_nickname``,
 * ``nickname``, ``evolution``, ``entry_ids`` plus any extra forwarded kwargs.
 *
 * @param {...any} call ``entry_ids`` (string | Manifest | string[]) then the keyword block.
 */
export function forget(...call) {
  let [call_args, kwargs] = _split_kwargs(call);
  let {
    policy: policy_ = null,
    pool = null,
    comm = null,
    prefix_scopes = null,
    policy_id = null,
    pool_id = null,
    pool_nickname = null,
    ...rest
  } = kwargs;
  kwargs = rest;

  if ("nickname" in kwargs) {
    call_args = [];
    kwargs.entry_ids = __resolve_nickname(kwargs, prefix_scopes);
    delete kwargs.nickname;
    delete kwargs.evolution;
  }

  // Accept the leading positional via the ``entry_ids=`` keyword too.
  if (!call_args.length && "entry_ids" in kwargs) {
    call_args = [kwargs.entry_ids];
    delete kwargs.entry_ids;
  }

  call_args = _resolve_entry_refs(call_args, prefix_scopes);

  // Back-compat: policy_id -> policy, pool_id/pool_nickname -> pool.
  if (policy_ === null) policy_ = policy_id;
  if (pool === null) pool = pool_id !== null ? pool_id : pool_nickname;

  const target_gid = _resolve_policy_ref(policy_);
  const active_gid = get_active_policy().global_id;

  if (target_gid !== null && target_gid !== active_gid) {
    return _route_to_policy(target_gid, "forget", call_args, { pool }, comm);
  }

  // A bare Manifest -> delete every referenced entry plus the manifest.
  const man = _lone_manifest(call_args);
  if (man !== null) return man.forget({ pool, ...kwargs });

  return get_active_policy().central.memory.forget(...call_args, { pool, ...kwargs });
}


/**
 * Connect to a remote policy and register it as a peer of the active policy.
 *
 * Delegates to ``Communication.add_peer``: picks a transport that can handle
 * *uri*, performs the handshake, and registers a ``RemotePolicyProxy`` in
 * ``laila.peers[<remote_gid>]`` and in ``_remote_policies``.
 *
 * @param {string} uri URI of the remote policy (``"ws://host:port"``, ``"tcp://..."``, ...).
 * @param {string} secret The remote policy's ``peer_secret_key``.
 * @returns {string} The ``global_id`` of the newly peered remote policy.
 */
export function add_peer(uri, secret) {
  return get_active_policy().central.communication.add_peer(uri, secret);
}


/**
 * Return a transport-bound proxy for a connected peer.
 *
 * Given a peered policy's ``global_id`` it returns its ``RemotePolicyProxy``,
 * optionally *bound* to a specific transport via *comm_protocol* so that
 * every call (and every follow-up on the futures it yields) travels over that
 * channel.
 *
 * @param {string} policy_id ``global_id`` of a peer registered via ``add_peer``.
 * @param {string|null} [comm_protocol] A connection ``global_id`` or protocol token.
 * @throws {ConnectionError} If *policy_id* is not a connected peer.
 */
export function request(policy_id, comm_protocol = null) {
  const proxy = _remote_policies[str(policy_id)] ?? null;
  if (proxy === null) {
    throw new ConnectionError(`Unknown peer ${JSON.stringify(str(policy_id))}: connect first with laila.add_peer().`);
  }
  return comm_protocol !== null && comm_protocol !== undefined ? proxy.via(comm_protocol) : proxy;
}


/**
 * Iterate the messages of a peer stream channel on the calling thread.
 *
 * Given a ``Channel`` (from ``laila.peers[gid].channel(name)``) -- or a peer
 * ``global_id`` plus a channel *name* -- returns the channel's ``Relay``, a
 * blocking iterator yielding one ``StreamEntry`` per sender-side ``send()``.
 *
 * Iteration ends when the channel closes (peer loss, remote/local
 * ``close()``, ``stop()``, ``terminate``).
 *
 * @param {any} channel A ``Channel``, or the peer ``global_id`` when *name* is given.
 * @param {string|null} [name] Channel name, required when *channel* is a peer id.
 * @param {{timeout?: number|null}} [opts] Per-message wait (``null`` blocks).
 * @returns {any} The channel's single ``Relay``.
 * @throws {ConnectionError} Unknown peer / no stream lanes / lane could not be opened.
 * @throws {RuntimeError} If the active policy is a remote proxy (morph mode).
 */
export function relay(channel, name = null, opts = {}) {
  if (name !== null && typeof name === "object" && !Array.isArray(name) && is_plain_object(name)) {
    opts = name;
    name = null;
  }
  const { timeout = null } = opts;

  if (channel instanceof _Channel) {
    if (name !== null) throw new PyTypeError("Pass either a Channel or (peer_id, name), not both.");
    return channel.relay(timeout);
  }
  if (name === null) throw new PyTypeError("laila.relay(peer_id, name) requires a channel name.");
  const active = get_active_policy();
  if (active instanceof RemotePolicyProxy) {
    throw new RuntimeError(
      "laila.relay needs a local active policy; the active policy is a remote " +
        "proxy (morph mode). Activate the local policy that holds the peer.",
    );
  }
  const peer_gid = _resolve_policy_ref(channel);
  const peers = active.central.communication.peers;
  const proxy = peers.get(str(peer_gid));
  if (proxy === null || proxy === undefined) {
    throw new ConnectionError(`Unknown peer ${JSON.stringify(str(peer_gid))}: connect first with laila.add_peer().`);
  }
  return proxy.__getitem__(name).relay(timeout);
}


/**
 * Look up the actual future object from a heterogeneous reference.
 *
 * Accepts ``RemoteFuture`` / ``GroupFuture`` / ``Future`` (returned as-is),
 * a ``_LAILA_IDENTIFIABLE_FUTURE`` identity, or a ``global_id`` string
 * (looked up in every local policy's ``future_bank`` and finally in the
 * active policy's bank).
 *
 * @throws {KeyError} If the gid is not found in any future bank.
 * @throws {TypeError} If *future_ref* is not one of the accepted types.
 */
export function _resolve_future(future_ref) {
  if (future_ref instanceof RemoteFuture) return future_ref;
  if (future_ref instanceof GroupFuture) return future_ref;
  if (future_ref instanceof Future) return future_ref;

  if (typeof future_ref === "string") {
    for (const policy_ of Object.values(_local_policies)) {
      if (dict_has(policy_.future_bank, future_ref)) return dict_get(policy_.future_bank, future_ref);
    }
    const bank = get_active_policy().future_bank;
    if (!dict_has(bank, future_ref)) throw new KeyError(future_ref);
    return dict_get(bank, future_ref);
  }

  if (future_ref instanceof _LAILA_IDENTIFIABLE_FUTURE) {
    const gid = future_ref.global_id;
    for (const policy_ of Object.values(_local_policies)) {
      if (dict_has(policy_.future_bank, gid)) return dict_get(policy_.future_bank, gid);
    }
    const bank = get_active_policy().future_bank;
    if (!dict_has(bank, gid)) throw new KeyError(gid);
    return dict_get(bank, gid);
  }

  throw new PyTypeError(`Cannot resolve future for ${future_ref?.constructor?.name ?? typeof future_ref}`);
}


/**
 * Return the lifecycle status of *future_ref*.
 * @deprecated Use ``laila.runtime.status`` directly.
 */
export function status(future_ref) {
  return runtime.status(future_ref);
}


/**
 * Block until *future_ref* completes and return its result.
 * @deprecated Use ``laila.runtime.wait`` directly.
 */
export function wait(future_ref, timeout = null) {
  return runtime.wait(future_ref, timeout);
}


/**
 * Relocate every on-disk sub-directory laila uses by default.
 *
 * Mutates ``LAILA_DEFAULT_DIRECTORIES`` in place. Pools created *after* this
 * call resolve their backing directories from the new root. ``~`` is
 * expanded. Call this *before* instantiating any pool that you want rooted
 * at the new location.
 *
 * @param {string} directory Filesystem path (may contain ``~``).
 */
export function set_default_directory(directory) {
  directory = _expanduser(directory);
  Object.assign(defaults.LAILA_DEFAULT_DIRECTORIES, {
    root: directory,
    pools: path.join(directory, "pools"),
    logs: path.join(directory, "logs"),
    secrets: path.join(directory, "secrets"),
    indices: path.join(directory, "indices"),
  });
}

/** ``os.path.expanduser`` */
function _expanduser(p) {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/") || p.startsWith("~\\")) return path.join(os.homedir(), p.slice(2));
  return p;
}


// ---------------------------------------------------------------------------
// The module object (Python: ``_LailaModule`` properties)
// ---------------------------------------------------------------------------

const laila = {
  __version__,
  // sub-packages
  atomic,
  basics,
  data,
  entry,
  macros,
  policy,
  utils,
  TaskForce,
  // classes / helpers
  Entry,
  Manifest,
  manifest,
  Logger,
  disable_logging,
  enable_logging,
  get_logger,
  set_log_level,
  guarantee,
  guarantee_async,
  ArgReader,
  _LAILA_IDENTIFIABLE_POLICY,
  _ENTRY_SCOPE,
  // aliases + defaults
  constant,
  variable,
  contingent,
  future,
  ...defaults,
  // state (``args`` is an accessor, see below)
  arg_reader,
  _local_policies,
  _remote_policies,
  _LailaArgs,
  _is_env_load_trigger,
  // free functions
  terminate,
  read_args,
  get_active_namespace,
  set_active_namespace,
  get_active_policy,
  activate_policy,
  _get_active_local_policy,
  build,
  _resolve_policy_ref,
  _lone_manifest,
  memorize,
  resolve_global_id,
  remember,
  forget,
  add_peer,
  request,
  relay,
  _resolve_future,
  status,
  wait,
  set_default_directory,
};

Object.defineProperties(laila, {
  /** ``laila.args`` -- rebinding it (``laila.args = new _LailaArgs()``) is honoured. */
  args: {
    get: () => args,
    set: (value) => {
      args = value;
    },
    enumerable: true,
  },
  /**
   * Module-private pointer. Writable like the Python module global
   * (``laila._active_policy_gid = None`` resets the active policy so the
   * next ``DefaultPolicy()`` / ``get_active_policy()`` installs a fresh one).
   */
  _active_policy_gid: {
    get: () => _active_policy_gid,
    set: (value) => {
      _active_policy_gid = value;
    },
    enumerable: false,
  },
  /** Module-private namespace; writable like the Python global (``laila._active_namespace = None``). */
  _active_namespace: {
    get: () => _active_namespace,
    set: (value) => {
      _active_namespace = value;
    },
    enumerable: false,
  },
  /** Currently active policy (lazy ``DefaultPolicy`` on first access). */
  active_policy: {
    get: () => get_active_policy(),
    set: (value) => activate_policy(value),
    enumerable: true,
  },
  /** Active policy's ``central.communication`` -- peers, protocols, RPC. */
  communication: {
    get: () => get_active_policy().central.communication,
    enumerable: true,
  },
  /** Active policy's ``central.memory`` -- memorize/remember/forget, pools. */
  memory: {
    get: () => get_active_policy().central.memory,
    enumerable: true,
  },
  /** Active policy's ``central.command`` -- taskforces, submit, futures. */
  command: {
    get: () => get_active_policy().central.command,
    enumerable: true,
  },
  /** Mapping of ``global_id`` -> ``RemotePolicyProxy`` for every connected peer. */
  peers: {
    get: () => get_active_policy().central.communication.peers,
    enumerable: true,
  },
  /** All local policies on this machine, keyed by ``global_id``. */
  local_policies: {
    get: () => _local_policies,
    enumerable: true,
  },
  /** All remote peer policies, keyed by ``global_id``. */
  remote_policies: {
    get: () => _remote_policies,
    enumerable: true,
  },
  /** Union of local and remote policies (locals take precedence). */
  universe: {
    get: () => ({ ..._remote_policies, ..._local_policies }),
    enumerable: true,
  },
  /** The active policy's default (alpha) pool instance. */
  alpha_pool: {
    get: () => {
      const mem = get_active_policy().central.memory;
      return dict_get(mem.pool_router.pools, mem.alpha_pool);
    },
    enumerable: true,
  },
  /** The ``laila/runtime`` module -- future status / wait / result helpers. */
  runtime: {
    get: () => runtime,
    set: () => {
      /* no-op: the sub-module is always resolved lazily */
    },
    enumerable: true,
  },
  /**
   * Process-wide Fernet key used by ``FernetEncryption`` when no explicit
   * ``key`` is given. Alias of ``laila.args.encryption.key``. ``null`` when
   * unset.
   */
  encryption_key: {
    get: () => {
      const section = args.get("encryption");
      if (section === null || section === undefined || typeof section.get !== "function") return null;
      const key = section.get("key");
      if (key === null || key === undefined || key === "") return null;
      if (key instanceof DotMap && key.__len__() === 0) return null;
      if (is_plain_object(key) && Object.keys(key).length === 0) return null;
      return key;
    },
    set: (value) => {
      args.encryption.key = value;
    },
    enumerable: true,
  },
  /** Process-wide ``Logger`` singleton (lazy). */
  logger: {
    get: () => get_logger(),
    set: (value) => {
      Logger.reset_singleton();
      Logger._singleton = value;
    },
    enumerable: true,
  },
});

register("laila", laila);

export default laila;
export { laila };
