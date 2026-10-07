/**
 * Base schema for a Laila policy and its central sub-components.
 *
 * A *policy* is the unit of ownership for everything stateful in laila:
 * storage pools, task-forces, peer connections, futures, and the
 * manifests/hints that drive memory routing. The base class defined here is
 * the bridge between the user-facing top-level API (``laila.memory``,
 * ``laila.command``, ...) and the four central sub-systems that actually do
 * the work.
 *
 * Policies are themselves identifiable. Their ``global_id`` is what peers use
 * to address them across the network (see ``RemotePolicyProxy``), and what
 * ``laila.activate_policy`` records as the active gid so the top-level
 * shortcuts know which subsystem instance to resolve against.
 */
import { KeyError, NotImplementedError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { BaseModel, ConfigDict, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { dict_get, type_name } from "../../_compat/pytypes.js";
import { is_enum_member } from "../../_compat/enum.js";
import { CLICapable, CLIExempt } from "../../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../basics/definitions/identifiable_object.js";
import { _POLICY_SCOPE } from "../../macros/strings.js";
import { _LAILA_IDENTIFIABLE_CENTRAL_COMMAND } from "../central/command/schema/base.js";

/**
 * Container struct for the four central sub-systems of a policy.
 *
 * Holds optional references to ``logic``, ``command``, ``memory``, and
 * ``communication``. All four are ``CLIExempt`` so they do not appear in
 * ``laila.args`` resolution paths -- they are wired directly by
 * ``_LAILA_IDENTIFIABLE_POLICY.model_post_init``, not from CLI args.
 */
class Central extends BaseModel {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      logic: ["Any | None", CLIExempt({ default: null })],
      command: [[_LAILA_IDENTIFIABLE_CENTRAL_COMMAND, "None"], CLIExempt({ default: null })],
      communication: ["Any | None", CLIExempt({ default: null })],
      memory: ["Any | None", CLIExempt({ default: null })],
    });
  }
}

/**
 * Top-level policy object owning central command, memory, communication, and logic.
 *
 * A policy bundles three runtime concerns into one identifiable object:
 *
 * 1. **Memory** -- where entries live and how they're routed.
 * 2. **Command** -- how work runs and how its lifecycle is tracked.
 * 3. **Communication** -- how this policy talks to peers.
 *
 * Plus a ``future_bank`` keyed by future ``global_id`` that every ``Future``
 * self-registers into on construction. The bank is the single source of
 * truth that lets remote-policy RPCs and ``laila.runtime.wait`` look futures
 * up by id. It is a plain dict with strong references and **nothing is
 * evicted automatically**: a future (and its result payload) stays in the
 * bank until the holder calls ``future.release()``.
 *
 * Lazy wiring
 * -----------
 * Every central sub-system has a default implementation
 * (``DefaultCentralCommand`` / ``DefaultCentralMemory`` /
 * ``DefaultCentralCommunication``) that ``model_post_init`` instantiates if
 * the user did not pass one explicitly. This means ``new DefaultPolicy()``
 * returns a fully-functional policy with no further setup -- pools,
 * taskforces, and a stopped communication instance ready to start on first
 * ``add_peer``.
 */
export class _LAILA_IDENTIFIABLE_POLICY extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static Central = Central;

  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_POLICY_SCOPE] }),
    });
    define_fields(this, {
      // Core components
      central: [Central, CLIExempt({ default_factory: () => new Central() })],
      /**
       * Live registry of every future this policy owns, keyed by ``global_id``.
       *
       * Entries persist until ``Future.release()`` / ``GroupFuture.release()``
       * is called on them; there is no automatic pruning.
       */
      future_bank: ["dict[str, Any]", CLIExempt({ default_factory: () => ({}) })],
    });
  }

  /**
   * Lazily wire the default central sub-systems if the user didn't supply them.
   *
   * For each of ``memory`` / ``command`` / ``communication``, if the slot is
   * ``null`` we instantiate the corresponding ``Default<Subsystem>`` from
   * ``laila.macros.defaults``.
   *
   * After wiring, ``communication._local_policy`` is back-reffed to ``this``
   * so inbound RPC dispatch can find the local subsystems by walking
   * attribute paths from the policy.
   *
   * ``logic`` is intentionally left ``null`` -- it is reserved for future
   * higher-level orchestration.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    const defaults = lazy("laila.macros.defaults");

    if (this.central.memory === null) this.central.memory = new defaults.DefaultCentralMemory();

    if (this.central.command === null) this.central.command = new defaults.DefaultCentralCommand({ policy_id: this.global_id });

    if (this.central.communication === null) {
      this.central.communication = new defaults.DefaultCentralCommunication({ policy_id: this.global_id });
    }

    this.central.communication._local_policy = this;
  }

  /**
   * Register a new pool with this policy's central memory.
   *
   * Equivalent to ``this.central.memory.extend(new_pool, ...)``, which
   * delegates to the underlying ``PoolRouter``. The pool's ``global_id``
   * becomes its routing key; once registered, subsequent ``memorize`` /
   * ``remember`` calls can target it with ``pool_id=`` (or with
   * ``pool_nickname=`` if a nickname was supplied to ``PoolRouter.extend``).
   *
   * @param {any} new_pool The ``_LAILA_IDENTIFIABLE_POOL`` to register.
   */
  extend(new_pool) {
    this.central.memory.extend(new_pool);
  }

  /**
   * Fetch a single entry from central memory by its ``global_id``.
   *
   * This is a *low-level*, blocking convenience used internally and from
   * tests; most callers should use the top-level ``laila.remember`` (which
   * returns a future and supports cache-back into the alpha pool).
   *
   * @param {string} global_id The unique identifier of the entry to recall.
   * @param {{global_fetch?: boolean, pool_subset?: object|null, hint?: string|null, _remote_called?: boolean}} [opts]
   *   ``global_fetch``: search across all known policies (currently raises
   *   ``NotImplementedError``). ``pool_subset``: restrict the search to a
   *   subset of pools. ``hint``: routing hint forwarded to central memory's
   *   pool resolver. ``_remote_called``: internal flag set when invoked
   *   through an inbound RPC.
   * @returns {any|null} The recovered entry, or ``null`` if not found in any
   *   inspected pool.
   * @throws {NotImplementedError} When ``global_fetch=true``.
   */
  remember(global_id, opts = {}) {
    const { global_fetch = false, pool_subset = null, hint = null } = opts;
    if (global_fetch) throw new NotImplementedError();
    if (pool_subset !== null || hint !== null) throw new NotImplementedError("pool_subset / hint routing is not implemented yet");

    let entry;
    try {
      const ref = this.central.memory.remember([global_id], { persist: false });
      entry = ref.wait();
    } catch (e) {
      if (e instanceof KeyError) return null;
      throw e;
    }
    if (Array.isArray(entry)) entry = entry.length ? entry[0] : null;
    return entry;
  }

  /**
   * Persist *entries* into central memory.
   *
   * Blocking thin wrapper around ``this.central.memory.memorize(entries)``
   * (the write lands in the alpha pool before this returns). Most callers
   * should use the top-level ``laila.memorize``, which returns the future
   * instead. The propagation kwargs (``require_local_update`` /
   * ``require_global_update``) are placeholders for future replication
   * semantics and are currently ignored -- writes affect only the routed pool.
   *
   * @param {any} entries
   * @param {{require_local_update?: boolean, require_global_update?: boolean}} [_opts]
   */
  memorize(entries, _opts = {}) {
    this.central.memory.memorize(entries).wait();
    return null;
  }

  // ------------------------------------------------------------------
  // RPC helpers for remote future introspection
  // ------------------------------------------------------------------

  _bank_future(future_id) {
    const future = dict_get(this.future_bank, future_id, null);
    if (future === null) throw new KeyError(`Future ${future_id} not in bank`);
    return future;
  }

  /**
   * RPC: return the status of a local future.
   *
   * Invoked by ``RemoteFuture`` (running on a peer process) to poll the
   * *real* future that lives in this policy's ``future_bank``. The returned
   * value is unwrapped from any ``Enum`` so it survives JSON serialization on
   * the wire.
   *
   * @throws {KeyError} If *future_id* is not in this policy's bank (either it
   *   was never created here or it has been garbage-collected).
   */
  _get_future_status(future_id) {
    const future = this._bank_future(future_id);
    const status = future.status;
    if (is_enum_member(status)) return status.value;
    return status;
  }

  /**
   * RPC: return a JSON-serializable view of a local future's exception.
   *
   * Returns the empty payload ``null`` when the future succeeded. Otherwise
   * returns ``{"type": <ExcClass>, "message": <str>}`` -- the exception's
   * full type isn't reconstructed on the remote side, but its name and
   * message are preserved for diagnostics.
   */
  _get_future_exception(future_id) {
    const future = this._bank_future(future_id);
    const exc = future.exception;
    if (exc === null || exc === undefined) return null;
    const message = typeof exc.__str__ === "function" ? exc.__str__() : exc instanceof Error ? exc.message : String(exc);
    return { type: type_name(exc), message };
  }

  /**
   * RPC: return the result entry's ``global_id`` for a local future.
   *
   * Used by ``RemoteFuture`` to discover *which entry* a peer's future
   * produced, so the peer can then ``laila.remember`` it from the appropriate
   * pool.
   *
   * For ``GroupFuture``, returns a list of child result ids (one per member
   * future) -- the caller is responsible for zipping it with
   * ``future.future_ids``.
   *
   * Returns ``null`` if the future has no recorded result id (e.g. the future
   * is still running, or it produced a non-entry result).
   */
  _get_future_result_id(future_id) {
    const future = this._bank_future(future_id);
    if ("result_global_id" in future) return future.result_global_id;
    if ("future_ids" in future) {
      const ids = [];
      for (const fid of future.future_ids) {
        const child = dict_get(this.future_bank, fid, null);
        if (child && "result_global_id" in child) ids.push(child.result_global_id);
        else ids.push(null);
      }
      return ids;
    }
    return null;
  }

  /**
   * RPC: block on a local future and return its result-entry gid.
   *
   * Companion to ``_get_future_result_id`` used by ``RemoteFuture`` to wait
   * synchronously for completion. The wait happens on this policy's process;
   * the peer's call is already inside its own thread of execution and is
   * fine to block.
   */
  _wait_future(future_id, timeout = null) {
    const future = this._bank_future(future_id);
    future.wait(timeout);
    if ("result_global_id" in future) return future.result_global_id;
    return null;
  }

  /**
   * Return a local future's result entry(ies), wire-serialized.
   *
   * Uses the canonical, self-describing ``Entry.serialize(transformation_base64)``
   * form so a ``RemoteFuture`` on a peer can rebuild the real ``Entry`` via
   * ``build_by_scope`` -- the payload actually crosses the wire (no shared
   * pool needed). For a ``GroupFuture`` a list of per-child blobs is returned.
   */
  _serialize_future_result(future) {
    const { transformation_base64 } = lazy("laila.entry");

    if ("future_ids" in future) {
      const results = future.result;
      return results.map((e) => (e !== null && e !== undefined ? e.serialize(transformation_base64) : null));
    }
    const result = future.result;
    if (result === null || result === undefined) return null;
    return result.serialize(transformation_base64);
  }

  /**
   * RPC: return a local future's result entry(ies), wire-serialized.
   *
   * The data-bearing counterpart of ``_get_future_result_id``: used by
   * ``RemoteFuture`` to materialize the real entry over the wire instead of a
   * bare gid. Blocks until the future resolves.
   */
  _get_future_result_entry(future_id) {
    const future = this._bank_future(future_id);
    return this._serialize_future_result(future);
  }

  /** RPC: block on a local future and return its result entry(ies), serialized. */
  _wait_future_entry(future_id, timeout = null) {
    const future = this._bank_future(future_id);
    future.wait(timeout);
    return this._serialize_future_result(future);
  }
}

register("laila.policy.schema.base", { _LAILA_IDENTIFIABLE_POLICY });
