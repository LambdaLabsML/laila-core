/**
 * Pydantic-backed proxy for a future that lives on a remote policy.
 *
 * ``RemoteFuture`` is the client-side handle returned whenever an RPC call on
 * a peer yields a serialized future payload (the peer's ``Future``
 * ``model_dump`` includes a ``__laila_future__`` marker that
 * ``Communication._maybe_wrap_remote_future`` rehydrates into a
 * ``RemoteFuture`` on this side).
 *
 * Why it inherits from ``_LAILA_IDENTIFIABLE_FUTURE``
 * --------------------------------------------------
 * The identity base class gives a remote future the *same* gid /
 * ``future_bank`` / guarantee-scope behaviour as a local future, so existing
 * helpers (``laila.runtime.wait``, ``with laila.guarantee:``, remote-fetch
 * chains) can treat both flavors uniformly. Only the status / result /
 * exception / wait accessors override the base to forward over the owning
 * ``Communication`` channel instead of reading local state.
 *
 * Result handling
 * ---------------
 * ``RemoteFuture`` fully mirrors a local ``Future``: ``.result`` / ``.wait()``
 * block until the peer's future completes, transfer the result entry's bytes
 * over the wire (serialized with ``transformation_base64`` on the peer,
 * rebuilt via ``build_by_scope`` here), and return the real ``Entry``;
 * ``.data`` returns its payload. The materialized result is cached so
 * repeated access never re-transfers. ``.status`` / ``.exception`` remain
 * lightweight proxies that do not move the payload.
 */
import { KeyError, ValueError } from "../../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../../_compat/lazy.js";
import * as asyncio from "../../../../../../_compat/asyncio.js";
import { ConfigDict, PrivateAttr, define_private, object_setattr } from "../../../../../../_compat/pydantic.js";
import { repr } from "../../../../../../_compat/pyrepr.js";
import { PyTuple, dict_set } from "../../../../../../_compat/pytypes.js";
import { _LAILA_IDENTIFIABLE_FUTURE } from "./future_identity.js";
import { FutureStatus } from "./future_status.js";

/**
 * Proxy for a future that lives on a remote policy's future bank.
 *
 * The local instance carries only identity data plus a back-reference to the
 * communication channel that serves the owning peer; every status or result
 * query is forwarded over that channel.
 *
 * - ``taskforce_id``: ``global_id`` of the taskforce on the remote side
 *   (falls back to ``peer_id`` when the remote payload omits it).
 * - ``policy_id``: ``global_id`` of the remote policy that owns the future.
 * - ``uuid``: the trailing uuid segment of the remote future's ``global_id``
 *   so the local identity's ``global_id`` matches its counterpart on the peer.
 */
export class RemoteFuture extends _LAILA_IDENTIFIABLE_FUTURE {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _comm: PrivateAttr({ default: null }),
      _is_group: PrivateAttr({ default: false }),
      _comm_selector: PrivateAttr({ default: null }),
      _materialized: PrivateAttr({ default: null }),
      _materialized_set: PrivateAttr({ default: false }),
    });
  }

  /**
   * Attach the communication channel and group-flag after construction.
   *
   * Called by ``Communication._maybe_wrap_remote_future`` once the proxy has
   * been built. The *comm* selector (a communication id / protocol token, or
   * ``null``) is remembered so every follow-up ``status`` / ``wait`` /
   * ``result`` call stays on the same transport that produced the future.
   *
   * @param {any} communication
   * @param {{is_group?: boolean, comm?: any}} [opts]
   */
  bind(communication, opts = {}) {
    const { is_group = false, comm = null } = opts;
    object_setattr(this, "_comm", communication);
    object_setattr(this, "_is_group", is_group);
    object_setattr(this, "_comm_selector", comm);
  }

  /**
   * Apply staged identity fields and self-register with the active local policy.
   *
   * Mirrors ``Future.model_post_init`` so a remote future participates in
   * guarantee scopes and the future bank just like a local one. The
   * communication channel is bound separately via ``bind`` after
   * construction completes.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    const { _get_active_local_policy } = lazy("laila");
    const policy = _get_active_local_policy();
    policy.central.command._register_future_with_active_guarantees(this);
    dict_set(policy.future_bank, this.global_id, this);
  }

  /** Return ``true`` when the remote future is a ``GroupFuture``. */
  get is_group() {
    return this._is_group;
  }

  _rpc(method, args, kwargs = {}) {
    return this._comm._send_rpc(String(this.policy_id), [method], PyTuple.from_iterable(args), kwargs, { comm: this._comm_selector });
  }

  /** Query the remote policy for this future's current status. */
  get status() {
    const raw = this._rpc("_get_future_status", [this.global_id]);
    if (raw instanceof FutureStatus) return raw;
    if (typeof raw === "string") {
      try {
        return FutureStatus(raw);
      } catch (e) {
        if (!(e instanceof ValueError)) throw e;
        try {
          return FutureStatus.__getitem__(raw);
        } catch (e2) {
          if (!(e2 instanceof KeyError)) throw e2;
          return raw;
        }
      }
    }
    return raw;
  }

  /** Rebuild a live Entry (or list for a group) from a wire blob. */
  _rebuild(blob) {
    const { build_by_scope } = lazy("laila.entry.constitution.build_maps");
    if (this._is_group) {
      blob = blob ?? [];
      return blob.map((b) => (b !== null && b !== undefined ? build_by_scope(b, { asynchronous: false }) : null));
    }
    if (blob === null || blob === undefined) return null;
    return build_by_scope(blob, { asynchronous: false });
  }

  /**
   * Return the rebuilt result ``Entry`` from the remote future.
   *
   * Mirrors a local ``Future.result``: blocks until the peer's future
   * completes, transfers the entry over the wire, and returns the rebuilt
   * ``Entry`` (a list for a group future). Cached so a second access does
   * not re-transfer.
   */
  get result() {
    if (this._materialized_set) return this._materialized;
    const blob = this._rpc("_get_future_result_entry", [this.global_id]);
    const entry = this._rebuild(blob);
    object_setattr(this, "_materialized", entry);
    object_setattr(this, "_materialized_set", true);
    return entry;
  }

  /**
   * Return the payload of the result entry (mirrors local ``Future.data``).
   *
   * Blocking: materializes the entry from the peer if needed, then unwraps
   * its ``data`` (a list of payloads for a group future).
   */
  get data() {
    const result = this.result;
    if (this._is_group) return result.map((e) => (e !== null && e !== undefined ? e.data : null));
    return result !== null && result !== undefined ? result.data : null;
  }

  /** Return just the result entry's ``global_id`` (cheap pointer, no payload). */
  get result_id() {
    return this._rpc("_get_future_result_id", [this.global_id]);
  }

  /** Return a serialized representation of the remote exception, if any. */
  get exception() {
    return this._rpc("_get_future_exception", [this.global_id]);
  }

  /**
   * Block until the remote future completes; return the rebuilt Entry.
   *
   * Mirrors a local ``Future.wait``: transfers the result entry over the
   * wire and returns the rebuilt ``Entry`` (cached).
   *
   * @throws {LoopBlockingWaitError} If called from a thread that owns an
   *   async event loop.
   */
  wait(timeout = null) {
    const { _check_not_loop_thread } = lazy("laila.policy.central.command.schema.exceptions");
    const { park_sync } = lazy("laila.policy.central.command.schema.parking");
    _check_not_loop_thread();

    if (this._materialized_set) return this._materialized;
    // The RPC blocks on a remote future; park the local slot meanwhile.
    const blob = park_sync(() => this._rpc("_wait_future_entry", [this.global_id], { timeout }));
    const entry = this._rebuild(blob);
    object_setattr(this, "_materialized", entry);
    object_setattr(this, "_materialized_set", true);
    return entry;
  }

  /**
   * Await the remote future without blocking the calling event loop.
   *
   * The blocking RPC ``wait`` is offloaded to a worker thread via
   * ``asyncio.to_thread``, so the event loop stays free to service other
   * coroutines while the wait is in flight. The current taskforce slot (if
   * any) is parked for the duration.
   */
  __await__() {
    const { park_async } = lazy("laila.policy.central.command.schema.parking");
    const _run = async () => await asyncio.to_thread(() => this.wait(null));
    return park_async(_run());
  }

  /** Return a short human-readable representation. */
  __repr__() {
    const kind = this._is_group ? "RemoteGroupFuture" : "RemoteFuture";
    return `${kind}(${repr(this.global_id)}, policy=${repr(this.policy_id)})`;
  }
}

register("laila.policy.central.command.schema.future.future.remote_future", { RemoteFuture });
