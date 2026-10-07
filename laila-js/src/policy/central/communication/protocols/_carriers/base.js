/**
 * Shared base for all RPC carriers.
 *
 * ``_CarrierRPCProtocol`` factors out everything that is identical across
 * transports regardless of the underlying wire:
 *
 * - **Config fields** common to every carrier: the wire ``codec``, the
 *   blocking ``rpc_timeout`` and ``handshake_timeout``, and the
 *   ``peer_secret_key`` presented during the handshake.
 * - **Peer bookkeeping**: a ``_connections`` map (``peer_id`` -> opaque
 *   transport handle) plus the two-tier ``_register_peer`` /
 *   ``_unregister_peer`` that also notify the owning
 *   ``_LAILA_IDENTIFIABLE_COMMUNICATION`` so a ``RemotePolicyProxy`` is
 *   created/destroyed.
 * - **Outbound correlation**: a ``_pending_rpcs`` table and the
 *   ``_register_pending`` / ``_complete_pending`` / ``_await_pending`` trio
 *   that let a synchronous ``send_rpc`` block on a ``threading.Event`` until
 *   the matching response frame arrives on the carrier's I/O loop.
 * - **Inbound dispatch**: ``_build_response`` runs the actual
 *   ``_execute_rpc`` on a worker (never the I/O loop) so a blocking remote
 *   call -- e.g. ``_wait_future`` -- cannot stall the transport.
 * - **Stream lanes**: the per-peer lane tables behind
 *   ``laila.peers[gid][name]`` -- ``open_channel``, the
 *   ``__comm_channel_open__`` / ``__comm_channel_close__`` control frames
 *   (intercepted exactly like ``__comm_ping__``: before admission, before
 *   ``_execute_rpc``), ``_on_stream_frame`` reassembly, and the
 *   close-all-lanes step that runs before a peer is unregistered. Carriers
 *   that can stream set ``supports_channels`` and implement
 *   ``_stream_enqueue``.
 *
 * Sync and async surfaces
 * -----------------------
 * Python's carriers are synchronous for the caller (they block a user
 * thread on a ``threading.Event``). The JS port keeps those exact methods
 * (``send_rpc``, ``connect``, ``open_channel``, ``ping``; they block by
 * pumping the Node loop) and adds ``*_async`` twins for callers that are
 * already inside a microtask and therefore cannot block.
 */
import { createRequire } from "node:module";

import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, RuntimeError, TimeoutError as PyTimeoutError, ValueError } from "../../../../../_compat/errors.js";
import { ThreadPoolExecutor } from "../../../../../_compat/executor.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { repr } from "../../../../../_compat/pyrepr.js";
import { dict_get, dict_items, eq, is_plain_object, type_name } from "../../../../../_compat/pytypes.js";
import { Event, Lock, blocking_wait, local as thread_local, with_lock } from "../../../../../_compat/threading.js";
import * as time from "../../../../../_compat/time.js";
import { uuid4 } from "../../../../../_compat/uuid.js";
import * as rpc_protocol from "../../protocol.js";
import { Channel } from "../../channel.js";
import { _LAILA_IDENTIFIABLE_COMM_PROTOCOL } from "../base.js";
import * as _codec from "./codec.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.base");
const _require = createRequire(import.meta.url);

/** Reserved dotted-path for the liveness ping control frame. */
export const _COMM_PING_PATH = Object.freeze(["__comm_ping__"]);
/** Reserved dotted-paths for stream-lane control. */
export const _COMM_CHANNEL_OPEN_PATH = Object.freeze(["__comm_channel_open__"]);
export const _COMM_CHANNEL_CLOSE_PATH = Object.freeze(["__comm_channel_close__"]);
/** Highest allocatable lane id; ``0`` is reserved for RPC/control. */
export const _MAX_LANE = 255;

/** Peering control methods (top-level JSON-RPC ``method`` values). */
export const _PEER_CONNECT = "peer.connect";
/**
 * Notification a side sends right before dropping a peer / stopping, so
 * the other side unregisters it at once rather than on liveness timeout.
 */
export const _PEER_DISCONNECT = "peer.disconnect";
/** Upper bound on how long ``disconnect()`` waits for the goodbye to go out. */
export const _GOODBYE_TIMEOUT = 1.0;

/**
 * Raised when a peer rejected an RPC with ``ERR_BUSY``.
 *
 * Distinct from a generic remote error so the sender's retry loop can scope
 * exponential backoff to overload only, and surface a clear final error if
 * the peer stays saturated.
 */
export class BackpressureError extends RuntimeError {}

/** ``msg.get(key, default)`` for wire dicts (plain object or Map). */
export function _mget(msg, key, dflt = null) {
  if (msg === null || msg === undefined || typeof msg !== "object") return dflt;
  return dict_get(msg, key, dflt);
}
function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}
/** Python list equality for wire paths. */
export function _path_eq(a, b) {
  return Array.isArray(a) && a.length === b.length && a.every((x, i) => x === b[i]);
}
/** ``f"{type(exc).__name__}: {exc}"`` */
export function _exc_text(exc) {
  const name = exc && exc.constructor && exc.constructor.name ? exc.constructor.name : type_name(exc);
  const msg = exc && exc.message !== undefined ? exc.message : String(exc);
  return `${name}: ${msg}`;
}
/** Python ``int(x)`` for wire numbers. */
function _int(x, dflt = 0) {
  if (x === null || x === undefined) return dflt;
  const n = Number(x);
  if (!Number.isFinite(n)) throw new ValueError(`invalid literal for int(): ${repr(x)}`);
  return Math.trunc(n);
}
/** Python ``list(x)`` */
function _list(x) {
  if (x === null || x === undefined) return [];
  return Array.isArray(x) ? [...x] : [...x];
}
/** Python ``dict(x)`` -> plain object */
function _dict(x) {
  if (x === null || x === undefined) return {};
  if (x instanceof Map) return Object.fromEntries(x);
  return { ...x };
}
/** Block (pumping the loop) until *promise* settles; return its value or throw. */
function _block_on(promise) {
  let settled = false;
  let value;
  let error;
  let failed = false;
  promise.then(
    (v) => {
      value = v;
      settled = true;
    },
    (e) => {
      error = e;
      failed = true;
      settled = true;
    },
  );
  blocking_wait(() => settled, null);
  if (failed) throw error;
  return value;
}

/**
 * Abstract carrier holding wire-agnostic RPC machinery.
 *
 * This class is never registered directly -- it has no ``protocol_name``
 * of its own. Subclasses set ``protocol_name``, the URI/token routing, and
 * the transport endpoint factories.
 *
 * Fields
 * ------
 * codec : str, default ``"json"``
 *     Wire serialisation, one of ``codec.CODECS``. ``"msgpack"`` is far
 *     more compact for bandwidth-constrained links.
 * rpc_timeout : float, default ``60.0``
 *     Seconds a blocking ``send_rpc`` waits for the response.
 * handshake_timeout : float, default ``10.0``
 *     Seconds the peering handshake waits for the remote reply.
 * peer_secret_key : str
 *     Shared secret a remote peer must present during the handshake.
 *     Defaults to a fresh UUID4 hex.
 * channel_queue_size : int, default ``256``
 *     Max *messages* buffered per inbound stream lane before the oldest is
 *     evicted (``Channel.dropped``).
 * channel_queue_bytes : int, default ``64 MiB``
 *     Max *bytes* buffered per inbound stream lane (same eviction).
 * stream_chunk_bytes : int, default ``65536``
 *     Outbound stream messages are sliced into chunks of this size so
 *     RPC/ping frames can interleave between chunks.
 * max_stream_frame_bytes : int, default ``64 MiB``
 *     Largest single stream message accepted in either direction.
 * static_lanes : dict[str, int]
 *     ``{channel_name: lane_id}`` bound at peer registration without a
 *     control handshake. Strictly for peers that emit lane frames but
 *     cannot answer ``__comm_channel_open__`` (e.g. MCU firmware).
 */
export class _CarrierRPCProtocol extends _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  /** Carriers are abstract; concrete transports override. */
  static protocol_name = "carrier";

  static {
    define_fields(this, {
      codec: ["str", Field({ default: "json" })],
      rpc_timeout: ["float", Field({ default: 60.0 })],
      handshake_timeout: ["float", Field({ default: 10.0 })],
      ping_timeout: ["float", Field({ default: 5.0 })],
      peer_secret_key: ["str", Field({ default_factory: () => uuid4().hex })],
      // Sender-side backoff when a peer replies ``ERR_BUSY`` (backpressure).
      rpc_backoff_base: ["float", Field({ default: 0.05 })],
      rpc_backoff_max: ["float", Field({ default: 5.0 })],
      max_rpc_retries: ["int", Field({ default: 5 })],
      // Stream-lane tuning (inert on carriers with ``supports_channels=False``).
      channel_queue_size: ["int", Field({ default: 256 })],
      channel_queue_bytes: ["int", Field({ default: 64 * 1024 * 1024 })],
      stream_chunk_bytes: ["int", Field({ default: 65536 })],
      max_stream_frame_bytes: ["int", Field({ default: 64 * 1024 * 1024 })],
      static_lanes: ["dict[str, int]", Field({ default_factory: () => ({}) })],
    });
    define_private(this, {
      _started: PrivateAttr({ default: false }),
      _connections: PrivateAttr({ default_factory: () => new Map() }),
      _pending_rpcs: PrivateAttr({ default_factory: () => new Map() }),
      _inbound_executor: PrivateAttr({ default: null }),
      // per-thread RPC wait override (set by ping(); see _await_pending)
      _rpc_wait_override: PrivateAttr({ default_factory: () => thread_local() }),
      // stream-lane state: peer -> rx-lane -> Channel, peer -> name -> Channel
      _lanes: PrivateAttr({ default_factory: () => new Map() }),
      _lane_names: PrivateAttr({ default_factory: () => new Map() }),
      _lane_cursor: PrivateAttr({ default_factory: () => new Map() }),
      _peer_caps: PrivateAttr({ default_factory: () => new Map() }),
      _lane_lock: PrivateAttr({ default_factory: () => new Lock() }),
      _unknown_lane_drops: PrivateAttr({ default: 0 }),
      _reserved_marker_drops: PrivateAttr({ default: 0 }),
      _malformed_stream_drops: PrivateAttr({ default: 0 }),
      // loop-thread handles (set by the carriers that own a loop)
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
    });
  }

  // ------------------------------------------------------------------
  // Inbound dispatch (runs the real call off the I/O thread)
  // ------------------------------------------------------------------

  /** Lazily create the worker pool used to run inbound RPCs. */
  _ensure_executor() {
    if (this._inbound_executor === null || this._inbound_executor === undefined) {
      this._inbound_executor = new ThreadPoolExecutor({
        max_workers: 8,
        thread_name_prefix: `${this.constructor.name}-inbound`,
      });
    }
    return this._inbound_executor;
  }

  /**
   * Execute an inbound ``rpc.call`` *msg* and return a response dict.
   *
   * Runs on the calling worker (carriers call this from a worker, never
   * the I/O loop). Unknown methods and execution failures are mapped to
   * JSON-RPC error envelopes. A JS target method that returns a native
   * ``Promise`` is awaited; laila futures (thenables with ``__await__``)
   * are returned as-is so the codec can tag them ``__laila_future__``.
   *
   * *peer_id* is the authenticated sender (known to the carrier from the
   * connection). It is only needed by the stream-lane control frames.
   * @returns {Promise<object>}
   */
  async _build_response(msg, peer_id = null) {
    const pre = this._build_response_head(msg, peer_id);
    if (pre.response !== undefined) return pre.response;
    try {
      let result = this._communication._execute_rpc(pre.path, pre.args, pre.kwargs);
      if (result instanceof Promise) result = await result;
      return rpc_protocol.make_result(pre.request_id, result);
    } catch (exc) {
      return rpc_protocol.make_error(pre.request_id, rpc_protocol.ERR_EXECUTION, _exc_text(exc));
    }
  }

  /**
   * Synchronous ``_build_response`` (Python's is synchronous; the loopback
   * transport calls it inline on the caller's thread). A JS target that
   * returns a native ``Promise`` is waited for by pumping the loop.
   */
  _build_response_sync(msg, peer_id = null) {
    const pre = this._build_response_head(msg, peer_id);
    if (pre.response !== undefined) return pre.response;
    try {
      let result = this._communication._execute_rpc(pre.path, pre.args, pre.kwargs);
      if (result instanceof Promise) result = _block_on(result);
      return rpc_protocol.make_result(pre.request_id, result);
    } catch (exc) {
      return rpc_protocol.make_error(pre.request_id, rpc_protocol.ERR_EXECUTION, _exc_text(exc));
    }
  }

  /** Shared validation + control fast-paths of ``_build_response``. */
  _build_response_head(msg, peer_id) {
    const request_id = _mget(msg, "id");
    const method = _mget(msg, "method");
    if (method !== "rpc.call") {
      return { request_id, response: rpc_protocol.make_error(request_id, rpc_protocol.ERR_METHOD_NOT_FOUND, `Unknown method: ${method}`) };
    }
    const params = _mget(msg, "params", {}) ?? {};
    const path = _mget(params, "path", []) ?? [];
    const args = _mget(params, "args", []) ?? [];
    const kwargs = _mget(params, "kwargs", {}) ?? {};
    // Liveness control frame: answered here, before the policy/worker
    // pool, so a ping never touches central.memory or the executor.
    if (_path_eq(path, _COMM_PING_PATH)) return { request_id, response: rpc_protocol.make_result(request_id, "pong") };
    if (this._is_control_path(path)) return { request_id, response: this._control_response(msg, peer_id) };
    return { request_id, path, args, kwargs };
  }

  // ------------------------------------------------------------------
  // Inbound admission control (per-policy backpressure)
  // ------------------------------------------------------------------

  /**
   * ``true`` for the reserved liveness ``__comm_ping__`` request.
   *
   * Pings must bypass admission entirely: a busy-but-alive peer must never
   * be wrongly dropped by the liveness loop just because its RPC queue is
   * full.
   */
  static _is_ping_frame(msg) {
    if (_mget(msg, "method") !== "rpc.call") return false;
    return _path_eq(_mget(_mget(msg, "params", {}) ?? {}, "path", []) ?? [], _COMM_PING_PATH);
  }
  _is_ping_frame(msg) {
    return _CarrierRPCProtocol._is_ping_frame(msg);
  }

  /**
   * Try to claim an inbound-RPC slot from the hub. Non-blocking.
   *
   * Returns ``true`` if admitted (caller must later ``_release_admit``),
   * ``false`` if the policy is at capacity and the request should be
   * rejected with ``ERR_BUSY``. When no hub is attached admission always
   * succeeds.
   */
  _try_admit() {
    const comm = this._communication;
    if (comm === null || comm === undefined) return true;
    return comm._acquire_rpc_slot();
  }

  /** Release a previously-claimed inbound-RPC slot. */
  _release_admit() {
    const comm = this._communication;
    if (comm !== null && comm !== undefined) comm._release_rpc_slot();
  }

  /** Build the ``ERR_BUSY`` envelope for a rejected inbound request. */
  _busy_response(msg) {
    return rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_BUSY, `${repr(this.protocol_name)} peer is busy: inbound RPC queue at capacity.`);
  }

  /**
   * Centralized inbound dispatch: ping fast-path + admission + queue.
   *
   * ``reply(resp_dict)`` sends a response back over the transport and
   * must be safe to invoke from a worker. *peer_id* is the authenticated
   * sender; carriers pass it so stream-lane control frames are keyed by
   * connection, never by a wire-supplied id.
   *
   * 1. A liveness ping is answered inline, *before* admission.
   * 2. Stream-lane control frames are answered inline the same way.
   * 3. Otherwise a slot is claimed non-blocking; past capacity the request
   *    is rejected immediately with ``ERR_BUSY``.
   * 4. Admitted requests run ``_build_response`` on a worker, releasing the
   *    slot when the reply is ready.
   */
  _handle_request_frame(msg, reply, peer_id = null) {
    if (this._is_ping_frame(msg)) {
      reply(rpc_protocol.make_result(_mget(msg, "id"), "pong"));
      return;
    }
    if (_mget(msg, "method") === "rpc.call" && this._is_control_path(_mget(_mget(msg, "params", {}) ?? {}, "path", []) ?? [])) {
      reply(this._control_response(msg, peer_id));
      return;
    }
    if (!this._try_admit()) {
      reply(this._busy_response(msg));
      return;
    }

    const _work = async () => {
      let resp;
      try {
        resp = await this._build_response(msg, peer_id);
      } finally {
        this._release_admit();
      }
      reply(resp);
    };

    this._ensure_executor().submit(_work);
  }

  // ------------------------------------------------------------------
  // Peer registry (two-tier)
  // ------------------------------------------------------------------

  /**
   * Record a live transport *handle* for *peer_id* and notify the hub.
   *
   * The communication hub creates the corresponding ``PeerProxy`` so user
   * code can immediately reach the new peer. Afterwards every
   * ``static_lanes`` entry is pre-bound so early stream frames are buffered
   * instead of dropped as unknown-lane.
   */
  _register_peer(peer_id, handle) {
    this._connections.set(peer_id, handle);
    if (this._communication !== null && this._communication !== undefined) this._communication._register_peer(peer_id);
    this._bind_static_lanes(peer_id);
  }

  /**
   * Drop *peer_id*: close its lanes, drop the handle, notify the hub. Idempotent.
   *
   * Lanes are closed *first* (waking every relay with a sentinel) so this
   * single place covers liveness drops, receive-loop EOF, ``remove_peer``,
   * ``protocol.stop()``, ``comm.stop()`` and ``remove_connection``.
   */
  _unregister_peer(peer_id) {
    this._close_peer_lanes(peer_id, "peer disconnected");
    this._peer_caps.delete(peer_id);
    this._lane_cursor.delete(peer_id);
    this._connections.delete(peer_id);
    if (this._communication !== null && this._communication !== undefined) this._communication._unregister_peer(peer_id);
  }

  /** Return ``true`` if a live connection to *peer_id* is held. */
  has_peer(peer_id) {
    return this._connections.has(peer_id);
  }

  /**
   * Gracefully drop a single peer. Idempotent.
   *
   * Sends the ``peer.disconnect`` notification (bounded, best effort) so
   * the remote unregisters us immediately, then unregisters the peer
   * locally -- which closes its lanes and, on carriers that hold a closable
   * handle, the handle itself (``_unregister_peer``).
   */
  disconnect(peer_id) {
    if (!this._connections.has(peer_id)) return;
    this._goodbye_threadsafe(peer_id);
    this._unregister_peer(peer_id);
  }

  async disconnect_async(peer_id) {
    if (!this._connections.has(peer_id)) return;
    await this._goodbye_threadsafe_async(peer_id);
    this._unregister_peer(peer_id);
  }

  // ------------------------------------------------------------------
  // Graceful goodbye (``peer.disconnect``)
  // ------------------------------------------------------------------

  /** The ``peer.disconnect`` notification this endpoint sends. */
  _goodbye_message() {
    const policy_id = this._communication ? this._communication.policy_id : null;
    return rpc_protocol.make_notification(_PEER_DISCONNECT, { from_id: policy_id });
  }

  /** ``true`` for an inbound ``peer.disconnect`` notification. */
  static _is_goodbye(msg) {
    return _mget(msg, "method") === _PEER_DISCONNECT;
  }
  _is_goodbye(msg) {
    return _CarrierRPCProtocol._is_goodbye(msg);
  }

  /**
   * Write the goodbye to *peer_id* and wait (bounded) for it to leave.
   *
   * Runs on the carrier loop. Default no-op; wire carriers override with
   * their own write primitive. Must never raise.
   */
  async _send_goodbye(_peer_id) {
    return null;
  }

  /** Say goodbye to every registered peer (first step of ``stop()``). */
  async _goodbye_all() {
    for (const peer_id of [...this._connections.keys()]) {
      try {
        await asyncio.wait_for(this._send_goodbye(peer_id), _GOODBYE_TIMEOUT);
      } catch (e) {
        log.debug("goodbye to %s failed", peer_id, { exc_info: e });
      }
    }
  }

  /** Run ``_send_goodbye`` from a user thread, bounded; never raises. */
  _goodbye_threadsafe(peer_id) {
    const loop = this._event_loop ?? null;
    if (loop === null || loop.is_closed() || !loop.is_running()) return;
    if (asyncio.get_running_loop() === loop) {
      // On the loop thread we cannot block; the caller is tearing the
      // connection down right after, so skip the goodbye.
      return;
    }
    try {
      const fut = asyncio.run_coroutine_threadsafe(() => this._send_goodbye(peer_id), loop);
      fut.result(_GOODBYE_TIMEOUT);
    } catch (e) {
      log.debug("goodbye to %s failed", peer_id, { exc_info: e });
    }
  }

  async _goodbye_threadsafe_async(peer_id) {
    const loop = this._event_loop ?? null;
    if (loop === null || loop.is_closed() || !loop.is_running()) return;
    try {
      const fut = asyncio.run_coroutine_threadsafe(() => this._send_goodbye(peer_id), loop);
      await asyncio.wait_for(fut, _GOODBYE_TIMEOUT);
    } catch (e) {
      log.debug("goodbye to %s failed", peer_id, { exc_info: e });
    }
  }

  /** Inbound ``peer.disconnect`` from an authenticated *peer_id*: drop it. */
  _on_peer_goodbye(peer_id) {
    if (peer_id !== null && peer_id !== undefined && this._connections.has(peer_id)) this._unregister_peer(peer_id);
  }

  /**
   * ``call_soon_threadsafe`` on the carrier loop, or ``ConnectionError``.
   *
   * The single place user-thread sends cross into the loop thread, so a
   * send after ``stop()`` (loop gone or closed) always surfaces as a clear
   * ``ConnectionError`` rather than an ``AttributeError`` or asyncio's
   * ``RuntimeError``.
   */
  _loop_call(fn, ...args) {
    const loop = this._event_loop ?? null;
    if (loop === null || loop.is_closed()) throw new ConnectionError(`${repr(this.protocol_name)} transport is shut down.`);
    try {
      loop.call_soon_threadsafe(fn, ...args);
    } catch (exc) {
      if (exc instanceof RuntimeError) {
        const err = new ConnectionError(`${repr(this.protocol_name)} transport is shut down.`);
        err.__cause__ = exc;
        throw err;
      }
      throw exc;
    }
  }

  // ------------------------------------------------------------------
  // Stream lanes
  // ------------------------------------------------------------------

  /** Capabilities advertised in ``peer.connect`` (``{"lanes": 1}`` if streaming). */
  _local_caps() {
    return this.constructor.supports_channels ? { lanes: 1 } : {};
  }

  /**
   * Remember the capabilities a peer advertised during the handshake.
   *
   * Call *before* ``_register_peer`` so a user opening a channel right
   * after the handshake already sees them. Older peers send no ``caps`` at
   * all; that is recorded as ``{}``.
   */
  _set_peer_caps(peer_id, caps) {
    this._peer_caps.set(peer_id, caps instanceof Map ? Object.fromEntries(caps) : is_plain_object(caps) ? { ...caps } : {});
  }

  /** ``true`` if *peer_id* advertised ``caps.lanes`` during the handshake. */
  _peer_has_lanes(peer_id) {
    const caps = this._peer_caps.get(peer_id) ?? {};
    return !!dict_get(caps, "lanes", null);
  }

  /** Names of the lanes currently open to *peer_id* on this carrier. */
  channel_names(peer_id) {
    return with_lock(this._lane_lock, () => [...(this._lane_names.get(peer_id) ?? new Map()).entries()].filter(([, ch]) => !ch.closed).map(([n]) => n));
  }

  /** Create and bind a ``Channel``. Caller holds ``_lane_lock``. */
  _new_channel(peer_id, name, lane, tx_lane = null) {
    const ch = new Channel(this, peer_id, name, lane, {
      queue_size: this.channel_queue_size,
      queue_bytes: this.channel_queue_bytes,
      max_message_bytes: this._max_message_bytes(),
      tx_lane_id: tx_lane,
    });
    if (!this._lanes.has(peer_id)) this._lanes.set(peer_id, new Map());
    this._lanes.get(peer_id).set(lane, ch);
    if (!this._lane_names.has(peer_id)) this._lane_names.set(peer_id, new Map());
    this._lane_names.get(peer_id).set(name, ch);
    return ch;
  }

  /** Largest stream message this carrier accepts (datagram carriers lower it). */
  _max_message_bytes() {
    return Math.trunc(this.max_stream_frame_bytes);
  }

  /**
   * Pick a free rx lane id for *peer_id*. Caller holds ``_lane_lock``.
   *
   * Prefers *preferred* (the opener's own lane id, so both sides usually
   * share one number); otherwise walks round-robin so an id freed by
   * ``close()`` is not reused immediately while late frames for it may
   * still be in flight.
   */
  _alloc_lane(peer_id, preferred = null) {
    if (!this._lanes.has(peer_id)) this._lanes.set(peer_id, new Map());
    const lanes = this._lanes.get(peer_id);
    if (preferred !== null && preferred !== undefined && preferred >= 1 && preferred <= _MAX_LANE && !lanes.has(preferred)) return preferred;
    let cursor = this._lane_cursor.get(peer_id) ?? 0;
    for (let i = 0; i < _MAX_LANE; i++) {
      cursor = (cursor % _MAX_LANE) + 1;
      if (!lanes.has(cursor)) {
        this._lane_cursor.set(peer_id, cursor);
        return cursor;
      }
    }
    throw new ConnectionError(`All ${_MAX_LANE} stream lanes to peer ${peer_id} are in use.`);
  }

  /** Remove *ch* from the lane tables. Caller holds ``_lane_lock``. */
  _drop_channel(ch) {
    const lanes = this._lanes.get(ch.peer_id);
    if (lanes !== undefined && lanes.get(ch.lane_id) === ch) lanes.delete(ch.lane_id);
    const names = this._lane_names.get(ch.peer_id);
    if (names !== undefined && names.get(ch.name) === ch) names.delete(ch.name);
  }

  /**
   * Shared head of ``open_channel`` / ``open_channel_async``: validate,
   * find or create the channel under the lane lock.
   * @returns {{ch: Channel, pending: boolean, opener: boolean}}
   */
  _open_channel_prepare(peer_id, name) {
    if (name === "default") throw new ValueError("'default' is the RPC proxy, not a stream channel.");
    if (!this.constructor.supports_channels) throw new ConnectionError(`${repr(this.protocol_name)} has no stream lanes.`);
    if (!this.has_peer(peer_id)) throw new ConnectionError(`No connection to peer ${peer_id}`);

    let opener = false;
    return with_lock(this._lane_lock, () => {
      let ch = (this._lane_names.get(peer_id) ?? new Map()).get(name) ?? null;
      if (ch !== null && ch.closed) {
        this._drop_channel(ch);
        ch = null;
      }
      if (ch === null) {
        const static_lane = dict_get(this.static_lanes, name, null);
        if (static_lane !== null && static_lane !== undefined) {
          const lane = _int(static_lane);
          if (!(lane >= 1 && lane <= _MAX_LANE)) throw new ConnectionError(`static_lanes[${repr(name)}]=${lane} is outside 1..${_MAX_LANE}.`);
          const other = (this._lanes.get(peer_id) ?? new Map()).get(lane) ?? null;
          if (other !== null && !other.closed) {
            throw new ConnectionError(`Lane ${lane} to peer ${peer_id} is already bound to channel ${repr(other.name)}.`);
          }
          return { ch: this._new_channel(peer_id, name, lane, lane), pending: false, opener: false };
        }
        if (!this._peer_has_lanes(peer_id)) {
          throw new ConnectionError(
            `Peer ${peer_id} did not advertise stream lanes during the handshake ` +
              "(an older laila or an RPC-only firmware). If it emits lane frames on " +
              `a fixed lane, configure static_lanes={${repr(name)}: <lane>} on this ` +
              "connection.",
          );
        }
        const lane = this._alloc_lane(peer_id);
        ch = this._new_channel(peer_id, name, lane);
        opener = true;
      }
      const pending = ch.tx_lane_id === null || ch.tx_lane_id === undefined;
      return { ch, pending, opener };
    });
  }

  /** Shared tail: commit the peer's lane (or roll back) after the open RPC. */
  _open_channel_commit(ch, name, peer_id, result, err) {
    if (err !== null) {
      const raced = with_lock(this._lane_lock, () => {
        if (ch.tx_lane_id !== null && ch.tx_lane_id !== undefined && !ch.closed) return true;
        this._drop_channel(ch);
        return false;
      });
      if (raced) return ch; // a remote open for the same name raced us and finished first
      ch._close_local(`open failed: ${err.message ?? err}`);
      const e = new ConnectionError(`Could not open channel ${repr(name)} to peer ${peer_id}: ${err.message ?? err}`);
      e.__cause__ = err;
      throw e;
    }
    const tx_lane = result;
    return with_lock(this._lane_lock, () => {
      if (ch.closed) throw new ConnectionError(`Channel ${repr(name)} to peer ${peer_id} closed while opening (${ch.closed_reason}).`);
      if (ch.tx_lane_id === null || ch.tx_lane_id === undefined) ch._mark_opened(tx_lane);
      return ch;
    });
  }

  _open_channel_parse(result) {
    const tx_lane = _int(_mget(result, "lane"));
    if (!(tx_lane >= 1 && tx_lane <= _MAX_LANE)) throw new ValueError(`peer returned lane ${tx_lane}`);
    return tx_lane;
  }

  /**
   * Return the (cached) ``Channel`` *name* to *peer_id*, opening it if needed.
   *
   * Idempotent and race-safe: if both sides open the same name at the same
   * time they converge on one channel per side. The lane lock is never
   * held across the network round trip: the channel is inserted in a
   * *pending* state, the RPC is sent lock-free, and the result is
   * committed under the lock afterwards.
   *
   * @throws {ConnectionError} If the carrier cannot stream, does not hold
   *   *peer_id*, the peer advertised no lanes and no ``static_lanes`` entry
   *   exists for *name*, or the open handshake failed.
   * @throws {ValueError} If *name* is ``"default"``.
   */
  open_channel(peer_id, name) {
    const { ch, pending, opener } = this._open_channel_prepare(peer_id, name);
    if (!pending) return ch;
    if (!opener) {
      // someone else's open is in flight; wait for it
      if (ch._opened.wait(this.rpc_timeout) && !ch.closed) return ch;
      throw new ConnectionError(`Channel ${repr(name)} to peer ${peer_id} did not finish opening (${ch.closed_reason || "timeout"}).`);
    }
    let result = null;
    let err = null;
    try {
      result = this._open_channel_parse(this.send_rpc(peer_id, [..._COMM_CHANNEL_OPEN_PATH], [], { name, lane: ch.lane_id, options: {} }));
    } catch (exc) {
      err = exc;
    }
    return this._open_channel_commit(ch, name, peer_id, result, err);
  }

  /** Awaitable ``open_channel``. */
  async open_channel_async(peer_id, name) {
    const { ch, pending, opener } = this._open_channel_prepare(peer_id, name);
    if (!pending) return ch;
    if (!opener) {
      if ((await ch._opened.wait_async(this.rpc_timeout)) && !ch.closed) return ch;
      throw new ConnectionError(`Channel ${repr(name)} to peer ${peer_id} did not finish opening (${ch.closed_reason || "timeout"}).`);
    }
    let result = null;
    let err = null;
    try {
      result = this._open_channel_parse(await this.send_rpc_async(peer_id, [..._COMM_CHANNEL_OPEN_PATH], [], { name, lane: ch.lane_id, options: {} }));
    } catch (exc) {
      err = exc;
    }
    return this._open_channel_commit(ch, name, peer_id, result, err);
  }

  /** Pre-bind every ``static_lanes`` entry for a freshly registered peer. */
  _bind_static_lanes(peer_id) {
    const items = dict_items(this.static_lanes ?? {});
    if (items.length === 0 || !this.constructor.supports_channels) return;
    with_lock(this._lane_lock, () => {
      for (const [name, raw_lane] of items) {
        const lane = _int(raw_lane);
        if (!(lane >= 1 && lane <= _MAX_LANE) || name === "default") {
          log.debug("Ignoring invalid static lane %r=%r", name, lane);
          continue;
        }
        if ((this._lanes.get(peer_id) ?? new Map()).has(lane)) continue;
        if ((this._lane_names.get(peer_id) ?? new Map()).has(name)) continue;
        this._new_channel(peer_id, name, lane, lane);
      }
    });
  }

  /** Close every lane to *peer_id* (waking relays). Idempotent. */
  _close_peer_lanes(peer_id, reason) {
    const [lanes, names] = with_lock(this._lane_lock, () => {
      const l = this._lanes.get(peer_id) ?? new Map();
      this._lanes.delete(peer_id);
      const n = this._lane_names.get(peer_id) ?? new Map();
      this._lane_names.delete(peer_id);
      return [l, n];
    });
    const seen = new Set();
    for (const ch of [...lanes.values(), ...names.values()]) {
      if (seen.has(ch)) continue;
      seen.add(ch);
      ch._close_local(reason);
    }
  }

  /** Close every lane on every peer (used by carriers' ``stop()``). */
  _shutdown_lanes(reason = "protocol stopped") {
    const peers = with_lock(this._lane_lock, () => new Set([...this._lanes.keys(), ...this._lane_names.keys()]));
    for (const peer_id of peers) this._close_peer_lanes(peer_id, reason);
  }

  /** ``true`` for the reserved stream-lane control paths. */
  _is_control_path(path) {
    return _path_eq(path, _COMM_CHANNEL_OPEN_PATH) || _path_eq(path, _COMM_CHANNEL_CLOSE_PATH);
  }

  /** Answer a stream-lane control request; never touches the policy. */
  _control_response(msg, peer_id) {
    const params = _mget(msg, "params", {}) ?? {};
    try {
      const result = this._handle_control(_mget(params, "path", []) ?? [], _mget(params, "kwargs", {}) || {}, peer_id);
      return rpc_protocol.make_result(_mget(msg, "id"), result);
    } catch (exc) {
      return rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_EXECUTION, _exc_text(exc));
    }
  }

  /**
   * Allocate / release a lane on behalf of the authenticated *peer_id*.
   *
   * ``__comm_channel_open__`` kwargs ``{"name", "lane", "options"}`` ->
   * ``{"lane": <our rx lane>}``. ``lane`` is the opener's rx lane (what we
   * will write in frames to it); ``options`` is reserved and ignored.
   * ``__comm_channel_close__`` kwargs ``{"lane"}`` (our rx lane) -> ``true``.
   */
  _handle_control(path, kwargs, peer_id) {
    if (peer_id === null || peer_id === undefined) throw new RuntimeError("Stream-lane control requires an authenticated peer.");
    if (!this.constructor.supports_channels) throw new ConnectionError(`${repr(this.protocol_name)} has no stream lanes.`);
    if (_path_eq(path, _COMM_CHANNEL_OPEN_PATH)) {
      const name = _mget(kwargs, "name");
      if (typeof name !== "string" || !name || name === "default") throw new ValueError(`Invalid channel name ${repr(name)}.`);
      const remote_lane = _int(_mget(kwargs, "lane", 0));
      if (!(remote_lane >= 1 && remote_lane <= _MAX_LANE)) throw new ValueError(`Invalid lane ${remote_lane}.`);
      return with_lock(this._lane_lock, () => {
        let ch = (this._lane_names.get(peer_id) ?? new Map()).get(name) ?? null;
        if (ch !== null && ch.closed) {
          this._drop_channel(ch);
          ch = null;
        }
        if (ch !== null) {
          // Either our own open is in flight (adopt the peer's lane) or
          // the peer re-opened after a lost close.
          if (ch.tx_lane_id === null || ch.tx_lane_id === undefined) ch._mark_opened(remote_lane);
          else ch.tx_lane_id = remote_lane;
          return { lane: ch.lane_id };
        }
        const lane = this._alloc_lane(peer_id, remote_lane);
        ch = this._new_channel(peer_id, name, lane, remote_lane);
        return { lane };
      });
    }
    if (_path_eq(path, _COMM_CHANNEL_CLOSE_PATH)) {
      const lane = _int(_mget(kwargs, "lane", 0));
      const ch = with_lock(this._lane_lock, () => {
        const c = (this._lanes.get(peer_id) ?? new Map()).get(lane) ?? null;
        if (c !== null) this._drop_channel(c);
        return c;
      });
      if (ch !== null) ch._close_local("closed by peer");
      return true;
    }
    throw new ValueError(`Unknown control path ${repr(path)}.`);
  }

  /** User called ``Channel.close()``: unbind and tell the peer (best effort). */
  _on_channel_closed_locally(ch) {
    with_lock(this._lane_lock, () => this._drop_channel(ch));
    if (ch.tx_lane_id === null || ch.tx_lane_id === undefined || !this.has_peer(ch.peer_id)) return;
    try {
      this._send_oneway(ch.peer_id, [..._COMM_CHANNEL_CLOSE_PATH], { lane: ch.tx_lane_id });
    } catch (e) {
      log.debug("channel close notice to %s failed", ch.peer_id, { exc_info: e });
    }
  }

  /**
   * Send a control request without waiting for its reply.
   *
   * Default: fire ``send_rpc_async`` in the background and discard the
   * outcome (Python uses a daemon thread). Carriers with a cheap
   * non-blocking write path override.
   */
  _send_oneway(peer_id, path, kwargs) {
    Promise.resolve()
      .then(() => this.send_rpc_async(peer_id, path, [], kwargs))
      .catch(() => {});
  }

  /**
   * Hand one outbound message to the wire. Carriers override.
   *
   * Called on the user's thread by ``Channel.send``; must not block on I/O
   * (marshal onto the carrier loop instead).
   */
  _stream_enqueue(_channel, _payload) {
    throw new ConnectionError(`${repr(this.protocol_name)} cannot carry stream lanes.`);
  }

  /** Resolve a receive-side lane id to its channel (``null`` if unknown). */
  _lookup_lane(peer_id, lane) {
    if (peer_id === null || peer_id === undefined) return null;
    const lanes = this._lanes.get(peer_id);
    if (lanes === undefined) return null;
    return lanes.get(lane) ?? null;
  }

  /**
   * Route one reserved-marker frame from *peer_id* (carrier inbound thread).
   *
   * Any failure is counted and logged at debug; it never propagates, so a
   * malformed stream frame cannot end a receive loop or unregister a peer.
   * Unknown lanes and unknown reserved markers are dropped silently.
   */
  _on_stream_frame(peer_id, raw) {
    try {
      if (!_codec.is_stream_frame(raw)) {
        this._reserved_marker_drops += 1;
        log.debug("dropping frame with reserved marker 0x%02x", raw && raw.length ? raw[0] : -1);
        return;
      }
      const [lane, seq, flags] = _codec.unpack_stream_header(raw);
      const ch = this._lookup_lane(peer_id, lane);
      if (ch === null || ch.closed) {
        this._unknown_lane_drops += 1;
        return;
      }
      ch._on_chunk(seq, flags, raw.subarray(_codec.STREAM_HEADER_LEN), time.monotonic());
    } catch (e) {
      this._malformed_stream_drops += 1;
      log.debug("malformed stream frame from %s dropped", peer_id, { exc_info: e });
    }
  }

  /** Deliver an already-complete message to our rx *lane* (loopback path). */
  _on_stream_payload(peer_id, lane, payload) {
    const ch = this._lookup_lane(peer_id, lane);
    if (ch === null || ch.closed) {
      this._unknown_lane_drops += 1;
      return;
    }
    ch._enqueue_inbound(payload);
  }

  /**
   * Yield the wire frames (header + data) for one outbound message.
   *
   * Slices *payload* into ``stream_chunk_bytes`` pieces with ``START`` on
   * the first and ``END`` on the last; ``b""`` yields one empty
   * ``START|END`` chunk.
   */
  *_stream_chunks(channel, payload) {
    const size = Math.max(1, Math.trunc(this.stream_chunk_bytes));
    const n = payload.length;
    const count = Math.max(1, Math.ceil(n / size));
    const first = channel._next_chunk_seqs(count);
    const lane = channel.tx_lane_id;
    if (count === 1) {
      yield _codec.pack_stream_frame(lane, first, _codec.FLAG_START | _codec.FLAG_END, payload);
      return;
    }
    for (let i = 0; i < count; i++) {
      let flags = 0;
      if (i === 0) flags |= _codec.FLAG_START;
      if (i === count - 1) flags |= _codec.FLAG_END;
      yield _codec.pack_stream_frame(lane, first + i, flags, payload.subarray(i * size, (i + 1) * size));
    }
  }

  /**
   * Round-trip a liveness control frame to *peer_id*.
   *
   * Reuses the carrier's own ``send_rpc`` path with the reserved
   * ``__comm_ping__`` frame, which the peer answers with ``"pong"`` before
   * it ever reaches the policy. Returns ``false`` on any transport error.
   */
  ping(peer_id, timeout = null) {
    if (!this.has_peer(peer_id)) return false;
    // Bound the *wait* by the ping deadline (not rpc_timeout): a deaf peer
    // must not pin a liveness worker for 60 s.
    this._rpc_wait_override.timeout = timeout !== null && timeout !== undefined ? timeout : this.ping_timeout;
    try {
      return this.send_rpc(peer_id, [..._COMM_PING_PATH], [], {}) === "pong";
    } catch {
      return false;
    } finally {
      this._rpc_wait_override.timeout = null;
    }
  }

  /** Awaitable ``ping``. */
  async ping_async(peer_id, timeout = null) {
    if (!this.has_peer(peer_id)) return false;
    const wait = timeout !== null && timeout !== undefined ? timeout : this.ping_timeout;
    try {
      return (await this.send_rpc_async(peer_id, [..._COMM_PING_PATH], [], {}, { wait_timeout: wait })) === "pong";
    } catch {
      return false;
    }
  }

  // ------------------------------------------------------------------
  // Outbound correlation
  // ------------------------------------------------------------------

  /** Allocate a request id + pending slot for an outbound RPC. */
  _register_pending() {
    const request_id = String(uuid4());
    const slot = { event: new Event() };
    this._pending_rpcs.set(request_id, slot);
    return [request_id, slot];
  }

  /** Resolve the pending slot named by response *msg*'s id. */
  _complete_pending(msg) {
    const request_id = _mget(msg, "id");
    if (request_id === null || request_id === undefined) return;
    const slot = this._pending_rpcs.get(request_id);
    if (slot === undefined) return;
    if (_has(msg, "error")) slot.error = _mget(msg, "error");
    else slot.result = _mget(msg, "result");
    slot.event.set();
  }

  _pending_outcome(request_id, slot, completed, timeout) {
    this._pending_rpcs.delete(request_id);
    if (!completed) throw new PyTimeoutError(`RPC to peer timed out after ${timeout}s on ${this.constructor.name}.`);
    if ("error" in slot) {
      const err = slot.error;
      if (_mget(err, "code") === rpc_protocol.ERR_BUSY) throw new BackpressureError(_mget(err, "message", "peer busy"));
      throw new RuntimeError(`Remote RPC error: ${_has(err, "message") ? _mget(err, "message") : repr(err)}`);
    }
    return slot.result ?? null;
  }

  /**
   * Block until *slot* is resolved (or ``rpc_timeout`` elapses).
   *
   * ``ping`` shortens the wait for the calling thread only via the
   * thread-local ``_rpc_wait_override``.
   */
  _await_pending(request_id, slot) {
    const timeout = this._rpc_wait_override.timeout || this.rpc_timeout;
    const completed = slot.event.wait(timeout);
    return this._pending_outcome(request_id, slot, completed, timeout);
  }

  /** Awaitable ``_await_pending``; *wait_timeout* overrides ``rpc_timeout``. */
  async _await_pending_async(request_id, slot, wait_timeout = null) {
    const timeout = wait_timeout || this._rpc_wait_override.timeout || this.rpc_timeout;
    const completed = await slot.event.wait_async(timeout);
    return this._pending_outcome(request_id, slot, completed, timeout);
  }

  /**
   * Send one ``rpc.call`` to *peer_id*, retrying under backpressure.
   *
   * Wraps the carrier-specific ``_send_once`` in an exponential backoff +
   * jitter retry loop scoped to ``BackpressureError`` (an ``ERR_BUSY``
   * reply). After ``max_rpc_retries`` BUSY replies the final
   * ``BackpressureError`` propagates so the caller learns the peer stayed
   * overwhelmed.
   */
  send_rpc(peer_id, path, args, kwargs) {
    let attempt = 0;
    for (;;) {
      try {
        return this._send_once(peer_id, path, args, kwargs);
      } catch (e) {
        if (!(e instanceof BackpressureError)) throw e;
        if (attempt >= this.max_rpc_retries) throw e;
        const delay = Math.min(this.rpc_backoff_max, this.rpc_backoff_base * 2 ** attempt);
        time.sleep(delay + Math.random() * delay);
        attempt += 1;
      }
    }
  }

  /** Awaitable ``send_rpc`` (``opts.wait_timeout`` bounds the response wait). */
  async send_rpc_async(peer_id, path, args, kwargs, opts = {}) {
    let attempt = 0;
    for (;;) {
      try {
        return await this._send_once_async(peer_id, path, args, kwargs, opts);
      } catch (e) {
        if (!(e instanceof BackpressureError)) throw e;
        if (attempt >= this.max_rpc_retries) throw e;
        const delay = Math.min(this.rpc_backoff_max, this.rpc_backoff_base * 2 ** attempt);
        await asyncio.sleep(delay + Math.random() * delay);
        attempt += 1;
      }
    }
  }

  /** Carrier-specific single attempt (blocking). Subclasses implement. */
  _send_once(_peer_id, _path, _args, _kwargs) {
    throw new ConnectionError(`${repr(this.protocol_name)} carrier cannot send RPC.`);
  }

  /** Carrier-specific single attempt (awaitable). Subclasses implement. */
  async _send_once_async(_peer_id, _path, _args, _kwargs, _opts = {}) {
    throw new ConnectionError(`${repr(this.protocol_name)} carrier cannot send RPC.`);
  }

  // ------------------------------------------------------------------
  // Wire helpers
  // ------------------------------------------------------------------

  /** Serialise *obj* with this carrier's configured codec. */
  _encode(obj) {
    return _codec.encode(obj, this.codec);
  }

  /** Deserialise *data* with this carrier's configured codec. */
  _decode(data) {
    return _codec.decode(data, this.codec);
  }

  /** Build the standard ``rpc.call`` request envelope. */
  _make_rpc_request(path, args, kwargs, request_id) {
    return rpc_protocol.make_request("rpc.call", { path: _list(path), args: _list(args), kwargs: _dict(kwargs) }, request_id);
  }

  /** Tear down the inbound worker pool (called from ``stop``). */
  _shutdown_executor() {
    if (this._inbound_executor !== null && this._inbound_executor !== undefined) {
      this._inbound_executor.shutdown({ wait: false, cancel_futures: true });
      this._inbound_executor = null;
    }
  }

  /**
   * Import each name in *modules*, raising a clear capability error.
   *
   * Concrete transports that wrap a third-party driver call this at the
   * top of their connection hook. When a driver is missing the raised
   * ``RuntimeError`` names both the package and the
   * ``npm install laila-core[<extra>]`` hint that provides it -- never a
   * bare module-not-found error and never a silent stub.
   * @param {string[]} modules
   * @param {string} extra
   * @returns {Record<string, any>}
   */
  _require_drivers(modules, extra) {
    const loaded = {};
    for (const name of modules) {
      try {
        loaded[name] = _require(name);
      } catch (exc) {
        if (exc && (exc.code === "MODULE_NOT_FOUND" || exc.code === "ERR_MODULE_NOT_FOUND")) {
          const err = new RuntimeError(`The ${repr(this.protocol_name)} transport requires ${repr(name)}, which is not installed. Install it with \`pip install laila-core[${extra}]\`.`);
          err.__cause__ = exc;
          throw err;
        }
        throw exc;
      }
    }
    return loaded;
  }
}

void eq;

register("laila.policy.central.communication.protocols._carriers.base", {
  _CarrierRPCProtocol,
  BackpressureError,
  _COMM_PING_PATH,
  _COMM_CHANNEL_OPEN_PATH,
  _COMM_CHANNEL_CLOSE_PATH,
  _MAX_LANE,
  _PEER_CONNECT,
  _PEER_DISCONNECT,
  _GOODBYE_TIMEOUT,
});
