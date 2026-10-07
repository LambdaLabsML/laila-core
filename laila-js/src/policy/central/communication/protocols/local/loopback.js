/**
 * In-process loopback communication transport.
 *
 * The degenerate transport: both policies live in the *same* process, so
 * there is no socket at all -- an RPC is dispatched by directly invoking the
 * target policy's inbound handler. It still goes through the codec (encode
 * then decode) so future-shaped results are virtualised into
 * ``RemoteFuture`` proxies exactly as they would be over a real wire, which
 * makes loopback a faithful, dependency-free way to exercise the full
 * inter-policy path in one process.
 *
 * - ``protocol_name`` ``"loopback"`` (aliases ``local`` / ``inproc``)
 * - URI scheme ``loopback://<policy_global_id>``
 *
 * Peers are matched through a process-wide registry keyed by the owning
 * policy's ``global_id``.
 *
 * Stream lanes work too: there is no wire, so a ``send()`` hands the
 * ``bytes`` object straight to the peer carrier's inbound lane (zero copy),
 * and the lane control frames go through the same direct
 * ``_build_response`` call as RPC.
 */
import { ConnectionError, RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { repr } from "../../../../../_compat/pyrepr.js";
import { uuid4 } from "../../../../../_compat/uuid.js";
import { _CarrierRPCProtocol, _mget } from "../_carriers/base.js";
import { uri_authority } from "../_carriers/uri.js";
import { register_comm_protocol } from "../base.js";

/** process-wide registry: policy global_id -> loopback protocol instance */
export const _LOOPBACK_REGISTRY = new Map();

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * Same-process loopback transport.
 *
 * Registers itself under its owning policy's ``global_id`` on ``start``, so
 * another in-process policy can peer with ``loopback://<that-global-id>``.
 */
export class _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL extends _CarrierRPCProtocol {
  static protocol_name = "loopback";
  static supports_channels = true;
  static _TOKEN_ALIASES = Object.freeze(new Set(["loopback", "local", "inproc"]));

  /** Accept ``"loopback"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``loopback://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("loopback://");
  }

  _policy_id() {
    return this._communication ? this._communication.policy_id : null;
  }

  /** Register this endpoint in the process-wide loopback registry. */
  start() {
    if (this._started) return;
    this._ensure_executor();
    const pid = this._policy_id();
    if (pid !== null && pid !== undefined) _LOOPBACK_REGISTRY.set(String(pid), this);
    this._started = true;
  }

  /** Deregister, tell every peer we are gone, drop them (idempotent). */
  stop() {
    if (!this._started) return;
    const pid = this._policy_id();
    if (pid !== null && pid !== undefined) _LOOPBACK_REGISTRY.delete(String(pid));
    for (const peer_id of [...this._connections.keys()]) this.disconnect(peer_id);
    this._shutdown_lanes();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  /**
   * Drop *peer_id* on both ends. Idempotent.
   *
   * There is no wire for a ``peer.disconnect`` frame, so the goodbye is a
   * direct call into the peer carrier; without it the other side keeps us
   * registered forever (its liveness pings still reach this live object and
   * succeed).
   */
  disconnect(peer_id) {
    const target = this._connections.get(peer_id) ?? null;
    if (target === null) return;
    this._unregister_peer(peer_id);
    const my_pid = this._policy_id();
    if (my_pid !== null && my_pid !== undefined) target._on_peer_goodbye(String(my_pid));
  }

  async disconnect_async(peer_id) {
    return this.disconnect(peer_id);
  }

  /** Peer with another in-process policy named by *uri*. */
  connect(uri, secret) {
    this.start();
    const target_pid = uri_authority(uri);
    const target = _LOOPBACK_REGISTRY.get(String(target_pid)) ?? null;
    if (target === null) {
      throw new ConnectionError(`No in-process loopback endpoint for policy ${repr(target_pid)}. Start the target policy's loopback connection first.`);
    }
    if (secret !== target.peer_secret_key) throw new ConnectionError("Invalid peer secret key.");

    const my_pid = this._policy_id();
    // Register both directions so either side can call the other.
    // Both ends are this very class, so lane capability is known.
    this._set_peer_caps(String(target_pid), target._local_caps());
    this._register_peer(String(target_pid), target);
    if (my_pid !== null && my_pid !== undefined) {
      target._set_peer_caps(String(my_pid), this._local_caps());
      target._register_peer(String(my_pid), this);
    }
    return String(target_pid);
  }

  async connect_async(uri, secret) {
    return this.connect(uri, secret);
  }

  /**
   * Dispatch an RPC directly into the peer policy (zero-copy).
   *
   * Since both policies are in the same process, loopback skips the codec
   * entirely and passes live objects (the returned value is the target's
   * real result/future, not a serialised envelope). This is the fastest
   * possible path -- no encode/decode, no socket.
   */
  send_rpc(peer_id, path, args, kwargs) {
    const target = this._connections.get(peer_id) ?? null;
    if (target === null) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const req = this._make_rpc_request(path, args, kwargs, String(uuid4()));
    // Run on the TARGET so its _execute_rpc walks the target policy.
    // Our own policy id identifies us as the authenticated caller.
    const resp = target._build_response_sync(req, this._policy_id());
    return this._unwrap(resp);
  }

  /** Awaitable ``send_rpc``. */
  async send_rpc_async(peer_id, path, args, kwargs) {
    const target = this._connections.get(peer_id) ?? null;
    if (target === null) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const req = this._make_rpc_request(path, args, kwargs, String(uuid4()));
    const resp = await target._build_response(req, this._policy_id());
    return this._unwrap(resp);
  }

  _unwrap(resp) {
    if (_has(resp, "error")) {
      const err = resp.error;
      throw new RuntimeError(`Remote RPC error: ${_has(err, "message") ? err.message : err}`);
    }
    return _mget(resp, "result");
  }

  /** Control frame without a reply: same direct dispatch, result ignored. */
  _send_oneway(peer_id, path, kwargs) {
    const target = this._connections.get(peer_id) ?? null;
    if (target === null) return;
    const req = this._make_rpc_request(path, [], kwargs, String(uuid4()));
    target._build_response_sync(req, this._policy_id());
  }

  /** Deliver *payload* straight into the peer's matching inbound lane. */
  _stream_enqueue(channel, payload) {
    const target = this._connections.get(channel.peer_id) ?? null;
    if (target === null) throw new ConnectionError(`No connection to peer ${channel.peer_id}`);
    const my_pid = this._policy_id();
    if (my_pid === null || my_pid === undefined) throw new ConnectionError("Loopback carrier has no owning policy id.");
    target._on_stream_payload(String(my_pid), channel.tx_lane_id, payload);
  }

  /** Convenience wrapper building the ``loopback://`` URI. */
  connect_loopback(policy_global_id, secret) {
    return this.connect(`loopback://${policy_global_id}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.local.loopback", {
  _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL,
  _LOOPBACK_REGISTRY,
});
