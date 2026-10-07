/**
 * Register / mailbox (master-slave bus) RPC carrier.
 *
 * ``_RegisterRPCProtocol`` carries JSON-RPC over a register/byte bus that
 * has no asynchronous push -- Modbus, I2C, SPI, 1-Wire, EtherNet/IP. On
 * such buses a message is exchanged by writing it into a *mailbox* region
 * on the peer and polling the peer's mailbox for replies.
 *
 * The carrier provides the reliable JSON-RPC layer (handshake,
 * request/response correlation, off-thread dispatch) and a polling loop; a
 * concrete transport supplies just two coroutines:
 *
 * - ``_deliver(data)`` -- write one length-prefixed frame to the peer's
 *   inbox region.
 * - ``_poll_inbound()`` -- read the next length-prefixed frame from our own
 *   inbox region, or return ``null`` when nothing is pending.
 *
 * These buses are point-to-point (one peer per link), so addressing is
 * implicit; the single peer is still tracked by ``global_id`` for
 * consistency with the other carriers.
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, NotImplementedError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { Event } from "../../../../../_compat/threading.js";
import * as rpc_protocol from "../../protocol.js";
import { _CarrierRPCProtocol, _PEER_CONNECT, _mget } from "./base.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./loopthread.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.register");

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * Carrier for polled master/slave register buses.
 *
 * Fields
 * ------
 * poll_interval : float, default ``0.01``
 *     Seconds between mailbox polls.
 */
export class _RegisterRPCProtocol extends _CarrierRPCProtocol {
  static {
    define_fields(this, {
      poll_interval: ["float", Field({ default: 0.01 })],
    });
    define_private(this, {
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _poll_task: PrivateAttr({ default: null }),
      _handshake_pending: PrivateAttr({ default_factory: () => new Map() }),
    });
  }

  // ------------------------------------------------------------------
  // Subclass hooks
  // ------------------------------------------------------------------

  /** Open the underlying bus/device. Runs in the loop. Default no-op. */
  async _open_bus() {
    return null;
  }

  /** Close the underlying bus/device. Default no-op. */
  async _close_bus() {
    return null;
  }

  /** Write one length-prefixed frame to the peer's inbox region. */
  async _deliver(_data) {
    throw new NotImplementedError();
  }

  /** Read the next inbound frame from our inbox region, or ``null``. */
  async _poll_inbound() {
    throw new NotImplementedError();
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Open the bus and start the polling loop (idempotent). */
  start() {
    if (this._started) return;
    this._ensure_executor();
    try {
      start_loop_thread(this, (ready) => this._async_start(ready), { ready_timeout: Math.max(this.handshake_timeout, 10.0) });
    } catch (e) {
      this._shutdown_executor();
      throw e;
    }
    this._started = true;
  }

  /** Awaitable ``start``. */
  async start_async() {
    if (this._started) return;
    this._ensure_executor();
    try {
      await start_loop_thread_async(this, (ready) => this._async_start(ready), { ready_timeout: Math.max(this.handshake_timeout, 10.0) });
    } catch (e) {
      this._shutdown_executor();
      throw e;
    }
    this._started = true;
  }

  async _async_start(ready) {
    await this._open_bus();
    this._poll_task = asyncio.ensure_future(() => this._poll_loop());
    ready.set();
  }

  /** Say goodbye, stop polling, close the bus, stop the loop (idempotent). */
  stop() {
    if (!this._started) return;
    stop_loop_thread(this, () => this._shutdown());
    this._after_stop();
  }

  /** Awaitable ``stop``. */
  async stop_async() {
    if (!this._started) return;
    await stop_loop_thread_async(this, () => this._shutdown());
    this._after_stop();
  }

  async _shutdown() {
    await this._goodbye_all();
    for (const peer_id of [...this._connections.keys()]) this._unregister_peer(peer_id);
    this._shutdown_lanes();
    if (this._poll_task !== null && this._poll_task !== undefined) {
      this._poll_task.cancel();
      await asyncio.gather(this._poll_task, { return_exceptions: true });
      this._poll_task = null;
    }
    await this._close_bus();
    await cancel_pending_tasks();
  }

  _after_stop() {
    this._shutdown_lanes();
    this._handshake_pending.clear();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  /** Write ``peer.disconnect`` into the peer's mailbox. */
  async _send_goodbye(peer_id) {
    if (!this._connections.has(peer_id)) return;
    await this._deliver(this._encode(this._goodbye_message()));
  }

  /** Continuously poll the mailbox and dispatch inbound frames. */
  async _poll_loop() {
    try {
      for (;;) {
        const data = await this._poll_inbound();
        if (data !== null && data !== undefined && data.length) this._feed_message(data);
        else await asyncio.sleep(this.poll_interval);
      }
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError)) log.debug("Register poll loop ended", { exc_info: e });
    }
  }

  // ------------------------------------------------------------------
  // Inbound routing
  // ------------------------------------------------------------------

  _feed_message(data) {
    let msg;
    try {
      msg = this._decode(data);
    } catch {
      return;
    }
    if (rpc_protocol.is_request(msg)) {
      if (_mget(msg, "method") === _PEER_CONNECT) {
        this._handle_handshake(msg);
      } else if (this._is_goodbye(msg)) {
        this._on_peer_goodbye(_mget(_mget(msg, "params") || {}, "from_id"));
      } else {
        this._dispatch_request(msg);
      }
    } else if (rpc_protocol.is_response(msg)) {
      const rid = _mget(msg, "id");
      if (this._handshake_pending.has(rid)) {
        const slot = this._handshake_pending.get(rid);
        if (slot !== null && slot !== undefined) {
          slot.msg = msg;
          slot.event.set();
        }
      } else {
        this._complete_pending(msg);
      }
    }
  }

  _handle_handshake(msg) {
    const params = _mget(msg, "params", {}) ?? {};
    const peer_id = _mget(params, "from_id");
    if (_mget(params, "secret") !== this.peer_secret_key) {
      const resp = rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key.");
      this._deliver_async(resp);
      return;
    }
    const policy_id = this._communication ? this._communication.policy_id : null;
    const resp = rpc_protocol.make_result(_mget(msg, "id"), { peer_id: policy_id });
    this._register_peer(peer_id, true);
    this._deliver_async(resp);
  }

  _dispatch_request(msg) {
    this._handle_request_frame(msg, (resp) => this._deliver_async(resp));
  }

  _deliver_async(obj) {
    const data = this._encode(obj);
    this._loop_call(() => asyncio.ensure_future(() => this._deliver(data)));
  }

  // ------------------------------------------------------------------
  // Peering / RPC
  // ------------------------------------------------------------------

  _connect_prepare(secret) {
    const policy_id = this._communication ? this._communication.policy_id : null;
    const req = rpc_protocol.make_request("peer.connect", { from_id: policy_id, secret });
    const rid = req.id;
    const slot = { event: new Event() };
    this._handshake_pending.set(rid, slot);
    this._deliver_async(req);
    return [rid, slot];
  }

  _connect_finish(rid, slot, completed) {
    this._handshake_pending.delete(rid);
    if (!completed) throw new ConnectionError("Peer handshake timed out.");
    const reply = slot.msg;
    if (_has(reply, "error")) {
      const error = _mget(reply, "error");
      throw new ConnectionError(`Peer rejected connection: ${_has(error, "message") ? _mget(error, "message") : error}`);
    }
    const peer_id = _mget(_mget(reply, "result", {}) ?? {}, "peer_id");
    if (peer_id === null || peer_id === undefined) throw new ConnectionError("Peer response missing peer_id.");
    this._register_peer(peer_id, true);
    return peer_id;
  }

  /** Handshake with the single peer on the other end of the bus. */
  connect(_uri, secret) {
    this.start();
    const [rid, slot] = this._connect_prepare(secret);
    const completed = slot.event.wait(this.handshake_timeout);
    return this._connect_finish(rid, slot, completed);
  }

  /** Awaitable ``connect``. */
  async connect_async(_uri, secret) {
    await this.start_async();
    const [rid, slot] = this._connect_prepare(secret);
    const completed = await slot.event.wait_async(this.handshake_timeout);
    return this._connect_finish(rid, slot, completed);
  }

  _send_once_prepare(peer_id, path, args, kwargs) {
    if (!this._connections.has(peer_id)) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const [request_id, slot] = this._register_pending();
    const req = this._make_rpc_request(path, args, kwargs, request_id);
    try {
      this._deliver_async(req);
    } catch (e) {
      if (e instanceof ConnectionError) this._pending_rpcs.delete(request_id);
      throw e;
    }
    return [request_id, slot];
  }

  /** Write an ``rpc.call`` to the bus and block for the polled reply. */
  _send_once(peer_id, path, args, kwargs) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending(request_id, slot);
  }

  async _send_once_async(peer_id, path, args, kwargs, opts = {}) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending_async(request_id, slot, opts.wait_timeout ?? null);
  }
}

register("laila.policy.central.communication.protocols._carriers.register", { _RegisterRPCProtocol });
