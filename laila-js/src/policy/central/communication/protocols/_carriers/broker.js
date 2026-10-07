/**
 * Broker / pub-sub mediated RPC carrier.
 *
 * ``_BrokerRPCProtocol`` carries JSON-RPC over a message broker or pub/sub
 * fabric (MQTT, AMQP, XMPP, DDS/RTPS, ZeroMQ, ...). There is no
 * point-to-point socket: instead every policy subscribes to its own *inbox*
 * topic and addresses a peer by publishing to the peer's inbox.
 *
 * Addressing & correlation
 * -------------------------
 * - Each endpoint owns an inbox topic ``laila/inbox/<policy_global_id>``.
 * - A request carries a ``reply_to`` (the sender's inbox) so the responder
 *   knows where to publish the reply; request/response are then correlated
 *   by the JSON-RPC ``id`` via the inherited pending-RPC table.
 * - Peering publishes a ``peer.connect`` to the target inbox; the acceptor
 *   records ``peer_id -> reply_to`` and replies.
 *
 * Concrete transports supply the broker plumbing: ``_broker_connect``,
 * ``_broker_subscribe``, ``_broker_publish`` and ``_broker_close``. Inbound
 * messages are delivered by calling ``_feed_message(data)`` (the carrier
 * figures out request vs response).
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, NotImplementedError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { PrivateAttr, define_private } from "../../../../../_compat/pydantic.js";
import { Event } from "../../../../../_compat/threading.js";
import * as rpc_protocol from "../../protocol.js";
import { _CarrierRPCProtocol, _PEER_CONNECT, _mget } from "./base.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./loopthread.js";
import { uri_authority } from "./uri.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.broker");
void log;

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * Carrier for broker / pub-sub fabrics.
 *
 * Like the other carriers it owns a dedicated event loop on a daemon thread;
 * the concrete broker client is created inside that loop.
 */
export class _BrokerRPCProtocol extends _CarrierRPCProtocol {
  static {
    define_private(this, {
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _inbox: PrivateAttr({ default: null }),
      _handshake_pending: PrivateAttr({ default_factory: () => new Map() }),
    });
  }

  // ------------------------------------------------------------------
  // Subclass hooks
  // ------------------------------------------------------------------

  /** Establish the broker client connection. Runs in the loop. */
  async _broker_connect() {
    throw new NotImplementedError();
  }

  /** Subscribe to *topic*; inbound payloads must reach ``_feed_message``. */
  async _broker_subscribe(_topic) {
    throw new NotImplementedError();
  }

  /** Publish *data* to *topic*. */
  async _broker_publish(_topic, _data) {
    throw new NotImplementedError();
  }

  /** Close the broker connection. Default: no-op. */
  async _broker_close() {
    return null;
  }

  /** Topic this endpoint listens on for inbound frames. */
  _inbox_topic() {
    const pid = this._communication ? this._communication.policy_id : null;
    return `laila/inbox/${pid}`;
  }

  /** Inbox topic of a peer addressed by its policy global_id. */
  _peer_inbox(peer_policy_id) {
    return `laila/inbox/${peer_policy_id}`;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Boot the loop, connect to the broker, subscribe to the inbox. */
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
    await this._broker_connect();
    this._inbox = this._inbox_topic();
    await this._broker_subscribe(this._inbox);
    ready.set();
  }

  /** Say goodbye, disconnect from the broker and stop the loop (idempotent). */
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
    await this._broker_close();
    await cancel_pending_tasks();
  }

  _after_stop() {
    this._shutdown_lanes();
    this._handshake_pending.clear();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  /** Publish ``peer.disconnect`` to the peer's inbox. */
  async _send_goodbye(peer_id) {
    const inbox = this._connections.get(peer_id) ?? null;
    if (inbox === null) return;
    await this._broker_publish(inbox, this._encode(this._goodbye_message()));
  }

  // ------------------------------------------------------------------
  // Inbound routing
  // ------------------------------------------------------------------

  /** Inbound entry point: decode a frame and route it. */
  _feed_message(data) {
    let msg;
    try {
      msg = this._decode(data);
    } catch {
      return;
    }

    if (rpc_protocol.is_request(msg)) {
      const method = _mget(msg, "method");
      if (method === _PEER_CONNECT) {
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
    const reply_to = _mget(params, "reply_to");
    if (_mget(params, "secret") !== this.peer_secret_key) {
      const resp = rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key.");
      this._publish_async(reply_to, resp);
      return;
    }
    const policy_id = this._communication ? this._communication.policy_id : null;
    const resp = rpc_protocol.make_result(_mget(msg, "id"), { peer_id: policy_id });
    this._register_peer(peer_id, reply_to);
    this._publish_async(reply_to, resp);
  }

  _dispatch_request(msg) {
    const reply_to = _mget(msg, "reply_to");
    this._handle_request_frame(msg, (resp) => this._publish_async(reply_to, resp));
  }

  _publish_async(topic, obj) {
    if (topic === null || topic === undefined) return;
    const data = this._encode(obj);
    this._loop_call(() => asyncio.ensure_future(() => this._broker_publish(topic, data)));
  }

  // ------------------------------------------------------------------
  // Peering / RPC
  // ------------------------------------------------------------------

  _connect_prepare(uri, secret) {
    const peer_policy_id = uri_authority(uri);
    const peer_inbox = this._peer_inbox(peer_policy_id);
    const policy_id = this._communication ? this._communication.policy_id : null;

    const req = rpc_protocol.make_request("peer.connect", { from_id: policy_id, secret, reply_to: this._inbox });
    const rid = req.id;
    const slot = { event: new Event() };
    this._handshake_pending.set(rid, slot);
    this._publish_async(peer_inbox, req);
    return [peer_inbox, rid, slot];
  }

  _connect_finish(peer_inbox, rid, slot, completed) {
    this._handshake_pending.delete(rid);
    if (!completed) throw new ConnectionError("Peer handshake timed out.");
    const reply = slot.msg;
    if (_has(reply, "error")) {
      const error = _mget(reply, "error");
      throw new ConnectionError(`Peer rejected connection: ${_has(error, "message") ? _mget(error, "message") : error}`);
    }
    const peer_id = _mget(_mget(reply, "result", {}) ?? {}, "peer_id");
    if (peer_id === null || peer_id === undefined) throw new ConnectionError("Peer response missing peer_id.");
    this._register_peer(peer_id, peer_inbox);
    return peer_id;
  }

  /** Handshake with the peer whose policy id is encoded in *uri*. */
  connect(uri, secret) {
    this.start();
    const [peer_inbox, rid, slot] = this._connect_prepare(uri, secret);
    const completed = slot.event.wait(this.handshake_timeout);
    return this._connect_finish(peer_inbox, rid, slot, completed);
  }

  /** Awaitable ``connect``. */
  async connect_async(uri, secret) {
    await this.start_async();
    const [peer_inbox, rid, slot] = this._connect_prepare(uri, secret);
    const completed = await slot.event.wait_async(this.handshake_timeout);
    return this._connect_finish(peer_inbox, rid, slot, completed);
  }

  _send_once_prepare(peer_id, path, args, kwargs) {
    const peer_inbox = this._connections.get(peer_id) ?? null;
    if (peer_inbox === null) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const [request_id, slot] = this._register_pending();
    const req = this._make_rpc_request(path, args, kwargs, request_id);
    req.reply_to = this._inbox;
    try {
      this._publish_async(peer_inbox, req);
    } catch (e) {
      if (e instanceof ConnectionError) this._pending_rpcs.delete(request_id);
      throw e;
    }
    return [request_id, slot];
  }

  /** Publish an ``rpc.call`` to *peer_id*'s inbox and block for the reply. */
  _send_once(peer_id, path, args, kwargs) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending(request_id, slot);
  }

  async _send_once_async(peer_id, path, args, kwargs, opts = {}) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending_async(request_id, slot, opts.wait_timeout ?? null);
  }
}

register("laila.policy.central.communication.protocols._carriers.broker", { _BrokerRPCProtocol });
