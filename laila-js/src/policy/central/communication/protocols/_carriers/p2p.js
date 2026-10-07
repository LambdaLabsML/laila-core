/**
 * Point-to-point duplex-stream RPC carrier.
 *
 * ``_P2PStreamRPCProtocol`` is the variant of the stream carrier for links
 * that are a *single, already-connected* bidirectional byte stream with no
 * listen/accept step: serial lines (UART/RS-232/RS-485), USB-CDC, Bluetooth
 * RFCOMM, a paired BLE characteristic, etc. Both endpoints simply open the
 * link; one side initiates the ``peer.connect`` handshake and the other
 * answers it on the same stream.
 *
 * A concrete transport supplies one coroutine, ``_open_stream``, returning
 * the link's ``[reader, writer]`` pair (an ``asyncio.StreamReader`` and a
 * writer exposing ``write`` / ``drain`` / ``close``). The carrier owns the
 * dedicated event loop, length-prefixed framing, the single-peer handshake,
 * the receive loop, off-thread dispatch and the pending-RPC table.
 *
 * Stream lanes ride the same link through one ``_Link`` (priority RPC queue
 * + chunked stream queue, see ``stream.js``). Because serial links are slow,
 * ``stream_chunk_bytes`` defaults to ``4096`` here: a 30 KB frame at 1 Mbaud
 * is ~300 ms of wire time, and a liveness ping must never wait behind it.
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, NotImplementedError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { Event } from "../../../../../_compat/threading.js";
import { uuid4 } from "../../../../../_compat/uuid.js";
import * as rpc_protocol from "../../protocol.js";
import * as _codec from "./codec.js";
import { _CarrierRPCProtocol, _PEER_CONNECT, _mget } from "./base.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./loopthread.js";
import { _Link } from "./stream.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.p2p");

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * Carrier for a single point-to-point duplex byte stream.
 *
 * The per-peer handle stored in ``_connections`` is ``true``; the one link
 * is held in ``_link``.
 */
export class _P2PStreamRPCProtocol extends _CarrierRPCProtocol {
  static supports_channels = true;

  static {
    define_fields(this, {
      // Serial-friendly default: small chunks keep RPC latency low.
      stream_chunk_bytes: ["int", Field({ default: 4096 })],
    });
    define_private(this, {
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _reader: PrivateAttr({ default: null }),
      _writer: PrivateAttr({ default: null }),
      _link: PrivateAttr({ default: null }),
      _recv_task: PrivateAttr({ default: null }),
      _handshake_pending: PrivateAttr({ default_factory: () => new Map() }),
    });
  }

  // ------------------------------------------------------------------
  // Subclass hooks
  // ------------------------------------------------------------------

  /** Open the link and return its ``[reader, writer]`` pair. */
  async _open_stream() {
    throw new NotImplementedError();
  }

  /** Close the link. Default: close the writer. */
  async _close_stream() {
    if (this._link !== null && this._link !== undefined) {
      this._link.close();
      this._link = null;
    }
    if (this._writer !== null && this._writer !== undefined) {
      try {
        this._writer.close();
      } catch {
        /* already closed */
      }
    }
    this._writer = null;
    this._reader = null;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Boot the loop, open the stream, start the receive loop. */
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
    [this._reader, this._writer] = await this._open_stream();
    this._link = new _Link(this, this._writer, this._event_loop);
    this._link.start();
    this._recv_task = asyncio.ensure_future(() => this._receive_loop());
    ready.set();
  }

  /**
   * Close lanes, the stream and the loop (idempotent).
   *
   * Order: goodbye to the peer, unregister it, stop the receive task (cancel
   * *and wait*, so ``read_frame()`` has finished with the reader before any
   * fd underneath it is closed), close the stream, cancel and await every
   * remaining task, stop and close the loop.
   */
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
    if (this._recv_task !== null && this._recv_task !== undefined) {
      this._recv_task.cancel();
      await asyncio.gather(this._recv_task, { return_exceptions: true });
      this._recv_task = null;
    }
    await this._close_stream();
    await cancel_pending_tasks();
  }

  _after_stop() {
    this._shutdown_lanes();
    this._handshake_pending.clear();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  /** Queue ``peer.disconnect`` on the link and wait for it to be written. */
  async _send_goodbye(peer_id) {
    const link = this._link;
    if (link === null || link === undefined || link.closed || !this._connections.has(peer_id)) return;
    this._queue_frame(this._goodbye_message());
    await link.flush(0.5);
  }

  // ------------------------------------------------------------------
  // Receive loop / dispatch
  // ------------------------------------------------------------------

  /** The single registered peer id, if any. */
  _sole_peer() {
    for (const peer_id of this._connections.keys()) return peer_id;
    return null;
  }

  async _receive_loop() {
    const reply = this._make_reply();
    try {
      for (;;) {
        const raw = await _codec.read_frame(this._reader);
        if (raw === null) break;
        if (_codec.is_reserved_frame(raw)) {
          const peer_id = this._sole_peer();
          if (peer_id === null) {
            // stream frame before any handshake: not ours yet
            this._unknown_lane_drops += 1;
            continue;
          }
          this._on_stream_frame(peer_id, raw);
          continue;
        }
        const msg = this._decode(raw);
        if (rpc_protocol.is_request(msg)) {
          if (_mget(msg, "method") === _PEER_CONNECT) {
            await this._handle_handshake(msg);
          } else if (this._is_goodbye(msg)) {
            // The link stays open (it is the wire itself); only the peering
            // is dropped, so a new handshake can follow.
            const from_id = _mget(_mget(msg, "params") || {}, "from_id");
            if (from_id === this._sole_peer()) this._on_peer_goodbye(from_id);
          } else {
            this._handle_request_frame(msg, reply, this._sole_peer());
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
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError) && !(e instanceof ConnectionError)) log.debug("P2P receive loop ended", { exc_info: e });
    } finally {
      for (const peer_id of [...this._connections.keys()]) this._unregister_peer(peer_id);
    }
  }

  /** Frame *obj* and queue it on the link (loop thread). */
  _queue_frame(obj) {
    if (this._link === null || this._link === undefined) return;
    this._link.put_rpc(_codec.frame(this._encode(obj)));
  }

  /** Queue *obj* for the writer task (kept for subclass compatibility). */
  async _write_frame(obj) {
    this._queue_frame(obj);
  }

  async _handle_handshake(msg) {
    const params = _mget(msg, "params", {}) ?? {};
    const peer_id = _mget(params, "from_id");
    if (_mget(params, "secret") !== this.peer_secret_key) {
      await this._write_frame(rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."));
      return;
    }
    const policy_id = this._communication ? this._communication.policy_id : null;
    this._set_peer_caps(peer_id, _mget(params, "caps"));
    this._register_peer(peer_id, true);
    await this._write_frame(rpc_protocol.make_result(_mget(msg, "id"), { peer_id: policy_id, caps: this._local_caps() }));
  }

  /**
   * Build a thread-safe ``reply(resp)`` that queues a frame on the link.
   *
   * Used by ``_handle_request_frame``; safe both inline on the I/O loop
   * (ping / control fast-path) and from an inbound worker thread.
   */
  _make_reply() {
    return (resp) => {
      try {
        this._loop_call(() => this._queue_frame(resp));
      } catch (e) {
        if (!(e instanceof ConnectionError)) throw e;
      }
    };
  }

  // ------------------------------------------------------------------
  // Peering / RPC / stream
  // ------------------------------------------------------------------

  _connect_prepare(secret) {
    const policy_id = this._communication ? this._communication.policy_id : null;
    const req = rpc_protocol.make_request("peer.connect", { from_id: policy_id, secret, caps: this._local_caps() });
    const rid = req.id;
    const slot = { event: new Event() };
    this._handshake_pending.set(rid, slot);
    this._loop_call(() => this._queue_frame(req));
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
    const result = _mget(reply, "result", {}) || {};
    const peer_id = _mget(result, "peer_id");
    if (peer_id === null || peer_id === undefined) throw new ConnectionError("Peer response missing peer_id.");
    this._set_peer_caps(peer_id, _mget(result, "caps"));
    this._register_peer(peer_id, true);
    return peer_id;
  }

  /** Initiate the handshake over the already-open point-to-point link. */
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

  _queue_rpc_threadsafe(msg) {
    if (this._link === null || this._link === undefined || this._link.closed) throw new ConnectionError("Point-to-point link is not open.");
    this._loop_call(() => this._queue_frame(msg));
  }

  _send_once_prepare(peer_id, path, args, kwargs) {
    if (!this._connections.has(peer_id)) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const [request_id, slot] = this._register_pending();
    const req = this._make_rpc_request(path, args, kwargs, request_id);
    try {
      this._queue_rpc_threadsafe(req);
    } catch (e) {
      if (e instanceof ConnectionError) this._pending_rpcs.delete(request_id);
      throw e;
    }
    return [request_id, slot];
  }

  /** Write an ``rpc.call`` to the link and block for the response. */
  _send_once(peer_id, path, args, kwargs) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending(request_id, slot);
  }

  async _send_once_async(peer_id, path, args, kwargs, opts = {}) {
    const [request_id, slot] = this._send_once_prepare(peer_id, path, args, kwargs);
    return this._await_pending_async(request_id, slot, opts.wait_timeout ?? null);
  }

  /** Queue a control request without waiting for the reply. */
  _send_oneway(peer_id, path, kwargs) {
    if (!this._connections.has(peer_id)) throw new ConnectionError(`No connection to peer ${peer_id}`);
    this._queue_rpc_threadsafe(this._make_rpc_request(path, [], kwargs, String(uuid4())));
  }

  /** Marshal one stream message onto the link (user thread). */
  _stream_enqueue(channel, payload) {
    if (!this._connections.has(channel.peer_id)) throw new ConnectionError(`No connection to peer ${channel.peer_id}`);
    const link = this._link;
    if (link === null || link === undefined || link.closed) throw new ConnectionError("Point-to-point link is not open.");
    this._loop_call(() => link.put_stream(channel, payload));
  }
}

register("laila.policy.central.communication.protocols._carriers.p2p", { _P2PStreamRPCProtocol });
