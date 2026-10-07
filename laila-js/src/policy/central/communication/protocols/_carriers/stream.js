/**
 * Reliable, ordered, duplex byte-stream RPC carrier.
 *
 * ``_StreamRPCProtocol`` implements the full
 * ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` contract over any transport that
 * looks like an ``asyncio.StreamReader`` / ``StreamWriter`` pair: TCP, TLS,
 * Unix domain sockets, serial lines, USB-CDC, RFCOMM, ...
 *
 * A concrete transport only supplies two coroutines:
 *
 * - ``_serve`` -- create and return a listening server whose per-connection
 *   callback is ``_handle_inbound_stream`` (e.g.
 *   ``await asyncio.start_server((r, w) => this._handle_inbound_stream(r, w), host, port)``).
 * - ``_open_connection`` -- open one outbound connection to a URI and return
 *   its ``[reader, writer]`` pair.
 *
 * Everything else -- the dedicated background event loop, length-prefixed
 * framing, the ``peer.connect`` handshake, the receive loop, off-thread
 * inbound dispatch, and the blocking pending-RPC table -- is handled here.
 *
 * Stream lanes and writer fairness
 * --------------------------------
 * Each peer connection is wrapped in a ``_Link``: the writer plus two
 * outbound queues (RPC/control, which has priority, and stream) drained by a
 * single writer task. **Every** write -- handshake frames, replies,
 * ``_send_once``, stream chunks -- goes through the link; nothing else calls
 * ``writer.write``. Stream messages are sliced into ``stream_chunk_bytes``
 * chunks and the RPC queue is drained between chunks, so a liveness ping is
 * never stuck behind a 200 KB frame. Each write is followed by
 * ``await writer.drain()`` and the transport's write-buffer high-water mark
 * is set to two chunks, so link backpressure is honoured instead of
 * buffering unboundedly.
 *
 * Inbound frames are discriminated on their first byte
 * (``codec.is_reserved_frame``): stream frames go straight to
 * ``_CarrierRPCProtocol._on_stream_frame`` and never touch the RPC codec,
 * admission or the executor.
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, NotImplementedError, RuntimeError, TimeoutError as PyTimeoutError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { PrivateAttr, define_private } from "../../../../../_compat/pydantic.js";
import { uuid4 } from "../../../../../_compat/uuid.js";
import * as rpc_protocol from "../../protocol.js";
import * as _codec from "./codec.js";
import { _CarrierRPCProtocol, _PEER_CONNECT, _mget } from "./base.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./loopthread.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.stream");

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * One peer connection: writer + prioritised outbound queues + writer task.
 *
 * All methods except ``close_threadsafe`` must be called on the carrier's
 * event loop thread. ``put_rpc`` / ``put_stream`` are marshalled there by
 * callers via ``call_soon_threadsafe``.
 */
export class _Link {
  /**
   * @param {_CarrierRPCProtocol} proto owning carrier
   * @param {any} writer object exposing ``write`` / ``drain`` / ``close``
   * @param {asyncio._Loop} loop
   */
  constructor(proto, writer, loop) {
    this.proto = proto;
    this.writer = writer;
    this.loop = loop;
    /** @type {Uint8Array[]} */
    this.rpc_q = [];
    /** @type {Array<[any, Uint8Array]>} */
    this.stream_q = [];
    this.stream_q_max = Math.max(1, Math.trunc(proto.channel_queue_size));
    this.wake = new asyncio.Event();
    this.closed = false;
    this.task = null;
    const transport = writer && writer.transport !== undefined ? writer.transport : null;
    if (transport !== null && transport !== undefined) {
      try {
        const high = 2 * Math.max(1, Math.trunc(proto.stream_chunk_bytes));
        transport.set_write_buffer_limits({ high, low: Math.trunc(high / 2) });
      } catch {
        /* transport without write-buffer limits */
      }
    }
  }

  /** Start the writer task (loop thread). */
  start() {
    if (this.task === null) this.task = this.loop.create_task(() => this._run(), { name: "link-writer" });
  }

  /** Queue one framed RPC/control payload (priority lane). */
  put_rpc(data) {
    if (this.closed) return;
    this.rpc_q.push(data);
    this.wake.set();
  }

  /** Queue one stream message; evicts the oldest when the queue is full. */
  put_stream(channel, payload) {
    if (this.closed) return;
    if (this.stream_q.length >= this.stream_q_max) {
      const [old_ch] = this.stream_q.shift();
      old_ch.tx_dropped += 1;
    }
    this.stream_q.push([channel, payload]);
    this.wake.set();
  }

  async _write(data) {
    this.writer.write(data);
    const drain = this.writer.drain;
    if (drain !== null && drain !== undefined) await this.writer.drain();
  }

  async _drain_rpc() {
    while (this.rpc_q.length && !this.closed) {
      await this._write(this.rpc_q.shift());
    }
  }

  async _run() {
    try {
      while (!this.closed) {
        if (!this.rpc_q.length && !this.stream_q.length) {
          this.wake.clear();
          await this.wake.wait();
          continue;
        }
        await this._drain_rpc();
        if (this.stream_q.length && !this.closed) {
          const [channel, payload] = this.stream_q.shift();
          if (channel.closed || channel.tx_lane_id === null || channel.tx_lane_id === undefined) continue;
          for (const chunk of this.proto._stream_chunks(channel, payload)) {
            await this._write(_codec.frame(chunk));
            await this._drain_rpc();
            if (this.closed) break;
          }
        }
      }
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError)) log.debug("link writer ended", { exc_info: e });
    } finally {
      this.closed = true;
    }
  }

  /** Wait (bounded) until the RPC queue has been written out. */
  async flush(timeout = 2.0) {
    const deadline = this.loop.time() + timeout;
    while (this.rpc_q.length && !this.closed && this.loop.time() < deadline) {
      await asyncio.sleep(0.005);
    }
  }

  /** Stop the writer task and close the writer (loop thread, idempotent). */
  close() {
    this.closed = true;
    this.wake.set();
    const task = this.task;
    if (task !== null && !task.done()) task.cancel();
    try {
      this.writer.close();
    } catch {
      /* already closed */
    }
  }

  /** Schedule ``close`` on the loop from any thread. */
  close_threadsafe() {
    try {
      this.loop.call_soon_threadsafe(() => this.close());
    } catch (e) {
      if (!(e instanceof RuntimeError)) throw e;
      this.closed = true;
    }
  }
}

/**
 * Carrier for reliable, ordered, duplex byte streams.
 *
 * All public lifecycle methods are idempotent. The carrier owns a dedicated
 * asyncio event loop on a daemon thread; transport I/O is scheduled there
 * via ``asyncio.run_coroutine_threadsafe`` so the rest of laila stays
 * synchronous. The per-peer handle stored in ``_connections`` is a
 * ``_Link``.
 */
export class _StreamRPCProtocol extends _CarrierRPCProtocol {
  static supports_channels = true;

  static {
    define_private(this, {
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _server: PrivateAttr({ default: null }),
    });
  }

  // ------------------------------------------------------------------
  // Subclass hooks
  // ------------------------------------------------------------------

  /**
   * Create and return a listening server.
   *
   * Subclasses must bind their transport and wire ``_handle_inbound_stream``
   * as the per-connection callback, recording any bound-address detail they
   * expose. Runs inside the carrier event loop.
   */
  async _serve() {
    throw new NotImplementedError();
  }

  /**
   * Open one outbound connection to *uri*; return its stream pair.
   * Runs inside the carrier event loop.
   * @returns {Promise<[asyncio.StreamReader, asyncio.StreamWriter]>}
   */
  async _open_connection(_uri) {
    throw new NotImplementedError();
  }

  /**
   * Hook called once a stream is established (inbound + outbound).
   *
   * Subclasses tune the socket here (e.g. TCP transports disable Nagle via
   * ``TCP_NODELAY`` for minimum small-frame latency). Default: no-op.
   */
  _on_stream_ready(_writer) {
    return null;
  }

  /** Close a server returned by ``_serve``. Default: ``close`` + wait. */
  async _close_server(server) {
    if (server === null || server === undefined) return;
    server.close();
    const wait_closed = server.wait_closed;
    if (wait_closed !== null && wait_closed !== undefined) {
      try {
        await asyncio.wait_for(server.wait_closed(), 2.0);
      } catch {
        /* best effort */
      }
    }
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Boot the event loop and start accepting connections (idempotent). */
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

  /** Bring up the server inside the loop, then signal *ready*. */
  async _async_start(ready) {
    this._server = await this._serve();
    ready.set();
  }

  /**
   * Tear down the server, every peer stream and lane, and the loop (idempotent).
   *
   * Order: say goodbye to every peer, unregister them (closing lanes --
   * waking relays -- and links, so the server's ``wait_closed()`` completes
   * promptly), close the server, cancel and await every remaining task, then
   * stop and **close** the loop.
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
    this._connections.clear();
    this._shutdown_lanes();
    await this._close_server(this._server);
    this._server = null;
    await cancel_pending_tasks();
  }

  _after_stop() {
    this._shutdown_lanes();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  // ------------------------------------------------------------------
  // Links
  // ------------------------------------------------------------------

  /** Wrap *writer* in a started ``_Link`` (loop thread). */
  _make_link(writer) {
    const link = new _Link(this, writer, this._event_loop);
    link.start();
    return link;
  }

  _on_loop_thread() {
    try {
      return asyncio.get_running_loop() === this._event_loop;
    } catch (e) {
      if (e instanceof RuntimeError) return false;
      throw e;
    }
  }

  /** Close lanes (base), then the link, then notify the hub. Idempotent. */
  _unregister_peer(peer_id) {
    const link = this._connections.get(peer_id) ?? null;
    super._unregister_peer(peer_id);
    if (link === null) return;
    if (this._on_loop_thread()) link.close();
    else link.close_threadsafe();
  }

  /** Queue ``peer.disconnect`` on the peer's link and wait for it to be written. */
  async _send_goodbye(peer_id) {
    const link = this._connections.get(peer_id) ?? null;
    if (link === null || link.closed) return;
    link.put_rpc(_codec.frame(this._encode(this._goodbye_message())));
    await link.flush(0.5);
  }

  // ------------------------------------------------------------------
  // Peering
  // ------------------------------------------------------------------

  /** Open an outbound stream to *uri* and complete the handshake. */
  connect(uri, secret) {
    this.start();
    const fut = asyncio.run_coroutine_threadsafe(() => this._connect_outbound(uri, secret), this._event_loop);
    return fut.result(Math.max(this.handshake_timeout * 3, 30.0));
  }

  /** Awaitable ``connect``. */
  async connect_async(uri, secret) {
    await this.start_async();
    const fut = asyncio.run_coroutine_threadsafe(() => this._connect_outbound(uri, secret), this._event_loop);
    return await asyncio.wait_for(fut, Math.max(this.handshake_timeout * 3, 30.0));
  }

  /** Client side of the ``peer.connect`` handshake. */
  async _connect_outbound(uri, secret) {
    const [reader, writer] = await this._open_connection(uri);
    this._on_stream_ready(writer);
    const link = this._make_link(writer);
    const policy_id = this._communication ? this._communication.policy_id : null;
    const req = rpc_protocol.make_request("peer.connect", { from_id: policy_id, secret, caps: this._local_caps() });
    link.put_rpc(_codec.frame(this._encode(req)));

    let raw;
    try {
      raw = await asyncio.wait_for(_codec.read_frame(reader), this.handshake_timeout);
    } catch (exc) {
      if (!(exc instanceof PyTimeoutError)) throw exc;
      link.close();
      const err = new ConnectionError("Peer handshake timed out.");
      err.__cause__ = exc;
      throw err;
    }
    if (raw === null) {
      link.close();
      throw new ConnectionError("Peer closed during handshake.");
    }
    if (_codec.is_reserved_frame(raw)) {
      link.close();
      throw new ConnectionError("Peer sent a stream frame before completing the handshake.");
    }

    const msg = this._decode(raw);
    if (_has(msg, "error")) {
      link.close();
      const error = _mget(msg, "error");
      throw new ConnectionError(`Peer rejected connection: ${_has(error, "message") ? _mget(error, "message") : error}`);
    }
    const result = _mget(msg, "result", {}) || {};
    const peer_id = _mget(result, "peer_id");
    if (peer_id === null || peer_id === undefined) {
      link.close();
      throw new ConnectionError("Peer response missing peer_id.");
    }

    this._set_peer_caps(peer_id, _mget(result, "caps"));
    this._register_peer(peer_id, link);
    asyncio.ensure_future(() => this._receive_loop(reader, link, peer_id));
    return peer_id;
  }

  /** Server side of the handshake, then the shared receive loop. */
  async _handle_inbound_stream(reader, writer) {
    let raw;
    try {
      raw = await asyncio.wait_for(_codec.read_frame(reader), this.handshake_timeout);
    } catch (exc) {
      if (!(exc instanceof PyTimeoutError)) throw exc;
      writer.close();
      return;
    }
    if (raw === null || _codec.is_reserved_frame(raw)) {
      // EOF, or a stream frame before the handshake: not a peer.
      writer.close();
      return;
    }

    this._on_stream_ready(writer);
    const link = this._make_link(writer);
    const msg = this._decode(raw);
    if (!rpc_protocol.is_request(msg) || _mget(msg, "method") !== _PEER_CONNECT) {
      const resp = rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_INVALID_REQUEST, "First message must be a peer.connect request.");
      link.put_rpc(_codec.frame(this._encode(resp)));
      await link.flush();
      link.close();
      return;
    }

    const params = _mget(msg, "params", {}) ?? {};
    const peer_id = _mget(params, "from_id");
    if (_mget(params, "secret") !== this.peer_secret_key) {
      const resp = rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key.");
      link.put_rpc(_codec.frame(this._encode(resp)));
      await link.flush();
      link.close();
      return;
    }

    const policy_id = this._communication ? this._communication.policy_id : null;
    const resp = rpc_protocol.make_result(_mget(msg, "id"), { peer_id: policy_id, caps: this._local_caps() });
    link.put_rpc(_codec.frame(this._encode(resp)));

    this._set_peer_caps(peer_id, _mget(params, "caps"));
    this._register_peer(peer_id, link);
    await this._receive_loop(reader, link, peer_id);
  }

  /** Decode frames and route requests/responses/stream chunks until EOF. */
  async _receive_loop(reader, link, peer_id) {
    const reply = this._make_reply(link);
    try {
      for (;;) {
        const raw = await _codec.read_frame(reader);
        if (raw === null) break;
        if (_codec.is_reserved_frame(raw)) {
          this._on_stream_frame(peer_id, raw);
          continue;
        }
        const msg = this._decode(raw);
        if (rpc_protocol.is_request(msg)) {
          // peer is dropping us gracefully: end this stream now
          if (this._is_goodbye(msg)) break;
          this._handle_request_frame(msg, reply, peer_id);
        } else if (rpc_protocol.is_response(msg)) {
          this._complete_pending(msg);
        }
      }
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError) && !(e instanceof ConnectionError)) {
        log.debug("Stream receive loop for peer %s ended", peer_id, { exc_info: e });
      }
    } finally {
      this._unregister_peer(peer_id);
    }
  }

  /**
   * Build a thread-safe ``reply(resp)`` that frames + queues on the link.
   *
   * Used by ``_handle_request_frame``; safe to call both inline on the I/O
   * loop (ping / control fast-path) and from an inbound worker thread.
   */
  _make_reply(link) {
    return (resp) => {
      const data = _codec.frame(this._encode(resp));
      try {
        this._loop_call(() => link.put_rpc(data));
      } catch (e) {
        if (!(e instanceof ConnectionError)) throw e;
      }
    };
  }

  // ------------------------------------------------------------------
  // Outbound RPC / stream
  // ------------------------------------------------------------------

  /** Frame *msg* and queue it on the peer's link from any thread. */
  _queue_rpc(peer_id, msg) {
    const link = this._connections.get(peer_id) ?? null;
    if (link === null || link.closed) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const data = _codec.frame(this._encode(msg));
    this._loop_call(() => link.put_rpc(data));
  }

  _send_once_prepare(peer_id, path, args, kwargs) {
    if (!this._connections.has(peer_id)) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const [request_id, slot] = this._register_pending();
    const req = this._make_rpc_request(path, args, kwargs, request_id);
    try {
      this._queue_rpc(peer_id, req);
    } catch (e) {
      if (e instanceof ConnectionError) this._pending_rpcs.delete(request_id);
      throw e;
    }
    return [request_id, slot];
  }

  /** Send one ``rpc.call`` to *peer_id* and block for the response. */
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
    this._queue_rpc(peer_id, this._make_rpc_request(path, [], kwargs, String(uuid4())));
  }

  /** Marshal one stream message onto the peer's link (user thread). */
  _stream_enqueue(channel, payload) {
    const link = this._connections.get(channel.peer_id) ?? null;
    if (link === null || link.closed) throw new ConnectionError(`No connection to peer ${channel.peer_id}`);
    this._loop_call(() => link.put_stream(channel, payload));
  }
}

register("laila.policy.central.communication.protocols._carriers.stream", { _Link, _StreamRPCProtocol });
