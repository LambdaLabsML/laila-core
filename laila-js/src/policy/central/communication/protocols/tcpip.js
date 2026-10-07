/**
 * TCP/IP (WebSocket) communication protocol implementation.
 *
 * Concrete ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` that uses the ``ws``
 * library (Python: ``websockets``) to expose a long-lived bidirectional
 * channel between two policies. The on-the-wire RPC envelope is defined in
 * ``../protocol.js``, the actual socket plumbing lives in
 * ``../connection.js``.
 *
 * Architecturally the protocol owns:
 *
 * - A dedicated background asyncio event loop running on a daemon thread
 *   (``_event_loop`` / ``_loop_thread``). All websocket I/O is scheduled
 *   there so the rest of laila does not have to be asyncio-aware.
 * - A websocket *server* (``_server``) accepting inbound peerings.
 * - A dict of *connections* (``_connections``) keyed by remote-policy
 *   ``global_id``, each holding the live websocket for that peer.
 * - A *pending-RPC table* (``_pending_rpcs``) keyed by request id. Each
 *   outbound ``send_rpc`` registers a slot here, waits on a
 *   ``threading.Event``, and is woken up by the inbound dispatcher when the
 *   matching response arrives.
 */
import * as asyncio from "../../../../_compat/asyncio.js";
import { ConnectionError, RuntimeError, TimeoutError as PyTimeoutError } from "../../../../_compat/errors.js";
import { register } from "../../../../_compat/lazy.js";
import { getLogger } from "../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { Event } from "../../../../_compat/threading.js";
import { uuid4 } from "../../../../_compat/uuid.js";
import * as rpc_protocol from "../protocol.js";
import * as connection from "../connection.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./_carriers/loopthread.js";
import { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, register_comm_protocol } from "./base.js";

const log = getLogger("laila.policy.central.communication.protocols.tcpip");

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * WebSocket-based peer-to-peer communication protocol.
 *
 * Implements the ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` contract over
 * ``ws://`` / ``wss://``. RPC frames are JSON-encoded by ``../protocol.js``;
 * the connection state machine (handshake, message dispatch, disconnect)
 * lives in ``../connection.js``.
 *
 * Fields
 * ------
 * host : str, default ``"0.0.0.0"``
 *     Bind address for the WebSocket server. ``"0.0.0.0"`` listens on every
 *     interface; pass a specific IP to restrict reachability.
 * port : int, default ``0``
 *     TCP port for the WebSocket server. ``0`` lets the OS pick a free port;
 *     the chosen port is then exposed via ``bound_port``.
 * peer_secret_key : str
 *     Shared secret that remote peers must present during the handshake.
 *     Defaults to a fresh UUID4 hex if not set, so each instance gets a
 *     unique secret out of the box.
 *
 * The protocol auto-bootstraps its own background event loop the first time
 * ``start`` is called, and tears it down on ``stop``. Clients of this class
 * never see the loop directly -- they call ``send_rpc`` / ``add_peer``
 * synchronously and the loop is hidden behind
 * ``asyncio.run_coroutine_threadsafe``.
 */
export class _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
  static protocol_name = "tcpip";
  /**
   * WebSocket-focused tokens. Raw TCP/UDP/TLS have dedicated transports
   * under ``protocols/ip_app/`` that own the ``tcp`` / ``udp`` / ``tls``
   * tokens, so ``tcpip`` claims only the WebSocket family to keep the token
   * map globally disjoint.
   */
  static _TOKEN_ALIASES = Object.freeze(new Set(["tcpip", "ws", "wss", "websocket"]));

  static {
    define_fields(this, {
      host: ["str", Field({ default: "0.0.0.0" })],
      port: ["int", Field({ default: 0 })],
      peer_secret_key: ["str", Field({ default_factory: () => uuid4().hex })],
      // Seconds a blocking ``send_rpc`` waits for the response.
      rpc_timeout: ["float", Field({ default: 60.0 })],
      // Bound on a liveness ``ping`` round trip.
      ping_timeout: ["float", Field({ default: 5.0 })],
    });
    define_private(this, {
      _server: PrivateAttr({ default: null }),
      _connections: PrivateAttr({ default_factory: () => new Map() }),
      _pending_rpcs: PrivateAttr({ default_factory: () => new Map() }),
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _started: PrivateAttr({ default: false }),
      _bound_port: PrivateAttr({ default: null }),
    });
  }

  /**
   * Port the server is currently listening on.
   *
   * After ``start``, this returns the actual OS-assigned port -- useful when
   * ``port=0`` was requested. Before ``start`` it falls back to the
   * configured ``port`` (which may still be ``0``).
   */
  get bound_port() {
    return this._bound_port !== null && this._bound_port !== undefined ? this._bound_port : this.port;
  }

  // ------------------------------------------------------------------
  // URI routing
  // ------------------------------------------------------------------

  /** Accept ``"tcpip"`` plus common aliases (``ws``, ``wss``, ``websocket``). */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``ws://`` and ``wss://`` URIs for this protocol. */
  static can_handle_uri(uri) {
    return uri.startsWith("ws://") || uri.startsWith("wss://");
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /**
   * Boot the dedicated event loop and start accepting WebSocket connections.
   *
   * Idempotent: a second call is a no-op once ``_started`` is set. Blocks
   * the caller until the server is bound and ready (or ten seconds elapse),
   * so by the time this returns ``bound_port`` is meaningful.
   */
  start() {
    if (this._started) return;
    start_loop_thread(this, (ready) => this._async_start(ready), { ready_timeout: 10.0 });
    this._after_start();
  }

  /** Awaitable ``start``. */
  async start_async() {
    if (this._started) return;
    await start_loop_thread_async(this, (ready) => this._async_start(ready), { ready_timeout: 10.0 });
    this._after_start();
  }

  _after_start() {
    this._started = true;
    const policy_id = this._communication ? this._communication.policy_id : null;
    log.info("TCP/IP protocol started for policy %s on port %s", policy_id, this.bound_port);
  }

  /**
   * Boot the server inside the protocol's event loop and signal *ready*.
   *
   * Delegates to ``connection.start_server``, which performs the actual
   * ``serve`` call and stashes the resulting server handle on
   * ``this._server``. The *ready* event lets the synchronous ``start``
   * caller block until the listener is actually bound.
   */
  async _async_start(ready) {
    await connection.start_server(this);
    ready.set();
  }

  /**
   * Tear down the server, close every peer socket, and stop the loop.
   *
   * Idempotent: returns immediately when ``_started`` is false. Closes every
   * peer socket (a WebSocket close frame is the graceful goodbye; the
   * remote's receive loop ends and it unregisters us), closes the server,
   * cancels *and awaits* every outstanding task, joins the loop thread and
   * closes the loop, then clears the pending-RPC table so a subsequent
   * ``start`` brings the protocol back up cleanly.
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
    const close_tasks = [];
    for (const ws of [...this._connections.values()]) close_tasks.push(asyncio.ensure_future(() => ws.close()));
    if (close_tasks.length) await asyncio.gather(...close_tasks, { return_exceptions: true });
    for (const peer_id of [...this._connections.keys()]) this._unregister_peer(peer_id);

    if (this._server !== null && this._server !== undefined) {
      this._server.close();
      try {
        await asyncio.wait_for(this._server.wait_closed(), 2.0);
      } catch {
        /* best effort */
      }
      this._server = null;
    }

    await cancel_pending_tasks();
  }

  _after_stop() {
    this._pending_rpcs.clear();
    this._bound_port = null;
    this._started = false;
    const policy_id = this._communication ? this._communication.policy_id : null;
    log.info("TCP/IP protocol stopped for policy %s", policy_id);
  }

  // ------------------------------------------------------------------
  // Peer management
  // ------------------------------------------------------------------

  /**
   * Open an outbound WebSocket connection to *uri* and complete the handshake.
   *
   * Auto-starts the protocol if needed. The actual handshake is run inside
   * the protocol's event loop via ``connection.connect_outbound``; this
   * function blocks the caller for up to thirty seconds for the handshake
   * to finish.
   * @returns {string} The remote policy's ``global_id``.
   */
  connect(uri, secret) {
    this.start();
    const future = asyncio.run_coroutine_threadsafe(() => connection.connect_outbound(this, uri, secret), this._event_loop);
    return future.result(30.0);
  }

  /** Awaitable ``connect``. */
  async connect_async(uri, secret) {
    await this.start_async();
    const future = asyncio.run_coroutine_threadsafe(() => connection.connect_outbound(this, uri, secret), this._event_loop);
    return await asyncio.wait_for(future, 30.0);
  }

  /**
   * Record an established WebSocket and notify the communication layer.
   *
   * Called by ``connection`` after a successful handshake (in either
   * direction). The communication layer creates the corresponding
   * ``RemotePolicyProxy`` so user code can immediately reach the new peer.
   */
  _register_peer(peer_id, ws) {
    this._connections.set(peer_id, ws);
    if (this._communication !== null && this._communication !== undefined) this._communication._register_peer(peer_id);
  }

  /**
   * Drop a peer's WebSocket and notify the communication layer.
   *
   * Called when the connection closes (either side). Idempotent: if the
   * peer is not in the table, this is a no-op.
   */
  _unregister_peer(peer_id) {
    this._connections.delete(peer_id);
    if (this._communication !== null && this._communication !== undefined) this._communication._unregister_peer(peer_id);
  }

  /** Return ``true`` if a live WebSocket to *peer_id* is currently held. */
  has_peer(peer_id) {
    return this._connections.has(peer_id);
  }

  /**
   * Close the WebSocket to *peer_id* and drop it. Idempotent.
   *
   * The close handshake is the graceful goodbye: the remote's receive loop
   * ends with ``ConnectionClosed`` and unregisters us. Bounded so a dead
   * peer cannot stall the caller.
   */
  disconnect(peer_id) {
    const ws = this._connections.get(peer_id);
    if (ws === undefined) return;
    const loop = this._event_loop;
    if (loop !== null && loop !== undefined && !loop.is_closed() && loop.is_running()) {
      try {
        asyncio.run_coroutine_threadsafe(() => ws.close(), loop).result(2.0);
      } catch (e) {
        log.debug("closing WebSocket to %s failed", peer_id, { exc_info: e });
      }
    }
    this._unregister_peer(peer_id);
  }

  /** Awaitable ``disconnect``. */
  async disconnect_async(peer_id) {
    const ws = this._connections.get(peer_id);
    if (ws === undefined) return;
    const loop = this._event_loop;
    if (loop !== null && loop !== undefined && !loop.is_closed() && loop.is_running()) {
      try {
        await asyncio.wait_for(asyncio.run_coroutine_threadsafe(() => ws.close(), loop), 2.0);
      } catch (e) {
        log.debug("closing WebSocket to %s failed", peer_id, { exc_info: e });
      }
    }
    this._unregister_peer(peer_id);
  }

  // ------------------------------------------------------------------
  // RPC (outbound)
  // ------------------------------------------------------------------

  /**
   * Round-trip the reserved ``__comm_ping__`` frame to *peer_id*.
   *
   * Answered by the remote's ``connection`` dispatcher before it touches the
   * policy. ``false`` on any transport error or timeout, so the liveness
   * loop drops only peers that really stopped answering.
   */
  ping(peer_id, timeout = null) {
    if (!this.has_peer(peer_id)) return false;
    try {
      return this._send(peer_id, ["__comm_ping__"], [], {}, { timeout: timeout !== null && timeout !== undefined ? timeout : this.ping_timeout }) === "pong";
    } catch {
      return false;
    }
  }

  /** Awaitable ``ping``. */
  async ping_async(peer_id, timeout = null) {
    if (!this.has_peer(peer_id)) return false;
    try {
      return (await this._send_async(peer_id, ["__comm_ping__"], [], {}, { timeout: timeout !== null && timeout !== undefined ? timeout : this.ping_timeout })) === "pong";
    } catch {
      return false;
    }
  }

  /**
   * Send an RPC call over the WebSocket to *peer_id* and block for the response.
   *
   * Allocates a fresh request id, registers a pending-RPC slot keyed by that
   * id, dispatches the encoded frame from the protocol's event loop, and
   * waits on a ``threading.Event`` for up to ``rpc_timeout`` seconds. The
   * inbound dispatcher in ``connection`` flips the event when the matching
   * response arrives.
   *
   * @throws {ConnectionError} If no live connection to *peer_id* is held.
   * @throws {TimeoutError} If no response arrived within ``rpc_timeout``.
   * @throws {RuntimeError} If the remote returned an error envelope.
   */
  send_rpc(peer_id, path, args, kwargs) {
    return this._send(peer_id, path, args, kwargs, { timeout: this.rpc_timeout });
  }

  /** Awaitable ``send_rpc``. */
  async send_rpc_async(peer_id, path, args, kwargs) {
    return this._send_async(peer_id, path, args, kwargs, { timeout: this.rpc_timeout });
  }

  _send_prepare(peer_id, path, args, kwargs) {
    const ws = this._connections.get(peer_id);
    if (ws === undefined) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const loop = this._event_loop;
    if (loop === null || loop === undefined || loop.is_closed()) throw new ConnectionError("TCP/IP transport is shut down.");

    const request_id = String(uuid4());
    const event = new Event();
    const slot = { event };
    this._pending_rpcs.set(request_id, slot);

    const req = rpc_protocol.make_request("rpc.call", { path, args: [...args], kwargs: { ...kwargs } }, request_id);

    try {
      asyncio.run_coroutine_threadsafe(() => ws.send(rpc_protocol.encode(req)), loop);
    } catch (exc) {
      if (!(exc instanceof RuntimeError)) throw exc;
      this._pending_rpcs.delete(request_id);
      const err = new ConnectionError("TCP/IP transport is shut down.");
      err.__cause__ = exc;
      throw err;
    }
    return [request_id, slot];
  }

  _send_finish(peer_id, request_id, slot, completed, timeout) {
    this._pending_rpcs.delete(request_id);
    if (!completed) throw new PyTimeoutError(`RPC to peer ${peer_id} timed out after ${timeout}s.`);

    if (_has(slot, "error")) {
      const err = slot.error;
      throw new RuntimeError(`Remote RPC error: ${_has(err, "message") ? err.message : err}`);
    }
    return slot.result ?? null;
  }

  _send(peer_id, path, args, kwargs, { timeout }) {
    const [request_id, slot] = this._send_prepare(peer_id, path, args, kwargs);
    const completed = slot.event.wait(timeout);
    return this._send_finish(peer_id, request_id, slot, completed, timeout);
  }

  async _send_async(peer_id, path, args, kwargs, { timeout }) {
    const [request_id, slot] = this._send_prepare(peer_id, path, args, kwargs);
    const completed = await slot.event.wait_async(timeout);
    return this._send_finish(peer_id, request_id, slot, completed, timeout);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.tcpip", { _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL });
