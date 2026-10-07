/**
 * WebSocket connection management for the TCP/IP protocol.
 *
 * This module factors the websocket plumbing out of
 * ``_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`` so the protocol class itself
 * can stay focused on lifecycle and the public RPC API. Three phases are
 * implemented here:
 *
 * 1. **Server bring-up** (``start_server``) -- bind a websocket server to
 *    ``proto.host:proto.port`` and stash the handle on *proto*. The server's
 *    per-connection callback is ``_handle_inbound``.
 * 2. **Handshake** (``_handle_inbound`` / ``connect_outbound``) -- the first
 *    frame on a freshly opened socket must be a ``peer.connect`` JSON-RPC
 *    request carrying the originating policy's ``global_id`` and the shared
 *    secret. On success both sides register the peer with their local
 *    protocol/communication and enter the shared receive loop.
 * 3. **Receive loop** (``_receive_loop``) -- one-per-connection task that
 *    decodes inbound frames and routes them to either the request handler
 *    (``_handle_rpc_request``) or the response handler
 *    (``_handle_rpc_response``).
 *
 * All functions are async and are expected to run inside the protocol
 * instance's dedicated event loop (the daemon thread spun up by
 * ``_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL.start``).
 *
 * The websocket implementation is the ``ws`` package (Python uses
 * ``websockets``); ``_WebSocket`` adapts it to the small
 * ``recv`` / ``send`` / ``close`` / async-iterator surface this module uses.
 */
import { createRequire } from "node:module";

import * as asyncio from "../../../_compat/asyncio.js";
import { ConnectionError, TimeoutError as PyTimeoutError } from "../../../_compat/errors.js";
import { getLogger } from "../../../_compat/logging.js";
import { hop } from "../../../_compat/pump.js";
import * as protocol from "./protocol.js";

const log = getLogger("laila.policy.central.communication.connection");
const _require = createRequire(import.meta.url);

/** Reserved dotted path of the liveness ping (same as the carriers'). */
export const _COMM_PING_PATH = Object.freeze(["__comm_ping__"]);

/** ``websockets.ConnectionClosed`` */
export class ConnectionClosed extends ConnectionError {
  constructor(code = 1006, reason = "") {
    super(`received ${code} (${reason || "no reason"})`);
    this.code = code;
    this.reason = reason;
  }
}

function _ws_module() {
  try {
    return _require("ws");
  } catch (exc) {
    const err = new ConnectionError("The 'tcpip' transport requires the 'ws' package, which is not installed. Install it with `npm install ws`.");
    err.__cause__ = exc;
    throw err;
  }
}

/**
 * Adapter giving a ``ws`` socket the ``websockets`` connection surface:
 * ``recv()``, ``send()``, ``close()``, ``wait_closed()`` and async iteration.
 */
export class _WebSocket {
  constructor(sock) {
    this._sock = sock;
    /** @type {Array<string|Buffer>} */
    this._queue = [];
    /** @type {Array<{resolve: Function, reject: Function}>} */
    this._waiters = [];
    this._closed = null;
    this._closed_event = new asyncio.Event();
    sock.on("message", (data, isBinary) => {
      const msg = isBinary ? Buffer.from(data) : Buffer.from(data).toString("utf8");
      const w = this._waiters.shift();
      if (w) w.resolve(msg);
      else this._queue.push(msg);
    });
    const on_closed = (code, reason) => {
      if (this._closed !== null) return;
      this._closed = new ConnectionClosed(typeof code === "number" ? code : 1006, reason ? String(reason) : "");
      this._closed_event.set();
      const ws = this._waiters;
      this._waiters = [];
      for (const w of ws) w.reject(this._closed);
    };
    sock.on("close", on_closed);
    sock.on("error", (e) => on_closed(1006, e && e.message ? e.message : ""));
  }

  /** ``true`` once the socket closed (either side). */
  get closed() {
    return this._closed !== null;
  }

  /** ``ws.remote_address`` -> ``[host, port]`` */
  get remote_address() {
    const s = this._sock._socket;
    return s ? [s.remoteAddress, s.remotePort] : null;
  }

  /** ``ws.local_address`` -> ``[host, port]`` */
  get local_address() {
    const s = this._sock._socket;
    return s ? [s.localAddress, s.localPort] : null;
  }

  /** Next message, or ``ConnectionClosed`` once the socket is gone. */
  recv() {
    if (this._queue.length) return Promise.resolve(this._queue.shift());
    if (this._closed !== null) return Promise.reject(this._closed);
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }));
  }

  /** Send one text (string) or binary (bytes) frame. */
  send(data) {
    if (this._closed !== null) return Promise.reject(this._closed);
    return new Promise((resolve, reject) => {
      try {
        this._sock.send(data, { binary: typeof data !== "string" }, (err) => (err ? reject(err) : resolve()));
      } catch (e) {
        reject(e);
      }
    });
  }

  /** Close handshake; resolves once the socket is closed. */
  async close(code = 1000, reason = "") {
    if (this._closed === null) {
      try {
        this._sock.close(code, reason);
      } catch {
        /* already closing */
      }
    }
    try {
      await asyncio.wait_for(this._closed_event.wait(), 2.0);
    } catch {
      try {
        this._sock.terminate();
      } catch {
        /* already gone */
      }
    }
  }

  async wait_closed() {
    await this._closed_event.wait();
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      let msg;
      try {
        msg = await this.recv();
      } catch (e) {
        if (e instanceof ConnectionClosed) return;
        throw e;
      }
      yield msg;
    }
  }
}

/** ``websockets.asyncio.server.Server`` surface over ``ws.WebSocketServer``. */
export class _Server {
  constructor(wss, http_server) {
    this._wss = wss;
    this._http = http_server;
    this._closed = new asyncio.Event();
    /** @type {Set<_WebSocket>} */
    this.connections = new Set();
  }
  /** ``server.sockets[0].getsockname()`` -> ``[host, port]`` */
  get sockets() {
    const srv = this._http;
    return [
      {
        getsockname: () => {
          const a = srv.address();
          return a && typeof a === "object" ? [a.address, a.port] : [null, 0];
        },
      },
    ];
  }
  close() {
    for (const c of this.connections) {
      try {
        c._sock.close(1001, "server shutdown");
      } catch {
        /* ignore */
      }
    }
    this._wss.close(() => {
      this._http.close(() => this._closed.set());
      // ``http.Server.close`` waits for keep-alive sockets; force them.
      if (typeof this._http.closeAllConnections === "function") this._http.closeAllConnections();
    });
  }
  async wait_closed() {
    await this._closed.wait();
  }
}

/**
 * ``websockets.asyncio.server.serve(handler, host, port)``.
 * @returns {Promise<_Server>}
 */
export async function serve(handler, host, port) {
  const { WebSocketServer } = _ws_module();
  const http = await import("node:http");
  const http_server = http.createServer((_req, res) => {
    res.writeHead(426, { "Content-Type": "text/plain" });
    res.end("Upgrade Required");
  });
  const wss = new WebSocketServer({ server: http_server });
  const server = new _Server(wss, http_server);
  wss.on("connection", (sock) => {
    const ws = new _WebSocket(sock);
    server.connections.add(ws);
    ws._closed_event.wait().then(() => server.connections.delete(ws));
    asyncio.ensure_future(() => Promise.resolve(handler(ws)));
  });
  await new Promise((resolve, reject) => {
    const on_err = (e) => reject(e);
    http_server.once("error", on_err);
    http_server.listen(port, host, () => {
      http_server.off("error", on_err);
      resolve();
    });
  });
  return server;
}

/**
 * ``websockets.asyncio.client.connect(uri)``.
 * @returns {Promise<_WebSocket>}
 */
export async function connect(uri) {
  const { WebSocket } = _ws_module();
  const sock = new WebSocket(uri);
  await new Promise((resolve, reject) => {
    sock.once("open", resolve);
    sock.once("error", (e) => {
      const err = new ConnectionError(`WebSocket connect to ${uri} failed: ${e && e.message ? e.message : e}`);
      err.__cause__ = e;
      reject(err);
    });
  });
  return new _WebSocket(sock);
}

function _mget(msg, key, dflt = null) {
  if (msg === null || msg === undefined || typeof msg !== "object") return dflt;
  if (msg instanceof Map) return msg.has(key) ? msg.get(key) : dflt;
  return Object.prototype.hasOwnProperty.call(msg, key) ? msg[key] : dflt;
}
function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}
function _path_eq(a, b) {
  return Array.isArray(a) && a.length === b.length && a.every((x, i) => x === b[i]);
}

/**
 * Start the WebSocket listener and store the server handle on *proto*.
 * @param {import("./protocols/tcpip.js")._LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL} proto
 */
export async function start_server(proto) {
  const server = await serve((ws) => _handle_inbound(proto, ws), proto.host, proto.port);
  const bound_port = server.sockets[0].getsockname()[1];
  proto._bound_port = bound_port;
  proto._server = server;
  log.info("Communication server listening on %s:%s", proto.host, proto.bound_port);
}

/**
 * Handle a freshly accepted inbound WebSocket connection.
 *
 * Expects the first message to be a ``peer.connect`` JSON-RPC request. On
 * success, registers the peer bidirectionally and enters the shared receive
 * loop.
 */
export async function _handle_inbound(proto, ws) {
  let raw;
  try {
    raw = await asyncio.wait_for(ws.recv(), 10.0);
  } catch (e) {
    if (e instanceof PyTimeoutError || e instanceof ConnectionClosed) return;
    throw e;
  }

  const msg = protocol.decode(raw);

  if (!protocol.is_request(msg) || _mget(msg, "method") !== "peer.connect") {
    const resp = protocol.make_error(_mget(msg, "id"), protocol.ERR_INVALID_REQUEST, "First message must be a peer.connect request.");
    await ws.send(protocol.encode(resp));
    await ws.close();
    return;
  }

  const params = _mget(msg, "params", {}) ?? {};
  const peer_id = _mget(params, "from_id");
  const secret = _mget(params, "secret");

  if (secret !== proto.peer_secret_key) {
    const resp = protocol.make_error(msg.id, protocol.ERR_AUTH_FAILED, "Invalid peer secret key.");
    await ws.send(protocol.encode(resp));
    await ws.close();
    return;
  }

  const policy_id = proto._communication ? proto._communication.policy_id : null;
  const resp = protocol.make_result(msg.id, { peer_id: policy_id });
  await ws.send(protocol.encode(resp));

  proto._register_peer(peer_id, ws);
  log.info("Accepted inbound peer %s", peer_id);

  await _receive_loop(proto, ws, peer_id);
}

/**
 * Initiate an outbound peering connection.
 *
 * Connects to *uri*, performs the ``peer.connect`` handshake, registers the
 * peer on both sides, and starts the shared receive loop.
 *
 * @returns {Promise<string>} The remote policy's ``global_id``.
 * @throws {ConnectionError} If the handshake is rejected or times out.
 */
export async function connect_outbound(proto, uri, secret) {
  const ws = await connect(uri);

  const policy_id = proto._communication ? proto._communication.policy_id : null;
  const req = protocol.make_request("peer.connect", { from_id: policy_id, secret });
  await ws.send(protocol.encode(req));

  let raw;
  try {
    raw = await asyncio.wait_for(ws.recv(), 10.0);
  } catch (exc) {
    if (!(exc instanceof PyTimeoutError) && !(exc instanceof ConnectionClosed)) throw exc;
    await ws.close();
    const err = new ConnectionError("Peer handshake timed out or connection lost.");
    err.__cause__ = exc;
    throw err;
  }

  const msg = protocol.decode(raw);

  if (_has(msg, "error")) {
    const err = msg.error;
    await ws.close();
    throw new ConnectionError(`Peer rejected connection: ${_has(err, "message") ? err.message : err}`);
  }

  const peer_id = _mget(_mget(msg, "result", {}) ?? {}, "peer_id");
  if (peer_id === null || peer_id === undefined) {
    await ws.close();
    throw new ConnectionError("Peer response missing peer_id.");
  }

  proto._register_peer(peer_id, ws);
  log.info("Connected to outbound peer %s at %s", peer_id, uri);

  asyncio.ensure_future(() => _receive_loop(proto, ws, peer_id));

  return peer_id;
}

/**
 * Read messages from *ws* and dispatch requests / responses.
 *
 * This loop runs identically on both the initiator and acceptor side of a
 * peered connection.
 */
export async function _receive_loop(proto, ws, peer_id) {
  try {
    for await (const raw of ws) {
      const msg = protocol.decode(raw);

      if (protocol.is_request(msg)) {
        // graceful goodbye from the peer: end this connection
        if (_mget(msg, "method") === "peer.disconnect") break;
        await _handle_rpc_request(proto, ws, msg);
      } else if (protocol.is_response(msg)) {
        _handle_rpc_response(proto, msg);
      } else {
        log.warning("Unrecognised message from %s: %s", peer_id, String(raw).slice(0, 200));
      }
    }
  } catch (e) {
    if (!(e instanceof ConnectionClosed)) throw e;
    log.info("Connection to peer %s closed.", peer_id);
  } finally {
    proto._unregister_peer(peer_id);
  }
}

/** Execute an inbound ``rpc.call`` and send the result back. */
export async function _handle_rpc_request(proto, ws, msg) {
  const request_id = _mget(msg, "id");
  const method = _mget(msg, "method");

  if (method !== "rpc.call") {
    const resp = protocol.make_error(request_id, protocol.ERR_METHOD_NOT_FOUND, `Unknown method: ${method}`);
    await ws.send(protocol.encode(resp));
    return;
  }

  const params = _mget(msg, "params", {}) ?? {};
  const path = _mget(params, "path", []) ?? [];
  const args = _mget(params, "args", []) ?? [];
  const kwargs = _mget(params, "kwargs", {}) ?? {};

  // Liveness control frame: answered here, never reaches the policy.
  if (_path_eq(path, _COMM_PING_PATH)) {
    await ws.send(protocol.encode(protocol.make_result(request_id, "pong")));
    return;
  }

  // Python runs ``_execute_rpc`` synchronously on the protocol's loop
  // *thread*, where a handler may block (``Future.wait()``). Here the
  // handler is hopped onto a fresh macrotask so a nested blocking wait can
  // pump the loop instead of deadlocking inside this coroutine's microtask.
  // The result is boxed so a laila ``Future`` (a thenable) is handed back
  // *as-is* for the codec to tag ``__laila_future__`` -- resolving a Promise
  // with it would adopt it and ship the awaited entry instead.
  let resp;
  try {
    const { value: result } = await new Promise((resolve, reject) =>
      hop(() => {
        try {
          const r = proto._communication._execute_rpc(path, args, kwargs);
          if (r instanceof Promise) r.then((v) => resolve({ value: v }), reject);
          else resolve({ value: r });
        } catch (e) {
          reject(e);
        }
      }),
    );
    resp = protocol.make_result(request_id, result);
  } catch (exc) {
    const name = exc && exc.constructor ? exc.constructor.name : "Error";
    resp = protocol.make_error(request_id, protocol.ERR_EXECUTION, `${name}: ${exc && exc.message !== undefined ? exc.message : exc}`);
  }

  await ws.send(protocol.encode(resp));
}

/** Resolve a pending outbound RPC call with the received response. */
export function _handle_rpc_response(proto, msg) {
  const request_id = _mget(msg, "id");
  if (request_id === null || request_id === undefined) return;

  const pending = proto._pending_rpcs.get(request_id);
  if (pending === undefined) {
    log.warning("Received response for unknown request %s", request_id);
    return;
  }

  if (_has(msg, "error")) pending.error = msg.error;
  else pending.result = _mget(msg, "result");

  pending.event.set();
}
