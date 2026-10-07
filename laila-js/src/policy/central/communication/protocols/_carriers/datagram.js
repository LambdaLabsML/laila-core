/**
 * Unreliable / segmented datagram RPC carrier.
 *
 * ``_DatagramRPCProtocol`` turns a lossy, MTU-limited packet link (UDP,
 * CoAP, LoRa, ESP-NOW, CAN, ...) into the same reliable JSON-RPC channel the
 * stream carrier provides. On top of a raw ``sendto(addr, bytes)`` /
 * inbound-packet pair it adds:
 *
 * - **Fragmentation / reassembly** -- each encoded JSON-RPC message is split
 *   into ``mtu``-sized fragments tagged with a 16-byte message id, a fragment
 *   index and a fragment count, and reassembled on arrival.
 * - **Per-fragment ack + retransmit** -- every received fragment is
 *   acknowledged; unacked fragments are resent every ``ack_timeout`` up to
 *   ``max_retries`` times, so messages survive moderate loss.
 * - **Dedup** -- a recently-seen message-id set prevents a retransmitted
 *   message from being processed twice after an ack is lost.
 *
 * The reliable JSON-RPC layer (handshake, request/response correlation,
 * off-thread dispatch) is inherited from ``_CarrierRPCProtocol``.
 *
 * Concrete transports supply three coroutines/helpers:
 * ``_create_datagram_endpoint``, ``_resolve_peer_addr`` and (optionally)
 * ``_close_endpoint``.
 *
 * Stream lanes over datagrams
 * ---------------------------
 * A stream message is **one datagram** carrying the standard lane header
 * (``[0x01][lane][seq][flags][data]``, ``START|END`` always set), sent with
 * ``sendto`` and nothing else: no ack, no retransmit, no dedup, no
 * fragmentation -- that machinery stays RPC-only. Lanes are therefore *lossy
 * and unordered by design*; the receiver's chunk-sequence check only makes a
 * lost datagram visible, it cannot recover it. A message larger than
 * ``mtu - 7`` raises ``ValueError`` on ``send()``. Inbound packets are
 * discriminated on their first byte before the fragment parser (the ``LDG``
 * magic starts with ``0x4C``, so there is no clash).
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, NotImplementedError, ValueError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { eq } from "../../../../../_compat/pytypes.js";
import { Struct } from "../../../../../_compat/struct.js";
import { Event } from "../../../../../_compat/threading.js";
import { uuid4 } from "../../../../../_compat/uuid.js";
import * as rpc_protocol from "../../protocol.js";
import * as _codec from "./codec.js";
import { _CarrierRPCProtocol, _PEER_CONNECT, _mget } from "./base.js";
import { cancel_pending_tasks, start_loop_thread, start_loop_thread_async, stop_loop_thread, stop_loop_thread_async } from "./loopthread.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.datagram");

export const _MAGIC = Buffer.from("LDG", "latin1");
export const _TYPE_DATA = 0;
export const _TYPE_ACK = 1;
/** magic(3) + type(1) + frag_index(2) + frag_count(2) */
export const _HEADER = new Struct(">3sBHH");
export const _MSG_ID_LEN = 16;
export const _PREFIX_LEN = _HEADER.size + _MSG_ID_LEN;

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

/**
 * Hashable key for a transport address (Python uses the tuple itself as a
 * dict key). Arrays (``[host, port]``) and strings map to a canonical string.
 */
export function _addr_key(addr) {
  if (addr === null || addr === undefined) return "";
  if (typeof addr === "string") return addr;
  if (Array.isArray(addr)) return addr.map((x) => (Array.isArray(x) ? `(${_addr_key(x)})` : String(x))).join("\u0000");
  if (Buffer.isBuffer(addr) || addr instanceof Uint8Array) return Buffer.from(addr).toString("hex");
  if (typeof addr === "object" && typeof addr.__hash__ === "function") return String(addr.__hash__());
  return JSON.stringify(addr);
}

/** Minimal ``asyncio.DatagramProtocol`` forwarding to a callback. */
export class _DatagramEndpoint extends asyncio.DatagramProtocol {
  /** @param {(addr: any, data: Buffer) => void} on_packet */
  constructor(on_packet) {
    super();
    this._on_packet = on_packet;
    this.transport = null;
  }
  connection_made(transport) {
    this.transport = transport;
  }
  datagram_received(data, addr) {
    this._on_packet(addr, data);
  }
  error_received(exc) {
    log.debug("Datagram error: %s", exc);
  }
}

/**
 * Carrier for unreliable, MTU-limited packet links.
 *
 * Fields
 * ------
 * mtu : int, default ``1200``
 *     Maximum payload bytes per fragment (link MTU minus headers).
 * ack_timeout : float, default ``0.5``
 *     Seconds between retransmits of unacked fragments.
 * max_retries : int, default ``5``
 *     How many times an unacked fragment is resent before giving up.
 */
export class _DatagramRPCProtocol extends _CarrierRPCProtocol {
  static supports_channels = true;

  static {
    define_fields(this, {
      mtu: ["int", Field({ default: 1200 })],
      ack_timeout: ["float", Field({ default: 0.5 })],
      max_retries: ["int", Field({ default: 5 })],
    });
    define_private(this, {
      _event_loop: PrivateAttr({ default: null }),
      _loop_thread: PrivateAttr({ default: null }),
      _transport: PrivateAttr({ default: null }),
      _endpoint: PrivateAttr({ default: null }),
      // (addr, msg_id) key -> {count, frags: Map<index, Buffer>}
      _reasm: PrivateAttr({ default_factory: () => new Map() }),
      _seen: PrivateAttr({ default: null }),
      _seen_order: PrivateAttr({ default_factory: () => [] }),
      // (addr, msg_id, index) key -> {addr, packet, tries}
      _unacked: PrivateAttr({ default_factory: () => new Map() }),
      _handshake_pending: PrivateAttr({ default_factory: () => new Map() }),
      _retransmit_task: PrivateAttr({ default: null }),
    });
  }

  // ------------------------------------------------------------------
  // Subclass hooks
  // ------------------------------------------------------------------

  /**
   * Create and return ``[transport, protocol]`` for the local socket.
   *
   * Subclasses typically call
   * ``loop.create_datagram_endpoint(() => new _DatagramEndpoint((a, d) => this._feed_packet(a, d)), ...)``.
   * Runs inside the carrier event loop.
   */
  async _create_datagram_endpoint() {
    throw new NotImplementedError();
  }

  /** Parse *uri* into the transport address used by ``sendto``. */
  async _resolve_peer_addr(_uri) {
    throw new NotImplementedError();
  }

  /** Close the datagram endpoint. Default: close the transport. */
  async _close_endpoint() {
    if (this._transport !== null && this._transport !== undefined) {
      try {
        this._transport.close();
      } catch {
        /* already closed */
      }
      this._transport = null;
    }
  }

  /** Send one raw datagram. Default uses the asyncio transport. */
  _sendto(addr, packet) {
    this._transport.sendto(packet, addr);
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Boot the event loop and bind the datagram endpoint (idempotent). */
  start() {
    if (this._started) return;
    this._seen = new Set();
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
    this._seen = new Set();
    this._ensure_executor();
    try {
      await start_loop_thread_async(this, (ready) => this._async_start(ready), { ready_timeout: Math.max(this.handshake_timeout, 10.0) });
    } catch (e) {
      this._shutdown_executor();
      throw e;
    }
    this._started = true;
  }

  /** Bind the endpoint and start the retransmit loop. */
  async _async_start(ready) {
    [this._transport, this._endpoint] = await this._create_datagram_endpoint();
    this._retransmit_task = asyncio.ensure_future(() => this._retransmit_loop());
    ready.set();
  }

  /**
   * Tear down the endpoint and loop (idempotent).
   *
   * Order: goodbye to every peer (a datagram peer has no EOF to notice, so
   * without it the remote only drops us on liveness timeout), unregister,
   * stop the retransmit timer, close the endpoint, cancel and await
   * remaining tasks, stop and close the loop.
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
    if (this._retransmit_task !== null && this._retransmit_task !== undefined) {
      this._retransmit_task.cancel();
      await asyncio.gather(this._retransmit_task, { return_exceptions: true });
      this._retransmit_task = null;
    }
    await this._close_endpoint();
    await cancel_pending_tasks();
  }

  _after_stop() {
    this._shutdown_lanes();
    this._reasm.clear();
    this._unacked.clear();
    this._handshake_pending.clear();
    this._pending_rpcs.clear();
    this._shutdown_executor();
    this._started = false;
  }

  // ------------------------------------------------------------------
  // Reliable message layer
  // ------------------------------------------------------------------

  /** Fragment *payload* and queue every fragment for reliable delivery. */
  _send_message(addr, payload) {
    const msg_id = uuid4().bytes;
    const mtu = Math.max(1, this.mtu);
    const fragments = [];
    for (let i = 0; i < payload.length; i += mtu) fragments.push(payload.subarray(i, i + mtu));
    if (fragments.length === 0) fragments.push(Buffer.alloc(0));
    const frag_count = fragments.length;
    const akey = _addr_key(addr);
    const mkey = msg_id.toString("hex");
    fragments.forEach((frag, index) => {
      const packet = Buffer.concat([_HEADER.pack(_MAGIC, _TYPE_DATA, index, frag_count), msg_id, frag]);
      this._unacked.set(`${akey}|${mkey}|${index}`, { addr, packet, tries: 0 });
      this._sendto(addr, packet);
    });
  }

  /** Resend unacked fragments every ``ack_timeout`` up to ``max_retries``. */
  async _retransmit_loop() {
    try {
      for (;;) {
        await asyncio.sleep(this.ack_timeout);
        for (const [key, slot] of [...this._unacked.entries()]) {
          if (slot.tries >= this.max_retries) {
            this._unacked.delete(key);
            continue;
          }
          slot.tries += 1;
          try {
            this._sendto(slot.addr, slot.packet);
          } catch {
            this._unacked.delete(key);
          }
        }
      }
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError)) throw e;
    }
  }

  /** Reverse-map a transport address to the peer registered on it. */
  _peer_for_addr(addr) {
    for (const [peer_id, a] of this._connections) {
      if (eq(a, addr)) return peer_id;
    }
    return null;
  }

  /**
   * Inbound-packet entry point (called by the endpoint protocol).
   *
   * Stream-lane datagrams (first byte ``< 0x20``) are routed before the
   * fragment parser; they are only accepted from registered peers.
   */
  _feed_packet(addr, data) {
    if (!Buffer.isBuffer(data)) data = Buffer.from(data);
    if (_codec.is_reserved_frame(data)) {
      const peer_id = this._peer_for_addr(addr);
      if (peer_id === null) {
        this._unknown_lane_drops += 1;
        return;
      }
      this._on_stream_frame(peer_id, data);
      return;
    }
    if (data.length < _PREFIX_LEN || !data.subarray(0, 3).equals(_MAGIC)) return;
    const [, ptype, frag_index, frag_count] = _HEADER.unpack(data.subarray(0, _HEADER.size));
    const msg_id = Buffer.from(data.subarray(_HEADER.size, _PREFIX_LEN));
    const payload = data.subarray(_PREFIX_LEN);
    const akey = _addr_key(addr);
    const mkey = msg_id.toString("hex");

    if (ptype === _TYPE_ACK) {
      this._unacked.delete(`${akey}|${mkey}|${frag_index}`);
      return;
    }

    // DATA: acknowledge this fragment, then try to reassemble.
    const ack = Buffer.concat([_HEADER.pack(_MAGIC, _TYPE_ACK, frag_index, frag_count), msg_id]);
    try {
      this._sendto(addr, ack);
    } catch {
      /* best effort */
    }

    if (this._seen.has(mkey)) return;

    const key = `${akey}|${mkey}`;
    let entry = this._reasm.get(key);
    if (entry === undefined) {
      entry = { count: frag_count, frags: new Map() };
      this._reasm.set(key, entry);
    }
    entry.frags.set(frag_index, payload);
    if (entry.frags.size < entry.count) return;

    this._reasm.delete(key);
    this._mark_seen(mkey);
    const parts = [];
    for (let i = 0; i < entry.count; i++) {
      const frag = entry.frags.get(i);
      if (frag === undefined) throw new KeyErrorLike(i);
      parts.push(frag);
    }
    this._on_message(addr, Buffer.concat(parts));
  }

  /** Record *msg_id* as processed, bounding the dedup set. */
  _mark_seen(msg_id) {
    this._seen.add(msg_id);
    this._seen_order.push(msg_id);
    if (this._seen_order.length > 4096) {
      const old = this._seen_order.shift();
      this._seen.delete(old);
    }
  }

  /** Decode a fully-reassembled JSON-RPC message and dispatch it. */
  _on_message(addr, payload) {
    let msg;
    try {
      msg = this._decode(payload);
    } catch {
      return;
    }

    if (rpc_protocol.is_request(msg)) {
      const method = _mget(msg, "method");
      if (method === _PEER_CONNECT) {
        this._handle_handshake(addr, msg);
      } else if (this._is_goodbye(msg)) {
        // only honoured from the address the peer is registered on
        const peer_id = this._peer_for_addr(addr);
        if (peer_id !== null && peer_id === _mget(_mget(msg, "params") || {}, "from_id")) this._on_peer_goodbye(peer_id);
      } else {
        this._dispatch_rpc(addr, msg);
      }
    } else if (rpc_protocol.is_response(msg)) {
      const rid = _mget(msg, "id");
      if (this._handshake_pending.has(rid)) {
        const slot = this._handshake_pending.get(rid);
        if (slot !== null && slot !== undefined) {
          slot.msg = msg;
          slot.addr = addr;
          slot.event.set();
        }
      } else {
        this._complete_pending(msg);
      }
    }
  }

  /** Server side of ``peer.connect`` over datagrams. */
  _handle_handshake(addr, msg) {
    const params = _mget(msg, "params", {}) ?? {};
    const peer_id = _mget(params, "from_id");
    if (_mget(params, "secret") !== this.peer_secret_key) {
      const resp = rpc_protocol.make_error(_mget(msg, "id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key.");
      this._send_message(addr, this._encode(resp));
      return;
    }
    const policy_id = this._communication ? this._communication.policy_id : null;
    const resp = rpc_protocol.make_result(_mget(msg, "id"), { peer_id: policy_id, caps: this._local_caps() });
    this._set_peer_caps(peer_id, _mget(params, "caps"));
    this._register_peer(peer_id, addr);
    this._send_message(addr, this._encode(resp));
  }

  /** Run an inbound ``rpc.call`` off the loop and reply to *addr*. */
  _dispatch_rpc(addr, msg) {
    const _reply = (resp) => {
      try {
        const data = this._encode(resp);
        this._loop_call(() => this._send_message(addr, data));
      } catch (e) {
        if (!(e instanceof ConnectionError)) throw e;
      }
    };
    this._handle_request_frame(msg, _reply, this._peer_for_addr(addr));
  }

  /** Send ``peer.disconnect`` to the peer's address (first try goes out at once). */
  async _send_goodbye(peer_id) {
    const addr = this._connections.get(peer_id) ?? null;
    if (addr === null || this._transport === null || this._transport === undefined) return;
    this._send_message(addr, this._encode(this._goodbye_message()));
  }

  // ------------------------------------------------------------------
  // Peering / RPC
  // ------------------------------------------------------------------

  _connect_prepare(addr, secret) {
    const policy_id = this._communication ? this._communication.policy_id : null;
    const req = rpc_protocol.make_request("peer.connect", { from_id: policy_id, secret, caps: this._local_caps() });
    const rid = req.id;
    const slot = { event: new Event() };
    this._handshake_pending.set(rid, slot);
    const data = this._encode(req);
    this._loop_call(() => this._send_message(addr, data));
    return [rid, slot];
  }

  _connect_finish(addr, rid, slot, completed) {
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
    this._register_peer(peer_id, addr);
    return peer_id;
  }

  /** Handshake with the remote endpoint named by *uri*. */
  connect(uri, secret) {
    this.start();
    const addr_fut = asyncio.run_coroutine_threadsafe(() => this._resolve_peer_addr(uri), this._event_loop);
    const addr = addr_fut.result(10.0);
    const [rid, slot] = this._connect_prepare(addr, secret);
    const completed = slot.event.wait(this.handshake_timeout);
    return this._connect_finish(addr, rid, slot, completed);
  }

  /** Awaitable ``connect``. */
  async connect_async(uri, secret) {
    await this.start_async();
    const addr_fut = asyncio.run_coroutine_threadsafe(() => this._resolve_peer_addr(uri), this._event_loop);
    const addr = await asyncio.wait_for(addr_fut, 10.0);
    const [rid, slot] = this._connect_prepare(addr, secret);
    const completed = await slot.event.wait_async(this.handshake_timeout);
    return this._connect_finish(addr, rid, slot, completed);
  }

  _send_once_prepare(peer_id, path, args, kwargs) {
    const addr = this._connections.get(peer_id) ?? null;
    if (addr === null) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const [request_id, slot] = this._register_pending();
    const req = this._make_rpc_request(path, args, kwargs, request_id);
    try {
      const data = this._encode(req);
      this._loop_call(() => this._send_message(addr, data));
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

  /** Send a control request (reliably) without waiting for its reply. */
  _send_oneway(peer_id, path, kwargs) {
    const addr = this._connections.get(peer_id) ?? null;
    if (addr === null) throw new ConnectionError(`No connection to peer ${peer_id}`);
    const req = this._make_rpc_request(path, [], kwargs, String(uuid4()));
    const data = this._encode(req);
    this._loop_call(() => this._send_message(addr, data));
  }

  // ------------------------------------------------------------------
  // Stream lanes (single datagram per message, best effort)
  // ------------------------------------------------------------------

  /** A stream message must fit one datagram: ``mtu - header``. */
  _max_message_bytes() {
    return Math.max(0, Math.min(Math.trunc(this.max_stream_frame_bytes), Math.trunc(this.mtu) - _codec.STREAM_HEADER_LEN));
  }

  /** Send *payload* as one unacknowledged datagram (user thread). */
  _stream_enqueue(channel, payload) {
    const addr = this._connections.get(channel.peer_id) ?? null;
    if (addr === null) throw new ConnectionError(`No connection to peer ${channel.peer_id}`);
    const limit = this._max_message_bytes();
    if (payload.length > limit) {
      throw new ValueError(
        `Stream message of ${payload.length} bytes does not fit one datagram ` +
          `(mtu=${this.mtu} - ${_codec.STREAM_HEADER_LEN} header = ${limit}). ` +
          "Datagram lanes do not fragment.",
      );
    }
    const seq = channel._next_chunk_seqs(1);
    const packet = _codec.pack_stream_frame(channel.tx_lane_id, seq, _codec.FLAG_START | _codec.FLAG_END, payload);
    this._loop_call(() => this._safe_sendto(addr, packet));
  }

  _safe_sendto(addr, packet) {
    try {
      this._sendto(addr, packet);
    } catch (e) {
      log.debug("stream datagram send failed", { exc_info: e });
    }
  }
}

/** ``KeyError`` raised when a fragment index is missing (cannot happen after the size check). */
class KeyErrorLike extends Error {
  constructor(k) {
    super(String(k));
    this.name = "KeyError";
  }
}

register("laila.policy.central.communication.protocols._carriers.datagram", {
  _DatagramEndpoint,
  _DatagramRPCProtocol,
  _MAGIC,
  _TYPE_DATA,
  _TYPE_ACK,
  _HEADER,
  _MSG_ID_LEN,
  _PREFIX_LEN,
});
