/**
 * Stream lanes: ``Channel``, ``Relay``, ``StreamEntry``.
 *
 * A *channel* is an opaque, bidirectional byte stream between two peered
 * policies that rides on the same transport connection as RPC. laila does
 * not interpret the bytes (H.264/H.265 access units, JPEGs, telemetry
 * structs, audio -- all the same here); interpretation is the policy's job.
 * The user-facing shape is::
 *
 *     const video = laila.peers[peer_gid]["video"];   // Channel; opens lazily
 *     for (const entry of laila.relay(video)) {       // blocks on the caller's thread
 *       const au = entry.data;                        // bytes: exactly one sender-side send()
 *     }
 *     video.send(Buffer.from("..."));                 // channels are bidirectional
 *
 * Three pieces live here:
 *
 * - ``Channel`` -- per ``(peer, name)`` handle owned by the carrier that
 *   negotiated it. Holds the bounded inbound queue, the send-side entry
 *   point, message-reassembly state and the drop counters.
 * - ``Relay`` -- the blocking iterator returned by ``laila.relay``. One per
 *   channel; it wraps each delivered message into a ``StreamEntry`` *on the
 *   consumer thread*, never on the carrier's I/O loop. In JS the relay is
 *   also an async iterator (``for await``) for callers that cannot block.
 * - ``StreamEntry`` -- an ``Entry`` whose payload is the received ``bytes``
 *   plus a read-only ``stream`` ``StreamMeta`` describing where the packet
 *   came from. It is a plain in-memory constant: not memorized, not pooled.
 *
 * Reconnect semantics
 * -------------------
 * A channel is bound to one peer *connection*. When the peer drops every
 * channel for that peer is closed: its relay ends with ``StopIteration``,
 * ``Channel.closed`` becomes ``true`` and ``Channel.send`` raises
 * ``ConnectionError``. After the peer reconnects, ``laila.peers[gid]`` is a
 * *new* proxy and ``laila.peers[gid][name]`` returns a *new* channel.
 *
 * Drop policy
 * -----------
 * Inbound queues are bounded by message count (``channel_queue_size``) and
 * bytes (``channel_queue_bytes``). When a consumer is too slow the oldest
 * queued message is evicted (``dropped`` increments) so the carrier loop
 * never blocks. Message boundaries are always preserved: a
 * partially-received message (lost chunk, oversize) is discarded whole
 * (``discarded`` increments) and never surfaces to the relay.
 */
import { ConnectionError, StopIteration, TimeoutError as PyTimeoutError, ValueError } from "../../../_compat/errors.js";
import { register } from "../../../_compat/lazy.js";
import { PrivateAttr, define_private } from "../../../_compat/pydantic.js";
import { repr } from "../../../_compat/pyrepr.js";
import { Empty, Full, Queue } from "../../../_compat/queue.js";
import { Event, Lock, with_lock } from "../../../_compat/threading.js";
import * as time from "../../../_compat/time.js";
import { Entry } from "../../../entry/entry.js";
import { EntryState } from "../../../entry/entry_state.js";
import * as _codec from "./wire.js";

/** Queue marker that ends a ``Relay``. */
const _SENTINEL = Object.freeze({ __stream_sentinel__: true });

/**
 * Provenance of one received stream message (frozen dataclass).
 *
 * @property {string} peer_id ``global_id`` of the sending policy.
 * @property {string} channel Channel name (e.g. ``"video"``).
 * @property {number} lane Local (receive-side) lane id the message arrived on.
 * @property {number} seq Message-level sequence number assigned on arrival
 *   (``0, 1, 2 ...`` per channel). Gaps seen by a consumer equal the number
 *   of messages evicted by the drop-oldest policy before it.
 * @property {number} arrived_at ``time.monotonic()`` when the complete
 *   message was enqueued.
 * @property {number} arrived_wall ``time.time()`` at the same instant.
 * @property {number} dropped_before Cumulative inbound drop count of the
 *   channel when *this* message was enqueued.
 */
export class StreamMeta {
  constructor(opts) {
    const { peer_id, channel, lane, seq, arrived_at, arrived_wall, dropped_before } = opts;
    this.peer_id = peer_id;
    this.channel = channel;
    this.lane = lane;
    this.seq = seq;
    this.arrived_at = arrived_at;
    this.arrived_wall = arrived_wall;
    this.dropped_before = dropped_before;
    Object.freeze(this);
  }
  __eq__(other) {
    return (
      other instanceof StreamMeta &&
      other.peer_id === this.peer_id &&
      other.channel === this.channel &&
      other.lane === this.lane &&
      other.seq === this.seq &&
      other.arrived_at === this.arrived_at &&
      other.arrived_wall === this.arrived_wall &&
      other.dropped_before === this.dropped_before
    );
  }
  __repr__() {
    return (
      `StreamMeta(peer_id=${repr(this.peer_id)}, channel=${repr(this.channel)}, lane=${this.lane}, ` +
      `seq=${this.seq}, arrived_at=${repr(this.arrived_at)}, arrived_wall=${repr(this.arrived_wall)}, ` +
      `dropped_before=${this.dropped_before})`
    );
  }
  toString() {
    return this.__repr__();
  }
}

/**
 * An ``Entry`` produced by ``Relay`` for one stream message.
 *
 * Identical to ``Entry.constant(payload)`` -- fresh ``uuid4``, ``evolution``
 * ``null``, scopes ``["ENTRY"]``, state ``READY`` -- plus a read-only
 * ``stream`` carrying ``StreamMeta``.
 */
export class StreamEntry extends Entry {
  static {
    define_private(this, {
      _stream: PrivateAttr({ default: null }),
    });
  }

  /** Where this packet came from (``null`` for non-stream entries). */
  get stream() {
    return this._stream ?? null;
  }

  /**
   * Build a READY constant entry around *payload*.
   *
   * Mirrors the body of ``Entry.constant`` (which hard-codes ``new Entry(...)``
   * and therefore cannot return a subclass).
   * @param {Uint8Array} payload
   * @returns {StreamEntry}
   */
  static _from_payload(payload) {
    return new this({ uuid: null, data: payload, state: EntryState.READY, evolution: null });
  }
}

/**
 * A named, bidirectional byte lane to one peer over one carrier.
 *
 * Obtained through ``laila.peers[gid][name]`` (or
 * ``laila.peers[gid].via(token)[name]`` to pin the transport). Channels are
 * created and cached by the carrier; user code never constructs them.
 */
export class Channel {
  /**
   * @param {any} protocol Owning carrier (held by weak reference).
   * @param {string} peer_id Remote policy ``global_id``.
   * @param {string} name Channel name (free-form; ``"default"`` is reserved for RPC).
   * @param {number} lane_id Local receive-side lane id (``1..255``).
   * @param {{queue_size: number, queue_bytes: number, max_message_bytes: number, tx_lane_id?: number|null}} opts
   */
  constructor(protocol, peer_id, name, lane_id, opts) {
    const { queue_size, queue_bytes, max_message_bytes, tx_lane_id = null } = opts;
    this._proto_ref = new WeakRef(protocol);
    this.peer_id = peer_id;
    this.name = name;
    this.lane_id = lane_id;
    this.tx_lane_id = tx_lane_id;
    this.max_message_bytes = max_message_bytes;
    this.closed = false;
    /** @type {string|null} */
    this.closed_reason = null;
    this.rx_seq = 0;
    this.tx_seq = 0;
    this.dropped = 0;
    this.discarded = 0;
    this.tx_dropped = 0;
    this._queue = new Queue(Math.max(1, Math.trunc(queue_size)));
    this._queue_bytes = 0;
    this._queue_bytes_cap = Math.max(1, Math.trunc(queue_bytes));
    this._lock = new Lock();
    this._opened = new Event();
    if (tx_lane_id !== null && tx_lane_id !== undefined) this._opened.set();
    /** @type {Relay|null} */
    this._relay = null;
    // reassembly state (touched only by the carrier's inbound thread)
    /** @type {Buffer[]} */
    this._rx_buf = [];
    this._rx_len = 0;
    /** @type {number|null} */
    this._rx_expected = null;
    this._rx_active = false;
    this._rx_poisoned = false;
    this._tx_chunk_seq = 0;
    /** Async consumers parked on an empty queue (JS-only ``for await``). @type {Array<() => void>} */
    this._async_waiters = [];
  }

  // ------------------------------------------------------------------
  // Introspection
  // ------------------------------------------------------------------

  /** The owning carrier, or ``null`` if it has been garbage-collected. */
  get protocol() {
    return this._proto_ref.deref() ?? null;
  }

  /** ``true`` once the lane handshake completed (``tx_lane_id`` known). */
  get opened() {
    return this.tx_lane_id !== null && this.tx_lane_id !== undefined && !this.closed;
  }

  /** Messages currently buffered for the consumer. */
  qsize() {
    return this._queue.qsize();
  }

  __repr__() {
    const state = this.closed ? "closed" : this.opened ? "open" : "opening";
    return `Channel(${repr(this.peer_id)}, ${repr(this.name)}, rx_lane=${this.lane_id}, tx_lane=${repr(this.tx_lane_id)}, ${state})`;
  }
  toString() {
    return this.__repr__();
  }

  // ------------------------------------------------------------------
  // Send side (user threads)
  // ------------------------------------------------------------------

  /**
   * Queue one message for the peer. One ``send()`` -> one entry remotely.
   *
   * Non-blocking: the bytes are handed to the carrier's bounded send queue.
   * On a saturated link the oldest *unsent* message is evicted and
   * ``tx_dropped`` increments.
   *
   * @param {Uint8Array|string} payload Opaque bytes. ``b""`` is a valid message.
   * @throws {ConnectionError} If the channel is closed, the carrier is gone,
   *   or the lane handshake did not complete in time.
   * @throws {ValueError} If ``payload.length`` exceeds the carrier's
   *   ``max_stream_frame_bytes`` (or, on datagram carriers, the per-datagram limit).
   */
  send(payload) {
    if (this.closed) {
      throw new ConnectionError(
        `Channel ${repr(this.name)} to peer ${this.peer_id} is closed ` +
          `(${this.closed_reason || "no reason"}). Re-index laila.peers[gid][name] ` +
          "after the peer reconnects.",
      );
    }
    if (!Buffer.isBuffer(payload)) payload = Buffer.from(payload);
    if (payload.length > this.max_message_bytes) {
      throw new ValueError(
        `Stream message of ${payload.length} bytes exceeds the ${this.max_message_bytes}-byte ` +
          `limit of channel ${repr(this.name)} (max_stream_frame_bytes, or mtu - header on ` +
          "datagram carriers).",
      );
    }
    const proto = this.protocol;
    if (proto === null) throw new ConnectionError("The carrier owning this channel no longer exists.");
    if (this.tx_lane_id === null || this.tx_lane_id === undefined) {
      const wait = Number(proto.rpc_timeout ?? 60.0);
      if (!this._opened.wait(wait) || this.tx_lane_id === null || this.tx_lane_id === undefined) {
        throw new ConnectionError(`Channel ${repr(this.name)} to peer ${this.peer_id} is still opening.`);
      }
      if (this.closed) {
        throw new ConnectionError(`Channel ${repr(this.name)} to peer ${this.peer_id} closed while opening (${this.closed_reason}).`);
      }
    }
    proto._stream_enqueue(this, payload);
    with_lock(this._lock, () => {
      this.tx_seq += 1;
    });
  }

  /**
   * Awaitable ``send``: waits for the lane handshake without blocking the
   * loop (JS-only convenience; same semantics as ``send``).
   */
  async send_async(payload) {
    if (!this.closed && (this.tx_lane_id === null || this.tx_lane_id === undefined)) {
      const proto = this.protocol;
      if (proto === null) throw new ConnectionError("The carrier owning this channel no longer exists.");
      const wait = Number(proto.rpc_timeout ?? 60.0);
      await this._opened.wait_async(wait);
    }
    return this.send(payload);
  }

  /** Reserve *count* consecutive wire chunk sequence numbers; return the first. */
  _next_chunk_seqs(count) {
    return with_lock(this._lock, () => {
      const first = this._tx_chunk_seq;
      this._tx_chunk_seq = (first + count) % _codec.SEQ_MODULUS;
      return first;
    });
  }

  // ------------------------------------------------------------------
  // Receive side (carrier inbound thread)
  // ------------------------------------------------------------------

  /**
   * Feed one wire chunk into the reassembly buffer.
   *
   * ``seq`` is the per-lane chunk counter; a gap means a chunk was lost and
   * the in-progress message is discarded whole. ``START`` resets the buffer,
   * ``END`` delivers.
   */
  _on_chunk(seq, flags, data, arrived_at) {
    const start = !!(flags & _codec.FLAG_START);
    const end = !!(flags & _codec.FLAG_END);
    if (start) {
      if (this._rx_active) {
        // previous message never saw its END -> lost tail
        this.discarded += 1;
      }
      this._rx_buf = [];
      this._rx_len = 0;
      this._rx_active = true;
      this._rx_poisoned = false;
    } else {
      if (!this._rx_active) {
        // continuation without a START -> head was lost; swallow
        this._rx_expected = (seq + 1) % _codec.SEQ_MODULUS;
        if (end) this.discarded += 1;
        return;
      }
      if (this._rx_expected !== null && seq !== this._rx_expected) this._rx_poisoned = true;
    }
    this._rx_expected = (seq + 1) % _codec.SEQ_MODULUS;

    if (!this._rx_poisoned) {
      this._rx_len += data.length;
      if (this._rx_len > this.max_message_bytes) {
        this._rx_poisoned = true;
        this._rx_buf = [];
      } else {
        this._rx_buf.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
      }
    }

    if (!end) return;
    this._rx_active = false;
    const buf = this._rx_buf;
    this._rx_buf = [];
    this._rx_len = 0;
    if (this._rx_poisoned) {
      this._rx_poisoned = false;
      this.discarded += 1;
      return;
    }
    let payload;
    if (buf.length === 1) payload = buf[0];
    else if (buf.length === 0) payload = Buffer.alloc(0);
    else payload = Buffer.concat(buf);
    this._enqueue_inbound(payload, arrived_at);
  }

  /**
   * Enqueue one complete message, evicting the oldest when over bounds.
   *
   * Safe from any thread (loopback delivers from the sender's thread).
   * Never blocks.
   */
  _enqueue_inbound(payload, arrived_at = null) {
    if (arrived_at === null || arrived_at === undefined) arrived_at = time.monotonic();
    const wall = time.time();
    const size = payload.length;
    with_lock(this._lock, () => {
      if (this.closed) return;
      const seq = this.rx_seq;
      this.rx_seq += 1;
      const item = [payload, seq, arrived_at, wall, this.dropped];
      for (;;) {
        const over_bytes = this._queue_bytes > 0 && this._queue_bytes + size > this._queue_bytes_cap;
        if (!over_bytes) {
          try {
            this._queue.put_nowait(item);
            this._queue_bytes += size;
            this._wake_async();
            return;
          } catch (e) {
            if (!(e instanceof Full)) throw e;
          }
        }
        let old;
        try {
          old = this._queue.get_nowait();
        } catch (e) {
          if (!(e instanceof Empty)) throw e;
          // consumer drained concurrently; account and retry
          this._queue_bytes = 0;
          continue;
        }
        if (old === _SENTINEL) {
          // cannot happen while not closed; be defensive
          this._queue.put_nowait(old);
          return;
        }
        this._queue_bytes -= old[0].length;
        this.dropped += 1;
      }
    });
  }

  /** Book-keeping hook called by ``Relay`` after a successful get. */
  _consumed(item) {
    with_lock(this._lock, () => {
      this._queue_bytes -= item[0].length;
      if (this._queue_bytes < 0) this._queue_bytes = 0;
    });
  }

  _wake_async() {
    if (this._async_waiters.length === 0) return;
    const ws = this._async_waiters;
    this._async_waiters = [];
    for (const w of ws) w();
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Finalize the open handshake (called by the carrier). */
  _mark_opened(tx_lane_id) {
    this.tx_lane_id = tx_lane_id;
    this._opened.set();
  }

  /**
   * Close without notifying the peer. Idempotent; returns ``true`` on first close.
   *
   * Wakes a blocked ``Relay`` by enqueuing the sentinel (evicting one message
   * if the queue is full so the wake-up cannot be lost).
   */
  _close_local(reason) {
    return with_lock(this._lock, () => {
      if (this.closed) return false;
      this.closed = true;
      this.closed_reason = reason;
      this._opened.set();
      for (;;) {
        try {
          this._queue.put_nowait(_SENTINEL);
          break;
        } catch (e) {
          if (!(e instanceof Full)) throw e;
          let old;
          try {
            old = this._queue.get_nowait();
          } catch (e2) {
            if (!(e2 instanceof Empty)) throw e2;
            continue;
          }
          if (old !== _SENTINEL) {
            this._queue_bytes -= old[0].length;
            this.dropped += 1;
          }
        }
      }
      this._rx_buf = [];
      this._rx_len = 0;
      this._rx_active = false;
      this._wake_async();
      return true;
    });
  }

  /**
   * Close this channel and tell the peer (best effort). Idempotent.
   *
   * The peer's matching channel ends its relay with ``StopIteration``.
   * Pending inbound messages are dropped; a blocked relay wakes up.
   */
  close(reason = "closed locally") {
    const proto = this.protocol;
    if (!this._close_local(reason)) return;
    if (proto !== null) {
      try {
        proto._on_channel_closed_locally(this);
      } catch {
        /* best effort */
      }
    }
  }

  /**
   * Return the channel's single ``Relay``, updating its *timeout*.
   *
   * See ``laila.relay``. Two consumers iterating the same relay split the
   * packets between them (it is one queue).
   */
  relay(timeout = null) {
    return with_lock(this._lock, () => {
      if (this._relay === null) this._relay = new Relay(this, { timeout });
      else this._relay.timeout = timeout;
      return this._relay;
    });
  }
}

/**
 * Blocking iterator over one ``Channel`` yielding ``StreamEntry``.
 *
 * Returned by ``laila.relay`` / ``Channel.relay``. Runs on the caller's
 * thread; no futures, no taskforce, no threads spawned by laila.
 *
 * - Iteration ends (``StopIteration`` / ``{done: true}``) when the channel
 *   closes: peer loss, remote ``close()``, local ``close()``,
 *   carrier/communication ``stop()`` or ``laila.terminate``. Messages
 *   already buffered are drained first. ``closed_reason`` says why.
 * - The wait loop wakes at least once a second to re-check the closed flag,
 *   so a missed sentinel cannot strand the consumer.
 * - ``for await (const e of relay)`` is the non-blocking form for callers
 *   that run inside a microtask (JS-only).
 */
export class Relay {
  /**
   * @param {Channel} channel Source channel.
   * @param {{timeout?: number|null}} [opts] Per-message wait. ``null`` waits
   *   until a message arrives or the channel closes; otherwise ``TimeoutError``
   *   is raised when no message arrives within *timeout* seconds.
   */
  constructor(channel, opts = {}) {
    const timeout = typeof opts === "number" ? opts : (opts.timeout ?? null);
    this.channel = channel;
    this.timeout = timeout;
  }

  /** Why the underlying channel closed (``null`` while open). */
  get closed_reason() {
    return this.channel.closed_reason;
  }

  __iter__() {
    return this;
  }

  /** Python ``__next__``: blocks; raises ``StopIteration`` when the channel closes. */
  __next__() {
    const ch = this.channel;
    const q = ch._queue;
    const timeout = this.timeout;
    const deadline = timeout === null || timeout === undefined ? null : time.monotonic() + timeout;
    for (;;) {
      if (ch.closed && q.empty()) throw new StopIteration();
      let wait;
      if (deadline === null) wait = 1.0;
      else {
        const remaining = deadline - time.monotonic();
        if (remaining <= 0) {
          throw new PyTimeoutError(`No message on channel ${repr(ch.name)} from ${ch.peer_id} within ${repr(timeout)}s.`);
        }
        wait = Math.min(1.0, remaining);
      }
      let item;
      try {
        item = q.get(true, wait);
      } catch (e) {
        if (e instanceof Empty) continue;
        throw e;
      }
      if (item === _SENTINEL) throw new StopIteration();
      ch._consumed(item);
      return this._wrap(item);
    }
  }

  /** JS iterator protocol (``for (const e of relay)``). */
  next() {
    try {
      return { value: this.__next__(), done: false };
    } catch (e) {
      if (e instanceof StopIteration) return { value: undefined, done: true };
      throw e;
    }
  }

  [Symbol.iterator]() {
    return this;
  }

  /** Awaitable ``__next__`` (``for await``). */
  async __anext__() {
    const ch = this.channel;
    const q = ch._queue;
    const timeout = this.timeout;
    const deadline = timeout === null || timeout === undefined ? null : time.monotonic() + timeout;
    for (;;) {
      if (ch.closed && q.empty()) throw new StopIteration();
      let item;
      try {
        item = q.get_nowait();
      } catch (e) {
        if (!(e instanceof Empty)) throw e;
        let wait = 1.0;
        if (deadline !== null) {
          const remaining = deadline - time.monotonic();
          if (remaining <= 0) {
            throw new PyTimeoutError(`No message on channel ${repr(ch.name)} from ${ch.peer_id} within ${repr(timeout)}s.`);
          }
          wait = Math.min(1.0, remaining);
        }
        await new Promise((res) => {
          const t = setTimeout(res, wait * 1000);
          ch._async_waiters.push(() => {
            clearTimeout(t);
            res();
          });
        });
        continue;
      }
      if (item === _SENTINEL) throw new StopIteration();
      ch._consumed(item);
      return this._wrap(item);
    }
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        try {
          return { value: await this.__anext__(), done: false };
        } catch (e) {
          if (e instanceof StopIteration) return { value: undefined, done: true };
          throw e;
        }
      },
    };
  }

  /**
   * Turn a queued ``[payload, seq, arrived_at, wall, dropped_before]`` into an entry.
   *
   * The single place where stream identity is minted (see ``StreamEntry``).
   */
  _wrap(item) {
    const [payload, seq, arrived_at, wall, dropped_before] = item;
    const ch = this.channel;
    const entry = StreamEntry._from_payload(payload);
    entry._stream = new StreamMeta({
      peer_id: ch.peer_id,
      channel: ch.name,
      lane: ch.lane_id,
      seq,
      arrived_at,
      arrived_wall: wall,
      dropped_before,
    });
    return entry;
  }

  __repr__() {
    return `Relay(${repr(this.channel)}, timeout=${repr(this.timeout)})`;
  }
  toString() {
    return this.__repr__();
  }
}

register("laila.policy.central.communication.channel", { Channel, Relay, StreamEntry, StreamMeta, _SENTINEL });
