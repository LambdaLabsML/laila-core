"""Stream lanes: :class:`Channel`, :class:`Relay`, :class:`StreamEntry`.

A *channel* is an opaque, bidirectional byte stream between two peered
policies that rides on the same transport connection as RPC. laila does
not interpret the bytes (H.264/H.265 access units, JPEGs, telemetry
structs, audio -- all the same here); interpretation is the policy's
job. The user-facing shape is::

    video = laila.peers[peer_gid]["video"]      # Channel; opens lazily
    for entry in laila.relay(video):            # blocks on the caller's thread
        au = entry.data                         # bytes: exactly one sender-side send()
    video.send(b"...")                          # channels are bidirectional

Three pieces live here:

- :class:`Channel` -- per ``(peer, name)`` handle owned by the carrier
  that negotiated it. Holds the bounded inbound queue, the send-side
  entry point, message-reassembly state and the drop counters.
- :class:`Relay` -- the blocking iterator returned by
  :func:`laila.relay`. One per channel; it wraps each delivered message
  into a :class:`StreamEntry` *on the consumer thread*, never on the
  carrier's I/O loop.
- :class:`StreamEntry` -- an :class:`~laila.entry.entry.Entry` whose
  payload is the received ``bytes`` plus a read-only
  :attr:`StreamEntry.stream` :class:`StreamMeta` describing where the
  packet came from. It is a plain in-memory constant: not memorized,
  not pooled. ``laila.memorize(entry)`` sends it through the ordinary
  memory path; the ``.stream`` metadata is local-only and is not
  preserved by a memorize/remember round trip.

Data path (what stream bytes *do not* touch)
--------------------------------------------
Stream frames use the carrier's event loop, reader/writer and outer
framing, and nothing else: no ``_handle_request_frame``, no admission
semaphore, no inbound executor, no ``_execute_rpc``, no codec decode.
No futures, no ``future_bank``, no taskforce, no threads spawned by
laila for relays. The receive loop routes them by a first-byte
discriminator straight into the lane's inbound queue.

Reconnect semantics
-------------------
A channel is bound to one peer *connection*. When the peer drops
(liveness, EOF, ``remove_peer``, ``stop()``) every channel for that peer
is closed: its relay ends with ``StopIteration``, :attr:`Channel.closed`
becomes ``True`` and :meth:`Channel.send` raises ``ConnectionError``.
After the peer reconnects, ``laila.peers[gid]`` is a *new* proxy and
``laila.peers[gid][name]`` returns a *new* channel; objects held from
before the drop stay closed. Re-index to resume.

Drop policy
-----------
Inbound queues are bounded by message count (``channel_queue_size``) and
bytes (``channel_queue_bytes``). When a consumer is too slow the oldest
queued message is evicted (``dropped`` increments) so the carrier loop
never blocks and memory never grows unbounded, even for channels nobody
calls :func:`laila.relay` on. Message boundaries are always preserved:
a partially-received message (lost chunk, oversize) is discarded whole
(``discarded`` increments) and never surfaces to the relay.
"""

from __future__ import annotations

import queue
import threading
import time
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import PrivateAttr

from ....entry.entry import Entry
from ....entry.entry_state import EntryState
from . import wire as _codec

if TYPE_CHECKING:
    from .protocols._carriers.base import _CarrierRPCProtocol

#: Queue marker that ends a :class:`Relay`.
_SENTINEL: Any = object()


@dataclass(frozen=True, slots=True)
class StreamMeta:
    """Provenance of one received stream message.

    Attributes
    ----------
    peer_id : str
        ``global_id`` of the sending policy.
    channel : str
        Channel name (e.g. ``"video"``).
    lane : int
        Local (receive-side) lane id the message arrived on.
    seq : int
        Message-level sequence number assigned on arrival (``0, 1, 2 ...``
        per channel). Gaps seen by a consumer equal the number of
        messages evicted by the drop-oldest policy before it.
    arrived_at : float
        ``time.monotonic()`` when the complete message was enqueued.
        Monotonic across entries of one channel; use for ordering and
        latency, not for wall-clock stamps.
    arrived_wall : float
        ``time.time()`` at the same instant.
    dropped_before : int
        Cumulative inbound drop count of the channel when *this* message
        was enqueued (messages evicted later, while it waited, are not
        included). ``seq`` gaps are the authoritative loss signal.
    """

    peer_id: str
    channel: str
    lane: int
    seq: int
    arrived_at: float
    arrived_wall: float
    dropped_before: int


class StreamEntry(Entry):
    """An :class:`Entry` produced by :class:`Relay` for one stream message.

    Identical to ``Entry.constant(payload)`` -- fresh ``uuid4``,
    ``evolution`` ``None``, scopes ``["ENTRY"]``, state ``READY`` -- plus
    a read-only :attr:`stream` carrying :class:`StreamMeta`. Nothing in
    the entry package changes for this; the subclass only adds one
    private attribute.

    Notes
    -----
    Rationale for *constant* identity: a packet is an immutable fact; a
    new ``global_id`` per packet is the current model. Identity creation
    is centralised in :meth:`Relay._wrap` so a later evolution-based
    scheme (``nickname=f"{peer}/{channel}", evolution=seq``) is a
    one-function change.
    """

    _stream: Any = PrivateAttr(default=None)

    @property
    def stream(self) -> StreamMeta | None:
        """Where this packet came from (``None`` for non-stream entries)."""
        return self._stream

    @classmethod
    def _from_payload(cls, payload: bytes) -> StreamEntry:
        """Build a READY constant entry around *payload*.

        Mirrors the body of :meth:`Entry.constant` (which hard-codes
        ``Entry(...)`` and therefore cannot return a subclass).
        """
        return cls(uuid=None, data=payload, state=EntryState.READY, evolution=None)


class Channel:
    """A named, bidirectional byte lane to one peer over one carrier.

    Obtained through ``laila.peers[gid][name]`` (or
    ``laila.peers[gid].via(token)[name]`` to pin the transport). Channels
    are created and cached by the carrier; user code never constructs
    them.

    Parameters
    ----------
    protocol : _CarrierRPCProtocol
        Owning carrier (held by weak reference).
    peer_id : str
        Remote policy ``global_id``.
    name : str
        Channel name (free-form; ``"default"`` is reserved for RPC).
    lane_id : int
        Local receive-side lane id (``1..255``). Frames the peer sends to
        us carry this id.
    queue_size, queue_bytes : int
        Inbound bounds (messages / bytes); oldest messages are evicted
        when exceeded.
    max_message_bytes : int
        Largest message accepted on either direction.
    tx_lane_id : int, optional
        The peer's receive-side lane id (what *we* put in frames). ``None``
        while an open handshake is still in flight.

    Attributes
    ----------
    closed : bool
        ``True`` once closed by either side, by peer loss or by shutdown.
    closed_reason : str | None
        Human-readable reason set at close time.
    rx_seq : int
        Messages enqueued so far (message-level sequence counter).
    tx_seq : int
        Messages accepted by :meth:`send`.
    dropped : int
        Inbound messages evicted because the consumer was too slow.
    discarded : int
        Inbound messages thrown away because a chunk was lost or the
        message exceeded ``max_message_bytes``.
    tx_dropped : int
        Outbound messages evicted from the carrier's bounded send queue.

    Notes
    -----
    After a peer drop this object stays closed forever; re-index
    ``laila.peers[gid][name]`` after the peer reconnects (see the module
    docstring).
    """

    __slots__ = (
        "__weakref__",
        "_lock",
        "_opened",
        "_proto_ref",
        "_queue",
        "_queue_bytes",
        "_queue_bytes_cap",
        "_relay",
        "_rx_active",
        "_rx_buf",
        "_rx_expected",
        "_rx_len",
        "_rx_poisoned",
        "_tx_chunk_seq",
        "closed",
        "closed_reason",
        "discarded",
        "dropped",
        "lane_id",
        "max_message_bytes",
        "name",
        "peer_id",
        "rx_seq",
        "tx_dropped",
        "tx_lane_id",
        "tx_seq",
    )

    def __init__(
        self,
        protocol: _CarrierRPCProtocol,
        peer_id: str,
        name: str,
        lane_id: int,
        *,
        queue_size: int,
        queue_bytes: int,
        max_message_bytes: int,
        tx_lane_id: int | None = None,
    ) -> None:
        self._proto_ref = weakref.ref(protocol)
        self.peer_id = peer_id
        self.name = name
        self.lane_id = lane_id
        self.tx_lane_id = tx_lane_id
        self.max_message_bytes = max_message_bytes
        self.closed = False
        self.closed_reason: str | None = None
        self.rx_seq = 0
        self.tx_seq = 0
        self.dropped = 0
        self.discarded = 0
        self.tx_dropped = 0
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, int(queue_size)))
        self._queue_bytes = 0
        self._queue_bytes_cap = max(1, int(queue_bytes))
        self._lock = threading.Lock()
        self._opened = threading.Event()
        if tx_lane_id is not None:
            self._opened.set()
        self._relay: Relay | None = None
        # reassembly state (touched only by the carrier's inbound thread)
        self._rx_buf: list[bytes] = []
        self._rx_len = 0
        self._rx_expected: int | None = None
        self._rx_active = False
        self._rx_poisoned = False
        self._tx_chunk_seq = 0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def protocol(self) -> _CarrierRPCProtocol | None:
        """The owning carrier, or ``None`` if it has been garbage-collected."""
        return self._proto_ref()

    @property
    def opened(self) -> bool:
        """``True`` once the lane handshake completed (``tx_lane_id`` known)."""
        return self.tx_lane_id is not None and not self.closed

    def qsize(self) -> int:
        """Messages currently buffered for the consumer."""
        return self._queue.qsize()

    def __repr__(self) -> str:
        state = "closed" if self.closed else ("open" if self.opened else "opening")
        return (
            f"Channel({self.peer_id!r}, {self.name!r}, rx_lane={self.lane_id}, "
            f"tx_lane={self.tx_lane_id}, {state})"
        )

    # ------------------------------------------------------------------
    # Send side (user threads)
    # ------------------------------------------------------------------

    def send(self, payload: bytes | bytearray | memoryview) -> None:
        """Queue one message for the peer. One ``send()`` -> one entry remotely.

        Parameters
        ----------
        payload : bytes-like
            Opaque bytes. ``b""`` is a valid message.

        Raises
        ------
        ConnectionError
            If the channel is closed, the carrier is gone, or the lane
            handshake did not complete in time.
        ValueError
            If ``len(payload)`` exceeds the carrier's
            ``max_stream_frame_bytes`` (or, on datagram carriers, the
            per-datagram limit).

        Notes
        -----
        Non-blocking: the bytes are handed to the carrier's bounded send
        queue. On a saturated link the oldest *unsent* message is evicted
        and :attr:`tx_dropped` increments.
        """
        if self.closed:
            raise ConnectionError(
                f"Channel {self.name!r} to peer {self.peer_id} is closed "
                f"({self.closed_reason or 'no reason'}). Re-index laila.peers[gid][name] "
                "after the peer reconnects."
            )
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        if len(payload) > self.max_message_bytes:
            raise ValueError(
                f"Stream message of {len(payload)} bytes exceeds the {self.max_message_bytes}-byte "
                f"limit of channel {self.name!r} (max_stream_frame_bytes, or mtu - header on "
                "datagram carriers)."
            )
        proto = self.protocol
        if proto is None:
            raise ConnectionError("The carrier owning this channel no longer exists.")
        if self.tx_lane_id is None:
            wait = float(getattr(proto, "rpc_timeout", 60.0))
            if not self._opened.wait(timeout=wait) or self.tx_lane_id is None:
                raise ConnectionError(
                    f"Channel {self.name!r} to peer {self.peer_id} is still opening."
                )
            if self.closed:
                raise ConnectionError(
                    f"Channel {self.name!r} to peer {self.peer_id} closed while opening "
                    f"({self.closed_reason})."
                )
        proto._stream_enqueue(self, payload)
        with self._lock:
            self.tx_seq += 1

    def _next_chunk_seqs(self, count: int) -> int:
        """Reserve *count* consecutive wire chunk sequence numbers; return the first."""
        with self._lock:
            first = self._tx_chunk_seq
            self._tx_chunk_seq = (first + count) % _codec.SEQ_MODULUS
        return first

    # ------------------------------------------------------------------
    # Receive side (carrier inbound thread)
    # ------------------------------------------------------------------

    def _on_chunk(self, seq: int, flags: int, data: bytes, arrived_at: float) -> None:
        """Feed one wire chunk into the reassembly buffer.

        ``seq`` is the per-lane chunk counter; a gap means a chunk was lost
        and the in-progress message is discarded whole. ``START`` resets
        the buffer, ``END`` delivers. Runs on the carrier's inbound
        thread only, so it is lock-free.
        """
        start = bool(flags & _codec.FLAG_START)
        end = bool(flags & _codec.FLAG_END)
        if start:
            if self._rx_active:
                # previous message never saw its END -> lost tail
                self.discarded += 1
            self._rx_buf = []
            self._rx_len = 0
            self._rx_active = True
            self._rx_poisoned = False
        else:
            if not self._rx_active:
                # continuation without a START -> head was lost; swallow
                self._rx_expected = (seq + 1) % _codec.SEQ_MODULUS
                if end:
                    self.discarded += 1
                return
            if self._rx_expected is not None and seq != self._rx_expected:
                self._rx_poisoned = True
        self._rx_expected = (seq + 1) % _codec.SEQ_MODULUS

        if not self._rx_poisoned:
            self._rx_len += len(data)
            if self._rx_len > self.max_message_bytes:
                self._rx_poisoned = True
                self._rx_buf = []
            else:
                self._rx_buf.append(data)

        if not end:
            return
        self._rx_active = False
        buf = self._rx_buf
        self._rx_buf = []
        self._rx_len = 0
        if self._rx_poisoned:
            self._rx_poisoned = False
            self.discarded += 1
            return
        if len(buf) == 1:
            payload = buf[0]
        elif not buf:
            payload = b""
        else:
            payload = b"".join(buf)
        self._enqueue_inbound(payload, arrived_at)

    def _enqueue_inbound(self, payload: bytes, arrived_at: float | None = None) -> None:
        """Enqueue one complete message, evicting the oldest when over bounds.

        Safe from any thread (loopback delivers from the sender's
        thread). Never blocks.
        """
        if arrived_at is None:
            arrived_at = time.monotonic()
        wall = time.time()
        size = len(payload)
        with self._lock:
            if self.closed:
                return
            seq = self.rx_seq
            self.rx_seq += 1
            item = (payload, seq, arrived_at, wall, self.dropped)
            while True:
                over_bytes = (
                    self._queue_bytes > 0 and self._queue_bytes + size > self._queue_bytes_cap
                )
                if not over_bytes:
                    try:
                        self._queue.put_nowait(item)
                        self._queue_bytes += size
                        return
                    except queue.Full:
                        pass
                try:
                    old = self._queue.get_nowait()
                except queue.Empty:
                    # consumer drained concurrently; account and retry
                    self._queue_bytes = 0
                    continue
                if old is _SENTINEL:
                    # cannot happen while not closed; be defensive
                    self._queue.put_nowait(old)
                    return
                self._queue_bytes -= len(old[0])
                self.dropped += 1

    def _consumed(self, item: tuple) -> None:
        """Book-keeping hook called by :class:`Relay` after a successful get."""
        with self._lock:
            self._queue_bytes -= len(item[0])
            if self._queue_bytes < 0:
                self._queue_bytes = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _mark_opened(self, tx_lane_id: int) -> None:
        """Finalize the open handshake (called by the carrier)."""
        self.tx_lane_id = tx_lane_id
        self._opened.set()

    def _close_local(self, reason: str) -> bool:
        """Close without notifying the peer. Idempotent; returns ``True`` on first close.

        Wakes a blocked :class:`Relay` by enqueuing the sentinel (evicting
        one message if the queue is full so the wake-up cannot be lost).
        """
        with self._lock:
            if self.closed:
                return False
            self.closed = True
            self.closed_reason = reason
            self._opened.set()
            while True:
                try:
                    self._queue.put_nowait(_SENTINEL)
                    break
                except queue.Full:
                    try:
                        old = self._queue.get_nowait()
                    except queue.Empty:
                        continue
                    if old is not _SENTINEL:
                        self._queue_bytes -= len(old[0])
                        self.dropped += 1
            self._rx_buf = []
            self._rx_len = 0
            self._rx_active = False
        return True

    def close(self, reason: str = "closed locally") -> None:
        """Close this channel and tell the peer (best effort). Idempotent.

        The peer's matching channel ends its relay with ``StopIteration``.
        Pending inbound messages are dropped; a blocked relay wakes up.
        """
        proto = self.protocol
        if not self._close_local(reason):
            return
        if proto is not None:
            try:
                proto._on_channel_closed_locally(self)
            except Exception:
                pass

    def relay(self, timeout: float | None = None) -> Relay:
        """Return the channel's single :class:`Relay`, updating its *timeout*.

        See :func:`laila.relay`. Two threads iterating the same relay
        split the packets between them (it is one queue).
        """
        with self._lock:
            if self._relay is None:
                self._relay = Relay(self, timeout=timeout)
            else:
                self._relay.timeout = timeout
            return self._relay


class Relay:
    """Blocking iterator over one :class:`Channel` yielding :class:`StreamEntry`.

    Returned by :func:`laila.relay` / :meth:`Channel.relay`. Runs on the
    caller's thread; no futures, no taskforce, no threads spawned by
    laila.

    Parameters
    ----------
    channel : Channel
        Source channel.
    timeout : float, optional
        Per-message wait. ``None`` waits until a message arrives or the
        channel closes; otherwise ``TimeoutError`` is raised when no
        message arrives within *timeout* seconds.

    Notes
    -----
    - Iteration ends (``StopIteration``) when the channel closes: peer
      loss, remote ``close()``, local ``close()``, carrier/communication
      ``stop()`` or :func:`laila.terminate`. Messages already buffered are
      drained first. :attr:`closed_reason` says why.
    - The wait loop wakes at least once a second to re-check the closed
      flag, so a missed sentinel or a Ctrl-C cannot strand the consumer.
    - After a peer reconnects this relay stays finished; re-index
      ``laila.peers[gid][name]`` and call :func:`laila.relay` again.
    """

    __slots__ = ("channel", "timeout")

    def __init__(self, channel: Channel, timeout: float | None = None) -> None:
        self.channel = channel
        self.timeout = timeout

    @property
    def closed_reason(self) -> str | None:
        """Why the underlying channel closed (``None`` while open)."""
        return self.channel.closed_reason

    def __iter__(self) -> Relay:
        return self

    def __next__(self) -> StreamEntry:
        ch = self.channel
        q = ch._queue
        timeout = self.timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if ch.closed and q.empty():
                raise StopIteration
            if deadline is None:
                wait = 1.0
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"No message on channel {ch.name!r} from {ch.peer_id} within {timeout}s."
                    )
                wait = min(1.0, remaining)
            try:
                item = q.get(timeout=wait)
            except queue.Empty:
                continue
            if item is _SENTINEL:
                raise StopIteration
            ch._consumed(item)
            return self._wrap(item)

    def _wrap(self, item: tuple) -> StreamEntry:
        """Turn a queued ``(payload, seq, arrived_at, wall, dropped_before)`` into an entry.

        The single place where stream identity is minted (see
        :class:`StreamEntry` notes).
        """
        payload, seq, arrived_at, wall, dropped_before = item
        ch = self.channel
        entry = StreamEntry._from_payload(payload)
        entry._stream = StreamMeta(
            peer_id=ch.peer_id,
            channel=ch.name,
            lane=ch.lane_id,
            seq=seq,
            arrived_at=arrived_at,
            arrived_wall=wall,
            dropped_before=dropped_before,
        )
        return entry

    def __repr__(self) -> str:
        return f"Relay({self.channel!r}, timeout={self.timeout!r})"
