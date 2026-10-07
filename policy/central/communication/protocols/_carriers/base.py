"""Shared base for all RPC carriers.

:class:`_CarrierRPCProtocol` factors out everything that is identical
across transports regardless of the underlying wire:

- **Config fields** common to every carrier: the wire ``codec``, the
  blocking ``rpc_timeout`` and ``handshake_timeout``, and the
  ``peer_secret_key`` presented during the handshake.
- **Peer bookkeeping**: a ``_connections`` map (``peer_id`` -> opaque
  transport handle) plus the two-tier ``_register_peer`` /
  ``_unregister_peer`` that also notify the owning
  :class:`_LAILA_IDENTIFIABLE_COMMUNICATION` so a
  :class:`RemotePolicyProxy` is created/destroyed.
- **Outbound correlation**: a ``_pending_rpcs`` table and the
  :meth:`_register_pending` / :meth:`_complete_pending` /
  :meth:`_await_pending` trio that let a synchronous ``send_rpc`` block
  on a :class:`threading.Event` until the matching response frame
  arrives on the carrier's I/O thread.
- **Inbound dispatch**: :meth:`_build_response` runs the actual
  ``_execute_rpc`` on a worker thread (never the I/O loop) so a blocking
  remote call -- e.g. ``_wait_future`` -- cannot stall the transport.
- **Stream lanes**: the per-peer lane tables behind
  ``laila.peers[gid][name]`` -- :meth:`open_channel`, the
  ``__comm_channel_open__`` / ``__comm_channel_close__`` control frames
  (intercepted exactly like ``__comm_ping__``: before admission, before
  ``_execute_rpc``), :meth:`_on_stream_frame` reassembly, and the
  close-all-lanes step that runs before a peer is unregistered. Carriers
  that can stream set :attr:`supports_channels` and implement
  :meth:`_stream_enqueue`.

Concrete carriers (:class:`._stream._StreamRPCProtocol`,
:class:`._datagram._DatagramRPCProtocol`) build their wire-specific
logic on top of these helpers.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ... import protocol as rpc_protocol
from ...channel import Channel
from ..base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from . import codec as _codec

log = logging.getLogger(__name__)

#: Reserved dotted-path for the liveness ping control frame. Intercepted
#: in :meth:`_build_response` before it can reach the policy.
_COMM_PING_PATH = ["__comm_ping__"]
#: Reserved dotted-paths for stream-lane control. Same interception point
#: as the ping: answered on the I/O thread before admission / executor.
_COMM_CHANNEL_OPEN_PATH = ["__comm_channel_open__"]
_COMM_CHANNEL_CLOSE_PATH = ["__comm_channel_close__"]
#: Highest allocatable lane id; ``0`` is reserved for RPC/control.
_MAX_LANE = 255

#: Peering control methods (top-level JSON-RPC ``method`` values).
_PEER_CONNECT = "peer.connect"
#: Notification a side sends right before dropping a peer / stopping, so
#: the other side unregisters it at once rather than on liveness timeout.
_PEER_DISCONNECT = "peer.disconnect"
#: Upper bound on how long ``disconnect()`` waits for the goodbye to go out.
_GOODBYE_TIMEOUT = 1.0


class BackpressureError(RuntimeError):
    """Raised when a peer rejected an RPC with ``ERR_BUSY``.

    Distinct from a generic remote error so the sender's retry loop can
    scope exponential backoff to overload only, and surface a clear final
    error if the peer stays saturated.
    """


class _CarrierRPCProtocol(_LAILA_IDENTIFIABLE_COMM_PROTOCOL):
    """Abstract carrier holding wire-agnostic RPC machinery.

    This class is never registered directly -- it has no
    :attr:`protocol_name` of its own. Subclasses set
    :attr:`protocol_name`, the URI/token routing, and the transport
    endpoint factories.

    Parameters
    ----------
    codec : str, default ``"json"``
        Wire serialisation, one of :data:`._codec.CODECS`. ``"msgpack"``
        is far more compact for bandwidth-constrained links.
    rpc_timeout : float, default ``60.0``
        Seconds a blocking :meth:`send_rpc` waits for the response.
    handshake_timeout : float, default ``10.0``
        Seconds the peering handshake waits for the remote reply.
    peer_secret_key : str
        Shared secret a remote peer must present during the handshake.
        Defaults to a fresh UUID4 hex.
    channel_queue_size : int, default ``256``
        Max *messages* buffered per inbound stream lane before the oldest
        is evicted (``Channel.dropped``).
    channel_queue_bytes : int, default ``64 MiB``
        Max *bytes* buffered per inbound stream lane (same eviction).
        Bounds memory for lanes nobody is relaying from.
    stream_chunk_bytes : int, default ``65536``
        Outbound stream messages are sliced into chunks of this size so
        RPC/ping frames can interleave between chunks. Lower it on slow
        links (serial carriers default to ``4096``): worst-case RPC delay
        is roughly ``2 * stream_chunk_bytes / link bandwidth``.
    max_stream_frame_bytes : int, default ``64 MiB``
        Largest single stream message accepted in either direction
        (``send()`` raises ``ValueError``; oversized inbound messages are
        discarded). Independent of :data:`codec.MAX_FRAME_BYTES`.
    static_lanes : dict[str, int]
        ``{channel_name: lane_id}`` bound at peer registration without a
        control handshake. Strictly for peers that emit lane frames but
        cannot answer ``__comm_channel_open__`` (e.g. MCU firmware). Never
        configure it against an older laila peer: it would feed ``0x01``
        frames to a receiver that treats them as RPC and drops the link.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    codec: str = Field(default="json")
    rpc_timeout: float = Field(default=60.0)
    handshake_timeout: float = Field(default=10.0)
    ping_timeout: float = Field(default=5.0)
    peer_secret_key: str = Field(default_factory=lambda: _uuid.uuid4().hex)
    #: Sender-side backoff when a peer replies ``ERR_BUSY`` (backpressure).
    rpc_backoff_base: float = Field(default=0.05)
    rpc_backoff_max: float = Field(default=5.0)
    max_rpc_retries: int = Field(default=5)

    #: Stream-lane tuning (inert on carriers with ``supports_channels=False``).
    channel_queue_size: int = Field(default=256)
    channel_queue_bytes: int = Field(default=64 * 1024 * 1024)
    stream_chunk_bytes: int = Field(default=65536)
    max_stream_frame_bytes: int = Field(default=64 * 1024 * 1024)
    static_lanes: dict[str, int] = Field(default_factory=dict)

    #: Carriers are abstract; concrete transports override.
    protocol_name: ClassVar[str] = "carrier"

    _started: bool = PrivateAttr(default=False)
    _connections: dict[str, Any] = PrivateAttr(default_factory=dict)
    _pending_rpcs: dict[str, dict[str, Any]] = PrivateAttr(default_factory=dict)
    _inbound_executor: Any = PrivateAttr(default=None)
    # per-thread RPC wait override (set by ping(); see _await_pending)
    _rpc_wait_override: Any = PrivateAttr(default_factory=threading.local)

    # stream-lane state: peer -> rx-lane -> Channel, peer -> name -> Channel
    _lanes: dict[str, dict[int, Channel]] = PrivateAttr(default_factory=dict)
    _lane_names: dict[str, dict[str, Channel]] = PrivateAttr(default_factory=dict)
    _lane_cursor: dict[str, int] = PrivateAttr(default_factory=dict)
    _peer_caps: dict[str, dict] = PrivateAttr(default_factory=dict)
    _lane_lock: Any = PrivateAttr(default_factory=threading.Lock)
    _unknown_lane_drops: int = PrivateAttr(default=0)
    _reserved_marker_drops: int = PrivateAttr(default=0)
    _malformed_stream_drops: int = PrivateAttr(default=0)

    # ------------------------------------------------------------------
    # Inbound dispatch (runs the real call off the I/O thread)
    # ------------------------------------------------------------------

    def _ensure_executor(self) -> ThreadPoolExecutor:
        """Lazily create the worker pool used to run inbound RPCs."""
        if self._inbound_executor is None:
            self._inbound_executor = ThreadPoolExecutor(
                max_workers=8,
                thread_name_prefix=f"{type(self).__name__}-inbound",
            )
        return self._inbound_executor

    def _build_response(self, msg: dict, peer_id: str | None = None) -> dict:
        """Execute an inbound ``rpc.call`` *msg* and return a response dict.

        Runs synchronously on the calling thread (carriers call this from
        a worker thread, never the I/O loop). Unknown methods and
        execution failures are mapped to JSON-RPC error envelopes.

        *peer_id* is the authenticated sender (known to the carrier from
        the connection). It is only needed by the stream-lane control
        frames, which carriers normally intercept earlier in
        :meth:`_handle_request_frame`; it is threaded here too so a
        carrier that dispatches directly (loopback) still gets them.
        """
        request_id = msg.get("id")
        method = msg.get("method")
        if method != "rpc.call":
            return rpc_protocol.make_error(
                request_id,
                rpc_protocol.ERR_METHOD_NOT_FOUND,
                f"Unknown method: {method}",
            )
        params = msg.get("params", {})
        path = params.get("path", [])
        args = params.get("args", [])
        kwargs = params.get("kwargs", {})
        # Liveness control frame: answered here, before the policy/worker
        # pool, so a ping never touches central.memory or the executor.
        if path == _COMM_PING_PATH:
            return rpc_protocol.make_result(request_id, "pong")
        if self._is_control_path(path):
            return self._control_response(msg, peer_id)
        try:
            result = self._communication._execute_rpc(path, args, kwargs)
            return rpc_protocol.make_result(request_id, result)
        except Exception as exc:
            return rpc_protocol.make_error(
                request_id,
                rpc_protocol.ERR_EXECUTION,
                f"{type(exc).__name__}: {exc}",
            )

    # ------------------------------------------------------------------
    # Inbound admission control (per-policy backpressure)
    # ------------------------------------------------------------------

    @staticmethod
    def _is_ping_frame(msg: dict) -> bool:
        """``True`` for the reserved liveness ``__comm_ping__`` request.

        Pings must bypass admission entirely: a busy-but-alive peer must
        never be wrongly dropped by the liveness loop just because its
        RPC queue is full.
        """
        if msg.get("method") != "rpc.call":
            return False
        return msg.get("params", {}).get("path", []) == _COMM_PING_PATH

    def _try_admit(self) -> bool:
        """Try to claim an inbound-RPC slot from the hub. Non-blocking.

        Returns ``True`` if admitted (caller must later
        :meth:`_release_admit`), ``False`` if the policy is at capacity
        and the request should be rejected with ``ERR_BUSY``. When no hub
        is attached (e.g. unit-testing a bare carrier) admission always
        succeeds.
        """
        comm = self._communication
        if comm is None:
            return True
        return comm._acquire_rpc_slot()

    def _release_admit(self) -> None:
        """Release a previously-claimed inbound-RPC slot."""
        comm = self._communication
        if comm is not None:
            comm._release_rpc_slot()

    def _busy_response(self, msg: dict) -> dict:
        """Build the ``ERR_BUSY`` envelope for a rejected inbound request."""
        return rpc_protocol.make_error(
            msg.get("id"),
            rpc_protocol.ERR_BUSY,
            f"{self.protocol_name!r} peer is busy: inbound RPC queue at capacity.",
        )

    def _handle_request_frame(self, msg: dict, reply, peer_id: str | None = None) -> None:
        """Centralized inbound dispatch: ping fast-path + admission + queue.

        ``reply(resp_dict)`` sends a response back over the transport and
        must be safe to invoke from a worker thread. *peer_id* is the
        authenticated sender; carriers pass it so stream-lane control
        frames are keyed by connection, never by a wire-supplied id.

        1. A liveness ping is answered inline, *before* admission, so it
           never queues and never gets rejected.
        2. Stream-lane control frames (``__comm_channel_open__`` /
           ``__comm_channel_close__``) are answered inline the same way:
           they never take an admission slot and never reach the policy.
        3. Otherwise a slot is claimed non-blocking; past capacity the
           request is rejected immediately with ``ERR_BUSY`` (it never
           enters the executor backlog, so the backlog stays bounded).
        4. Admitted requests run :meth:`_build_response` on a worker
           thread, releasing the slot when the reply is ready.
        """
        if self._is_ping_frame(msg):
            reply(rpc_protocol.make_result(msg.get("id"), "pong"))
            return
        if msg.get("method") == "rpc.call" and self._is_control_path(
            msg.get("params", {}).get("path", [])
        ):
            reply(self._control_response(msg, peer_id))
            return
        if not self._try_admit():
            reply(self._busy_response(msg))
            return

        def _work() -> None:
            try:
                resp = self._build_response(msg)
            finally:
                self._release_admit()
            reply(resp)

        self._ensure_executor().submit(_work)

    # ------------------------------------------------------------------
    # Peer registry (two-tier)
    # ------------------------------------------------------------------

    def _register_peer(self, peer_id: str, handle: Any) -> None:
        """Record a live transport *handle* for *peer_id* and notify the hub.

        The communication hub creates the corresponding
        :class:`PeerProxy` so user code can immediately reach the new
        peer. Afterwards every :attr:`static_lanes` entry is pre-bound so
        early stream frames are buffered instead of dropped as
        unknown-lane.
        """
        self._connections[peer_id] = handle
        if self._communication is not None:
            self._communication._register_peer(peer_id)
        self._bind_static_lanes(peer_id)

    def _unregister_peer(self, peer_id: str) -> None:
        """Drop *peer_id*: close its lanes, drop the handle, notify the hub. Idempotent.

        Lanes are closed *first* (waking every relay with a sentinel) so
        this single place covers liveness drops, receive-loop EOF,
        ``remove_peer``, ``protocol.stop()``, ``comm.stop()`` and
        ``remove_connection``.
        """
        self._close_peer_lanes(peer_id, "peer disconnected")
        self._peer_caps.pop(peer_id, None)
        self._lane_cursor.pop(peer_id, None)
        self._connections.pop(peer_id, None)
        if self._communication is not None:
            self._communication._unregister_peer(peer_id)

    def has_peer(self, peer_id: str) -> bool:
        """Return ``True`` if a live connection to *peer_id* is held."""
        return peer_id in self._connections

    def disconnect(self, peer_id: str) -> None:
        """Gracefully drop a single peer. Idempotent.

        Sends the ``peer.disconnect`` notification (bounded, best effort)
        so the remote unregisters us immediately, then unregisters the
        peer locally -- which closes its lanes and, on carriers that hold
        a closable handle, the handle itself (:meth:`_unregister_peer`).
        """
        if peer_id not in self._connections:
            return
        self._goodbye_threadsafe(peer_id)
        self._unregister_peer(peer_id)

    # ------------------------------------------------------------------
    # Graceful goodbye (``peer.disconnect``)
    # ------------------------------------------------------------------

    def _goodbye_message(self) -> dict:
        """The ``peer.disconnect`` notification this endpoint sends."""
        policy_id = self._communication.policy_id if self._communication else None
        return rpc_protocol.make_notification(_PEER_DISCONNECT, {"from_id": policy_id})

    @staticmethod
    def _is_goodbye(msg: dict) -> bool:
        """``True`` for an inbound ``peer.disconnect`` notification."""
        return msg.get("method") == _PEER_DISCONNECT

    async def _send_goodbye(self, peer_id: str) -> None:
        """Write the goodbye to *peer_id* and wait (bounded) for it to leave.

        Runs on the carrier loop. Default no-op; wire carriers override
        with their own write primitive. Must never raise.
        """
        return None

    async def _goodbye_all(self) -> None:
        """Say goodbye to every registered peer (first step of ``stop()``)."""
        for peer_id in list(self._connections):
            try:
                await asyncio.wait_for(self._send_goodbye(peer_id), timeout=_GOODBYE_TIMEOUT)
            except Exception:
                log.debug("goodbye to %s failed", peer_id, exc_info=True)

    def _goodbye_threadsafe(self, peer_id: str) -> None:
        """Run :meth:`_send_goodbye` from a user thread, bounded; never raises."""
        loop = getattr(self, "_event_loop", None)
        if loop is None or loop.is_closed() or not loop.is_running():
            return
        try:
            if asyncio.get_running_loop() is loop:
                # On the loop thread we cannot block; the caller is tearing
                # the connection down right after, so skip the goodbye.
                return
        except RuntimeError:
            pass
        try:
            fut = asyncio.run_coroutine_threadsafe(self._send_goodbye(peer_id), loop)
            fut.result(timeout=_GOODBYE_TIMEOUT)
        except Exception:
            log.debug("goodbye to %s failed", peer_id, exc_info=True)

    def _on_peer_goodbye(self, peer_id: str | None) -> None:
        """Inbound ``peer.disconnect`` from an authenticated *peer_id*: drop it."""
        if peer_id is not None and peer_id in self._connections:
            self._unregister_peer(peer_id)

    def _loop_call(self, fn: Any, *args: Any) -> None:
        """``call_soon_threadsafe`` on the carrier loop, or ``ConnectionError``.

        The single place user-thread sends cross into the loop thread, so
        a send after ``stop()`` (loop gone or closed) always surfaces as a
        clear :class:`ConnectionError` rather than an ``AttributeError``
        or asyncio's ``RuntimeError``.
        """
        loop = getattr(self, "_event_loop", None)
        if loop is None or loop.is_closed():
            raise ConnectionError(f"{self.protocol_name!r} transport is shut down.")
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError as exc:
            raise ConnectionError(f"{self.protocol_name!r} transport is shut down.") from exc

    # ------------------------------------------------------------------
    # Stream lanes
    # ------------------------------------------------------------------

    def _local_caps(self) -> dict:
        """Capabilities advertised in ``peer.connect`` (``{"lanes": 1}`` if streaming)."""
        return {"lanes": 1} if type(self).supports_channels else {}

    def _set_peer_caps(self, peer_id: str, caps: Any) -> None:
        """Remember the capabilities a peer advertised during the handshake.

        Call *before* :meth:`_register_peer` so a user opening a channel
        right after the handshake already sees them. Older peers send no
        ``caps`` at all; that is recorded as ``{}``.
        """
        self._peer_caps[peer_id] = dict(caps) if isinstance(caps, dict) else {}

    def _peer_has_lanes(self, peer_id: str) -> bool:
        """``True`` if *peer_id* advertised ``caps.lanes`` during the handshake."""
        return bool(self._peer_caps.get(peer_id, {}).get("lanes"))

    def channel_names(self, peer_id: str) -> list[str]:
        """Names of the lanes currently open to *peer_id* on this carrier."""
        with self._lane_lock:
            return [n for n, ch in self._lane_names.get(peer_id, {}).items() if not ch.closed]

    def _new_channel(
        self, peer_id: str, name: str, lane: int, tx_lane: int | None = None
    ) -> Channel:
        """Create and bind a :class:`Channel`. Caller holds :attr:`_lane_lock`."""
        ch = Channel(
            self,
            peer_id,
            name,
            lane,
            queue_size=self.channel_queue_size,
            queue_bytes=self.channel_queue_bytes,
            max_message_bytes=self._max_message_bytes(),
            tx_lane_id=tx_lane,
        )
        self._lanes.setdefault(peer_id, {})[lane] = ch
        self._lane_names.setdefault(peer_id, {})[name] = ch
        return ch

    def _max_message_bytes(self) -> int:
        """Largest stream message this carrier accepts (datagram carriers lower it)."""
        return int(self.max_stream_frame_bytes)

    def _alloc_lane(self, peer_id: str, preferred: int | None = None) -> int:
        """Pick a free rx lane id for *peer_id*. Caller holds :attr:`_lane_lock`.

        Prefers *preferred* (the opener's own lane id, so both sides
        usually share one number); otherwise walks round-robin so an id
        freed by ``close()`` is not reused immediately while late frames
        for it may still be in flight.
        """
        lanes = self._lanes.setdefault(peer_id, {})
        if preferred is not None and 1 <= preferred <= _MAX_LANE and preferred not in lanes:
            return preferred
        cursor = self._lane_cursor.get(peer_id, 0)
        for _ in range(_MAX_LANE):
            cursor = cursor % _MAX_LANE + 1
            if cursor not in lanes:
                self._lane_cursor[peer_id] = cursor
                return cursor
        raise ConnectionError(f"All {_MAX_LANE} stream lanes to peer {peer_id} are in use.")

    def _drop_channel(self, ch: Channel) -> None:
        """Remove *ch* from the lane tables. Caller holds :attr:`_lane_lock`."""
        lanes = self._lanes.get(ch.peer_id)
        if lanes is not None and lanes.get(ch.lane_id) is ch:
            lanes.pop(ch.lane_id, None)
        names = self._lane_names.get(ch.peer_id)
        if names is not None and names.get(ch.name) is ch:
            names.pop(ch.name, None)

    def open_channel(self, peer_id: str, name: str) -> Channel:
        """Return the (cached) :class:`Channel` *name* to *peer_id*, opening it if needed.

        Idempotent and race-safe: if both sides open the same name at the
        same time they converge on one channel per side. The lane lock is
        never held across the network round trip (see Notes).

        Parameters
        ----------
        peer_id : str
            A peer this carrier currently holds.
        name : str
            Channel name; ``"default"`` is reserved for RPC.

        Raises
        ------
        ConnectionError
            If the carrier cannot stream, does not hold *peer_id*, the
            peer advertised no lanes and no :attr:`static_lanes` entry
            exists for *name*, or the open handshake failed.
        ValueError
            If *name* is ``"default"``.

        Notes
        -----
        Over loopback the peer's control handler runs synchronously on
        *this* thread, and on wire carriers it runs on the loop thread
        that must also read our reply -- holding the lock across the RPC
        would deadlock either way. The channel is therefore inserted in a
        *pending* state, the RPC is sent lock-free, and the result is
        committed under the lock afterwards.
        """
        if name == "default":
            raise ValueError("'default' is the RPC proxy, not a stream channel.")
        if not type(self).supports_channels:
            raise ConnectionError(f"{self.protocol_name!r} has no stream lanes.")
        if not self.has_peer(peer_id):
            raise ConnectionError(f"No connection to peer {peer_id}")

        opener = False
        with self._lane_lock:
            ch = self._lane_names.get(peer_id, {}).get(name)
            if ch is not None and ch.closed:
                self._drop_channel(ch)
                ch = None
            if ch is None:
                static = self.static_lanes.get(name)
                if static is not None:
                    lane = int(static)
                    if not 1 <= lane <= _MAX_LANE:
                        raise ConnectionError(
                            f"static_lanes[{name!r}]={lane} is outside 1..{_MAX_LANE}."
                        )
                    other = self._lanes.get(peer_id, {}).get(lane)
                    if other is not None and not other.closed:
                        raise ConnectionError(
                            f"Lane {lane} to peer {peer_id} is already bound to channel "
                            f"{other.name!r}."
                        )
                    return self._new_channel(peer_id, name, lane, tx_lane=lane)
                if not self._peer_has_lanes(peer_id):
                    raise ConnectionError(
                        f"Peer {peer_id} did not advertise stream lanes during the handshake "
                        "(an older laila or an RPC-only firmware). If it emits lane frames on "
                        f"a fixed lane, configure static_lanes={{{name!r}: <lane>}} on this "
                        "connection."
                    )
                lane = self._alloc_lane(peer_id)
                ch = self._new_channel(peer_id, name, lane)
                opener = True
            pending = ch.tx_lane_id is None

        if not pending:
            return ch
        if not opener:
            # someone else's open is in flight; wait for it
            if ch._opened.wait(timeout=self.rpc_timeout) and not ch.closed:
                return ch
            raise ConnectionError(
                f"Channel {name!r} to peer {peer_id} did not finish opening "
                f"({ch.closed_reason or 'timeout'})."
            )

        try:
            result = self.send_rpc(
                peer_id,
                list(_COMM_CHANNEL_OPEN_PATH),
                (),
                {"name": name, "lane": ch.lane_id, "options": {}},
            )
            tx_lane = int(result["lane"])
            if not 1 <= tx_lane <= _MAX_LANE:
                raise ValueError(f"peer returned lane {tx_lane}")
        except Exception as exc:
            with self._lane_lock:
                if ch.tx_lane_id is not None and not ch.closed:
                    # a remote open for the same name raced us and finished first
                    return ch
                self._drop_channel(ch)
            ch._close_local(f"open failed: {exc}")
            raise ConnectionError(
                f"Could not open channel {name!r} to peer {peer_id}: {exc}"
            ) from exc

        with self._lane_lock:
            if ch.closed:
                raise ConnectionError(
                    f"Channel {name!r} to peer {peer_id} closed while opening ({ch.closed_reason})."
                )
            if ch.tx_lane_id is None:
                ch._mark_opened(tx_lane)
        return ch

    def _bind_static_lanes(self, peer_id: str) -> None:
        """Pre-bind every :attr:`static_lanes` entry for a freshly registered peer."""
        if not self.static_lanes or not type(self).supports_channels:
            return
        with self._lane_lock:
            for name, lane in self.static_lanes.items():
                lane = int(lane)
                if not 1 <= lane <= _MAX_LANE or name == "default":
                    log.debug("Ignoring invalid static lane %r=%r", name, lane)
                    continue
                if lane in self._lanes.get(peer_id, {}):
                    continue
                if name in self._lane_names.get(peer_id, {}):
                    continue
                self._new_channel(peer_id, name, lane, tx_lane=lane)

    def _close_peer_lanes(self, peer_id: str, reason: str) -> None:
        """Close every lane to *peer_id* (waking relays). Idempotent."""
        with self._lane_lock:
            lanes = self._lanes.pop(peer_id, None) or {}
            names = self._lane_names.pop(peer_id, None) or {}
        seen: set[int] = set()
        for ch in list(lanes.values()) + list(names.values()):
            if id(ch) in seen:
                continue
            seen.add(id(ch))
            ch._close_local(reason)

    def _shutdown_lanes(self, reason: str = "protocol stopped") -> None:
        """Close every lane on every peer (used by carriers' ``stop()``)."""
        with self._lane_lock:
            peers = set(self._lanes) | set(self._lane_names)
        for peer_id in peers:
            self._close_peer_lanes(peer_id, reason)

    def _is_control_path(self, path: Any) -> bool:
        """``True`` for the reserved stream-lane control paths."""
        return path == _COMM_CHANNEL_OPEN_PATH or path == _COMM_CHANNEL_CLOSE_PATH

    def _control_response(self, msg: dict, peer_id: str | None) -> dict:
        """Answer a stream-lane control request; never touches the policy."""
        params = msg.get("params", {})
        try:
            result = self._handle_control(
                params.get("path", []), params.get("kwargs", {}) or {}, peer_id
            )
            return rpc_protocol.make_result(msg.get("id"), result)
        except Exception as exc:
            return rpc_protocol.make_error(
                msg.get("id"),
                rpc_protocol.ERR_EXECUTION,
                f"{type(exc).__name__}: {exc}",
            )

    def _handle_control(self, path: list, kwargs: dict, peer_id: str | None) -> Any:
        """Allocate / release a lane on behalf of the authenticated *peer_id*.

        ``__comm_channel_open__`` kwargs ``{"name", "lane", "options"}`` ->
        ``{"lane": <our rx lane>}``. ``lane`` is the opener's rx lane (what
        we will write in frames to it); ``options`` is reserved and
        ignored. ``__comm_channel_close__`` kwargs ``{"lane"}`` (our rx
        lane) -> ``True``.
        """
        if peer_id is None:
            raise RuntimeError("Stream-lane control requires an authenticated peer.")
        if not type(self).supports_channels:
            raise ConnectionError(f"{self.protocol_name!r} has no stream lanes.")
        if path == _COMM_CHANNEL_OPEN_PATH:
            name = kwargs.get("name")
            if not isinstance(name, str) or not name or name == "default":
                raise ValueError(f"Invalid channel name {name!r}.")
            remote_lane = int(kwargs.get("lane", 0))
            if not 1 <= remote_lane <= _MAX_LANE:
                raise ValueError(f"Invalid lane {remote_lane}.")
            with self._lane_lock:
                ch = self._lane_names.get(peer_id, {}).get(name)
                if ch is not None and ch.closed:
                    self._drop_channel(ch)
                    ch = None
                if ch is not None:
                    # Either our own open is in flight (adopt the peer's
                    # lane) or the peer re-opened after a lost close.
                    if ch.tx_lane_id is None:
                        ch._mark_opened(remote_lane)
                    else:
                        ch.tx_lane_id = remote_lane
                    return {"lane": ch.lane_id}
                lane = self._alloc_lane(peer_id, preferred=remote_lane)
                ch = self._new_channel(peer_id, name, lane, tx_lane=remote_lane)
                return {"lane": lane}
        if path == _COMM_CHANNEL_CLOSE_PATH:
            lane = int(kwargs.get("lane", 0))
            with self._lane_lock:
                ch = self._lanes.get(peer_id, {}).get(lane)
                if ch is not None:
                    self._drop_channel(ch)
            if ch is not None:
                ch._close_local("closed by peer")
            return True
        raise ValueError(f"Unknown control path {path!r}.")

    def _on_channel_closed_locally(self, ch: Channel) -> None:
        """User called ``Channel.close()``: unbind and tell the peer (best effort)."""
        with self._lane_lock:
            self._drop_channel(ch)
        if ch.tx_lane_id is None or not self.has_peer(ch.peer_id):
            return
        try:
            self._send_oneway(ch.peer_id, list(_COMM_CHANNEL_CLOSE_PATH), {"lane": ch.tx_lane_id})
        except Exception:
            log.debug("channel close notice to %s failed", ch.peer_id, exc_info=True)

    def _send_oneway(self, peer_id: str, path: list, kwargs: dict) -> None:
        """Send a control request without waiting for its reply.

        Default: fire ``send_rpc`` on a daemon thread and discard the
        outcome. Carriers with a cheap non-blocking write path override.
        """

        def _go() -> None:
            try:
                self.send_rpc(peer_id, path, (), kwargs)
            except Exception:
                pass

        threading.Thread(target=_go, daemon=True, name="comm-channel-close").start()

    def _stream_enqueue(self, channel: Channel, payload: bytes) -> None:
        """Hand one outbound message to the wire. Carriers override.

        Called on the user's thread by :meth:`Channel.send`; must not
        block on I/O (marshal onto the carrier loop instead).
        """
        raise ConnectionError(f"{self.protocol_name!r} cannot carry stream lanes.")

    def _lookup_lane(self, peer_id: str | None, lane: int) -> Channel | None:
        """Resolve a receive-side lane id to its channel (``None`` if unknown)."""
        if peer_id is None:
            return None
        lanes = self._lanes.get(peer_id)
        if lanes is None:
            return None
        return lanes.get(lane)

    def _on_stream_frame(self, peer_id: str | None, raw: bytes) -> None:
        """Route one reserved-marker frame from *peer_id* (carrier inbound thread).

        Any failure is counted and logged at debug; it never propagates,
        so a malformed stream frame cannot end a receive loop or
        unregister a peer. Unknown lanes and unknown reserved markers are
        dropped silently.
        """
        try:
            if not _codec.is_stream_frame(raw):
                self._reserved_marker_drops += 1
                log.debug("dropping frame with reserved marker 0x%02x", raw[0] if raw else -1)
                return
            lane, seq, flags = _codec.unpack_stream_header(raw)
            ch = self._lookup_lane(peer_id, lane)
            if ch is None or ch.closed:
                self._unknown_lane_drops += 1
                return
            ch._on_chunk(seq, flags, raw[_codec.STREAM_HEADER_LEN :], time.monotonic())
        except Exception:
            self._malformed_stream_drops += 1
            log.debug("malformed stream frame from %s dropped", peer_id, exc_info=True)

    def _on_stream_payload(self, peer_id: str, lane: int, payload: bytes) -> None:
        """Deliver an already-complete message to our rx *lane* (loopback path)."""
        ch = self._lookup_lane(peer_id, lane)
        if ch is None or ch.closed:
            self._unknown_lane_drops += 1
            return
        ch._enqueue_inbound(payload)

    def _stream_chunks(self, channel: Channel, payload: bytes):
        """Yield the wire frames (header + data) for one outbound message.

        Slices *payload* into :attr:`stream_chunk_bytes` pieces with
        ``START`` on the first and ``END`` on the last; ``b""`` yields one
        empty ``START|END`` chunk.
        """
        size = max(1, int(self.stream_chunk_bytes))
        n = len(payload)
        count = max(1, -(-n // size))
        first = channel._next_chunk_seqs(count)
        lane = channel.tx_lane_id
        if count == 1:
            yield _codec.pack_stream_frame(
                lane, first, _codec.FLAG_START | _codec.FLAG_END, payload
            )
            return
        for i in range(count):
            flags = 0
            if i == 0:
                flags |= _codec.FLAG_START
            if i == count - 1:
                flags |= _codec.FLAG_END
            yield _codec.pack_stream_frame(
                lane, first + i, flags, payload[i * size : (i + 1) * size]
            )

    def ping(self, peer_id: str, timeout: float | None = None) -> bool:
        """Round-trip a liveness control frame to *peer_id*.

        Reuses the carrier's own :meth:`send_rpc` path with the reserved
        ``__comm_ping__`` frame, which the peer answers with ``"pong"``
        before it ever reaches the policy. Returns ``False`` on any
        transport error. The communication liveness loop bounds the call
        with its own deadline, so a silently-dead peer cannot stall it.
        """
        if not self.has_peer(peer_id):
            return False
        # Bound the *wait* by the ping deadline (not rpc_timeout): a deaf
        # peer must not pin a liveness worker for 60 s.
        self._rpc_wait_override.timeout = timeout if timeout is not None else self.ping_timeout
        try:
            return self.send_rpc(peer_id, list(_COMM_PING_PATH), (), {}) == "pong"
        except Exception:
            return False
        finally:
            self._rpc_wait_override.timeout = None

    # ------------------------------------------------------------------
    # Outbound correlation
    # ------------------------------------------------------------------

    def _register_pending(self) -> tuple[str, dict]:
        """Allocate a request id + pending slot for an outbound RPC."""
        request_id = str(_uuid.uuid4())
        slot: dict[str, Any] = {"event": threading.Event()}
        self._pending_rpcs[request_id] = slot
        return request_id, slot

    def _complete_pending(self, msg: dict) -> None:
        """Resolve the pending slot named by response *msg*'s id."""
        request_id = msg.get("id")
        if request_id is None:
            return
        slot = self._pending_rpcs.get(request_id)
        if slot is None:
            return
        if "error" in msg:
            slot["error"] = msg["error"]
        else:
            slot["result"] = msg.get("result")
        slot["event"].set()

    def _await_pending(self, request_id: str, slot: dict) -> Any:
        """Block until *slot* is resolved (or :attr:`rpc_timeout` elapses).

        :meth:`ping` shortens the wait for the calling thread only via the
        thread-local ``_rpc_wait_override``.
        """
        timeout = getattr(self._rpc_wait_override, "timeout", None) or self.rpc_timeout
        completed = slot["event"].wait(timeout=timeout)
        self._pending_rpcs.pop(request_id, None)
        if not completed:
            raise TimeoutError(f"RPC to peer timed out after {timeout}s on {type(self).__name__}.")
        if "error" in slot:
            err = slot["error"]
            if err.get("code") == rpc_protocol.ERR_BUSY:
                raise BackpressureError(err.get("message", "peer busy"))
            raise RuntimeError(f"Remote RPC error: {err.get('message', err)}")
        return slot.get("result")

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send one ``rpc.call`` to *peer_id*, retrying under backpressure.

        Wraps the carrier-specific :meth:`_send_once` in an exponential
        backoff + jitter retry loop scoped to :class:`BackpressureError`
        (an ``ERR_BUSY`` reply). Liveness pings never hit this path's
        retries because the peer answers them before admission, so a busy
        peer still reads as alive. After :attr:`max_rpc_retries` BUSY
        replies the final :class:`BackpressureError` propagates so the
        caller learns the peer stayed overwhelmed.
        """
        import random
        import time

        attempt = 0
        while True:
            try:
                return self._send_once(peer_id, path, args, kwargs)
            except BackpressureError:
                if attempt >= self.max_rpc_retries:
                    raise
                delay = min(
                    self.rpc_backoff_max,
                    self.rpc_backoff_base * (2**attempt),
                )
                time.sleep(delay + random.uniform(0, delay))
                attempt += 1

    # ------------------------------------------------------------------
    # Wire helpers
    # ------------------------------------------------------------------

    def _encode(self, obj: Any) -> bytes:
        """Serialise *obj* with this carrier's configured codec."""
        return _codec.encode(obj, self.codec)

    def _decode(self, data: bytes) -> Any:
        """Deserialise *data* with this carrier's configured codec."""
        return _codec.decode(data, self.codec)

    def _make_rpc_request(
        self, path: list[str], args: tuple, kwargs: dict, request_id: str
    ) -> dict:
        """Build the standard ``rpc.call`` request envelope."""
        return rpc_protocol.make_request(
            "rpc.call",
            {"path": list(path), "args": list(args), "kwargs": dict(kwargs)},
            request_id=request_id,
        )

    def _shutdown_executor(self) -> None:
        """Tear down the inbound worker pool (called from :meth:`stop`)."""
        if self._inbound_executor is not None:
            self._inbound_executor.shutdown(wait=False, cancel_futures=True)
            self._inbound_executor = None

    def _require_drivers(self, modules: tuple, extra: str) -> dict:
        """Import each name in *modules*, raising a clear capability error.

        Concrete transports that wrap a third-party driver call this at
        the top of their connection hook. When a driver is missing the
        raised :class:`RuntimeError` names both the package and the
        ``pip install laila-core[<extra>]`` that provides it -- never a
        bare ``ImportError`` and never a silent stub.
        """
        import importlib

        loaded: dict = {}
        for name in modules:
            try:
                loaded[name] = importlib.import_module(name)
            except ImportError as exc:
                raise RuntimeError(
                    f"The {self.protocol_name!r} transport requires {name!r}, which "
                    f"is not installed. Install it with `pip install laila-core[{extra}]`."
                ) from exc
        return loaded
