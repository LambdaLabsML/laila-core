"""Reliable, ordered, duplex byte-stream RPC carrier.

:class:`_StreamRPCProtocol` implements the full
:class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` contract over any transport
that looks like an :class:`asyncio.StreamReader` / ``StreamWriter``
pair: TCP, TLS, Unix domain sockets, serial lines, USB-CDC, RFCOMM, ...

A concrete transport only supplies two coroutines:

- :meth:`_serve` -- create and return a listening server whose
  per-connection callback is :meth:`_handle_inbound_stream` (e.g.
  ``await asyncio.start_server(self._handle_inbound_stream, host, port)``).
- :meth:`_open_connection` -- open one outbound connection to a URI and
  return its ``(reader, writer)`` pair.

Everything else -- the dedicated background event loop, length-prefixed
framing, the ``peer.connect`` handshake, the receive loop, off-thread
inbound dispatch, and the blocking pending-RPC table -- is handled here.

Stream lanes and writer fairness
--------------------------------
Each peer connection is wrapped in a :class:`_Link`: the writer plus two
outbound queues (RPC/control, which has priority, and stream) drained by
a single writer task. **Every** write -- handshake frames, replies,
``_send_once``, stream chunks -- goes through the link; nothing else
calls ``writer.write``. Stream messages are sliced into
``stream_chunk_bytes`` chunks and the RPC queue is drained between
chunks, so a liveness ping is never stuck behind a 200 KB frame. Each
write is followed by ``await writer.drain()`` and the transport's
write-buffer high-water mark is set to two chunks, so link backpressure
(e.g. the UART ``FlowControlMixin``) is honoured instead of buffering
unboundedly inside asyncio.

Inbound frames are discriminated on their first byte
(:func:`codec.is_reserved_frame`): stream frames go straight to
:meth:`_CarrierRPCProtocol._on_stream_frame` and never touch the RPC
codec, admission or the executor.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import deque
from typing import Any

from pydantic import PrivateAttr

from ... import protocol as rpc_protocol
from ...channel import Channel
from . import codec as _codec
from .base import _PEER_CONNECT, _CarrierRPCProtocol
from .loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread

log = logging.getLogger(__name__)


class _Link:
    """One peer connection: writer + prioritised outbound queues + writer task.

    Parameters
    ----------
    proto : _CarrierRPCProtocol
        Owning carrier (for ``_stream_chunks`` / ``channel_queue_size``).
    writer : asyncio.StreamWriter-like
        Object exposing ``write`` / ``drain`` / ``close`` (and optionally
        ``transport``).

    Notes
    -----
    All methods except :meth:`close_threadsafe` must be called on the
    carrier's event loop thread. ``put_rpc`` / ``put_stream`` are
    marshalled there by callers via ``call_soon_threadsafe``.
    """

    __slots__ = (
        "closed",
        "loop",
        "proto",
        "rpc_q",
        "stream_q",
        "stream_q_max",
        "task",
        "wake",
        "writer",
    )

    def __init__(self, proto: _CarrierRPCProtocol, writer: Any, loop: asyncio.AbstractEventLoop):
        self.proto = proto
        self.writer = writer
        self.loop = loop
        self.rpc_q: deque[bytes] = deque()
        self.stream_q: deque[tuple[Channel, bytes]] = deque()
        self.stream_q_max = max(1, int(proto.channel_queue_size))
        self.wake = asyncio.Event()
        self.closed = False
        self.task: asyncio.Task | None = None
        transport = getattr(writer, "transport", None)
        if transport is not None:
            try:
                high = 2 * max(1, int(proto.stream_chunk_bytes))
                transport.set_write_buffer_limits(high=high, low=high // 2)
            except Exception:
                pass

    def start(self) -> None:
        """Start the writer task (loop thread)."""
        if self.task is None:
            self.task = self.loop.create_task(self._run())

    def put_rpc(self, data: bytes) -> None:
        """Queue one framed RPC/control payload (priority lane)."""
        if self.closed:
            return
        self.rpc_q.append(data)
        self.wake.set()

    def put_stream(self, channel: Channel, payload: bytes) -> None:
        """Queue one stream message; evicts the oldest when the queue is full."""
        if self.closed:
            return
        if len(self.stream_q) >= self.stream_q_max:
            old_ch, _ = self.stream_q.popleft()
            old_ch.tx_dropped += 1
        self.stream_q.append((channel, payload))
        self.wake.set()

    async def _write(self, data: bytes) -> None:
        self.writer.write(data)
        drain = getattr(self.writer, "drain", None)
        if drain is not None:
            await drain()

    async def _drain_rpc(self) -> None:
        while self.rpc_q and not self.closed:
            await self._write(self.rpc_q.popleft())

    async def _run(self) -> None:
        try:
            while not self.closed:
                if not self.rpc_q and not self.stream_q:
                    self.wake.clear()
                    await self.wake.wait()
                    continue
                await self._drain_rpc()
                if self.stream_q and not self.closed:
                    channel, payload = self.stream_q.popleft()
                    if channel.closed or channel.tx_lane_id is None:
                        continue
                    for chunk in self.proto._stream_chunks(channel, payload):
                        await self._write(_codec.frame(chunk))
                        await self._drain_rpc()
                        if self.closed:
                            break
        except asyncio.CancelledError:
            pass
        except Exception:
            log.debug("link writer ended", exc_info=True)
        finally:
            self.closed = True

    async def flush(self, timeout: float = 2.0) -> None:
        """Wait (bounded) until the RPC queue has been written out."""
        deadline = self.loop.time() + timeout
        while self.rpc_q and not self.closed and self.loop.time() < deadline:
            await asyncio.sleep(0.005)

    def close(self) -> None:
        """Stop the writer task and close the writer (loop thread, idempotent)."""
        self.closed = True
        self.wake.set()
        task = self.task
        if task is not None and not task.done():
            task.cancel()
        try:
            self.writer.close()
        except Exception:
            pass

    def close_threadsafe(self) -> None:
        """Schedule :meth:`close` on the loop from any thread."""
        try:
            self.loop.call_soon_threadsafe(self.close)
        except RuntimeError:
            self.closed = True


class _StreamRPCProtocol(_CarrierRPCProtocol):
    """Carrier for reliable, ordered, duplex byte streams.

    Notes
    -----
    All public lifecycle methods are idempotent. The carrier owns a
    dedicated asyncio event loop on a daemon thread; transport I/O is
    scheduled there via :func:`asyncio.run_coroutine_threadsafe` so the
    rest of laila stays synchronous. The per-peer handle stored in
    ``_connections`` is a :class:`_Link`.
    """

    supports_channels = True

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _server: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    async def _serve(self) -> Any:
        """Create and return a listening server.

        Subclasses must bind their transport and wire
        :meth:`_handle_inbound_stream` as the per-connection callback,
        recording any bound-address detail they expose. Runs inside the
        carrier event loop.
        """
        raise NotImplementedError

    async def _open_connection(self, uri: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open one outbound connection to *uri*; return its stream pair.

        Runs inside the carrier event loop.
        """
        raise NotImplementedError

    def _on_stream_ready(self, writer: asyncio.StreamWriter) -> None:
        """Hook called once a stream is established (inbound + outbound).

        Subclasses tune the socket here (e.g. TCP transports disable
        Nagle via ``TCP_NODELAY`` for minimum small-frame latency).
        Receives the raw ``StreamWriter``. Default: no-op.
        """
        return None

    async def _close_server(self, server: Any) -> None:
        """Close a server returned by :meth:`_serve`. Default: ``close``+wait."""
        if server is None:
            return
        server.close()
        wait_closed = getattr(server, "wait_closed", None)
        if wait_closed is not None:
            try:
                await asyncio.wait_for(wait_closed(), timeout=2.0)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the event loop and start accepting connections (idempotent)."""
        if self._started:
            return
        self._ensure_executor()
        try:
            start_loop_thread(
                self, self._async_start, ready_timeout=max(self.handshake_timeout, 10.0)
            )
        except BaseException:
            self._shutdown_executor()
            raise
        self._started = True

    async def _async_start(self, ready: threading.Event) -> None:
        """Bring up the server inside the loop, then signal *ready*."""
        self._server = await self._serve()
        ready.set()

    def stop(self) -> None:
        """Tear down the server, every peer stream and lane, and the loop (idempotent).

        Order: say goodbye to every peer, unregister them (closing lanes
        -- waking relays -- and links, so the server's ``wait_closed()``
        completes promptly), close the server, cancel and await every
        remaining task, then stop and **close** the loop.
        """
        if not self._started:
            return

        async def _shutdown() -> None:
            await self._goodbye_all()
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)
            self._connections.clear()
            self._shutdown_lanes()
            await self._close_server(self._server)
            self._server = None
            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._shutdown_lanes()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    # ------------------------------------------------------------------
    # Links
    # ------------------------------------------------------------------

    def _make_link(self, writer: Any) -> _Link:
        """Wrap *writer* in a started :class:`_Link` (loop thread)."""
        link = _Link(self, writer, self._event_loop)
        link.start()
        return link

    def _on_loop_thread(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._event_loop
        except RuntimeError:
            return False

    def _unregister_peer(self, peer_id: str) -> None:
        """Close lanes (base), then the link, then notify the hub. Idempotent."""
        link = self._connections.get(peer_id)
        super()._unregister_peer(peer_id)
        if link is None:
            return
        if self._on_loop_thread():
            link.close()
        else:
            link.close_threadsafe()

    async def _send_goodbye(self, peer_id: str) -> None:
        """Queue ``peer.disconnect`` on the peer's link and wait for it to be written."""
        link = self._connections.get(peer_id)
        if link is None or link.closed:
            return
        link.put_rpc(_codec.frame(self._encode(self._goodbye_message())))
        await link.flush(timeout=0.5)

    # ------------------------------------------------------------------
    # Peering
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Open an outbound stream to *uri* and complete the handshake."""
        self.start()
        fut = asyncio.run_coroutine_threadsafe(
            self._connect_outbound(uri, secret), self._event_loop
        )
        return fut.result(timeout=max(self.handshake_timeout * 3, 30.0))

    async def _connect_outbound(self, uri: str, secret: str) -> str:
        """Client side of the ``peer.connect`` handshake."""
        reader, writer = await self._open_connection(uri)
        self._on_stream_ready(writer)
        link = self._make_link(writer)
        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request(
            "peer.connect",
            {"from_id": policy_id, "secret": secret, "caps": self._local_caps()},
        )
        link.put_rpc(_codec.frame(self._encode(req)))

        try:
            raw = await asyncio.wait_for(_codec.read_frame(reader), timeout=self.handshake_timeout)
        except TimeoutError as exc:
            link.close()
            raise ConnectionError("Peer handshake timed out.") from exc
        if raw is None:
            link.close()
            raise ConnectionError("Peer closed during handshake.")
        if _codec.is_reserved_frame(raw):
            link.close()
            raise ConnectionError("Peer sent a stream frame before completing the handshake.")

        msg = self._decode(raw)
        if "error" in msg:
            link.close()
            raise ConnectionError(
                f"Peer rejected connection: {msg['error'].get('message', msg['error'])}"
            )
        result = msg.get("result", {}) or {}
        peer_id = result.get("peer_id")
        if peer_id is None:
            link.close()
            raise ConnectionError("Peer response missing peer_id.")

        self._set_peer_caps(peer_id, result.get("caps"))
        self._register_peer(peer_id, link)
        asyncio.ensure_future(self._receive_loop(reader, link, peer_id))
        return peer_id

    async def _handle_inbound_stream(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Server side of the handshake, then the shared receive loop."""
        try:
            raw = await asyncio.wait_for(_codec.read_frame(reader), timeout=self.handshake_timeout)
        except TimeoutError:
            writer.close()
            return
        if raw is None or _codec.is_reserved_frame(raw):
            # EOF, or a stream frame before the handshake: not a peer.
            writer.close()
            return

        self._on_stream_ready(writer)
        link = self._make_link(writer)
        msg = self._decode(raw)
        if not rpc_protocol.is_request(msg) or msg.get("method") != _PEER_CONNECT:
            resp = rpc_protocol.make_error(
                msg.get("id"),
                rpc_protocol.ERR_INVALID_REQUEST,
                "First message must be a peer.connect request.",
            )
            link.put_rpc(_codec.frame(self._encode(resp)))
            await link.flush()
            link.close()
            return

        params = msg.get("params", {})
        peer_id = params.get("from_id")
        if params.get("secret") != self.peer_secret_key:
            resp = rpc_protocol.make_error(
                msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
            )
            link.put_rpc(_codec.frame(self._encode(resp)))
            await link.flush()
            link.close()
            return

        policy_id = self._communication.policy_id if self._communication else None
        resp = rpc_protocol.make_result(
            msg.get("id"), {"peer_id": policy_id, "caps": self._local_caps()}
        )
        link.put_rpc(_codec.frame(self._encode(resp)))

        self._set_peer_caps(peer_id, params.get("caps"))
        self._register_peer(peer_id, link)
        await self._receive_loop(reader, link, peer_id)

    async def _receive_loop(self, reader: asyncio.StreamReader, link: _Link, peer_id: str) -> None:
        """Decode frames and route requests/responses/stream chunks until EOF."""
        reply = self._make_reply(link)
        try:
            while True:
                raw = await _codec.read_frame(reader)
                if raw is None:
                    break
                if _codec.is_reserved_frame(raw):
                    self._on_stream_frame(peer_id, raw)
                    continue
                msg = self._decode(raw)
                if rpc_protocol.is_request(msg):
                    if self._is_goodbye(msg):
                        # peer is dropping us gracefully: end this stream now
                        break
                    self._handle_request_frame(msg, reply, peer_id=peer_id)
                elif rpc_protocol.is_response(msg):
                    self._complete_pending(msg)
        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception:
            log.debug("Stream receive loop for peer %s ended", peer_id, exc_info=True)
        finally:
            self._unregister_peer(peer_id)

    def _make_reply(self, link: _Link):
        """Build a thread-safe ``reply(resp)`` that frames + queues on the link.

        Used by :meth:`_handle_request_frame`; safe to call both inline on
        the I/O loop (ping / control fast-path) and from an inbound
        worker thread.
        """

        def _reply(resp: dict) -> None:
            data = _codec.frame(self._encode(resp))
            try:
                self._loop_call(link.put_rpc, data)
            except ConnectionError:
                pass

        return _reply

    # ------------------------------------------------------------------
    # Outbound RPC / stream
    # ------------------------------------------------------------------

    def _queue_rpc(self, peer_id: str, msg: dict) -> None:
        """Frame *msg* and queue it on the peer's link from any thread."""
        link = self._connections.get(peer_id)
        if link is None or link.closed:
            raise ConnectionError(f"No connection to peer {peer_id}")
        self._loop_call(link.put_rpc, _codec.frame(self._encode(msg)))

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send one ``rpc.call`` to *peer_id* and block for the response."""
        if peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        try:
            self._queue_rpc(peer_id, req)
        except ConnectionError:
            self._pending_rpcs.pop(request_id, None)
            raise
        return self._await_pending(request_id, slot)

    def _send_oneway(self, peer_id: str, path: list, kwargs: dict) -> None:
        """Queue a control request without waiting for the reply."""
        import uuid as _uuid

        self._queue_rpc(peer_id, self._make_rpc_request(path, (), kwargs, str(_uuid.uuid4())))

    def _stream_enqueue(self, channel: Channel, payload: bytes) -> None:
        """Marshal one stream message onto the peer's link (user thread)."""
        link = self._connections.get(channel.peer_id)
        if link is None or link.closed:
            raise ConnectionError(f"No connection to peer {channel.peer_id}")
        self._loop_call(link.put_stream, channel, payload)
