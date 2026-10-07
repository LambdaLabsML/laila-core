"""Point-to-point duplex-stream RPC carrier.

:class:`_P2PStreamRPCProtocol` is the variant of the stream carrier for
links that are a *single, already-connected* bidirectional byte stream
with no listen/accept step: serial lines (UART/RS-232/RS-485), USB-CDC,
Bluetooth RFCOMM, a paired BLE characteristic, etc. Both endpoints
simply open the link; one side initiates the ``peer.connect`` handshake
and the other answers it on the same stream.

A concrete transport supplies one coroutine, :meth:`_open_stream`,
returning the link's ``(reader, writer)`` pair (an
:class:`asyncio.StreamReader` and a writer exposing ``write`` /
``drain`` / ``close``). The carrier owns the dedicated event loop,
length-prefixed framing, the single-peer handshake, the receive loop,
off-thread dispatch and the pending-RPC table.

Stream lanes ride the same link through one :class:`._stream._Link`
(priority RPC queue + chunked stream queue, see that module). Because
serial links are slow, ``stream_chunk_bytes`` defaults to ``4096`` here:
a 30 KB frame at 1 Mbaud is ~300 ms of wire time, and a liveness ping
must never wait behind it.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid as _uuid
from typing import Any

from pydantic import Field, PrivateAttr

from ... import protocol as rpc_protocol
from ...channel import Channel
from . import codec as _codec
from .base import _PEER_CONNECT, _CarrierRPCProtocol
from .loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread
from .stream import _Link

log = logging.getLogger(__name__)


class _P2PStreamRPCProtocol(_CarrierRPCProtocol):
    """Carrier for a single point-to-point duplex byte stream.

    The per-peer handle stored in ``_connections`` is ``True``; the one
    link is held in ``_link``.
    """

    supports_channels = True

    #: Serial-friendly default: small chunks keep RPC latency low.
    stream_chunk_bytes: int = Field(default=4096)

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _reader: Any = PrivateAttr(default=None)
    _writer: Any = PrivateAttr(default=None)
    _link: Any = PrivateAttr(default=None)
    _recv_task: Any = PrivateAttr(default=None)
    _handshake_pending: dict = PrivateAttr(default_factory=dict)

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        """Open the link and return its ``(reader, writer)`` pair."""
        raise NotImplementedError

    async def _close_stream(self) -> None:
        """Close the link. Default: close the writer."""
        if self._link is not None:
            self._link.close()
            self._link = None
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        self._writer = None
        self._reader = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the loop, open the stream, start the receive loop."""
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
        self._reader, self._writer = await self._open_stream()
        self._link = _Link(self, self._writer, self._event_loop)
        self._link.start()
        self._recv_task = asyncio.ensure_future(self._receive_loop())
        ready.set()

    def stop(self) -> None:
        """Close lanes, the stream and the loop (idempotent).

        Order: goodbye to the peer, unregister it, stop the receive task
        (cancel *and wait*, so ``read_frame()`` has finished with the
        reader before any fd underneath it is closed), close the stream,
        cancel and await every remaining task, stop and close the loop.
        """
        if not self._started:
            return

        async def _shutdown() -> None:
            await self._goodbye_all()
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)
            self._shutdown_lanes()
            if self._recv_task is not None:
                self._recv_task.cancel()
                await asyncio.gather(self._recv_task, return_exceptions=True)
                self._recv_task = None
            await self._close_stream()
            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._shutdown_lanes()
        self._handshake_pending.clear()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    async def _send_goodbye(self, peer_id: str) -> None:
        """Queue ``peer.disconnect`` on the link and wait for it to be written."""
        link = self._link
        if link is None or link.closed or peer_id not in self._connections:
            return
        self._queue_frame(self._goodbye_message())
        await link.flush(timeout=0.5)

    # ------------------------------------------------------------------
    # Receive loop / dispatch
    # ------------------------------------------------------------------

    def _sole_peer(self) -> str | None:
        """The single registered peer id, if any."""
        for peer_id in self._connections:
            return peer_id
        return None

    async def _receive_loop(self) -> None:
        reply = self._make_reply()
        try:
            while True:
                raw = await _codec.read_frame(self._reader)
                if raw is None:
                    break
                if _codec.is_reserved_frame(raw):
                    peer_id = self._sole_peer()
                    if peer_id is None:
                        # stream frame before any handshake: not ours yet
                        self._unknown_lane_drops += 1
                        continue
                    self._on_stream_frame(peer_id, raw)
                    continue
                msg = self._decode(raw)
                if rpc_protocol.is_request(msg):
                    if msg.get("method") == _PEER_CONNECT:
                        await self._handle_handshake(msg)
                    elif self._is_goodbye(msg):
                        # The link stays open (it is the wire itself); only
                        # the peering is dropped, so a new handshake can follow.
                        from_id = (msg.get("params") or {}).get("from_id")
                        if from_id == self._sole_peer():
                            self._on_peer_goodbye(from_id)
                    else:
                        self._handle_request_frame(msg, reply, peer_id=self._sole_peer())
                elif rpc_protocol.is_response(msg):
                    rid = msg.get("id")
                    if rid in self._handshake_pending:
                        slot = self._handshake_pending.get(rid)
                        if slot is not None:
                            slot["msg"] = msg
                            slot["event"].set()
                    else:
                        self._complete_pending(msg)
        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception:
            log.debug("P2P receive loop ended", exc_info=True)
        finally:
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)

    def _queue_frame(self, obj: dict) -> None:
        """Frame *obj* and queue it on the link (loop thread)."""
        if self._link is None:
            return
        self._link.put_rpc(_codec.frame(self._encode(obj)))

    async def _write_frame(self, obj: dict) -> None:
        """Queue *obj* for the writer task (kept for subclass compatibility)."""
        self._queue_frame(obj)

    async def _handle_handshake(self, msg: dict) -> None:
        params = msg.get("params", {})
        peer_id = params.get("from_id")
        if params.get("secret") != self.peer_secret_key:
            await self._write_frame(
                rpc_protocol.make_error(
                    msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
                )
            )
            return
        policy_id = self._communication.policy_id if self._communication else None
        self._set_peer_caps(peer_id, params.get("caps"))
        self._register_peer(peer_id, True)
        await self._write_frame(
            rpc_protocol.make_result(
                msg.get("id"), {"peer_id": policy_id, "caps": self._local_caps()}
            )
        )

    def _make_reply(self):
        """Build a thread-safe ``reply(resp)`` that queues a frame on the link.

        Used by :meth:`_handle_request_frame`; safe both inline on the I/O
        loop (ping / control fast-path) and from an inbound worker thread.
        """

        def _reply(resp: dict) -> None:
            try:
                self._loop_call(self._queue_frame, resp)
            except ConnectionError:
                pass

        return _reply

    # ------------------------------------------------------------------
    # Peering / RPC / stream
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Initiate the handshake over the already-open point-to-point link."""
        self.start()
        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request(
            "peer.connect",
            {"from_id": policy_id, "secret": secret, "caps": self._local_caps()},
        )
        rid = req["id"]
        slot: dict[str, Any] = {"event": threading.Event()}
        self._handshake_pending[rid] = slot
        self._loop_call(self._queue_frame, req)

        completed = slot["event"].wait(timeout=self.handshake_timeout)
        self._handshake_pending.pop(rid, None)
        if not completed:
            raise ConnectionError("Peer handshake timed out.")
        reply = slot["msg"]
        if "error" in reply:
            raise ConnectionError(
                f"Peer rejected connection: {reply['error'].get('message', reply['error'])}"
            )
        result = reply.get("result", {}) or {}
        peer_id = result.get("peer_id")
        if peer_id is None:
            raise ConnectionError("Peer response missing peer_id.")
        self._set_peer_caps(peer_id, result.get("caps"))
        self._register_peer(peer_id, True)
        return peer_id

    def _queue_rpc_threadsafe(self, msg: dict) -> None:
        if self._link is None or self._link.closed:
            raise ConnectionError("Point-to-point link is not open.")
        self._loop_call(self._queue_frame, msg)

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Write an ``rpc.call`` to the link and block for the response."""
        if peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        try:
            self._queue_rpc_threadsafe(req)
        except ConnectionError:
            self._pending_rpcs.pop(request_id, None)
            raise
        return self._await_pending(request_id, slot)

    def _send_oneway(self, peer_id: str, path: list, kwargs: dict) -> None:
        """Queue a control request without waiting for the reply."""
        if peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {peer_id}")
        self._queue_rpc_threadsafe(self._make_rpc_request(path, (), kwargs, str(_uuid.uuid4())))

    def _stream_enqueue(self, channel: Channel, payload: bytes) -> None:
        """Marshal one stream message onto the link (user thread)."""
        if channel.peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {channel.peer_id}")
        link = self._link
        if link is None or link.closed:
            raise ConnectionError("Point-to-point link is not open.")
        self._loop_call(link.put_stream, channel, payload)
