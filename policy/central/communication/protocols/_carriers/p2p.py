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
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from pydantic import PrivateAttr

from ... import protocol as rpc_protocol
from . import codec as _codec
from .base import _CarrierRPCProtocol

log = logging.getLogger(__name__)


class _P2PStreamRPCProtocol(_CarrierRPCProtocol):
    """Carrier for a single point-to-point duplex byte stream."""

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _reader: Any = PrivateAttr(default=None)
    _writer: Any = PrivateAttr(default=None)
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
        self._event_loop = asyncio.new_event_loop()
        ready = threading.Event()
        boot: dict[str, BaseException] = {}

        def _run_loop() -> None:
            asyncio.set_event_loop(self._event_loop)
            try:
                self._event_loop.run_until_complete(self._async_start(ready))
            except BaseException as exc:
                boot["error"] = exc
                ready.set()
                return
            self._event_loop.run_forever()

        self._loop_thread = threading.Thread(
            target=_run_loop, daemon=True, name=f"{type(self).__name__}-loop"
        )
        self._loop_thread.start()
        ready.wait(timeout=max(self.handshake_timeout, 10.0))
        if "error" in boot:
            self._started = False
            raise boot["error"]
        self._started = True

    async def _async_start(self, ready: threading.Event) -> None:
        self._reader, self._writer = await self._open_stream()
        self._recv_task = asyncio.ensure_future(self._receive_loop())
        ready.set()

    def stop(self) -> None:
        """Close the stream and stop the loop (idempotent)."""
        if not self._started:
            return

        async def _shutdown() -> None:
            if self._recv_task is not None:
                self._recv_task.cancel()
            await self._close_stream()
            pending = [
                task
                for task in asyncio.all_tasks(self._event_loop)
                if task is not asyncio.current_task()
            ]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        if self._event_loop is not None and self._event_loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(_shutdown(), self._event_loop)
                fut.result(timeout=5.0)
            except Exception:
                pass
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)

        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            self._loop_thread = None

        self._handshake_pending.clear()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    # ------------------------------------------------------------------
    # Receive loop / dispatch
    # ------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        try:
            while True:
                raw = await _codec.read_frame(self._reader)
                if raw is None:
                    break
                msg = self._decode(raw)
                if rpc_protocol.is_request(msg):
                    if msg.get("method") == "peer.connect":
                        await self._handle_handshake(msg)
                    else:
                        self._handle_request_frame(msg, self._make_reply())
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

    async def _write_frame(self, obj: dict) -> None:
        self._writer.write(_codec.frame(self._encode(obj)))
        drain = getattr(self._writer, "drain", None)
        if drain is not None:
            await drain()

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
        self._register_peer(peer_id, True)
        await self._write_frame(rpc_protocol.make_result(msg.get("id"), {"peer_id": policy_id}))

    def _make_reply(self):
        """Build a thread-safe ``reply(resp)`` that writes a frame on the loop.

        Used by :meth:`_handle_request_frame`; safe both inline on the I/O
        loop (ping fast-path) and from an inbound worker thread.
        """

        def _reply(resp: dict) -> None:
            asyncio.run_coroutine_threadsafe(self._write_frame(resp), self._event_loop)

        return _reply

    # ------------------------------------------------------------------
    # Peering / RPC
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Initiate the handshake over the already-open point-to-point link."""
        self.start()
        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request("peer.connect", {"from_id": policy_id, "secret": secret})
        rid = req["id"]
        slot: dict[str, Any] = {"event": threading.Event()}
        self._handshake_pending[rid] = slot

        async def _send() -> None:
            await self._write_frame(req)

        asyncio.run_coroutine_threadsafe(_send(), self._event_loop)

        completed = slot["event"].wait(timeout=self.handshake_timeout)
        self._handshake_pending.pop(rid, None)
        if not completed:
            raise ConnectionError("Peer handshake timed out.")
        reply = slot["msg"]
        if "error" in reply:
            raise ConnectionError(
                f"Peer rejected connection: {reply['error'].get('message', reply['error'])}"
            )
        peer_id = reply.get("result", {}).get("peer_id")
        if peer_id is None:
            raise ConnectionError("Peer response missing peer_id.")
        self._register_peer(peer_id, True)
        return peer_id

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Write an ``rpc.call`` to the link and block for the response."""
        if peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)

        async def _send() -> None:
            await self._write_frame(req)

        asyncio.run_coroutine_threadsafe(_send(), self._event_loop)
        return self._await_pending(request_id, slot)
