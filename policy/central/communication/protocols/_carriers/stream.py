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


class _StreamRPCProtocol(_CarrierRPCProtocol):
    """Carrier for reliable, ordered, duplex byte streams.

    Notes
    -----
    All public lifecycle methods are idempotent. The carrier owns a
    dedicated asyncio event loop on a daemon thread; transport I/O is
    scheduled there via :func:`asyncio.run_coroutine_threadsafe` so the
    rest of laila stays synchronous.
    """

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
        Default: no-op.
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
        """Bring up the server inside the loop, then signal *ready*."""
        self._server = await self._serve()
        ready.set()

    def stop(self) -> None:
        """Tear down the server, every peer stream, and the loop (idempotent)."""
        if not self._started:
            return

        async def _shutdown() -> None:
            # Close peer streams first so the server's wait_closed() can
            # complete promptly, then tear the server down.
            for writer in list(self._connections.values()):
                try:
                    writer.close()
                except Exception:
                    pass
            self._connections.clear()
            await self._close_server(self._server)
            self._server = None
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

        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

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
        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request("peer.connect", {"from_id": policy_id, "secret": secret})
        writer.write(_codec.frame(self._encode(req)))
        await writer.drain()

        try:
            raw = await asyncio.wait_for(_codec.read_frame(reader), timeout=self.handshake_timeout)
        except TimeoutError as exc:
            writer.close()
            raise ConnectionError("Peer handshake timed out.") from exc
        if raw is None:
            writer.close()
            raise ConnectionError("Peer closed during handshake.")

        msg = self._decode(raw)
        if "error" in msg:
            writer.close()
            raise ConnectionError(
                f"Peer rejected connection: {msg['error'].get('message', msg['error'])}"
            )
        peer_id = msg.get("result", {}).get("peer_id")
        if peer_id is None:
            writer.close()
            raise ConnectionError("Peer response missing peer_id.")

        self._register_peer(peer_id, writer)
        asyncio.ensure_future(self._receive_loop(reader, writer, peer_id))
        return peer_id

    def disconnect(self, peer_id: str) -> None:
        """Close a single peer's stream and drop it (idempotent)."""
        writer = self._connections.get(peer_id)
        if writer is not None and self._event_loop is not None:
            try:
                self._event_loop.call_soon_threadsafe(writer.close)
            except Exception:
                pass
        self._unregister_peer(peer_id)

    async def _handle_inbound_stream(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Server side of the handshake, then the shared receive loop."""
        try:
            raw = await asyncio.wait_for(_codec.read_frame(reader), timeout=self.handshake_timeout)
        except TimeoutError:
            writer.close()
            return
        if raw is None:
            writer.close()
            return

        self._on_stream_ready(writer)
        msg = self._decode(raw)
        if not rpc_protocol.is_request(msg) or msg.get("method") != "peer.connect":
            resp = rpc_protocol.make_error(
                msg.get("id"),
                rpc_protocol.ERR_INVALID_REQUEST,
                "First message must be a peer.connect request.",
            )
            writer.write(_codec.frame(self._encode(resp)))
            await writer.drain()
            writer.close()
            return

        params = msg.get("params", {})
        peer_id = params.get("from_id")
        if params.get("secret") != self.peer_secret_key:
            resp = rpc_protocol.make_error(
                msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
            )
            writer.write(_codec.frame(self._encode(resp)))
            await writer.drain()
            writer.close()
            return

        policy_id = self._communication.policy_id if self._communication else None
        resp = rpc_protocol.make_result(msg.get("id"), {"peer_id": policy_id})
        writer.write(_codec.frame(self._encode(resp)))
        await writer.drain()

        self._register_peer(peer_id, writer)
        await self._receive_loop(reader, writer, peer_id)

    async def _receive_loop(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, peer_id: str
    ) -> None:
        """Decode frames and route requests/responses until the stream ends."""
        try:
            while True:
                raw = await _codec.read_frame(reader)
                if raw is None:
                    break
                msg = self._decode(raw)
                if rpc_protocol.is_request(msg):
                    self._handle_request_frame(msg, self._make_reply(writer))
                elif rpc_protocol.is_response(msg):
                    self._complete_pending(msg)
        except (asyncio.CancelledError, ConnectionError):
            pass
        except Exception:
            log.debug("Stream receive loop for peer %s ended", peer_id, exc_info=True)
        finally:
            self._unregister_peer(peer_id)

    def _make_reply(self, writer: asyncio.StreamWriter):
        """Build a thread-safe ``reply(resp)`` that frames + writes on the loop.

        Used by :meth:`_handle_request_frame`; safe to call both inline on
        the I/O loop (ping fast-path) and from an inbound worker thread.
        """

        def _reply(resp: dict) -> None:
            data = _codec.frame(self._encode(resp))

            async def _w() -> None:
                try:
                    writer.write(data)
                    await writer.drain()
                except Exception:
                    pass

            asyncio.run_coroutine_threadsafe(_w(), self._event_loop)

        return _reply

    # ------------------------------------------------------------------
    # Outbound RPC
    # ------------------------------------------------------------------

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send one ``rpc.call`` to *peer_id* and block for the response."""
        writer = self._connections.get(peer_id)
        if writer is None:
            raise ConnectionError(f"No connection to peer {peer_id}")

        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        data = _codec.frame(self._encode(req))

        async def _send() -> None:
            writer.write(data)
            await writer.drain()

        asyncio.run_coroutine_threadsafe(_send(), self._event_loop)
        return self._await_pending(request_id, slot)
