"""TCP/IP (WebSocket) communication protocol implementation."""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid as _uuid
from typing import Any, Dict, Optional

from pydantic import Field, PrivateAttr

from .base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from .. import protocol as rpc_protocol

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_COMM_PROTOCOL):
    """WebSocket-based peer-to-peer communication protocol.

    Parameters
    ----------
    host : str
        Bind address for the WebSocket server.  ``"0.0.0.0"`` listens on
        all interfaces.
    port : int
        TCP port for the WebSocket server.  ``0`` lets the OS pick a free
        port.
    peer_secret_key : str
        Shared secret that remote peers must supply to connect.
    """

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=0)
    peer_secret_key: str = Field(default_factory=lambda: _uuid.uuid4().hex)

    _server: Any = PrivateAttr(default=None)
    _connections: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _pending_rpcs: Dict[str, Dict[str, Any]] = PrivateAttr(default_factory=dict)
    _event_loop: Optional[asyncio.AbstractEventLoop] = PrivateAttr(default=None)
    _loop_thread: Optional[threading.Thread] = PrivateAttr(default=None)
    _started: bool = PrivateAttr(default=False)
    _bound_port: Optional[int] = PrivateAttr(default=None)

    @property
    def bound_port(self) -> int:
        """The actual port the server is listening on.

        After ``start()`` this returns the OS-assigned port (useful when
        ``port`` is ``0``).  Before ``start()`` it falls back to ``port``.
        """
        return self._bound_port if self._bound_port is not None else self.port

    # ------------------------------------------------------------------
    # URI routing
    # ------------------------------------------------------------------

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        return uri.startswith("ws://") or uri.startswith("wss://")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the background event loop and WebSocket server."""
        if self._started:
            return

        self._event_loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run_loop() -> None:
            asyncio.set_event_loop(self._event_loop)
            self._event_loop.run_until_complete(self._async_start(ready))
            self._event_loop.run_forever()

        self._loop_thread = threading.Thread(target=_run_loop, daemon=True)
        self._loop_thread.start()
        ready.wait(timeout=10.0)
        self._started = True
        policy_id = self._communication.policy_id if self._communication else None
        log.info(
            "TCP/IP protocol started for policy %s on port %s",
            policy_id, self.bound_port,
        )

    async def _async_start(self, ready: threading.Event) -> None:
        from .. import connection
        await connection.start_server(self)
        ready.set()

    def stop(self) -> None:
        """Shut down the WebSocket server and all connections."""
        if not self._started:
            return

        async def _shutdown() -> None:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

            close_tasks = []
            for ws in list(self._connections.values()):
                close_tasks.append(asyncio.ensure_future(ws.close()))
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            self._connections.clear()

            for task in asyncio.all_tasks(self._event_loop):
                if task is not asyncio.current_task():
                    task.cancel()

        if self._event_loop is not None and self._event_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                _shutdown(), self._event_loop,
            )
            try:
                future.result(timeout=5.0)
            except Exception:
                pass
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)

        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            self._loop_thread = None

        self._pending_rpcs.clear()
        self._bound_port = None
        self._started = False
        policy_id = self._communication.policy_id if self._communication else None
        log.info("TCP/IP protocol stopped for policy %s", policy_id)

    # ------------------------------------------------------------------
    # Peer management
    # ------------------------------------------------------------------

    def add_peer(self, uri: str, secret: str) -> str:
        """Connect to a remote policy over WebSocket."""
        self.start()

        from .. import connection
        future = asyncio.run_coroutine_threadsafe(
            connection.connect_outbound(self, uri, secret),
            self._event_loop,
        )
        return future.result(timeout=30.0)

    def _register_peer(self, peer_id: str, ws: Any) -> None:
        """Store the WebSocket locally and notify Communication."""
        self._connections[peer_id] = ws
        if self._communication is not None:
            self._communication._register_peer(peer_id)

    def _unregister_peer(self, peer_id: str) -> None:
        """Remove the WebSocket locally and notify Communication."""
        self._connections.pop(peer_id, None)
        if self._communication is not None:
            self._communication._unregister_peer(peer_id)

    def has_peer(self, peer_id: str) -> bool:
        return peer_id in self._connections

    # ------------------------------------------------------------------
    # RPC (outbound)
    # ------------------------------------------------------------------

    def send_rpc(
        self, peer_id: str, path: list[str], args: tuple, kwargs: dict
    ) -> Any:
        """Send a JSON-RPC call over the WebSocket to *peer_id*."""
        ws = self._connections.get(peer_id)
        if ws is None:
            raise ConnectionError(f"No connection to peer {peer_id}")

        request_id = str(_uuid.uuid4())
        event = threading.Event()
        slot: Dict[str, Any] = {"event": event}
        self._pending_rpcs[request_id] = slot

        req = rpc_protocol.make_request("rpc.call", {
            "path": path,
            "args": list(args),
            "kwargs": dict(kwargs),
        }, request_id=request_id)

        asyncio.run_coroutine_threadsafe(
            ws.send(rpc_protocol.encode(req)),
            self._event_loop,
        )

        event.wait(timeout=60.0)
        self._pending_rpcs.pop(request_id, None)

        if "error" in slot:
            err = slot["error"]
            raise RuntimeError(f"Remote RPC error: {err.get('message', err)}")

        return slot.get("result")
