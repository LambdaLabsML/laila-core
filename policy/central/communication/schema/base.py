"""Core communication sub-system for inter-policy peer-to-peer RPC.

Manages a WebSocket server, outbound peer connections, and a JSON-RPC 2.0
dispatch layer that lets remote policies invoke local methods transparently.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid as _uuid
from typing import Any, Dict, Optional

from pydantic import Field, PrivateAttr, ConfigDict
from .....basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS

from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMUNICATION_SCOPE
from ..proxy import RemotePolicyProxy
from .. import protocol

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_COMMUNICATION(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Central communication that owns WebSocket transport and peer registry.

    Parameters
    ----------
    host : str
        Bind address for the WebSocket server.  ``"0.0.0.0"`` listens on all
        interfaces.
    port : int
        TCP port for the WebSocket server.  ``0`` means the OS picks a free
        port.
    peer_secret_key : str
        Shared secret that remote peers must supply to connect.
    policy_id : str, optional
        ``global_id`` of the owning policy (set automatically during wiring).
    """

    _scopes: list[str] = PrivateAttr(
        default_factory=lambda: [_CENTRAL_COMMUNICATION_SCOPE],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=0)
    peer_secret_key: str = Field(default_factory=lambda: _uuid.uuid4().hex)
    policy_id: Optional[str] = CLIExempt(default=None)

    peers: Dict[str, RemotePolicyProxy] = CLIExempt(default_factory=dict)

    _server: Any = PrivateAttr(default=None)
    _connections: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _pending_rpcs: Dict[str, Dict[str, Any]] = PrivateAttr(default_factory=dict)
    _event_loop: Optional[asyncio.AbstractEventLoop] = PrivateAttr(default=None)
    _loop_thread: Optional[threading.Thread] = PrivateAttr(default=None)
    _started: bool = PrivateAttr(default=False)
    _local_policy: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the background event loop and WebSocket server.

        Safe to call multiple times; subsequent calls are no-ops.
        """
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
        log.info("Communication started for policy %s on port %s", self.policy_id, self.port)

    async def _async_start(self, ready: threading.Event) -> None:
        """Start the WS server inside the dedicated event loop.

        Parameters
        ----------
        ready : threading.Event
            Signalled once the server is bound and listening.
        """
        from .. import connection
        await connection.start_server(self)
        ready.set()

    def stop(self) -> None:
        """Shut down the server, close all peer connections, and stop the loop."""
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
            future = asyncio.run_coroutine_threadsafe(_shutdown(), self._event_loop)
            try:
                future.result(timeout=5.0)
            except Exception:
                pass
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)

        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            self._loop_thread = None

        self.peers.clear()
        self._pending_rpcs.clear()
        self._started = False
        log.info("Communication stopped for policy %s", self.policy_id)

    # ------------------------------------------------------------------
    # Peer management
    # ------------------------------------------------------------------

    def add_peer(self, uri: str, secret: str) -> str:
        """Initiate a peering connection to a remote policy.

        Starts the communication server if it has not been started yet,
        then performs the ``peer.connect`` handshake over WebSocket.

        Parameters
        ----------
        uri : str
            WebSocket URI of the remote policy (e.g. ``"ws://host:port"``).
        secret : str
            The remote policy's ``peer_secret_key``.

        Returns
        -------
        str
            The ``global_id`` of the newly peered remote policy.
        """
        self.start()

        from .. import connection
        future = asyncio.run_coroutine_threadsafe(
            connection.connect_outbound(self, uri, secret),
            self._event_loop,
        )
        return future.result(timeout=30.0)

    def _register_peer(self, peer_id: str, ws: Any) -> None:
        """Record a peer connection in the internal registries.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        ws : WebSocket
            The live WebSocket connection.
        """
        self._connections[peer_id] = ws
        if peer_id not in self.peers:
            self.peers[peer_id] = RemotePolicyProxy(peer_id, self)

    def _unregister_peer(self, peer_id: str) -> None:
        """Remove a peer after its connection closes.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        """
        self._connections.pop(peer_id, None)
        self.peers.pop(peer_id, None)

    # ------------------------------------------------------------------
    # RPC dispatch (inbound)
    # ------------------------------------------------------------------

    def _execute_rpc(self, path: list[str], args: list, kwargs: dict) -> Any:
        """Execute a dotted-path method call on the local policy.

        Parameters
        ----------
        path : list[str]
            Attribute chain relative to the local policy object,
            e.g. ``["central", "memory", "memorize"]``.
        args : list
            Positional arguments.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        Any
            Return value of the invoked method.

        Raises
        ------
        AttributeError
            If any segment of *path* does not exist on the target.
        """
        obj = self._local_policy
        if obj is None:
            raise RuntimeError("Communication has no reference to the local policy.")
        for segment in path:
            obj = getattr(obj, segment)
        return obj(*args, **kwargs)

    # ------------------------------------------------------------------
    # RPC dispatch (outbound)
    # ------------------------------------------------------------------

    def _send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC call to a peer and block until the response arrives.

        Parameters
        ----------
        peer_id : str
            Target peer ``global_id``.
        path : list[str]
            Dotted attribute chain on the remote policy.
        args : tuple
            Positional arguments.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        Any
            The deserialized return value from the remote call.

        Raises
        ------
        ConnectionError
            If the peer is not connected.
        RuntimeError
            If the remote side returned an error.
        """
        ws = self._connections.get(peer_id)
        if ws is None:
            raise ConnectionError(f"No connection to peer {peer_id}")

        request_id = str(_uuid.uuid4())
        event = threading.Event()
        slot: Dict[str, Any] = {"event": event}
        self._pending_rpcs[request_id] = slot

        req = protocol.make_request("rpc.call", {
            "path": path,
            "args": list(args),
            "kwargs": dict(kwargs),
        }, request_id=request_id)

        asyncio.run_coroutine_threadsafe(
            ws.send(protocol.encode(req)),
            self._event_loop,
        )

        event.wait(timeout=60.0)
        self._pending_rpcs.pop(request_id, None)

        if "error" in slot:
            err = slot["error"]
            raise RuntimeError(f"Remote RPC error: {err.get('message', err)}")

        return slot.get("result")
