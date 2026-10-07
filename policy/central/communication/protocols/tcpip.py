"""TCP/IP (WebSocket) communication protocol implementation.

Concrete :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` that uses the
``websockets`` library to expose a long-lived bidirectional channel
between two policies. The on-the-wire RPC envelope is defined in
:mod:`.protocol`, the actual socket plumbing lives in
:mod:`.connection`.

Architecturally the protocol owns:

- A dedicated background asyncio event loop running on a daemon
  thread (``_event_loop`` / ``_loop_thread``). All websocket I/O is
  scheduled there so the rest of laila does not have to be
  asyncio-aware.
- A websocket *server* (``_server``) accepting inbound peerings.
- A dict of *connections* (``_connections``) keyed by remote-policy
  ``global_id``, each holding the live websocket for that peer.
- A *pending-RPC table* (``_pending_rpcs``) keyed by request id. Each
  outbound :meth:`send_rpc` registers a slot here, waits on a
  :class:`threading.Event`, and is woken up by the inbound dispatcher
  when the matching response arrives.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid as _uuid
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .. import protocol as rpc_protocol
from ._carriers.loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread
from .base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_COMM_PROTOCOL):
    """WebSocket-based peer-to-peer communication protocol.

    Implements the :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` contract
    over ``ws://`` / ``wss://``. RPC frames are JSON-encoded by
    :mod:`.protocol`; the connection state machine (handshake,
    message dispatch, disconnect) lives in :mod:`.connection`.

    Parameters
    ----------
    host : str, default ``"0.0.0.0"``
        Bind address for the WebSocket server. ``"0.0.0.0"`` listens
        on every interface; pass a specific IP to restrict reachability.
    port : int, default ``0``
        TCP port for the WebSocket server. ``0`` lets the OS pick a
        free port; the chosen port is then exposed via :attr:`bound_port`.
    peer_secret_key : str
        Shared secret that remote peers must present during the
        handshake. Defaults to a fresh UUID4 hex if not set, so each
        instance gets a unique secret out of the box.

    Notes
    -----
    The protocol auto-bootstraps its own background event loop the
    first time :meth:`start` is called, and tears it down on
    :meth:`stop`. Clients of this class never see the loop directly --
    they call :meth:`send_rpc`/``add_peer`` synchronously and the loop
    is hidden behind :func:`asyncio.run_coroutine_threadsafe`.
    """

    protocol_name: ClassVar[str] = "tcpip"
    #: WebSocket-focused tokens. Raw TCP/UDP/TLS now have dedicated
    #: transports under ``protocols/ip_app/`` that own the ``tcp`` /
    #: ``udp`` / ``tls`` tokens, so ``tcpip`` claims only the
    #: WebSocket family to keep the token map globally disjoint.
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"tcpip", "ws", "wss", "websocket"})

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=0)
    peer_secret_key: str = Field(default_factory=lambda: _uuid.uuid4().hex)
    #: Seconds a blocking :meth:`send_rpc` waits for the response.
    rpc_timeout: float = Field(default=60.0)
    #: Bound on a liveness :meth:`ping` round trip.
    ping_timeout: float = Field(default=5.0)

    _server: Any = PrivateAttr(default=None)
    _connections: dict[str, Any] = PrivateAttr(default_factory=dict)
    _pending_rpcs: dict[str, dict[str, Any]] = PrivateAttr(default_factory=dict)
    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _started: bool = PrivateAttr(default=False)
    _bound_port: int | None = PrivateAttr(default=None)

    @property
    def bound_port(self) -> int:
        """Port the server is currently listening on.

        After :meth:`start`, this returns the actual OS-assigned port
        -- useful when ``port=0`` was requested. Before :meth:`start`
        it falls back to the configured :attr:`port` (which may still
        be ``0``).
        """
        return self._bound_port if self._bound_port is not None else self.port

    # ------------------------------------------------------------------
    # URI routing
    # ------------------------------------------------------------------

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"tcpip"`` plus common aliases (``tcp``, ``tcp-ip``, ``ws``...)."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``ws://`` and ``wss://`` URIs for this protocol."""
        return uri.startswith("ws://") or uri.startswith("wss://")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the dedicated event loop and start accepting WebSocket connections.

        Idempotent: a second call is a no-op once ``_started`` is set.
        Spawns a daemon thread (``_loop_thread``) that owns a fresh
        :class:`asyncio.AbstractEventLoop`, kicks off the server via
        :meth:`_async_start`, then drops into ``loop.run_forever()``.
        Blocks the caller until the server is bound and ready (or
        ten seconds elapse), so by the time this returns
        :attr:`bound_port` is meaningful.
        """
        if self._started:
            return

        start_loop_thread(self, self._async_start, ready_timeout=10.0)
        self._started = True
        policy_id = self._communication.policy_id if self._communication else None
        log.info(
            "TCP/IP protocol started for policy %s on port %s",
            policy_id,
            self.bound_port,
        )

    async def _async_start(self, ready: threading.Event) -> None:
        """Boot the server inside the protocol's event loop and signal *ready*.

        Delegates to :func:`.connection.start_server`, which performs
        the actual ``websockets.serve`` call and stashes the resulting
        server handle on ``self._server``. The *ready* event lets the
        synchronous :meth:`start` caller block until the listener is
        actually bound.
        """
        from .. import connection

        await connection.start_server(self)
        ready.set()

    def stop(self) -> None:
        """Tear down the server, close every peer socket, and stop the loop.

        Idempotent: returns immediately when ``_started`` is False.
        Closes every peer socket (a WebSocket close frame is the
        graceful goodbye; the remote's receive loop ends and it
        unregisters us), closes the server, cancels *and awaits* every
        outstanding task, joins the loop thread and closes the loop,
        then clears the pending-RPC table so a subsequent :meth:`start`
        brings the protocol back up cleanly.
        """
        if not self._started:
            return

        async def _shutdown() -> None:
            close_tasks = []
            for ws in list(self._connections.values()):
                close_tasks.append(asyncio.ensure_future(ws.close()))
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)

            if self._server is not None:
                self._server.close()
                try:
                    await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
                except Exception:
                    pass
                self._server = None

            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._pending_rpcs.clear()
        self._bound_port = None
        self._started = False
        policy_id = self._communication.policy_id if self._communication else None
        log.info("TCP/IP protocol stopped for policy %s", policy_id)

    # ------------------------------------------------------------------
    # Peer management
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Open an outbound WebSocket connection to *uri* and complete the handshake.

        Auto-starts the protocol if needed (so users don't need to
        remember a separate ``start()`` call). The actual handshake
        is run inside the protocol's event loop via
        :func:`.connection.connect_outbound`; this function blocks the
        caller for up to thirty seconds for the handshake to finish.

        Returns
        -------
        str
            The remote policy's ``global_id`` as resolved during the
            handshake.
        """
        self.start()

        from .. import connection

        future = asyncio.run_coroutine_threadsafe(
            connection.connect_outbound(self, uri, secret),
            self._event_loop,
        )
        return future.result(timeout=30.0)

    def _register_peer(self, peer_id: str, ws: Any) -> None:
        """Record an established WebSocket and notify the communication layer.

        Called by :mod:`.connection` after a successful handshake (in
        either direction). The communication layer creates the
        corresponding :class:`RemotePolicyProxy` so user code can
        immediately reach the new peer.
        """
        self._connections[peer_id] = ws
        if self._communication is not None:
            self._communication._register_peer(peer_id)

    def _unregister_peer(self, peer_id: str) -> None:
        """Drop a peer's WebSocket and notify the communication layer.

        Called when the connection closes (either side). Idempotent:
        if the peer is not in the table, this is a no-op.
        """
        self._connections.pop(peer_id, None)
        if self._communication is not None:
            self._communication._unregister_peer(peer_id)

    def has_peer(self, peer_id: str) -> bool:
        """Return ``True`` if a live WebSocket to *peer_id* is currently held."""
        return peer_id in self._connections

    def disconnect(self, peer_id: str) -> None:
        """Close the WebSocket to *peer_id* and drop it. Idempotent.

        The close handshake is the graceful goodbye: the remote's receive
        loop ends with ``ConnectionClosed`` and unregisters us. Bounded
        so a dead peer cannot stall the caller.
        """
        ws = self._connections.get(peer_id)
        if ws is None:
            return
        loop = self._event_loop
        if loop is not None and not loop.is_closed() and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(ws.close(), loop).result(timeout=2.0)
            except Exception:
                log.debug("closing WebSocket to %s failed", peer_id, exc_info=True)
        self._unregister_peer(peer_id)

    # ------------------------------------------------------------------
    # RPC (outbound)
    # ------------------------------------------------------------------

    def ping(self, peer_id: str, timeout: float | None = None) -> bool:
        """Round-trip the reserved ``__comm_ping__`` frame to *peer_id*.

        Answered by the remote's :mod:`.connection` dispatcher before it
        touches the policy. ``False`` on any transport error or timeout,
        so the liveness loop drops only peers that really stopped
        answering.
        """
        if not self.has_peer(peer_id):
            return False
        try:
            return (
                self._send(
                    peer_id,
                    ["__comm_ping__"],
                    (),
                    {},
                    timeout=timeout if timeout is not None else self.ping_timeout,
                )
                == "pong"
            )
        except Exception:
            return False

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC call over the WebSocket to *peer_id* and block for the response.

        Allocates a fresh request id, registers a pending-RPC slot
        keyed by that id, dispatches the encoded frame from the
        protocol's event loop, and waits on a
        :class:`threading.Event` for up to :attr:`rpc_timeout` seconds.
        The inbound dispatcher in :mod:`.connection` flips the event
        when the matching response arrives.

        Raises
        ------
        ConnectionError
            If no live connection to *peer_id* is held.
        TimeoutError
            If no response arrived within :attr:`rpc_timeout`.
        RuntimeError
            If the remote returned an error envelope.
        """
        return self._send(peer_id, path, args, kwargs, timeout=self.rpc_timeout)

    def _send(
        self, peer_id: str, path: list[str], args: tuple, kwargs: dict, *, timeout: float
    ) -> Any:
        ws = self._connections.get(peer_id)
        if ws is None:
            raise ConnectionError(f"No connection to peer {peer_id}")
        loop = self._event_loop
        if loop is None or loop.is_closed():
            raise ConnectionError("TCP/IP transport is shut down.")

        request_id = str(_uuid.uuid4())
        event = threading.Event()
        slot: dict[str, Any] = {"event": event}
        self._pending_rpcs[request_id] = slot

        req = rpc_protocol.make_request(
            "rpc.call",
            {
                "path": path,
                "args": list(args),
                "kwargs": dict(kwargs),
            },
            request_id=request_id,
        )

        try:
            asyncio.run_coroutine_threadsafe(ws.send(rpc_protocol.encode(req)), loop)
        except RuntimeError as exc:
            self._pending_rpcs.pop(request_id, None)
            raise ConnectionError("TCP/IP transport is shut down.") from exc

        completed = event.wait(timeout=timeout)
        self._pending_rpcs.pop(request_id, None)
        if not completed:
            raise TimeoutError(f"RPC to peer {peer_id} timed out after {timeout}s.")

        if "error" in slot:
            err = slot["error"]
            raise RuntimeError(f"Remote RPC error: {err.get('message', err)}")

        return slot.get("result")
