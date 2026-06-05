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

Concrete carriers (:class:`._stream._StreamRPCProtocol`,
:class:`._datagram._DatagramRPCProtocol`) build their wire-specific
logic on top of these helpers.
"""

from __future__ import annotations

import threading
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ... import protocol as rpc_protocol
from ..base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from . import codec as _codec

#: Reserved dotted-path for the liveness ping control frame. Intercepted
#: in :meth:`_build_response` before it can reach the policy.
_COMM_PING_PATH = ["__comm_ping__"]


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

    #: Carriers are abstract; concrete transports override.
    protocol_name: ClassVar[str] = "carrier"

    _started: bool = PrivateAttr(default=False)
    _connections: dict[str, Any] = PrivateAttr(default_factory=dict)
    _pending_rpcs: dict[str, dict[str, Any]] = PrivateAttr(default_factory=dict)
    _inbound_executor: Any = PrivateAttr(default=None)

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

    def _build_response(self, msg: dict) -> dict:
        """Execute an inbound ``rpc.call`` *msg* and return a response dict.

        Runs synchronously on the calling thread (carriers call this from
        a worker thread, never the I/O loop). Unknown methods and
        execution failures are mapped to JSON-RPC error envelopes.
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
        try:
            result = self._communication._execute_rpc(path, args, kwargs)
            return rpc_protocol.make_result(request_id, result)
        except Exception as exc:  # noqa: BLE001 - errors travel back to the caller
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

    def _handle_request_frame(self, msg: dict, reply) -> None:
        """Centralized inbound dispatch: ping fast-path + admission + queue.

        ``reply(resp_dict)`` sends a response back over the transport and
        must be safe to invoke from a worker thread.

        1. A liveness ping is answered inline, *before* admission, so it
           never queues and never gets rejected.
        2. Otherwise a slot is claimed non-blocking; past capacity the
           request is rejected immediately with ``ERR_BUSY`` (it never
           enters the executor backlog, so the backlog stays bounded).
        3. Admitted requests run :meth:`_build_response` on a worker
           thread, releasing the slot when the reply is ready.
        """
        if self._is_ping_frame(msg):
            reply(rpc_protocol.make_result(msg.get("id"), "pong"))
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
        :class:`RemotePolicyProxy` so user code can immediately reach
        the new peer.
        """
        self._connections[peer_id] = handle
        if self._communication is not None:
            self._communication._register_peer(peer_id)

    def _unregister_peer(self, peer_id: str) -> None:
        """Drop *peer_id*'s handle and notify the hub. Idempotent."""
        self._connections.pop(peer_id, None)
        if self._communication is not None:
            self._communication._unregister_peer(peer_id)

    def has_peer(self, peer_id: str) -> bool:
        """Return ``True`` if a live connection to *peer_id* is held."""
        return peer_id in self._connections

    def disconnect(self, peer_id: str) -> None:
        """Tear down a single peer's connection. Idempotent.

        Carriers that hold a closable handle (socket/writer) override this
        to close it; the base just drops the registry entry and notifies
        the hub.
        """
        if peer_id in self._connections:
            self._unregister_peer(peer_id)

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
        try:
            return self.send_rpc(peer_id, list(_COMM_PING_PATH), (), {}) == "pong"
        except Exception:  # noqa: BLE001
            return False

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
        """Block until *slot* is resolved (or :attr:`rpc_timeout` elapses)."""
        completed = slot["event"].wait(timeout=self.rpc_timeout)
        self._pending_rpcs.pop(request_id, None)
        if not completed:
            raise TimeoutError(
                f"RPC to peer timed out after {self.rpc_timeout}s "
                f"on {type(self).__name__}."
            )
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
