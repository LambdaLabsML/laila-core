"""In-process loopback communication transport.

The degenerate transport: both policies live in the *same* process, so
there is no socket at all -- an RPC is dispatched by directly invoking
the target policy's inbound handler. It still goes through the codec
(encode then decode) so future-shaped results are virtualised into
:class:`RemoteFuture` proxies exactly as they would be over a real wire,
which makes loopback a faithful, dependency-free way to exercise the
full inter-policy path in one process.

- ``protocol_name`` ``"loopback"`` (aliases ``local`` / ``inproc``)
- URI scheme ``loopback://<policy_global_id>``

Peers are matched through a process-wide registry keyed by the owning
policy's ``global_id``.
"""

from __future__ import annotations

import uuid as _uuid
from typing import Any, ClassVar

from .._carriers.base import _CarrierRPCProtocol
from .._carriers.uri import uri_authority

#: process-wide registry: policy global_id -> loopback protocol instance
_LOOPBACK_REGISTRY: dict[str, _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL] = {}


class _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL(_CarrierRPCProtocol):
    """Same-process loopback transport.

    Notes
    -----
    Registers itself under its owning policy's ``global_id`` on
    :meth:`start`, so another in-process policy can peer with
    ``loopback://<that-global-id>``.
    """

    protocol_name: ClassVar[str] = "loopback"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"loopback", "local", "inproc"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"loopback"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``loopback://`` URIs."""
        return uri.startswith("loopback://")

    def _policy_id(self) -> str | None:
        return self._communication.policy_id if self._communication else None

    def start(self) -> None:
        """Register this endpoint in the process-wide loopback registry."""
        if self._started:
            return
        self._ensure_executor()
        pid = self._policy_id()
        if pid is not None:
            _LOOPBACK_REGISTRY[str(pid)] = self
        self._started = True

    def stop(self) -> None:
        """Deregister and drop all peers (idempotent)."""
        if not self._started:
            return
        pid = self._policy_id()
        if pid is not None:
            _LOOPBACK_REGISTRY.pop(str(pid), None)
        for peer_id in list(self._connections):
            self._unregister_peer(peer_id)
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    def connect(self, uri: str, secret: str) -> str:
        """Peer with another in-process policy named by *uri*."""
        self.start()
        target_pid = uri_authority(uri)
        target = _LOOPBACK_REGISTRY.get(str(target_pid))
        if target is None:
            raise ConnectionError(
                f"No in-process loopback endpoint for policy {target_pid!r}. "
                "Start the target policy's loopback connection first."
            )
        if secret != target.peer_secret_key:
            raise ConnectionError("Invalid peer secret key.")

        my_pid = self._policy_id()
        # Register both directions so either side can call the other.
        self._register_peer(str(target_pid), target)
        if my_pid is not None:
            target._register_peer(str(my_pid), self)
        return str(target_pid)

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Dispatch an RPC directly into the peer policy (zero-copy).

        Since both policies are in the same process, loopback skips the
        codec entirely and passes live objects (the returned value is the
        target's real result/future, not a serialised envelope). This is
        the fastest possible path -- no encode/decode, no socket.
        """
        target = self._connections.get(peer_id)
        if target is None:
            raise ConnectionError(f"No connection to peer {peer_id}")
        req = self._make_rpc_request(path, args, kwargs, str(_uuid.uuid4()))
        # Run on the TARGET so its _execute_rpc walks the target policy.
        resp = target._build_response(req)
        if "error" in resp:
            err = resp["error"]
            raise RuntimeError(f"Remote RPC error: {err.get('message', err)}")
        return resp.get("result")

    def connect_loopback(self, policy_global_id: str, secret: str) -> str:
        """Convenience wrapper building the ``loopback://`` URI."""
        return self.connect(f"loopback://{policy_global_id}", secret)
