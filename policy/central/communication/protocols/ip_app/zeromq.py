"""ZeroMQ (and nanomsg/nng) communication transport.

Brokerless message transport built on the :class:`_BrokerRPCProtocol`
carrier. ZeroMQ has no central broker, so each policy *binds* a ``PULL``
socket on a deterministic endpoint derived from its ``global_id`` (its
inbox) and addresses a peer by ``connect``-ing a ``PUSH`` socket to the
peer's endpoint. Request/response correlation is handled by the carrier.

- ``protocol_name`` ``"zeromq"`` (aliases ``zmq`` / ``nanomsg`` / ``nng``)
- URI scheme ``zmq://<peer_policy_id>``
- transport endpoints default to ``ipc://`` under the temp dir; set
  ``endpoint_scheme="tcp"`` to use TCP instead.

``pyzmq`` is imported lazily; a missing library raises a clear,
actionable error.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.broker import _BrokerRPCProtocol

log = logging.getLogger(__name__)

_INSTALL_HINT = (
    "The ZeroMQ transport requires pyzmq. Install it with "
    "`pip install laila-core[zmq]` (or `pip install pyzmq`)."
)


class _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL(_BrokerRPCProtocol):
    """ZeroMQ PUSH/PULL brokerless transport."""

    protocol_name: ClassVar[str] = "zeromq"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"zeromq", "zmq", "nanomsg", "nng"}
    )

    endpoint_scheme: str = Field(default="ipc")
    tcp_host: str = Field(default="127.0.0.1")

    _ctx: Any = PrivateAttr(default=None)
    _zmq: Any = PrivateAttr(default=None)
    _pull: Any = PrivateAttr(default=None)
    _pushes: dict = PrivateAttr(default_factory=dict)
    _recv_task: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"zeromq"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``zmq://`` and ``zeromq://`` URIs."""
        return uri.startswith("zmq://") or uri.startswith("zeromq://")

    def _endpoint_for(self, policy_id: str) -> str:
        digest = hashlib.sha1(str(policy_id).encode("utf-8")).hexdigest()[:16]
        if self.endpoint_scheme == "ipc":
            return f"ipc://{os.path.join(tempfile.gettempdir(), f'laila_zmq_{digest}.ipc')}"
        # tcp fallback uses a deterministic high port from the digest
        port = 20000 + (int(digest, 16) % 20000)
        return f"tcp://{self.tcp_host}:{port}"

    def _inbox_topic(self) -> str:
        pid = self._communication.policy_id if self._communication else None
        return self._endpoint_for(pid)

    def _peer_inbox(self, peer_policy_id: str) -> str:
        return self._endpoint_for(peer_policy_id)

    async def _broker_connect(self) -> None:
        try:
            import zmq  # noqa: PLC0415
            import zmq.asyncio  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised when dep absent
            raise RuntimeError(_INSTALL_HINT) from exc
        self._ctx = zmq.asyncio.Context()
        self._zmq = zmq

    async def _broker_subscribe(self, topic: str) -> None:
        # `topic` is our own inbox endpoint; bind a PULL and consume it.
        self._pull = self._ctx.socket(self._zmq.PULL)
        self._pull.bind(topic)
        self._recv_task = asyncio.ensure_future(self._recv_loop())

    async def _recv_loop(self) -> None:
        try:
            while True:
                data = await self._pull.recv()
                self._feed_message(data)
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            log.debug("ZeroMQ recv loop ended", exc_info=True)

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        push = self._pushes.get(topic)
        if push is None:
            push = self._ctx.socket(self._zmq.PUSH)
            push.connect(topic)
            self._pushes[topic] = push
        await push.send(data)

    async def _broker_close(self) -> None:
        if self._recv_task is not None:
            self._recv_task.cancel()
        for push in self._pushes.values():
            try:
                push.close(linger=0)
            except Exception:  # noqa: BLE001
                pass
        self._pushes.clear()
        if self._pull is not None:
            try:
                self._pull.close(linger=0)
            except Exception:  # noqa: BLE001
                pass
            self._pull = None
        if self._ctx is not None:
            try:
                self._ctx.term()
            except Exception:  # noqa: BLE001
                pass
            self._ctx = None
