"""AMQP (RabbitMQ) communication transport.

Broker-mediated JSON-RPC built on the :class:`_BrokerRPCProtocol`
carrier via ``aio-pika``. Each policy declares an auto-delete inbox
queue named ``laila/inbox/<policy_id>`` and addresses peers by
publishing to their queue through the default exchange.

- ``protocol_name`` ``"amqp"`` (aliases ``amqps`` / ``rabbitmq``)
- URI scheme ``amqp://<peer_policy_id>`` (the broker is configured via
  ``broker_url``)

``aio-pika`` is imported lazily; a missing library raises a clear,
actionable error and an unreachable broker surfaces as a connection
error.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.broker import _BrokerRPCProtocol

log = logging.getLogger(__name__)

_INSTALL_HINT = (
    "The AMQP transport requires aio-pika. Install it with "
    "`pip install laila-core[amqp]` (or `pip install aio-pika`)."
)


class _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL(_BrokerRPCProtocol):
    """AMQP/RabbitMQ broker-mediated transport.

    Parameters
    ----------
    broker_url : str, default ``"amqp://guest:guest@127.0.0.1/"``
        Connection URL for the AMQP broker.
    """

    protocol_name: ClassVar[str] = "amqp"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"amqp", "amqps", "rabbitmq"})

    broker_url: str = Field(default="amqp://guest:guest@127.0.0.1/")

    _conn: Any = PrivateAttr(default=None)
    _channel: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"amqp"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``amqp://`` and ``amqps://`` URIs."""
        return uri.startswith("amqp://") or uri.startswith("amqps://")

    async def _broker_connect(self) -> None:
        try:
            import aio_pika  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised when dep absent
            raise RuntimeError(_INSTALL_HINT) from exc
        try:
            self._conn = await aio_pika.connect_robust(self.broker_url)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(
                f"Could not reach AMQP broker at {self.broker_url}: {exc}"
            ) from exc
        self._channel = await self._conn.channel()

    async def _broker_subscribe(self, topic: str) -> None:
        queue = await self._channel.declare_queue(topic, auto_delete=True)

        async def _on_message(message: Any) -> None:
            async with message.process():
                self._feed_message(message.body)

        await queue.consume(_on_message)

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        import aio_pika  # noqa: PLC0415

        await self._channel.default_exchange.publish(
            aio_pika.Message(body=data), routing_key=topic
        )

    async def _broker_close(self) -> None:
        if self._conn is not None:
            try:
                await asyncio.wait_for(self._conn.close(), timeout=2.0)
            except Exception:  # noqa: BLE001
                pass
            self._conn = None
            self._channel = None
