"""MQTT (and MQTT-SN) communication transport.

Broker-mediated JSON-RPC built on the :class:`_BrokerRPCProtocol`
carrier. Each policy subscribes to its inbox topic
``laila/inbox/<policy_id>`` on the MQTT broker; peers address each other
by publishing to those topics.

- ``protocol_name`` ``"mqtt"`` (aliases ``mqtts`` / ``mqtt-sn``)
- URI scheme ``mqtt://<peer_policy_id>`` (the broker itself is given by
  ``broker_host`` / ``broker_port``)

``paho-mqtt`` is imported lazily; a missing library raises a clear,
actionable error and a missing broker surfaces as a connection error.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.broker import _BrokerRPCProtocol

log = logging.getLogger(__name__)

_INSTALL_HINT = (
    "The MQTT transport requires paho-mqtt. Install it with "
    "`pip install laila-core[mqtt]` (or `pip install paho-mqtt`)."
)


class _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL(_BrokerRPCProtocol):
    """MQTT broker-mediated transport.

    Parameters
    ----------
    broker_host : str, default ``"127.0.0.1"``
        MQTT broker hostname.
    broker_port : int, default ``1883``
        MQTT broker port.
    """

    protocol_name: ClassVar[str] = "mqtt"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"mqtt", "mqtts", "mqtt-sn"})

    broker_host: str = Field(default="127.0.0.1")
    broker_port: int = Field(default=1883)

    _client: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"mqtt"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``mqtt://`` and ``mqtts://`` URIs."""
        return uri.startswith("mqtt://") or uri.startswith("mqtts://")

    async def _broker_connect(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:  # pragma: no cover - exercised when dep absent
            raise RuntimeError(_INSTALL_HINT) from exc

        client = mqtt.Client()

        def _on_message(_client, _userdata, message) -> None:
            self._feed_message(message.payload)

        client.on_message = _on_message
        try:
            client.connect(self.broker_host, self.broker_port)
        except OSError as exc:
            raise ConnectionError(
                f"Could not reach MQTT broker at {self.broker_host}:{self.broker_port}: {exc}"
            ) from exc
        client.loop_start()
        self._client = client

    async def _broker_subscribe(self, topic: str) -> None:
        self._client.subscribe(topic)

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        self._client.publish(topic, data)

    async def _broker_close(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
            self._client = None
