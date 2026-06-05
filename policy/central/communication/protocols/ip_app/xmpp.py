"""XMPP communication transport (broker-mediated).

Carries JSON-RPC over an XMPP server via ``slixmpp``, addressing peers
by their inbox (a per-policy JID resource). Built on the broker carrier;
the driver is imported on connect and a missing library raises a clear
capability error.

- ``protocol_name`` ``"xmpp"`` (aliases ``jabber``)
- URI scheme ``xmpp://<peer_policy_id>``
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.broker import _BrokerRPCProtocol


class _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL(_BrokerRPCProtocol):
    """XMPP broker-mediated transport."""

    protocol_name: ClassVar[str] = "xmpp"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"xmpp", "jabber"})

    jid: str = Field(default="")
    password: str = Field(default="")
    server_host: str = Field(default="127.0.0.1")
    server_port: int = Field(default=5222)

    _client: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"xmpp"`` / ``"jabber"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``xmpp://`` URIs."""
        return uri.startswith("xmpp://")

    async def _broker_connect(self) -> None:
        self._require_drivers(("slixmpp",), "xmpp")
        import slixmpp  # noqa: PLC0415

        self._client = slixmpp.ClientXMPP(self.jid, self.password)
        self._client.connect((self.server_host, self.server_port))

    async def _broker_subscribe(self, topic: str) -> None:
        # XMPP delivers to our JID directly; messages are routed to
        # _feed_message by the client's message handler.
        if self._client is not None:
            self._client.add_event_handler(
                "message", lambda msg: self._feed_message(str(msg["body"]).encode())
            )

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        self._client.send_message(mto=topic, mbody=data.decode("latin-1"))

    async def _broker_close(self) -> None:
        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
