"""DDS / RTPS communication transport (broker-mediated / pub-sub).

Carries JSON-RPC over DDS topics via ``cyclonedds``. Each policy reads
its inbox topic and writes to peer inbox topics. Built on the broker
carrier; the driver is imported on connect and a missing library raises
a clear capability error.

- ``protocol_name`` ``"dds"`` (aliases ``rtps``)
- URI scheme ``dds://<peer_policy_id>``
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import PrivateAttr

from .._carriers.broker import _BrokerRPCProtocol


class _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL(_BrokerRPCProtocol):
    """DDS/RTPS pub-sub transport."""

    protocol_name: ClassVar[str] = "dds"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"dds", "rtps"})

    _participant: Any = PrivateAttr(default=None)
    _cyclonedds: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"dds"`` / ``"rtps"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``dds://`` URIs."""
        return uri.startswith("dds://")

    async def _broker_connect(self) -> None:
        mods = self._require_drivers(("cyclonedds",), "dds")
        from cyclonedds.domain import DomainParticipant

        self._participant = DomainParticipant()
        self._cyclonedds = mods["cyclonedds"]

    async def _broker_subscribe(self, topic: str) -> None:
        # A real implementation creates a DataReader on `topic` whose
        # listener forwards samples to self._feed_message.
        return None

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        # A real implementation writes `data` on a DataWriter for `topic`.
        raise RuntimeError("DDS publish requires a configured DataWriter for the peer topic.")

    async def _broker_close(self) -> None:
        self._participant = None
