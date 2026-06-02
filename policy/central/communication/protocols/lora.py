"""LoRa communication protocol (scaffold / planned transport).

This module sketches a future :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`
transport for **LoRa** -- a long-range, low-bandwidth radio link well
suited to peer-to-peer policy gossip across kilometres where no IP
network exists. It is intentionally *not implemented yet*: every method
that would touch the radio raises :class:`NotImplementedError` with a
note on what a real implementation must do.

What lands here so the rest of laila can already "see" LoRa:

- :attr:`protocol_name` ``"lora"`` and the ``lora://`` URI scheme, so
  ``laila.request(peer, comm_protocol="lora")`` and URI-based peering
  resolve to this class once a real driver exists.
- The full method surface of
  :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`, documenting the contract a
  LoRa driver must satisfy.

Implementation notes for a future contributor
----------------------------------------------
LoRa is half-duplex, framed, and lossy. A real implementation should:

- Own a serial/SPI link to a LoRa modem on a background thread (mirror
  the dedicated-event-loop pattern in
  :class:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`).
- Fragment/reassemble RPC frames to fit the small LoRa MTU and add an
  ack/retry layer because delivery is not guaranteed.
- Map a node address (``lora://<node-id>``) to a peer ``global_id`` via
  the same ``peer.connect`` handshake used by the TCP/IP transport.
"""

from __future__ import annotations

from typing import Any, ClassVar

from .base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL

_NOT_IMPLEMENTED = (
    "The LoRa transport is a planned protocol and is not implemented yet. "
    "Use the TCP/IP protocol (comm_protocol='tcpip') for now."
)


class _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_COMM_PROTOCOL):
    """Planned LoRa radio transport (scaffold; raises until implemented).

    Subclass of :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` reserving the
    ``"lora"`` token and ``lora://`` URI scheme. All transport methods
    raise :class:`NotImplementedError` for now -- the class exists so
    transport selection (``comm_protocol="lora"``) and URI routing have
    a real target to resolve to.
    """

    protocol_name: ClassVar[str] = "lora"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``lora://`` URIs for this transport."""
        return uri.startswith("lora://")

    def start(self) -> None:
        """Bring up the LoRa modem link. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def stop(self) -> None:
        """Tear down the LoRa modem link. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def add_peer(self, uri: str, secret: str) -> str:
        """Peer with a remote node over LoRa. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC frame over LoRa. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def has_peer(self, peer_id: str) -> bool:
        """No LoRa peers are ever held by this scaffold."""
        return False
