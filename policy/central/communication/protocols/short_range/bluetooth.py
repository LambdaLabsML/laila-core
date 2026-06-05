"""Bluetooth communication protocol (scaffold / planned transport).

This module sketches a future :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`
transport for **Bluetooth** -- short-range, IP-less peer-to-peer links
between nearby devices (e.g. an edge sensor handing entries to a laptop
without any network). It is intentionally *not implemented yet*: every
method that would touch the radio raises :class:`NotImplementedError`
with a note on what a real implementation must do.

What lands here so the rest of laila can already "see" Bluetooth:

- :attr:`protocol_name` ``"bluetooth"`` and the ``bt://`` URI scheme, so
  ``laila.request(peer, comm_protocol="bluetooth")`` and URI-based
  peering resolve to this class once a real driver exists.
- The full method surface of
  :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`, documenting the contract a
  Bluetooth driver must satisfy.

Implementation notes for a future contributor
----------------------------------------------
A real implementation should build on the
:class:`_P2PStreamRPCProtocol` carrier: use an RFCOMM (Bluetooth
Classic) socket or a GATT characteristic (BLE) as the byte stream, and
map a device address (``bt://<MAC>``) to a peer ``global_id`` via the
shared ``peer.connect`` handshake.
"""

from __future__ import annotations

from typing import Any, ClassVar

from ..base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL

_NOT_IMPLEMENTED = (
    "The Bluetooth transport is a planned protocol and is not implemented yet. "
    "Use the TCP/IP protocol (comm_protocol='tcpip') for now."
)


class _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_COMM_PROTOCOL):
    """Planned Bluetooth transport (scaffold; raises until implemented).

    Subclass of :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` reserving the
    ``"bluetooth"`` token and ``bt://`` URI scheme. All transport methods
    raise :class:`NotImplementedError` for now -- the class exists so
    transport selection (``comm_protocol="bluetooth"``) and URI routing
    have a real target to resolve to.
    """

    protocol_name: ClassVar[str] = "bluetooth"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"bluetooth", "bt", "ble"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"bluetooth"`` plus aliases (``bt``, ``ble``)."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``bt://`` URIs for this transport."""
        return uri.startswith("bt://")

    def start(self) -> None:
        """Bring up the Bluetooth link. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def stop(self) -> None:
        """Tear down the Bluetooth link. No-op (nothing is ever started).

        ``stop`` must be idempotent and safe per the base contract, so
        the scaffold returns cleanly rather than raising.
        """
        return None

    def connect(self, uri: str, secret: str) -> str:
        """Peer with a remote device over Bluetooth. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC frame over Bluetooth. Not implemented yet."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def has_peer(self, peer_id: str) -> bool:
        """No Bluetooth peers are ever held by this scaffold."""
        return False
