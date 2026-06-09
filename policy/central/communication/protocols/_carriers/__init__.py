"""Reusable RPC *carrier* base classes for communication transports.

A laila communication transport only has to move framed JSON-RPC bytes
between two peers; the handshake, peer registry, future virtualisation
and inbound dispatch are identical across every wire. Rather than
re-implement that for each of the dozens of supported transports, the
carriers in this sub-package implement the shared machinery once and
expose a small set of hooks that a concrete transport fills in.

Carriers
--------
- :class:`_StreamRPCProtocol` (:mod:`._stream`)
    Reliable, ordered, duplex byte streams (TCP, TLS, Unix sockets,
    serial lines, USB-CDC, RFCOMM, ...). Subclasses provide an async
    server factory and an async client-connect factory; the carrier
    does length-prefixed framing, the ``peer.connect`` handshake, the
    receive loop and the pending-RPC table.
- :class:`_DatagramRPCProtocol` (:mod:`._datagram`)
    Unreliable / segmented links (UDP, CoAP, LoRa, ESP-NOW, CAN, ...).
    Adds MTU fragmentation/reassembly, sequence numbers and ack/retry
    on top of a packet ``send``/``receive`` pair supplied by the
    subclass.

All carriers share the codec in :mod:`._codec` (pluggable ``json`` /
``msgpack`` with identical laila future-tagging semantics) and the
inbound-dispatch / peer-registration helpers in :mod:`._base`.
"""

from .base import _CarrierRPCProtocol
from .broker import _BrokerRPCProtocol
from .datagram import _DatagramRPCProtocol
from .register import _RegisterRPCProtocol
from .stream import _StreamRPCProtocol

__all__ = [
    "_BrokerRPCProtocol",
    "_CarrierRPCProtocol",
    "_DatagramRPCProtocol",
    "_RegisterRPCProtocol",
    "_StreamRPCProtocol",
]
