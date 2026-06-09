"""IP / application-protocol transports.

Working transports built on the reusable carriers: raw TCP, UDP and
TLS. The WebSocket/WSS transport lives in the package-level
``tcpip`` module (the original reference transport) and is re-exported
here for catalog completeness.
"""

from ..tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
from .ethernet import _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL
from .tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL
from .tls import _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL
from .udp import _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL

__all__ = [
    "_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
]
