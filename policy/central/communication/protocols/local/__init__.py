"""In-process / local transports: loopback and Unix domain socket."""

from .loopback import _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL
from .unixsocket import _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL

__all__ = [
    "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
]
