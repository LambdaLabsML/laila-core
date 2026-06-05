"""Communication protocol implementations.

The base contract is :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`; the
reusable RPC carriers live under :mod:`._carriers`. Concrete transports
are grouped into category subpackages (``ip_app``, ``local``,
``lpwan``, ``short_range``, ``cellular``, ``wired``) and re-exported
here for convenience.
"""

from .base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from .ip_app.tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL
from .ip_app.tls import _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL
from .ip_app.udp import _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL
from .local.loopback import _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL
from .local.unixsocket import _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL
from .tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL

__all__ = [
    "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
    "iter_comm_protocols",
    "comm_protocol_for_token",
]


def _autoload_transports() -> None:
    """Import every transport module so new files are discoverable.

    Walks this package's subpackages and imports each module, so a new
    transport added under ``protocols/<category>/`` is registered as a
    :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` subclass with no edits
    anywhere else. Driver libraries are imported lazily inside the
    transports' methods, so this import walk never pulls an optional
    dependency.
    """
    import importlib
    import logging
    import pkgutil

    for _finder, name, _ispkg in pkgutil.walk_packages(__path__, prefix=__name__ + "."):
        try:
            importlib.import_module(name)
        except Exception:  # noqa: BLE001 - a broken optional module must not break import
            logging.getLogger(__name__).debug("autoload skipped %s", name, exc_info=True)


def iter_comm_protocols() -> list[type]:
    """Return every concrete transport class currently loaded."""
    seen: dict[str, type] = {}

    def _walk(cls: type) -> None:
        for sub in cls.__subclasses__():
            seen[sub.__name__] = sub
            _walk(sub)

    _walk(_LAILA_IDENTIFIABLE_COMM_PROTOCOL)
    return [c for c in seen.values() if c.protocol_name not in ("base", "carrier")]


def comm_protocol_for_token(token: str) -> type | None:
    """Return the transport class answering to *token*, or ``None``."""
    for cls in iter_comm_protocols():
        if cls.matches_token(token):
            return cls
    return None


_autoload_transports()
