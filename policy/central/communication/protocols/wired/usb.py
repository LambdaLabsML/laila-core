"""USB communication transport (CDC-ACM serial).

The most portable USB device class for a byte stream is CDC-ACM, which
the OS exposes as a serial port (e.g. ``/dev/ttyACM0``). This transport
therefore subclasses the serial (UART) transport. Bulk/HID modes would
require ``pyusb`` and are out of scope for this CDC-ACM transport.

- ``protocol_name`` ``"usb"`` (aliases ``cdc-acm`` / ``cdcacm``)
- URI scheme ``usb:///dev/ttyACM0``
"""

from __future__ import annotations

from typing import ClassVar

from .uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """USB CDC-ACM transport (serial over USB)."""

    protocol_name: ClassVar[str] = "usb"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"usb", "cdc-acm", "cdcacm"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``usb://`` URIs."""
        return uri.startswith("usb://")
