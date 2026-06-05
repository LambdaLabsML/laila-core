"""Lazy datagram carrier for driver/hardware-backed packet radios.

Many packet transports (Zigbee, Thread, Z-Wave, ANT, ESP-NOW, ...) ride
on the :class:`_DatagramRPCProtocol` carrier but need a third-party
driver and real radio hardware. :class:`_LazyDatagramRPCProtocol`
standardises their connection hook: it imports the declared driver(s)
via :meth:`_require_drivers` (raising a clear ``laila-core[<extra>]``
error when missing) and then defers to :meth:`_setup_endpoint`, which a
concrete transport overrides with the real radio bring-up.

For transports with no off-the-shelf Python driver (the device is
firmware, e.g. ESP-NOW), ``_DRIVER_MODULES`` is empty and
:meth:`_setup_endpoint` raises a clear, actionable runtime error
describing the hardware requirement -- never a silent stub.
"""

from __future__ import annotations

from typing import Any, ClassVar

from .datagram import _DatagramRPCProtocol
from .uri import uri_authority


class _LazyDatagramRPCProtocol(_DatagramRPCProtocol):
    """Datagram carrier that lazy-loads a radio driver on start."""

    #: import names required before the radio can be brought up.
    _DRIVER_MODULES: ClassVar[tuple] = ()
    #: optional-extra name for the install hint.
    _DRIVER_EXTRA: ClassVar[str] = ""

    async def _create_datagram_endpoint(self) -> tuple[Any, Any]:
        mods = self._require_drivers(self._DRIVER_MODULES, self._DRIVER_EXTRA)
        return await self._setup_endpoint(mods)

    async def _setup_endpoint(self, drivers: dict) -> tuple[Any, Any]:
        """Bring up the real radio endpoint.

        Overridden by transports with a concrete driver integration. The
        default makes the hardware requirement explicit for transports
        whose device is firmware with no host-side Python driver.
        """
        raise RuntimeError(
            f"The {self.protocol_name!r} transport requires dedicated radio "
            "hardware/firmware and an out-of-band interface configuration; "
            "no host-side driver endpoint is available to bring up automatically."
        )

    async def _resolve_peer_addr(self, uri: str) -> Any:
        """Default: the URI authority is the radio node address."""
        return uri_authority(uri)
