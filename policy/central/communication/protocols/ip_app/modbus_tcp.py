"""Modbus-TCP communication transport.

Carries JSON-RPC over a Modbus-TCP holding-register *mailbox* using the
:class:`_RegisterRPCProtocol` carrier and ``pymodbus``. The local policy
acts as the Modbus master: it writes framed messages into the peer's
inbox register block and polls its own outbox block for replies. The
peer must expose a cooperating Modbus datastore as the mailbox.

- ``protocol_name`` ``"modbus-tcp"`` (aliases ``modbus`` / ``modbustcp``)
- URI scheme ``modbustcp://host:port``

``pymodbus`` is imported lazily; a missing library raises a clear,
actionable error.
"""

from __future__ import annotations

import struct
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.register import _RegisterRPCProtocol
from .._carriers.uri import split_host_port


class _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL(_RegisterRPCProtocol):
    """Modbus-TCP register-mailbox transport (master side)."""

    protocol_name: ClassVar[str] = "modbus-tcp"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"modbus-tcp", "modbus", "modbustcp"}
    )

    host: str = Field(default="127.0.0.1")
    port: int = Field(default=502)
    unit_id: int = Field(default=1)
    inbox_base: int = Field(default=0)
    outbox_base: int = Field(default=2000)

    _client: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"modbus-tcp"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``modbustcp://`` URIs."""
        return uri.startswith("modbustcp://")

    async def _open_bus(self) -> None:
        self._require_drivers(("pymodbus",), "modbus")
        from pymodbus.client import AsyncModbusTcpClient  # noqa: PLC0415

        self._client = AsyncModbusTcpClient(self.host, port=self.port)
        await self._client.connect()

    async def _close_bus(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

    @staticmethod
    def _to_words(data: bytes) -> list[int]:
        padded = data + (b"\x00" if len(data) % 2 else b"")
        words = [len(data)]
        words += [struct.unpack(">H", padded[i : i + 2])[0] for i in range(0, len(padded), 2)]
        return words

    @staticmethod
    def _from_words(words: list[int]) -> bytes:
        length = words[0]
        body = b"".join(struct.pack(">H", w) for w in words[1:])
        return body[:length]

    async def _deliver(self, data: bytes) -> None:
        await self._client.write_registers(
            self.outbox_base, self._to_words(data), slave=self.unit_id
        )

    async def _poll_inbound(self) -> bytes | None:
        rr = await self._client.read_holding_registers(
            self.inbox_base, count=1, slave=self.unit_id
        )
        if rr.isError() or not rr.registers or rr.registers[0] == 0:
            return None
        length = rr.registers[0]
        words_needed = 1 + (length + 1) // 2
        full = await self._client.read_holding_registers(
            self.inbox_base, count=words_needed, slave=self.unit_id
        )
        # clear the length cell to acknowledge consumption
        await self._client.write_register(self.inbox_base, 0, slave=self.unit_id)
        return self._from_words(full.registers)
