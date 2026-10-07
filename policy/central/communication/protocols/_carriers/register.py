"""Register / mailbox (master-slave bus) RPC carrier.

:class:`_RegisterRPCProtocol` carries JSON-RPC over a register/byte bus
that has no asynchronous push -- Modbus, I2C, SPI, 1-Wire, EtherNet/IP.
On such buses a message is exchanged by writing it into a *mailbox*
region on the peer and polling the peer's mailbox for replies.

The carrier provides the reliable JSON-RPC layer (handshake,
request/response correlation, off-thread dispatch) and a polling loop;
a concrete transport supplies just two coroutines:

- :meth:`_deliver(data)` -- write one length-prefixed frame to the
  peer's inbox region.
- :meth:`_poll_inbound()` -- read the next length-prefixed frame from
  our own inbox region, or return ``None`` when nothing is pending.

These buses are point-to-point (one peer per link), so addressing is
implicit; the single peer is still tracked by ``global_id`` for
consistency with the other carriers.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from pydantic import Field, PrivateAttr

from ... import protocol as rpc_protocol
from .base import _PEER_CONNECT, _CarrierRPCProtocol
from .loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread

log = logging.getLogger(__name__)


class _RegisterRPCProtocol(_CarrierRPCProtocol):
    """Carrier for polled master/slave register buses.

    Parameters
    ----------
    poll_interval : float, default ``0.01``
        Seconds between mailbox polls.
    """

    poll_interval: float = Field(default=0.01)

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _poll_task: Any = PrivateAttr(default=None)
    _handshake_pending: dict = PrivateAttr(default_factory=dict)

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    async def _open_bus(self) -> None:
        """Open the underlying bus/device. Runs in the loop. Default no-op."""
        return None

    async def _close_bus(self) -> None:
        """Close the underlying bus/device. Default no-op."""
        return None

    async def _deliver(self, data: bytes) -> None:
        """Write one length-prefixed frame to the peer's inbox region."""
        raise NotImplementedError

    async def _poll_inbound(self) -> bytes | None:
        """Read the next inbound frame from our inbox region, or ``None``."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Open the bus and start the polling loop (idempotent)."""
        if self._started:
            return
        self._ensure_executor()
        try:
            start_loop_thread(
                self, self._async_start, ready_timeout=max(self.handshake_timeout, 10.0)
            )
        except BaseException:
            self._shutdown_executor()
            raise
        self._started = True

    async def _async_start(self, ready: threading.Event) -> None:
        await self._open_bus()
        self._poll_task = asyncio.ensure_future(self._poll_loop())
        ready.set()

    def stop(self) -> None:
        """Say goodbye, stop polling, close the bus, stop the loop (idempotent)."""
        if not self._started:
            return

        async def _shutdown() -> None:
            await self._goodbye_all()
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)
            self._shutdown_lanes()
            if self._poll_task is not None:
                self._poll_task.cancel()
                await asyncio.gather(self._poll_task, return_exceptions=True)
                self._poll_task = None
            await self._close_bus()
            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._shutdown_lanes()
        self._handshake_pending.clear()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    async def _send_goodbye(self, peer_id: str) -> None:
        """Write ``peer.disconnect`` into the peer's mailbox."""
        if peer_id not in self._connections:
            return
        await self._deliver(self._encode(self._goodbye_message()))

    async def _poll_loop(self) -> None:
        """Continuously poll the mailbox and dispatch inbound frames."""
        try:
            while True:
                data = await self._poll_inbound()
                if data:
                    self._feed_message(data)
                else:
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.debug("Register poll loop ended", exc_info=True)

    # ------------------------------------------------------------------
    # Inbound routing
    # ------------------------------------------------------------------

    def _feed_message(self, data: bytes) -> None:
        try:
            msg = self._decode(data)
        except Exception:
            return
        if rpc_protocol.is_request(msg):
            if msg.get("method") == _PEER_CONNECT:
                self._handle_handshake(msg)
            elif self._is_goodbye(msg):
                self._on_peer_goodbye((msg.get("params") or {}).get("from_id"))
            else:
                self._dispatch_request(msg)
        elif rpc_protocol.is_response(msg):
            rid = msg.get("id")
            if rid in self._handshake_pending:
                slot = self._handshake_pending.get(rid)
                if slot is not None:
                    slot["msg"] = msg
                    slot["event"].set()
            else:
                self._complete_pending(msg)

    def _handle_handshake(self, msg: dict) -> None:
        params = msg.get("params", {})
        peer_id = params.get("from_id")
        if params.get("secret") != self.peer_secret_key:
            resp = rpc_protocol.make_error(
                msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
            )
            self._deliver_async(resp)
            return
        policy_id = self._communication.policy_id if self._communication else None
        resp = rpc_protocol.make_result(msg.get("id"), {"peer_id": policy_id})
        self._register_peer(peer_id, True)
        self._deliver_async(resp)

    def _dispatch_request(self, msg: dict) -> None:
        self._handle_request_frame(msg, self._deliver_async)

    def _deliver_async(self, obj: dict) -> None:
        data = self._encode(obj)
        self._loop_call(lambda: asyncio.ensure_future(self._deliver(data)))

    # ------------------------------------------------------------------
    # Peering / RPC
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Handshake with the single peer on the other end of the bus."""
        self.start()
        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request("peer.connect", {"from_id": policy_id, "secret": secret})
        rid = req["id"]
        slot: dict[str, Any] = {"event": threading.Event()}
        self._handshake_pending[rid] = slot
        self._deliver_async(req)

        completed = slot["event"].wait(timeout=self.handshake_timeout)
        self._handshake_pending.pop(rid, None)
        if not completed:
            raise ConnectionError("Peer handshake timed out.")
        reply = slot["msg"]
        if "error" in reply:
            raise ConnectionError(
                f"Peer rejected connection: {reply['error'].get('message', reply['error'])}"
            )
        peer_id = reply.get("result", {}).get("peer_id")
        if peer_id is None:
            raise ConnectionError("Peer response missing peer_id.")
        self._register_peer(peer_id, True)
        return peer_id

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Write an ``rpc.call`` to the bus and block for the polled reply."""
        if peer_id not in self._connections:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        try:
            self._deliver_async(req)
        except ConnectionError:
            self._pending_rpcs.pop(request_id, None)
            raise
        return self._await_pending(request_id, slot)
