"""Unreliable / segmented datagram RPC carrier.

:class:`_DatagramRPCProtocol` turns a lossy, MTU-limited packet link
(UDP, CoAP, LoRa, ESP-NOW, CAN, ...) into the same reliable JSON-RPC
channel the stream carrier provides. On top of a raw
``sendto(addr, bytes)`` / inbound-packet pair it adds:

- **Fragmentation / reassembly** -- each encoded JSON-RPC message is
  split into ``mtu``-sized fragments tagged with a 16-byte message id,
  a fragment index and a fragment count, and reassembled on arrival.
- **Per-fragment ack + retransmit** -- every received fragment is
  acknowledged; unacked fragments are resent every ``ack_timeout`` up
  to ``max_retries`` times, so messages survive moderate loss.
- **Dedup** -- a recently-seen message-id set prevents a retransmitted
  message from being processed twice after an ack is lost.

The reliable JSON-RPC layer (handshake, request/response correlation,
off-thread dispatch) is inherited from :class:`_CarrierRPCProtocol`.

Concrete transports supply three coroutines/helpers:
:meth:`_create_datagram_endpoint`, :meth:`_resolve_peer_addr` and
(optionally) :meth:`_close_endpoint`.

Stream lanes over datagrams
---------------------------
A stream message is **one datagram** carrying the standard lane header
(``[0x01][lane][seq][flags][data]``, ``START|END`` always set), sent
with ``sendto`` and nothing else: no ack, no retransmit, no dedup, no
fragmentation -- that machinery stays RPC-only. Lanes are therefore
*lossy and unordered by design*; the receiver's chunk-sequence check
only makes a lost datagram visible, it cannot recover it. A message
larger than ``mtu - 7`` raises ``ValueError`` on ``send()``. Inbound
packets are discriminated on their first byte before the fragment
parser (the ``LDG`` magic starts with ``0x4C``, so there is no clash).
"""

from __future__ import annotations

import asyncio
import logging
import struct
import threading
import uuid as _uuid
from typing import Any

from pydantic import Field, PrivateAttr

from ... import protocol as rpc_protocol
from ...channel import Channel
from . import codec as _codec
from .base import _PEER_CONNECT, _CarrierRPCProtocol
from .loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread

log = logging.getLogger(__name__)

_MAGIC = b"LDG"
_TYPE_DATA = 0
_TYPE_ACK = 1
#: magic(3) + type(1) + frag_index(2) + frag_count(2)
_HEADER = struct.Struct(">3sBHH")
_MSG_ID_LEN = 16
_PREFIX_LEN = _HEADER.size + _MSG_ID_LEN


class _DatagramEndpoint(asyncio.DatagramProtocol):
    """Minimal :class:`asyncio.DatagramProtocol` forwarding to a callback."""

    def __init__(self, on_packet: Any) -> None:
        self._on_packet = on_packet
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: Any) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self._on_packet(addr, data)

    def error_received(self, exc: Exception) -> None:
        log.debug("Datagram error: %s", exc)


class _DatagramRPCProtocol(_CarrierRPCProtocol):
    """Carrier for unreliable, MTU-limited packet links.

    Parameters
    ----------
    mtu : int, default ``1200``
        Maximum payload bytes per fragment (link MTU minus headers).
    ack_timeout : float, default ``0.5``
        Seconds between retransmits of unacked fragments.
    max_retries : int, default ``5``
        How many times an unacked fragment is resent before giving up.
    """

    supports_channels = True

    mtu: int = Field(default=1200)
    ack_timeout: float = Field(default=0.5)
    max_retries: int = Field(default=5)

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _transport: Any = PrivateAttr(default=None)
    _endpoint: Any = PrivateAttr(default=None)
    _reasm: dict = PrivateAttr(default_factory=dict)
    _seen: Any = PrivateAttr(default=None)
    _seen_order: Any = PrivateAttr(default_factory=list)
    _unacked: dict = PrivateAttr(default_factory=dict)
    _handshake_pending: dict = PrivateAttr(default_factory=dict)
    _retransmit_task: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    async def _create_datagram_endpoint(self) -> tuple[Any, Any]:
        """Create and return ``(transport, protocol)`` for the local socket.

        Subclasses typically call
        ``loop.create_datagram_endpoint(lambda: _DatagramEndpoint(self._feed_packet), ...)``.
        Runs inside the carrier event loop.
        """
        raise NotImplementedError

    async def _resolve_peer_addr(self, uri: str) -> Any:
        """Parse *uri* into the transport address used by ``sendto``."""
        raise NotImplementedError

    async def _close_endpoint(self) -> None:
        """Close the datagram endpoint. Default: close the transport."""
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None

    def _sendto(self, addr: Any, packet: bytes) -> None:
        """Send one raw datagram. Default uses the asyncio transport."""
        self._transport.sendto(packet, addr)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the event loop and bind the datagram endpoint (idempotent)."""
        if self._started:
            return
        self._seen = set()
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
        """Bind the endpoint and start the retransmit loop."""
        self._transport, self._endpoint = await self._create_datagram_endpoint()
        self._retransmit_task = asyncio.ensure_future(self._retransmit_loop())
        ready.set()

    def stop(self) -> None:
        """Tear down the endpoint and loop (idempotent).

        Order: goodbye to every peer (a datagram peer has no EOF to
        notice, so without it the remote only drops us on liveness
        timeout), unregister, stop the retransmit timer, close the
        endpoint, cancel and await remaining tasks, stop and close the loop.
        """
        if not self._started:
            return

        async def _shutdown() -> None:
            await self._goodbye_all()
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)
            self._shutdown_lanes()
            if self._retransmit_task is not None:
                self._retransmit_task.cancel()
                await asyncio.gather(self._retransmit_task, return_exceptions=True)
                self._retransmit_task = None
            await self._close_endpoint()
            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._shutdown_lanes()
        self._reasm.clear()
        self._unacked.clear()
        self._handshake_pending.clear()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    # ------------------------------------------------------------------
    # Reliable message layer
    # ------------------------------------------------------------------

    def _send_message(self, addr: Any, payload: bytes) -> None:
        """Fragment *payload* and queue every fragment for reliable delivery."""
        msg_id = _uuid.uuid4().bytes
        mtu = max(1, self.mtu)
        fragments = [payload[i : i + mtu] for i in range(0, len(payload), mtu)] or [b""]
        frag_count = len(fragments)
        for index, frag in enumerate(fragments):
            packet = _HEADER.pack(_MAGIC, _TYPE_DATA, index, frag_count) + msg_id + frag
            self._unacked[(addr, msg_id, index)] = {"packet": packet, "tries": 0}
            self._sendto(addr, packet)

    async def _retransmit_loop(self) -> None:
        """Resend unacked fragments every ``ack_timeout`` up to ``max_retries``."""
        try:
            while True:
                await asyncio.sleep(self.ack_timeout)
                for key, slot in list(self._unacked.items()):
                    if slot["tries"] >= self.max_retries:
                        self._unacked.pop(key, None)
                        continue
                    slot["tries"] += 1
                    addr = key[0]
                    try:
                        self._sendto(addr, slot["packet"])
                    except Exception:
                        self._unacked.pop(key, None)
        except asyncio.CancelledError:
            pass

    def _peer_for_addr(self, addr: Any) -> str | None:
        """Reverse-map a transport address to the peer registered on it."""
        for peer_id, a in self._connections.items():
            if a == addr:
                return peer_id
        return None

    def _feed_packet(self, addr: Any, data: bytes) -> None:
        """Inbound-packet entry point (called by the endpoint protocol).

        Stream-lane datagrams (first byte ``< 0x20``) are routed before
        the fragment parser; they are only accepted from registered
        peers.
        """
        if _codec.is_reserved_frame(data):
            peer_id = self._peer_for_addr(addr)
            if peer_id is None:
                self._unknown_lane_drops += 1
                return
            self._on_stream_frame(peer_id, data)
            return
        if len(data) < _PREFIX_LEN or data[:3] != _MAGIC:
            return
        _magic, ptype, frag_index, frag_count = _HEADER.unpack(data[: _HEADER.size])
        msg_id = data[_HEADER.size : _PREFIX_LEN]
        payload = data[_PREFIX_LEN:]

        if ptype == _TYPE_ACK:
            self._unacked.pop((addr, msg_id, frag_index), None)
            return

        # DATA: acknowledge this fragment, then try to reassemble.
        ack = _HEADER.pack(_MAGIC, _TYPE_ACK, frag_index, frag_count) + msg_id
        try:
            self._sendto(addr, ack)
        except Exception:
            pass

        if msg_id in self._seen:
            return

        key = (addr, msg_id)
        entry = self._reasm.get(key)
        if entry is None:
            entry = {"count": frag_count, "frags": {}}
            self._reasm[key] = entry
        entry["frags"][frag_index] = payload
        if len(entry["frags"]) < entry["count"]:
            return

        self._reasm.pop(key, None)
        self._mark_seen(msg_id)
        full = b"".join(entry["frags"][i] for i in range(entry["count"]))
        self._on_message(addr, full)

    def _mark_seen(self, msg_id: bytes) -> None:
        """Record *msg_id* as processed, bounding the dedup set."""
        self._seen.add(msg_id)
        self._seen_order.append(msg_id)
        if len(self._seen_order) > 4096:
            old = self._seen_order.pop(0)
            self._seen.discard(old)

    def _on_message(self, addr: Any, payload: bytes) -> None:
        """Decode a fully-reassembled JSON-RPC message and dispatch it."""
        try:
            msg = self._decode(payload)
        except Exception:
            return

        if rpc_protocol.is_request(msg):
            method = msg.get("method")
            if method == _PEER_CONNECT:
                self._handle_handshake(addr, msg)
            elif self._is_goodbye(msg):
                # only honoured from the address the peer is registered on
                peer_id = self._peer_for_addr(addr)
                if peer_id is not None and peer_id == (msg.get("params") or {}).get("from_id"):
                    self._on_peer_goodbye(peer_id)
            else:
                self._dispatch_rpc(addr, msg)
        elif rpc_protocol.is_response(msg):
            rid = msg.get("id")
            if rid in self._handshake_pending:
                slot = self._handshake_pending.get(rid)
                if slot is not None:
                    slot["msg"] = msg
                    slot["addr"] = addr
                    slot["event"].set()
            else:
                self._complete_pending(msg)

    def _handle_handshake(self, addr: Any, msg: dict) -> None:
        """Server side of ``peer.connect`` over datagrams."""
        params = msg.get("params", {})
        peer_id = params.get("from_id")
        if params.get("secret") != self.peer_secret_key:
            resp = rpc_protocol.make_error(
                msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
            )
            self._send_message(addr, self._encode(resp))
            return
        policy_id = self._communication.policy_id if self._communication else None
        resp = rpc_protocol.make_result(
            msg.get("id"), {"peer_id": policy_id, "caps": self._local_caps()}
        )
        self._set_peer_caps(peer_id, params.get("caps"))
        self._register_peer(peer_id, addr)
        self._send_message(addr, self._encode(resp))

    def _dispatch_rpc(self, addr: Any, msg: dict) -> None:
        """Run an inbound ``rpc.call`` off the loop and reply to *addr*."""

        def _reply(resp: dict) -> None:
            try:
                self._loop_call(self._send_message, addr, self._encode(resp))
            except ConnectionError:
                pass

        self._handle_request_frame(msg, _reply, peer_id=self._peer_for_addr(addr))

    async def _send_goodbye(self, peer_id: str) -> None:
        """Send ``peer.disconnect`` to the peer's address (first try goes out at once)."""
        addr = self._connections.get(peer_id)
        if addr is None or self._transport is None:
            return
        self._send_message(addr, self._encode(self._goodbye_message()))

    # ------------------------------------------------------------------
    # Peering / RPC
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Handshake with the remote endpoint named by *uri*."""
        self.start()
        addr_fut = asyncio.run_coroutine_threadsafe(self._resolve_peer_addr(uri), self._event_loop)
        addr = addr_fut.result(timeout=10.0)

        policy_id = self._communication.policy_id if self._communication else None
        req = rpc_protocol.make_request(
            "peer.connect",
            {"from_id": policy_id, "secret": secret, "caps": self._local_caps()},
        )
        rid = req["id"]
        slot: dict[str, Any] = {"event": threading.Event()}
        self._handshake_pending[rid] = slot
        self._loop_call(self._send_message, addr, self._encode(req))

        completed = slot["event"].wait(timeout=self.handshake_timeout)
        self._handshake_pending.pop(rid, None)
        if not completed:
            raise ConnectionError("Peer handshake timed out.")
        reply = slot["msg"]
        if "error" in reply:
            raise ConnectionError(
                f"Peer rejected connection: {reply['error'].get('message', reply['error'])}"
            )
        result = reply.get("result", {}) or {}
        peer_id = result.get("peer_id")
        if peer_id is None:
            raise ConnectionError("Peer response missing peer_id.")
        self._set_peer_caps(peer_id, result.get("caps"))
        self._register_peer(peer_id, addr)
        return peer_id

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send one ``rpc.call`` to *peer_id* and block for the response."""
        addr = self._connections.get(peer_id)
        if addr is None:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        try:
            self._loop_call(self._send_message, addr, self._encode(req))
        except ConnectionError:
            self._pending_rpcs.pop(request_id, None)
            raise
        return self._await_pending(request_id, slot)

    def _send_oneway(self, peer_id: str, path: list, kwargs: dict) -> None:
        """Send a control request (reliably) without waiting for its reply."""
        addr = self._connections.get(peer_id)
        if addr is None:
            raise ConnectionError(f"No connection to peer {peer_id}")
        req = self._make_rpc_request(path, (), kwargs, str(_uuid.uuid4()))
        self._loop_call(self._send_message, addr, self._encode(req))

    # ------------------------------------------------------------------
    # Stream lanes (single datagram per message, best effort)
    # ------------------------------------------------------------------

    def _max_message_bytes(self) -> int:
        """A stream message must fit one datagram: ``mtu - header``."""
        return max(
            0, min(int(self.max_stream_frame_bytes), int(self.mtu) - _codec.STREAM_HEADER_LEN)
        )

    def _stream_enqueue(self, channel: Channel, payload: bytes) -> None:
        """Send *payload* as one unacknowledged datagram (user thread)."""
        addr = self._connections.get(channel.peer_id)
        if addr is None:
            raise ConnectionError(f"No connection to peer {channel.peer_id}")
        limit = self._max_message_bytes()
        if len(payload) > limit:
            raise ValueError(
                f"Stream message of {len(payload)} bytes does not fit one datagram "
                f"(mtu={self.mtu} - {_codec.STREAM_HEADER_LEN} header = {limit}). "
                "Datagram lanes do not fragment."
            )
        seq = channel._next_chunk_seqs(1)
        packet = _codec.pack_stream_frame(
            channel.tx_lane_id, seq, _codec.FLAG_START | _codec.FLAG_END, payload
        )
        self._loop_call(self._safe_sendto, addr, packet)

    def _safe_sendto(self, addr: Any, packet: bytes) -> None:
        try:
            self._sendto(addr, packet)
        except Exception:
            log.debug("stream datagram send failed", exc_info=True)
