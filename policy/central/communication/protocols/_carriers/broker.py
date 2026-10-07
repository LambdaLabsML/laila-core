"""Broker / pub-sub mediated RPC carrier.

:class:`_BrokerRPCProtocol` carries JSON-RPC over a message broker or
pub/sub fabric (MQTT, AMQP, XMPP, DDS/RTPS, ZeroMQ, ...). There is no
point-to-point socket: instead every policy subscribes to its own
*inbox* topic and addresses a peer by publishing to the peer's inbox.

Addressing & correlation
-------------------------
- Each endpoint owns an inbox topic ``laila/inbox/<policy_global_id>``.
- A request carries a ``reply_to`` (the sender's inbox) so the responder
  knows where to publish the reply; request/response are then correlated
  by the JSON-RPC ``id`` via the inherited pending-RPC table.
- Peering publishes a ``peer.connect`` to the target inbox; the acceptor
  records ``peer_id -> reply_to`` and replies.

Concrete transports supply the broker plumbing:
:meth:`_broker_connect`, :meth:`_broker_subscribe`,
:meth:`_broker_publish` and :meth:`_broker_close`. Inbound messages are
delivered by calling :meth:`_feed_message(data)` (the carrier figures
out request vs response).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from pydantic import PrivateAttr

from ... import protocol as rpc_protocol
from .base import _PEER_CONNECT, _CarrierRPCProtocol
from .loopthread import cancel_pending_tasks, start_loop_thread, stop_loop_thread
from .uri import uri_authority

log = logging.getLogger(__name__)


class _BrokerRPCProtocol(_CarrierRPCProtocol):
    """Carrier for broker / pub-sub fabrics.

    Notes
    -----
    Like the other carriers it owns a dedicated event loop on a daemon
    thread; the concrete broker client is created inside that loop.
    """

    _event_loop: asyncio.AbstractEventLoop | None = PrivateAttr(default=None)
    _loop_thread: threading.Thread | None = PrivateAttr(default=None)
    _inbox: str | None = PrivateAttr(default=None)
    _handshake_pending: dict = PrivateAttr(default_factory=dict)

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    async def _broker_connect(self) -> None:
        """Establish the broker client connection. Runs in the loop."""
        raise NotImplementedError

    async def _broker_subscribe(self, topic: str) -> None:
        """Subscribe to *topic*; inbound payloads must reach :meth:`_feed_message`."""
        raise NotImplementedError

    async def _broker_publish(self, topic: str, data: bytes) -> None:
        """Publish *data* to *topic*."""
        raise NotImplementedError

    async def _broker_close(self) -> None:
        """Close the broker connection. Default: no-op."""
        return None

    def _inbox_topic(self) -> str:
        """Topic this endpoint listens on for inbound frames."""
        pid = self._communication.policy_id if self._communication else None
        return f"laila/inbox/{pid}"

    def _peer_inbox(self, peer_policy_id: str) -> str:
        """Inbox topic of a peer addressed by its policy global_id."""
        return f"laila/inbox/{peer_policy_id}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Boot the loop, connect to the broker, subscribe to the inbox."""
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
        await self._broker_connect()
        self._inbox = self._inbox_topic()
        await self._broker_subscribe(self._inbox)
        ready.set()

    def stop(self) -> None:
        """Say goodbye, disconnect from the broker and stop the loop (idempotent)."""
        if not self._started:
            return

        async def _shutdown() -> None:
            await self._goodbye_all()
            for peer_id in list(self._connections):
                self._unregister_peer(peer_id)
            self._shutdown_lanes()
            await self._broker_close()
            await cancel_pending_tasks()

        stop_loop_thread(self, _shutdown)
        self._shutdown_lanes()
        self._handshake_pending.clear()
        self._pending_rpcs.clear()
        self._shutdown_executor()
        self._started = False

    async def _send_goodbye(self, peer_id: str) -> None:
        """Publish ``peer.disconnect`` to the peer's inbox."""
        inbox = self._connections.get(peer_id)
        if inbox is None:
            return
        await self._broker_publish(inbox, self._encode(self._goodbye_message()))

    # ------------------------------------------------------------------
    # Inbound routing
    # ------------------------------------------------------------------

    def _feed_message(self, data: bytes) -> None:
        """Inbound entry point: decode a frame and route it."""
        try:
            msg = self._decode(data)
        except Exception:
            return

        if rpc_protocol.is_request(msg):
            method = msg.get("method")
            if method == _PEER_CONNECT:
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
        reply_to = params.get("reply_to")
        if params.get("secret") != self.peer_secret_key:
            resp = rpc_protocol.make_error(
                msg.get("id"), rpc_protocol.ERR_AUTH_FAILED, "Invalid peer secret key."
            )
            self._publish_async(reply_to, resp)
            return
        policy_id = self._communication.policy_id if self._communication else None
        resp = rpc_protocol.make_result(msg.get("id"), {"peer_id": policy_id})
        self._register_peer(peer_id, reply_to)
        self._publish_async(reply_to, resp)

    def _dispatch_request(self, msg: dict) -> None:
        reply_to = msg.get("reply_to")
        self._handle_request_frame(msg, lambda resp: self._publish_async(reply_to, resp))

    def _publish_async(self, topic: str | None, obj: dict) -> None:
        if topic is None:
            return
        data = self._encode(obj)
        self._loop_call(lambda: asyncio.ensure_future(self._broker_publish(topic, data)))

    # ------------------------------------------------------------------
    # Peering / RPC
    # ------------------------------------------------------------------

    def connect(self, uri: str, secret: str) -> str:
        """Handshake with the peer whose policy id is encoded in *uri*."""
        self.start()
        peer_policy_id = uri_authority(uri)
        peer_inbox = self._peer_inbox(peer_policy_id)
        policy_id = self._communication.policy_id if self._communication else None

        req = rpc_protocol.make_request(
            "peer.connect",
            {"from_id": policy_id, "secret": secret, "reply_to": self._inbox},
        )
        rid = req["id"]
        slot: dict[str, Any] = {"event": threading.Event()}
        self._handshake_pending[rid] = slot
        self._publish_async(peer_inbox, req)

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
        self._register_peer(peer_id, peer_inbox)
        return peer_id

    def _send_once(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Publish an ``rpc.call`` to *peer_id*'s inbox and block for the reply."""
        peer_inbox = self._connections.get(peer_id)
        if peer_inbox is None:
            raise ConnectionError(f"No connection to peer {peer_id}")
        request_id, slot = self._register_pending()
        req = self._make_rpc_request(path, args, kwargs, request_id)
        req["reply_to"] = self._inbox
        try:
            self._publish_async(peer_inbox, req)
        except ConnectionError:
            self._pending_rpcs.pop(request_id, None)
            raise
        return self._await_pending(request_id, slot)
