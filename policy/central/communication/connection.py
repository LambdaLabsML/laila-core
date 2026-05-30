"""WebSocket connection management for the TCP/IP protocol.

This module factors the websocket plumbing out of
:class:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL` so the protocol class
itself can stay focused on lifecycle and the public RPC API. Three
phases are implemented here:

1. **Server bring-up** (:func:`start_server`) -- bind a websockets
   server to ``proto.host:proto.port`` and stash the handle on
   *proto*. The server's per-connection callback is
   :func:`_handle_inbound`.
2. **Handshake** (:func:`_handle_inbound` / :func:`connect_outbound`) --
   the first frame on a freshly opened socket must be a
   ``peer.connect`` JSON-RPC request carrying the originating policy's
   ``global_id`` and the shared secret. On success both sides register
   the peer with their local protocol/communication and enter the
   shared receive loop.
3. **Receive loop** (:func:`_receive_loop`) -- one-per-connection task
   that decodes inbound frames and routes them to either the
   request handler (:func:`_handle_rpc_request`) or the response
   handler (:func:`_handle_rpc_response`).

All functions are async and are expected to run inside the protocol
instance's dedicated event loop (the daemon thread spun up by
:meth:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL.start`).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve

from . import protocol

if TYPE_CHECKING:
    from .protocols.tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL

log = logging.getLogger(__name__)


async def start_server(proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL) -> None:
    """Start the WebSocket listener and store the server handle on *proto*.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        The TCP/IP protocol instance whose ``host`` / ``port`` to bind.
    """
    server = await serve(
        lambda ws: _handle_inbound(proto, ws),
        proto.host,
        proto.port,
    )
    bound_port = server.sockets[0].getsockname()[1]
    proto._bound_port = bound_port
    proto._server = server
    log.info("Communication server listening on %s:%s", proto.host, proto.bound_port)


async def _handle_inbound(
    proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, ws: ServerConnection
) -> None:
    """Handle a freshly accepted inbound WebSocket connection.

    Expects the first message to be a ``peer.connect`` JSON-RPC request.
    On success, registers the peer bidirectionally and enters the shared
    receive loop.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        Local TCP/IP protocol instance.
    ws : ServerConnection
        The new WebSocket connection.
    """
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
    except (TimeoutError, websockets.ConnectionClosed):
        return

    msg = protocol.decode(raw)

    if not protocol.is_request(msg) or msg.get("method") != "peer.connect":
        resp = protocol.make_error(
            msg.get("id"),
            protocol.ERR_INVALID_REQUEST,
            "First message must be a peer.connect request.",
        )
        await ws.send(protocol.encode(resp))
        await ws.close()
        return

    params = msg.get("params", {})
    peer_id = params.get("from_id")
    secret = params.get("secret")

    if secret != proto.peer_secret_key:
        resp = protocol.make_error(
            msg["id"],
            protocol.ERR_AUTH_FAILED,
            "Invalid peer secret key.",
        )
        await ws.send(protocol.encode(resp))
        await ws.close()
        return

    policy_id = proto._communication.policy_id if proto._communication else None
    resp = protocol.make_result(msg["id"], {"peer_id": policy_id})
    await ws.send(protocol.encode(resp))

    proto._register_peer(peer_id, ws)
    log.info("Accepted inbound peer %s", peer_id)

    await _receive_loop(proto, ws, peer_id)


async def connect_outbound(
    proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, uri: str, secret: str
) -> str:
    """Initiate an outbound peering connection.

    Connects to *uri*, performs the ``peer.connect`` handshake, registers
    the peer on both sides, and starts the shared receive loop.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        Local TCP/IP protocol instance.
    uri : str
        WebSocket URI of the remote policy (e.g. ``ws://host:port``).
    secret : str
        The remote policy's ``peer_secret_key``.

    Returns
    -------
    str
        The remote policy's ``global_id``.

    Raises
    ------
    ConnectionError
        If the handshake is rejected or times out.
    """
    ws = await connect(uri)

    policy_id = proto._communication.policy_id if proto._communication else None
    req = protocol.make_request(
        "peer.connect",
        {
            "from_id": policy_id,
            "secret": secret,
        },
    )
    await ws.send(protocol.encode(req))

    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
    except (TimeoutError, websockets.ConnectionClosed) as exc:
        await ws.close()
        raise ConnectionError("Peer handshake timed out or connection lost.") from exc

    msg = protocol.decode(raw)

    if "error" in msg:
        err = msg["error"]
        await ws.close()
        raise ConnectionError(f"Peer rejected connection: {err.get('message', err)}")

    peer_id = msg.get("result", {}).get("peer_id")
    if peer_id is None:
        await ws.close()
        raise ConnectionError("Peer response missing peer_id.")

    proto._register_peer(peer_id, ws)
    log.info("Connected to outbound peer %s at %s", peer_id, uri)

    asyncio.ensure_future(_receive_loop(proto, ws, peer_id))

    return peer_id


async def _receive_loop(
    proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, ws: Any, peer_id: str
) -> None:
    """Read messages from *ws* and dispatch requests / responses.

    This loop runs identically on both the initiator and acceptor side of
    a peered connection.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        Local TCP/IP protocol instance.
    ws : WebSocket
        The shared full-duplex WebSocket.
    peer_id : str
        ``global_id`` of the remote peer.
    """
    try:
        async for raw in ws:
            msg = protocol.decode(raw)

            if protocol.is_request(msg):
                await _handle_rpc_request(proto, ws, msg)
            elif protocol.is_response(msg):
                _handle_rpc_response(proto, msg)
            else:
                log.warning("Unrecognised message from %s: %s", peer_id, raw[:200])
    except websockets.ConnectionClosed:
        log.info("Connection to peer %s closed.", peer_id)
    finally:
        proto._unregister_peer(peer_id)


async def _handle_rpc_request(
    proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, ws: Any, msg: dict
) -> None:
    """Execute an inbound ``rpc.call`` and send the result back.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        Local TCP/IP protocol instance.
    ws : WebSocket
        Socket to reply on.
    msg : dict
        Parsed JSON-RPC request.
    """
    request_id = msg.get("id")
    method = msg.get("method")

    if method != "rpc.call":
        resp = protocol.make_error(
            request_id,
            protocol.ERR_METHOD_NOT_FOUND,
            f"Unknown method: {method}",
        )
        await ws.send(protocol.encode(resp))
        return

    params = msg.get("params", {})
    path = params.get("path", [])
    args = params.get("args", [])
    kwargs = params.get("kwargs", {})

    try:
        result = proto._communication._execute_rpc(path, args, kwargs)
        resp = protocol.make_result(request_id, result)
    except Exception as exc:
        resp = protocol.make_error(
            request_id,
            protocol.ERR_EXECUTION,
            f"{type(exc).__name__}: {exc}",
        )

    await ws.send(protocol.encode(resp))


def _handle_rpc_response(proto: _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, msg: dict) -> None:
    """Resolve a pending outbound RPC call with the received response.

    Parameters
    ----------
    proto : _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
        Local TCP/IP protocol instance.
    msg : dict
        Parsed JSON-RPC response.
    """
    request_id = msg.get("id")
    if request_id is None:
        return

    pending = proto._pending_rpcs.get(request_id)
    if pending is None:
        log.warning("Received response for unknown request %s", request_id)
        return

    if "error" in msg:
        pending["error"] = msg["error"]
    else:
        pending["result"] = msg.get("result")

    pending["event"].set()
