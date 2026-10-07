/**
 * Inter-policy peer-to-peer communication sub-system.
 *
 * This sub-package wires together everything needed for one laila ``Policy``
 * to talk to another in a different process or on a different host. It is
 * structured around four collaborating pieces:
 *
 * - ``_LAILA_IDENTIFIABLE_COMMUNICATION``
 *     Per-policy "central communication" hub. Owns the local listener that
 *     exposes the policy to remote peers, the registry of known peers, and
 *     the proxy bookkeeping that lets remote calls feel like local method
 *     invocations. Always reachable from a policy through
 *     ``policy.central.communication``.
 *
 * - ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL``
 *     Pluggable transport. Concrete subclasses (currently
 *     ``_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL``) implement the actual wire
 *     encoding, listener loop, and request/response routing. A protocol is
 *     chosen per ``Communication`` instance and is decoupled from the API
 *     surface.
 *
 * - ``RemotePolicyProxy``
 *     Client-side handle to a remote peer's policy. Method calls on the proxy
 *     are translated into protocol messages, dispatched, and awaited; the
 *     result is returned to the caller as if it had been produced locally.
 *     Pickling-friendly so a proxy can be shipped across process boundaries.
 *
 * - The ``connection`` module's helpers
 *     Thin abstractions over a single live transport, used by the protocols
 *     when they need point-to-point streams (for example, long-lived TCP/IP
 *     sockets between two known peers).
 *
 * Stream lanes
 * ------------
 * Besides RPC, stream-capable carriers (``_StreamRPCProtocol``,
 * ``_P2PStreamRPCProtocol``, ``_DatagramRPCProtocol`` and loopback; see
 * ``supports_channels``) multiplex opaque, bidirectional byte *lanes* on the
 * same connection::
 *
 *     const video = laila.peers[peer_gid]["video"];   // Channel, opened lazily
 *     for (const entry of laila.relay(video)) {       // blocks on the caller's thread
 *       const au = entry.data;                        // bytes: one sender-side send()
 *     }
 *     laila.peers[peer_gid]["default"];               // the ordinary RPC proxy
 *     video.send(Buffer.from("..."));
 *
 * - **Wire**: stream frames share the carrier's outer framing and are told
 *   apart from RPC by their first byte. Bytes ``0x00``-``0x1F`` are reserved
 *   binary markers (RPC payloads start with ``{`` or a msgpack map byte);
 *   ``0x01`` is the lane frame ``[0x01][lane u8][seq u32 BE][flags u8][data]``
 *   with ``flags`` bit0 = END and bit1 = START of a message and ``seq`` a
 *   per-lane chunk counter. Unknown reserved markers are dropped. See
 *   ``./wire.js``.
 * - **Control**: ``__comm_channel_open__`` (``{"name", "lane", "options"}``
 *   -> ``{"lane"}``) and ``__comm_channel_close__`` (``{"lane"}`` -> ``True``)
 *   ride the RPC lane but are intercepted inside the carrier exactly like
 *   ``__comm_ping__`` -- before admission, before ``_execute_rpc`` -- so they
 *   never reach the policy, memory or command. Lane ``0`` is reserved; the
 *   handshake advertises ``caps: {"lanes": 1}``; a peer without it (older
 *   laila, RPC-only firmware) raises ``ConnectionError`` immediately on
 *   ``peers[gid][name]`` unless ``static_lanes={name: lane}`` is configured on
 *   the connection.
 * - **Data path**: stream bytes never touch ``_handle_request_frame``, the
 *   admission semaphore, the inbound executor, ``_execute_rpc``, the codec,
 *   futures or memory. ``Relay`` is a plain blocking iterator yielding
 *   ``StreamEntry`` (an in-memory ``Entry.constant``-shaped entry with
 *   ``.stream`` provenance) wrapped on the consumer thread.
 * - **Fairness**: each link has one writer task with a priority RPC queue and
 *   a chunked stream queue (``stream_chunk_bytes``) so pings and replies
 *   interleave between chunks; stream traffic is never liveness evidence and
 *   never takes an admission slot.
 * - **Bounds**: inbound lanes drop oldest past ``channel_queue_size`` /
 *   ``channel_queue_bytes``; messages over ``max_stream_frame_bytes`` are
 *   rejected / discarded whole; incomplete messages never surface.
 * - **Reconnect**: a peer drop closes all its lanes (relays end with
 *   ``StopIteration``, ``send()`` raises). After reconnect,
 *   ``laila.peers[gid][name]`` is a *new* channel; re-index.
 */
import { register } from "../../../_compat/lazy.js";

export { Channel, Relay, StreamEntry, StreamMeta } from "./channel.js";
export { _LAILA_IDENTIFIABLE_COMM_PROTOCOL } from "./protocols/base.js";
export { _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL } from "./protocols/lpwan/lora.js";
export { _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL } from "./protocols/short_range/bluetooth.js";
export { _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL } from "./protocols/tcpip.js";
export { RemotePolicyProxy } from "./proxy.js";
export { PeerProxy, PeerRegistry } from "./registry.js";
export { _LAILA_IDENTIFIABLE_COMMUNICATION } from "./schema/base.js";

// Sub-modules (``from laila.policy.central.communication import protocols``).
export * as protocols from "./protocols/index.js";
export * as wire from "./wire.js";
export * as protocol from "./protocol.js";
export * as connection from "./connection.js";

export const __all__ = Object.freeze([
  "_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_COMMUNICATION",
  "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
  "Channel",
  "PeerProxy",
  "PeerRegistry",
  "Relay",
  "RemotePolicyProxy",
  "StreamEntry",
  "StreamMeta",
]);

import * as _self from "./index.js";
register("laila.policy.central.communication", _self);
