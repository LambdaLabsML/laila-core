/**
 * Reusable RPC *carrier* base classes for communication transports.
 *
 * A laila communication transport only has to move framed JSON-RPC bytes
 * between two peers; the handshake, peer registry, future virtualisation and
 * inbound dispatch are identical across every wire. Rather than re-implement
 * that for each of the dozens of supported transports, the carriers in this
 * sub-package implement the shared machinery once and expose a small set of
 * hooks that a concrete transport fills in.
 *
 * Carriers
 * --------
 * - ``_StreamRPCProtocol`` (``./stream.js``)
 *     Reliable, ordered, duplex byte streams (TCP, TLS, Unix sockets, serial
 *     lines, USB-CDC, RFCOMM, ...). Subclasses provide an async server
 *     factory and an async client-connect factory; the carrier does
 *     length-prefixed framing, the ``peer.connect`` handshake, the receive
 *     loop and the pending-RPC table.
 * - ``_DatagramRPCProtocol`` (``./datagram.js``)
 *     Unreliable / segmented links (UDP, CoAP, LoRa, ESP-NOW, CAN, ...).
 *     Adds MTU fragmentation/reassembly, sequence numbers and ack/retry on
 *     top of a packet ``send``/``receive`` pair supplied by the subclass.
 *
 * All carriers share the codec in ``./codec.js`` (pluggable ``json`` /
 * ``msgpack`` with identical laila future-tagging semantics) and the
 * inbound-dispatch / peer-registration helpers in ``./base.js``.
 */
export { _CarrierRPCProtocol } from "./base.js";
export { _BrokerRPCProtocol } from "./broker.js";
export { _DatagramRPCProtocol } from "./datagram.js";
export { _RegisterRPCProtocol } from "./register.js";
export { _StreamRPCProtocol } from "./stream.js";

export const __all__ = Object.freeze(["_BrokerRPCProtocol", "_CarrierRPCProtocol", "_DatagramRPCProtocol", "_RegisterRPCProtocol", "_StreamRPCProtocol"]);
