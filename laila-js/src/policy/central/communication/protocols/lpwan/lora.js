/**
 * LoRa communication protocol (scaffold / planned transport).
 *
 * This module sketches a future ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL``
 * transport for **LoRa** -- a long-range, low-bandwidth radio link well
 * suited to peer-to-peer policy gossip across kilometres where no IP network
 * exists. It is intentionally *not implemented yet*: every method that would
 * touch the radio raises ``NotImplementedError`` with a note on what a real
 * implementation must do.
 *
 * What lands here so the rest of laila can already "see" LoRa:
 *
 * - ``protocol_name`` ``"lora"`` and the ``lora://`` URI scheme, so
 *   ``laila.request(peer, comm_protocol="lora")`` and URI-based peering
 *   resolve to this class once a real driver exists.
 * - The full method surface of ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL``,
 *   documenting the contract a LoRa driver must satisfy.
 *
 * Implementation notes for a future contributor
 * ----------------------------------------------
 * LoRa is half-duplex, framed, and lossy. A real implementation should build
 * on the ``_DatagramRPCProtocol`` carrier (fragmentation + ack/retry already
 * provided) and supply a SX127x/SX126x modem endpoint (``spidev``) plus a
 * node-address -> peer ``global_id`` mapping via the shared ``peer.connect``
 * handshake.
 */
import { NotImplementedError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, register_comm_protocol } from "../base.js";

const _NOT_IMPLEMENTED = "The LoRa transport is a planned protocol and is not implemented yet. Use the TCP/IP protocol (comm_protocol='tcpip') for now.";

/**
 * Planned LoRa radio transport (scaffold; raises until implemented).
 *
 * Subclass of ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` reserving the ``"lora"``
 * token and ``lora://`` URI scheme. All transport methods raise
 * ``NotImplementedError`` for now -- the class exists so transport selection
 * (``comm_protocol="lora"``) and URI routing have a real target to resolve
 * to.
 */
export class _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
  static protocol_name = "lora";

  /** Claim ``lora://`` URIs for this transport. */
  static can_handle_uri(uri) {
    return uri.startsWith("lora://");
  }

  /** Bring up the LoRa modem link. Not implemented yet. */
  start() {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }


  /**
   * Tear down the LoRa modem link. No-op (nothing is ever started).
   *
   * ``stop`` must be idempotent and safe per the base contract, so the
   * scaffold returns cleanly rather than raising.
   */
  stop() {
    return null;
  }


  /** Peer with a remote node over LoRa. Not implemented yet. */
  connect(_uri, _secret) {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }


  /** Send an RPC frame over LoRa. Not implemented yet. */
  send_rpc(_peer_id, _path, _args, _kwargs) {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }


  /** No LoRa peers are ever held by this scaffold. */
  has_peer(_peer_id) {
    return false;
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.lora", { _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL });
