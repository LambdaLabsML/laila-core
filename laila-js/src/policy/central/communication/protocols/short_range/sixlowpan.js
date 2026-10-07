/**
 * 6LoWPAN communication transport.
 *
 * 6LoWPAN compresses IPv6 over 802.15.4; the data path is IPv6/UDP, so this
 * subclasses the UDP transport with 6LoWPAN's token/URI.
 *
 * - ``protocol_name`` ``"6lowpan"`` (aliases ``sixlowpan``)
 * - URI scheme ``sixlowpan://[addr]:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL } from "../ip_app/udp.js";

/** 6LoWPAN transport (IPv6/UDP over 802.15.4). */
export class _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL {
  static protocol_name = "6lowpan";
  static _TOKEN_ALIASES = Object.freeze(new Set(["6lowpan", "sixlowpan"]));

  /** Claim ``sixlowpan://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("sixlowpan://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.sixlowpan", { _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL });
