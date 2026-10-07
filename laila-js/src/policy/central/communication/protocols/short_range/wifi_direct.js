/**
 * Wi-Fi Direct (P2P) communication transport.
 *
 * Once a Wi-Fi Direct group is formed the data path is IP, so this
 * subclasses the raw-TCP transport with Wi-Fi Direct's token/URI.
 *
 * - ``protocol_name`` ``"wifi-direct"`` (aliases ``wifidirect`` / ``wfd`` / ``p2p``)
 * - URI scheme ``wifidirect://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "../ip_app/tcp.js";

/** Wi-Fi Direct (P2P) transport (TCP data path over a P2P group). */
export class _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "wifi-direct";
  static _TOKEN_ALIASES = Object.freeze(new Set(["wifi-direct", "wifidirect", "wfd", "p2p"]));

  /** Claim ``wifidirect://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("wifidirect://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.wifi_direct", { _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL });
