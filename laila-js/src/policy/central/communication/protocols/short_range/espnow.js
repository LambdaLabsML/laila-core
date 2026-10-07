/**
 * ESP-NOW communication transport (datagram radio).
 *
 * ESP-NOW is Espressif's connectionless Wi-Fi-layer protocol implemented in
 * ESP32/ESP8266 firmware; there is no host-side driver, so this lazy
 * datagram transport raises a clear hardware requirement error on start.
 *
 * - ``protocol_name`` ``"esp-now"`` (aliases ``espnow``)
 * - URI scheme ``espnow://<mac>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** ESP-NOW transport (requires ESP firmware). */
export class _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "esp-now";
  static _TOKEN_ALIASES = Object.freeze(new Set(["esp-now", "espnow"]));

  /** Accept ``"esp-now"`` / ``"espnow"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``espnow://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("espnow://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.espnow", { _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL });
