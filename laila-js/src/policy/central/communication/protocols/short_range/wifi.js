/**
 * Wi-Fi communication transport.
 *
 * Wi-Fi is an IP carrier: once associated (station) or hosting (AP) the data
 * path is plain TCP, so this subclasses the raw-TCP transport and records
 * the SSID/mode for diagnostics.
 *
 * - ``protocol_name`` ``"wifi"`` (aliases ``wlan`` / ``sta`` / ``ap``)
 * - URI scheme ``wifi://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "../ip_app/tcp.js";

/** Wi-Fi station/AP transport (TCP data path over a wireless NIC). */
export class _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "wifi";
  static _TOKEN_ALIASES = Object.freeze(new Set(["wifi", "wlan", "sta", "ap"]));

  static {
    define_fields(this, {
      ssid: ["str | None", Field({ default: null })],
      mode: ["str", Field({ default: "station" })],
    });
  }

  /** Claim ``wifi://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("wifi://");
  }

  /** Convenience wrapper building the ``wifi://`` URI. */
  connect_wifi(host, port, secret) {
    return this.connect(`wifi://${host}:${port}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.wifi", { _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL });
