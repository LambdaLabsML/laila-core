/**
 * Matter (CHIP) communication transport.
 *
 * Matter runs over IP (Thread/Wi-Fi/Ethernet), so the data path is plain
 * TCP; this subclasses the raw-TCP transport with Matter's token/URI.
 *
 * - ``protocol_name`` ``"matter"`` (aliases ``chip``)
 * - URI scheme ``matter://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "../ip_app/tcp.js";

/** Matter transport (IP data path over Thread/Wi-Fi). */
export class _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "matter";
  static _TOKEN_ALIASES = Object.freeze(new Set(["matter", "chip"]));

  /** Claim ``matter://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("matter://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.matter", { _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL });
