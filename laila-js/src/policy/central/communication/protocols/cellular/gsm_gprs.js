/**
 * GSM / GPRS (2G) communication transport.
 *
 * 2G data (GPRS/EDGE) as an IP carrier; a thin subclass of the generic
 * cellular transport with 2G-specific tokens/URI.
 *
 * - ``protocol_name`` ``"gsm"`` (aliases ``gprs`` / ``2g`` / ``edge``)
 * - URI scheme ``gsm://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL } from "./cellular.js";

/** GSM/GPRS (2G) transport. */
export class _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL {
  static protocol_name = "gsm";
  static _TOKEN_ALIASES = Object.freeze(new Set(["gsm", "gprs", "2g", "edge"]));

  /** Claim ``gsm://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("gsm://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.cellular.gsm_gprs", { _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL });
