/**
 * LTE-M (Cat-M1) communication transport.
 *
 * LTE-M is a cellular LPWAN bearer; once attached the data path is IP, so
 * this subclasses the generic cellular transport with LTE-M-specific
 * tokens/URI.
 *
 * - ``protocol_name`` ``"ltem"`` (aliases ``lte-m`` / ``cat-m1`` / ``catm1``)
 * - URI scheme ``ltem://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL } from "../cellular/cellular.js";

/** LTE-M (Cat-M1) cellular LPWAN transport (IP data path). */
export class _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL {
  static protocol_name = "ltem";
  static _TOKEN_ALIASES = Object.freeze(new Set(["ltem", "lte-m", "cat-m1", "catm1"]));

  /** Claim ``ltem://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("ltem://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.ltem", { _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL });
