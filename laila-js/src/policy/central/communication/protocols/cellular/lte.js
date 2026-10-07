/**
 * LTE (4G) communication transport.
 *
 * 4G LTE data as an IP carrier; a thin subclass of the generic cellular
 * transport with LTE-specific tokens/URI.
 *
 * - ``protocol_name`` ``"lte"`` (aliases ``4g``)
 * - URI scheme ``lte://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL } from "./cellular.js";

/** LTE (4G) transport. */
export class _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL {
  static protocol_name = "lte";
  static _TOKEN_ALIASES = Object.freeze(new Set(["lte", "4g"]));

  /** Claim ``lte://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("lte://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.cellular.lte", { _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL });
