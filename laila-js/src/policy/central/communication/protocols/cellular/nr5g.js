/**
 * 5G NR communication transport.
 *
 * 5G New Radio data as an IP carrier; a thin subclass of the generic
 * cellular transport with 5G-specific tokens/URI.
 *
 * - ``protocol_name`` ``"nr5g"`` (aliases ``5g`` / ``5g-nr`` / ``nr``)
 * - URI scheme ``nr5g://host:port`` (URI schemes cannot begin with a digit,
 *   so ``5g://`` is spelled ``nr5g://``)
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL } from "./cellular.js";

/** 5G NR transport. */
export class _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL {
  static protocol_name = "nr5g";
  static _TOKEN_ALIASES = Object.freeze(new Set(["nr5g", "5g", "5g-nr", "nr"]));

  /** Claim ``nr5g://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("nr5g://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.cellular.nr5g", { _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL });
