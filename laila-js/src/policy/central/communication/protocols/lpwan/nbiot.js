/**
 * NB-IoT (Narrowband IoT) communication transport.
 *
 * NB-IoT is a cellular LPWAN bearer; once the modem attaches and a PDP
 * context is up the data path is IP, so this subclasses the generic cellular
 * transport with NB-IoT-specific tokens/URI.
 *
 * - ``protocol_name`` ``"nbiot"`` (aliases ``nb-iot`` / ``nb1``)
 * - URI scheme ``nbiot://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL } from "../cellular/cellular.js";

/** NB-IoT cellular LPWAN transport (IP data path). */
export class _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL {
  static protocol_name = "nbiot";
  static _TOKEN_ALIASES = Object.freeze(new Set(["nbiot", "nb-iot", "nb1"]));

  /** Claim ``nbiot://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("nbiot://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.nbiot", { _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL });
