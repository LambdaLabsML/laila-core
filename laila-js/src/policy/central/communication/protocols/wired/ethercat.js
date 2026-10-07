/**
 * EtherCAT (industrial fieldbus) communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over EtherCAT via a SOEM master binding on a
 * dedicated NIC. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"ethercat"``
 * - URI scheme ``ethercat://<slave>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** EtherCAT fieldbus transport. */
export class _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "ethercat";
  static _TOKEN_ALIASES = Object.freeze(new Set(["ethercat"]));
  static _DRIVER_MODULES = Object.freeze(["pysoem"]);
  static _DRIVER_EXTRA = "ethercat";

  /** Claim ``ethercat://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("ethercat://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.ethercat", { _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL });
