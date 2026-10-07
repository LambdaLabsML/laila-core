/**
 * PROFINET (industrial fieldbus) communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over PROFINET RT via the ``p-net`` stack on an
 * Ethernet interface. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"profinet"``
 * - URI scheme ``profinet://<station>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** PROFINET fieldbus transport. */
export class _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "profinet";
  static _TOKEN_ALIASES = Object.freeze(new Set(["profinet"]));
  static _DRIVER_MODULES = Object.freeze(["pnet"]);
  static _DRIVER_EXTRA = "profinet";

  /** Claim ``profinet://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("profinet://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.profinet", { _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL });
