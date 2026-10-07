/**
 * UWB (ultra-wideband) communication transport (datagram).
 *
 * UWB data links are vendor hardware (DW1000/DW3000 modules) with no generic
 * host-side driver, so this lazy datagram transport raises a clear hardware
 * requirement error on start.
 *
 * - ``protocol_name`` ``"uwb"``
 * - URI scheme ``uwb://<anchor>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** UWB transport (vendor hardware). */
export class _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "uwb";
  static _TOKEN_ALIASES = Object.freeze(new Set(["uwb"]));

  /** Claim ``uwb://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("uwb://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.uwb", { _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL });
