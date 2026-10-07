/**
 * Zigbee (802.15.4) communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over a Zigbee coordinator via a ``zigpy``-style
 * driver. Built on the lazy datagram carrier; the driver is imported on
 * start and a missing library raises a clear capability error.
 *
 * - ``protocol_name`` ``"zigbee"``
 * - URI scheme ``zigbee://<node>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** Zigbee 802.15.4 transport. */
export class _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "zigbee";
  static _TOKEN_ALIASES = Object.freeze(new Set(["zigbee"]));
  static _DRIVER_MODULES = Object.freeze(["zigpy"]);
  static _DRIVER_EXTRA = "zigbee";

  /** Claim ``zigbee://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("zigbee://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.zigbee", { _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL });
