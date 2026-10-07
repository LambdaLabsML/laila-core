/**
 * Z-Wave communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over a Z-Wave controller via the Z-Wave JS
 * server driver. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"zwave"`` (aliases ``z-wave``)
 * - URI scheme ``zwave://<node>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** Z-Wave transport. */
export class _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "zwave";
  static _TOKEN_ALIASES = Object.freeze(new Set(["zwave", "z-wave"]));
  static _DRIVER_MODULES = Object.freeze(["zwave_js_server"]);
  static _DRIVER_EXTRA = "zwave";

  /** Accept ``"zwave"`` / ``"z-wave"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``zwave://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("zwave://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.zwave", { _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL });
