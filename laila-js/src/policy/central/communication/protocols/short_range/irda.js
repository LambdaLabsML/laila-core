/**
 * IrDA / infrared communication transport (datagram).
 *
 * IrDA stacks are legacy and have no maintained host-side driver, so this
 * lazy datagram transport raises a clear hardware requirement error on
 * start.
 *
 * - ``protocol_name`` ``"irda"`` (aliases ``ir``)
 * - URI scheme ``irda://<device>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** IrDA / IR transport. */
export class _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "irda";
  static _TOKEN_ALIASES = Object.freeze(new Set(["irda", "ir"]));

  /** Accept ``"irda"`` / ``"ir"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``irda://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("irda://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.irda", { _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL });
