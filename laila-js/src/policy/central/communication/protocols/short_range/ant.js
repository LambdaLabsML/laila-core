/**
 * ANT / ANT+ communication transport (datagram radio).
 *
 * Carries fragmented JSON-RPC over an ANT USB stick via an ``openant``-style
 * driver. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"ant"`` (aliases ``ant+``)
 * - URI scheme ``ant://<device>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** ANT / ANT+ transport. */
export class _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "ant";
  static _TOKEN_ALIASES = Object.freeze(new Set(["ant", "ant+"]));
  static _DRIVER_MODULES = Object.freeze(["openant"]);
  static _DRIVER_EXTRA = "ant";

  /** Accept ``"ant"`` / ``"ant+"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``ant://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("ant://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.ant", { _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL });
