/**
 * HTTP/3 (QUIC) communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over QUIC. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"http3"`` (aliases ``h3`` / ``quic``)
 * - URI scheme ``http3://host:port`` (also ``quic://``)
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** HTTP/3 (QUIC) transport. */
export class _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "http3";
  static _TOKEN_ALIASES = Object.freeze(new Set(["http3", "h3", "quic"]));
  static _DRIVER_MODULES = Object.freeze(["aioquic"]);
  static _DRIVER_EXTRA = "quic";

  /** Accept ``"http3"`` / ``"h3"`` / ``"quic"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``http3://`` and ``quic://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("http3://") || uri.startsWith("quic://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.http3", { _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL });
