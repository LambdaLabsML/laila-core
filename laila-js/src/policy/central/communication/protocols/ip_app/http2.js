/**
 * HTTP/2 communication transport.
 *
 * Carries JSON-RPC over a long-lived HTTP/2 stream. Built on the
 * point-to-point stream carrier; the endpoint must be configured before
 * peering and a missing configuration raises a clear capability error.
 *
 * - ``protocol_name`` ``"http2"`` (aliases ``h2``)
 * - URI scheme ``http2://host:port``
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { _P2PStreamRPCProtocol } from "../_carriers/p2p.js";
import { register_comm_protocol } from "../base.js";

/** HTTP/2 stream transport. */
export class _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL extends _P2PStreamRPCProtocol {
  static protocol_name = "http2";
  static _TOKEN_ALIASES = Object.freeze(new Set(["http2", "h2"]));

  /** Accept ``"http2"`` / ``"h2"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``http2://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("http2://");
  }

  async _open_stream() {
    this._require_drivers(["node:http2"], "http");
    throw new RuntimeError("HTTP/2 transport requires an established h2 connection/stream; configure the HTTP/2 endpoint before peering.");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.http2", { _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL });
