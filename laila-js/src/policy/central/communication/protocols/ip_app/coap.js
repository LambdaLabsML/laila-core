/**
 * CoAP communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over CoAP (UDP) via ``aiocoap``. Built on the
 * lazy datagram carrier; the driver is imported on start and a missing
 * library raises a clear capability error.
 *
 * - ``protocol_name`` ``"coap"`` (aliases ``coaps``)
 * - URI scheme ``coap://host:port`` (also ``coaps://``)
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** CoAP transport. */
export class _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "coap";
  static _TOKEN_ALIASES = Object.freeze(new Set(["coap", "coaps"]));
  static _DRIVER_MODULES = Object.freeze(["aiocoap"]);
  static _DRIVER_EXTRA = "coap";

  /** Accept ``"coap"`` / ``"coaps"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``coap://`` and ``coaps://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("coap://") || uri.startsWith("coaps://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.coap", { _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL });
