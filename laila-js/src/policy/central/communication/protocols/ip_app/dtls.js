/**
 * DTLS communication transport (datagram, secured).
 *
 * DTLS secures a UDP datagram channel (the basis of secure CoAP). It requires
 * a DTLS-capable stack (OpenSSL DTLS bindings), which the runtime does not
 * expose, so this lazy datagram transport raises a clear requirement error
 * on start.
 *
 * - ``protocol_name`` ``"dtls"``
 * - URI scheme ``dtls://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** DTLS-secured datagram transport. */
export class _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "dtls";
  static _TOKEN_ALIASES = Object.freeze(new Set(["dtls"]));

  /** Claim ``dtls://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("dtls://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.dtls", { _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL });
