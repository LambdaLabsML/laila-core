/**
 * TLS communication transport (TCP + SSL).
 *
 * Subclasses the raw-TCP transport and wraps both the listener and the
 * outbound connection in a TLS context built from the configured certificate
 * material. Working but **config-required**: a server needs ``certfile`` (+
 * ``keyfile``); there is no auto-generated certificate, so a missing cert
 * raises a clear error at ``start``.
 *
 * - ``protocol_name`` ``"tls"`` (aliases ``tcps`` / ``ssl`` / ``tls/tcp``)
 * - URI scheme ``tls://host:port`` (also ``tcps://``)
 */
import fs from "node:fs";

import * as asyncio from "../../../../../_compat/asyncio.js";
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { split_host_port } from "../_carriers/uri.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "./tcp.js";

/**
 * TLS-secured peer-to-peer transport.
 *
 * Fields
 * ------
 * certfile : str, optional
 *     PEM certificate for the listener (required to serve).
 * keyfile : str, optional
 *     PEM private key for ``certfile``.
 * cafile : str, optional
 *     CA bundle used to verify the peer. When omitted on the client side,
 *     verification is disabled (``check_hostname=False``,
 *     ``verify_mode=CERT_NONE``) so self-signed test certs work.
 * server_hostname : str, optional
 *     Name presented/verified during the client TLS handshake.
 */
export class _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "tls";
  static _TOKEN_ALIASES = Object.freeze(new Set(["tls", "tcps", "ssl", "tls/tcp"]));

  static {
    define_fields(this, {
      certfile: ["str | None", Field({ default: null })],
      keyfile: ["str | None", Field({ default: null })],
      cafile: ["str | None", Field({ default: null })],
      server_hostname: ["str | None", Field({ default: null })],
    });
  }

  /** Claim ``tls://`` and ``tcps://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("tls://") || uri.startsWith("tcps://");
  }

  /** ``ssl.SSLContext(PROTOCOL_TLS_SERVER)`` with the cert chain loaded (Node tls options). */
  _server_ssl_context() {
    if (!this.certfile) {
      throw new RuntimeError("TLS transport requires `certfile` (and `keyfile`) to serve. Set them on the protocol or via laila.args.");
    }
    return { cert: fs.readFileSync(this.certfile), key: fs.readFileSync(this.keyfile || this.certfile) };
  }

  /** ``ssl.SSLContext(PROTOCOL_TLS_CLIENT)``; unverified when no ``cafile`` (Node tls options). */
  _client_ssl_context() {
    if (this.cafile) return { ca: fs.readFileSync(this.cafile), rejectUnauthorized: true };
    return { rejectUnauthorized: false, checkServerIdentity: () => undefined };
  }

  async _serve() {
    // No certificate -> client-only endpoint (can peer out, cannot accept
    // inbound). Avoids forcing a cert on pure clients.
    if (!this.certfile) return null;
    const server = await asyncio.start_server((r, w) => this._handle_inbound_stream(r, w), this.host, this.port, { ssl: this._server_ssl_context() });
    this._bound_port = server.sockets[0].getsockname()[1];
    return server;
  }

  async _open_connection(uri) {
    const [host, port] = split_host_port(uri);
    return await asyncio.open_connection(host, port, { ssl: this._client_ssl_context(), server_hostname: this.server_hostname || host });
  }

  /** Convenience wrapper building the ``tls://`` URI for ``connect``. */
  connect_tls(host, port, secret) {
    return this.connect(`tls://${host}:${port}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.tls", { _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL });
