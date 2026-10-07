/**
 * Raw TCP communication transport.
 *
 * A direct, framed TCP transport (no WebSocket/HTTP upgrade) built on the
 * reliable ``_StreamRPCProtocol`` carrier. Lighter than the WebSocket-based
 * ``tcpip`` transport and the natural choice for embedded peers that just
 * want a socket.
 *
 * - ``protocol_name`` ``"tcp"`` (aliases ``tcp4`` / ``tcp6`` / ``raw-tcp``)
 * - URI scheme ``tcp://host:port``
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _StreamRPCProtocol } from "../_carriers/stream.js";
import { split_host_port } from "../_carriers/uri.js";
import { register_comm_protocol } from "../base.js";

/**
 * Raw-TCP peer-to-peer transport (framed JSON-RPC over a TCP socket).
 *
 * Fields
 * ------
 * host : str, default ``"0.0.0.0"``
 *     Bind address for the listener.
 * port : int, default ``0``
 *     TCP port (``0`` lets the OS choose; see ``bound_port``).
 */
export class _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL extends _StreamRPCProtocol {
  static protocol_name = "tcp";
  static _TOKEN_ALIASES = Object.freeze(new Set(["tcp", "tcp4", "tcp6", "raw-tcp", "rawtcp"]));

  static {
    define_fields(this, {
      host: ["str", Field({ default: "0.0.0.0" })],
      port: ["int", Field({ default: 0 })],
    });
    define_private(this, {
      _bound_port: PrivateAttr({ default: null }),
    });
  }

  /** OS-assigned port after ``start`` (falls back to ``port``). */
  get bound_port() {
    return this._bound_port !== null && this._bound_port !== undefined ? this._bound_port : this.port;
  }

  /** Accept ``"tcp"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``tcp://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("tcp://");
  }

  /** Disable Nagle (TCP_NODELAY) so small RPC frames go out immediately. */
  _on_stream_ready(writer) {
    const sock = writer.get_extra_info("socket");
    if (sock !== null && sock !== undefined) {
      try {
        sock.setNoDelay(true);
      } catch {
        /* OSError / AttributeError */
      }
    }
  }

  async _serve() {
    const server = await asyncio.start_server((r, w) => this._handle_inbound_stream(r, w), this.host, this.port);
    this._bound_port = server.sockets[0].getsockname()[1];
    return server;
  }

  async _open_connection(uri) {
    const [host, port] = split_host_port(uri);
    return await asyncio.open_connection(host, port);
  }

  /** Convenience wrapper building the ``tcp://`` URI for ``connect``. */
  connect_tcp(host, port, secret) {
    return this.connect(`tcp://${host}:${port}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.tcp", { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL });
