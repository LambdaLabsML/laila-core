/**
 * UDP communication transport.
 *
 * Connectionless datagram transport built on the ``_DatagramRPCProtocol``
 * carrier, which layers fragmentation, ack/retry and dedup on top of raw UDP
 * so the lossy link still carries reliable JSON-RPC.
 *
 * - ``protocol_name`` ``"udp"`` (aliases ``udp4`` / ``udp6``)
 * - URI scheme ``udp://host:port``
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _DatagramEndpoint, _DatagramRPCProtocol } from "../_carriers/datagram.js";
import { split_host_port } from "../_carriers/uri.js";
import { register_comm_protocol } from "../base.js";

/**
 * UDP peer-to-peer transport with a reliability layer.
 *
 * Fields
 * ------
 * host : str, default ``"0.0.0.0"``
 *     Local bind address.
 * port : int, default ``0``
 *     Local UDP port (``0`` lets the OS choose; see ``bound_port``).
 */
export class _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL extends _DatagramRPCProtocol {
  static protocol_name = "udp";
  static _TOKEN_ALIASES = Object.freeze(new Set(["udp", "udp4", "udp6"]));

  static {
    define_fields(this, {
      host: ["str", Field({ default: "0.0.0.0" })],
      port: ["int", Field({ default: 0 })],
    });
    define_private(this, {
      _bound_port: PrivateAttr({ default: null }),
    });
  }

  /** OS-assigned local port after ``start``. */
  get bound_port() {
    return this._bound_port !== null && this._bound_port !== undefined ? this._bound_port : this.port;
  }

  /** Accept ``"udp"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``udp://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("udp://");
  }

  async _create_datagram_endpoint() {
    const loop = asyncio.get_running_loop();
    const [transport, proto] = await loop.create_datagram_endpoint(() => new _DatagramEndpoint((a, d) => this._feed_packet(a, d)), { local_addr: [this.host, this.port] });
    this._bound_port = transport.get_extra_info("sockname")[1];
    return [transport, proto];
  }

  async _resolve_peer_addr(uri) {
    const [host, port] = split_host_port(uri);
    return [host, port];
  }

  /** Convenience wrapper building the ``udp://`` URI for ``connect``. */
  connect_udp(host, port, secret) {
    return this.connect(`udp://${host}:${port}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.udp", { _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL });
