/**
 * Ethernet (incl. PoE) communication transport.
 *
 * Ethernet is an IP carrier: once the link is up the data path is plain TCP,
 * so this transport subclasses the raw-TCP transport and only adds link
 * identity (the bound NIC) and its own token/URI. Physical link bring-up
 * (cable/PoE, DHCP) is handled by the OS; ``interface`` is recorded for
 * diagnostics and future link-management.
 *
 * - ``protocol_name`` ``"ethernet"`` (aliases ``eth`` / ``poe``)
 * - URI scheme ``ethernet://host:port``
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "./tcp.js";

/** Ethernet/PoE transport (TCP data path over a wired NIC). */
export class _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "ethernet";
  static _TOKEN_ALIASES = Object.freeze(new Set(["ethernet", "eth", "poe"]));

  static {
    define_fields(this, {
      // Optional NIC to bind/diagnose (e.g. ``"eth0"``). Informational;
      // the socket still binds by ``host``.
      interface: ["str | None", Field({ default: null })],
    });
  }

  /** Claim ``ethernet://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("ethernet://");
  }

  /** Convenience wrapper building the ``ethernet://`` URI. */
  connect_ethernet(host, port, secret) {
    return this.connect(`ethernet://${host}:${port}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.ethernet", { _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL });
