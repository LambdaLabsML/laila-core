/**
 * Satellite (Iridium SBD / Swarm) communication transport.
 *
 * Satellite short-burst-data modems (Iridium SBD, Swarm) are reached through
 * a serial AT interface, so this subclasses the serial (UART) transport.
 * Payloads are small and costly; the caller is responsible for keeping
 * messages within the modem's limits.
 *
 * - ``protocol_name`` ``"satellite"`` (aliases ``iridium`` / ``swarm`` / ``sbd``)
 * - URI scheme ``satellite:///dev/ttyUSB0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "../wired/uart.js";

/** Satellite SBD transport (serial AT modem). */
export class _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "satellite";
  static _TOKEN_ALIASES = Object.freeze(new Set(["satellite", "iridium", "swarm", "sbd"]));

  /** Claim ``satellite://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("satellite://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.satellite", { _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL });
