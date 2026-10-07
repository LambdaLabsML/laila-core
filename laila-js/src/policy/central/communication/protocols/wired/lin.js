/**
 * LIN (Local Interconnect Network) communication transport.
 *
 * LIN is a single-wire automotive serial bus whose byte layer is a UART
 * (break + sync + frame), so this transport subclasses the serial (UART)
 * transport with LIN's token/URI.
 *
 * - ``protocol_name`` ``"lin"``
 * - URI scheme ``lin:///dev/ttyUSB0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "./uart.js";

/** LIN automotive serial transport. */
export class _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "lin";
  static _TOKEN_ALIASES = Object.freeze(new Set(["lin"]));

  /** Claim ``lin://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("lin://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.lin", { _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL });
