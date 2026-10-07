/**
 * RS-232 communication transport.
 *
 * RS-232 is a serial-line electrical standard; at the byte level it is the
 * same asynchronous UART stream, so this transport is a thin subclass of the
 * UART transport with its own token/URI.
 *
 * - ``protocol_name`` ``"rs232"`` (aliases ``rs-232``)
 * - URI scheme ``rs232:///dev/ttyS0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "./uart.js";

/** RS-232 serial transport. */
export class _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "rs232";
  static _TOKEN_ALIASES = Object.freeze(new Set(["rs232", "rs-232"]));

  /** Claim ``rs232://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("rs232://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.rs232", { _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL });
