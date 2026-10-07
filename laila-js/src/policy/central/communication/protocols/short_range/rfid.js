/**
 * RFID communication transport.
 *
 * RFID reader modules are reached through a serial interface, so this
 * subclasses the serial (UART) transport with RFID's token/URI.
 *
 * - ``protocol_name`` ``"rfid"``
 * - URI scheme ``rfid:///dev/ttyUSB0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "../wired/uart.js";

/** RFID transport (serial reader module). */
export class _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "rfid";
  static _TOKEN_ALIASES = Object.freeze(new Set(["rfid"]));

  /** Claim ``rfid://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("rfid://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.rfid", { _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL });
