/**
 * RS-485 communication transport.
 *
 * RS-485 is a half-duplex, multidrop serial bus. At the byte level it is the
 * same UART stream, so this subclasses the UART transport and enables RS-485
 * direction-control (DE/RE toggling) when the driver supports it.
 *
 * - ``protocol_name`` ``"rs485"`` (aliases ``rs-485``)
 * - URI scheme ``rs485:///dev/ttyUSB0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "./uart.js";

const log = getLogger("laila.policy.central.communication.protocols.wired.rs485");

/**
 * RS-485 half-duplex multidrop serial transport.
 *
 * Parameters
 * ----------
 * rs485_mode : bool, default ``True``
 *     Enable RS-485 direction control on the port.
 */
export class _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "rs485";
  static _TOKEN_ALIASES = Object.freeze(new Set(["rs485", "rs-485"]));

  static {
    define_fields(this, {
      rs485_mode: ["bool", Field({ default: true })],
    });
  }

  /** Claim ``rs485://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("rs485://");
  }

  _configure_serial(ser) {
    if (!this.rs485_mode) return;
    try {
      // ``serialport`` has no RS485Settings equivalent (pyserial's
      // ``serial.rs485``); direction control is delegated to the kernel
      // driver / adapter. Record the requested mode on the port object so
      // it is introspectable like pyserial's ``rs485_mode`` attribute.
      if (typeof ser.set !== "function") throw new TypeError("no modem-control interface");
      ser.rs485_mode = { rts_level_for_tx: true, rts_level_for_rx: false, delay_before_tx: null, delay_before_rx: null };
    } catch {
      log.debug("RS-485 direction control unavailable on %s", this.port);
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.rs485", { _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL });
