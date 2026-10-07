/**
 * USB communication transport (CDC-ACM serial).
 *
 * The most portable USB device class for a byte stream is CDC-ACM, which the
 * OS exposes as a serial port (e.g. ``/dev/ttyACM0``). This transport
 * therefore subclasses the serial (UART) transport. Bulk/HID modes would
 * require a libusb binding and are out of scope for this CDC-ACM transport.
 *
 * - ``protocol_name`` ``"usb"`` (aliases ``cdc-acm`` / ``cdcacm``)
 * - URI scheme ``usb:///dev/ttyACM0``
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "./uart.js";

/** USB CDC-ACM transport (serial over USB). */
export class _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "usb";
  static _TOKEN_ALIASES = Object.freeze(new Set(["usb", "cdc-acm", "cdcacm"]));

  /** Claim ``usb://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("usb://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.usb", { _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL });
