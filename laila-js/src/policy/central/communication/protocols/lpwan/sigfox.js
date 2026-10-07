/**
 * Sigfox communication transport.
 *
 * Sigfox is an ultra-narrowband LPWAN reached through a serial AT modem, so
 * this subclasses the serial (UART) transport. Note Sigfox uplinks are tiny
 * (12 bytes) and rate-limited (~140 msgs/day); the caller is responsible for
 * keeping payloads within those limits.
 *
 * - ``protocol_name`` ``"sigfox"``
 * - URI scheme ``sigfox:///dev/ttyUSB0`` (port comes from the ``port`` field)
 */
import { register } from "../../../../../_compat/lazy.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "../wired/uart.js";

/** Sigfox LPWAN transport (serial AT modem). */
export class _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL {
  static protocol_name = "sigfox";
  static _TOKEN_ALIASES = Object.freeze(new Set(["sigfox"]));

  /** Claim ``sigfox://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("sigfox://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.sigfox", { _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL });
