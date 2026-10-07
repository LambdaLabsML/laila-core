/**
 * Cellular (generic WWAN) communication transport.
 *
 * Cellular is an IP carrier: once the modem has a data session (PDP/PDN
 * context up), the data path is plain TCP, so this transport subclasses the
 * raw-TCP transport and adds modem/APN identity plus its own token/URI.
 * Bringing the data session up is a modem/OS concern (``ModemManager`` / AT
 * commands); ``apn`` / ``modem_device`` are recorded for diagnostics and
 * future link bring-up.
 *
 * - ``protocol_name`` ``"cellular"`` (aliases ``wwan`` / ``modem``)
 * - URI scheme ``cellular://host:port``
 *
 * Specific generations (2G/4G/5G) subclass this with their own tokens.
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "../ip_app/tcp.js";

/** Generic cellular WWAN transport (TCP data path over a modem). */
export class _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL {
  static protocol_name = "cellular";
  static _TOKEN_ALIASES = Object.freeze(new Set(["cellular", "wwan", "modem"]));

  static {
    define_fields(this, {
      /** Access Point Name for the data session (carrier-specific). */
      apn: ["str | None", Field({ default: null })],
      /** Modem control device (e.g. ``"/dev/cdc-wdm0"``). Informational. */
      modem_device: ["str | None", Field({ default: null })],
    });
  }

  /** Claim ``cellular://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("cellular://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.cellular.cellular", { _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL });
