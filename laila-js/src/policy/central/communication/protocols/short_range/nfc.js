/**
 * NFC communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over an NFC reader (``nfcpy``-style driver).
 * Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"nfc"``
 * - URI scheme ``nfc://<reader>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** NFC transport. */
export class _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "nfc";
  static _TOKEN_ALIASES = Object.freeze(new Set(["nfc"]));
  static _DRIVER_MODULES = Object.freeze(["nfc"]);
  static _DRIVER_EXTRA = "nfc";

  /** Claim ``nfc://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("nfc://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.nfc", { _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL });
