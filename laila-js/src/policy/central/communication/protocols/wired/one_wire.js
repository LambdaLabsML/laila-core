/**
 * 1-Wire (Dallas) communication transport (datagram).
 *
 * 1-Wire is a master/slave sensor bus (Linux ``w1`` sysfs); it is not a
 * symmetric peer link, so this lazy datagram transport raises a clear
 * hardware-requirement error on start.
 *
 * - ``protocol_name`` ``"1-wire"`` (aliases ``onewire`` / ``w1``)
 * - URI scheme ``onewire://<id>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** 1-Wire sensor-bus transport. */
export class _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "1-wire";
  static _TOKEN_ALIASES = Object.freeze(new Set(["1-wire", "onewire", "w1"]));

  /** Accept ``"1-wire"`` / ``"onewire"`` / ``"w1"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``onewire://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("onewire://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.one_wire", { _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL });
