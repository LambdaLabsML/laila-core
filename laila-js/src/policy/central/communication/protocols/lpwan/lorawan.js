/**
 * LoRaWAN communication transport (datagram radio).
 *
 * LoRaWAN end-devices are modems/firmware joined to a network server; there
 * is no general host-side driver to bring up automatically, so this lazy
 * datagram transport raises a clear hardware/infrastructure requirement
 * error on start (a LoRaWAN modem + network server/ChirpStack are required).
 *
 * - ``protocol_name`` ``"lorawan"``
 * - URI scheme ``lorawan://<deveui>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** LoRaWAN transport (modem + network server). */
export class _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "lorawan";
  static _TOKEN_ALIASES = Object.freeze(new Set(["lorawan"]));

  /** Claim ``lorawan://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("lorawan://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.lpwan.lorawan", { _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL });
