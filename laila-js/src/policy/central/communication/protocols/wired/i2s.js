/**
 * I2S (audio) communication transport (datagram, experimental).
 *
 * I2S is a synchronous audio bus, not a packet link; carrying RPC over it
 * requires modulating data into an audio stream, which needs dedicated DSP.
 * This lazy datagram transport raises a clear requirement error on start.
 *
 * - ``protocol_name`` ``"i2s"``
 * - URI scheme ``i2s://<card>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** I2S audio-bus transport (experimental). */
export class _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "i2s";
  static _TOKEN_ALIASES = Object.freeze(new Set(["i2s"]));

  /** Claim ``i2s://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("i2s://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.i2s", { _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL });
