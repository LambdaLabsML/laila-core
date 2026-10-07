/**
 * Thread (802.15.4 mesh) communication transport (datagram).
 *
 * Carries fragmented JSON-RPC over a Thread radio co-processor via the
 * Spinel/OpenThread host driver. Built on the lazy datagram carrier.
 *
 * - ``protocol_name`` ``"thread"``
 * - URI scheme ``thread://<node>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** Thread mesh transport. */
export class _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "thread";
  static _TOKEN_ALIASES = Object.freeze(new Set(["thread"]));
  static _DRIVER_MODULES = Object.freeze(["spinel"]);
  static _DRIVER_EXTRA = "thread";

  /** Claim ``thread://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("thread://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.thread", { _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL });
