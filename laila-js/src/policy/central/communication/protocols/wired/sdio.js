/**
 * SDIO communication transport (datagram).
 *
 * SDIO is a block/register card interface, not a symmetric peer link, and
 * has no userspace peer-RPC driver, so this lazy datagram transport raises a
 * clear hardware-requirement error on start.
 *
 * - ``protocol_name`` ``"sdio"``
 * - URI scheme ``sdio://<func>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { _LazyDatagramRPCProtocol } from "../_carriers/lazy.js";
import { register_comm_protocol } from "../base.js";

/** SDIO transport. */
export class _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL extends _LazyDatagramRPCProtocol {
  static protocol_name = "sdio";
  static _TOKEN_ALIASES = Object.freeze(new Set(["sdio"]));

  /** Claim ``sdio://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("sdio://");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.sdio", { _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL });
