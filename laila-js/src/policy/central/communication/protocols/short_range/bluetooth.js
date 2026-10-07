/**
 * Bluetooth communication protocol (scaffold / planned transport).
 *
 * This module sketches a future ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL``
 * transport for **Bluetooth** (Classic RFCOMM / BLE GATT). It is
 * intentionally *not implemented yet*: every method that would touch the
 * radio raises ``NotImplementedError`` with a note on what a real
 * implementation must do.
 *
 * - ``protocol_name`` ``"bluetooth"`` (aliases ``bt`` / ``ble``)
 * - URI scheme ``bt://<addr>``
 */
import { NotImplementedError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, register_comm_protocol } from "../base.js";

const _NOT_IMPLEMENTED = "The Bluetooth transport is a planned protocol and is not implemented yet. Use the TCP/IP protocol (comm_protocol='tcpip') for now.";

/**
 * Planned Bluetooth transport (scaffold; raises until implemented).
 *
 * Subclass of ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` reserving the
 * ``"bluetooth"`` token and ``bt://`` URI scheme. All transport methods
 * raise ``NotImplementedError`` for now -- the class exists so transport
 * selection (``comm_protocol="bluetooth"``) and URI routing have a real
 * target to resolve to.
 */
export class _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
  static protocol_name = "bluetooth";
  static _TOKEN_ALIASES = Object.freeze(new Set(["bluetooth", "bt", "ble"]));

  /** Accept ``"bluetooth"`` plus aliases (``bt``, ``ble``). */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``bt://`` URIs for this transport. */
  static can_handle_uri(uri) {
    return uri.startsWith("bt://");
  }

  /** Bring up the Bluetooth link. Not implemented yet. */
  start() {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }

  /**
   * Tear down the Bluetooth link. No-op (nothing is ever started).
   *
   * ``stop`` must be idempotent and safe per the base contract, so the
   * scaffold returns cleanly rather than raising.
   */
  stop() {
    return null;
  }

  /** Peer with a remote device over Bluetooth. Not implemented yet. */
  connect(_uri, _secret) {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }

  /** Send an RPC frame over Bluetooth. Not implemented yet. */
  send_rpc(_peer_id, _path, _args, _kwargs) {
    throw new NotImplementedError(_NOT_IMPLEMENTED);
  }

  /** No Bluetooth peers are ever held by this scaffold. */
  has_peer(_peer_id) {
    return false;
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.short_range.bluetooth", { _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL });
