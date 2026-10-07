/**
 * Lazy datagram carrier for driver/hardware-backed packet radios.
 *
 * Many packet transports (Zigbee, Thread, Z-Wave, ANT, ESP-NOW, ...) ride on
 * the ``_DatagramRPCProtocol`` carrier but need a third-party driver and real
 * radio hardware. ``_LazyDatagramRPCProtocol`` standardises their connection
 * hook: it imports the declared driver(s) via ``_require_drivers`` (raising a
 * clear ``laila-core[<extra>]`` error when missing) and then defers to
 * ``_setup_endpoint``, which a concrete transport overrides with the real
 * radio bring-up.
 *
 * For transports with no off-the-shelf driver (the device is firmware, e.g.
 * ESP-NOW), ``_DRIVER_MODULES`` is empty and ``_setup_endpoint`` raises a
 * clear, actionable runtime error describing the hardware requirement --
 * never a silent stub.
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { repr } from "../../../../../_compat/pyrepr.js";
import { _DatagramRPCProtocol } from "./datagram.js";
import { uri_authority } from "./uri.js";

/** Datagram carrier that lazy-loads a radio driver on start. */
export class _LazyDatagramRPCProtocol extends _DatagramRPCProtocol {
  /** import names required before the radio can be brought up. */
  static _DRIVER_MODULES = Object.freeze([]);
  /** optional-extra name for the install hint. */
  static _DRIVER_EXTRA = "";

  async _create_datagram_endpoint() {
    const mods = this._require_drivers(this.constructor._DRIVER_MODULES, this.constructor._DRIVER_EXTRA);
    return await this._setup_endpoint(mods);
  }

  /**
   * Bring up the real radio endpoint.
   *
   * Overridden by transports with a concrete driver integration. The default
   * makes the hardware requirement explicit for transports whose device is
   * firmware with no host-side driver.
   */
  async _setup_endpoint(_drivers) {
    throw new RuntimeError(
      `The ${repr(this.protocol_name)} transport requires dedicated radio ` +
        "hardware/firmware and an out-of-band interface configuration; " +
        "no host-side driver endpoint is available to bring up automatically.",
    );
  }

  /** Default: the URI authority is the radio node address. */
  async _resolve_peer_addr(uri) {
    return uri_authority(uri);
  }
}

register("laila.policy.central.communication.protocols._carriers.lazy", { _LazyDatagramRPCProtocol });
