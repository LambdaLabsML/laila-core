/**
 * SPI communication transport (register mailbox).
 *
 * Carries JSON-RPC over a SPI slave's framed *mailbox* using the
 * ``_RegisterRPCProtocol`` carrier and ``spi-device`` (Python: ``spidev``).
 * The local policy is the SPI master; each transfer is a length-prefixed
 * frame and polling clocks out a 2-byte length header followed by the
 * payload.
 *
 * - ``protocol_name`` ``"spi"``
 * - URI scheme ``spi://<bus>.<device>``
 *
 * The driver is imported lazily; a missing library raises a clear error.
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _RegisterRPCProtocol } from "../_carriers/register.js";
import { register_comm_protocol } from "../base.js";

/** SPI register-mailbox transport (master side). */
export class _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL extends _RegisterRPCProtocol {
  static protocol_name = "spi";
  static _TOKEN_ALIASES = Object.freeze(new Set(["spi"]));

  static {
    define_fields(this, {
      bus: ["int", Field({ default: 0 })],
      device: ["int", Field({ default: 0 })],
      max_speed_hz: ["int", Field({ default: 1_000_000 })],
    });
    define_private(this, {
      _spi: PrivateAttr({ default: null }),
    });
  }

  /** Claim ``spi://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("spi://");
  }

  async _open_bus() {
    const { "spi-device": spidev } = this._require_drivers(["spi-device"], "spi");
    this._spi = spidev.openSync(this.bus, this.device, { maxSpeedHz: this.max_speed_hz });
  }

  async _close_bus() {
    if (this._spi !== null && this._spi !== undefined) {
      try {
        this._spi.closeSync();
      } catch {
        /* ignore */
      }
      this._spi = null;
    }
  }

  /** ``spidev.xfer2`` -- full-duplex transfer returning the received bytes. */
  _xfer2(send) {
    const sendBuffer = Buffer.from(send);
    const receiveBuffer = Buffer.alloc(sendBuffer.length);
    this._spi.transferSync([{ sendBuffer, receiveBuffer, byteLength: sendBuffer.length, speedHz: this.max_speed_hz }]);
    return receiveBuffer;
  }

  async _deliver(data) {
    const body = Buffer.from(data);
    const frame = Buffer.concat([Buffer.from([(body.length >> 8) & 0xff, body.length & 0xff]), body]);
    this._xfer2(frame);
  }

  async _poll_inbound() {
    const header = this._xfer2([0, 0]);
    const length = (header[0] << 8) | header[1];
    if (length === 0) return null;
    return this._xfer2(Buffer.alloc(length));
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.spi", { _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL });
