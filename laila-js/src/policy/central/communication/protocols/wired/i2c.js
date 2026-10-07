/**
 * I2C communication transport (register mailbox).
 *
 * Carries JSON-RPC over an I2C slave's register-mapped *mailbox* using the
 * ``_RegisterRPCProtocol`` carrier and ``i2c-bus`` (Python: ``smbus2``). The
 * local policy is the bus master; the peer device must expose a cooperating
 * inbox/outbox register area.
 *
 * - ``protocol_name`` ``"i2c"``
 * - URI scheme ``i2c://<bus>/<addr>``
 *
 * The driver is imported lazily; a missing library raises a clear error.
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _RegisterRPCProtocol } from "../_carriers/register.js";
import { register_comm_protocol } from "../base.js";

/** I2C register-mailbox transport (master side). */
export class _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL extends _RegisterRPCProtocol {
  static protocol_name = "i2c";
  static _TOKEN_ALIASES = Object.freeze(new Set(["i2c"]));

  static {
    define_fields(this, {
      bus_number: ["int", Field({ default: 1 })],
      address: ["int", Field({ default: 0x20 })],
      inbox_reg: ["int", Field({ default: 0x00 })],
      outbox_reg: ["int", Field({ default: 0x40 })],
    });
    define_private(this, {
      _bus: PrivateAttr({ default: null }),
    });
  }

  /** Claim ``i2c://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("i2c://");
  }

  async _open_bus() {
    const { "i2c-bus": i2c } = this._require_drivers(["i2c-bus"], "i2c");
    this._bus = i2c.openSync(this.bus_number);
  }

  async _close_bus() {
    if (this._bus !== null && this._bus !== undefined) {
      try {
        this._bus.closeSync();
      } catch {
        /* ignore */
      }
      this._bus = null;
    }
  }

  async _deliver(data) {
    const body = Buffer.from(data);
    const payload = Buffer.concat([Buffer.from([(body.length >> 8) & 0xff, body.length & 0xff]), body]);
    for (let offset = 0; offset < payload.length; offset += 30) {
      const chunk = payload.subarray(offset, offset + 30);
      this._bus.writeI2cBlockSync(this.address, this.outbox_reg, chunk.length, Buffer.from(chunk));
    }
  }

  async _poll_inbound() {
    const header = Buffer.alloc(2);
    this._bus.readI2cBlockSync(this.address, this.inbox_reg, 2, header);
    const length = (header[0] << 8) | header[1];
    if (length === 0) return null;
    const parts = [];
    let remaining = length;
    while (remaining > 0) {
      const n = Math.min(30, remaining);
      const buf = Buffer.alloc(n);
      this._bus.readI2cBlockSync(this.address, this.inbox_reg + 2, n, buf);
      parts.push(buf);
      remaining -= n;
    }
    this._bus.writeI2cBlockSync(this.address, this.inbox_reg, 2, Buffer.from([0, 0]));
    return Buffer.concat(parts).subarray(0, length);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.i2c", { _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL });
