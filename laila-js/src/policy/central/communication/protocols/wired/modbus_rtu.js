/**
 * Modbus-RTU (over RS-485) communication transport.
 *
 * Same register-mailbox approach as Modbus-TCP but over an RS-485 serial
 * line via ``modbus-serial``'s buffered RTU client.
 *
 * - ``protocol_name`` ``"modbus-rtu"`` (aliases ``modbusrtu``)
 * - URI scheme ``modbusrtu:///dev/ttyUSB0``
 *
 * The driver is imported lazily; a missing library raises a clear error.
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { register_comm_protocol } from "../base.js";
import { _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL } from "../ip_app/modbus_tcp.js";

/** Modbus-RTU register-mailbox transport over RS-485. */
export class _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL extends _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL {
  static protocol_name = "modbus-rtu";
  static _TOKEN_ALIASES = Object.freeze(new Set(["modbus-rtu", "modbusrtu"]));

  static {
    define_fields(this, {
      port: ["str", Field({ default: "" })], // serial device, e.g. /dev/ttyUSB0
      baudrate: ["int", Field({ default: 9600 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
    });
  }

  /** Claim ``modbusrtu://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("modbusrtu://");
  }

  async _open_bus() {
    const { "modbus-serial": ModbusRTU } = this._require_drivers(["modbus-serial"], "modbus");
    if (!this.port) {
      throw new RuntimeError(`${this.constructor.name} requires a serial \`port\` (e.g. '/dev/ttyUSB0').`);
    }
    this._client = new ModbusRTU();
    await this._client.connectRTUBuffered(this.port, { baudRate: this.baudrate });
    this._client.setID(this.unit_id);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.modbus_rtu", { _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL });
