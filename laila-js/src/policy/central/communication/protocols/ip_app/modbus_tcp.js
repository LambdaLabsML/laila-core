/**
 * Modbus-TCP communication transport.
 *
 * Carries JSON-RPC over a Modbus-TCP holding-register *mailbox* using the
 * ``_RegisterRPCProtocol`` carrier and ``modbus-serial`` (Python:
 * ``pymodbus``). The local policy acts as the Modbus master: it writes
 * framed messages into the peer's inbox register block and polls its own
 * outbox block for replies. The peer must expose a cooperating Modbus
 * datastore as the mailbox.
 *
 * - ``protocol_name`` ``"modbus-tcp"`` (aliases ``modbus`` / ``modbustcp``)
 * - URI scheme ``modbustcp://host:port``
 *
 * The driver is imported lazily; a missing library raises a clear,
 * actionable error.
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { Struct } from "../../../../../_compat/struct.js";
import { _RegisterRPCProtocol } from "../_carriers/register.js";
import { register_comm_protocol } from "../base.js";

const _H = new Struct(">H");

/** Modbus-TCP register-mailbox transport (master side). */
export class _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL extends _RegisterRPCProtocol {
  static protocol_name = "modbus-tcp";
  static _TOKEN_ALIASES = Object.freeze(new Set(["modbus-tcp", "modbus", "modbustcp"]));

  static {
    define_fields(this, {
      host: ["str", Field({ default: "127.0.0.1" })],
      port: ["int", Field({ default: 502 })],
      unit_id: ["int", Field({ default: 1 })],
      inbox_base: ["int", Field({ default: 0 })],
      outbox_base: ["int", Field({ default: 2000 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"modbus-tcp"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``modbustcp://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("modbustcp://");
  }

  async _open_bus() {
    const { "modbus-serial": ModbusRTU } = this._require_drivers(["modbus-serial"], "modbus");
    this._client = new ModbusRTU();
    await this._client.connectTCP(this.host, { port: this.port });
    this._client.setID(this.unit_id);
  }

  async _close_bus() {
    if (this._client !== null && this._client !== undefined) {
      try {
        this._client.close();
      } catch {
        /* ignore */
      }
      this._client = null;
    }
  }

  /** ``[len, word, word, ...]`` big-endian 16-bit words with a leading byte-length cell. */
  static _to_words(data) {
    const padded = data.length % 2 ? Buffer.concat([data, Buffer.from([0])]) : Buffer.from(data);
    const words = [data.length];
    for (let i = 0; i < padded.length; i += 2) words.push(_H.unpack(padded.subarray(i, i + 2))[0]);
    return words;
  }

  static _from_words(words) {
    const length = words[0];
    const body = Buffer.concat(words.slice(1).map((w) => _H.pack(w)));
    return body.subarray(0, length);
  }

  async _deliver(data) {
    await this._client.writeRegisters(this.outbox_base, this.constructor._to_words(Buffer.from(data)));
  }

  async _poll_inbound() {
    const rr = await this._client.readHoldingRegisters(this.inbox_base, 1);
    if (!rr || !rr.data || !rr.data.length || rr.data[0] === 0) return null;
    const length = rr.data[0];
    const words_needed = 1 + Math.trunc((length + 1) / 2);
    const full = await this._client.readHoldingRegisters(this.inbox_base, words_needed);
    // clear the length cell to acknowledge consumption
    await this._client.writeRegister(this.inbox_base, 0);
    return this.constructor._from_words(full.data);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.modbus_tcp", { _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL });
