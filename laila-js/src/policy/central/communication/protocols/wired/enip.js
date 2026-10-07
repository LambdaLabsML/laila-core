/**
 * EtherNet/IP (CIP) communication transport.
 *
 * Carries JSON-RPC over an EtherNet/IP tag mailbox using the
 * ``_RegisterRPCProtocol`` carrier and ``ethernet-ip`` (Python: ``pycomm3``).
 * The local policy reads/writes string tags on the peer PLC that serve as
 * the inbox/outbox.
 *
 * - ``protocol_name`` ``"ethernet-ip"`` (aliases ``enip`` / ``ethernetip``)
 * - URI scheme ``enip://host``
 *
 * The driver is imported lazily; a missing library raises a clear error.
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _RegisterRPCProtocol } from "../_carriers/register.js";
import { register_comm_protocol } from "../base.js";

/** EtherNet/IP tag-mailbox transport. */
export class _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL extends _RegisterRPCProtocol {
  static protocol_name = "ethernet-ip";
  static _TOKEN_ALIASES = Object.freeze(new Set(["ethernet-ip", "enip", "ethernetip"]));

  static {
    define_fields(this, {
      host: ["str", Field({ default: "127.0.0.1" })],
      inbox_tag: ["str", Field({ default: "LAILA_IN" })],
      outbox_tag: ["str", Field({ default: "LAILA_OUT" })],
    });
    define_private(this, {
      _plc: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"ethernet-ip"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``enip://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("enip://");
  }

  async _open_bus() {
    const { "ethernet-ip": enip } = this._require_drivers(["ethernet-ip"], "enip");
    this._plc = new enip.Controller();
    this._Tag = enip.Tag;
    await this._plc.connect(this.host);
  }

  async _close_bus() {
    if (this._plc !== null && this._plc !== undefined) {
      try {
        this._plc.destroy();
      } catch {
        /* ignore */
      }
      this._plc = null;
    }
  }

  async _deliver(data) {
    const tag = new this._Tag(this.outbox_tag);
    tag.value = Buffer.from(data).toString("base64");
    await this._plc.writeTag(tag);
  }

  async _poll_inbound() {
    const tag = new this._Tag(this.inbox_tag);
    await this._plc.readTag(tag);
    const value = tag.value;
    if (!value) return null;
    const clear = new this._Tag(this.inbox_tag);
    clear.value = "";
    await this._plc.writeTag(clear);
    return Buffer.from(String(value), "base64");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.enip", { _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL });
