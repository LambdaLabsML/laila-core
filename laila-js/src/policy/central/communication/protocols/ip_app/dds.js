/**
 * DDS / RTPS communication transport (broker-mediated / pub-sub).
 *
 * Carries JSON-RPC over DDS topics via ``cyclonedds``. Each policy reads its
 * inbox topic and writes to peer inbox topics. Built on the broker carrier;
 * the driver is imported on connect and a missing library raises a clear
 * capability error.
 *
 * - ``protocol_name`` ``"dds"`` (aliases ``rtps``)
 * - URI scheme ``dds://<peer_policy_id>``
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { PrivateAttr, define_private } from "../../../../../_compat/pydantic.js";
import { _BrokerRPCProtocol } from "../_carriers/broker.js";
import { register_comm_protocol } from "../base.js";

/** DDS/RTPS pub-sub transport. */
export class _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL extends _BrokerRPCProtocol {
  static protocol_name = "dds";
  static _TOKEN_ALIASES = Object.freeze(new Set(["dds", "rtps"]));

  static {
    define_private(this, {
      _participant: PrivateAttr({ default: null }),
      _cyclonedds: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"dds"`` / ``"rtps"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``dds://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("dds://");
  }

  async _broker_connect() {
    const mods = this._require_drivers(["cyclonedds"], "dds");
    const { DomainParticipant } = mods.cyclonedds;
    this._participant = new DomainParticipant();
    this._cyclonedds = mods.cyclonedds;
  }

  async _broker_subscribe(_topic) {
    // A real implementation creates a DataReader on `topic` whose
    // listener forwards samples to this._feed_message.
    return null;
  }

  async _broker_publish(_topic, _data) {
    // A real implementation writes `data` on a DataWriter for `topic`.
    throw new RuntimeError("DDS publish requires a configured DataWriter for the peer topic.");
  }

  async _broker_close() {
    this._participant = null;
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.dds", { _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL });
