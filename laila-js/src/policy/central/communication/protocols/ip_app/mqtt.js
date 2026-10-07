/**
 * MQTT (and MQTT-SN) communication transport.
 *
 * Broker-mediated JSON-RPC built on the ``_BrokerRPCProtocol`` carrier. Each
 * policy subscribes to its inbox topic ``laila/inbox/<policy_id>`` on the
 * MQTT broker; peers address each other by publishing to those topics.
 *
 * - ``protocol_name`` ``"mqtt"`` (aliases ``mqtts`` / ``mqtt-sn``)
 * - URI scheme ``mqtt://<peer_policy_id>`` (the broker itself is given by
 *   ``broker_host`` / ``broker_port``)
 *
 * The ``mqtt`` package (Python: ``paho-mqtt``) is imported lazily; a missing
 * library raises a clear, actionable error and a missing broker surfaces as
 * a connection error.
 */
import { ConnectionError, RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _BrokerRPCProtocol } from "../_carriers/broker.js";
import { register_comm_protocol } from "../base.js";

const log = getLogger("laila.policy.central.communication.protocols.ip_app.mqtt");
void log;

const _INSTALL_HINT = "The MQTT transport requires mqtt. Install it with `npm install mqtt` (Python: `pip install laila-core[mqtt]`).";

/**
 * MQTT broker-mediated transport.
 *
 * Fields
 * ------
 * broker_host : str, default ``"127.0.0.1"``
 *     MQTT broker hostname.
 * broker_port : int, default ``1883``
 *     MQTT broker port.
 */
export class _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL extends _BrokerRPCProtocol {
  static protocol_name = "mqtt";
  static _TOKEN_ALIASES = Object.freeze(new Set(["mqtt", "mqtts", "mqtt-sn"]));

  static {
    define_fields(this, {
      broker_host: ["str", Field({ default: "127.0.0.1" })],
      broker_port: ["int", Field({ default: 1883 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"mqtt"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``mqtt://`` and ``mqtts://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("mqtt://") || uri.startsWith("mqtts://");
  }

  async _broker_connect() {
    let mqtt;
    try {
      mqtt = this._require_drivers(["mqtt"], "mqtt").mqtt;
    } catch (exc) {
      const err = new RuntimeError(_INSTALL_HINT);
      err.__cause__ = exc;
      throw err;
    }
    const client = mqtt.connect({ host: this.broker_host, port: this.broker_port, protocol: "mqtt" });
    client.on("message", (_topic, payload) => this._feed_message(payload));
    try {
      await new Promise((resolve, reject) => {
        client.once("connect", resolve);
        client.once("error", reject);
      });
    } catch (exc) {
      try {
        client.end(true);
      } catch {
        /* ignore */
      }
      const err = new ConnectionError(`Could not reach MQTT broker at ${this.broker_host}:${this.broker_port}: ${exc?.message ?? exc}`);
      err.__cause__ = exc;
      throw err;
    }
    this._client = client;
  }

  async _broker_subscribe(topic) {
    this._client.subscribe(topic);
  }

  async _broker_publish(topic, data) {
    this._client.publish(topic, Buffer.from(data));
  }

  async _broker_close() {
    if (this._client !== null && this._client !== undefined) {
      try {
        this._client.end(true);
      } catch {
        /* ignore */
      }
      this._client = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.mqtt", { _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL });
