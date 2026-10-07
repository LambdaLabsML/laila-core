/**
 * AMQP (RabbitMQ) communication transport.
 *
 * Broker-mediated JSON-RPC built on the ``_BrokerRPCProtocol`` carrier via
 * ``amqplib`` (Python: ``aio-pika``). Each policy declares an auto-delete
 * inbox queue named ``laila/inbox/<policy_id>`` and addresses peers by
 * publishing to their queue through the default exchange.
 *
 * - ``protocol_name`` ``"amqp"`` (aliases ``amqps`` / ``rabbitmq``)
 * - URI scheme ``amqp://<peer_policy_id>`` (the broker is configured via
 *   ``broker_url``)
 *
 * ``amqplib`` is imported lazily; a missing library raises a clear,
 * actionable error and an unreachable broker surfaces as a connection error.
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { ConnectionError, RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _BrokerRPCProtocol } from "../_carriers/broker.js";
import { register_comm_protocol } from "../base.js";

const log = getLogger("laila.policy.central.communication.protocols.ip_app.amqp");
void log;

const _INSTALL_HINT = "The AMQP transport requires amqplib. Install it with `npm install amqplib` (Python: `pip install laila-core[amqp]`).";

/**
 * AMQP/RabbitMQ broker-mediated transport.
 *
 * Fields
 * ------
 * broker_url : str, default ``"amqp://guest:guest@127.0.0.1/"``
 *     Connection URL for the AMQP broker.
 */
export class _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL extends _BrokerRPCProtocol {
  static protocol_name = "amqp";
  static _TOKEN_ALIASES = Object.freeze(new Set(["amqp", "amqps", "rabbitmq"]));

  static {
    define_fields(this, {
      broker_url: ["str", Field({ default: "amqp://guest:guest@127.0.0.1/" })],
    });
    define_private(this, {
      _conn: PrivateAttr({ default: null }),
      _channel: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"amqp"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``amqp://`` and ``amqps://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("amqp://") || uri.startsWith("amqps://");
  }

  async _broker_connect() {
    let amqplib;
    try {
      amqplib = this._require_drivers(["amqplib"], "amqp").amqplib;
    } catch (exc) {
      const err = new RuntimeError(_INSTALL_HINT);
      err.__cause__ = exc;
      throw err;
    }
    try {
      this._conn = await amqplib.connect(this.broker_url);
    } catch (exc) {
      const err = new ConnectionError(`Could not reach AMQP broker at ${this.broker_url}: ${exc?.message ?? exc}`);
      err.__cause__ = exc;
      throw err;
    }
    this._channel = await this._conn.createChannel();
  }

  async _broker_subscribe(topic) {
    await this._channel.assertQueue(topic, { autoDelete: true });
    await this._channel.consume(topic, (message) => {
      if (message === null) return;
      try {
        this._feed_message(message.content);
      } finally {
        this._channel.ack(message);
      }
    });
  }

  async _broker_publish(topic, data) {
    this._channel.sendToQueue(topic, Buffer.from(data));
  }

  async _broker_close() {
    if (this._conn !== null && this._conn !== undefined) {
      try {
        await asyncio.wait_for(this._conn.close(), 2.0);
      } catch {
        /* best effort */
      }
      this._conn = null;
      this._channel = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.amqp", { _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL });
