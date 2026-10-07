/**
 * XMPP communication transport (broker-mediated).
 *
 * Carries JSON-RPC over an XMPP server via ``@xmpp/client`` (Python:
 * ``slixmpp``), addressing peers by their inbox (a per-policy JID resource).
 * Built on the broker carrier; the driver is imported on connect and a
 * missing library raises a clear capability error.
 *
 * - ``protocol_name`` ``"xmpp"`` (aliases ``jabber``)
 * - URI scheme ``xmpp://<peer_policy_id>``
 */
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _BrokerRPCProtocol } from "../_carriers/broker.js";
import { register_comm_protocol } from "../base.js";

/** XMPP broker-mediated transport. */
export class _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL extends _BrokerRPCProtocol {
  static protocol_name = "xmpp";
  static _TOKEN_ALIASES = Object.freeze(new Set(["xmpp", "jabber"]));

  static {
    define_fields(this, {
      jid: ["str", Field({ default: "" })],
      password: ["str", Field({ default: "" })],
      server_host: ["str", Field({ default: "127.0.0.1" })],
      server_port: ["int", Field({ default: 5222 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"xmpp"`` / ``"jabber"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``xmpp://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("xmpp://");
  }

  async _broker_connect() {
    const { "@xmpp/client": xmpp } = this._require_drivers(["@xmpp/client"], "xmpp");
    const [username, domain] = String(this.jid).split("@");
    this._client = xmpp.client({ service: `xmpp://${this.server_host}:${this.server_port}`, domain: domain || this.server_host, username, password: this.password });
    this._client.start().catch(() => {});
  }

  async _broker_subscribe(_topic) {
    // XMPP delivers to our JID directly; messages are routed to
    // _feed_message by the client's message handler.
    if (this._client !== null && this._client !== undefined) {
      this._client.on("stanza", (stanza) => {
        if (!stanza.is("message")) return;
        const body = stanza.getChildText("body");
        if (body !== null && body !== undefined) this._feed_message(Buffer.from(String(body), "latin1"));
      });
    }
  }

  async _broker_publish(topic, data) {
    const { xml } = this._require_drivers(["@xmpp/client"], "xmpp")["@xmpp/client"];
    await this._client.send(xml("message", { to: topic, type: "chat" }, xml("body", {}, Buffer.from(data).toString("latin1"))));
  }

  async _broker_close() {
    if (this._client !== null && this._client !== undefined) {
      try {
        await this._client.stop();
      } catch {
        /* ignore */
      }
      this._client = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.xmpp", { _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL });
