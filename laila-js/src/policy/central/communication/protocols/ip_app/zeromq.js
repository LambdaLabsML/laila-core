/**
 * ZeroMQ (and nanomsg/nng) communication transport.
 *
 * Brokerless message transport built on the ``_BrokerRPCProtocol`` carrier.
 * ZeroMQ has no central broker, so each policy *binds* a ``PULL`` socket on
 * a deterministic endpoint derived from its ``global_id`` (its inbox) and
 * addresses a peer by ``connect``-ing a ``PUSH`` socket to the peer's
 * endpoint. Request/response correlation is handled by the carrier.
 *
 * - ``protocol_name`` ``"zeromq"`` (aliases ``zmq`` / ``nanomsg`` / ``nng``)
 * - URI scheme ``zmq://<peer_policy_id>``
 * - transport endpoints default to ``ipc://`` under the temp dir; set
 *   ``endpoint_scheme="tcp"`` to use TCP instead.
 *
 * The ``zeromq`` package (Python: ``pyzmq``) is imported lazily; a missing
 * library raises a clear, actionable error.
 */
import { createHash } from "node:crypto";
import os from "node:os";
import path_mod from "node:path";

import * as asyncio from "../../../../../_compat/asyncio.js";
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _BrokerRPCProtocol } from "../_carriers/broker.js";
import { register_comm_protocol } from "../base.js";

const log = getLogger("laila.policy.central.communication.protocols.ip_app.zeromq");

const _INSTALL_HINT = "The ZeroMQ transport requires zeromq. Install it with `npm install zeromq` (Python: `pip install laila-core[zmq]`).";

/** ZeroMQ PUSH/PULL brokerless transport. */
export class _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL extends _BrokerRPCProtocol {
  static protocol_name = "zeromq";
  static _TOKEN_ALIASES = Object.freeze(new Set(["zeromq", "zmq", "nanomsg", "nng"]));

  static {
    define_fields(this, {
      endpoint_scheme: ["str", Field({ default: "ipc" })],
      tcp_host: ["str", Field({ default: "127.0.0.1" })],
    });
    define_private(this, {
      _ctx: PrivateAttr({ default: null }),
      _zmq: PrivateAttr({ default: null }),
      _pull: PrivateAttr({ default: null }),
      _pushes: PrivateAttr({ default_factory: () => new Map() }),
      _recv_task: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"zeromq"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``zmq://`` and ``zeromq://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("zmq://") || uri.startsWith("zeromq://");
  }

  _endpoint_for(policy_id) {
    const digest = createHash("sha1").update(String(policy_id), "utf8").digest("hex").slice(0, 16);
    if (this.endpoint_scheme === "ipc") return `ipc://${path_mod.join(os.tmpdir(), `laila_zmq_${digest}.ipc`)}`;
    // tcp fallback uses a deterministic high port from the digest
    const port = 20000 + Number(BigInt(`0x${digest}`) % 20000n);
    return `tcp://${this.tcp_host}:${port}`;
  }

  _inbox_topic() {
    const pid = this._communication ? this._communication.policy_id : null;
    return this._endpoint_for(pid);
  }

  _peer_inbox(peer_policy_id) {
    return this._endpoint_for(peer_policy_id);
  }

  async _broker_connect() {
    let zmq;
    try {
      zmq = this._require_drivers(["zeromq"], "zmq").zeromq;
    } catch (exc) {
      const err = new RuntimeError(_INSTALL_HINT);
      err.__cause__ = exc;
      throw err;
    }
    this._ctx = new zmq.Context();
    this._zmq = zmq;
  }

  async _broker_subscribe(topic) {
    // `topic` is our own inbox endpoint; bind a PULL and consume it.
    this._pull = new this._zmq.Pull({ context: this._ctx });
    await this._pull.bind(topic);
    this._recv_task = asyncio.ensure_future(() => this._recv_loop());
  }

  async _recv_loop() {
    try {
      for (;;) {
        const [data] = await this._pull.receive();
        this._feed_message(data);
      }
    } catch (e) {
      if (!(e instanceof asyncio.CancelledError)) log.debug("ZeroMQ recv loop ended", { exc_info: e });
    }
  }

  async _broker_publish(topic, data) {
    let push = this._pushes.get(topic);
    if (push === undefined) {
      push = new this._zmq.Push({ context: this._ctx });
      push.connect(topic);
      this._pushes.set(topic, push);
    }
    await push.send(Buffer.from(data));
  }

  async _broker_close() {
    if (this._recv_task !== null && this._recv_task !== undefined) this._recv_task.cancel();
    for (const push of this._pushes.values()) {
      try {
        push.linger = 0;
        push.close();
      } catch {
        /* ignore */
      }
    }
    this._pushes.clear();
    if (this._pull !== null && this._pull !== undefined) {
      try {
        this._pull.linger = 0;
        this._pull.close();
      } catch {
        /* ignore */
      }
      this._pull = null;
    }
    if (this._ctx !== null && this._ctx !== undefined) {
      this._ctx = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.zeromq", { _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL });
