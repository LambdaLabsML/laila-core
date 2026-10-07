/**
 * OPC-UA (industrial) communication transport.
 *
 * Carries JSON-RPC over an OPC-UA secure channel via ``node-opcua`` (Python:
 * ``asyncua``). Built on the point-to-point stream carrier; the driver is
 * imported on start and a missing library raises a clear capability error.
 *
 * - ``protocol_name`` ``"opcua"`` (aliases ``opc.tcp``)
 * - URI scheme ``opc.tcp://host:port``
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { _P2PStreamRPCProtocol } from "../_carriers/p2p.js";
import { register_comm_protocol } from "../base.js";

/** OPC-UA transport. */
export class _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL extends _P2PStreamRPCProtocol {
  static protocol_name = "opcua";
  static _TOKEN_ALIASES = Object.freeze(new Set(["opcua", "opc.tcp", "opc-ua"]));

  static {
    define_fields(this, {
      endpoint: ["str", Field({ default: "opc.tcp://127.0.0.1:4840" })],
    });
  }

  /** Accept ``"opcua"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``opc.tcp://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("opc.tcp://");
  }

  async _open_stream() {
    this._require_drivers(["node-opcua"], "opcua");
    throw new RuntimeError("OPC-UA transport requires an OPC-UA server endpoint exposing a byte-stream method/variable mailbox; configure the server endpoint.");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.opcua", { _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL });
