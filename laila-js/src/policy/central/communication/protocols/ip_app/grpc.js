/**
 * gRPC communication transport.
 *
 * Carries JSON-RPC frames over a bidirectional gRPC stream via
 * ``@grpc/grpc-js`` (Python: ``grpcio``). Built on the point-to-point stream
 * carrier; the driver is imported on start and a missing library raises a
 * clear capability error.
 *
 * - ``protocol_name`` ``"grpc"``
 * - URI scheme ``grpc://host:port``
 */
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, define_fields } from "../../../../../_compat/pydantic.js";
import { _P2PStreamRPCProtocol } from "../_carriers/p2p.js";
import { register_comm_protocol } from "../base.js";

/** gRPC bidirectional-stream transport. */
export class _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL extends _P2PStreamRPCProtocol {
  static protocol_name = "grpc";
  static _TOKEN_ALIASES = Object.freeze(new Set(["grpc"]));

  static {
    define_fields(this, {
      target: ["str", Field({ default: "127.0.0.1:50051" })],
    });
  }

  /** Claim ``grpc://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("grpc://");
  }

  async _open_stream() {
    this._require_drivers(["@grpc/grpc-js"], "grpc");
    throw new RuntimeError("gRPC transport requires a generated bidi-streaming service stub; configure the gRPC channel/target before peering.");
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.ip_app.grpc", { _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL });
