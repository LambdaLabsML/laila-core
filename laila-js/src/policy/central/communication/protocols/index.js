/**
 * Communication protocol implementations.
 *
 * The base contract is ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL``; the reusable
 * RPC carriers live under ``./_carriers``. Concrete transports are grouped
 * into category subpackages (``ip_app``, ``local``, ``lpwan``,
 * ``short_range``, ``cellular``, ``wired``) and re-exported here for
 * convenience.
 *
 * Python's ``_autoload_transports`` walks the package and imports every
 * transport module so each one registers itself as a
 * ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` subclass. ESM has no package walk, so
 * the category ``index.js`` modules enumerate their transports explicitly and
 * are imported here for the same effect. Driver libraries are imported lazily
 * inside the transports' methods, so loading this module never pulls an
 * optional dependency.
 */
import { register } from "../../../../_compat/lazy.js";

export { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, comm_protocol_for_token, iter_comm_protocols, register_comm_protocol } from "./base.js";
export { _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL } from "./ip_app/tcp.js";
export { _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL } from "./ip_app/tls.js";
export { _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL } from "./ip_app/udp.js";
export { _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL } from "./local/loopback.js";
export { _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL } from "./local/unixsocket.js";
export { _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL } from "./tcpip.js";

// -- ``_autoload_transports()`` -------------------------------------------
export * as _carriers from "./_carriers/index.js";
export * as cellular from "./cellular/index.js";
export * as ip_app from "./ip_app/index.js";
export * as local from "./local/index.js";
export * as lpwan from "./lpwan/index.js";
export * as short_range from "./short_range/index.js";
export * as wired from "./wired/index.js";

export const __all__ = Object.freeze([
  "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
  "comm_protocol_for_token",
  "iter_comm_protocols",
]);

import * as _self from "./index.js";
register("laila.policy.central.communication.protocols", _self);
