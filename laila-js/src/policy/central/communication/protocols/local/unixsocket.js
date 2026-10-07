/**
 * Unix domain socket communication transport.
 *
 * Same-host inter-policy RPC over an ``asyncio.start_unix_server`` stream --
 * the fast, file-permission-secured local channel (no TCP port exposed).
 * Built on the reliable ``_StreamRPCProtocol`` carrier.
 *
 * - ``protocol_name`` ``"unix"`` (aliases ``uds`` / ``unixsocket``)
 * - URI scheme ``unix:///path/to/socket``
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path_mod from "node:path";

import * as asyncio from "../../../../../_compat/asyncio.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _StreamRPCProtocol } from "../_carriers/stream.js";
import { uri_path } from "../_carriers/uri.js";
import { register_comm_protocol } from "../base.js";

/**
 * Unix-domain-socket peer-to-peer transport.
 *
 * Fields
 * ------
 * path : str, optional
 *     Filesystem path of the listening socket. When empty a stable path is
 *     derived under the system temp dir from this protocol's uuid, so
 *     ``bound_path`` is meaningful even with no config.
 */
export class _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL extends _StreamRPCProtocol {
  static protocol_name = "unix";
  static _TOKEN_ALIASES = Object.freeze(new Set(["unix", "uds", "unixsocket"]));

  static {
    define_fields(this, {
      path: ["str", Field({ default: "" })],
    });
    define_private(this, {
      _bound_path: PrivateAttr({ default: null }),
    });
  }

  /** Path the server is (or would be) listening on. */
  get bound_path() {
    return this._bound_path || this._effective_path();
  }

  _effective_path() {
    if (this.path) return this.path;
    const digest = createHash("sha1").update(String(this.uuid), "utf8").digest("hex").slice(0, 16);
    return path_mod.join(os.tmpdir(), `laila_unix_${digest}.sock`);
  }

  /** Accept ``"unix"`` plus aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``unix://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("unix://");
  }

  async _serve() {
    const path = this._effective_path();
    if (fs.existsSync(path)) fs.unlinkSync(path);
    const server = await asyncio.start_unix_server((r, w) => this._handle_inbound_stream(r, w), path);
    this._bound_path = path;
    return server;
  }

  async _open_connection(uri) {
    const path = uri_path(uri);
    return await asyncio.open_unix_connection(path);
  }

  async _close_server(server) {
    await super._close_server(server);
    if (this._bound_path && fs.existsSync(this._bound_path)) {
      try {
        fs.unlinkSync(this._bound_path);
      } catch {
        /* OSError: best effort */
      }
    }
    this._bound_path = null;
  }

  /** Convenience wrapper building the ``unix://`` URI for ``connect``. */
  connect_unix(path, secret) {
    return this.connect(`unix://${path}`, secret);
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.local.unixsocket", { _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL });
