/**
 * Abstract base class for communication-protocol implementations.
 *
 * A *protocol* is the transport layer that sits between
 * ``_LAILA_IDENTIFIABLE_COMMUNICATION`` and the wire. Concrete subclasses
 * know how to:
 *
 * - Open a listener that accepts inbound peering handshakes.
 * - Initiate outbound peering handshakes against a URI + secret.
 * - Encode RPC frames (``path`` + ``args`` + ``kwargs``), send them to a
 *   known peer, and decode the response.
 * - Tear themselves down cleanly on ``stop``.
 *
 * The base class only fixes the *shape* of that contract -- the specifics
 * (TCP/IP via WebSockets, shared-memory queues, InfiniBand, gRPC, ...) are
 * entirely up to the subclass.
 *
 * Each protocol instance carries a ``_communication`` back-reference set by
 * ``_LAILA_IDENTIFIABLE_COMMUNICATION.add_connection`` so it can forward
 * inbound RPCs back to the policy and notify the communication layer when
 * peers connect or disconnect.
 *
 * Subclass discovery
 * ------------------
 * Python walks ``__subclasses__()``; JS has no such hook, so every transport
 * class registers itself with ``register_comm_protocol(cls)`` in a
 * ``static {}`` block (the ``@`` of a metaclass). ``iter_comm_protocols`` /
 * ``comm_protocol_for_token`` read that registry.
 */
import { NotImplementedError } from "../../../../_compat/errors.js";
import { register } from "../../../../_compat/lazy.js";
import { ConfigDict, PrivateAttr, define_private } from "../../../../_compat/pydantic.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../../../../atomic/definitions/locally_atomic_identifiable_object.js";
import { CLICapable } from "../../../../basics/definitions/cli_capable.js";
import { _COMM_PROTOCOL_SCOPE } from "../../../../macros/strings.js";

/** Ordered registry of every loaded ``_LAILA_IDENTIFIABLE_COMM_PROTOCOL`` subclass (by class name). */
const _SUBCLASSES = new Map();

/**
 * Record *cls* as a loaded transport (Python's ``__subclasses__`` walk).
 * @template T
 * @param {T} cls
 * @returns {T}
 */
export function register_comm_protocol(cls) {
  _SUBCLASSES.set(cls.name, cls);
  return cls;
}

/**
 * Base class for transport-layer protocol implementations.
 *
 * Subclasses (TCP/IP, shared memory, InfiniBand, etc.) implement the
 * abstract interface below. Each protocol instance is registered on a
 * ``_LAILA_IDENTIFIABLE_COMMUNICATION`` via ``add_connection``, which sets
 * the ``_communication`` back-reference and calls ``start``.
 *
 * All public lifecycle methods are required to be *idempotent*:
 *
 * - Calling ``start`` on a running protocol should be a no-op.
 * - Calling ``stop`` on a stopped protocol should be a no-op.
 */
export class _LAILA_IDENTIFIABLE_COMM_PROTOCOL extends CLICapable(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_COMM_PROTOCOL_SCOPE] }),
      _communication: PrivateAttr({ default: null }),
    });
  }

  /**
   * Short, stable token identifying this transport family (e.g.
   * ``"tcpip"``, ``"lora"``, ``"bluetooth"``). It is the value users pass as
   * ``comm_protocol`` to ``laila.request`` and the key
   * ``_LAILA_IDENTIFIABLE_COMMUNICATION._resolve_protocol_for_token``
   * matches against. Subclasses MUST override it.
   */
  static protocol_name = "base";

  /**
   * Whether a peering holds a long-lived connection (``true``) or is
   * re-established per request (``false``). Persistent transports avoid a
   * handshake per call and are pinged by the communication layer's
   * liveness loop; per-request transports are not pinged.
   */
  static persistent = true;

  /**
   * Whether ``ping`` is meaningful for this transport. The communication
   * liveness loop only pings protocols that are both ``persistent`` and
   * ``supports_ping``.
   */
  static supports_ping = true;

  /**
   * Whether this transport can carry opaque byte-stream *lanes* alongside
   * RPC (``laila.peers[gid][name]`` / ``laila.relay``). Stream-capable
   * carriers set this ``true``; a transport that only speaks framed
   * JSON-RPC leaves it ``false`` and ``peers[gid][name]`` raises
   * ``ConnectionError`` for it.
   */
  static supports_channels = false;

  /** Instance mirrors of the class-level flags (``self.protocol_name`` in Python). */
  get protocol_name() {
    return this.constructor.protocol_name;
  }
  get persistent() {
    return this.constructor.persistent;
  }
  get supports_ping() {
    return this.constructor.supports_ping;
  }
  get supports_channels() {
    return this.constructor.supports_channels;
  }

  // ------------------------------------------------------------------
  // Abstract interface
  // ------------------------------------------------------------------

  /**
   * Return ``true`` if this protocol answers to the transport *token*.
   *
   * The default compares *token* case-insensitively against
   * ``protocol_name``. Subclasses may override to accept aliases (e.g.
   * ``"tcp"`` / ``"tcp-ip"`` for the TCP/IP protocol).
   * @param {string} token
   */
  static matches_token(token) {
    return token.toLowerCase() === this.protocol_name.toLowerCase();
  }

  /**
   * Return ``true`` if this protocol claims responsibility for *uri*.
   *
   * Used by ``_LAILA_IDENTIFIABLE_COMMUNICATION._resolve_protocol_for_uri``
   * to dispatch outbound peering. Subclasses typically inspect the URI
   * scheme, e.g. ``uri.startsWith("ws://")``. The default returns ``false``.
   * @param {string} _uri
   */
  static can_handle_uri(_uri) {
    return false;
  }

  /** Start the protocol's listener loop and any background workers. */
  start() {
    throw new NotImplementedError();
  }

  /** Awaitable ``start`` (defaults to the synchronous form). */
  async start_async() {
    return this.start();
  }

  /** Tear down all connections and release transport resources. */
  stop() {
    throw new NotImplementedError();
  }

  /** Awaitable ``stop`` (defaults to the synchronous form). */
  async stop_async() {
    return this.stop();
  }

  /**
   * Establish an outbound connection/peering to *uri* using *secret*.
   *
   * On success the protocol must call
   * ``_LAILA_IDENTIFIABLE_COMMUNICATION._register_peer`` so a
   * ``RemotePolicyProxy`` is created in the local registry.
   * @param {string} _uri
   * @param {string} _secret
   * @returns {string} The remote policy's ``global_id``.
   */
  connect(_uri, _secret) {
    throw new NotImplementedError();
  }

  /** Awaitable ``connect`` (defaults to the synchronous form). */
  async connect_async(uri, secret) {
    return this.connect(uri, secret);
  }

  /**
   * Tear down the connection to a single *peer_id*.
   *
   * Idempotent and safe by default (a transport that holds no peer has
   * nothing to disconnect).
   * @param {string} _peer_id
   */
  disconnect(_peer_id) {
    return null;
  }

  /** Awaitable ``disconnect``. */
  async disconnect_async(peer_id) {
    return this.disconnect(peer_id);
  }

  /**
   * Return ``true`` if *peer_id* is reachable right now.
   *
   * Used by the communication layer's liveness loop to detect dead peers
   * without touching the data path. The default returns ``false``.
   * @param {string} _peer_id
   * @param {number|null} [_timeout]
   */
  ping(_peer_id, _timeout = null) {
    return false;
  }

  /** Awaitable ``ping``. */
  async ping_async(peer_id, timeout = null) {
    return this.ping(peer_id, timeout);
  }

  /**
   * Send an RPC frame to *peer_id* and block for the deserialized response.
   *
   * The frame layout is the protocol's choice; the only contract is that
   * the remote ``_LAILA_IDENTIFIABLE_COMMUNICATION._execute_rpc`` is
   * invoked with ``path``, ``args``, ``kwargs`` and that the result (or a
   * future-shaped envelope) flows back here.
   * @param {string} _peer_id
   * @param {string[]} _path
   * @param {any[]} _args
   * @param {object} _kwargs
   */
  send_rpc(_peer_id, _path, _args, _kwargs) {
    throw new NotImplementedError();
  }

  /** Awaitable ``send_rpc`` (defaults to the synchronous form). */
  async send_rpc_async(peer_id, path, args, kwargs) {
    return this.send_rpc(peer_id, path, args, kwargs);
  }

  /**
   * Return ``true`` if this protocol currently holds a live connection to *peer_id*.
   *
   * Used by ``_LAILA_IDENTIFIABLE_COMMUNICATION._send_rpc`` to pick the
   * right protocol when more than one is registered.
   * @param {string} _peer_id
   */
  has_peer(_peer_id) {
    return false;
  }
}

/** Return every concrete transport class currently loaded. */
export function iter_comm_protocols() {
  return [..._SUBCLASSES.values()].filter((c) => c.protocol_name !== "base" && c.protocol_name !== "carrier");
}

/**
 * Return the transport class answering to *token*, or ``null``.
 * @param {string} token
 */
export function comm_protocol_for_token(token) {
  for (const cls of iter_comm_protocols()) if (cls.matches_token(token)) return cls;
  return null;
}

register("laila.policy.central.communication.protocols.base", {
  _LAILA_IDENTIFIABLE_COMM_PROTOCOL,
  iter_comm_protocols,
  comm_protocol_for_token,
  register_comm_protocol,
});
