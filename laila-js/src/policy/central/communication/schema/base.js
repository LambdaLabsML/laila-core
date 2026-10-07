/**
 * Core communication sub-system for inter-policy peer-to-peer RPC.
 *
 * The ``_LAILA_IDENTIFIABLE_COMMUNICATION`` class is the *central
 * communication* subsystem of every laila ``Policy``. Its job is to mediate
 * calls between local and remote policies without any of the upstream code
 * having to know which transport is in play. Concretely it owns three
 * things:
 *
 * - A *protocol registry* (``connections``): one or more transport-level
 *   drivers. Each protocol implements its own listener loop, peer-handshake,
 *   and wire encoding. The communication object only ever asks "do you
 *   handle this URI?" or "do you have this peer connected?" -- everything
 *   else is delegated.
 * - A *peer registry* (``peers``): a ``PeerRegistry`` (a ``dict``) from
 *   remote-policy ``global_id`` to a ``PeerProxy``. Proxies look like local
 *   policies to the rest of the codebase but route every method call through
 *   ``_send_rpc``; indexing one with a channel name (``peers[gid]["video"]``)
 *   yields a stream ``Channel``.
 * - An *inbound dispatcher* (``_execute_rpc``): when a protocol finishes
 *   deserializing an incoming RPC frame it hands the dotted attribute path +
 *   args/kwargs back to the communication object, which walks the path on
 *   the local policy and invokes the resolved method.
 *
 * The class is also responsible for *future virtualisation*: when a remote
 * call returns a future-shaped envelope (as marked by the
 * ``__laila_future__`` key) it transparently wraps the envelope in a
 * ``RemoteFuture`` that proxies status / wait / result calls back to the
 * originating peer (see ``_maybe_wrap_remote_future``).
 */
import * as asyncio from "../../../../_compat/asyncio.js";
import { AttributeError, ConnectionError, RuntimeError, ValueError } from "../../../../_compat/errors.js";
import { ThreadPoolExecutor } from "../../../../_compat/executor.js";
import { lazy, register } from "../../../../_compat/lazy.js";
import { getLogger } from "../../../../_compat/logging.js";
import { Annotated, BeforeValidator, ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { repr } from "../../../../_compat/pyrepr.js";
import { dict_get, dict_has, dict_set, getattr, is_plain_object } from "../../../../_compat/pytypes.js";
import { BoundedSemaphore, Event, Lock, Thread, with_lock } from "../../../../_compat/threading.js";
import { CLICapable, CLIExempt } from "../../../../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../basics/definitions/identifiable_object.js";
import { _CENTRAL_COMMUNICATION_SCOPE, _FUTURE_SCOPE, _GROUP_FUTURE_SCOPE } from "../../../../macros/strings.js";
import { _LAILA_IDENTIFIABLE_COMM_PROTOCOL } from "../protocols/base.js";
import { PeerProxy, PeerRegistry } from "../registry.js";

const log = getLogger("laila.policy.central.communication.schema.base");

/** Accept a plain mapping for ``peers`` and upgrade it to ``PeerRegistry``. */
export function _coerce_peer_registry(value) {
  if (value instanceof PeerRegistry) return value;
  if (value instanceof Map || is_plain_object(value)) return new PeerRegistry(value);
  return value;
}

/** ``laila._remote_policies`` when the root module is loaded, else ``null``. */
function _remote_policies() {
  try {
    return lazy("laila")._remote_policies ?? null;
  } catch {
    return null;
  }
}

/**
 * Central-communication hub for a policy.
 *
 * Owns transport protocols, the peer registry, and an inbound RPC
 * dispatcher. The full interaction loop is::
 *
 *     local user code
 *         -> RemotePolicyProxy.foo()
 *         -> Communication._send_rpc(...)
 *         -> Protocol.send_rpc(...)
 *         ~~ wire ~~
 *         -> remote Protocol receives, decodes
 *         -> remote Communication._execute_rpc(["foo"], args, kwargs)
 *         -> result returned along the same path in reverse
 *
 * Fields
 * ------
 * policy_id : str, optional
 *     ``global_id`` of the owning policy. Wired automatically by the policy
 *     during construction; only set manually in tests or when a
 *     communication object lives outside a policy.
 * peers : PeerRegistry
 *     Remote-policy ``global_id`` -> ``PeerProxy``. Populated by
 *     ``_register_peer`` after a successful handshake. Plain dict semantics;
 *     ``peers[gid][name]`` resolves a stream channel.
 * connections : dict[str, _LAILA_IDENTIFIABLE_COMM_PROTOCOL]
 *     Protocol ``global_id`` -> protocol instance. Each protocol manages its
 *     own connections, listeners and peer set; this map is just a registry
 *     that lets the communication layer find the right transport for a URI
 *     or peer.
 */
export class _LAILA_IDENTIFIABLE_COMMUNICATION extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      policy_id: ["str | None", CLIExempt({ default: null })],
      peers: [Annotated(PeerRegistry, BeforeValidator(_coerce_peer_registry)), CLIExempt({ default_factory: () => new PeerRegistry() })],
      connections: [`dict[str, ${_LAILA_IDENTIFIABLE_COMM_PROTOCOL.name}]`, CLIExempt({ default_factory: () => ({}) })],
      // How often (seconds) the async liveness loop pings each peer.
      liveness_interval: ["float", Field({ default: 15.0 })],
      // Whether the async liveness loop runs at all.
      liveness_enabled: ["bool", Field({ default: true })],
      // Max inbound RPCs this policy will run/queue concurrently across all
      // transports before rejecting new ones with ``ERR_BUSY``. Bounds the
      // per-policy backlog so a flood cannot exhaust memory; embedded /
      // low-resource configs should set this low (e.g. 32-64) via
      // ``laila.args``. Liveness pings bypass this cap entirely.
      max_inflight_rpcs: ["int", Field({ default: 1000 })],
    });
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_CENTRAL_COMMUNICATION_SCOPE] }),
      _local_policy: PrivateAttr({ default: null }),
      _liveness_thread: PrivateAttr({ default: null }),
      _liveness_stop: PrivateAttr({ default: null }),
      _rpc_semaphore: PrivateAttr({ default: null }),
      _rpc_semaphore_lock: PrivateAttr({ default: null }),
    });
  }

  // ------------------------------------------------------------------
  // Protocol management
  // ------------------------------------------------------------------

  /**
   * Register and start a transport protocol.
   *
   * Sets the protocol's back-reference, adds it to the registry, and calls
   * ``protocol.start()`` so the connection is live when this method
   * returns. Symmetric with ``remove_connection`` which calls
   * ``protocol.stop()``.
   */
  add_connection(protocol) {
    protocol._communication = this;
    dict_set(this.connections, protocol.global_id, protocol);
    protocol.start();
  }

  /** Awaitable ``add_connection``. */
  async add_connection_async(protocol) {
    protocol._communication = this;
    dict_set(this.connections, protocol.global_id, protocol);
    await protocol.start_async();
  }

  /**
   * Stop a transport protocol and remove it from this communication instance.
   *
   * The protocol is responsible for all its own cleanup -- closing sockets,
   * unregistering peers, etc. Communication only calls ``stop()`` and
   * removes the protocol from its registry.
   */
  remove_connection(protocol) {
    const proto_id = protocol.global_id;
    if (!dict_has(this.connections, proto_id)) return;
    protocol.stop();
    this._connections_delete(proto_id);
    protocol._communication = null;
  }

  /** Awaitable ``remove_connection``. */
  async remove_connection_async(protocol) {
    const proto_id = protocol.global_id;
    if (!dict_has(this.connections, proto_id)) return;
    await protocol.stop_async();
    this._connections_delete(proto_id);
    protocol._communication = null;
  }

  _connections_delete(proto_id) {
    if (this.connections instanceof Map) this.connections.delete(proto_id);
    else delete this.connections[proto_id];
  }

  /** ``list(self.connections.values())`` */
  _connection_values() {
    return this.connections instanceof Map ? [...this.connections.values()] : Object.values(this.connections);
  }

  /**
   * Find a registered protocol that can handle *uri*.
   *
   * Each protocol class implements a ``can_handle_uri`` classmethod (e.g.
   * TCP/IP returns ``true`` for ``ws://`` URIs). The first registered
   * protocol that claims the URI is returned; if none claims it, the first
   * registered protocol is returned as a best-effort fallback (the protocol
   * may still error out).
   *
   * @throws {ConnectionError} If no protocols are registered.
   */
  _resolve_protocol_for_uri(uri) {
    const protos = this._connection_values();
    if (protos.length === 0) throw new ConnectionError("No communication protocols configured. Call add_connection() first.");
    for (const proto of protos) {
      if (proto.constructor.can_handle_uri(uri)) return proto;
    }
    return protos[0];
  }

  /**
   * Find a registered protocol matching a transport *token*.
   *
   * Drives the ``comm_protocol`` argument of ``laila.request``. Each
   * protocol class implements ``matches_token`` (e.g. TCP/IP answers to
   * ``"tcpip"`` / ``"ws"``). When *token* is ``null`` the first registered
   * protocol is returned.
   *
   * @throws {ConnectionError} If no protocols are registered, or none matches *token*.
   */
  _resolve_protocol_for_token(token) {
    const protos = this._connection_values();
    if (protos.length === 0) throw new ConnectionError("No communication protocols configured. Call add_connection() first.");
    if (token === null || token === undefined) {
      // Transport-agnostic default: the first registered protocol.
      return protos[0];
    }
    for (const proto of protos) {
      if (proto.constructor.matches_token(token)) return proto;
    }
    throw new ConnectionError(`No registered communication protocol matches ${repr(token)}. Register one with add_connection().`);
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /**
   * Start every registered protocol's listener loop.
   *
   * Idempotent at the protocol level -- calling ``start`` on an
   * already-started protocol is a no-op. Logged at INFO so the policy
   * lifecycle is auditable.
   */
  start() {
    for (const proto of this._connection_values()) proto.start();
    this._start_liveness();
    log.info("Communication started for policy %s", this.policy_id);
  }

  /** Awaitable ``start``. */
  async start_async() {
    for (const proto of this._connection_values()) await proto.start_async();
    this._start_liveness();
    log.info("Communication started for policy %s", this.policy_id);
  }

  /**
   * Stop every registered protocol and drop all peer proxies.
   *
   * Note that the protocols are responsible for breaking their own
   * connections (each says goodbye to its peers, closes its endpoints and
   * its event loop); this method just clears the in-memory peer registry
   * afterwards so subsequent code does not try to talk to detached proxies.
   * Best-effort: a protocol whose ``stop()`` raises is logged and the
   * remaining ones still stop.
   */
  stop() {
    this._stop_liveness();
    for (const proto of this._connection_values()) {
      try {
        proto.stop();
      } catch (e) {
        log.warning("Protocol %s failed to stop cleanly for policy %s", proto.constructor.name, this.policy_id, { exc_info: e });
      }
    }
    for (const peer_id of [...this.peers.keys()]) this._unregister_peer(peer_id);
    log.info("Communication stopped for policy %s", this.policy_id);
  }

  /** Awaitable ``stop``. */
  async stop_async() {
    await this._stop_liveness_async();
    for (const proto of this._connection_values()) {
      try {
        await proto.stop_async();
      } catch (e) {
        log.warning("Protocol %s failed to stop cleanly for policy %s", proto.constructor.name, this.policy_id, { exc_info: e });
      }
    }
    for (const peer_id of [...this.peers.keys()]) this._unregister_peer(peer_id);
    log.info("Communication stopped for policy %s", this.policy_id);
  }

  // ------------------------------------------------------------------
  // Peer management
  // ------------------------------------------------------------------

  /**
   * Initiate a peering connection to a remote policy.
   *
   * Resolves the appropriate protocol for *uri* and delegates the
   * transport-level handshake.
   * @param {string} uri URI of the remote policy (e.g. ``"ws://host:port"``).
   * @param {string} secret The remote policy's ``peer_secret_key``.
   * @returns {string} The ``global_id`` of the newly peered remote policy.
   */
  add_peer(uri, secret) {
    const proto = this._resolve_protocol_for_uri(uri);
    return proto.connect(uri, secret);
  }

  /** Awaitable ``add_peer``. */
  async add_peer_async(uri, secret) {
    const proto = this._resolve_protocol_for_uri(uri);
    return proto.connect_async(uri, secret);
  }

  /**
   * Peer with a remote policy over TCP/IP (WebSocket).
   *
   * Convenience wrapper that builds the ``ws://`` URI internally.
   * @returns {string} The ``global_id`` of the newly peered remote policy.
   */
  add_tcpip_peer(host, port, secret) {
    return this.add_peer(`ws://${host}:${port}`, secret);
  }

  /** Awaitable ``add_tcpip_peer``. */
  async add_tcpip_peer_async(host, port, secret) {
    return this.add_peer_async(`ws://${host}:${port}`, secret);
  }

  /** Disconnect *peer_id* via whichever protocol holds it. Idempotent. */
  remove_peer(peer_id) {
    for (const proto of this._connection_values()) {
      if (proto.has_peer(peer_id)) {
        try {
          proto.disconnect(peer_id);
        } catch {
          /* best effort */
        }
        return;
      }
    }
    this._unregister_peer(peer_id);
  }

  /** Awaitable ``remove_peer``. */
  async remove_peer_async(peer_id) {
    for (const proto of this._connection_values()) {
      if (proto.has_peer(peer_id)) {
        try {
          await proto.disconnect_async(peer_id);
        } catch {
          /* best effort */
        }
        return;
      }
    }
    this._unregister_peer(peer_id);
  }

  /** Return the registered protocol currently holding *peer_id*, if any. */
  _holder_protocol(peer_id) {
    for (const proto of this._connection_values()) {
      if (proto.has_peer(peer_id)) return proto;
    }
    return null;
  }

  // ------------------------------------------------------------------
  // Liveness (async per-peer ping loop)
  // ------------------------------------------------------------------

  /** Start the background liveness loop (idempotent). */
  _start_liveness() {
    if (!this.liveness_enabled) return;
    if (this._liveness_thread !== null && this._liveness_thread !== undefined && this._liveness_thread.is_alive()) return;
    this._liveness_stop = new Event();
    this._liveness_thread = new Thread({
      target: () => this._liveness_loop(),
      daemon: true,
      name: `comm-liveness-${this.policy_id}`,
    });
    this._liveness_thread.start();
  }

  /** Stop the background liveness loop (idempotent). */
  _stop_liveness() {
    if (this._liveness_stop !== null && this._liveness_stop !== undefined) this._liveness_stop.set();
    if (this._liveness_thread !== null && this._liveness_thread !== undefined) {
      this._liveness_thread.join(2.0);
      this._liveness_thread = null;
    }
    this._liveness_stop = null;
  }

  async _stop_liveness_async() {
    if (this._liveness_stop !== null && this._liveness_stop !== undefined) this._liveness_stop.set();
    if (this._liveness_thread !== null && this._liveness_thread !== undefined) {
      await this._liveness_thread.join_async(2.0);
      this._liveness_thread = null;
    }
    this._liveness_stop = null;
  }

  /**
   * Ping each peer every ``liveness_interval`` and drop dead ones.
   *
   * Runs entirely off the data path on its own thread. Each ping is bounded
   * by the protocol's ``ping_timeout`` so a silently-dead peer cannot stall
   * the sweep. Only protocols that are ``persistent`` and ``supports_ping``
   * are probed.
   */
  async _liveness_loop() {
    const stop = this._liveness_stop;
    const pool = new ThreadPoolExecutor({ max_workers: 4, thread_name_prefix: "comm-ping" });
    try {
      while (!(await stop.wait_async(this.liveness_interval))) {
        // snapshot keys so concurrent register/unregister is safe
        for (const peer_id of [...this.peers.keys()]) {
          const proto = this._holder_protocol(peer_id);
          if (proto === null) continue;
          if (!((proto.persistent ?? true) && (proto.supports_ping ?? true))) continue;
          const deadline = proto.ping_timeout ?? 5.0;
          let alive;
          try {
            alive = await asyncio.wait_for(pool.submit(() => proto.ping_async(peer_id)), deadline);
          } catch {
            alive = false;
          }
          if (!alive) {
            try {
              await proto.disconnect_async(peer_id);
            } catch {
              /* best effort */
            }
            this._unregister_peer(peer_id);
          }
        }
      }
    } finally {
      pool.shutdown({ wait: false, cancel_futures: true });
    }
  }

  /**
   * Create a proxy for a newly connected peer.
   *
   * Called by protocol instances after a successful handshake. One
   * ``PeerProxy`` object is built and stored in *both* ``this.peers`` and
   * ``laila.remote_policies`` so ``laila.peers[gid] is laila._remote_policies[gid]``.
   */
  _register_peer(peer_id) {
    if (!this.peers.has(peer_id)) {
      const proxy = new PeerProxy(peer_id, this);
      this.peers.set(peer_id, proxy);
      const remote = _remote_policies();
      if (remote !== null) dict_set(remote, peer_id, proxy);
      // ensure liveness monitoring is running once we have a peer
      this._start_liveness();
    }
  }

  /**
   * Remove a peer proxy after the transport connection closes.
   *
   * Also removes from ``laila.remote_policies``.
   */
  _unregister_peer(peer_id) {
    this.peers.pop(peer_id, null);
    const remote = _remote_policies();
    if (remote !== null) {
      if (remote instanceof Map) remote.delete(peer_id);
      else if (typeof remote.pop === "function") remote.pop(peer_id, null);
      else delete remote[peer_id];
    }
  }

  // ------------------------------------------------------------------
  // Inbound admission control (backpressure)
  // ------------------------------------------------------------------

  /**
   * Lazily build the shared bounded semaphore guarding inbound RPCs.
   *
   * One semaphore per policy (shared by every transport) sized to
   * ``max_inflight_rpcs``, so the cap is a true *per-policy* budget rather
   * than per-connection.
   */
  _rpc_gate() {
    if (this._rpc_semaphore_lock === null || this._rpc_semaphore_lock === undefined) this._rpc_semaphore_lock = new Lock();
    if (this._rpc_semaphore === null || this._rpc_semaphore === undefined) {
      with_lock(this._rpc_semaphore_lock, () => {
        if (this._rpc_semaphore === null || this._rpc_semaphore === undefined) {
          this._rpc_semaphore = new BoundedSemaphore(Math.max(1, Math.trunc(this.max_inflight_rpcs)));
        }
      });
    }
    return this._rpc_semaphore;
  }

  /** Claim an inbound-RPC slot without blocking. ``true`` if admitted. */
  _acquire_rpc_slot() {
    return this._rpc_gate().acquire({ blocking: false });
  }

  /** Release a previously-claimed inbound-RPC slot. Idempotent-safe. */
  _release_rpc_slot() {
    try {
      this._rpc_gate().release();
    } catch (e) {
      // Released more than acquired -- ignore (defensive).
      if (!(e instanceof ValueError)) throw e;
    }
  }

  // ------------------------------------------------------------------
  // RPC dispatch (inbound)
  // ------------------------------------------------------------------

  /**
   * Execute a dotted-path method call on the local policy.
   *
   * @param {string[]} path Attribute chain relative to the local policy
   *   object, e.g. ``["central", "memory", "memorize"]``.
   * @param {any[]} args Positional arguments.
   * @param {object} kwargs Keyword arguments.
   * @returns {any} Return value of the invoked method.
   * @throws {AttributeError} If any segment of *path* does not exist on the target.
   */
  _execute_rpc(path, args, kwargs) {
    let obj = this._local_policy;
    if (obj === null || obj === undefined) throw new RuntimeError("Communication has no reference to the local policy.");
    let owner = null;
    for (const segment of path) {
      owner = obj;
      obj = getattr(obj, segment);
    }
    if (typeof obj !== "function") {
      if (obj !== null && obj !== undefined && typeof obj.__call__ === "function") return obj.__call__(...args, kwargs);
      throw new AttributeError(`'${owner?.constructor?.name ?? typeof owner}' object at ${repr(path)} is not callable`);
    }
    const has_kwargs = kwargs !== null && kwargs !== undefined && (kwargs instanceof Map ? kwargs.size > 0 : Object.keys(kwargs).length > 0);
    const kw = kwargs instanceof Map ? Object.fromEntries(kwargs) : kwargs;
    return has_kwargs ? obj.call(owner, ...args, kw) : obj.call(owner, ...args);
  }

  // ------------------------------------------------------------------
  // RPC dispatch (outbound)
  // ------------------------------------------------------------------

  /**
   * Pick the transport that carries the call to *peer_id*.
   *
   * @param {string} peer_id Target peer ``global_id``.
   * @param {string|null} [comm] A *communication id* -- either a registered
   *   connection's ``global_id`` or a protocol token (e.g. ``"tcp"``,
   *   ``"lora"``). When given, that specific channel is used and must already
   *   hold the peer; when ``null`` the first registered protocol holding
   *   *peer_id* is used.
   * @throws {ConnectionError} If the requested channel is unknown, or no
   *   channel holds the peer.
   */
  _select_protocol_for_peer(peer_id, comm = null) {
    if (comm !== null && comm !== undefined) {
      let proto = dict_get(this.connections, String(comm), null);
      if (proto === null) {
        try {
          proto = this._resolve_protocol_for_token(String(comm));
        } catch (e) {
          if (!(e instanceof ConnectionError)) throw e;
          proto = null;
        }
      }
      if (proto === null) throw new ConnectionError(`Unknown communication channel ${repr(comm)}: not a connection id or a registered protocol token.`);
      if (!proto.has_peer(peer_id)) throw new ConnectionError(`Communication channel ${repr(comm)} has no connection to peer ${peer_id}.`);
      return proto;
    }
    for (const proto of this._connection_values()) {
      if (proto.has_peer(peer_id)) return proto;
    }
    throw new ConnectionError(`No connection to peer ${peer_id}`);
  }

  /**
   * Send an RPC call to a peer over the selected transport.
   *
   * If the deserialized response contains a ``__laila_future__`` marker it
   * is automatically wrapped in a ``RemoteFuture`` and registered in the
   * local policy's ``future_bank``.
   *
   * @param {string} peer_id Target peer ``global_id``.
   * @param {string[]} path Dotted attribute chain on the remote policy.
   * @param {any[]} args Positional arguments.
   * @param {object} kwargs Keyword arguments.
   * @param {{comm?: string|null}} [opts] Communication id / protocol token
   *   selecting the transport. Threaded into the returned ``RemoteFuture``
   *   so its later ``status`` / ``wait`` / ``result`` calls stay on the same
   *   channel.
   * @returns {any} The deserialized return value from the remote call, or a
   *   ``RemoteFuture`` when the remote returned a future.
   * @throws {ConnectionError} If no protocol holds a connection to *peer_id*.
   * @throws {RuntimeError} If the remote side returned an error.
   */
  _send_rpc(peer_id, path, args, kwargs, opts = {}) {
    const comm = opts.comm ?? null;
    const proto = this._select_protocol_for_peer(peer_id, comm);
    const result = proto.send_rpc(peer_id, path, args, kwargs);
    return this._maybe_wrap_remote_future(result, peer_id, { comm });
  }

  /**
   * Awaitable ``_send_rpc``.
   *
   * Resolves with the RPC result. A ``RemoteFuture`` result is a thenable,
   * so the returned promise *adopts* it: awaiting a future-returning call
   * yields the remote future's result (Python's ``await proxy.fn(...)``
   * reading). Use ``_send_rpc_async_boxed`` to get the ``RemoteFuture`` itself.
   */
  _send_rpc_async(peer_id, path, args, kwargs, opts = {}) {
    return this._send_rpc_async_boxed(peer_id, path, args, kwargs, opts).then((box) => box.result);
  }

  /**
   * Awaitable ``_send_rpc`` resolving with ``{ result }`` -- the box keeps a
   * ``RemoteFuture`` result from being adopted by the promise machinery, so
   * the caller can hold the future (``status``, ``wait()``, ``await``).
   */
  _send_rpc_async_boxed(peer_id, path, args, kwargs, opts = {}) {
    const comm = opts.comm ?? null;
    const proto = this._select_protocol_for_peer(peer_id, comm);
    return proto.send_rpc_async(peer_id, path, args, kwargs).then((result) => ({ result: this._maybe_wrap_remote_future(result, peer_id, { comm }) }));
  }

  /**
   * Promote a future-shaped envelope into a real ``RemoteFuture``.
   *
   * Detected by the ``__laila_future__`` flag on the deserialized result.
   * The envelope carries enough identity information (uuid, evolution,
   * scopes, taskforce/policy ids) to reconstruct a stable identity-only
   * ``RemoteFuture`` on the local side without round-tripping the
   * underlying value. The new future is then bound to *this* so subsequent
   * ``status``/``wait``/``result`` calls route back to *peer_id*.
   *
   * ``RemoteFuture.model_post_init`` self-registers into the active local
   * policy's ``future_bank`` and guarantee stack, so this helper only has to
   * build the instance and call ``RemoteFuture.bind`` to attach the
   * communication channel.
   *
   * Group futures (``__is_group__``) are flagged so the bound proxy uses the
   * GroupFuture-shaped status payload.
   */
  _maybe_wrap_remote_future(result, peer_id, opts = {}) {
    const comm = opts.comm ?? null;
    if (!(result instanceof Map || is_plain_object(result)) || !dict_get(result, "__laila_future__", false)) return result;

    const { RemoteFuture } = lazy("laila.policy.central.command.schema.future.future.remote_future");

    const remote_gid = dict_get(result, "global_id");
    let parsed;
    try {
      parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(remote_gid);
    } catch (exc) {
      if (!(exc instanceof ValueError)) throw exc;
      const err = new ValueError(`Invalid remote future gid: ${repr(remote_gid)}`);
      err.__cause__ = exc;
      throw err;
    }
    const remote_uuid = parsed.uuid;
    const evolution = parsed.evolution;
    const is_group = !!dict_get(result, "__is_group__", false);

    const rf = new RemoteFuture({
      taskforce_id: dict_get(result, "taskforce_id", peer_id),
      policy_id: dict_get(result, "policy_id", peer_id),
      uuid: remote_uuid,
      scopes: [is_group ? _GROUP_FUTURE_SCOPE : _FUTURE_SCOPE],
      evolution,
    });
    rf.bind(this, { is_group, comm });
    return rf;
  }
}

register("laila.policy.central.communication.schema.base", {
  _LAILA_IDENTIFIABLE_COMMUNICATION,
  _coerce_peer_registry,
});
