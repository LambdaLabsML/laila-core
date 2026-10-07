/**
 * Peer registry and channel-aware peer proxy.
 *
 * ``PeerRegistry`` is the ``dict`` subtype stored on ``communication.peers``.
 * It keeps exact dict semantics (``len``, iteration, ``in``, ``.get`` /
 * ``.pop`` / ``.clear``) for every existing caller and adds exactly one
 * resolver, ``PeerRegistry.channel``, which turns
 * ``(peer, name, transport-selector)`` into a ``Channel`` by asking the
 * carrier that holds the peer. It holds no live stream state and no
 * back-reference to the communication object.
 *
 * ``PeerProxy`` is the value type stored in the registry. It is a
 * ``RemotePolicyProxy`` that additionally supports item access::
 *
 *     laila.peers[gid]["default"]      // the ordinary RPC proxy itself
 *     laila.peers[gid]["video"]        // a Channel (opened lazily, cached)
 *     laila.peers[gid].via("uart")["video"]
 *
 * In JS the registry is a ``Map`` subclass exposed through the ``indexable``
 * proxy (``peers[gid]`` -> ``__getitem__``), and ``PeerProxy`` routes
 * ``proxy[name]`` to ``__getitem__`` through its own Proxy trap.
 */
import { ConnectionError, KeyError, TypeError as PyTypeError, ValueError } from "../../../_compat/errors.js";
import { register } from "../../../_compat/lazy.js";
import { indexable } from "../../../_compat/proxy.js";
import { repr } from "../../../_compat/pyrepr.js";
import { RemotePolicyProxy } from "./proxy.js";

/** Reserved channel name that resolves to the RPC proxy itself. */
export const DEFAULT_CHANNEL = "default";

const _MISSING = Symbol("PeerRegistry.MISSING");

/**
 * ``dict[str, PeerProxy]`` with a single channel resolver.
 *
 * Channel names never select a transport; that stays on ``PeerProxy.via`` /
 * ``laila.request(gid, comm_protocol=...)``.
 */
export class PeerRegistry {
  constructor(iterable = null) {
    /** @type {Map<string, any>} */
    Object.defineProperty(this, "_map", { value: new Map(), writable: true, configurable: true });
    if (iterable) {
      const entries = iterable instanceof Map || iterable instanceof PeerRegistry ? iterable.entries() : Array.isArray(iterable) ? iterable : Object.entries(iterable);
      for (const [k, v] of entries) this._map.set(k, v);
    }
    return indexable(this);
  }

  // ---- dict protocol -------------------------------------------------

  get size() {
    return this._map.size;
  }
  has(key) {
    return this._map.has(key);
  }
  set(key, value) {
    this._map.set(key, value);
    return this;
  }
  delete(key) {
    return this._map.delete(key);
  }
  clear() {
    this._map.clear();
  }
  keys() {
    return this._map.keys();
  }
  values() {
    return this._map.values();
  }
  entries() {
    return this._map.entries();
  }
  [Symbol.iterator]() {
    return this._map.keys();
  }
  forEach(fn, this_arg) {
    this._map.forEach(fn, this_arg);
  }
  __getitem__(key) {
    if (!this._map.has(key)) throw new KeyError(key);
    return this._map.get(key);
  }
  __setitem__(key, value) {
    this._map.set(key, value);
  }
  __delitem__(key) {
    if (!this._map.has(key)) throw new KeyError(key);
    this._map.delete(key);
  }
  __contains__(key) {
    return this._map.has(key);
  }
  __len__() {
    return this._map.size;
  }
  __iter__() {
    return this._map.keys();
  }
  __bool__() {
    return this._map.size > 0;
  }
  /** ``dict.get(key, default=None)`` */
  get(key, dflt = null) {
    return this._map.has(key) ? this._map.get(key) : dflt;
  }
  /** ``dict.pop(key[, default])`` */
  pop(key, dflt = _MISSING) {
    if (this._map.has(key)) {
      const v = this._map.get(key);
      this._map.delete(key);
      return v;
    }
    if (dflt === _MISSING) throw new KeyError(key);
    return dflt;
  }
  /** ``dict.setdefault`` */
  setdefault(key, dflt = null) {
    if (!this._map.has(key)) this._map.set(key, dflt);
    return this._map.get(key);
  }
  /** ``dict.update(mapping)`` */
  update(other) {
    const entries = other instanceof Map || other instanceof PeerRegistry ? other.entries() : Object.entries(other);
    for (const [k, v] of entries) this._map.set(k, v);
  }
  /** ``dict.items()`` */
  items() {
    return [...this._map.entries()];
  }
  /** ``dict.copy()`` -> plain ``PeerRegistry`` */
  copy() {
    return new PeerRegistry([...this._map.entries()]);
  }
  /** ``dict(registry)`` -> plain object */
  toDict() {
    return Object.fromEntries(this._map.entries());
  }
  toJSON() {
    return this.toDict();
  }
  __repr__() {
    return "{" + [...this._map.entries()].map(([k, v]) => `${repr(k)}: ${repr(v)}`).join(", ") + "}";
  }
  [Symbol.for("nodejs.util.inspect.custom")]() {
    return `PeerRegistry(${this.__repr__()})`;
  }

  // ---- the one resolver -------------------------------------------------

  /**
   * Resolve ``(peer_id, name)`` to a ``Channel`` on the right carrier.
   *
   * 1. ``proto = comm._select_protocol_for_peer(peer_id, selector)``.
   * 2. Require ``proto.constructor.supports_channels``.
   * 3. Return ``proto.open_channel(peer_id, name)`` (cached inside the
   *    carrier; the first call may perform one blocking RPC).
   *
   * @throws {ConnectionError} If no (matching) transport holds the peer, the
   *   transport has no stream lanes, or the peer cannot open the lane.
   * @throws {ValueError} If *name* is the reserved ``"default"``.
   */
  channel(comm, peer_id, name, selector = null) {
    if (name === DEFAULT_CHANNEL) throw new ValueError(`${repr(DEFAULT_CHANNEL)} is the RPC proxy, not a stream channel.`);
    const proto = comm._select_protocol_for_peer(peer_id, selector);
    if (!proto.constructor.supports_channels) {
      throw new ConnectionError(
        `${repr(proto.constructor.protocol_name)} has no stream lanes; use a stream-capable ` +
          "transport (e.g. tcp://, serial, loopback) for laila.peers[gid][name].",
      );
    }
    return proto.open_channel(peer_id, name);
  }

  /** Awaitable form of ``channel`` (no blocking RPC on the caller). */
  async channel_async(comm, peer_id, name, selector = null) {
    if (name === DEFAULT_CHANNEL) throw new ValueError(`${repr(DEFAULT_CHANNEL)} is the RPC proxy, not a stream channel.`);
    const proto = comm._select_protocol_for_peer(peer_id, selector);
    if (!proto.constructor.supports_channels) {
      throw new ConnectionError(
        `${repr(proto.constructor.protocol_name)} has no stream lanes; use a stream-capable ` +
          "transport (e.g. tcp://, serial, loopback) for laila.peers[gid][name].",
      );
    }
    return proto.open_channel_async(peer_id, name);
  }
}


/**
 * A ``RemotePolicyProxy`` with ``[name]`` channel access.
 *
 * Adds only methods -- no new instance attributes -- so identity checks
 * (``x instanceof RemotePolicyProxy``), ``laila.activate_policy`` (morph
 * mode) and ``laila.request`` keep working unchanged.
 *
 * ``proxy[name]`` is *not* a cheap dict lookup: the first access to a
 * negotiated lane performs one blocking RPC (bounded by the carrier's
 * ``rpc_timeout``) and may raise ``ConnectionError``. Later accesses hit the
 * carrier's cache. Because JS cannot tell ``proxy.video`` from
 * ``proxy["video"]``, only names that are *not* valid remote attribute
 * chains are routed to channels: a channel name must be requested through
 * ``proxy["<name>"]`` where ``<name>`` is passed via ``__getitem__``; plain
 * attribute reads keep returning RPC chains. Use ``proxy.channel(name)`` for
 * the unambiguous form.
 */
export class PeerProxy extends RemotePolicyProxy {
  /** ``"default"`` -> this proxy; any other name -> a ``Channel``. */
  __getitem__(name) {
    if (typeof name !== "string") {
      throw new PyTypeError(`Channel names are strings, got ${name === null ? "NoneType" : typeof name}; use 'default' for the RPC proxy.`);
    }
    if (name === DEFAULT_CHANNEL) return this;
    return this._comm.peers.channel(this._comm, this._peer_id, name, this._comm_selector);
  }

  /** Explicit, unambiguous channel accessor (``proxy["video"]`` in Python). */
  channel(name) {
    return this.__getitem__(name);
  }

  /** Awaitable channel accessor: never blocks the caller's loop. */
  async channel_async(name) {
    if (typeof name !== "string") {
      throw new PyTypeError(`Channel names are strings, got ${name === null ? "NoneType" : typeof name}; use 'default' for the RPC proxy.`);
    }
    if (name === DEFAULT_CHANNEL) return this;
    return this._comm.peers.channel_async(this._comm, this._peer_id, name, this._comm_selector);
  }

  /**
   * Names of channels currently open to this peer (union over carriers).
   *
   * Never opens anything. With a ``via()`` selector only that transport is
   * consulted.
   * @returns {string[]}
   */
  channels() {
    const comm = this._comm;
    const names = [];
    let protos;
    if (this._comm_selector !== null && this._comm_selector !== undefined) {
      protos = [comm._select_protocol_for_peer(this._peer_id, this._comm_selector)];
    } else {
      protos = comm._connection_values().filter((p) => p.has_peer(this._peer_id));
    }
    for (const proto of protos) {
      const fn = proto.channel_names;
      if (typeof fn !== "function") continue;
      for (const n of fn.call(proto, this._peer_id)) if (!names.includes(n)) names.push(n);
    }
    return names;
  }

  /** Return a ``PeerProxy`` bound to transport *comm* (see base class). */
  via(comm) {
    return new PeerProxy(this._peer_id, this._comm, comm);
  }

  __repr__() {
    if (this._comm_selector !== null && this._comm_selector !== undefined) {
      return `PeerProxy(${repr(this._peer_id)}, via=${repr(this._comm_selector)})`;
    }
    return `PeerProxy(${repr(this._peer_id)})`;
  }
}
register("laila.policy.central.communication.registry", { DEFAULT_CHANNEL, PeerRegistry, PeerProxy });
