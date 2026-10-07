/**
 * Transparent remote-policy proxy for inter-policy RPC.
 *
 * A ``RemotePolicyProxy`` is the local stand-in for a *remote* policy. From
 * the caller's point of view it looks (and quacks) like a local
 * ``_LAILA_IDENTIFIABLE_POLICY`` -- you can pass it to
 * ``laila.activate_policy``, query its ``global_id``, and reach its
 * ``central.memory.memorize`` / ``central.command.submit`` methods exactly as
 * though it were local. The difference is that every terminal call is sent
 * over the wire instead of executed in-process.
 *
 * The trick that keeps the call site clean is *attribute-chain accumulation*
 * via ``_RemoteAttrChain``. Each attribute access on a proxy returns a chain
 * object that records the dotted path so far without doing any I/O. Only
 * when the chain is finally invoked does a single RPC frame get serialised
 * and dispatched through the communication layer. That means
 *
 *     proxy.central.memory.remember("x")
 *
 * results in exactly one network round-trip, with the remote side receiving
 * ``["central", "memory", "remember"]`` plus ``args=("x",), kwargs={}``.
 *
 * Keyword arguments
 * -----------------
 * Python's ``chain(*args, **kwargs)`` maps to the port-wide convention: a
 * *trailing plain object* is the keyword-argument dict, everything before it
 * is positional. ``chain.call_with(args, kwargs)`` is the explicit form for
 * the rare case where the last positional argument is itself a plain dict.
 */
import { register } from "../../../_compat/lazy.js";
import { repr } from "../../../_compat/pyrepr.js";
import { PyTuple, is_plain_object } from "../../../_compat/pytypes.js";

/**
 * Names that are never captured as remote attributes: JS protocol names
 * (so a proxy can be awaited, logged, compared and serialised safely) and
 * the dunder namespace.
 */
const _NO_CHAIN = new Set([
  "then",
  "catch",
  "finally",
  "toJSON",
  "constructor",
  "prototype",
  "inspect",
  "nodeType",
  "asymmetricMatch",
  "$$typeof",
  "__proto__",
  "toString",
  "valueOf",
  "length",
  "size",
  "toDict",
  "asyncDispose",
  "dispose",
]);

function _is_protocol(prop) {
  return typeof prop !== "string" || _NO_CHAIN.has(prop) || (prop.startsWith("__") && prop.endsWith("__"));
}

/**
 * Split a JS argument list into Python ``(args, kwargs)``: a trailing plain
 * object is the keyword dict.
 * @param {any[]} call_args
 * @returns {[PyTuple, object]}
 */
export function _split_call_args(call_args) {
  let args = call_args;
  let kwargs = {};
  if (call_args.length > 0) {
    const last = call_args[call_args.length - 1];
    if (is_plain_object(last)) {
      kwargs = last;
      args = call_args.slice(0, -1);
    }
  }
  return [PyTuple.from_iterable(args), kwargs];
}

/**
 * Wrap *target* so that every unknown attribute read becomes a remote
 * attribute chain rooted at that name.
 */
function _chain_proxy(target) {
  return new Proxy(target, {
    get(t, prop, receiver) {
      if (typeof prop === "symbol" || prop in t) return Reflect.get(t, prop, receiver);
      if (_is_protocol(prop)) return undefined;
      return t.__getattr__(prop);
    },
    has(t, prop) {
      return prop in t;
    },
  });
}

/**
 * Client-side proxy representing a peered remote policy.
 *
 * Exposes ``global_id`` so that ``laila.activate_policy`` and other identity
 * checks work transparently. All *other* attribute access is captured as the
 * start of a chain (``_RemoteAttrChain``) and only triggers a network
 * round-trip when the chain is finally invoked.
 */
export class RemotePolicyProxy {
  /**
   * @param {string} peer_id The remote policy's ``global_id``.
   * @param {any} communication The local central-communication instance that
   *   owns the underlying transport to *peer_id*.
   * @param {any} [comm_selector] Transport selector (see ``via``).
   */
  constructor(peer_id, communication, comm_selector = null) {
    // ``new RemotePolicyProxy(peer, comm, { comm_selector })`` keyword form.
    if (is_plain_object(comm_selector) && "comm_selector" in comm_selector) comm_selector = comm_selector.comm_selector;
    Object.defineProperty(this, "_peer_id", { value: peer_id, writable: true, configurable: true });
    Object.defineProperty(this, "_comm", { value: communication, writable: true, configurable: true });
    Object.defineProperty(this, "_comm_selector", { value: comm_selector, writable: true, configurable: true });
    return _chain_proxy(this);
  }

  /** The remote policy's ``global_id`` (resolved without I/O). */
  get global_id() {
    return this._peer_id;
  }

  /**
   * Return a proxy bound to a specific transport *comm*.
   *
   * *comm* is a *communication id* -- a registered connection's
   * ``global_id`` or a protocol token (``"tcp"``, ``"lora"``, ...). Every RPC
   * issued through the returned proxy (and every follow-up call on the
   * futures it returns) travels over that channel. The original proxy is
   * unchanged.
   *
   * Example: ``laila.peers[gid].via("lora").central.memory.remember(eid)``
   */
  via(comm) {
    return new RemotePolicyProxy(this._peer_id, this._comm, comm);
  }

  /**
   * Begin a remote attribute chain rooted at *name*.
   *
   * Called for any attribute that is not a real attribute of the proxy.
   * Returns a ``_RemoteAttrChain``; further attribute accesses extend the
   * path, invocation materialises the RPC.
   */
  __getattr__(name) {
    return new _RemoteAttrChain(this._comm, this._peer_id, [name], this._comm_selector);
  }

  __repr__() {
    if (this._comm_selector !== null && this._comm_selector !== undefined) {
      return `RemotePolicyProxy(${repr(this._peer_id)}, via=${repr(this._comm_selector)})`;
    }
    return `RemotePolicyProxy(${repr(this._peer_id)})`;
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}

/**
 * Accumulator for a dotted attribute path; flushes as an RPC when called.
 *
 * Each attribute access appends to the path *immutably* (a new chain object
 * is returned, the existing one is unchanged), so the chain is safe to
 * share. Calling the chain sends one RPC frame containing the full path
 * plus the call's positional and keyword arguments.
 *
 * Instances are callable: ``chain(...args)`` is Python's ``chain(*args,
 * **kwargs)`` with the trailing-options convention; ``chain.call_with(args,
 * kwargs)`` is the explicit form.
 */
export class _RemoteAttrChain {
  /**
   * @param {any} communication Local communication instance used to dispatch the RPC.
   * @param {string} peer_id Target peer ``global_id``.
   * @param {string[]} path Attribute segments accumulated so far.
   * @param {any} [comm_selector]
   */
  constructor(communication, peer_id, path, comm_selector = null) {
    // The callable surface: a function whose prototype chain is this class,
    // so ``chain instanceof _RemoteAttrChain`` and ``chain(...)`` both work.
    const callable = function _remote_attr_chain() {};
    Object.setPrototypeOf(callable, new.target.prototype);
    Object.defineProperty(callable, "_comm", { value: communication, writable: true, configurable: true });
    Object.defineProperty(callable, "_peer_id", { value: peer_id, writable: true, configurable: true });
    Object.defineProperty(callable, "_path", { value: path, writable: true, configurable: true });
    Object.defineProperty(callable, "_comm_selector", { value: comm_selector, writable: true, configurable: true });
    Object.defineProperty(callable, "name", { value: path[path.length - 1] ?? "", configurable: true });
    return new Proxy(callable, {
      get(t, prop, receiver) {
        if (typeof prop === "symbol" || prop in t) return Reflect.get(t, prop, receiver);
        if (_is_protocol(prop)) return undefined;
        return t.__getattr__(prop);
      },
      apply(t, _this, call_args) {
        return t.__call__(...call_args);
      },
    });
  }

  /** Return a *new* chain whose path is the current path plus *name*. */
  __getattr__(name) {
    return new _RemoteAttrChain(this._comm, this._peer_id, [...this._path, name], this._comm_selector);
  }

  /**
   * Dispatch the accumulated path + arguments as a single RPC.
   *
   * Blocks the calling thread until the remote responds. If the remote
   * returns a future-shaped envelope, the communication layer transparently
   * wraps it in a ``RemoteFuture`` before returning. The bound transport
   * selector (if any) is passed through so the call -- and the future it
   * yields -- stay on the chosen channel.
   */
  __call__(...call_args) {
    const [args, kwargs] = _split_call_args(call_args);
    return this.call_with(args, kwargs);
  }

  /** Explicit ``(args, kwargs)`` form of ``__call__``. */
  call_with(args, kwargs = {}) {
    return this._comm._send_rpc(this._peer_id, this._path, PyTuple.from_iterable(args), kwargs, { comm: this._comm_selector });
  }

  /**
   * Awaitable form: resolves with the RPC result without blocking the loop.
   * A ``RemoteFuture`` result is adopted (awaited through), so this yields
   * the remote future's *result*; see ``call_async_boxed`` to get the future.
   */
  call_async(...call_args) {
    const [args, kwargs] = _split_call_args(call_args);
    return this._comm._send_rpc_async(this._peer_id, this._path, PyTuple.from_iterable(args), kwargs, { comm: this._comm_selector });
  }

  /**
   * Awaitable form resolving with ``{ result }`` -- the non-thenable box
   * hands back a ``RemoteFuture`` *as an object* (the JS stand-in for
   * Python's synchronous ``rf = proxy.fn(...)`` inside a coroutine).
   */
  call_async_boxed(...call_args) {
    const [args, kwargs] = _split_call_args(call_args);
    return this._comm._send_rpc_async_boxed(this._peer_id, this._path, PyTuple.from_iterable(args), kwargs, { comm: this._comm_selector });
  }

  __repr__() {
    const dotted = this._path.join(".");
    return `_RemoteAttrChain(${repr(this._peer_id)}, ${repr(dotted)})`;
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}

register("laila.policy.central.communication.proxy", { RemotePolicyProxy, _RemoteAttrChain, _split_call_args });
