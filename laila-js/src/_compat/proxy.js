/**
 * Item-access proxy: makes ``obj[key]`` / ``obj[key] = v`` / ``key in obj`` /
 * ``delete obj[key]`` route to the Python protocol methods ``__getitem__`` /
 * ``__setitem__`` / ``__contains__`` / ``__delitem__`` for every name that is
 * *not* a real attribute of the object (fields, methods, accessors).
 *
 * Real attributes always win -- ``d.data``, ``d.atomic()``, ``d.keys()`` keep
 * their meaning -- so the item namespace is "everything else", exactly as a
 * Python caller experiences a ``MutableMapping`` subclass: attribute syntax for
 * attributes, subscript syntax for items.
 *
 * JS cannot tell ``d.x`` from ``d["x"]``, so a *missing* key read through the
 * proxy yields ``undefined`` (the Python ``getattr(d, "x", None)`` outcome)
 * rather than raising; call ``d.__getitem__(k)`` (or ``getitem(d, k)``) for the
 * raising ``KeyError`` / ``IndexError`` form. JS protocol names (``then``,
 * ``toJSON``, ...) and dunder names are never treated as keys so the object
 * stays safe to ``await``, log and introspect.
 */
import { KeyError, IndexError } from "./errors.js";

const _NO_INDEX = new Set([
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

/**
 * Names that are never item keys through the proxy: JS protocol names, dunder
 * names, and *underscore-prefixed* names. The last rule keeps lazily-created
 * private attributes (``this._local_lock``, ``this._cache``...) -- which in
 * Python are plain attribute accesses -- from being routed to ``__getitem__``
 * before they exist. Keys that start with ``_`` are still reachable through
 * ``obj.__getitem__("_k")`` / ``getitem(obj, "_k")``.
 */
function _is_protocol(prop) {
  return _NO_INDEX.has(prop) || prop.startsWith("_");
}

/**
 * @param {object} target
 * @param {{index_key?: (prop: string) => any}} [opts]
 *   ``index_key`` converts the property string to the key passed to the
 *   dunder methods (e.g. numeric strings -> integers for sequences).
 */
export function indexable(target, opts = {}) {
  const key_of = opts.index_key ?? ((p) => p);
  return new Proxy(target, {
    get(t, prop, receiver) {
      if (typeof prop === "symbol" || prop in t) return Reflect.get(t, prop, receiver);
      if (_is_protocol(prop)) return undefined;
      try {
        return t.__getitem__.call(receiver, key_of(prop));
      } catch (e) {
        if (e instanceof KeyError || e instanceof IndexError) return undefined;
        throw e;
      }
    },
    set(t, prop, value, receiver) {
      if (typeof prop === "symbol" || prop in t || _is_protocol(prop)) {
        // Accessor setters must see ``this === receiver`` (the proxy), so an
        // identity stored by a setter (``pool._proxy_to = this``) compares
        // equal to the object the caller holds.
        return Reflect.set(t, prop, value, receiver);
      }
      t.__setitem__.call(receiver, key_of(prop), value);
      return true;
    },
    has(t, prop) {
      if (typeof prop === "symbol" || prop in t) return true;
      if (_is_protocol(prop)) return false;
      return t.__contains__(key_of(prop));
    },
    deleteProperty(t, prop) {
      if (typeof prop === "symbol" || Object.prototype.hasOwnProperty.call(t, prop)) return Reflect.deleteProperty(t, prop);
      if (_is_protocol(prop)) return true;
      t.__delitem__(key_of(prop));
      return true;
    },
  });
}

/** ``index_key`` for sequences: ``"3"`` -> ``3``, ``"-1"`` -> ``-1``. */
export function sequence_key(prop) {
  return /^-?\d+$/.test(prop) ? Number(prop) : prop;
}
