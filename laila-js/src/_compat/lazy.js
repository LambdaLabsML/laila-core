/**
 * Lazy module registry -- the ESM replacement for Python's function-level
 * imports (``from ... import X`` inside a function body, used to break cycles).
 *
 * ESM resolves import cycles with live bindings only when nothing is *used*
 * during module evaluation; ``class X extends Y`` and top-level calls need
 * the target already evaluated. laila's module graph has such edges, so every
 * in-function import becomes ``lazy("laila.some.module").Name`` -- resolved
 * at call time from this registry, which ``src/_modules.js`` fills with every
 * module namespace once the package is loaded.
 */
import { ImportError } from "./errors.js";

const _registry = new Map();

/**
 * Register a module namespace under its Python dotted name.
 * @param {string} name e.g. ``"laila.entry.entry"``
 * @param {object} ns module namespace (``import * as ns``) or a plain object
 */
export function register(name, ns) {
  _registry.set(name, ns);
  return ns;
}

/** Resolve a registered module; raises ``ImportError`` if it is not loaded yet. */
export function resolve(name) {
  const ns = _registry.get(name);
  if (ns === undefined)
    throw new ImportError(
      `module '${name}' is not loaded; import "laila-core" (src/index.js) before using this module`,
    );
  return ns;
}

/** True when ``name`` is registered. */
export function is_loaded(name) {
  return _registry.has(name);
}

/** Remove a registration (tests swapping stub modules in and out). */
export function unregister(name) {
  return _registry.delete(name);
}

const _proxies = new Map();

/**
 * A proxy that resolves attributes from the module on every access, so
 * ``const { Entry } = lazy("laila.entry.entry")`` inside a function behaves
 * like Python's in-function import.
 */
export function lazy(name) {
  let p = _proxies.get(name);
  if (p) return p;
  p = new Proxy(Object.create(null), {
    get(_t, prop) {
      if (prop === Symbol.toStringTag) return `lazy(${name})`;
      const ns = resolve(name);
      const v = ns[prop];
      if (v === undefined && !(prop in ns))
        throw new ImportError(`cannot import name '${String(prop)}' from '${name}'`);
      return v;
    },
    has(_t, prop) {
      return prop in resolve(name);
    },
    ownKeys() {
      return Reflect.ownKeys(resolve(name));
    },
    getOwnPropertyDescriptor(_t, prop) {
      const ns = resolve(name);
      if (!(prop in ns)) return undefined;
      return { value: ns[prop], writable: false, enumerable: true, configurable: true };
    },
  });
  _proxies.set(name, p);
  return p;
}

/** Names currently registered (debugging / conformance checks). */
export function registered_modules() {
  return [..._registry.keys()];
}
