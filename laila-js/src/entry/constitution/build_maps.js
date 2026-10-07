/**
 * Scope-based build dispatch for serialized entries.
 *
 * Different ``Entry`` subclasses (the base ``Entry``, the ``Manifest``, future
 * user-defined subclasses, ...) need different deserialization logic.
 * Hard-coding the dispatch in ``Entry.from_dict`` would couple the base class
 * to every subclass; instead each subclass registers a *builder pair*
 * ``[sync_fn, async_fn]`` keyed by its scope string in ``BUILDER_MAP``, and
 * ``build_by_scope`` reads the ``_scopes`` field of a serialized dict to look
 * up the right builder.
 *
 * This is also what makes the deserialization side of pool reads extensible
 * -- third-party Entry subclasses can be deserialized cleanly as long as they
 * register their own builders at import time.
 */
import { RuntimeError, ValueError } from "../../_compat/errors.js";
import { dict_get, isdict } from "../../_compat/pytypes.js";
import * as json from "../../_compat/pyjson.js";
import { lazy, register } from "../../_compat/lazy.js";

/**
 * Each value is ``[sync_builder, async_builder]``.
 * @type {Object<string, [Function, Function]>}
 */
export const BUILDER_MAP = {};

/**
 * Register a (sync, async) builder pair for the given scope.
 *
 * Both builders take the serialized dict and return a hydrated ``Entry``
 * (sync) or a promise that resolves to one (async). Subclasses should call
 * this exactly once at module import time. Re-registering the same scope
 * silently overwrites the previous pair, which is the right behavior for
 * hot-reload during development but means production code should not rely
 * on it for "namespace" tricks.
 *
 * @param {string} scope The first element of the subclass's ``_scopes`` list
 *   (e.g. ``"ENTRY"``, ``"MANIFEST"``).
 * @param {Function} sync_fn The synchronous builder.
 * @param {Function} async_fn The async builder.
 */
export function register_builder(scope, sync_fn, async_fn) {
  BUILDER_MAP[scope] = [sync_fn, async_fn];
}

/**
 * Dispatch hydration based on the ``_scopes`` field of *in_dict*.
 *
 * Accepted input shapes
 * ---------------------
 * - ``dict`` -- the standard serialized form. The first scope is looked up
 *   in ``BUILDER_MAP``; defaults to ``"ENTRY"`` when the dict has no
 *   ``_scopes`` key.
 * - ``str`` -- treated as a JSON-encoded dict and parsed before dispatch
 *   (convenient when reading directly from a JSON-blob pool).
 * - live ``Entry`` -- short-circuited and returned as-is in the sync path;
 *   the async path wraps it in an already-resolved promise so the caller can
 *   ``await`` it uniformly.
 *
 * @param {object|string|import("../entry.js").Entry} in_dict Serialized representation.
 * @param {{asynchronous?: boolean, [k: string]: any}} [opts] ``asynchronous``
 *   (default ``false``): if ``true``, route to the async builder and return a
 *   promise the caller must ``await``. Otherwise call the sync builder inline
 *   and return the hydrated entry directly. Any other keys are forwarded to
 *   the resolved builder as its trailing options object.
 * @returns {any} The hydrated entry, or a promise resolving to it.
 * @throws {ValueError} If the dispatched scope has no registered builder.
 * @throws {RuntimeError} If *in_dict* is not a dict, str, or live Entry.
 * @throws {JSONDecodeError} If *in_dict* is a string that fails to parse as JSON.
 */
export function build_by_scope(in_dict, opts = {}) {
  const { asynchronous = false, ...kwargs } = opts;

  if (!isdict(in_dict)) {
    if (typeof in_dict === "string") in_dict = json.loads(in_dict);
    else {
      const { Entry } = lazy("laila.entry.entry");

      if (in_dict instanceof Entry) {
        if (asynchronous) {
          const _identity = async (entry = in_dict) => entry;

          return _identity();
        }
        return in_dict;
      }
      throw new RuntimeError("Invalid input for entry build.");
    }
  }

  const scopes = dict_get(in_dict, "_scopes", ["ENTRY"]);
  const scope = scopes && scopes.length ? scopes[0] : "ENTRY";

  const builder_pair = BUILDER_MAP[scope] ?? null;
  if (builder_pair === null) {
    throw new ValueError(`No builder registered for scope '${scope}'. ` + `Registered scopes: [${Object.keys(BUILDER_MAP).map((k) => `'${k}'`).join(", ")}]`);
  }

  const [sync_fn, async_fn] = builder_pair;
  const fn = asynchronous ? async_fn : sync_fn;
  return Object.keys(kwargs).length ? fn(in_dict, kwargs) : fn(in_dict);
}

register("laila.entry.constitution.build_maps", { BUILDER_MAP, register_builder, build_by_scope });
