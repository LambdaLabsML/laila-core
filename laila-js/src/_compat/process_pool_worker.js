/**
 * Worker side of ``ProcessPoolExecutor`` (see ``process_pool.js``).
 *
 * Protocol (IPC, advanced serialization):
 *   in : ``{ id, payload: Buffer, globals: [[module, name], ...] }``
 *   out: ``{ id, ok: true, payload: Buffer }`` |
 *        ``{ id, ok: false, error: { name, message, stack } }``
 *
 * ``payload`` is a pickle of ``(fn, args)``; every ``GLOBAL`` it references
 * is listed in ``globals`` so the referenced modules can be imported (ESM
 * imports are async) *before* the synchronous unpickle resolves them.
 */
import * as pickle from "../_codecs/pickle.js";

const _module_cache = new Map();

async function _import(module) {
  if (!_module_cache.has(module)) _module_cache.set(module, import(module));
  return _module_cache.get(module);
}

function _walk(ns, qualname) {
  let cur = ns;
  for (const part of qualname.split(".")) {
    if (cur === null || cur === undefined) return undefined;
    cur = cur[part];
  }
  return cur;
}

async function _resolve_globals(pairs) {
  const globals = new Map();
  for (const [module, name] of pairs) {
    const key = `${module}.${name}`;
    if (globals.has(key)) continue;
    let value;
    try {
      const ns = await _import(module);
      value = _walk(ns, name);
    } catch (err) {
      throw new Error(`Can't get attribute '${name}' on module '${module}': ${err && err.message}`);
    }
    if (value === undefined) throw new Error(`Can't get attribute '${name}' on <module '${module}'>`);
    globals.set(key, value);
  }
  return globals;
}

async function _handle(msg) {
  const { id, payload, globals: pairs } = msg;
  try {
    const globals = await _resolve_globals(pairs ?? []);
    const [fn, args] = pickle.loads(Buffer.from(payload), { globals });
    let out = fn(...args);
    if (out && typeof out.then === "function") out = await out;
    process.send({ id, ok: true, payload: pickle.dumps(out === undefined ? null : out) });
  } catch (err) {
    process.send({
      id,
      ok: false,
      error: {
        name: (err && err.constructor && err.constructor.name) || (err && err.name) || "Error",
        message: err && err.message !== undefined ? String(err.message) : String(err),
        stack: err && err.stack ? String(err.stack) : null,
      },
    });
  }
}

process.on("message", (msg) => {
  if (msg && typeof msg === "object" && "id" in msg) _handle(msg);
});
process.on("disconnect", () => process.exit(0));
