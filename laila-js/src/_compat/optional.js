/**
 * Optional dependencies -- the ``try: import x / except ImportError: x = None``
 * idiom.
 *
 * Every storage / transport backend depends on a client library the user
 * may not have installed. Python guards the import and leaves a ``None``
 * sentinel the class checks at construction time (raising ``ImportError``
 * with an actionable message). ``optional_import`` does the same for the
 * npm peer dependencies declared in ``package.json``: it resolves the
 * package from the consumer's ``node_modules`` and returns ``null`` when it
 * is not installed, without masking genuine load errors inside a package
 * that *is* installed.
 *
 * Resolution is synchronous (``require``), which Node >= 22.12 supports
 * for ESM packages as well, so the backends keep the sync construction path
 * their Python counterparts have.
 */
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

const _require = createRequire(import.meta.url);
const _cache = new Map();

/** Locate an installed package's directory (``require.resolve.paths`` walk). */
export function find_package_dir(name) {
  const dirs = _require.resolve.paths(name) ?? [];
  for (const dir of dirs) {
    const candidate = path.join(dir, name);
    if (fs.existsSync(path.join(candidate, "package.json"))) return candidate;
  }
  return null;
}

/**
 * @param {string} name package specifier, e.g. ``"redis"`` or ``"@aws-sdk/client-s3"``
 * @param {{file?: string}} [opts] ``file``: load this path inside the package
 *   instead of its main entry (for ESM-only packages whose ``exports`` map
 *   has no ``require`` condition -- Node >= 22.12 can ``require`` an ESM
 *   file by absolute path).
 * @returns {any|null} the module namespace (``module.exports``) or ``null``
 */
export function optional_import(name, opts = {}) {
  const key = opts.file ? `${name}::${opts.file}` : name;
  if (_cache.has(key)) return _cache.get(key);
  let mod = null;
  try {
    if (opts.file) {
      const dir = find_package_dir(name);
      if (dir === null) {
        const err = new Error(`Cannot find module '${name}'`);
        err.code = "MODULE_NOT_FOUND";
        throw err;
      }
      mod = _require(path.join(dir, opts.file));
    } else {
      mod = _require(name);
    }
  } catch (e) {
    const missing_self =
      e &&
      (e.code === "MODULE_NOT_FOUND" || e.code === "ERR_MODULE_NOT_FOUND") &&
      // only swallow "<name> itself is missing", not a broken transitive import
      (typeof e.message !== "string" || e.message.includes(`'${name}'`) || e.message.includes(`'${name}/`) || e.message.includes(`${name}`));
    if (!missing_self) throw e;
    mod = null;
  }
  _cache.set(key, mod);
  return mod;
}

/** Drop the memoized result (tests that install a package mid-process). */
export function forget_optional(name) {
  _cache.delete(name);
}
