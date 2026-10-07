/**
 * Runtime argument loading from multiple file formats and the terminal.
 *
 * Defines ``ArgReader``, the loader behind ``laila.args``. The class accepts
 * arguments from any of the following formats and merges them into a single
 * attribute-accessible mapping (defaulting to ``laila.args``, which is an
 * ``AtomicDotMap``):
 *
 * - ``.json`` -- JSON object at the top level (``from_json``).
 * - ``.toml`` -- TOML table at the top level (``from_toml``).
 * - ``.env`` -- ``KEY=VALUE`` lines, ``#`` comments allowed (``from_env``).
 * - ``.xml`` -- one level of nesting allowed; siblings become nested dicts
 *   (``from_xml``).
 * - ``terminal`` -- ``key=value`` tokens parsed from ``process.argv``
 *   (``from_terminal``).
 * - ``load(source)`` -- auto-detects format from the file suffix.
 *
 * Values are coerced from strings to the most specific Python scalar they fit
 * (bool, None, int, float, JSON-shaped collection, quoted string, fall-back
 * raw string) by ``_coerce_scalar``. Nested dicts in JSON / TOML files are
 * flattened *one level* with underscore-joined keys -- deeper nesting should
 * be expressed via explicit dotted keys instead.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

import { ImportError, ParseError, ValueError, os_error } from "../../_compat/errors.js";
import { lazy } from "../../_compat/lazy.js";
import { loads as json_loads } from "../../_compat/pyjson.js";
import { PyFloat, float, int, is_integral, is_plain_object, isdict, dict_items, dict_keys } from "../../_compat/pytypes.js";

const _require = createRequire(import.meta.url);

/** ``open(path, encoding="utf-8").read()`` with CPython's ``OSError`` family. */
function _read_text(path_) {
  try {
    return readFileSync(String(path_), "utf8");
  } catch (e) {
    throw os_error(e, String(path_));
  }
}

/**
 * Loader of runtime args into an attribute-accessible target mapping.
 *
 * All ``load`` / ``from_*`` methods mutate the target in place and return
 * ``null``. The target defaults to ``laila.args`` (an ``AtomicDotMap``) when
 * not specified, so the typical usage pattern is just::
 *
 *   const reader = new ArgReader();
 *   reader.load("config.json");
 *   reader.load("terminal");
 *
 * A custom target lets test suites or library callers run with a private
 * DotMap-like object instead.
 *
 * Notes
 * -----
 * The reader is intentionally tolerant: malformed lines in ``.env``-style
 * files are skipped, JSON-coerced collections that fail to parse fall back to
 * the original string, etc. The aim is to never lose arguments to a strict
 * parse error -- user-friendly "best-effort" semantics.
 */
export class ArgReader {
  // Supported sources: .env, .json, .toml, .xml, or ``terminal`` (``key=value`` tokens).

  /**
   * Initialise the reader.
   * @param {any} [target] Object to set attributes on. Defaults to ``laila.args``.
   */
  constructor(target = null) {
    this._target = target;
  }

  /** Return the target mapping, falling back to ``laila.args``. */
  _target_map() {
    if (this._target !== null && this._target !== undefined) return this._target;
    const laila = lazy("laila"); // lazy import to avoid circular import at module load
    return laila.args;
  }

  /** Coerce a string value to its most specific Python scalar type. */
  static _coerce_scalar(value) {
    if (typeof value !== "string") return value;

    const stripped = value.trim();
    const lowered = stripped.toLowerCase();
    if (lowered === "true" || lowered === "false") return lowered === "true";
    if (lowered === "none" || lowered === "null") return null;

    try {
      return int(stripped);
    } catch (e) {
      if (!(e instanceof ValueError)) throw e;
    }

    try {
      const f = float(stripped);
      return is_integral(f) ? new PyFloat(f) : f;
    } catch (e) {
      if (!(e instanceof ValueError)) throw e;
    }

    if ((stripped.startsWith("{") && stripped.endsWith("}")) || (stripped.startsWith("[") && stripped.endsWith("]"))) {
      try {
        return json_loads(stripped);
      } catch {
        return value;
      }
    }

    if (
      ((stripped.startsWith('"') && stripped.endsWith('"')) || (stripped.startsWith("'") && stripped.endsWith("'"))) &&
      stripped.length >= 2
    ) {
      return stripped.slice(1, -1);
    }

    return value;
  }

  /** Flatten nested dicts one level, joining keys with ``_``. */
  static _flatten_one_level(payload) {
    const out = {};
    for (const [key, value] of dict_items(payload)) {
      if (isdict(value)) {
        for (const [sub_key, sub_value] of dict_items(value)) out[`${key}_${sub_key}`] = this._coerce_scalar(sub_value);
      } else {
        out[key] = this._coerce_scalar(value);
      }
    }
    return out;
  }

  /** Flatten and set each key/value pair on the target. */
  _apply(payload) {
    const flat = this.constructor._flatten_one_level(payload);
    const target = this._target_map();
    for (const [key, value] of Object.entries(flat)) target[key] = value;
  }

  /** Remove all keys from the target mapping. */
  clear() {
    const target = this._target_map();
    // ``hasattr(target, "keys")`` -- every mapping (``DotMap`` / ``dict``)
    // has it; the ``DotMap`` proxy's ``has`` trap only answers for *keys*.
    const keys = typeof target.keys === "function" ? [...target.keys()] : isdict(target) ? dict_keys(target) : null;
    if (keys !== null) {
      for (const key of keys) {
        try {
          delete target[key];
        } catch {
          /* pass */
        }
      }
    }
  }

  /**
   * Load arguments from a JSON file.
   * @param {string} path Path to the ``.json`` file.
   */
  from_json(path_) {
    const data = json_loads(_read_text(path_));
    if (!isdict(data)) throw new ValueError("JSON args file must be a key/value object.");
    this._apply(data);
  }

  /**
   * Load arguments from a TOML file.
   * @param {string} path Path to the ``.toml`` file.
   */
  from_toml(path_) {
    let toml;
    try {
      toml = _require("smol-toml");
    } catch (e) {
      throw new ImportError("tomllib is required for TOML parsing.", { cause: e });
    }

    const data = toml.parse(_read_text(path_));
    if (!isdict(data)) throw new ValueError("TOML args file must be a key/value table.");
    this._apply(data);
  }

  /**
   * Load arguments from a ``.env``-style file.
   * @param {string} path Path to the ``.env`` file.
   */
  from_env(path_) {
    const parsed = {};
    for (const raw of _read_text(path_).split("\n")) {
      const line = raw.trim();
      if (!line || line.startsWith("#")) continue;
      if (!line.includes("=")) continue;
      const i = line.indexOf("=");
      const k = line.slice(0, i);
      const v = line.slice(i + 1);
      parsed[k.trim()] = this.constructor._coerce_scalar(v.trim());
    }
    this._apply(parsed);
  }

  /**
   * Load arguments from an XML file.
   * @param {string} path Path to the ``.xml`` file.
   */
  from_xml(path_) {
    const { XMLParser, XMLValidator } = _require("fast-xml-parser");
    const text = _read_text(path_);
    // ``ET.parse`` is a strict parser (expat) and raises ``ParseError`` on
    // malformed input; fast-xml-parser is lenient unless asked to validate.
    const valid = XMLValidator.validate(text);
    if (valid !== true) {
      const err = valid.err ?? {};
      throw new ParseError(`${err.msg ?? "not well-formed"}: line ${err.line ?? 1}, column ${err.col ?? 0}`);
    }
    const parser = new XMLParser({ ignoreAttributes: true, parseTagValue: false, trimValues: false, textNodeName: "#text" });
    const doc = parser.parse(text);
    const root_tag = Object.keys(doc).find((k) => k !== "?xml");
    const root = root_tag === undefined ? {} : doc[root_tag];
    const last = (v) => (Array.isArray(v) ? v[v.length - 1] : v);
    const text_of = (v) => {
      v = last(v);
      if (v === null || v === undefined) return "";
      if (typeof v === "string") return v;
      if (is_plain_object(v)) return typeof v["#text"] === "string" ? v["#text"] : "";
      return String(v);
    };
    const parsed = {};
    if (is_plain_object(root)) {
      for (const [tag, raw] of Object.entries(root)) {
        if (tag === "#text") continue;
        const child = last(raw);
        if (is_plain_object(child) && Object.keys(child).some((k) => k !== "#text")) {
          const sub = {};
          for (const [gc_tag, gc] of Object.entries(child)) {
            if (gc_tag === "#text") continue;
            sub[gc_tag] = text_of(gc);
          }
          parsed[tag] = sub;
        } else {
          parsed[tag] = text_of(child);
        }
      }
    }
    this._apply(parsed);
  }

  /**
   * Load arguments from ``key=value`` command-line tokens.
   * @param {Iterable<string>} [args] Tokens to parse. Defaults to ``process.argv.slice(2)``.
   */
  from_terminal(args = null) {
    const tokens = [...(args === null || args === undefined ? process.argv.slice(2) : args)];
    const parsed = {};
    for (const token of tokens) {
      if (!token.includes("=")) continue;
      const i = token.indexOf("=");
      const key = token.slice(0, i).trim();
      if (!key) continue;
      parsed[key] = this.constructor._coerce_scalar(token.slice(i + 1).trim());
    }
    this._apply(parsed);
  }

  /**
   * Auto-detect format and load arguments.
   *
   * @param {string} source File path (suffix selects format) or the literal ``"terminal"``.
   * @param {{terminal_args?: Iterable<string>}} [opts] ``terminal_args`` is
   *   passed to ``from_terminal`` when *source* is ``"terminal"``.
   * @throws {ValueError} If the file suffix is not supported.
   */
  load(source, opts = {}) {
    const { terminal_args = null } = opts;
    const p = String(source);
    const suffix = path.extname(p).toLowerCase();
    if (suffix === ".json") {
      this.from_json(p);
      return;
    }
    if (suffix === ".toml") {
      this.from_toml(p);
      return;
    }
    if (suffix === ".env") {
      this.from_env(p);
      return;
    }
    if (suffix === ".xml") {
      this.from_xml(p);
      return;
    }
    if (p.toLowerCase() === "terminal") {
      this.from_terminal(terminal_args);
      return;
    }
    throw new ValueError(`Unsupported args source: ${source}`);
  }
}
