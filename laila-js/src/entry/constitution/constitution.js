/**
 * Abstract ``Constitution`` base class -- recipe for materializing an
 * ``Entry``'s payload.
 *
 * A *constitution* tells the build pipeline how to recover (or compute) an
 * entry's payload from upstream input. Constitutions are first-class,
 * serializable objects so an entry can be persisted with its build recipe
 * intact and re-executed in another process / on another machine.
 *
 * Two concrete subclasses cover the bulk of the design space:
 *
 * - ``SimpleConstitution``
 *     An ordered list of source strings. Each source string defines exactly
 *     one top-level callable, and the build threads ``payload_input`` through
 *     the chain in order: ``code[0](input) -> code[1](output) -> ...``. The
 *     canonical use case is the *inverse* transformation chain emitted by
 *     ``Entry.serialize``: e.g. base64-decode -> zlib-decompress ->
 *     pickle-loads. Those snippets are the Python recovery templates laila
 *     emits; laila-js recognises them and runs native JS equivalents (it never
 *     evaluates Python). Any JS source defining one one-argument function is
 *     fair game too.
 *
 * - ``ComplexConstitution``
 *     A single source string defining ``f(manifest) -> payload`` plus a
 *     ``Manifest`` (or its ``global_id``). At build time the manifest is
 *     resolved from the active policy's memory if necessary, its referenced
 *     entries are forced to materialize, and the callable is applied to the
 *     live manifest. This is how derived / composite entries are described
 *     declaratively (``output = f(input1, input2)``). In laila-js the source
 *     string is *JavaScript* (the analogue of Python's ``exec`` is
 *     ``new Function``).
 *
 * Both subclasses implement ``build`` and ``as_dict``. The dispatcher
 * ``Constitution.from_dict`` selects the right subclass by reading the
 * ``_kind`` tag baked into every serialized constitution (``"simple"`` or
 * ``"complex"``).
 */
import { BaseModel, ConfigDict, finalize_model } from "../../_compat/pydantic.js";
import { check_abstract } from "../../_compat/abc.js";
import { KeyError, SyntaxError as PySyntaxError, TypeError as PyTypeError, ValueError } from "../../_compat/errors.js";
import { dict_get, dict_has, isdict } from "../../_compat/pytypes.js";
import { compile_backward, recognize } from "../../_codecs/recovery_codes.js";

/** @type {Object<string, typeof Constitution>} */
export const _REGISTRY = {};

/**
 * Decorator that registers a ``Constitution`` subclass under a string
 * ``_kind`` tag and stamps the same tag onto the class itself.
 *
 * The dispatcher ``Constitution.from_dict`` uses ``_REGISTRY`` to map
 * serialized ``_kind`` values back to the right subclass. Subclasses of
 * ``Constitution`` should be decorated with ``_register_kind("name")(cls)``
 * exactly once at class definition time.
 *
 * @param {string} kind A short, lowercase, globally-unique tag (``"simple"``,
 *   ``"complex"``, ...).
 */
export function _register_kind(kind) {
  return function deco(cls) {
    _REGISTRY[kind] = cls;
    cls._kind = kind;
    return cls;
  };
}

const _TOP_LEVEL_DECL = /^(?:(?:async\s+)?function\s*\*?\s*([A-Za-z_$][\w$]*)|(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=|class\s+([A-Za-z_$][\w$]*))/gm;

/**
 * Compile a source string and return its single defined callable.
 *
 * The "exactly one callable" rule keeps constitution code tiny and
 * unambiguous: a constitution slot in a serialized entry is meant to
 * describe *one* transformation step, so a source string that defines two
 * helpers leaves the chain ill-defined. The check tolerates private helpers
 * (names starting with ``__``) but treats every other callable as a
 * candidate.
 *
 * Two source languages are accepted:
 *
 * 1. laila's Python recovery snippets (``def backward(inp): ...`` emitted by
 *    the transformations) -- recognised by template and dispatched to the
 *    native JS backends in ``_codecs/recovery_codes.js``. Arbitrary Python is
 *    *not* executed and raises ``ValueError``.
 * 2. JavaScript source defining exactly one top-level callable (``function
 *    f(m) {...}``, ``const f = (m) => ...``, or a bare function expression).
 *    It is evaluated in an empty scope, mirroring ``exec(code, {})``: imports
 *    must be performed inside the source itself.
 *
 * @param {string} code
 * @returns {(arg: any) => any} The single callable defined by *code*.
 * @throws {ValueError} If *code* defines zero or multiple top-level callables.
 */
export function _exec_one_fn(code) {
  if (typeof code !== "string") throw new PyTypeError(`constitution code must be a str, not ${code === null ? "NoneType" : typeof code}`);
  if (recognize(code) !== null) return compile_backward(code);

  const names = [];
  for (const m of code.matchAll(_TOP_LEVEL_DECL)) names.push(m[1] ?? m[2] ?? m[3]);

  let namespace;
  try {
    if (names.length) {
      namespace = new Function(`"use strict";\n${code}\nreturn {${[...new Set(names)].join(", ")}};`)();
    } else {
      const value = new Function(`"use strict";\nreturn (\n${code}\n);`)();
      namespace = { __expr__: value };
      if (typeof value === "function") namespace = { f: value };
    }
  } catch (e) {
    if (e instanceof globalThis.SyntaxError || e instanceof ReferenceError) {
      if (/^\s*(def|import|from|class)\s/m.test(code)) throw new ValueError("constitution code is not a recognised laila recovery snippet; laila-js cannot execute arbitrary Python");
      // ``compile()`` raises ``SyntaxError`` in Python; it propagates unchanged.
      if (e instanceof globalThis.SyntaxError) throw new PySyntaxError(e.message);
      throw new ValueError(`constitution code failed to compile: ${e.message}`);
    }
    throw e;
  }
  const fns = Object.entries(namespace)
    .filter(([k, v]) => !k.startsWith("__") && typeof v === "function")
    .map(([, v]) => v);
  if (fns.length !== 1) throw new ValueError("constitution code must define exactly one callable function");
  return fns[0];
}

/**
 * Abstract base for entry constitutions.
 *
 * A constitution knows how to produce an entry's payload value. The
 * ``payload_input`` argument to ``build`` is used by simple constitutions to
 * receive the serialized-blob bytes that need to be decoded; complex
 * constitutions ignore it and read from their bound manifest instead.
 *
 * Subclasses must implement ``build``, ``as_dict``, and the ``_from_dict``
 * hook used by the dispatcher.
 */
export class Constitution extends BaseModel {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  /** @type {string} */
  static _kind = "";

  /** ``ABC``: refuse to instantiate while ``build`` / ``as_dict`` / ``_from_dict`` are abstract. */
  constructor(data = {}) {
    check_abstract(new.target, Constitution, ["build", "as_dict"], ["_from_dict"]);
    super(data);
  }

  static {
    finalize_model(this);
  }

  /** Instance view of the class-level ``_kind`` tag. */
  get _kind() {
    return this.constructor._kind;
  }

  /**
   * Produce and return the entry's payload value.
   *
   * @abstract
   * @param {any} [payload_input] For ``SimpleConstitution``: the serialized
   *   blob to be threaded through the inverse-transformation chain. For
   *   ``ComplexConstitution``: ignored (input comes from the bound manifest).
   */
  build(_payload_input = null) {
    throw new PyTypeError(`Can't instantiate abstract class ${this.constructor.name} with abstract method build`);
  }

  /**
   * Return the JSON-friendly serialized representation.
   *
   * The result must include a ``_kind`` key so ``from_dict`` can dispatch to
   * the right subclass on rehydration.
   * @abstract
   * @returns {object}
   */
  as_dict() {
    throw new PyTypeError(`Can't instantiate abstract class ${this.constructor.name} with abstract method as_dict`);
  }

  /**
   * Reconstruct a ``Constitution`` from its serialized dict.
   *
   * Reads the ``_kind`` tag, looks up the registered subclass in
   * ``_REGISTRY``, and delegates to that subclass's ``_from_dict`` to perform
   * the actual rebuild.
   *
   * @param {object|Map|null} in_dict The serialized constitution, including a
   *   ``_kind`` tag. ``null`` is allowed and short-circuits to ``null`` so
   *   callers can pass through optional fields without a guard.
   * @returns {Constitution|null}
   * @throws {KeyError} If *in_dict* is missing the ``_kind`` tag.
   * @throws {ValueError} If the ``_kind`` is not in ``_REGISTRY`` (typically
   *   because the subclass module has not been imported in this process yet).
   */
  static from_dict(in_dict) {
    if (in_dict === null || in_dict === undefined) return null;
    if (!isdict(in_dict) || !dict_has(in_dict, "_kind")) throw new KeyError("serialized constitution is missing the '_kind' tag");
    const kind = dict_get(in_dict, "_kind");
    const sub_cls = _REGISTRY[kind] ?? null;
    if (sub_cls === null) {
      throw new ValueError(`no Constitution subclass registered for kind '${kind}'. ` + `Registered: [${Object.keys(_REGISTRY).map((k) => `'${k}'`).join(", ")}]`);
    }
    return sub_cls._from_dict(in_dict);
  }

  /**
   * Subclass-specific dict -> instance hook used by ``from_dict``.
   *
   * Implementations should NOT re-check the ``_kind`` tag (the dispatcher has
   * already done so) and may freely raise ``KeyError`` / ``ValueError`` if
   * subclass-specific fields are missing or malformed.
   * @abstract
   * @param {object} _in_dict
   * @returns {Constitution}
   */
  static _from_dict(_in_dict) {
    throw new PyTypeError(`Can't instantiate abstract class ${this.name} with abstract method _from_dict`);
  }
}
