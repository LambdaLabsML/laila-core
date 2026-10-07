/**
 * Simple constitution: an ordered chain of single-callable code strings.
 *
 * A ``SimpleConstitution`` carries a list of source strings, each defining
 * exactly one callable. ``build(payload_input)`` threads ``payload_input``
 * through every callable in order:
 *
 *     out = code[0](payload_input)
 *     out = code[1](out)
 *     ...
 *
 * The canonical use case is the *inverse* transformation chain emitted by
 * ``Entry.serialize``. When a pool serializes an entry through
 * ``base64 -> zlib -> msgpack``, the corresponding simple constitution
 * contains three source strings whose composition decodes the bytes back into
 * the original value -- in the reverse order: ``msgpack-loads`` first, then
 * ``zlib-decompress``, then ``base64-decode``. (The transformation pipeline
 * emits its inverses already in reverse order, so the constitution itself is
 * just a flat list applied left-to-right.)
 */
import { PrivateAttr, SKIP_VALIDATION, define_private, normalize_kwargs } from "../../_compat/pydantic.js";
import { TypeError as PyTypeError } from "../../_compat/errors.js";
import { dict_get, is_list } from "../../_compat/pytypes.js";
import { Constitution, _exec_one_fn, _register_kind } from "./constitution.js";

/**
 * An ordered chain of single-callable code strings.
 *
 * Each element of ``codes`` is a self-contained source string defining
 * exactly one top-level callable. The chain is applied left-to-right at build
 * time. An empty chain is valid and returns ``payload_input`` unchanged.
 */
export class SimpleConstitution extends Constitution {
  static {
    define_private(this, { _codes: PrivateAttr({ default_factory: () => [] }) });
  }

  /**
   * Accept ``codes`` as the list of source strings.
   *
   * The list is shallow-copied internally so subsequent mutation of the
   * caller's list does not affect this constitution.
   *
   * @param {{codes?: string[]}} [data]
   * @throws {TypeError} If ``codes`` is not a list of strings.
   */
  constructor(data = {}) {
    if (data === SKIP_VALIDATION) {
      super(data);
      return;
    }
    super({});
    data = normalize_kwargs(data, new.target);
    const codes = data.codes ?? [];
    if (!is_list(codes) || !codes.every((c) => typeof c === "string")) throw new PyTypeError("codes must be a list of Python source strings");
    this._codes = [...codes];
  }

  /**
   * A defensive copy of the ordered list of source strings.
   *
   * Returning a copy means callers cannot accidentally mutate the
   * constitution by editing the returned list -- treat ``SimpleConstitution``
   * instances as immutable from the outside.
   * @returns {string[]}
   */
  get codes() {
    return [...this._codes];
  }

  /**
   * Thread *payload_input* through every code in order.
   *
   * Each step compiles its source string via ``_exec_one_fn`` and invokes
   * the resulting callable on the running value. Compilation happens on
   * every build (no cache), which keeps the memory footprint small for
   * constitutions that are rarely re-run but does mean tight inner loops
   * should not call ``build`` in a hot path.
   *
   * @param {any} [payload_input] The starting value (typically the encoded
   *   bytes loaded from a pool).
   * @returns {any} The result of applying every code string in order.
   */
  build(payload_input = null) {
    let current = payload_input;
    for (const code of this._codes) current = _exec_one_fn(code)(current);
    return current;
  }

  /** Serialize as ``{"_kind": "simple", "codes": [...]}``. */
  as_dict() {
    return { _kind: "simple", codes: [...this._codes] };
  }

  /**
   * Rebuild a ``SimpleConstitution`` from its serialized dict.
   *
   * Missing ``codes`` is treated as an empty chain (a valid no-op
   * constitution) rather than raising, mirroring the constructor's default.
   * @param {object|Map} in_dict
   * @returns {SimpleConstitution}
   */
  static _from_dict(in_dict) {
    return new this({ codes: dict_get(in_dict, "codes", []) });
  }
}

_register_kind("simple")(SimpleConstitution);
