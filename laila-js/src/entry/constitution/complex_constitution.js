/**
 * Complex constitution: a single source string driven by a ``Manifest``.
 *
 * A ``ComplexConstitution`` carries one source string that defines a single
 * callable ``f(manifest) -> payload``. At build time it resolves its bound
 * ``Manifest`` from the active policy's memory (using a stored ``global_id``
 * if the live object is not on hand), forces every entry the manifest
 * references to materialize inside a ``laila.guarantee`` scope, and then
 * applies the callable to the manifest.
 *
 * Why complex constitutions exist
 * -------------------------------
 * ``SimpleConstitution`` is a fine vehicle for inverse serialization chains,
 * but it cannot express *derivations* -- "this entry's payload is
 * ``f(other_entry_a, other_entry_b)``". A complex constitution carries both
 * the recipe (the source string) and the inputs (the manifest) so the
 * derivation can be re-executed in any process that can fetch the inputs
 * from a shared pool.
 *
 * Identity preservation across serialization
 * ------------------------------------------
 * A complex constitution does not embed the manifest's *contents* in its
 * serialized form; it embeds only the manifest's ``global_id``. The live
 * manifest is then re-fetched from the active policy's memory at build time.
 * This keeps serialized constitutions small and ensures a build always sees
 * the *current* state of the referenced entries, even if they have evolved
 * since the constitution was first created.
 *
 * Source language
 * ---------------
 * In laila-js the ``code`` string is JavaScript (``function f(m) {...}`` or
 * ``(m) => ...``), executed with ``new Function`` -- the direct analogue of
 * Python's ``exec``. Each runtime executes constitution code written in its
 * own language; cross-language complex constitutions are not supported.
 */
import { PrivateAttr, SKIP_VALIDATION, define_private, normalize_kwargs } from "../../_compat/pydantic.js";
import { AttributeError, RuntimeError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { dict_get } from "../../_compat/pytypes.js";
import { repr } from "../../_compat/pyrepr.js";
import { lazy } from "../../_compat/lazy.js";
import { with_ } from "../../_compat/contextlib.js";
import { Constitution, _exec_one_fn, _register_kind } from "./constitution.js";

/**
 * A one-shot constitution function bound to a ``Manifest``.
 *
 * Once assigned, neither the code nor the manifest can be reassigned -- the
 * constitution is intentionally immutable so that the gid derived from it is
 * stable. The manifest may be provided directly as a live ``Manifest``
 * instance or, when rehydrating a serialized entry, as a
 * ``manifest_global_id`` to be resolved lazily from the active policy's
 * memory at build time.
 *
 * Construction shapes
 * -------------------
 *
 *     // Direct: manifest object in hand
 *     const c = new ComplexConstitution({ code: src, manifest: m });
 *
 *     // Lazy: only the gid (typical after deserialization)
 *     const c = new ComplexConstitution({ code: src, manifest_global_id: gid });
 */
export class ComplexConstitution extends Constitution {
  static {
    define_private(this, {
      _code: PrivateAttr({ default: null }),
      _manifest: PrivateAttr({ default: null }),
      _manifest_global_id: PrivateAttr({ default: null }),
    });
  }

  /**
   * Initialise from optional ``code``, ``manifest``, and
   * ``manifest_global_id`` options.
   *
   * Either ``manifest`` or ``manifest_global_id`` may be supplied, not both
   * -- a live manifest takes precedence when both are present (the gid is
   * derived from it). Omitting both is allowed but the resulting constitution
   * cannot be built until a manifest is attached via the setter.
   *
   * @param {{code?: string, manifest?: any, manifest_global_id?: string}} [data]
   */
  constructor(data = {}) {
    if (data === SKIP_VALIDATION) {
      super(data);
      return;
    }
    super({});
    data = normalize_kwargs(data, new.target);
    const code = data.code ?? null;
    const manifest = data.manifest ?? null;
    const manifest_global_id = data.manifest_global_id ?? null;
    if (code !== null) this.code = code;
    if (manifest !== null) this.manifest = manifest;
    else if (manifest_global_id !== null) this._manifest_global_id = manifest_global_id;
  }

  /**
   * The single source string defining the constitution callable.
   *
   * Read-only after first assignment. The string must define exactly one
   * top-level callable taking a manifest and returning the payload value --
   * this is enforced by ``_exec_one_fn`` at build time, not at assignment
   * time, so syntactically invalid sources won't surface until the build
   * runs.
   * @returns {string|null}
   */
  get code() {
    return this._code;
  }

  /**
   * Assign the constitution code. One-time only.
   * @throws {AttributeError} If ``code`` is already set on this instance.
   * @throws {TypeError} If ``value`` is not a string.
   */
  set code(value) {
    if (this._code !== null && this._code !== undefined) throw new AttributeError("ComplexConstitution code is already set and cannot be reassigned.");
    if (typeof value !== "string") throw new PyTypeError("code must be a Python source string");
    this._code = value;
  }

  /**
   * The bound ``Manifest``, if available.
   *
   * May be ``null`` immediately after deserialization (only the gid is
   * preserved on disk). The first call to ``_resolve_manifest_sync`` or
   * ``_resolve_manifest_async`` populates this slot.
   */
  get manifest() {
    return this._manifest;
  }

  /**
   * Bind a live ``Manifest``. One-time only.
   *
   * Also records the manifest's ``global_id`` so the binding can be
   * re-established after deserialization.
   *
   * @throws {AttributeError} If a manifest is already bound.
   * @throws {TypeError} If ``value`` is not a ``Manifest``.
   */
  set manifest(value) {
    const { Manifest } = lazy("laila.policy.central.memory.schema.manifest");

    if (this._manifest !== null && this._manifest !== undefined) throw new AttributeError("ComplexConstitution manifest is already set and cannot be reassigned.");
    if (!(value instanceof Manifest)) throw new PyTypeError("manifest must be a Manifest instance");
    this._manifest = value;
    this._manifest_global_id = value.global_id;
  }

  /**
   * The bound manifest's ``global_id``, if known.
   *
   * Set automatically when ``manifest`` is assigned, and restored from the
   * serialized form by ``_from_dict``. Stable once set -- a complex
   * constitution does not allow rebinding to a different manifest.
   * @returns {string|null}
   */
  get manifest_global_id() {
    return this._manifest_global_id;
  }

  /**
   * Synchronously resolve ``_manifest`` from ``_manifest_global_id``.
   *
   * Cache hit: if ``_manifest`` is already populated, return it without any
   * I/O.
   *
   * Cache miss: submit a ``laila.remember`` for the stored gid and block the
   * calling thread on the resulting future. The fetched manifest is then
   * cached in ``_manifest`` for subsequent builds.
   *
   * Safe to call from any non-loop thread. If called from inside an async
   * loop thread the underlying ``Future.wait()`` will raise
   * ``LoopBlockingWaitError`` -- use ``_resolve_manifest_async`` from
   * coroutine contexts.
   *
   * @returns {any|null} The resolved manifest, or ``null`` if neither a live
   *   manifest nor a gid is bound.
   */
  _resolve_manifest_sync() {
    if (this._manifest !== null && this._manifest !== undefined) return this._manifest;
    if (this._manifest_global_id === null || this._manifest_global_id === undefined) return null;

    const laila = lazy("laila");

    const ref = laila.remember(this._manifest_global_id);
    let resolved;
    try {
      resolved = ref.wait(null);
    } finally {
      // Internal, self-consumed future: release it from the bank.
      ref.release();
    }
    return this._record_resolved_manifest(resolved);
  }

  /**
   * Asynchronously resolve ``_manifest`` from its global id.
   *
   * ``await``s the ``laila.remember`` future so the calling coroutine can
   * yield the loop while the underlying read / build pipeline runs. This is
   * the path ``Entry._build_async`` uses to pre-resolve the manifest before
   * invoking the user's sync constitution body in a worker thread, ensuring
   * the loop never blocks on a sync wait.
   */
  async _resolve_manifest_async() {
    if (this._manifest !== null && this._manifest !== undefined) return this._manifest;
    if (this._manifest_global_id === null || this._manifest_global_id === undefined) return null;

    const laila = lazy("laila");

    const ref = laila.remember(this._manifest_global_id);
    let resolved;
    try {
      resolved = await ref;
    } finally {
      ref.release();
    }
    return this._record_resolved_manifest(resolved);
  }

  /**
   * Dispatch to ``_resolve_manifest_async`` (returns a promise) or
   * ``_resolve_manifest_sync`` (blocks).
   * @param {{asynchronous?: boolean}} [opts]
   */
  _resolve_manifest(opts = {}) {
    const { asynchronous = false } = opts;
    if (asynchronous) return this._resolve_manifest_async();
    return this._resolve_manifest_sync();
  }

  /**
   * Cache the resolved manifest in ``_manifest`` and return it.
   *
   * ``laila.remember`` returns a list when it was given a list of ids; we
   * collapse a single-element list to its lone element (matching the
   * single-id call we issued) before caching.
   *
   * @throws {RuntimeError} If the lookup returned ``null`` -- the manifest
   *   gid is present in the constitution but not in the active policy's
   *   memory, so there is no way to materialize this entry. The caller almost
   *   certainly needs to ``laila.add_peer`` to the policy that owns the
   *   manifest, or copy the manifest into the active alpha pool first.
   */
  _record_resolved_manifest(resolved) {
    if (Array.isArray(resolved)) resolved = resolved.length ? resolved[0] : null;
    if (resolved === null || resolved === undefined) {
      throw new RuntimeError("Could not resolve manifest from global_id " + `${repr(this._manifest_global_id)}: not found in active memory.`);
    }
    this._manifest = resolved;
    return this._manifest;
  }

  /**
   * Resolve the manifest and apply the single constitution callable.
   *
   * ``payload_input`` is ignored -- complex constitutions read from their
   * bound manifest, never from a serialized blob.
   *
   * Always uses the *sync* manifest-resolution path because user constitution
   * code is sync and runs inside ``Entry._build_inplace``. Coroutine-aware
   * callers (``Entry._build_async``) pre-resolve the manifest via
   * ``await this._resolve_manifest_async()`` before invoking
   * ``_build_inplace``, so the sync call here is a cache hit and never
   * actually waits.
   *
   * The ``with_(laila.guarantee, ...)`` block around ``target.realized``
   * forces every entry the manifest references to be materialized on the
   * active policy's alpha pool before the user's callable runs, so referenced
   * entries are guaranteed to be ``READY`` when the callable accesses them.
   *
   * @throws {RuntimeError} If no constitution code is set, or the manifest
   *   cannot be resolved.
   */
  build(_payload_input = null) {
    const laila = lazy("laila");

    if (this._code === null || this._code === undefined) throw new RuntimeError("no constitution code defined.");

    const target = this._resolve_manifest_sync();
    if (target === null || target === undefined) throw new RuntimeError("no manifest available to run constitution.");

    with_(laila.guarantee, () => {
      void target.realized;
    });

    return _exec_one_fn(this._code)(target);
  }

  /** Serialize as ``{"_kind": "complex", "code": ..., "manifest_global_id": ...}``. */
  as_dict() {
    return {
      _kind: "complex",
      code: this._code,
      manifest_global_id: this._manifest_global_id,
    };
  }

  /**
   * Build a ``ComplexConstitution`` from its serialized dict.
   * @param {object|Map} in_dict
   * @returns {ComplexConstitution}
   */
  static _from_dict(in_dict) {
    return new this({
      code: dict_get(in_dict, "code"),
      manifest_global_id: dict_get(in_dict, "manifest_global_id"),
    });
  }
}

_register_kind("complex")(ComplexConstitution);
