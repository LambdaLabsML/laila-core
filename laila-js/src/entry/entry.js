/**
 * Core ``Entry`` class -- the fundamental unit of data in LAILA.
 *
 * An ``Entry`` couples three concerns into one identifiable object:
 *
 * 1. **Identity.** Each entry has a UUID (random, explicit, or
 *    nickname-derived), a list of hierarchical scopes, and an optional
 *    *evolution* counter. Together they form a canonical ``global_id`` string
 *    that makes the entry addressable across processes and machines (see
 *    ``_LAILA_IDENTIFIABLE_OBJECT``).
 * 2. **Lifecycle state.** A small ``EntryState`` enum captures whether the
 *    entry's payload is materialized (``READY``), staged for build
 *    (``STAGED``), in flight to a pool (``POOLING``), pooled (``POOLED``),
 *    stale, or "not applicable" for subclasses that don't carry a payload
 *    (``NA``).
 * 3. **Either** a concrete payload (wrapped in a ``ComputationalData`` for
 *    type-aware serialization) **or** a ``Constitution`` describing how to
 *    build the payload. The two are mutually exclusive at construction time
 *    but may both be transiently present during a build.
 *
 * Two flavors
 * -----------
 * - **Constants** (``Entry.constant(data, ...)``): immutable. The
 *   ``evolution`` field is ``null`` and ``evolve()`` raises.
 * - **Variables** (``Entry.variable(data, ...)``): mutable in place via
 *   ``Entry.evolve`` which bumps the ``evolution`` counter. Two entries with
 *   the same UUID but different evolutions denote different versions of the
 *   same logical thing.
 *
 * Build paths
 * -----------
 * When created from a constitution, an entry starts ``STAGED`` and must be
 * materialized before its ``data`` can be read:
 *
 * - ``laila.build(entry).wait()`` -- sync caller; returns the same entry with
 *   ``state=READY``.
 * - ``await laila.build(entry)`` -- async caller; non-blocking.
 * - ``laila.remember(entry_id)``  -- if the entry is already memorized,
 *   fetching it routes through the pool's ``SimpleConstitution`` (the
 *   inverse-transformation chain) and returns a ``READY`` entry.
 *
 * See ``laila.build`` for the unified async build pipeline and the
 * distinction between ``SimpleConstitution`` (pure CPU) and
 * ``ComplexConstitution`` (manifest-driven, may await further fetches).
 */
import { ConfigDict, PrivateAttr, SKIP_VALIDATION, define_private, normalize_kwargs } from "../_compat/pydantic.js";
import { NotImplementedError, RuntimeError, TypeError as PyTypeError, ValueError } from "../_compat/errors.js";
import { dict_get, getitem, isdict, is_plain_object } from "../_compat/pytypes.js";
import { repr } from "../_compat/pyrepr.js";
import * as json from "../_compat/pyjson.js";
import * as asyncio from "../_compat/asyncio.js";
import { with_ } from "../_compat/contextlib.js";
import { lazy, register as _register_module } from "../_compat/lazy.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../atomic/definitions/locally_atomic_identifiable_object.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../basics/definitions/identifiable_object.js";
import { _now_creation_timestamp } from "../basics/definitions/laila_object.js";
import { _ENTRY_SCOPE, _POOL_INDEX_SCOPE } from "../macros/strings.js";
import { synchronized } from "../utils/decorators/synchronized.js";
import { ComputationalData } from "./compdata/index.js";
import { TransformationSequence } from "./compdata/transformation/index.js";
import { ComplexConstitution, Constitution, SimpleConstitution } from "./constitution/index.js";
import { _exec_one_fn } from "./constitution/constitution.js";
import { register_builder } from "./constitution/build_maps.js";
import { EntryState } from "./entry_state.js";
import { EntryNotBuiltError } from "./exceptions.js";

export { ComputationalData };

/**
 * The fundamental unit of data in LAILA.
 *
 * An ``Entry`` wraps arbitrary data (arrays, dicts, Manifests, etc.) with
 * three things:
 *
 * - **Identity** -- UUID + scopes + optional evolution counter; see
 *   ``_LAILA_IDENTIFIABLE_OBJECT``.
 * - **State** -- an ``EntryState`` reflecting where the entry is in its
 *   lifecycle (``READY`` / ``STAGED`` / ``POOLING`` / ``POOLED`` / ``STALE``
 *   / ``NA``).
 * - **Payload XOR Constitution** -- either a materialized
 *   ``ComputationalData`` payload, or a ``Constitution`` describing how to
 *   build one (see ``laila.build``).
 *
 * Mutability
 * ----------
 * - Use ``Entry.constant`` to create an *immutable* entry whose ``evolution``
 *   is ``null``. ``evolve`` raises on these.
 * - Use ``Entry.variable`` to create a *mutable* entry. Each call to
 *   ``evolve`` bumps ``evolution`` by 1 and replaces the payload, leaving the
 *   UUID stable.
 *
 * Public construction
 * -------------------
 * Prefer the class-method factories rather than the raw constructor:
 *
 *     const c    = Entry.constant({ x: 1 }, { nickname: "config" });
 *     const v    = Entry.variable(NDArray.zeros([8]), { evolution: 0 });
 *     const lazy = Entry.variable(null, { constitution: src, manifest: m });
 *
 * Each factory enforces the right combination of identity / payload /
 * constitution options and gives clearer errors than the constructor.
 *
 * Thread safety
 * -------------
 * All mutators (data setter, constitution setter, state setter, ``evolve``)
 * are decorated with ``synchronized`` and run under the entry's per-instance
 * ``atomic`` lock, so concurrent threads can read and update the same entry
 * without external locking.
 */
export class Entry extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
  static model_config = ConfigDict({ private_attributes: true, use_enum_values: true });

  static _ALLOWS_NON_NA_STATE = true;

  static _DEFAULT_SCOPES = [_ENTRY_SCOPE];

  static {
    define_private(this, {
      _state: PrivateAttr({ default: EntryState.STAGED }),
      _constitution: PrivateAttr({ default: null }),
      _payload: PrivateAttr({ default: null }),
      // Process-local "payload changed since the last memorize" marker. Set
      // by the ``data`` setter, cleared by ``mark_memorized`` (called from
      // central memory after a successful write) and reset to ``false`` at
      // the end of construction / deserialization / build so that only
      // *user* mutations count. Never serialized.
      _locally_modified: PrivateAttr({ default: false }),
    });
  }

  /**
   * Initialise an Entry from keyword arguments.
   *
   * Most callers should use the higher-level factories ``Entry.constant`` /
   * ``Entry.variable`` / ``Entry.contingent`` instead -- they validate
   * argument combinations and provide clearer errors. The raw constructor is
   * only invoked directly by deserialization paths (``from_dict``, the build
   * pipeline, etc.).
   *
   * Initialisation is split across four helpers so subclasses can override
   * individual phases without rewriting the whole constructor:
   *
   * 1. ``_initialize_identity`` parses ``global_id`` / ``uuid`` /
   *    ``evolution`` / ``nickname`` / ``scopes`` and threads them through the
   *    parent identifiable-object machinery.
   * 2. ``_initialize_payload`` wraps any ``data`` / ``payload`` value in a
   *    ``ComputationalData`` (subclass dispatched by payload type).
   * 3. ``_initialize_constitution`` accepts a live ``Constitution``, a list
   *    of source strings (auto-wrapped in ``SimpleConstitution``), or a
   *    single source string plus a ``manifest`` (auto-wrapped in
   *    ``ComplexConstitution``).
   * 4. ``_initialize_state`` picks the initial ``EntryState`` -- ``STAGED``
   *    when a constitution is attached, otherwise the explicit ``state``
   *    option or ``STAGED``.
   *
   * Accepted keys (all optional unless required by the chosen factory):
   *
   * ``data`` / ``payload``
   *     Raw payload value. Wrapped automatically.
   * ``uuid``
   *     Explicit UUID string. Mutually exclusive with ``global_id``.
   * ``evolution``
   *     Integer evolution counter, or ``null`` for constants.
   * ``global_id``
   *     Composite ``"LAILA:<scopes>:<uuid>[@evolution=<n>]"`` string.
   * ``nickname``
   *     Human-readable name; deterministically converted to a UUID-5 against
   *     the active namespace.
   * ``scopes``
   *     Override the default ``[ENTRY]`` scope list.
   * ``constitution``
   *     A ``Constitution``, a ``string[]`` (-> SimpleConstitution), or a
   *     ``string`` (-> ComplexConstitution, requires ``manifest``).
   * ``manifest``
   *     A ``Manifest`` to bind to a complex constitution.
   * ``state``
   *     Initial ``EntryState``.
   *
   * @param {object|Map} [data]
   */
  constructor(data = {}) {
    // ``cls.__new__(cls)``: the Python ``__init__`` bypasses pydantic and
    // populates the instance through ``_initialize_identity`` itself.
    super(SKIP_VALIDATION);
    if (data === SKIP_VALIDATION) return;
    data = normalize_kwargs(data, new.target);
    this._initialize_identity(data);
    this._initialize_payload(data);
    this._initialize_constitution(data);
    this._initialize_state(data);
    // The initial payload is the baseline, not a change.
    this._locally_modified = false;
  }

  /**
   * Parse identity fields from *data* and initialise the parent identity.
   *
   * ``global_id`` is preferred over the (uuid, evolution, scopes) triple when
   * present -- it is parsed via ``_LAILA_IDENTIFIABLE_OBJECT.process_global_id``
   * and the decoded fields populate the identity directly. Otherwise the
   * explicit ``uuid``, ``evolution``, ``nickname``, and ``scopes`` options are
   * forwarded to the parent constructor (which itself applies
   * nickname-to-UUID-5 derivation when relevant).
   * @param {object} data
   */
  _initialize_identity(data) {
    let identity_data = {};

    const global_id = data.global_id ?? null;
    const uuid = data.uuid ?? null;
    const evolution = data.evolution ?? null;
    const nickname = data.nickname ?? null;
    const scopes = data.scopes ?? null;

    if (global_id !== null) {
      identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id);
      _LAILA_IDENTIFIABLE_OBJECT._init_identity(this, identity_data);
      return;
    }

    if (uuid !== null) identity_data.uuid = uuid;

    if (evolution !== null) identity_data.evolution = evolution;

    if (nickname !== null) identity_data.nickname = nickname;

    if (scopes !== null) identity_data.scopes = scopes;

    _LAILA_IDENTIFIABLE_OBJECT._init_identity(this, identity_data);
  }

  /**
   * Set the entry's payload from ``payload`` or ``data`` options.
   *
   * Accepts either key for symmetry with the serialized representation
   * (``payload`` is the on-disk name) and the in-memory convention
   * (``data``). The setter wraps the raw value in a ``ComputationalData`` of
   * the appropriate subclass via the constructor's type-dispatch.
   * @param {object} data
   */
  _initialize_payload(data) {
    this.data = Object.prototype.hasOwnProperty.call(data, "payload") ? data.payload : (data.data ?? null);
  }

  /**
   * Set up a ``Constitution`` if one was provided.
   *
   * Three input shapes are normalized into the right ``Constitution``
   * subclass:
   *
   * - A live ``Constitution`` instance is used as-is. If it is a
   *   ``ComplexConstitution`` whose ``manifest`` is unset and a separate
   *   ``manifest`` option was supplied, the manifest is attached
   *   opportunistically (the one-shot manifest setter on
   *   ``ComplexConstitution`` enforces this is harmless).
   * - A ``string[]`` is interpreted as a chain of inverse-transform source
   *   strings and wrapped in ``SimpleConstitution``.
   * - A single ``string`` is interpreted as the source of a
   *   ``ComplexConstitution``'s callable; the optional ``manifest`` option
   *   is forwarded along.
   *
   * The attached constitution is cleared by ``_build_inplace`` /
   * ``_build_async`` once materialization succeeds, leaving
   * ``_constitution = null`` and ``_state = READY``.
   *
   * @param {object} data
   * @throws {TypeError} If ``constitution`` is not a ``Constitution``,
   *   ``string[]``, or ``string``.
   */
  _initialize_constitution(data) {
    const constitution = data.constitution ?? null;
    const manifest = data.manifest ?? null;
    if (constitution === null) return;
    if (constitution instanceof Constitution) {
      if (manifest !== null && constitution instanceof ComplexConstitution) {
        if (constitution.manifest === null || constitution.manifest === undefined) constitution.manifest = manifest;
      }
      this.constitution = constitution;
      return;
    }
    if (Array.isArray(constitution)) {
      this.constitution = new SimpleConstitution({ codes: constitution });
      return;
    }
    if (typeof constitution === "string") {
      const kwargs = { code: constitution };
      if (manifest !== null) kwargs.manifest = manifest;
      this.constitution = new ComplexConstitution(kwargs);
      return;
    }
    throw new PyTypeError("constitution must be a Constitution, list[str], or str");
  }

  /**
   * Determine the initial ``EntryState`` from *data*.
   *
   * Rules
   * -----
   * - Subclasses with ``_ALLOWS_NON_NA_STATE = false`` (e.g. ``Manifest``)
   *   are pinned to ``EntryState.NA`` regardless of input.
   * - When a constitution is attached, the entry starts ``STAGED`` (its
   *   payload must be built before access). The user's explicit ``state``
   *   option is ignored in this case to avoid a misleading ``READY`` flag on
   *   an unbuilt entry.
   * - Otherwise the explicit ``state`` option wins, defaulting to ``STAGED``
   *   if absent (``Entry.constant`` / ``Entry.variable`` override this to
   *   ``READY`` for the common case of inline data).
   * @param {object} data
   */
  _initialize_state(data) {
    if (!this.constructor._ALLOWS_NON_NA_STATE) {
      this.state = EntryState.NA;
      return;
    }

    const constitution = data.constitution ?? null;
    if (constitution !== null) this.state = EntryState.STAGED;
    else this.state = Object.prototype.hasOwnProperty.call(data, "state") ? data.state : EntryState.STAGED;
  }

  // ###################################################
  // Properties
  // ###################################################

  /**
   * Return the unwrapped payload value (or ``null`` if unset).
   *
   * This getter is **read-only** -- it never triggers an implicit build,
   * even when a constitution is attached. The reason is deadlock-avoidance:
   * if the calling thread is itself a taskforce loop thread (e.g. inside
   * another constitution body), an implicit build would submit work to the
   * same loop and then block on the loop, hanging forever. The user must
   * therefore materialize the entry explicitly:
   *
   * - Sync caller: ``laila.build(entry).wait()``.
   * - Async caller: ``await laila.build(entry)``.
   * - Or, if the entry is already pooled, ``laila.remember(entry_id)``.
   *
   * @returns {any|null} The raw value held by the entry's
   *   ``ComputationalData`` wrapper, or ``null`` when the entry has neither a
   *   payload nor a constitution.
   * @throws {EntryNotBuiltError} If a constitution is attached but the entry
   *   has not been built (no payload yet).
   */
  get data() {
    return with_(this.atomic({ scope: "local" }), () => {
      if (this._payload !== null && this._payload !== undefined) return this._payload.data;
      if (this._constitution !== null && this._constitution !== undefined) {
        throw new EntryNotBuiltError(
          `Entry ${this.global_id} is not built. ` +
            "Use `laila.build(entry).wait()` (or " +
            "`await laila.build(entry, asynchronous=True)`) " +
            "to materialize it before accessing .data.",
        );
      }
      return null;
    });
  }

  /**
   * Replace the payload value.
   *
   * Raw values (``NDArray``, dict, list, bytes, etc.) are auto-wrapped in the
   * appropriate ``ComputationalData`` subclass via the registered
   * ``TYPE_TO_WRAPPER`` map. Already-wrapped values pass through unchanged.
   * Pass ``null`` to clear the payload entirely.
   *
   * The setter is decorated with ``synchronized``, so concurrent writes from
   * different threads serialize on the entry's atomic lock.
   *
   * Assigning marks the entry *locally modified*: the next ``laila.memorize``
   * of a variable entry will bump its evolution (see
   * ``bump_evolution_if_locally_modified``). In-place mutation of the payload
   * object (``entry.data[0] = 1``) is not observed; re-assign
   * ``entry.data = value`` to flag the change.
   */
  set data(new_data) {
    if (new_data !== null && new_data !== undefined && !(new_data instanceof ComputationalData)) new_data = new ComputationalData(new_data);
    this._payload = new_data ?? null;
    this._locally_modified = true;
  }

  /**
   * ``true`` when the payload was re-assigned since the last memorize.
   *
   * Process-local bookkeeping only -- never serialized, never restored from
   * a pool. See ``bump_evolution_if_locally_modified``.
   * @returns {boolean}
   */
  get locally_modified() {
    return this._locally_modified;
  }

  /** Clear the locally-modified flag. Called by central memory after a successful write. */
  mark_memorized() {
    this._locally_modified = false;
  }

  /**
   * Advance ``evolution`` in place if the payload changed since the last memorize.
   *
   * This is the hook ``laila.memorize`` uses to decide whether a variable
   * entry is being re-written as the *same* evolution (payload untouched ->
   * idempotent overwrite of the same key) or as a *new* one (payload
   * re-assigned -> ``evolution += 1`` and a fresh ``creation_timestamp`` so
   * that time-based lookups can tell the versions apart). Constants
   * (``evolution === null``) are never bumped.
   *
   * Unlike ``evolve``, this mutates ``this``: its ``global_id`` (and
   * therefore its hash) changes, so any dict / set membership computed
   * before the memorize is stale afterwards.
   *
   * @returns {boolean} ``true`` if the evolution was incremented.
   */
  bump_evolution_if_locally_modified() {
    return with_(this.atomic({ scope: "local" }), () => {
      if (this._evolution === null || this._evolution === undefined || !this._locally_modified) return false;
      this._evolution += 1;
      this._creation_timestamp = _now_creation_timestamp();
      return true;
    });
  }

  /**
   * Attached ``Constitution``, or ``null`` once the entry has been built.
   *
   * A non-``null`` value here means "this entry knows how to materialize its
   * payload but hasn't done so yet". Once ``_build_inplace`` succeeds,
   * ``constitution`` is reset to ``null`` and ``state`` flips to ``READY``.
   */
  get constitution() {
    return this._constitution;
  }

  /**
   * Attach or clear the entry's ``Constitution``.
   *
   * Pass ``null`` to detach (typically only the build pipeline does this,
   * after a successful build). Pass any ``SimpleConstitution`` or
   * ``ComplexConstitution`` to attach a new build recipe.
   *
   * @throws {TypeError} If *new_constitution* is neither a ``Constitution``
   *   nor ``null``.
   */
  set constitution(new_constitution) {
    if (new_constitution !== null && new_constitution !== undefined && !(new_constitution instanceof Constitution)) throw new PyTypeError("constitution must be a Constitution or None");
    this._constitution = new_constitution ?? null;
  }

  /**
   * Current ``EntryState`` of this Entry.
   *
   * Set automatically by the build / serialize / hydrate paths. Outside
   * callers should rarely need to assign it directly.
   */
  get state() {
    return this._state;
  }

  /**
   * Replace the lifecycle state.
   *
   * Subclasses may pin themselves to ``EntryState.NA`` by setting
   * ``_ALLOWS_NON_NA_STATE = false`` (e.g. ``Manifest`` does this because
   * lifecycle is meaningless for it). On such subclasses any other value
   * raises.
   *
   * @throws {ValueError} If *new_state* is not an ``EntryState``, or if this
   *   subclass disallows non-``NA`` states.
   */
  set state(new_state) {
    if (!(new_state instanceof EntryState)) throw new ValueError("state must be an EntryState");
    if (!this.constructor._ALLOWS_NON_NA_STATE && new_state !== EntryState.NA) throw new ValueError(`${this.constructor.name} only accepts EntryState.NA`);
    this._state = new_state;
  }

  /**
   * Entry metadata (not yet implemented).
   * @throws {NotImplementedError} Always.
   */
  get metadata() {
    throw new NotImplementedError("Metadata is not implemented for Entry");
  }

  // ###################################################
  // Constitution Operations
  // ###################################################

  /**
   * Synchronously materialize this entry by running its constitution.
   *
   * Runs ``_build_inplace`` directly on the calling thread -- no command
   * submission, no future. Mostly an escape hatch for unit tests and the
   * SimpleConstitution branch of the deserialization pipeline; public callers
   * go through ``laila.build`` which submits ``_build_async`` to a taskforce
   * instead.
   *
   * @returns {Entry} ``this``, for convenient chaining.
   * @throws {RuntimeError} If the entry is already built (``state == READY``)
   *   or has no constitution attached.
   */
  _build_sync() {
    if (this._state === EntryState.READY) throw new RuntimeError("Entry is already built.");
    if (this._constitution === null || this._constitution === undefined) throw new RuntimeError("Entry has no constitution attached.");
    this._build_inplace();
    return this;
  }

  /**
   * Asynchronously materialize this entry by running its constitution.
   *
   * Two paths:
   *
   * - **Simple constitutions** are pure-CPU inverse-transformation chains
   *   with no I/O to ``await``. We call ``_build_inplace`` directly on the
   *   loop thread; the work is bounded and runs to completion before any
   *   other coroutine gets a turn.
   * - **Complex constitutions** are user-defined builders bound to a
   *   ``Manifest``. We:
   *
   *   1. ``await`` ``ComplexConstitution._resolve_manifest_async`` to fetch
   *      the manifest if only its global_id was carried through
   *      serialization;
   *   2. ``await target.async_realized`` to recursively materialize every
   *      entry the manifest references (the memory's direct-await resolver
   *      reads the pool on this very loop without creating per-child futures
   *      or blocking on a sync ``Future.wait()``);
   *   3. offload the user's *sync* constitution body to the owning
   *      taskforce's sync executor (``run_sync``) -- or ``asyncio.to_thread``
   *      outside a taskforce -- so any internal blocking calls
   *      (``manifest.realized``, ``Future.wait()``) park the slot instead of
   *      blocking the loop.
   *
   * On success the entry is mutated in place: ``_payload`` is set,
   * ``_constitution`` becomes ``null``, ``_state`` becomes ``READY``. The
   * promise resolves to ``this``.
   *
   * @returns {Promise<Entry>} ``this``.
   * @throws {RuntimeError} If the entry is already built, has no
   *   constitution, or (for complex constitutions) cannot resolve its
   *   manifest.
   */
  async _build_async() {
    if (this._state === EntryState.READY) throw new RuntimeError("Entry is already built.");
    if (this._constitution === null || this._constitution === undefined) throw new RuntimeError("Entry has no constitution attached.");

    if (this._constitution instanceof ComplexConstitution) {
      const c = this._constitution;
      if (c._code === null || c._code === undefined) throw new RuntimeError("no constitution code defined.");

      const target = await c._resolve_manifest_async();
      if (target === null || target === undefined) throw new RuntimeError("no manifest available to run constitution.");

      await target.async_realized;
      // User constitution code is sync and may itself call
      // `manifest.realized` (a sync wait). Offload to an executor
      // thread so it doesn't block the loop; blocking ``wait()``
      // paths inside the body park the taskforce slot.
      const { _CURRENT_SLOT } = lazy("laila.policy.central.command.schema.parking");

      const body = _exec_one_fn(c._code);
      const slot = _CURRENT_SLOT.get();
      const tf = slot !== null && slot !== undefined ? (slot.tf ?? null) : null;
      let result;
      if (tf !== null && typeof tf.run_sync === "function") result = await tf.run_sync(body, target);
      else result = await asyncio.to_thread(body, target);
      this._post_build(result);
      this.constitution = null;
      this.state = EntryState.READY;
      this._locally_modified = false;
      return this;
    }

    this._build_inplace();
    return this;
  }

  /**
   * Dispatch to ``_build_async`` (returns a promise) or ``_build_sync``
   * (runs inline).
   * @param {{asynchronous?: boolean}} [opts]
   */
  _build(opts = {}) {
    const { asynchronous = false } = opts;
    if (asynchronous) return this._build_async();
    return this._build_sync();
  }

  /**
   * Run the attached constitution synchronously, in place.
   *
   * Threads the current payload (if any) into the constitution as
   * ``payload_input`` and routes the result through ``_post_build``. After a
   * successful build the constitution is detached and the state flips to
   * ``READY``.
   *
   * Called from:
   *
   * - ``_build_sync`` -- direct sync materialization.
   * - ``_build_async`` -- the SimpleConstitution branch.
   * - ``_build_from_dict_sync`` -- inline rebuild during deserialization for
   *   SimpleConstitution entries.
   *
   * @throws {RuntimeError} If the entry has no constitution attached.
   */
  _build_inplace() {
    if (this._constitution === null || this._constitution === undefined) throw new RuntimeError("Entry has no constitution attached.");

    const payload_input = this._payload !== null && this._payload !== undefined ? this._payload.data : null;
    const result = this._constitution.build(payload_input);
    this._post_build(result);
    this.constitution = null;
    this.state = EntryState.READY;
    // Materializing stored bytes is not a user change.
    this._locally_modified = false;
  }

  /**
   * Place the build result into the entry's payload slot.
   *
   * Default behavior is to assign ``result`` to ``data`` (which wraps it in a
   * ``ComputationalData``). Subclasses such as ``Manifest`` override this hook
   * to perform a different side-effect (e.g. populating internal caches
   * instead of a single payload).
   */
  _post_build(result) {
    this.data = result;
  }

  // ###################################################
  // Variable
  // ###################################################

  /**
   * Create a *mutable* (evolvable) Entry.
   *
   * A variable entry has an integer ``evolution`` counter starting at ``0``
   * (or the user-supplied value). Each subsequent call to ``evolve`` bumps
   * the counter and replaces the payload, leaving the UUID stable -- two
   * snapshots of "the same logical thing" therefore differ only in their
   * ``@evolution=`` attribute. ``laila.memorize`` also advances the counter
   * automatically when the payload was re-assigned since the last memorize
   * (see ``bump_evolution_if_locally_modified``).
   *
   * Argument groups
   * ---------------
   * Pass *exactly one* of these payload shapes:
   *
   * - ``data`` -- a concrete payload value. The returned entry is ``READY``
   *   immediately.
   * - ``constitution`` *and* ``manifest`` -- a builder source string and a
   *   manifest to drive it. The returned entry is ``STAGED``; call
   *   ``laila.build(entry)`` to materialize.
   *
   * Pass *at most one* of these identity shapes:
   *
   * - ``global_id`` -- composite ``"LAILA:<scopes>:<uuid>[@evolution=<n>]"``.
   * - ``uuid`` (with optional ``evolution``).
   * - ``nickname`` -- deterministic UUID-5 derivation against the active
   *   namespace; useful for cross-process addressability.
   * - none of the above -- a fresh random UUID-4.
   *
   * @param {any} [data] The raw payload. Mutually exclusive with
   *   ``constitution`` / ``manifest``.
   * @param {{uuid?: string, evolution?: number, state?: any, constitution?: string,
   *   manifest?: any, global_id?: string, nickname?: string}} [opts]
   *   - ``uuid``: Explicit UUID. Mutually exclusive with ``global_id``.
   *   - ``evolution``: Starting evolution counter (defaults to ``0``).
   *   - ``state``: Initial state; auto-set to ``READY`` when no constitution
   *     is given. Ignored when ``constitution`` is supplied (the entry must
   *     start ``STAGED``).
   *   - ``constitution``: Source string defining exactly one function that
   *     takes a ``Manifest`` and returns the payload. When provided,
   *     ``manifest`` is required.
   *   - ``manifest``: Manifest feeding the constitution. Required when
   *     ``constitution`` is set; ignored otherwise.
   *   - ``global_id``: Composite identifier.
   *   - ``nickname``: Human-readable name used to derive a deterministic UUID-5.
   * @returns {Entry} A new variable Entry. When a constitution is supplied the
   *   entry starts in ``STAGED`` with the constitution attached.
   * @throws {RuntimeError} If conflicting identity arguments are provided, or
   *   if both ``constitution`` and ``data`` are set.
   * @throws {ValueError} If ``constitution`` is given without ``manifest`` or
   *   vice versa.
   *
   * Examples
   * --------
   * Plain payload, random UUID:
   *
   *     const e = Entry.variable(NDArray.zeros([8]));
   *
   * Nickname-derived identity:
   *
   *     const cfg = Entry.variable({ lr: 1e-3 }, { nickname: "train_config" });
   *
   * Lazy build from a constitution + manifest:
   *
   *     const src = "function f(m) { return m.realized.x.data + m.realized.y.data; }";
   *     const e = Entry.variable(null, { constitution: src, manifest: new Manifest({ x, y }) });
   *     laila.build(e).wait();
   */
  static variable(data = null, opts = {}) {
    let { uuid = null, evolution = null, state = null, constitution = null, manifest = null, global_id = null, nickname = null } = opts;
    data = data ?? null;

    if (global_id !== null && (uuid !== null || evolution !== null)) throw new RuntimeError("Cannot set both global_id and <uuid, evolution> at the same time.");

    if (constitution !== null && data !== null) throw new RuntimeError("Cannot set both constitution and data.");

    if ((constitution === null) !== (manifest === null)) throw new ValueError("`constitution` and `manifest` must both be provided together.");

    let scopes = null;
    if (global_id !== null) {
      const identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id);
      uuid = identity_data.uuid;
      evolution = identity_data.evolution;
      scopes = identity_data.scopes && identity_data.scopes.length ? identity_data.scopes : null;
    }

    evolution = evolution !== null && evolution !== undefined ? evolution : 0;

    uuid = this._merge_nickname_identity(uuid, nickname);

    const identity_kwargs = { uuid, evolution };
    if (scopes !== null) identity_kwargs.scopes = scopes;

    if (constitution !== null) {
      return new Entry({
        constitution,
        manifest,
        ...identity_kwargs,
      });
    }

    if (state === null) state = EntryState.READY;

    return new Entry({ data, state, ...identity_kwargs });
  }

  /**
   * Combine an explicit ``uuid`` with a ``nickname``.
   *
   * A nickname is a deterministic uuid generator and, exactly as in the
   * ``Entry`` constructor, it takes precedence over an explicit ``uuid`` /
   * ``global_id`` uuid when both are given.
   */
  static _merge_nickname_identity(uuid, nickname) {
    if (nickname === null || nickname === undefined) return uuid;
    return this.generate_uuid_from_nickname(nickname);
  }

  /**
   * Return a new Entry representing the next evolution.
   *
   * Unlike a typical mutator, ``evolve`` does **not** modify ``this``.
   * Instead it returns a freshly constructed ``Entry`` that shares the
   * original's identity -- same ``uuid`` and ``scopes`` -- but carries:
   *
   * - the new payload supplied via *data*, and
   * - ``evolution = this.evolution + 1``.
   *
   * The new entry is ``state == READY`` and, being a freshly constructed
   * object, carries its own ``creation_timestamp`` (the original's creation
   * stamp is not inherited). The original entry is left untouched, so callers
   * must rebind to capture the next version:
   *
   *     let v = laila.Entry.variable([1, 2, 3]);
   *     v = v.evolve([1, 2, 3, 4]);  // new entry, same uuid, evolution += 1
   *
   * Identity snapshotting (``_uuid``, ``_scopes``, ``_evolution``) happens
   * under the entry's per-instance ``atomic`` lock so that a concurrent
   * reader never observes a torn (uuid, evolution) pair while a new
   * evolution is being branched off.
   *
   * @param {any} [data] New payload value for the returned entry. ``null``
   *   yields an entry with no payload.
   * @returns {Entry} A new ``Entry`` with ``uuid`` and ``scopes`` matching
   *   ``this`` and ``evolution`` equal to ``this.evolution + 1``.
   * @throws {RuntimeError} If the Entry is a constant (``evolution === null``).
   * @throws {NotImplementedError} If the entry still carries an unbuilt
   *   constitution. Constitution-driven payload changes are out of scope
   *   here; materialize the entry first via ``laila.build`` or attach a fresh
   *   constitution explicitly.
   */
  evolve(data = null, ...rest) {
    // ``def evolve(self, data=None)`` takes no keyword arguments; a trailing
    // options object (e.g. ``{ constitution }``) is a TypeError in Python too.
    if (rest.length) {
      const extra = is_plain_object(rest[0]) ? Object.keys(rest[0])[0] : null;
      throw new PyTypeError(extra ? `Entry.evolve() got an unexpected keyword argument '${extra}'` : `Entry.evolve() takes from 1 to 2 positional arguments but ${rest.length + 2} were given`);
    }
    if (this._evolution === null || this._evolution === undefined) throw new RuntimeError("Can't evolve a constant.");

    if (this.constitution !== null && this.constitution !== undefined) {
      throw new NotImplementedError("Entry has not been built yet, internal logic cannot change " + "payload while constitution is not None.");
    }

    const [next_evolution, scopes_snapshot, uuid_snapshot] = with_(this.atomic({ scope: "local" }), () => [this._evolution + 1, [...this._scopes], this._uuid]);

    return Entry.contingent({
      uuid: uuid_snapshot,
      scopes: scopes_snapshot,
      evolution: next_evolution,
      data,
      state: EntryState.READY,
    });
  }

  // ###################################################
  // Constant
  // ###################################################

  /**
   * Create an *immutable* Entry whose ``evolution`` is ``null``.
   *
   * Constants do not support ``evolve`` and therefore do not carry an
   * ``@evolution=`` attribute in their global_id. The combination ``(uuid,
   * scopes)`` uniquely identifies a constant.
   *
   * This factory is the right choice for immutable artefacts that you want
   * to address by nickname across processes -- model checkpoints,
   * configuration blobs, pretrained weights, etc.
   *
   * @param {any} data The raw payload. Required (no constitution path on
   *   constants).
   * @param {{global_id?: string, uuid?: string, nickname?: string}} [opts]
   *   - ``global_id``: Composite identifier. Must NOT include an evolution
   *     component (since constants don't have one).
   *   - ``uuid``: Explicit UUID. Mutually exclusive with ``global_id``.
   *   - ``nickname``: Human-readable name used to derive a deterministic
   *     UUID-5 against the active namespace.
   * @returns {Entry} A new constant Entry, ``state == READY``,
   *   ``evolution === null``.
   * @throws {RuntimeError} If both ``global_id`` and ``uuid`` are supplied, or
   *   if ``global_id`` includes an evolution component.
   *
   * Examples
   * --------
   *
   *     const checkpoint = Entry.constant(weights, { nickname: "resnet50_v1" });
   *     laila.memorize(checkpoint).wait();
   *     // ... in another process, with the same active namespace:
   *     const same = laila.remember({ nickname: "resnet50_v1" }).wait();
   */
  static constant(data, opts = {}) {
    let { global_id = null, uuid = null, nickname = null } = opts;

    if (global_id !== null && uuid !== null) throw new RuntimeError("Cannot set both global_id and uuid at the same time.");

    let scopes = null;
    if (uuid === null && global_id !== null) {
      const identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id);
      uuid = identity_data.uuid;
      scopes = identity_data.scopes && identity_data.scopes.length ? identity_data.scopes : null;
      if (identity_data.evolution !== null && identity_data.evolution !== undefined) throw new RuntimeError("Cannot have a constant with an evolution.");
    }

    uuid = this._merge_nickname_identity(uuid, nickname);

    const identity_kwargs = { uuid, evolution: null };
    if (scopes !== null) identity_kwargs.scopes = scopes;

    const new_entry = new Entry({ data, state: EntryState.READY, ...identity_kwargs });

    return new_entry;
  }

  // ###################################################
  // Contingent
  // ###################################################

  /**
   * Create an Entry from raw keyword arguments, bypassing the consistency
   * checks performed by ``constant`` / ``variable``.
   *
   * This is the escape hatch for advanced callers (test fixtures,
   * deserializers, framework code) that already know the kwargs are
   * well-formed and don't want to be slowed down by mutual-exclusivity
   * checks.
   *
   * End users should almost always prefer ``constant`` or ``variable``.
   *
   * @param {object} [kwargs] Forwarded directly to the ``Entry`` constructor;
   *   see its docstring for the accepted keys.
   * @returns {Entry} A new Entry, configured exactly as the kwargs describe.
   */
  static contingent(kwargs = {}) {
    return new Entry(kwargs);
  }

  // ###################################################
  // Serialize and Recovery
  // ###################################################

  /**
   * Return the unified serialized shape for this entry.
   *
   * Unlike ``serialize``, this method does NOT apply any
   * ``TransformationSequence`` to the payload -- it embeds the raw payload
   * value directly. Use it for in-process inspection (e.g. converting an
   * entry to JSON for logging) where binary round-tripping is not needed.
   *
   * Returned keys (mirroring the on-disk schema):
   *
   * - ``_uuid`` : str
   * - ``_evolution`` : int or null
   * - ``_scopes`` : list[str]
   * - ``_state`` : str (the ``EntryState`` member name)
   * - ``_creation_timestamp`` : str or null (ISO-8601 UTC creation time
   *   inherited from ``_LAILA_OBJECT``)
   * - ``payload`` : Any (the unwrapped payload value, or null)
   * - ``constitution`` : dict or null (output of ``Constitution.as_dict``)
   *
   * @returns {object} Plain object suitable for JSON serialization (assuming
   *   the payload itself is JSON-friendly).
   */
  as_dict() {
    const constitution_dict = this._constitution !== null && this._constitution !== undefined ? this._constitution.as_dict() : null;
    let payload_value = null;
    if (this._payload !== null && this._payload !== undefined) payload_value = this._payload.data;
    return {
      _uuid: this._uuid,
      _evolution: this._evolution,
      _scopes: [...this._scopes],
      _state: this._state.name,
      _creation_timestamp: this._creation_timestamp,
      payload: payload_value,
      constitution: constitution_dict,
    };
  }

  /**
   * Serialize the Entry into a plain dict suitable for pool storage.
   *
   * Pipeline (when *transformations* is provided)
   * ---------------------------------------------
   * 1. The payload is serialized to bytes via its ``ComputationalData``
   *    ``.serialize()`` method, which also emits an *inverse* code string
   *    (e.g. ``"def f(b): return pickle.loads(b)"``) capable of recovering
   *    the original value from the bytes.
   * 2. The serialized bytes are fed through the ``TransformationSequence``
   *    (e.g. zlib then base64), each of which emits its own inverse code
   *    string.
   * 3. All inverse code strings are bundled into a fresh
   *    ``SimpleConstitution`` and packed into the returned dict under
   *    ``constitution``. On read, that constitution is run against the
   *    stored ``payload`` to recover the original value bit-for-bit.
   *
   * When *transformations* is ``null``, the in-memory pool path skips
   * serialization entirely: a *constant* is returned as the live ``Entry``
   * instance itself, while a *variable* is returned as a shallow
   * ``_snapshot`` so that later in-place evolution bumps (see
   * ``bump_evolution_if_locally_modified``) do not retroactively alter what
   * was stored under the previous evolution's key.
   *
   * @param {TransformationSequence|null} [transformations] Pipeline applied
   *   to the serialized payload bytes. Pass the same sequence the destination
   *   pool uses; the inverse chain recorded in the result lets the read side
   *   rebuild without knowing the pool's transformations.
   * @returns {Entry|object} The live ``Entry`` when *transformations* is
   *   ``null``; otherwise a serialized dict with keys ``_uuid``,
   *   ``_evolution``, ``_scopes``, ``_state``, ``_creation_timestamp``,
   *   ``payload``, and ``constitution``.
   * @throws {RuntimeError} If the entry is not ``EntryState.READY`` (we
   *   refuse to persist staged / stale entries because their payload is not
   *   yet a faithful snapshot).
   */
  serialize(transformations = null) {
    if (this._state !== EntryState.READY) {
      throw new RuntimeError(`Cannot serialize entry in state ${repr(this._state.name)}; ` + "only READY entries can be serialized.");
    }

    if (transformations === null || transformations === undefined) return this._evolution === null || this._evolution === undefined ? this : this._snapshot();

    let transformed_payload;
    let codes;
    if (this._payload !== null && this._payload !== undefined) {
      const [serialized_payload, payload_backward_code] = this._payload.serialize();
      let transformation_inverse_code;
      [transformed_payload, transformation_inverse_code] = transformations.forward(serialized_payload);
      codes = [...transformation_inverse_code, payload_backward_code];
    } else {
      transformed_payload = null;
      codes = [];
    }

    const constitution = new SimpleConstitution({ codes });

    return {
      _uuid: this._uuid,
      _evolution: this._evolution,
      _scopes: [...this._scopes],
      _state: this._state.name,
      _creation_timestamp: this._creation_timestamp,
      payload: transformed_payload,
      constitution: constitution.as_dict(),
    };
  }

  /**
   * Shallow copy sharing the payload object but with independent identity.
   *
   * Used by ``serialize`` for variables stored without transformations.
   * Identity fields (``_uuid``, ``_scopes``, ``_evolution``), state,
   * constitution and creation_timestamp are copied by value / reference; the
   * payload wrapper is shared (re-assigning ``data`` on the original does not
   * affect the copy). The copy gets its own atomic lock and starts clean (not
   * locally modified).
   * @returns {Entry}
   */
  _snapshot() {
    const clone = this.model_copy();
    Object.defineProperty(clone, "_local_lock", { value: null, writable: true, enumerable: true, configurable: true });
    clone._locally_modified = false;
    return clone;
  }

  /**
   * Hydrate an Entry from a serialized dict *without* running its constitution.
   *
   * This is the "raw" deserialization step -- it reconstructs identity,
   * attaches the stored payload as-is, and re-attaches the constitution if
   * one was serialized. The recovery chain is NOT executed; callers must
   * invoke ``_build_inplace`` themselves (or use ``_build_from_dict_sync`` /
   * ``_build_from_dict_async`` which combine both steps).
   *
   * @param {object|Map} in_dict A dict produced by ``serialize`` (or
   *   ``as_dict``), with the same keys.
   * @returns {Entry} A fresh Entry with identity restored, payload set to the
   *   raw stored value (typically still encoded), constitution re-attached,
   *   and state taken from the dict. The original ``creation_timestamp`` is
   *   restored when present; dicts that predate the field (or come from
   *   non-Python producers) are stamped with the current time instead.
   */
  static from_dict(in_dict) {
    const entry = new this(SKIP_VALIDATION);
    Entry.prototype._initialize_identity.call(entry, {
      uuid: getitem(in_dict, "_uuid"),
      evolution: dict_get(in_dict, "_evolution"),
      scopes: dict_get(in_dict, "_scopes"),
    });
    // ``cls.__new__`` bypasses ``_LAILA_OBJECT.__init__``, so the
    // creation_timestamp must be restored (or freshly stamped) explicitly.
    entry._creation_timestamp = dict_get(in_dict, "_creation_timestamp") || _now_creation_timestamp();
    entry.data = dict_get(in_dict, "payload");
    entry.state = EntryState.__getitem__(dict_get(in_dict, "_state", "STAGED") ?? "STAGED");
    entry.constitution = Constitution.from_dict(dict_get(in_dict, "constitution"));
    // A pool round-trip restores the memorized baseline: not locally modified.
    entry._locally_modified = false;
    return entry;
  }

  /**
   * Synchronously hydrate an entry from a serialized dict.
   *
   * Combines ``from_dict`` (raw rebuild) with ``_build_inplace`` for the
   * SimpleConstitution case so the common "fetch from pool, immediately use"
   * flow returns a ``READY`` entry in one call.
   *
   * Behavior by input shape
   * -----------------------
   * - **str** -- parsed as JSON; falls through to the dict path.
   * - **dict** -- ``from_dict`` followed by an inline ``_build_inplace`` if
   *   the constitution is a ``SimpleConstitution``. ``ComplexConstitution``
   *   entries are left STAGED -- materialising them requires ``laila.build``
   *   which submits to a taskforce, and we don't want to recurse into the
   *   taskforce machinery from a deserialization path.
   * - **Entry** -- returned as-is. (Already-live entries flowing through
   *   serialization paths are a no-op.)
   *
   * @returns {Entry} The hydrated entry. ``READY`` for simple-constitution
   *   inputs, ``STAGED`` for complex.
   * @throws {ValueError} If *in_dict* is a string that fails to parse as JSON.
   * @throws {RuntimeError} For any other unsupported input type.
   */
  static _build_from_dict_sync(in_dict) {
    let local = in_dict;
    if (typeof local === "string") {
      try {
        local = json.loads(local);
      } catch (e) {
        const err = new ValueError("Invalid JSON string");
        err.__cause__ = e;
        throw err;
      }
    }

    if (local instanceof Entry) return local;

    if (!isdict(local)) throw new RuntimeError("Invalid input for entry build.");

    const entry = this.from_dict(local);
    if (entry._constitution instanceof SimpleConstitution) entry._build_inplace();
    return entry;
  }

  /**
   * Async variant of ``_build_from_dict_sync``.
   *
   * Used inside the per-entry coroutines submitted by
   * ``_parallel_individual_fetch``. Although the body looks identical to the
   * sync version, having an async entry point means callers can ``await`` it
   * without context-switching to a worker thread: the from_dict step is pure
   * compute, and the SimpleConstitution branch is pure CPU, so nothing blocks
   * the loop here.
   *
   * Important: both the sync and async paths *always* run the
   * SimpleConstitution chain on the freshly-hydrated entry, even when the
   * serialized ``_state`` is already ``READY``. That's because the
   * SimpleConstitution attached on serialize *is* the recipe for reversing
   * the pool's storage transformations -- the ``READY`` state on disk just
   * records what the entry's lifecycle was at memorize time, not whether the
   * on-disk bytes are already the user's original payload.
   */
  static async _build_from_dict_async(in_dict) {
    let local = in_dict;
    if (typeof local === "string") {
      try {
        local = json.loads(local);
      } catch (e) {
        const err = new ValueError("Invalid JSON string");
        err.__cause__ = e;
        throw err;
      }
    }

    if (local instanceof Entry) return local;

    if (!isdict(local)) throw new RuntimeError("Invalid input for entry build.");

    const entry = this.from_dict(local);
    if (entry._constitution instanceof SimpleConstitution) entry._build_inplace();
    return entry;
  }

  /**
   * Router: dispatch to async or sync ``_build_from_dict``.
   * @param {any} in_dict
   * @param {{asynchronous?: boolean}} [opts]
   */
  static _build_from_dict(in_dict, opts = {}) {
    const { asynchronous = false } = opts;
    if (asynchronous) return this._build_from_dict_async(in_dict);
    return this._build_from_dict_sync(in_dict);
  }

  // ###################################################
  // String Representation
  // ###################################################

  /** Return the global identifier string. */
  __str__() {
    return this.global_id;
  }

  /** Return the global identifier string. */
  __repr__() {
    return this.global_id;
  }

  toString() {
    return this.global_id;
  }
}

// ``@synchronized`` on the property accessors (Python stacks the decorator
// under ``@property`` / ``@x.setter``). The ``data`` setter's positional
// argument may itself be a dict payload, which Python would not inspect for
// lockable values -- disable keyword inspection there.
{
  const proto = Entry.prototype;
  const wrap = (name, { get = false, set = false, opts = {} }) => {
    const d = Object.getOwnPropertyDescriptor(proto, name);
    if (get) d.get = synchronized(d.get, opts);
    if (set) d.set = synchronized(d.set, opts);
    Object.defineProperty(proto, name, d);
  };
  wrap("data", { set: true, opts: { kwargs: false } });
  wrap("constitution", { get: true, set: true });
  wrap("state", { get: true, set: true });
  wrap("metadata", { get: true });
}

register_builder(_ENTRY_SCOPE, Entry._build_from_dict_sync.bind(Entry), Entry._build_from_dict_async.bind(Entry));
// Pool index shards (data/schema/pool_index.py) are plain entries whose
// payload is a dict; registering them keeps a raw read of a shard from
// tripping ``build_by_scope``.
register_builder(_POOL_INDEX_SCOPE, Entry._build_from_dict_sync.bind(Entry), Entry._build_from_dict_async.bind(Entry));

// ``sys.modules["laila.entry.entry"]`` -- lets modules that import ``Entry``
// inside a function body (``build_maps.build_by_scope``) resolve it lazily.
_register_module("laila.entry.entry", { Entry, ComputationalData });
