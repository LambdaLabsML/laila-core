/**
 * Identifiable-object base class with UUID and global-ID support.
 *
 * The single class in this module, ``_LAILA_IDENTIFIABLE_OBJECT``, is the
 * foundation every *identifiable* laila object inherits from (it itself
 * derives from ``_LAILA_OBJECT``, the shared root that carries the creation
 * ``creation_timestamp``). It pins down the *identity* contract that the rest
 * of the codebase relies on:
 *
 * - A UUID (defaulting to a fresh ``uuid4``).
 * - A list of hierarchical *scopes* (e.g. ``["POLICY"]``, ``["FUTURE"]``).
 * - An optional *evolution* counter that distinguishes successive versions of
 *   the same logical object (``null`` for "constant" / unversioned objects, an
 *   integer for "variable" / mutable ones).
 *
 * These three components combine into the canonical *global ID* string
 * produced by ``global_id`` -- the form is::
 *
 *   LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]
 *
 * Everything after ``@`` is a comma-separated ``key=value`` list of
 * *attributes*. ``evolution`` is the only attribute that is part of the
 * identity and the only one ``global_id`` ever emits; any other key
 * (``creation_timestamp=...``) is accepted by the parser as a *search
 * argument*, exposed through ``get_attributes_from_global_id``, and consumed
 * by ``laila.remember`` -- it is never folded into identity.
 *
 * The string is used for hashing, equality, dict keys, RPC envelopes,
 * serialised forms, and pretty-printing alike, so consistency is critical.
 *
 * The ``GLOBAL_ID_REGEX_PATTERN`` regex at the top of the module is the
 * single source of truth for parsing global ids; all helpers funnel through
 * it via ``process_global_id``.
 *
 * Implementation note: identity fields are passed at construction time
 * through a thread-local "staging" object (``_INIT_PENDING``). This is needed
 * because Pydantic v2's ``validate_python`` wipes any private attributes set
 * before ``super()`` returns -- the staging trick lets the values be picked
 * up safely in ``model_post_init``.
 */
import { PrivateAttr, define_private, SKIP_VALIDATION, normalize_kwargs, construct_into } from "../../_compat/pydantic.js";
import { ValueError } from "../../_compat/errors.js";
import { local as threading_local } from "../../_compat/threading.js";
import { uuid4, uuid5 } from "../../_compat/uuid.js";
import { dumps as json_dumps } from "../../_compat/pyjson.js";
import { NotImplemented, type_name } from "../../_compat/pytypes.js";
import { repr } from "../../_compat/pyrepr.js";
import { lazy, register } from "../../_compat/lazy.js";
import { _ENTRY_SCOPE, _OBJECT_SCOPE, _TOPMOST_SCOPE } from "../../macros/strings.js";
import { _LAILA_OBJECT, _now_creation_timestamp } from "./laila_object.js";

// ``key=value`` attribute pair. Keys are identifiers; values may contain
// anything except the pair separator ``,`` and the attribute marker ``@``
// (ISO timestamps with ``:`` / ``+`` / ``.`` are therefore fine).
const _ATTRIBUTE_PAIR = "[A-Za-z_][A-Za-z0-9_]*=[^,@]*";

export const GLOBAL_ID_REGEX_PATTERN = new RegExp(
  "^(?<scopes>(?:[A-Za-z0-9_]+:)+)" + "(?<uuid>[0-9a-fA-F-]{36})" + `(?:@(?<attributes>${_ATTRIBUTE_PAIR}(?:,${_ATTRIBUTE_PAIR})*))?$`,
);

const _UUID_RE = /^[0-9a-fA-F-]{36}$/;
// Hex digits and dashes in the neighbourhood of a uuid's length: what a
// truncated / corrupted uuid (or a uuid with junk appended) looks like.
const _UUID_LIKE_RE = /^[0-9a-fA-F-]{30,48}$/;
const _ATTRIBUTE_RE = /^(?<key>[A-Za-z_][A-Za-z0-9_]*)=(?<value>[^,@]*)$/;
// Evolution as written in a *reference*: negatives (``-1`` = latest) are
// search arguments resolved by ``laila.remember``; identity is non-negative.
const _REFERENCE_EVOLUTION_RE = /^-?\d+$/;

/** Name of the single attribute that is part of an object's identity. */
export const EVOLUTION_ATTRIBUTE = "evolution";

// Scopes assumed for a scope-less *reference* (``"run-3"``,
// ``"counter@evolution=3"``) when the resolving class is the generic base.
const _DEFAULT_REFERENCE_SCOPES = [_ENTRY_SCOPE];

export const _INIT_PENDING = threading_local();

function _list_eq(a, b) {
  return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => x === b[i]);
}

/**
 * Parse the ``@``-suffix of a global id into an ordered ``{key: value}`` dict.
 *
 * @param {string|null} attributes The text after ``@``
 *   (``"evolution=3,creation_timestamp=..."``), or ``null`` / ``""`` for no
 *   attributes.
 * @returns {Object<string,string>} Raw string values keyed by attribute name,
 *   in the order written.
 * @throws {ValueError} On a malformed pair (missing ``=``, empty key, illegal
 *   key characters, trailing comma) or a duplicated key.
 */
export function parse_global_id_attributes(attributes) {
  if (!attributes) return {};
  const out = {};
  for (const pair of attributes.split(",")) {
    const m = _ATTRIBUTE_RE.exec(pair);
    if (m === null) throw new ValueError(`Invalid global id attribute: ${repr(pair)}`);
    const key = m.groups.key;
    if (Object.prototype.hasOwnProperty.call(out, key)) throw new ValueError(`Duplicate global id attribute: ${repr(key)}`);
    out[key] = m.groups.value;
  }
  return out;
}

/**
 * Inverse of ``parse_global_id_attributes`` (without the leading ``@``).
 *
 * ``null`` values are skipped so callers can pass ``{evolution: null}`` for
 * constants; an empty result means "no suffix".
 * @param {Object<string,any>|Map<string,any>} attributes
 */
export function format_global_id_attributes(attributes) {
  const entries = attributes instanceof Map ? [...attributes.entries()] : Object.entries(attributes);
  return entries
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

/**
 * Split ``"<head>@<attributes>"`` into ``[head, {key: value}]``.
 *
 * *head* is everything before the first ``@`` (scopes plus uuid or nickname).
 * Works on full global ids and on shorthand references alike; a reference
 * without ``@`` yields ``[ref, {}]``.
 * @param {string} ref
 * @returns {[string, Object<string,string>]}
 */
export function split_global_id_attributes(ref) {
  const i = ref.indexOf("@");
  if (i < 0) return [ref, {}];
  const head = ref.slice(0, i);
  const tail = ref.slice(i + 1);
  if (!tail) throw new ValueError(`Invalid GID format: ${ref}`);
  return [head, parse_global_id_attributes(tail)];
}

/**
 * Pydantic base model providing UUID-based identity and global-ID encoding.
 *
 * Every laila object that needs a stable identifier subclasses this. Inherits
 * the creation ``creation_timestamp`` from ``_LAILA_OBJECT``. The identity is
 * the triple (uuid, scopes, evolution) -- combined into a canonical *global
 * ID* by ``global_id`` and used for hashing, equality, serialisation, and
 * routing.
 *
 * Constructor options (``new Cls({uuid, scopes, evolution, nickname, ...})``):
 *
 * - ``uuid`` -- explicit UUID to assign. Auto-generated as a fresh ``uuid4``
 *   when omitted. Pass an explicit value for objects whose identity must be
 *   reproducible (manifests, named pools, ...).
 * - ``scopes`` -- hierarchical scope segments slotted between the topmost and
 *   ``GID`` scopes in the encoded global id. Defaults to ``["OBJECT"]``.
 *   Subclasses typically override the ``_scopes`` private attribute to
 *   provide their own domain-specific scope list (for example ``["FUTURE"]``
 *   for futures or ``["POLICY"]`` for policies).
 * - ``evolution`` -- version / evolution counter. ``null`` marks the object
 *   as *constant* (unversioned, immutable identity); a non-negative integer
 *   marks it as *variable* (versioned, mutable identity).
 * - ``nickname`` -- human-readable alias. Converted to a deterministic UUID-5
 *   scoped under the active namespace via ``generate_uuid_from_nickname``, so
 *   two objects with the same nickname under the same namespace share a UUID.
 *
 * Notes
 * -----
 * Identity is intentionally exposed via both private attributes (``_uuid``,
 * ``_scopes``, ``_evolution``) and public properties (``uuid``, ``scopes``,
 * ``evolution``) plus an aggregate ``global_id``. The properties are
 * settable, so identity can be mutated post-construction when needed (rare,
 * but supported for record-rewriting workflows).
 *
 * Performance note: the private attributes deliberately use plain
 * ``PrivateAttr({default: null})`` rather than ``default_factory``. Pydantic
 * re-inspects a private ``default_factory``'s signature on *every*
 * instantiation; identity objects are constructed on the hot path of every
 * task and entry, so defaults are computed in the constructor instead.
 * Subclasses set their scope list through the ``_DEFAULT_SCOPES`` class
 * variable (``static _DEFAULT_SCOPES = [_FUTURE_SCOPE]``) instead of
 * overriding ``_scopes`` with a factory.
 */
export class _LAILA_IDENTIFIABLE_OBJECT extends _LAILA_OBJECT {
  static _DEFAULT_SCOPES = [_OBJECT_SCOPE];

  static {
    define_private(this, {
      _uuid: PrivateAttr({ default: null }),
      _scopes: PrivateAttr({ default: null }),
      _evolution: PrivateAttr({ default: null }),
    });
  }

  /**
   * Stash identity fields in thread-local storage and delegate to Pydantic.
   *
   * The *effective* identity (explicit values or freshly generated defaults)
   * lands on ``_INIT_PENDING`` (a thread-local) so it survives Pydantic v2's
   * ``validate_python`` boundary and is already valid when any
   * ``model_post_init`` in the hierarchy runs (subclasses register themselves
   * by ``global_id`` there). ``model_post_init`` applies it; a fallback after
   * ``super()`` covers subclasses whose ``model_post_init`` does not chain to
   * this base.
   */
  constructor(data = {}) {
    if (data === SKIP_VALIDATION) {
      super(data);
      return;
    }
    data = normalize_kwargs(data, new.target);
    const uuid_input = data.uuid ?? null;
    const scopes_input = data.scopes ?? null;
    const evolution_input = data.evolution ?? null;
    const nickname_input = data.nickname ?? null;
    delete data.uuid;
    delete data.scopes;
    delete data.evolution;
    delete data.nickname;

    let pending_uuid;
    if (nickname_input !== null) pending_uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname_input);
    else if (uuid_input !== null) pending_uuid = String(uuid_input);
    else pending_uuid = String(uuid4());

    const scopes_explicit = scopes_input !== null;
    const pending_scopes = scopes_explicit ? [...scopes_input] : [...new.target._DEFAULT_SCOPES];

    // Save the enclosing construction's staged identity (if any): a nested
    // identifiable object may be built while Pydantic validates our fields,
    // and it must not wipe our values before our own ``model_post_init`` has
    // consumed them.
    const prev = [
      _INIT_PENDING.uuid ?? null,
      _INIT_PENDING.scopes ?? null,
      _INIT_PENDING.evolution ?? null,
      _INIT_PENDING.scopes_explicit ?? false,
    ];
    _INIT_PENDING.uuid = pending_uuid;
    _INIT_PENDING.scopes = pending_scopes;
    _INIT_PENDING.evolution = evolution_input;
    _INIT_PENDING.scopes_explicit = scopes_explicit;

    try {
      super(data);
    } finally {
      [_INIT_PENDING.uuid, _INIT_PENDING.scopes, _INIT_PENDING.evolution, _INIT_PENDING.scopes_explicit] = prev;
    }

    // Fallback for subclasses whose model_post_init does not chain to ours
    // (or that still declare ``_scopes`` with a default_factory).
    if (this._uuid === null || this._uuid === undefined) this._uuid = pending_uuid;
    if (this._scopes === null || this._scopes === undefined || (scopes_explicit && !_list_eq(this._scopes, pending_scopes))) this._scopes = pending_scopes;
    if (evolution_input !== null && (this._evolution === null || this._evolution === undefined)) this._evolution = evolution_input;
  }

  /**
   * ``_LAILA_IDENTIFIABLE_OBJECT.__init__(self, **data)`` applied to an
   * *existing* instance.
   *
   * Python lets a subclass call the base ``__init__`` explicitly on an object
   * created with ``cls.__new__(cls)`` (``Entry.from_dict`` does exactly that
   * to bypass the subclass constructor). JavaScript constructors cannot be
   * re-run on an instance, so this helper reproduces the body of the
   * constructor above -- identity staging, Pydantic population (via
   * ``construct_into``, which also runs the ``model_post_init`` chain),
   * ``_LAILA_OBJECT``'s creation stamp, and the post-init fallback -- on
   * *self*. *self* must have been created with ``new Cls(SKIP_VALIDATION)``.
   *
   * @param {_LAILA_IDENTIFIABLE_OBJECT} self
   * @param {object} [data]
   */
  static _init_identity(self, data = {}) {
    data = normalize_kwargs(data, self.constructor);
    const uuid_input = data.uuid ?? null;
    const scopes_input = data.scopes ?? null;
    const evolution_input = data.evolution ?? null;
    const nickname_input = data.nickname ?? null;
    delete data.uuid;
    delete data.scopes;
    delete data.evolution;
    delete data.nickname;

    let pending_uuid;
    if (nickname_input !== null) pending_uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname_input);
    else if (uuid_input !== null) pending_uuid = String(uuid_input);
    else pending_uuid = String(uuid4());

    const scopes_explicit = scopes_input !== null;
    const pending_scopes = scopes_explicit ? [...scopes_input] : [...self.constructor._DEFAULT_SCOPES];

    const prev = [
      _INIT_PENDING.uuid ?? null,
      _INIT_PENDING.scopes ?? null,
      _INIT_PENDING.evolution ?? null,
      _INIT_PENDING.scopes_explicit ?? false,
    ];
    _INIT_PENDING.uuid = pending_uuid;
    _INIT_PENDING.scopes = pending_scopes;
    _INIT_PENDING.evolution = evolution_input;
    _INIT_PENDING.scopes_explicit = scopes_explicit;

    try {
      construct_into(self, self.constructor, data);
      // ``_LAILA_OBJECT.__init__`` stamps the creation time after pydantic.
      self._creation_timestamp = _now_creation_timestamp();
    } finally {
      [_INIT_PENDING.uuid, _INIT_PENDING.scopes, _INIT_PENDING.evolution, _INIT_PENDING.scopes_explicit] = prev;
    }

    if (self._uuid === null || self._uuid === undefined) self._uuid = pending_uuid;
    if (self._scopes === null || self._scopes === undefined || (scopes_explicit && !_list_eq(self._scopes, pending_scopes))) self._scopes = pending_scopes;
    if (evolution_input !== null && (self._evolution === null || self._evolution === undefined)) self._evolution = evolution_input;
  }

  /**
   * Copy staged identity values from ``_INIT_PENDING`` onto private attrs.
   *
   * This is the back half of the construction trick described in the
   * constructor. Anything that was stashed on the thread-local is now safely
   * applied to the instance after Pydantic has finished validation.
   * Subclasses that override this method must call
   * ``super.model_post_init(_context)`` *first* so that ``this.global_id`` is
   * valid for their own registration logic. This method itself chains to
   * ``super`` so the hook stays cooperative across mixin diamonds.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    const pending_uuid = _INIT_PENDING.uuid ?? null;
    if (pending_uuid !== null) this._uuid = pending_uuid;
    const pending_scopes = _INIT_PENDING.scopes ?? null;
    if (pending_scopes !== null) {
      // A subclass may still declare ``_scopes`` with its own
      // default_factory; an explicit ``scopes=`` argument always wins, but a
      // generated default must not clobber it.
      if (this._scopes === null || this._scopes === undefined || (_INIT_PENDING.scopes_explicit ?? false)) this._scopes = pending_scopes;
    }
    const pending_evolution = _INIT_PENDING.evolution ?? null;
    if (pending_evolution !== null) this._evolution = pending_evolution;
  }

  /**
   * Construct an instance from an encoded global ID string.
   *
   * @param {string} global_id A full global ID matching
   *   ``GLOBAL_ID_REGEX_PATTERN``, or a scoped shorthand such as
   *   ``"POLICY:my_policy"`` (see ``resolve_global_id``).
   * @returns {_LAILA_IDENTIFIABLE_OBJECT} New instance with identity fields
   *   parsed from *global_id*.
   * @throws {ValueError} If *global_id* does not match the expected format.
   */
  static from_global_id(global_id) {
    const identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id);
    return new this(identity_data);
  }

  /**
   * Decode a global-id *reference* into a full global ID.
   *
   * This is the single place where the shorthand forms accepted all over
   * laila (``laila.remember("MANIFEST:my_dataset")``,
   * ``dst_policy="POLICY:trainer"``, ...) are expanded. Accepted forms for
   * *ref*:
   *
   * - A complete global id (``LAILA:ENTRY:<uuid>[@evolution=<n>]``) --
   *   returned unchanged (extra search attributes included).
   * - ``SCOPE[:SCOPE...]:<uuid | nickname>[@attrs]`` -- the scopes are placed
   *   after *prefix_scopes*; the tail is used verbatim when it is a UUID and
   *   hashed to a UUID-5 under the active namespace when it is a nickname. An
   *   ``evolution=<n>`` attribute is read as the evolution counter; other
   *   attributes are carried over verbatim as search arguments::
   *
   *     "MANIFEST:my_dataset"          -> LAILA:MANIFEST:<uuid5>
   *     "POLICY:trainer"               -> LAILA:POLICY:<uuid5>
   *     "POLICY:3f2a...-c47c"          -> LAILA:POLICY:3f2a...-c47c
   *     "ENTRY:counter@evolution=3"    -> LAILA:ENTRY:<uuid5>@evolution=3
   *     "ENTRY:counter@creation_timestamp=2026-..."
   *                                    -> LAILA:ENTRY:<uuid5>@creation_timestamp=2026-...
   *
   * - ``<uuid | nickname>[@attrs]`` with no scope segment -- scoped with
   *   *default_scopes*, which defaults to the class's ``_DEFAULT_SCOPES``
   *   (``Entry`` -> ``ENTRY``, ``Manifest`` -> ``MANIFEST``); on the generic
   *   base class a scope-less reference means an ``ENTRY`` (``"run-3"`` ==
   *   ``"ENTRY:run-3"``). A tail that looks like a truncated / corrupted uuid
   *   is rejected rather than hashed as a nickname.
   * - ``evolution`` may be negative in a *reference*
   *   (``"ENTRY:counter@evolution=-1"`` = latest stored evolution); it is
   *   resolved by ``laila.remember`` and never part of an identity.
   *
   * If the caller spells out the prefix anyway (``"LAILA:MANIFEST:my_dataset"``)
   * it is stripped rather than doubled.
   *
   * @param {string} ref Global id or shorthand.
   * @param {{evolution?: number|null, prefix_scopes?: string[]|null, default_scopes?: string[]|null, parse_evolution?: boolean}} [opts]
   *   - ``evolution``: explicit evolution counter. Overrides an ``evolution=``
   *     attribute present in *ref*.
   *   - ``prefix_scopes``: scopes placed before the user-supplied ones.
   *     Defaults to ``["LAILA"]``.
   *   - ``default_scopes``: scopes used when *ref* has no scope segment.
   *     Defaults to ``cls._DEFAULT_SCOPES``.
   *   - ``parse_evolution`` (default ``true``): whether an ``@...`` suffix on
   *     the reference is interpreted. Pass ``false`` to take the whole tail
   *     literally as a nickname (``"run@evolution=3"`` becomes the nickname).
   * @returns {string} The assembled global id.
   * @throws {ValueError} If *ref* is not a string, or has an empty tail /
   *   empty scope segment (``"POLICY:"``, ``":trainer"``, ``"A::b"``), or a
   *   malformed attribute list.
   */
  static resolve_global_id(ref, opts = {}) {
    const { evolution = null, prefix_scopes = null, default_scopes = null, parse_evolution = true } = opts;
    const cls = this;
    if (typeof ref !== "string") throw new ValueError(`global id reference must be a string, got ${type_name(ref)}`);

    const prefix = prefix_scopes !== null ? [...prefix_scopes] : [_TOPMOST_SCOPE];

    let head, attrs;
    if (parse_evolution) [head, attrs] = split_global_id_attributes(ref);
    else [head, attrs] = [ref, {}];

    let scopes = head.split(":");
    const tail = scopes.pop();
    if (!tail || !scopes.every((s) => s)) throw new ValueError(`Invalid GID format: ${ref}`);

    // Already a full global id (well-formed *and* framed by the prefix)?
    // ``POLICY:<uuid>`` also matches the regex but lacks the frame, so it
    // falls through and is expanded like any other shorthand.
    if (
      GLOBAL_ID_REGEX_PATTERN.test(ref) &&
      _list_eq(scopes.slice(0, prefix.length), prefix) &&
      scopes.length > prefix.length &&
      (evolution === null || attrs[EVOLUTION_ATTRIBUTE] === String(evolution))
    ) {
      return ref;
    }

    if (evolution !== null) attrs[EVOLUTION_ATTRIBUTE] = String(evolution);
    else if (EVOLUTION_ATTRIBUTE in attrs && !_REFERENCE_EVOLUTION_RE.test(attrs[EVOLUTION_ATTRIBUTE])) throw new ValueError(`Invalid evolution in GID: ${ref}`);

    let uid;
    if (_UUID_RE.test(tail)) uid = tail;
    else if (_UUID_LIKE_RE.test(tail)) {
      // Hex-and-dashes that is not a well-formed uuid: a truncated or
      // corrupted uuid, or the legacy ``<uuid>-<evolution>`` form. A
      // malformed id, not a nickname -- hashing it would silently produce a
      // *different* valid id.
      throw new ValueError(`Invalid GID format: ${ref}`);
    } else uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(tail);

    // Tolerate a spelled-out prefix.
    if (prefix.length && _list_eq(scopes.slice(0, prefix.length), prefix)) scopes = scopes.slice(prefix.length);
    if (!scopes.length) {
      if (default_scopes !== null) scopes = [...default_scopes];
      else if (_list_eq(cls._DEFAULT_SCOPES, [_OBJECT_SCOPE])) {
        // The base class has no domain of its own: a scope-less *reference*
        // means an entry (``laila.remember("run-3")``).
        scopes = [..._DEFAULT_REFERENCE_SCOPES];
      } else scopes = [...cls._DEFAULT_SCOPES];
    }

    const gid = `${[...prefix, ...scopes].join(":")}:${uid}`;
    const suffix = format_global_id_attributes(attrs);
    return suffix ? `${gid}@${suffix}` : gid;
  }

  /**
   * Build a global ID string from its constituent parts.
   *
   * @param {{uuid?: string|null, scopes?: string[]|null, evolution?: number|null, nickname?: string|null}} [opts]
   *   - ``uuid``: the UUID segment.
   *   - ``scopes``: scope segments inserted after the top-level ``LAILA`` scope.
   *   - ``evolution``: if provided, appended as an ``@evolution=<n>`` attribute.
   *   - ``nickname``: converted to a deterministic UUID-5 before encoding.
   * @returns {string} The assembled global ID.
   */
  static to_global_id(opts = {}) {
    let { uuid = null, scopes = null, evolution = null, nickname = null } = opts;
    if (nickname !== null) uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname);

    if (scopes === null || scopes === undefined || scopes.length === 0) scopes = [_OBJECT_SCOPE];

    const base = `${[_TOPMOST_SCOPE, ...scopes].join(":")}:${uuid}`;
    if (evolution === null || evolution === undefined) return base;
    return `${base}@${EVOLUTION_ATTRIBUTE}=${evolution}`;
  }

  /**
   * Return ``true`` if *global_id* parses as a laila global-ID string.
   *
   * Uses ``GLOBAL_ID_REGEX_PATTERN``. Useful for input validation before
   * passing strings to constructors that expect global ids (e.g.
   * ``from_global_id``).
   * @param {string} global_id
   */
  static is_laila_resource(global_id) {
    return GLOBAL_ID_REGEX_PATTERN.test(global_id);
  }

  /**
   * Fully-qualified global ID string for this instance.
   *
   * Composed as ``LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]`` from the
   * current ``uuid`` / ``scopes`` / ``evolution`` values. Setting this
   * property re-parses the string and re-assigns the underlying private attrs
   * (useful for in-place identity rebinding, e.g. while restoring from disk).
   * @returns {string}
   */
  get global_id() {
    return _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: this._uuid, scopes: this._scopes, evolution: this._evolution });
  }

  set global_id(value) {
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(value);
    this._uuid = parsed.uuid;
    this._scopes = parsed.scopes;
    this._evolution = parsed.evolution;
  }

  /** Bare UUID string (no scopes, no evolution suffix). */
  get uuid() {
    return this._uuid ?? null;
  }

  set uuid(value) {
    this._uuid = value;
  }

  /** Hierarchical scope segments, in the order they appear in the global id. */
  get scopes() {
    return this._scopes ?? null;
  }

  set scopes(value) {
    this._scopes = value;
  }

  /**
   * Evolution counter, or ``null`` for constant (unversioned) identities.
   *
   * See ``Entry.variable`` and ``Entry.constant`` for the most common
   * producers of versioned vs unversioned identities.
   */
  get evolution() {
    return this._evolution ?? null;
  }

  set evolution(value) {
    this._evolution = value;
  }

  /** Extract the scopes list from a global-id string without instantiating. */
  static get_scopes_from_global_id(global_id) {
    return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id).scopes;
  }

  /** Extract the bare UUID from a global-id string without instantiating. */
  static get_uuid_from_global_id(global_id) {
    return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id).uuid;
  }

  /**
   * Extract the evolution counter from a global-id string without instantiating.
   *
   * Returns ``null`` for global ids without an ``evolution`` attribute.
   */
  static get_evolution_from_global_id(global_id) {
    return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id).evolution;
  }

  /**
   * Return every ``@key=value`` attribute of *global_id* as raw strings.
   *
   * Unlike ``process_global_id`` this keeps *all* attributes, including
   * search arguments such as ``creation_timestamp`` that are not part of the
   * identity. ``evolution`` (when present) is returned as a string too.
   * Scoped shorthands are expanded first.
   * @param {string} global_id
   * @returns {Object<string,string>}
   */
  static get_attributes_from_global_id(global_id) {
    global_id = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(global_id);
    const match = GLOBAL_ID_REGEX_PATTERN.exec(global_id);
    if (match === null) throw new ValueError(`Invalid GID format: ${global_id}`);
    return parse_global_id_attributes(match.groups.attributes ?? null);
  }

  /**
   * Return *global_id* without its ``@...`` suffix (``LAILA:scopes:<uuid>``).
   *
   * The result is the common prefix shared by every evolution of the same
   * object, which is what pool-side searches key on.
   * @param {string} global_id
   */
  static strip_global_id_attributes(global_id) {
    const i = global_id.indexOf("@");
    return i < 0 ? global_id : global_id.slice(0, i);
  }

  /**
   * Return ``true`` if this object has a non-``null`` evolution counter.
   *
   * Convenience wrapper around ``this.evolution !== null``; equivalent to "is
   * this a *variable* (versioned) identity?"
   */
  has_evolution() {
    return this._evolution !== null && this._evolution !== undefined;
  }

  /**
   * Parse a global ID into its ``uuid``, ``scopes``, and ``evolution`` parts.
   *
   * @param {string} global_id A full global-ID string, or a *scoped*
   *   shorthand such as ``"POLICY:trainer"`` / ``"ENTRY:counter@evolution=3"``
   *   which is first expanded through ``resolve_global_id``. A bare string
   *   with no scope segment (``"counter"``) is an ``ENTRY`` reference.
   *   Attributes other than ``evolution`` are ignored (see
   *   ``get_attributes_from_global_id``); a *negative* evolution is a search
   *   argument, not an identity, and is rejected here.
   * @returns {{uuid: string, scopes: string[], evolution: number|null}}
   * @throws {ValueError} If *global_id* does not match the expected format.
   */
  static process_global_id(global_id) {
    if (typeof global_id !== "string" || !global_id) throw new ValueError(`Invalid GID format: ${global_id}`);
    // Framed full gids pass through unchanged; scoped shorthands
    // ("POLICY:trainer", "POOL:<uuid>") are expanded.
    global_id = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(global_id);
    const match = GLOBAL_ID_REGEX_PATTERN.exec(global_id);
    if (match === null) throw new ValueError(`Invalid GID format: ${global_id}`);
    const attrs = parse_global_id_attributes(match.groups.attributes ?? null);
    const evolution_raw = attrs[EVOLUTION_ATTRIBUTE] ?? null;
    if (evolution_raw !== null && !/^\d+$/.test(evolution_raw)) throw new ValueError(`Invalid evolution in GID: ${global_id}`);
    // The scopes group is "LAILA:<mid...>:"; drop the leading TOPMOST and
    // the empty string after the final ':' so that
    // ``to_global_id(**parsed)`` round-trips exactly.
    return {
      uuid: match.groups.uuid,
      scopes: match.groups.scopes.split(":").slice(1, -1),
      evolution: evolution_raw !== null ? parseInt(evolution_raw, 10) : null,
    };
  }

  /**
   * Classify a global id as ``"variable"`` (has evolution) or ``"constant"``.
   *
   * Useful when introspecting raw strings (e.g. on the receiving side of an
   * RPC) without committing to constructing the underlying object.
   * @param {string} x
   */
  static type(x) {
    const processed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(x);
    if (processed.evolution !== null) return "variable";
    return "constant";
  }

  /** ``str(obj)`` -> ``global_id``. Stable, human-readable, parseable. */
  __str__() {
    return this.global_id;
  }

  /**
   * ``repr(obj)`` -> ``global_id``. Same as ``__str__``.
   *
   * Identical to ``__str__`` because the global id is already canonical and
   * unambiguous; no need for additional debug framing.
   */
  __repr__() {
    return this.global_id;
  }

  toString() {
    return this.__str__();
  }

  /**
   * Hash by ``global_id`` so instances are usable as set / dict keys.
   *
   * Two identities with the same global id (same uuid + scopes + evolution)
   * hash equal even if they are distinct objects -- mirroring the equality
   * contract. (JS collections key by reference; use the returned string as
   * the ``Map`` key.)
   */
  __hash__() {
    return this.global_id;
  }

  /**
   * Equality follows identity: same ``global_id`` means the same object,
   * regardless of payload, timestamps or process-local state. This is the
   * counterpart of ``__hash__``; without it two handles to one identity would
   * hash alike yet compare unequal and set / dict de-duplication would be
   * unreliable.
   */
  __eq__(other) {
    if (!(other instanceof _LAILA_IDENTIFIABLE_OBJECT)) return NotImplemented;
    return this.global_id === other.global_id;
  }

  __ne__(other) {
    const eq = this.__eq__(other);
    return eq === NotImplemented ? eq : !eq;
  }

  /**
   * Return a minimal dict describing this object's identity.
   *
   * Always includes ``"uuid"``; includes ``"scopes"`` only when non-empty and
   * ``"evolution"`` only when non-``null``. The result is suitable for passing
   * back into a constructor or sending across an RPC envelope.
   * @returns {{uuid: string, scopes?: string[], evolution?: number}}
   */
  identity() {
    const identity = { uuid: this.uuid };
    if (this.scopes && this.scopes.length) identity.scopes = this.scopes;
    if (this.evolution !== null) identity.evolution = this.evolution;
    return identity;
  }

  /** Return ``identity()`` serialized to a JSON string. */
  identity_as_json() {
    return json_dumps(this.identity());
  }

  /**
   * Deterministically derive a UUID from a human-readable nickname.
   *
   * Uses ``uuid5`` with the *active namespace* (looked up via
   * ``laila.get_active_namespace()``) so two objects with the same nickname
   * under the same namespace share an identity. Switching namespaces produces
   * a different UUID for the same nickname -- this is how laila keeps
   * independent users from accidentally clobbering each other's named
   * resources.
   * @param {string} nickname
   * @returns {string}
   */
  static generate_uuid_from_nickname(nickname) {
    const { get_active_namespace } = lazy("laila");
    return String(uuid5(get_active_namespace(), nickname));
  }
}

register("laila.basics.definitions.identifiable_object", {
  GLOBAL_ID_REGEX_PATTERN,
  EVOLUTION_ATTRIBUTE,
  _INIT_PENDING,
  parse_global_id_attributes,
  format_global_id_attributes,
  split_global_id_attributes,
  _LAILA_IDENTIFIABLE_OBJECT,
});
