/**
 * Record model -- decorates an ``Entry`` with provenance metadata for storage.
 *
 * A ``Record`` is what actually lands in a pool. It wraps an entry with
 * three pieces of provenance:
 *
 * - **recorder** -- the policy gid that originated the write (defaults to
 *   the active policy at construction time);
 * - **borrower** -- the policy gid that requested the entry on behalf of
 *   someone else (``null`` unless a caller sets it; reserved for future
 *   attribution flows);
 * - **record_timestamp** -- ISO-8601 UTC timestamp of when the record was
 *   constructed.
 *
 * Pools never see raw entries; they always see (and persist) a serialized
 * ``Record``. On read, ``Record._build_async`` / ``Record._build_sync`` strip
 * the envelope and re-hydrate the inner entry via the registered
 * scope-specific builder.
 */
import { now_iso_ms } from "../../../../_compat/datetime.js";
import { TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import { lazy, register } from "../../../../_compat/lazy.js";
import { BaseModel, ConfigDict, Field, define_fields } from "../../../../_compat/pydantic.js";
import * as json from "../../../../_compat/pyjson.js";
import { dict_copy, dict_get, dict_has, dict_set, getitem, isdict, type_name } from "../../../../_compat/pytypes.js";

/**
 * Immutable wrapper pairing an ``Entry`` with recorder/borrower metadata.
 *
 * Construct with ``new Record({ entry })``; the recorder defaults to the
 * active policy's ``global_id`` and the timestamp is captured at
 * construction time. Use ``serialize`` to turn the record into the on-disk
 * dict shape (which embeds the entry's serialized form), and ``_build_sync``
 * / ``_build_async`` to invert that on read.
 */
export class Record extends BaseModel {
  // ``extra="forbid"`` so a misspelled provenance kwarg (``creator=``)
  // fails loudly instead of being silently dropped.
  static model_config = ConfigDict({ arbitrary_types_allowed: true, extra: "forbid" });

  static {
    define_fields(this, {
      entry: ["Any"],
      recorder: ["str | None", null],
      borrower: ["str | None", null],
      record_timestamp: ["str", Field({ default_factory: () => now_iso_ms() })],
    });
  }

  /**
   * Default the recorder to the active policy's gid when not provided.
   *
   * ``borrower`` is left exactly as given (``null`` unless a caller
   * attributes the write to another policy).
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    const { active_policy } = lazy("laila");

    if (this.recorder === null || this.recorder === undefined) this.recorder = active_policy.global_id;
  }

  /**
   * Serialize the record into a pool-storable dict.
   *
   * The wrapped entry is run through ``Entry.serialize`` with the provided
   * *transformations* (which is also the pool's ``transformations``
   * attribute) and the resulting dict replaces the ``entry`` slot in the
   * record. The pool then persists this whole dict atomically.
   *
   * @param {any} transformations The pool's transformation pipeline.
   * @returns {object} A pool-storable dict with ``entry`` replaced by its
   *   serialized form.
   */
  serialize(transformations) {
    const record_as_dict = this.as_dict;
    record_as_dict.entry = this.entry.serialize(transformations);

    return record_as_dict;
  }

  /** Return the global ID of the wrapped entry. */
  get entry_id() {
    if (this.entry !== null && this.entry !== undefined && "global_id" in Object(this.entry)) return this.entry.global_id;

    if (isdict(this.entry)) return getitem(this.entry, "_global_id");

    throw new PyTypeError(
      "Record.entry must expose a `global_id` or be a serialized entry mapping with " + `\`_global_id\`; got ${type_name(this.entry)}`,
    );
  }

  /** Return a plain dict representation, preserving the raw entry object. */
  get as_dict() {
    const data = this.model_dump();

    // Since it model_dumps entry in a weird way
    data.entry = this.entry;
    return data;
  }

  /**
   * Construct a Record from the dict shape produced by ``as_dict``.
   *
   * ``entry`` may be a live ``Entry`` or its ``to_dict`` / serialized-dict
   * form, in which case it is rebuilt through ``Entry.from_dict``.
   * ``recorder``, ``borrower`` and ``record_timestamp`` are passed through
   * when present.
   *
   * @throws {TypeError} *in_dict* is not a mapping.
   * @throws {ValueError} *in_dict* has no ``entry`` slot.
   */
  static from_dict(in_dict) {
    if (!isdict(in_dict)) throw new PyTypeError(`Record.from_dict expects a mapping, got ${type_name(in_dict)}`);
    if (!dict_has(in_dict, "entry")) throw new ValueError("Record.from_dict requires an 'entry' slot");

    let entry = getitem(in_dict, "entry");
    if (isdict(entry)) {
      const { Entry } = lazy("laila.entry.entry");

      entry = Entry.from_dict(dict_copy(entry));
    }

    const kwargs = { entry };
    for (const name of ["recorder", "borrower", "record_timestamp"]) {
      const v = dict_get(in_dict, name, null);
      if (v !== null && v !== undefined) kwargs[name] = v;
    }
    return new this(kwargs);
  }

  /**
   * Synchronously hydrate a record dict from a JSON string or raw dict.
   *
   * Dispatches to the registered builder for the entry's scope via
   * ``build_by_scope`` (sync path), which returns a fully hydrated ``Entry``.
   * Mutates *record* in place to replace the serialized ``entry`` field with
   * the live entry, and returns the same record.
   *
   * Returning a dict (rather than a fresh ``Record``) is intentional:
   * callers in the read pipeline only need the inner entry; the metadata
   * fields (recorder/borrower/timestamp) are read directly off the dict for
   * logging and audit.
   */
  static _build_sync(record) {
    if (typeof record === "string") record = json.loads(record);

    const { build_by_scope } = lazy("laila.entry.constitution.build_maps");

    dict_set(record, "entry", build_by_scope(getitem(record, "entry"), { asynchronous: false }));
    return record;
  }

  /**
   * Async variant of ``_build_sync``.
   *
   * Awaits the registered builder for the entry's scope (async path),
   * allowing nested manifest fetches to yield the loop instead of blocking
   * it.
   */
  static async _build_async(record) {
    if (typeof record === "string") record = json.loads(record);

    const { build_by_scope } = lazy("laila.entry.constitution.build_maps");

    dict_set(record, "entry", await build_by_scope(getitem(record, "entry"), { asynchronous: true }));
    return record;
  }

  /** Router: dispatch to ``_build_async`` or ``_build_sync``. */
  static _build(record, opts = {}) {
    const { asynchronous = false } = opts;
    if (asynchronous) return this._build_async(record);
    return this._build_sync(record);
  }
}

register("laila.policy.central.memory.record.record", { Record });
