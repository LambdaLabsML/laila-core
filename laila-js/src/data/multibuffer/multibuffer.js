/**
 * MultiBuffer -- an integer-indexed ring of records with independent
 * read/write heads.
 *
 * Where a pool is a *map* keyed by entry ``global_id``, a ``MultiBuffer`` is
 * a *list*: a fixed number of slots addressed by integer index, plus two
 * cursors that walk those slots modulo the capacity. It is the container a
 * microcontroller puts in front of a device that produces data faster than
 * it can be persisted -- the canonical example being a camera with a double
 * (or triple) frame buffer.
 *
 * Value contract (shared with pools)
 * ----------------------------------
 * - ``buf[i] = value`` stores a ``Record``. A bare ``Entry`` is wrapped as
 *   ``Record(entry=...)`` on the way in, exactly as central memory does
 *   before a pool write; raw payloads are first lifted to a constant entry.
 * - ``buf[i]`` returns the bare ``Entry`` (``record.entry``). Raw slot
 *   contents that were never passed through ``__setitem__`` -- bytes
 *   deposited by hardware -- are wrapped into a constant entry on the way
 *   out. Empty slots read as ``null``.
 *
 * Heads
 * -----
 * ``write()`` targets ``_write_head`` and ``read()`` targets ``_read_head``;
 * each call advances its own head by one, wrapping at ``capacity``. The heads
 * are independent so a producer can run ahead of the consumer by up to
 * ``capacity`` slots (the usual double-buffer hand-off), and both are only
 * ever mutated under the instance's atomic lock.
 *
 * Mapped mode
 * -----------
 * When the buffer stands in for memory that *something else* fills -- a DMA
 * engine, a camera driver, a shared buffer -- construct it with
 * ``mapped=true`` and pass that memory as ``slots``. The list is used as-is
 * (never copied) so external writes are visible immediately. In this mode
 * ``write()`` does **not** go through ``__setitem__``: it only advances the
 * write head (optionally depositing a raw value directly into the slot
 * first). ``read()`` is unchanged -- it still goes through ``__getitem__``,
 * which is where the raw bytes at the read head become an ``Entry``.
 *
 * Typical microcontroller loop::
 *
 *     const frames = new MultiBuffer({ slots: dma_slots, mapped: true });
 *     const entry = frames.read();   // raw bytes at _read_head -> Entry
 *     laila.memorize(entry);         // Record into the policy's pool
 */
import { with_ } from "../../_compat/contextlib.js";
import { TypeError as PyTypeError, ValueError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { sequence_key } from "../../_compat/proxy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { is_bool, is_integral, len, type_name } from "../../_compat/pytypes.js";
import { CLIExempt } from "../../basics/definitions/cli_capable.js";
import { _MULTI_BUFFER_SCOPE } from "../../macros/strings.js";
import { _LAILA_IDENTIFIABLE_DATA_CONTAINER } from "../schema/data_container.js";

const _UNSET = Symbol("_UNSET");

/** Python ``%`` (result takes the sign of the divisor). */
function _mod(a, n) {
  return ((a % n) + n) % n;
}

/**
 * Fixed-capacity ring of records with separate read and write heads.
 *
 * Attributes
 * ----------
 * capacity : int
 *     Number of slots. Defaults to ``2`` (a double buffer). When ``slots``
 *     is supplied explicitly this is overwritten with ``len(slots)``.
 * mapped : bool
 *     ``true`` when ``slots`` is externally-owned memory that is filled
 *     outside laila (see module docstring). Changes only what ``write``
 *     does; reads always go through the item protocol.
 * slots : list-like
 *     The backing storage. Sized to ``capacity`` with ``null`` when not
 *     provided. When provided it is used by reference, so a caller (or
 *     hardware) mutating the same object is observed by the buffer. Must
 *     support ``len``, integer ``__getitem__`` and ``__setitem__``.
 */
export class MultiBuffer extends _LAILA_IDENTIFIABLE_DATA_CONTAINER {
  // ``mb[4]`` / ``mb[-1]``: integer slot indices arrive as property strings.
  static _index_key = sequence_key;

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_MULTI_BUFFER_SCOPE] }),
      _read_head: PrivateAttr({ default: 0 }),
      _write_head: PrivateAttr({ default: 0 }),
    });
    define_fields(this, {
      capacity: ["int", Field({ default: 2, ge: 1 })],
      mapped: ["bool", Field({ default: false })],
      // Typed as Any (not list[Any]) so pydantic hands the caller's object
      // through untouched -- copying would break mapped mode.
      slots: ["Any", CLIExempt({ default: null })],
    });
  }

  model_post_init(_context) {
    super.model_post_init(_context);
    if (this.slots === null || this.slots === undefined) {
      this.slots = new Array(this.capacity).fill(null);
      return;
    }
    let n;
    try {
      n = len(this.slots);
    } catch (exc) {
      const err = new PyTypeError("MultiBuffer.slots must be a sized, indexable sequence");
      err.__cause__ = exc;
      throw err;
    }
    if (n === 0) throw new ValueError("MultiBuffer.slots must contain at least one slot");
    this.capacity = n;
  }

  // -------- Heads --------
  /** Index of the slot the next ``read`` will return. */
  get read_head() {
    return this._read_head;
  }

  /** Index of the slot the next ``write`` will fill. */
  get write_head() {
    return this._write_head;
  }

  _index(key) {
    if (is_bool(key) || !is_integral(key)) throw new PyTypeError(`MultiBuffer indices must be integers, not ${type_name(key)}`);
    return _mod(Number(key), this.capacity);
  }

  __len__() {
    return this.capacity;
  }

  // -------- Item protocol --------
  /**
   * Return the ``Entry`` at slot *key* (modulo capacity), or ``null``.
   *
   * A stored ``Record`` yields its ``entry``. Raw contents (anything that is
   * neither a record nor an entry -- e.g. bytes written by mapped hardware)
   * are wrapped into a constant entry.
   */
  __getitem__(key) {
    const { Entry } = lazy("laila.entry.entry");
    const { Record } = lazy("laila.policy.central.memory.record.record");

    const value = with_(this.atomic(), () => _slot_get(this.slots, this._index(key)));

    if (value === null || value === undefined) return null;
    if (value instanceof Record) return value.entry;
    if (value instanceof Entry) return value;
    return Entry.constant(value);
  }

  /**
   * Store a ``Record`` at slot *key* (modulo capacity).
   *
   * Accepts a ``Record`` (stored as-is), an ``Entry`` (wrapped in a fresh
   * record), a raw payload (lifted to a constant entry, then wrapped), or
   * ``null`` to clear the slot.
   */
  __setitem__(key, value) {
    const { Entry } = lazy("laila.entry.entry");
    const { Record } = lazy("laila.policy.central.memory.record.record");

    let record;
    if (value === null || value === undefined) record = null;
    else if (value instanceof Record) record = value;
    else if (value instanceof Entry) record = new Record({ entry: value });
    else record = new Record({ entry: Entry.constant(value) });

    with_(this.atomic(), () => {
      _slot_set(this.slots, this._index(key), record);
    });
  }

  /** Clear every slot and rewind both heads to slot ``0``. */
  empty() {
    with_(this.atomic(), () => {
      for (let i = 0; i < this.capacity; i++) _slot_set(this.slots, i, null);
      this._read_head = 0;
      this._write_head = 0;
    });
  }

  // -------- Verbs --------
  /**
   * Fill the slot at the write head, then advance the head.
   *
   * Unmapped: ``self[write_head] = value`` (so the slot ends up holding a
   * ``Record``); *value* is required.
   *
   * Mapped: ``__setitem__`` is bypassed. If *value* is given it is deposited
   * raw into the slot; if omitted the slot is assumed to have been filled
   * externally and only the head moves.
   *
   * @returns {number} The index of the slot that was written.
   */
  write(value = _UNSET) {
    return with_(this.atomic(), () => {
      const idx = this._write_head;
      if (this.mapped) {
        if (value !== _UNSET) _slot_set(this.slots, idx, value);
      } else {
        if (value === _UNSET) throw new PyTypeError("MultiBuffer.write() requires a value when not mapped");
        this.__setitem__(idx, value);
      }
      this._write_head = (idx + 1) % this.capacity;
      return idx;
    });
  }

  /**
   * Return the ``Entry`` at the read head, then advance the head.
   *
   * Goes through ``__getitem__``, so in mapped mode this is where raw
   * accumulated bytes are wrapped into an entry. An empty slot yields
   * ``null`` (the head still advances).
   */
  read() {
    return with_(this.atomic(), () => {
      const idx = this._read_head;
      const entry = this.__getitem__(idx);
      this._read_head = (idx + 1) % this.capacity;
      return entry;
    });
  }
}

/** ``slots[i]`` for arrays, typed arrays and objects exposing ``__getitem__``. */
function _slot_get(slots, i) {
  if (typeof slots.__getitem__ === "function" && !Array.isArray(slots) && !ArrayBuffer.isView(slots)) return slots.__getitem__(i);
  return slots[i];
}

/** ``slots[i] = v`` for arrays, typed arrays and objects exposing ``__setitem__``. */
function _slot_set(slots, i, v) {
  if (typeof slots.__setitem__ === "function" && !Array.isArray(slots) && !ArrayBuffer.isView(slots)) return slots.__setitem__(i, v);
  slots[i] = v;
  return undefined;
}

register("laila.data.multibuffer.multibuffer", { MultiBuffer });
