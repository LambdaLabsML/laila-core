/**
 * Per-pool evolution / creation-timestamp index.
 *
 * A ``PoolIndex`` answers, for one *base* global id (``LAILA:ENTRY:<uuid>``,
 * no ``@`` suffix), the questions ``laila.remember`` asks when a reference
 * carries search attributes:
 *
 * - which evolutions are stored (and whether a constant key exists), so
 *   ``@evolution=-1`` (latest) / ``-k`` resolve without listing keys;
 * - which evolution was created at a given ``creation_timestamp``, so
 *   ``@creation_timestamp=<iso>`` resolves without reading every record.
 *
 * Layout
 * ------
 * One **shard per base**. A shard is an evolvable ``Entry`` with scope
 * ``POOL_INDEX`` and the deterministic id
 * ``LAILA:POOL_INDEX:<uuid5("pool_index:<owner uuid>:<base>")>`` whose
 * payload is::
 *
 *     {"base": base, "evolutions": [0, 1, 2], "constant": False,
 *      "creation_timestamps": {"2026-...": 2, ...}}
 *
 * Shards live in ``owner.index_pool`` (default: the owner pool itself; point
 * it at an in-memory pool for cheap index writes). Only the latest shard
 * evolution is kept: after shard evolution N is written, N-1 is deleted.
 * Rewriting a shard is O(size of that base's history), not O(size of the
 * pool), and shards are loaded on demand.
 *
 * Consistency model
 * -----------------
 * The index is a **validated cache, never an authority**. Every hit is
 * confirmed by the read that follows it in central memory; a miss
 * invalidates the base and falls back to the key scan. A failed shard write
 * logs, invalidates the base, and never fails the user's ``memorize``.
 * ``PoolIndex.rebuild`` repopulates every shard from the owner's keys.
 *
 * Maintenance is **write-through**: the pool's ``write`` / ``delete``
 * wrappers call ``record`` / ``remove``, which update the shard in memory and
 * persist it immediately, all under the owner's atomic lock. Shard keys are
 * never indexed themselves and are hidden from the pool's public ``keys()``.
 */
import { with_ } from "../../_compat/contextlib.js";
import { lazy, register } from "../../_compat/lazy.js";
import * as logging from "../../_compat/logging.js";
import * as json from "../../_compat/pyjson.js";
import { dict_del, dict_get, dict_has, dict_items, dict_set, getattr, isdict, sorted, str } from "../../_compat/pytypes.js";
import { _LAILA_IDENTIFIABLE_OBJECT, EVOLUTION_ATTRIBUTE, split_global_id_attributes } from "../../basics/definitions/identifiable_object.js";
import { _POOL_INDEX_SCOPE, _TOPMOST_SCOPE } from "../../macros/strings.js";

const _LOG = logging.getLogger("laila.data.schema.pool_index");

const _INDEX_KEY_PREFIX = `${_TOPMOST_SCOPE}:${_POOL_INDEX_SCOPE}:`;

/** Name of the search attribute keyed by an entry's creation stamp. */
export const CREATION_TIMESTAMP_ATTRIBUTE = "creation_timestamp";

/** ``true`` for storage keys that belong to an index shard. */
export function is_index_key(key) {
  return typeof key === "string" && key.startsWith(_INDEX_KEY_PREFIX);
}

/**
 * Pull the entry ``creation_timestamp`` out of a stored record without
 * rebuilding it.
 *
 * *raw* is whatever the pool holds: a JSON string / bytes, a record dict
 * whose ``entry`` is a serialized dict, or (in-memory pools with no
 * transformations) a record dict whose ``entry`` is a live ``Entry``.
 * @param {any} raw
 * @returns {string|null}
 */
export function _record_creation_timestamp(raw) {
  if (raw instanceof Uint8Array) raw = Buffer.from(raw).toString("utf8");
  if (typeof raw === "string") {
    try {
      raw = json.loads(raw);
    } catch {
      return null;
    }
  }
  if (!isdict(raw)) return getattr(raw, "creation_timestamp", null);
  const entry = dict_get(raw, "entry", raw);
  if (isdict(entry)) return dict_get(entry, "_creation_timestamp", null);
  return getattr(entry, "creation_timestamp", null);
}

/** Evolution encoded in a storage key; ``null`` for a constant (no ``@``). */
export function _key_evolution(key) {
  const [, attrs] = split_global_id_attributes(key);
  const raw = attrs[EVOLUTION_ATTRIBUTE] ?? null;
  if (raw === null || !/^\d+$/.test(raw)) return null;
  return Number(raw);
}

export function _evolution_key(base, evolution) {
  return evolution === null || evolution === undefined ? base : `${base}@${EVOLUTION_ATTRIBUTE}=${evolution}`;
}

/** Sort rank: constants (no evolution) sort below every evolution. */
export function _rank(evolution) {
  return evolution === null || evolution === undefined ? -1 : evolution;
}

const _ABSENT = Symbol("__absent__");

/**
 * Shard-per-base evolution / creation-timestamp index of one pool.
 *
 * Created lazily by ``_LAILA_IDENTIFIABLE_POOL.index``. All methods take the
 * owner's atomic lock (re-entrant), so callers may hold it already.
 */
export class PoolIndex {
  /**
   * @param {any} owner The pool whose keys are indexed. ``owner.index_pool``
   *   (or the owner itself) stores the shards.
   */
  constructor(owner) {
    this._owner = owner;
    /** @type {Record<string, any>} base -> shard dict */
    this._shards = {};
    /** @type {Record<string, any>} base -> live shard Entry */
    this._entries = {};
    /** @type {Set<string>} bases known to have no shard */
    this._missing = new Set();
  }

  // ------------------------------------------------------------------
  // Identity helpers
  // ------------------------------------------------------------------
  /** Pool that stores the shards (``owner.index_pool`` or the owner). */
  get index_pool() {
    return this._owner.index_pool || this._owner;
  }

  /** Deterministic base id of the shard for *base* (no evolution attribute). */
  shard_id(base) {
    return _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ nickname: `pool_index:${this._owner.uuid}:${base}`, scopes: [_POOL_INDEX_SCOPE] });
  }

  // ------------------------------------------------------------------
  // Queries
  // ------------------------------------------------------------------
  /** Every stored key of *base*, or ``null`` when no shard exists. */
  candidates(base) {
    const shard = this._shard(base);
    if (shard === null) return null;
    const keys = shard.constant ? [base] : [];
    for (const e of shard.evolutions) keys.push(_evolution_key(base, e));
    return keys;
  }

  /** Key of the highest evolution (or the constant key), or ``null``. */
  latest(base) {
    return this.nth(base, -1);
  }

  /**
   * Key of the *n*-th evolution of *base*.
   *
   * ``n >= 0`` is an exact evolution; ``n < 0`` counts from the end of the
   * sorted evolutions (``-1`` = latest). A constant key is preferred for
   * ``n < 0`` when it is the only thing stored, and counts as the lowest rank
   * otherwise. ``null`` when out of range or unindexed.
   */
  nth(base, n) {
    const shard = this._shard(base);
    if (shard === null) return null;
    if (n >= 0) return shard.evolutions.includes(n) ? _evolution_key(base, n) : null;
    const ranked = [...(shard.constant ? [null] : []), ...shard.evolutions];
    const idx = ranked.length + n;
    if (idx < 0 || idx >= ranked.length) return null;
    return _evolution_key(base, ranked[idx]);
  }

  /**
   * Key of the evolution created at *timestamp* (exact match).
   *
   * With *evolution* given, the match must also be that evolution (negative
   * values count from the end, as in ``nth``). ``null`` when unindexed or no
   * such stamp.
   */
  by_creation_timestamp(base, timestamp, evolution = null) {
    const shard = this._shard(base);
    if (shard === null) return null;
    if (!dict_has(shard.creation_timestamps, timestamp)) return null;
    const found = shard.creation_timestamps[timestamp];
    const key = _evolution_key(base, found);
    if (evolution !== null && this.nth(base, evolution) !== key) return null;
    return key;
  }

  // ------------------------------------------------------------------
  // Maintenance (write-through)
  // ------------------------------------------------------------------
  /** Index a key that was just written with record *value*, then persist the shard. */
  record(key, value) {
    if (is_index_key(key)) return;
    const [base] = split_global_id_attributes(key);
    const evolution = _key_evolution(key);
    const stamp = _record_creation_timestamp(value);
    with_(this._owner.atomic(), () => {
      const shard = this._shard(base, true);
      if (evolution === null) shard.constant = true;
      else if (!shard.evolutions.includes(evolution)) {
        shard.evolutions.push(evolution);
        shard.evolutions.sort((a, b) => a - b);
      }
      if (stamp !== null) {
        const stamps = shard.creation_timestamps;
        // An evolution re-written with a different stamp: drop the stale one.
        for (const [old, evo] of dict_items(stamps)) {
          if (evo === evolution && old !== stamp) dict_del(stamps, old);
        }
        // Same-millisecond collision: keep the highest evolution.
        const prev = dict_has(stamps, stamp) ? stamps[stamp] : _ABSENT;
        if (prev === _ABSENT || _rank(evolution) > _rank(prev)) stamps[stamp] = evolution;
      }
      this._flush(base);
    });
  }

  /** Un-index a key that was just deleted, then persist (or drop) the shard. */
  remove(key) {
    if (is_index_key(key)) return;
    const [base] = split_global_id_attributes(key);
    const evolution = _key_evolution(key);
    with_(this._owner.atomic(), () => {
      const shard = this._shard(base);
      if (shard === null) return;
      if (evolution === null) shard.constant = false;
      else if (shard.evolutions.includes(evolution)) shard.evolutions.splice(shard.evolutions.indexOf(evolution), 1);
      const kept = {};
      for (const [ts, evo] of dict_items(shard.creation_timestamps)) if (evo !== evolution) kept[ts] = evo;
      shard.creation_timestamps = kept;
      if (!shard.constant && shard.evolutions.length === 0) this._drop_shard(base);
      else this._flush(base);
    });
  }

  /**
   * Forget the in-memory state of *base* (or of every base).
   *
   * The next query reloads the shard from the index pool; central memory
   * calls this when an index hit fails validation.
   */
  invalidate(base = null) {
    with_(this._owner.atomic(), () => {
      if (base === null) {
        this._shards = {};
        this._entries = {};
        this._missing.clear();
      } else {
        delete this._shards[base];
        delete this._entries[base];
        this._missing.delete(base);
      }
    });
  }

  /**
   * Delete every shard of the owner's current keys from the index pool.
   *
   * Used by ``pool.empty()``: shards are addressed per base, so the owner's
   * keys tell us which shards exist.
   */
  clear() {
    with_(this._owner.atomic(), () => {
      const bases = new Set();
      for (const k of this._owner._keys()) if (!is_index_key(k)) bases.add(split_global_id_attributes(k)[0]);
      for (const base of bases) this._drop_shard(base);
      this.invalidate();
    });
  }

  /**
   * Rebuild every shard from the owner's keys and records.
   *
   * Reads each record once (for its creation stamp). Replaces the shard
   * contents outright, so evolutions that no longer exist are dropped too.
   */
  rebuild() {
    with_(this._owner.atomic(), () => {
      const by_base = new Map();
      for (const key of this._owner._keys()) {
        if (is_index_key(key)) continue;
        const base = split_global_id_attributes(key)[0];
        if (!by_base.has(base)) by_base.set(base, []);
        by_base.get(base).push(key);
      }
      for (const [base, keys] of by_base) {
        this._shard(base, true); // load (to keep the shard entry's counter)
        const shard = this._shards[base];
        shard.evolutions = [];
        shard.constant = false;
        shard.creation_timestamps = {};
        for (const key of keys) {
          const evolution = _key_evolution(key);
          if (evolution === null) shard.constant = true;
          else shard.evolutions.push(evolution);
          const stamp = _record_creation_timestamp(this._owner._read(key));
          if (stamp !== null) {
            const prev = dict_has(shard.creation_timestamps, stamp) ? shard.creation_timestamps[stamp] : _ABSENT;
            if (prev === _ABSENT || _rank(evolution) > _rank(prev)) shard.creation_timestamps[stamp] = evolution;
          }
        }
        shard.evolutions.sort((a, b) => a - b);
        this._flush(base);
      }
    });
  }

  // ------------------------------------------------------------------
  // Shard storage
  // ------------------------------------------------------------------
  _new_shard(base) {
    return { base, evolutions: [], constant: false, creation_timestamps: {} };
  }

  /** Return the in-memory shard for *base*, loading it from the index pool on first use. */
  _shard(base, create = false) {
    return with_(this._owner.atomic(), () => {
      let shard = this._shards[base] ?? null;
      if (shard !== null) return shard;
      if (!this._missing.has(base)) {
        const loaded = this._load(base);
        if (loaded !== null) return loaded;
        this._missing.add(base);
      }
      if (!create) return null;
      this._missing.delete(base);
      shard = this._new_shard(base);
      this._shards[base] = shard;
      return shard;
    });
  }

  /** Load the latest persisted shard of *base*; delete straggler evolutions. */
  _load(base) {
    const { Record } = lazy("laila.policy.central.memory.record.record");

    const sid = this.shard_id(base);
    try {
      const pool = this.index_pool;
      const keys = sorted(pool._candidate_keys(sid), { key: (k) => _rank(_key_evolution(k)) });
      if (!keys.length) return null;
      const latest = keys[keys.length - 1];
      const raw = pool._read(latest);
      if (raw === null || raw === undefined) return null;
      const entry = Record._build_sync(raw).entry;
      const data = entry.data;
      if (!isdict(data)) return null;
      const shard = this._new_shard(base);
      shard.evolutions = sorted([...(dict_get(data, "evolutions", null) ?? [])].map((e) => Number(e)));
      shard.constant = Boolean(dict_get(data, "constant", false));
      shard.creation_timestamps = {};
      for (const [ts, evo] of dict_items(dict_get(data, "creation_timestamps", null) ?? {})) {
        shard.creation_timestamps[str(ts)] = evo === null || evo === undefined ? null : Number(evo);
      }
      this._shards[base] = shard;
      this._entries[base] = entry;
      for (const straggler of keys.slice(0, -1)) {
        try {
          pool._delete(straggler);
        } catch {
          // best effort cleanup
        }
      }
      return shard;
    } catch (exc) {
      // index is a cache: never let it break the caller
      _LOG.warning("pool index: could not load shard for %s: %s", base, exc);
      return null;
    }
  }

  /** Persist the in-memory shard of *base* as the next shard evolution. */
  _flush(base) {
    const { Entry } = lazy("laila.entry.entry");
    const { EntryState } = lazy("laila.entry.entry_state");
    const { Record } = lazy("laila.policy.central.memory.record.record");

    const shard = this._shards[base] ?? null;
    if (shard === null) return;
    const pool = this.index_pool;
    try {
      // Copy so an in-memory index pool (which stores a snapshot sharing
      // the payload object) never aliases the live shard dict.
      const payload = {
        base: shard.base,
        evolutions: [...shard.evolutions],
        constant: Boolean(shard.constant),
        creation_timestamps: { ...shard.creation_timestamps },
      };
      let entry = this._entries[base] ?? null;
      let previous_key;
      if (entry === null) {
        // First shard evolution: constructed with its payload so it is
        // not "locally modified" and lands as ``@evolution=0``.
        entry = Entry.contingent({
          uuid: _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(`pool_index:${this._owner.uuid}:${base}`),
          scopes: [_POOL_INDEX_SCOPE],
          evolution: 0,
          data: payload,
          state: EntryState.READY,
        });
        this._entries[base] = entry;
        previous_key = null;
      } else {
        previous_key = entry.global_id;
        entry.data = payload;
      }
      entry.bump_evolution_if_locally_modified();
      const record = new Record({ entry });
      const blob = record.serialize(pool.transformations);
      pool._write(entry.global_id, blob);
      entry.mark_memorized();
      if (previous_key !== null && previous_key !== entry.global_id) pool._delete(previous_key);
    } catch (exc) {
      _LOG.warning("pool index: could not persist shard for %s: %s", base, exc);
      this.invalidate(base);
    }
  }

  /** Delete every persisted evolution of the shard for *base* and forget it. */
  _drop_shard(base) {
    const pool = this.index_pool;
    try {
      for (const key of pool._candidate_keys(this.shard_id(base))) pool._delete(key);
    } catch (exc) {
      _LOG.warning("pool index: could not drop shard for %s: %s", base, exc);
    }
    delete this._shards[base];
    delete this._entries[base];
    this._missing.add(base);
  }
}

register("laila.data.schema.pool_index", {
  CREATION_TIMESTAMP_ATTRIBUTE,
  is_index_key,
  _record_creation_timestamp,
  _key_evolution,
  PoolIndex,
});
