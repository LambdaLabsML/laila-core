/**
 * Base schema for all LAILA storage-pool implementations.
 *
 * A *pool* is laila's name for a key-value store that persists serialized
 * ``Entry`` blobs. Pools are the leaves of the storage hierarchy that the
 * central memory subsystem routes ``memorize`` / ``remember`` / ``forget``
 * calls to. Concrete subclasses plug into different backends -- in-memory,
 * filesystem, S3, Redis, Postgres, SQLite, DuckDB, HDF5, GCS, Azure,
 * BackBlaze, Cloudflare, HuggingFace -- by overriding the small set of
 * "internal storage hooks" described below.
 *
 * Public surface (proxy-aware)
 * ----------------------------
 * - ``pool[key]`` -> stored blob, transparently falling back through the
 *   proxy chain on miss and caching the result on the way back.
 * - ``pool[key] = entry`` -> local-only write.
 * - ``delete pool[key]`` -> local-only delete.
 * - ``pool[manifest]`` -> ``PoolWrapper`` view scoped by a manifest (use
 *   ``pool.__getitem__(manifest)``: JS property keys are strings).
 * - ``pool.exists(key)`` / ``key in pool`` -> local-only existence check.
 * - ``pool.keys({as_generator})`` -> local-only key enumeration.
 * - ``pool.empty()`` -> local-only wipe.
 * - ``pool.sync()`` -> flush in-memory cache (raises if cacheless).
 * - ``cache.__le__(origin)`` (``cache <= origin``) -> bulk duplicate via
 *   central memory.
 *
 * Subclass contract (override these)
 * ----------------------------------
 * Synchronous: ``_read``, ``_write``, ``_delete``, ``_exists``, ``_keys``,
 * ``_empty``. Default implementations operate on the in-memory ``resource``
 * dict.
 *
 * Asynchronous: ``_read_async``, ``_write_async``, ``_delete_async``,
 * ``_exists_async``. Default implementations just call the sync hook on the
 * calling event loop -- subclasses with a real async client should override
 * these.
 *
 * Proxy chains
 * ------------
 * Pools can be wired into proxy chains via ``_proxy_to``. A read that misses
 * the local store falls through to the upstream pool, and a successful
 * upstream read is cached locally before being returned. This lets users
 * compose layered caches like
 *
 *     in_memory.__lshift__(hdf5).__lshift__(s3)      // in_memory << hdf5 << s3
 *
 * so that hot reads never leave the in-memory tier.
 */
import * as asyncio from "../../_compat/asyncio.js";
import { with_ } from "../../_compat/contextlib.js";
import { NotImplementedError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { NotImplemented, dict_del, dict_has, dict_keys, dict_set, dict_clear, getitem } from "../../_compat/pytypes.js";
import { CLIExempt } from "../../basics/definitions/cli_capable.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _POOL_SCOPE } from "../../macros/strings.js";
import { _LAILA_IDENTIFIABLE_DATA_CONTAINER } from "./data_container.js";
import { PoolIndex, is_index_key } from "./pool_index.js";

/**
 * Abstract base class for laila storage pools.
 *
 * A pool is the *map* flavour of ``_LAILA_IDENTIFIABLE_DATA_CONTAINER``:
 * keys are entry ``global_id`` strings.
 *
 * Implements the proxy-aware public read/write/delete/exists/keys API and
 * provides default in-memory hook implementations so simple pools can be
 * created by just inheriting and overriding nothing. Real backends override
 * the ``_read`` / ``_write`` / ``_delete`` / ``_keys`` / ``_exists`` /
 * ``_empty`` hooks (and, optionally, their ``_async`` counterparts).
 *
 * Attributes
 * ----------
 * resource : dict[str, Any]
 *     Default in-memory backing store. Concrete subclasses can repurpose this
 *     (e.g. an in-memory cache fronting a remote store) or leave it unused.
 * batch_accelerated : bool
 *     Whether the backend can handle batched writes more efficiently than
 *     individual ones. Consulted by central memory when choosing a write
 *     strategy.
 * transformations : TransformationSequence | None
 *     Optional pipeline of ``Transformation`` objects applied to every blob
 *     before write and reversed on read. Lets a pool opaquely add
 *     compression, encoding, or encryption on top of the storage backend.
 * index_enabled : bool
 *     Maintain the per-base evolution / creation-timestamp index (see
 *     ``data/schema/pool_index.js``) on every write and delete. Turn off for
 *     pools whose keys are never looked up by attribute (log sinks, ring
 *     buffers) to save the extra shard write per store.
 * index_pool : _LAILA_IDENTIFIABLE_POOL | None
 *     Where the index shards are stored. ``null`` (default) means this pool
 *     itself; point it at an in-memory pool when index writes on the data
 *     backend are too expensive.
 *
 * Index bookkeeping keys (``LAILA:POOL_INDEX:...``) are hidden from ``keys``
 * unless ``include_index=true`` is passed.
 */
export class _LAILA_IDENTIFIABLE_POOL extends _LAILA_IDENTIFIABLE_DATA_CONTAINER {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_POOL_SCOPE] }),
      _proxy_to: PrivateAttr({ default: null }),
      _index: PrivateAttr({ default: null }),
    });
    define_fields(this, {
      resource: ["dict[str, Any]", CLIExempt({ default_factory: () => ({}) })],
      batch_accelerated: ["bool", Field({ default: false })],
      transformations: [[TransformationSequence, "None"], CLIExempt({ default: null })],
      index_enabled: ["bool", Field({ default: true })],
      index_pool: ["Any | None", CLIExempt({ default: null })],
    });
  }

  /** Unique identifier for this pool. Alias for ``global_id``. */
  get pool_id() {
    return this.global_id;
  }

  // -------- Index --------
  /** This pool's ``PoolIndex`` (created on first access). */
  get index() {
    if (this._index === null || this._index === undefined) this._index = new PoolIndex(this);
    return this._index;
  }

  _index_record(key, value) {
    if (this.index_enabled && !is_index_key(key)) this.index.record(key, value);
  }

  _index_remove(key) {
    if (this.index_enabled && !is_index_key(key)) this.index.remove(key);
  }

  /**
   * Answer an attribute lookup from the index alone, or ``null``.
   *
   * @param {string} base_gid ``LAILA:ENTRY:<uuid>`` (no ``@`` suffix).
   * @param {Record<string, string>} attributes Search arguments of the
   *   reference: ``evolution`` (may be negative, ``-1`` = latest) and / or
   *   ``creation_timestamp``.
   * @returns {string|null} A storage key the index believes exists (central
   *   memory validates it), or ``null`` when the index cannot answer and the
   *   caller should fall back to scanning.
   */
  _resolve_indexed(base_gid, attributes) {
    if (!this.index_enabled) return null;
    const raw_evolution = attributes.evolution ?? null;
    const evolution = raw_evolution !== null ? parseInt(raw_evolution, 10) : null;
    const timestamp = attributes.creation_timestamp ?? null;
    if (timestamp !== null) return this.index.by_creation_timestamp(base_gid, timestamp, evolution);
    return this.index.nth(base_gid, evolution === null ? -1 : evolution);
  }

  // -------- Index-maintaining write / delete wrappers --------
  // Every writer (central memory, proxy write-back, ``pool[k] = v``) goes
  // through these so the index sees all mutations. Backends keep overriding
  // only the ``_write`` / ``_delete`` (``_async``) hooks.
  /** Store *value* under *key* locally and index it. */
  write(key, value) {
    this._write(key, value);
    this._index_record(key, value);
  }

  /** Delete *key* locally and un-index it. */
  delete(key) {
    this._delete(key);
    this._index_remove(key);
  }

  /** Async ``write`` (index maintenance runs on a worker thread). */
  async write_async(key, value) {
    await this._write_async(key, value);
    if (this.index_enabled && !is_index_key(key)) await asyncio.to_thread(() => this.index.record(key, value));
  }

  /** Async ``delete`` (index maintenance runs on a worker thread). */
  async delete_async(key) {
    await this._delete_async(key);
    if (this.index_enabled && !is_index_key(key)) await asyncio.to_thread(() => this.index.remove(key));
  }

  // -------- Proxy properties --------
  /**
   * Write-only property used to wire proxy relationships.
   *
   * Always returns ``null`` -- the property exists so that the
   * natural-looking assignment ``origin.proxy = cache`` can be used to
   * express "make ``cache`` a proxy for ``origin``" (i.e. set
   * ``cache._proxy_to = origin``). Use ``proxy_to`` if you want to read the
   * relationship.
   */
  get proxy() {
    return null;
  }

  set proxy(pool) {
    if (pool !== null && pool !== undefined) pool._proxy_to = this;
  }

  /** The origin pool this one is a cache/proxy for, or ``null``. */
  get proxy_to() {
    return this._proxy_to;
  }

  set proxy_to(pool) {
    this._proxy_to = pool;
  }

  /**
   * ``cache << origin``: install ``cache`` as a proxy for ``origin``.
   *
   * Returns *other* (the origin) so the operator chains right-to-left,
   * letting expressions like ``mem << hdf5 << s3`` build a multi-tier cache
   * where ``mem`` fronts ``hdf5`` which fronts ``s3``.
   */
  __lshift__(other) {
    other.proxy = this;
    return other;
  }

  /**
   * ``origin >> cache``: install ``cache`` as a proxy for ``origin``.
   *
   * Returns *other* (the cache) so the operator chains left-to-right,
   * letting expressions like ``s3 >> hdf5 >> mem`` build a multi-tier cache
   * where ``mem`` fronts ``hdf5`` which fronts ``s3``.
   */
  __rshift__(other) {
    this.proxy = other;
    return other;
  }

  // -------- Internal storage hooks (override in subclasses) --------
  /**
   * Read *key* from this pool's own storage. Override in subclasses.
   *
   * The default implementation reads from the in-memory ``resource`` dict
   * under the pool's atomic lock and returns ``null`` for missing keys (so
   * the proxy-aware ``__getitem__`` can detect a miss and fall through to
   * the upstream pool).
   */
  _read(key) {
    const blob = with_(this.atomic(), () => {
      if (!dict_has(this.resource, key)) return null;
      return getitem(this.resource, key);
    });
    if (blob === null || blob === undefined) return null;
    return blob;
  }

  /**
   * Write *value* under *key* into this pool's own storage. Override in
   * subclasses.
   *
   * Default implementation writes into ``resource`` under the atomic lock.
   * Implementations should treat *value* as opaque bytes/dict (the central
   * memory layer has already serialised the entry).
   */
  _write(key, value) {
    with_(this.atomic(), () => {
      dict_set(this.resource, key, value);
    });
  }

  /**
   * Delete *key* from this pool's own storage. Override in subclasses.
   *
   * Default implementation removes the key from ``resource`` under the
   * atomic lock. Missing keys are silently tolerated.
   */
  _delete(key) {
    with_(this.atomic(), () => {
      if (dict_has(this.resource, key)) dict_del(this.resource, key);
    });
  }

  /**
   * Return ``true`` if *key* is present in this pool's own storage. Override
   * in subclasses.
   *
   * Default implementation checks ``resource`` under the atomic lock. Should
   * not consult the proxy chain (use the public ``exists`` if you want a
   * local-only view, which it is).
   */
  _exists(key) {
    return with_(this.atomic(), () => dict_has(this.resource, key));
  }

  /**
   * Enumerate this pool's own keys. Override in subclasses.
   *
   * @param {boolean} [as_generator=false] When ``false``, snapshots and
   *   returns a list under the atomic lock (cheap, O(N) memory). When
   *   ``true``, returns a generator that holds the lock while iterating --
   *   useful when keys are expensive to materialize but the caller wants to
   *   stream.
   * @returns {Iterable<string>}
   */
  _keys(as_generator = false) {
    if (!as_generator) {
      return with_(this.atomic(), () => dict_keys(this.resource));
    }
    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        for (const k of dict_keys(self.resource)) yield k;
      } finally {
        cm.__exit__(null, null, null);
      }
    }
    return _gen();
  }

  /**
   * Candidate keys for an attribute lookup (``remember("ENTRY:x@...")``).
   *
   * Central memory calls this *before* falling back to a full ``_keys`` scan
   * when it has to find "the evolutions of *base_gid*"
   * (``LAILA:ENTRY:<uuid>``, no ``@`` suffix) that satisfy *attributes*
   * (``{evolution: "3"}``, ``{creation_timestamp: "..."}`` ...).
   *
   * The default answers from this pool's ``index`` when it has a shard for
   * *base_gid* and returns ``null`` otherwise ("no index, scan"). Returning
   * a list -- even an empty one -- is authoritative and skips the scan;
   * central memory still validates the key it picks against the store.
   * @returns {string[]|null}
   */
  _search_keys(base_gid, _attributes) {
    if (!this.index_enabled) return null;
    return this.index.candidates(base_gid);
  }

  /**
   * Local-only list of keys that are evolutions of *base_gid*.
   *
   * Matches the exact key (a constant) and every ``base_gid@...`` key
   * (variables). Default implementation filters ``_keys``; listing-based
   * backends can push the prefix down.
   * @returns {string[]}
   */
  _candidate_keys(base_gid) {
    const prefix = `${base_gid}@`;
    return [...this._keys()].filter((k) => k === base_gid || k.startsWith(prefix));
  }

  /**
   * Wipe this pool's own storage. Override in subclasses.
   *
   * Default implementation clears ``resource`` under the atomic lock. Should
   * not propagate to the proxy chain.
   */
  _empty() {
    with_(this.atomic(), () => {
      dict_clear(this.resource);
    });
  }

  // -------- Default async hooks (override in async-capable pools) --------
  /**
   * Async read; default just delegates to the sync ``_read`` inline.
   *
   * Subclasses backed by a native-async client should override this to
   * ``await`` non-blocking I/O. The default implementation runs the sync
   * call on the calling loop, which blocks every other coroutine on that
   * loop for the read's duration -- correct but honest about the backend's
   * true (sync) nature.
   */
  async _read_async(key) {
    return this._read(key);
  }

  /** Async write; default delegates to sync ``_write`` (see ``_read_async``). */
  async _write_async(key, value) {
    this._write(key, value);
  }

  /** Async delete; default delegates to sync ``_delete`` (see ``_read_async``). */
  async _delete_async(key) {
    this._delete(key);
  }

  /** Async exists; default delegates to sync ``_exists`` (see ``_read_async``). */
  async _exists_async(key) {
    return this._exists(key);
  }

  /**
   * Async, proxy-aware read: the coroutine counterpart of ``__getitem__``.
   *
   * Reads from this pool's own async storage and, on a miss, falls through
   * the ``_proxy_to`` chain. A successful upstream read is cached back into
   * this pool (via ``_write_async``) before being returned, so later reads
   * bypass the upstream -- exactly the layered-cache semantics
   * ``mem << hdf5 << s3`` relies on.
   *
   * The synchronous ``__getitem__`` already does this walk; this method gives
   * the async fetch path (used by ``remember``) the same fall-through so a
   * read routed at the front of a proxy chain reaches the tier that actually
   * holds the data instead of reporting a miss.
   */
  async _read_through_async(key) {
    let value = await this._read_async(key);
    if (value !== null && value !== undefined) return value;

    if (this._proxy_to !== null && this._proxy_to !== undefined) {
      value = await this._proxy_to._read_through_async(key);
      if (value !== null && value !== undefined) {
        await this.write_async(key, value);
        return value;
      }
    }

    return null;
  }

  // -------- Proxy-aware public API --------
  /**
   * Retrieve the blob for *key*, with proxy fall-through and write-back.
   *
   * Two special cases:
   *
   * - If *key* is a ``Manifest``, the call short-circuits and returns a
   *   ``PoolWrapper`` view scoped by that manifest. That lets users write
   *   ``pool[manifest]["nested_key"]`` to read keys *as resolved through*
   *   the manifest.
   * - If the local read misses and ``_proxy_to`` is set, the request is
   *   forwarded to the upstream pool. On a successful upstream read, the
   *   value is *cached* into this pool before being returned, so subsequent
   *   reads bypass the upstream.
   */
  __getitem__(key) {
    const { Manifest } = lazy("laila.policy.central.memory.schema.manifest");

    if (key instanceof Manifest) {
      const { PoolWrapper } = lazy("laila.data.schema.pool_wrapper");

      return new PoolWrapper(this, key);
    }

    let value = this._read(key);
    if (value !== null && value !== undefined) return value;

    if (this._proxy_to !== null && this._proxy_to !== undefined) {
      value = this._proxy_to.__getitem__(key);
      if (value !== null && value !== undefined) {
        this.write(key, value);
        return value;
      }
    }

    return null;
  }

  /** Store *entry* under *key*. Local-only; never propagates to a proxy origin. */
  __setitem__(key, entry) {
    this.write(key, entry);
  }

  /** Delete the entry for *key*. Local-only; never propagates to a proxy origin. */
  __delitem__(key) {
    this.delete(key);
  }

  /**
   * Remove every entry from this pool. Local-only; never propagates to a
   * proxy origin.
   *
   * Also drops this pool's index shards, including those held by a separate
   * ``index_pool``.
   */
  empty() {
    if (this.index_enabled) this.index.clear();
    this._empty();
  }

  /** Return ``true`` if *key* is present in this pool. Local-only check. */
  exists(key) {
    return this._exists(key);
  }

  /** ``key in pool`` -- thin alias for ``exists``. */
  __contains__(key) {
    return this.exists(key);
  }

  /**
   * Return the keys stored in this pool. Local-only enumeration.
   *
   * @param {boolean|{as_generator?: boolean, include_index?: boolean}} [as_generator=false]
   *   If ``false``, return a snapshot list (cheap-ish, O(N) memory). If
   *   ``true``, return an iterator that holds the atomic lock for its full
   *   lifetime -- prefer this when keys are expensive to materialize but the
   *   consumer wants to stream.
   * @param {boolean} [include_index=false] Also yield the pool's own index
   *   bookkeeping keys (``LAILA:POOL_INDEX:...``). Hidden by default so
   *   consumers that enumerate a pool (``duplicate_pool``, manifests, user
   *   code) only see entries.
   * @returns {Iterable<string>} Pool keys (snapshot list or generator
   *   depending on *as_generator*).
   */
  keys(as_generator = false, include_index = false) {
    if (as_generator !== null && typeof as_generator === "object") {
      ({ as_generator = false, include_index = false } = as_generator);
    }
    const raw = this._keys(as_generator);
    if (include_index) return raw;
    if (as_generator) {
      return (function* () {
        for (const k of raw) if (!is_index_key(k)) yield k;
      })();
    }
    return [...raw].filter((k) => !is_index_key(k));
  }

  /**
   * Flush any in-memory write cache to the backing store.
   *
   * The base implementation always raises -- override in subclasses that
   * maintain a write-back cache (e.g. some HDF5 / DuckDB configurations).
   * Pools that write directly to storage on every set should leave the base
   * implementation in place; callers can then suppress
   * ``NotImplementedError`` when treating ``sync`` as a no-op.
   *
   * @throws {NotImplementedError} If the pool is cacheless and operates
   *   directly on storage.
   */
  sync() {
    throw new NotImplementedError(
      "Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.",
    );
  }

  /**
   * ``self <= other`` -- bulk-copy ``other`` pool's contents into this one.
   *
   * Delegates to ``central.memory._duplicate_pool`` on the active local
   * policy, so the copy goes through the same routing / manifest machinery
   * as a normal ``memorize`` workflow. *other* may be a pool instance or a
   * pool nickname/global-id string.
   */
  __le__(other) {
    const { active_policy } = lazy("laila");

    if (!(other instanceof _LAILA_IDENTIFIABLE_POOL || typeof other === "string")) return NotImplemented;

    return active_policy.central.memory._duplicate_pool(other, this);
  }
}

register("laila.data.schema.base", { _LAILA_IDENTIFIABLE_POOL });
