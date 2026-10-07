/**
 * Manifest -- a structured map of user-defined keys to ``global_id`` references.
 *
 * A Manifest is an ``Entry`` whose payload is a *blueprint*: a nested
 * dictionary whose leaves are ``global_id`` strings (or lists thereof).
 * A manifest is therefore a normal `READY` entry -- its payload is the
 * blueprint dict itself.  The ``manifest.realized`` property batch-fetches
 * every referenced entry through the active policy's central memory and
 * returns a nested dict of ``Entry`` objects mirroring the blueprint. The
 * fetch uses the memory's direct-await resolver (``_read_entries_async`` /
 * ``_read_entries_direct``): one task for the whole batch rather than one
 * future per child, released as soon as the entries have been collected.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import { PrivateAttr, SKIP_VALIDATION, define_private, normalize_kwargs } from "../../../../_compat/pydantic.js";
import { KeyError, RuntimeError, SqliteProgrammingError, TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import {
  NotImplemented,
  PyFloat,
  PyTuple,
  dict_copy,
  dict_get,
  dict_has,
  dict_items,
  dict_keys,
  dict_len,
  dict_values,
  getitem,
  is_bool,
  is_bytes,
  is_int,
  is_list,
  is_none,
  is_number,
  is_str,
  isdict,
  sorted,
  tuple,
  type_name,
} from "../../../../_compat/pytypes.js";
import { repr } from "../../../../_compat/pyrepr.js";
import { deepcopy } from "../../../../_compat/copy.js";
import { with_ } from "../../../../_compat/contextlib.js";
import { lazy, register as _register_module } from "../../../../_compat/lazy.js";
import { indexable } from "../../../../_compat/proxy.js";
import { RLock } from "../../../../_compat/threading.js";
import { uuid4 } from "../../../../_compat/uuid.js";
import { Entry } from "../../../../entry/entry.js";
import { ComputationalData } from "../../../../entry/compdata/index.js";
import { EntryState } from "../../../../entry/entry_state.js";
import { register_builder } from "../../../../entry/constitution/build_maps.js";
import { _MANIFEST_SCOPE } from "../../../../macros/strings.js";
import { EVOLUTION_ATTRIBUTE, _LAILA_IDENTIFIABLE_OBJECT, split_global_id_attributes } from "../../../../basics/definitions/identifiable_object.js";
import { CREATION_TIMESTAMP_ATTRIBUTE } from "../../../../data/schema/pool_index.js";
import { _CURRENT_SLOT } from "../../command/schema/parking.js";
import { GroupFuture } from "../../command/schema/future/future/group_future.js";

// ----------------------------------------------------------------------
// Non-memorizing algorithmic help: SQL index
// ----------------------------------------------------------------------
// The Manifest SQL index is a *non-memorizing algorithmic helper* -- a
// lightweight, temporary, query-side convenience that a manifest owns
// directly while it is being operated on.  It deliberately bypasses
// ``central.memory`` (see ``agentic/internal/memory.md`` for the sanctioned
// exemption): it is never memorized, never registered with a pool
// router, and never travels with the manifest on the wire.  It is a
// plain sqlite database file under ``<laila_root>/indices`` that can
// be cleared or invalidated at any time.

/** ``isinstance(val, (str, int, float, bool, bytes, type(None)))`` */
function _is_primitive(val) {
  return is_str(val) || is_number(val) || is_bool(val) || is_bytes(val) || is_none(val);
}
const _FORBIDDEN_SQL = ["ORDER BY", "LIMIT", "GROUP BY", "HAVING", "JOIN", "UNION"];
const _SQL_INDEX_TABLE = "manifest";
const _SQL_ROW_IDX_COL = "__row_idx__";
const _SQL_IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

const _SQL_META_TABLE = "__laila_index_meta__";

/** Python ``str(type(x))`` -> ``<class 'name'>``. */
function _type_str(x) {
  const ctor = x !== null && typeof x === "object" ? x.constructor : null;
  const mod = ctor && ctor.__module__;
  return `<class '${mod ? `${mod}.${ctor.name}` : type_name(x)}'>`;
}

/** ``s.strip(chars)`` */
function _strip_chars(s, chars) {
  let a = 0;
  let b = s.length;
  while (a < b && chars.includes(s[a])) a += 1;
  while (b > a && chars.includes(s[b - 1])) b -= 1;
  return s.slice(a, b);
}

/** Eagerly fetched query result with the cursor methods the index uses. */
export class _SqlResult {
  /** @param {any[]} rows */
  constructor(rows) {
    this._rows = rows;
  }

  fetchall() {
    const rows = this._rows;
    this._rows = [];
    return rows;
  }

  fetchone() {
    return this._rows.length ? this._rows.shift() : null;
  }

  *__iter__() {
    yield* this.fetchall();
  }

  [Symbol.iterator]() {
    return this.__iter__();
  }
}

/**
 * ``sqlite3`` parameter binding: Python ints bind as INTEGER (node:sqlite
 * binds every JS number as REAL, so integral numbers go through BigInt),
 * bools as 0/1, floats as REAL, ``None`` as NULL, str as TEXT, bytes as BLOB.
 */
function _bind_param(v) {
  if (is_none(v)) return null;
  if (is_bool(v)) return v ? 1n : 0n;
  if (typeof v === "bigint") return v;
  if (v instanceof PyFloat) return Number(v);
  if (is_int(v)) return BigInt(v);
  if (typeof v === "number") return v;
  if (is_str(v)) return v;
  if (is_bytes(v)) return v;
  throw new PyTypeError(`Error binding parameter: type '${type_name(v)}' is not supported`);
}

const _DML_RE = /^\s*(INSERT|UPDATE|DELETE|REPLACE)\b/i;

/**
 * Serialize every use of one sqlite connection behind an ``RLock``.
 *
 * The index connection can be used from any context (taskforce workers
 * included); every statement here runs *and* fetches under the lock, so
 * callers get a complete, consistent result regardless of caller.
 *
 * The connection emulates the ``sqlite3`` module's legacy transaction
 * control: a DML statement implicitly opens a transaction that
 * :meth:`commit` closes. Unknown attributes are forwarded to the
 * underlying ``DatabaseSync`` (``__getattr__``).
 */
/**
 * Whether anything other than whitespace follows the first ``;`` that
 * terminates a statement (string literals, quoted identifiers and comments
 * skipped), i.e. whether *sql* holds more than one statement.
 */
function _sql_statement_tail(sql) {
  const n = sql.length;
  let i = 0;
  while (i < n) {
    const c = sql[i];
    if (c === "'" || c === '"' || c === "`") {
      // literal / quoted identifier; doubled quote escapes
      i += 1;
      while (i < n) {
        if (sql[i] === c) {
          if (sql[i + 1] === c) {
            i += 2;
            continue;
          }
          break;
        }
        i += 1;
      }
      i += 1;
    } else if (c === "[") {
      const j = sql.indexOf("]", i + 1);
      i = j < 0 ? n : j + 1;
    } else if (c === "-" && sql[i + 1] === "-") {
      const j = sql.indexOf("\n", i + 2);
      i = j < 0 ? n : j + 1;
    } else if (c === "/" && sql[i + 1] === "*") {
      const j = sql.indexOf("*/", i + 2);
      i = j < 0 ? n : j + 2;
    } else if (c === ";") {
      return /\S/.test(_strip_sql_trivia(sql.slice(i + 1)));
    } else {
      i += 1;
    }
  }
  return false;
}

function _strip_sql_trivia(tail) {
  return tail.replace(/--[^\n]*/g, "").replace(/\/\*[\s\S]*?\*\//g, "").replace(/;/g, "");
}

export class _LockedConnection {
  /** @param {DatabaseSync} conn */
  constructor(conn) {
    this._conn = conn;
    this.lock = new RLock();
    return new Proxy(this, {
      get(t, prop, receiver) {
        if (typeof prop === "symbol" || prop in t) return Reflect.get(t, prop, receiver);
        const v = t._conn[prop];
        return typeof v === "function" ? v.bind(t._conn) : v;
      },
    });
  }

  _begin_if_needed(sql) {
    if (_DML_RE.test(sql) && !this._conn.isTransaction) this._conn.exec("BEGIN");
  }

  /**
   * ``sqlite3.Cursor.execute`` compiles one statement and raises
   * ``ProgrammingError`` when anything but whitespace/comments follows it;
   * ``node:sqlite``'s ``prepare`` silently drops that tail.
   */
  static _check_single_statement(sql) {
    if (_sql_statement_tail(sql)) throw new SqliteProgrammingError("You can only execute one statement at a time.");
  }

  /**
   * @param {string} sql
   * @param {Iterable<any>} [params]
   * @returns {_SqlResult}
   */
  execute(sql, params = []) {
    return with_(this.lock, () => {
      _LockedConnection._check_single_statement(sql);
      this._begin_if_needed(sql);
      const stmt = this._conn.prepare(sql);
      const rows = stmt.all(...[...params].map(_bind_param));
      return new _SqlResult(rows.map((r) => tuple(Object.values(r))));
    });
  }

  /**
   * @param {string} sql
   * @param {Iterable<Iterable<any>>} seq_of_params
   */
  executemany(sql, seq_of_params) {
    with_(this.lock, () => {
      this._begin_if_needed(sql);
      const stmt = this._conn.prepare(sql);
      for (const params of seq_of_params) stmt.run(...[...params].map(_bind_param));
    });
  }

  commit() {
    with_(this.lock, () => {
      if (this._conn.isTransaction) this._conn.exec("COMMIT");
    });
  }

  close() {
    with_(this.lock, () => {
      this._conn.close();
    });
  }
}

/**
 * In-memory handle for a manifest's non-memorizing SQL index.
 *
 * Holds the owned sqlite connection plus the data-side metadata needed
 * to map query results back to blueprint keys.  Stored on the manifest
 * as a ``PrivateAttr`` so it never affects the manifest's on-the-wire
 * serialization.
 */
export class _SqlState {
  /**
   * @param {{conn: _LockedConnection, db_path: string, is_persistent: boolean,
   *   table_name: string, columns: string[], indexed: Set<any>, row_keys: any[],
   *   stale?: boolean}} fields
   */
  constructor(fields) {
    const { conn, db_path, is_persistent, table_name, columns, indexed, row_keys, stale = false } = fields;
    this.conn = conn;
    this.db_path = db_path;
    this.is_persistent = is_persistent;
    this.table_name = table_name;
    this.columns = columns;
    this.indexed = indexed;
    this.row_keys = row_keys;
    this.stale = stale;
  }
}

// ``weakref.finalize`` emulation: a per-instance cleanup callback that runs
// when the manifest is garbage-collected or at process exit (``atexit=True``),
// and that can be detached.
const _finalizers = new Set();
const _registry = new FinalizationRegistry((fin) => fin._run());
let _exit_hooked = false;

class _Finalizer {
  constructor(obj, fn, ...args) {
    this._fn = fn;
    this._args = args;
    this._alive = true;
    _finalizers.add(this);
    _registry.register(obj, this, this);
    if (!_exit_hooked) {
      _exit_hooked = true;
      process.on("exit", () => {
        for (const f of [..._finalizers]) f._run();
      });
    }
  }

  get alive() {
    return this._alive;
  }

  _run() {
    if (!this._alive) return null;
    this._alive = false;
    _finalizers.delete(this);
    return this._fn(...this._args);
  }

  /** Run the callback now (``weakref.finalize.__call__``). */
  __call__() {
    return this._run();
  }

  /** Cancel without running; returns ``(obj, func, args, kwargs)``-like tuple or ``null``. */
  detach() {
    if (!this._alive) return null;
    this._alive = false;
    _finalizers.delete(this);
    _registry.unregister(this);
    return tuple([null, this._fn, this._args, {}]);
  }
}

/**
 * Entry subclass wrapping a nested dict of ``global_id`` references.
 *
 * A manifest maps user-defined string keys to ``global_id`` strings, lists
 * of ``global_id`` strings, or recursively nested dicts following the same
 * rules.  It can be constructed from raw ID strings or from ``Entry``
 * objects.
 *
 * The blueprint *is* the manifest's payload, so ``manifest.data`` returns
 * the nested mapping of ``global_id`` strings.  ``manifest.realized``
 * synchronously fetches every referenced entry via the active policy's
 * central memory and returns a nested dict of ``Entry`` objects mirroring
 * the blueprint structure.
 *
 * SQL filtering
 * -------------
 * ``manifest.sql("SELECT ... FROM ... WHERE ...")`` filters the blueprint via
 * a small sqlite-backed index built on demand.  This index is a
 * *non-memorizing algorithmic helper*: a lightweight, temporary,
 * query-side convenience that the manifest owns directly and that
 * deliberately bypasses ``central.memory`` (it is never memorized,
 * never routed, and never travels with the manifest).  Subclasses can
 * customise it by overriding :meth:`_sql_rows` and :meth:`_sql_project`.
 *
 * Constructor options
 * -------------------
 * ``data`` : dict, optional
 *     A nested dict whose leaves are ``global_id`` strings, lists of
 *     ``global_id`` strings, or ``Entry`` instances.  Entry instances are
 *     converted to a blueprint of ``global_id`` strings and stashed for
 *     a subsequent ``memorize()`` call.
 * ``blueprint`` : dict, optional
 *     Alias of ``data`` for explicitness.
 * ``uuid`` : str, optional
 *     Explicit UUID for the manifest's own identity.
 * ``nickname`` : str, optional
 *     Human-readable name converted to a deterministic UUID.
 * ``global_id`` : str, optional
 *     Composite identifier used to set identity.
 */
export class Manifest extends Entry {
  static _DEFAULT_SCOPES = [_MANIFEST_SCOPE];

  static {
    define_private(this, {
      _pending_entries: PrivateAttr({ default: null }),
      _sql_state: PrivateAttr({ default: null }),
      _sql_finalizer: PrivateAttr({ default: null }),
    });
  }

  /** @param {object|Map} [data] */
  constructor(data = {}) {
    if (data === SKIP_VALIDATION) {
      super(SKIP_VALIDATION);
      return indexable(this);
    }
    data = normalize_kwargs(data, new.target);
    const raw_data = Object.prototype.hasOwnProperty.call(data, "data") ? data.data : null;
    delete data.data;
    const blueprint_kwarg = Object.prototype.hasOwnProperty.call(data, "blueprint") ? data.blueprint : null;
    delete data.blueprint;

    let blueprint = null;
    let pending = null;

    const source = raw_data !== null && raw_data !== undefined ? raw_data : blueprint_kwarg;

    if (source !== null && source !== undefined) {
      const [has_entries, has_strings] = Manifest._classify_data(source);

      if (has_entries && has_strings) {
        throw new ValueError("Manifest data must contain either all Entry objects or all " + "global_id strings, not a mix of both.");
      }

      if (has_entries) {
        blueprint = Manifest._extract_blueprint(source);
        pending = [...Manifest._iter_entries(source)];
      } else {
        Manifest._validate_blueprint(source);
        blueprint = deepcopy(source);
      }
    }

    const entry_kwargs = { ...data };
    entry_kwargs.evolution = null;
    if (blueprint !== null) {
      entry_kwargs.data = blueprint;
      if (!Object.prototype.hasOwnProperty.call(entry_kwargs, "state") || entry_kwargs.state === undefined) {
        entry_kwargs.state = EntryState.READY;
      }
    }

    super(entry_kwargs);
    this._pending_entries = pending;
    // ``manifest[key]`` -> ``__getitem__``; ``gid in manifest`` -> ``__contains__``.
    return indexable(this);
  }

  // ------------------------------------------------------------------
  // Properties
  // ------------------------------------------------------------------

  /**
   * The nested dict with ``global_id`` strings as leaf values.
   *
   * Equivalent to ``manifest.data``.
   */
  get blueprint() {
    return this.data;
  }

  /**
   * Synchronously fetch all referenced entries and return them.
   *
   * Blocks until every leaf ``global_id`` has been materialised via the
   * active policy's central memory. Returns a nested dict of
   * ``Entry`` objects mirroring the blueprint structure. No caching --
   * each access re-fetches.
   *
   * @throws {RuntimeError} If the manifest has no blueprint.
   * @throws {KeyError} If any referenced entry is missing from the routed pool.
   */
  get realized() {
    const laila = lazy("laila");

    const bp = this.data;
    if (bp === null || bp === undefined) throw new RuntimeError("No blueprint to resolve — manifest is empty.");

    const all_gids = [...this];
    if (!all_gids.length) return Manifest._rebuild_with_entries(bp, {});

    const memory = laila.get_active_policy().central.memory;
    // Direct-await resolver: one task on the internal taskforce reads
    // every child concurrently, instead of one future per child.
    const ref = memory._read_entries_direct(all_gids);
    let results;
    try {
      results = ref.wait(null).data;
    } finally {
      // The manifest owns this future; release it from the bank once
      // consumed so a large realized() does not pin its children.
      ref.release();
    }
    results = [...results];

    if (results.length !== all_gids.length || results.some((r) => r === null || r === undefined)) {
      throw new KeyError("Manifest.realized: one or more referenced entries failed to resolve.");
    }

    const resolved_map = new Map(all_gids.map((gid, i) => [gid, results[i]]));
    return Manifest._rebuild_with_entries(bp, resolved_map);
  }

  /**
   * Awaitable that asynchronously resolves all referenced entries.
   *
   * Returns a promise so callers can ``await manifest.async_realized``
   * inside an async context (e.g. ``laila.guarantee_async``). Mirrors the
   * semantics of ``realized`` but awaits the underlying ``remember`` future
   * instead of blocking.
   *
   * @throws {RuntimeError} If the manifest has no blueprint.
   * @throws {KeyError} If any referenced entry is missing from the routed pool.
   */
  get async_realized() {
    const _resolve = async () => {
      const laila = lazy("laila");

      const bp = this.data;
      if (bp === null || bp === undefined) throw new RuntimeError("No blueprint to resolve — manifest is empty.");

      const all_gids = [...this];
      if (!all_gids.length) return Manifest._rebuild_with_entries(bp, {});

      const memory = laila.get_active_policy().central.memory;
      let results;
      if (_CURRENT_SLOT.get() !== null) {
        // Already running on a taskforce loop: await the pool
        // directly -- zero futures for the whole batch.
        results = await memory._read_entries_async(all_gids);
      } else {
        // Foreign loop (user's own async context): keep pool I/O on the
        // taskforce loops via a single submitted task.
        const ref = memory._read_entries_direct(all_gids);
        try {
          results = (await ref).data;
        } finally {
          ref.release();
        }
      }
      results = [...results];

      if (results.length !== all_gids.length || results.some((r) => r === null || r === undefined)) {
        throw new KeyError("Manifest.async_realized: one or more referenced entries failed to resolve.");
      }

      const resolved_map = new Map(all_gids.map((gid, i) => [gid, results[i]]));
      return Manifest._rebuild_with_entries(bp, resolved_map);
    };

    return _resolve();
  }

  // ------------------------------------------------------------------
  // Core operations (all return GroupFuture)
  // ------------------------------------------------------------------

  /**
   * Upload all referenced entries and store the manifest itself.
   *
   * Collects any pending ``Entry`` objects provided at construction time,
   * uploads them in batches, then stores the manifest itself (whose
   * payload is the blueprint dict).
   *
   * ``laila.memorize(manifest, {dst_pool})`` dispatches here, so the
   * two spellings are equivalent.
   *
   * @param {{pool?: any, pool_nickname?: string|null, pool_id?: string|null, batch_size?: number}} [opts]
   *   ``pool`` -- destination pool: a live pool, its ``global_id`` or a
   *   registered nickname (same forms as ``laila.memorize({dst_pool})``).
   *   ``pool_nickname`` / ``pool_id`` -- back-compat aliases for *pool*.
   *   ``batch_size`` -- number of pending entries per underlying
   *   ``laila.memorize`` call (default 128).
   */
  memorize(opts = {}) {
    const { pool = null, pool_nickname = null, pool_id = null, batch_size = 128 } = opts;
    const laila = lazy("laila");

    if (this.data === null || this.data === undefined) throw new RuntimeError("Nothing to memorize — manifest has no blueprint.");

    const routed = Manifest._resolve_pool(pool, pool_nickname, pool_id);
    const all_future_ids = [];
    const policy = laila.get_active_policy();

    if (this._pending_entries && this._pending_entries.length) {
      for (let i = 0; i < this._pending_entries.length; i += batch_size) {
        const batch = this._pending_entries.slice(i, i + batch_size);
        const ref = laila.memorize(batch, { dst_pool: routed });
        all_future_ids.push(...Manifest._collect_future_ids(ref));
      }
      this._pending_entries = null;
    }

    // Wrapped in a list on purpose: a *bare* Manifest handed to
    // ``laila.memorize`` dispatches back to this method, whereas a
    // list stores the manifest as a plain entry (blueprint payload).
    const self_ref = laila.memorize([this], { dst_pool: routed });
    all_future_ids.push(...Manifest._collect_future_ids(self_ref));

    return new GroupFuture({
      taskforce_id: policy.central.command.internal_taskforce,
      policy_id: policy.global_id,
      future_ids: all_future_ids,
    });
  }

  /**
   * Recall all referenced entries from the pool.
   *
   * ``laila.remember(manifest, {dst_pool})`` dispatches here. The
   * returned ``GroupFuture``'s ``.data`` is the flat list of entries in
   * ``[...manifest]`` order.
   *
   * @param {{pool?: any, pool_nickname?: string|null, pool_id?: string|null,
   *   batch_size?: number, persist?: boolean}} [opts]
   *   ``pool`` -- source pool (live pool, ``global_id`` or nickname).
   *   ``pool_nickname`` / ``pool_id`` -- back-compat aliases for *pool*.
   *   ``batch_size`` -- number of gids per underlying ``laila.remember``
   *   call (default 128).
   *   ``persist`` -- forwarded to ``laila.remember``: cache fetched entries
   *   into the alpha pool (default ``true``).
   */
  remember(opts = {}) {
    const { pool = null, pool_nickname = null, pool_id = null, batch_size = 128, persist = true } = opts;
    const laila = lazy("laila");

    if (this.data === null || this.data === undefined) throw new RuntimeError("No blueprint to resolve — manifest is empty.");

    const routed = Manifest._resolve_pool(pool, pool_nickname, pool_id);
    const all_gids = [...this];
    const all_future_ids = [];
    const policy = laila.get_active_policy();

    for (let i = 0; i < all_gids.length; i += batch_size) {
      const batch = all_gids.slice(i, i + batch_size);
      const ref = laila.remember(batch, { dst_pool: routed, persist });
      all_future_ids.push(...Manifest._collect_future_ids(ref));
    }

    return new GroupFuture({
      taskforce_id: policy.central.command.internal_taskforce,
      policy_id: policy.global_id,
      future_ids: all_future_ids,
    });
  }

  /**
   * Delete all referenced entries and the manifest itself from the pool.
   *
   * ``laila.forget(manifest, {pool})`` dispatches here.
   *
   * @param {{pool?: any, pool_nickname?: string|null, pool_id?: string|null, batch_size?: number}} [opts]
   *   ``pool`` -- pool to delete from (live pool, ``global_id`` or nickname).
   *   ``pool_nickname`` / ``pool_id`` -- back-compat aliases for *pool*.
   *   ``batch_size`` -- number of gids per underlying ``laila.forget`` call
   *   (default 128).
   */
  forget(opts = {}) {
    const { pool = null, pool_nickname = null, pool_id = null, batch_size = 128 } = opts;
    const laila = lazy("laila");

    if (this.data === null || this.data === undefined) throw new RuntimeError("No blueprint — nothing to forget.");

    const routed = Manifest._resolve_pool(pool, pool_nickname, pool_id);
    const all_gids = [...this];
    const all_future_ids = [];
    const policy = laila.get_active_policy();

    for (let i = 0; i < all_gids.length; i += batch_size) {
      const batch = all_gids.slice(i, i + batch_size);
      const ref = laila.forget(batch, { pool: routed });
      all_future_ids.push(...Manifest._collect_future_ids(ref));
    }

    const self_ref = laila.forget(this.global_id, { pool: routed });
    all_future_ids.push(...Manifest._collect_future_ids(self_ref));

    return new GroupFuture({
      taskforce_id: policy.central.command.internal_taskforce,
      policy_id: policy.global_id,
      future_ids: all_future_ids,
    });
  }

  // ------------------------------------------------------------------
  // SQL index  (non-memorizing algorithmic help)
  // ------------------------------------------------------------------

  /**
   * Filter the manifest with ``SELECT ... FROM ... [WHERE ...]``.
   *
   * A small, regex-based SQL surface over a *non-memorizing* sqlite
   * index that this manifest owns directly (it bypasses
   * ``central.memory`` entirely -- it is never memorized and never
   * leaves the local machine).  The index is built lazily on first
   * use and reused until :meth:`invalidate_index` /
   * :meth:`clear_index` is called or the blueprint mutates.
   *
   * Supported surface
   * ------------------
   * - ``SELECT <items> FROM <name> [WHERE <predicate>]`` only.
   * - ``ORDER BY`` / ``LIMIT`` / ``GROUP BY`` / ``HAVING`` / ``JOIN``
   *   / ``UNION`` are rejected up front with a ``ValueError``.
   * - ``==`` is normalized to ``=`` so casual queries work.
   * - The table name in ``FROM`` is treated as an alias and ignored
   *   (any identifier is accepted).
   * - ``SELECT`` items may be ``*``, ``<name>``, or ``<alias>.<name>``;
   *   the alias prefix is stripped.  Items beyond ``*`` are ignored
   *   by the default projection.
   * - String literals in ``WHERE`` must be single-quoted
   *   (``WHERE owner = 'alice'``).  A bareword right-hand side is
   *   interpreted by sqlite as a column name; if it can't be
   *   resolved the resulting error is re-raised as a ``ValueError``
   *   suggesting single quotes.
   *
   * @param {string} query
   * @returns {Manifest} A fresh, same-type manifest containing the matched
   *   rows. ``sql()`` never mutates ``this`` in place.
   */
  sql(query) {
    const [select_items, _from_alias, where] = Manifest._parse_sql(query);

    // The manifest's own (re-entrant) lock serializes build / rebuild /
    // query / clear across threads; the connection adds its own
    // statement-level lock underneath.
    const matched_keys = with_(this.atomic(), () => {
      if (this._sql_state === null || this._sql_state === undefined) {
        this.build_index();
      } else if (this._sql_state.stale) {
        this._sql_rebuild_in_place({ widen: true });
      }

      const state = this._sql_state;
      let sql_text = `SELECT "${_SQL_ROW_IDX_COL}" FROM "${state.table_name}"`;
      if (where) sql_text += ` WHERE ${where}`;

      let matched_idx;
      try {
        const cursor = state.conn.execute(sql_text);
        matched_idx = cursor.fetchall().map((row) => row[0]);
      } catch (exc) {
        if (exc && exc.code === "ERR_SQLITE_ERROR") {
          const message = String(exc.message);
          if (message.includes("no such column")) {
            const err = new ValueError(
              `${message}. If you meant a string literal, wrap it in single ` +
                "quotes (e.g. WHERE owner = 'alice'); unquoted barewords are " +
                "treated as column names by SQL.",
            );
            err.__cause__ = exc;
            throw err;
          }
        }
        throw exc;
      }

      return matched_idx.map((i) => state.row_keys[Number(i)]);
    });
    return this._sql_project(matched_keys, select_items);
  }

  /**
   * Materialize the manifest into a sqlite table + indexes.
   *
   * Idempotent: a subsequent call is a no-op while the cached index
   * is fresh, and triggers an in-place rebuild if the index was
   * invalidated.  Subsequent :meth:`sql` calls reuse the same
   * connection until :meth:`invalidate_index` is called or the
   * blueprint mutates.
   *
   * ``persist``, if provided, is the path to a file-backed sqlite db
   * that survives process restarts (and is attached without a
   * rebuild when it already holds a matching table); otherwise the
   * index lives under ``<laila_root>/indices/<manifest-uuid>/``.
   *
   * @param {{on?: Iterable<string>, composite?: Iterable<string[]>, persist?: string|null, widen?: boolean}} [opts]
   */
  build_index(opts = {}) {
    const { on = [], composite = [], persist = null, widen = true } = opts;
    with_(this.atomic(), () => {
      if (this._sql_state !== null && this._sql_state !== undefined) {
        if (this._sql_state.stale) this._sql_rebuild_in_place({ widen });
        return;
      }
      this._sql_build_fresh({ on, composite, persist });
    });
  }

  /**
   * Mark the cached index stale without closing the connection.
   *
   * Cheap: the next :meth:`sql` does an in-place rebuild
   * (``DELETE FROM ...`` then re-INSERT, indexes preserved).  Called
   * automatically by :meth:`extend` / ``__iadd__``; subclasses that mutate
   * their own blueprint should call this after writing.
   */
  invalidate_index() {
    if (this._sql_state !== null && this._sql_state !== undefined) this._sql_state.stale = true;
  }

  /**
   * Close the sqlite connection and drop the cached index.
   *
   * A non-persistent (temporary) index file is disposable and is
   * always unlinked here.  A user-chosen ``persist=`` file is left in
   * place unless ``remove_persisted`` is true.  After
   * ``clear_index()`` the next :meth:`sql` rebuilds from scratch.
   *
   * @param {{remove_persisted?: boolean}} [opts]
   */
  clear_index(opts = {}) {
    const { remove_persisted = false } = opts;
    with_(this.atomic(), () => {
      const finalizer = this._sql_finalizer;
      if (finalizer !== null && finalizer !== undefined) {
        finalizer.detach();
        this._sql_finalizer = null;
      }

      const state = this._sql_state;
      if (state === null || state === undefined) return;
      try {
        state.conn.close();
      } catch {
        // ignore
      }
      if (!state.is_persistent || remove_persisted) Manifest._sql_unlink_db(state.db_path);
      this._sql_state = null;
    });
  }

  // ----- Subclass hooks --------------------------------------------

  /**
   * Return ``[rows, columns]`` for the SQL index.
   *
   * Each row is ``[row_key, row_dict]``.  ``row_key`` uniquely
   * addresses the row inside the blueprint (any hashable; tuples
   * allowed).  ``row_dict`` is the flat column->value map used for
   * filtering.  ``columns`` is the union of queryable keys.
   *
   * Default implementation: one row per top-level key, using the
   * value as the row dict if it is a dict, otherwise wrapping it as
   * ``{"value": <scalar>}``.  Columns are the union of inner dict
   * keys (or ``{"value"}`` for scalar entries).
   *
   * IMPORTANT: ``row_dict`` values must be primitive scalars
   * (``str``, ``int``, ``float``, ``bool``, ``bytes``, ``None``).
   * ``_LAILA_IDENTIFIABLE_OBJECT`` instances and non-primitive
   * containers are rejected at :meth:`build_index` time.  If your
   * blueprint references Entries, pass ``.global_id`` (a ``str``)
   * instead; if it contains lists/dicts, flatten them here (e.g.
   * ``tags.join(",")`` or ``json.dumps(meta)``).
   *
   * The row order must be deterministic for a given blueprint so a
   * reattached on-disk index can remap rows back to keys without a
   * rebuild.
   *
   * @returns {[Array<[any, object]>, Set<string>]}
   */
  _sql_rows() {
    const blueprint = this.data ?? {};
    const rows = [];
    const columns = new Set();
    for (const [key, value] of dict_items(blueprint)) {
      let row;
      if (isdict(value)) row = dict_copy(value);
      else row = { value };
      rows.push(tuple([key, row]));
      for (const k of dict_keys(row)) columns.add(k);
    }
    return [rows, columns];
  }

  /**
   * Build a new same-type manifest from matched row keys.
   *
   * Default implementation keeps the top-level entries whose key
   * appears in ``matched_keys`` and ignores ``SELECT`` items beyond
   * ``*``.
   *
   * @param {any[]} matched_keys
   * @param {string[]} _select_items
   * @returns {Manifest}
   */
  _sql_project(matched_keys, _select_items) {
    const blueprint = this.data ?? {};
    const subset = {};
    for (const k of matched_keys) if (dict_has(blueprint, k)) subset[k] = deepcopy(getitem(blueprint, k));
    return new this.constructor({ blueprint: subset });
  }

  // ----- SQL parser helpers ----------------------------------------

  /**
   * Parse a tiny ``SELECT ... FROM ... [WHERE ...]`` query.
   *
   * Strips a trailing ``;``, normalizes ``==`` to ``=``, rejects
   * forbidden clauses, and returns ``[select_items, from_alias,
   * where_clause_or_null]``.
   *
   * @param {string} query
   * @returns {[string[], string, string|null]}
   */
  static _parse_sql(query) {
    if (!is_str(query)) throw new ValueError("sql() query must be a string.");

    const normalized = query.trim().replace(/;+$/, "").trim().replaceAll("==", "=");
    const upper = normalized.toUpperCase();
    for (const clause of _FORBIDDEN_SQL) {
      const pattern = new RegExp("\\b" + clause.replaceAll(" ", "\\s+") + "\\b");
      if (pattern.test(upper)) {
        throw new ValueError(`Unsupported SQL clause: ${clause}. Only ` + "'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.");
      }
    }

    const match = /^\s*SELECT\s+(?<items>.+?)\s+FROM\s+(?<from>[`"']?[A-Za-z_][A-Za-z0-9_]*[`"']?)\s*(?:WHERE\s+(?<where>.+))?$/is.exec(normalized);
    if (!match) throw new ValueError("Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.");

    const select_items = match.groups.items.split(",").map((item) => Manifest._strip_table_prefix(item.trim()));
    const from_alias = _strip_chars(match.groups.from, "`\"'");
    let where = match.groups.where ?? null;
    if (where !== null) where = where.trim();
    return [select_items, from_alias, where];
  }

  /**
   * Drop an ``alias.`` prefix and surrounding quotes from a SELECT item.
   * @param {string} item
   */
  static _strip_table_prefix(item) {
    let stripped = _strip_chars(item.trim(), '`"');
    if (stripped === "*") return stripped;
    if (stripped.includes(".")) stripped = stripped.slice(stripped.indexOf(".") + 1);
    return _strip_chars(stripped, '`"');
  }

  /**
   * Reject non-primitive row values before any sqlite state exists.
   * @param {Array<[any, object]>} rows
   */
  static _validate_primitive_rows(rows) {
    for (const [row_key, row_dict] of rows) {
      for (const [col, val] of dict_items(row_dict)) {
        if (val instanceof _LAILA_IDENTIFIABLE_OBJECT) {
          throw new PyTypeError(
            `Cannot index column ${repr(col)}: row ${repr(row_key)} has value ` +
              `of type ${val.constructor.name} (Laila identifiable object). ` +
              "Index columns must be primitive scalars " +
              "(str, int, float, bool, bytes, None). " +
              "Pass `.global_id` instead.",
          );
        }
        if (!_is_primitive(val)) {
          throw new PyTypeError(
            `Cannot index column ${repr(col)}: row ${repr(row_key)} has value ` +
              `of type ${type_name(val)}. Index columns must be ` +
              "primitive scalars. Flatten lists/dicts in your " +
              "_sql_rows() override (e.g. ','.join(tags) or " +
              "json.dumps(meta)).",
          );
        }
      }
    }
  }

  // ----- SQL index internals ---------------------------------------

  /**
   * Default on-disk path for this manifest's index file.
   *
   * One file per *instance* (``<pid>-<random>.laila_sqlitedb``) inside
   * the per-manifest ``indices/<uuid>/`` directory: two live manifests
   * that share a uuid (same nickname, or the same gid loaded twice)
   * must never attach to each other's temporary table.
   */
  _index_db_path() {
    const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

    const base = path.join(LAILA_DEFAULT_DIRECTORIES.indices, this.uuid);
    fs.mkdirSync(base, { recursive: true });
    return path.join(base, `${process.pid}-${uuid4().hex.slice(0, 8)}.laila_sqlitedb`);
  }

  /**
   * Build the index from scratch (or attach a matching on-disk file).
   * @param {{on: Iterable<string>, composite: Iterable<string[]>, persist: string|null}} opts
   */
  _sql_build_fresh(opts) {
    const { on, composite, persist } = opts;
    const [rows, raw_columns] = this._sql_rows();
    Manifest._validate_primitive_rows(rows);
    const columns = sorted(raw_columns);
    const row_keys = rows.map(([row_key]) => row_key);

    const is_persistent = persist !== null && persist !== undefined;
    let db_path;
    if (persist !== null && persist !== undefined) {
      db_path = persist === "~" || persist.startsWith("~/") ? path.join(os.homedir(), persist.slice(1)) : persist;
      const parent = path.dirname(db_path);
      if (parent) fs.mkdirSync(parent, { recursive: true });
    } else {
      db_path = this._index_db_path();
    }

    const conn = new _LockedConnection(new DatabaseSync(db_path));
    const fingerprint = Manifest._sql_fingerprint(columns, rows);

    if (Manifest._index_table_exists(conn) && Manifest._index_row_count(conn) === rows.length) {
      const existing_columns = Manifest._existing_columns(conn);
      const same_cols = existing_columns.length === new Set(existing_columns).size && new Set(existing_columns).size === new Set(columns).size && columns.every((c) => existing_columns.includes(c));
      if (same_cols && Manifest._read_fingerprint(conn) === fingerprint) {
        this._sql_state = new _SqlState({
          conn,
          db_path,
          is_persistent,
          table_name: _SQL_INDEX_TABLE,
          columns: existing_columns,
          indexed: Manifest._existing_indexes(conn),
          row_keys,
          stale: false,
        });
        this._sql_register_gc_cleanup();
        return;
      }
    }

    Manifest._create_and_populate(conn, columns, rows);
    Manifest._write_fingerprint(conn, fingerprint);
    const indexed = Manifest._apply_indexes(conn, on, composite);
    this._sql_state = new _SqlState({
      conn,
      db_path,
      is_persistent,
      table_name: _SQL_INDEX_TABLE,
      columns,
      indexed,
      row_keys,
      stale: false,
    });
    this._sql_register_gc_cleanup();
  }

  /**
   * Re-materialize rows into the existing table, preserving indexes.
   * @param {{widen?: boolean}} [opts]
   */
  _sql_rebuild_in_place(opts = {}) {
    const { widen = true } = opts;
    const state = this._sql_state;
    if (state === null || state === undefined) return;

    const [rows, raw_columns] = this._sql_rows();
    Manifest._validate_primitive_rows(rows);
    const columns = sorted(raw_columns);
    const conn = state.conn;

    const new_columns = columns.filter((c) => !state.columns.includes(c));
    if (new_columns.length) {
      if (!widen) throw new ValueError(`Index rebuild needs new columns ${repr(new_columns)} but widen=False.`);
      for (const col of new_columns) conn.execute(`ALTER TABLE "${state.table_name}" ADD COLUMN "${col}"`);
    }

    const all_columns = [...state.columns, ...new_columns];
    conn.execute(`DELETE FROM "${state.table_name}"`);
    Manifest._insert_rows(conn, state.table_name, all_columns, rows);
    Manifest._write_fingerprint(conn, Manifest._sql_fingerprint(columns, rows));
    conn.commit();

    state.columns = all_columns;
    state.row_keys = rows.map(([row_key]) => row_key);
    state.stale = false;
  }

  /**
   * Content hash of what the index table holds (columns + rows).
   *
   * Rows are validated to be primitive scalars before this runs, so
   * ``repr`` is a stable, order-preserving encoding.
   *
   * @param {string[]} columns
   * @param {Array<[any, object]>} rows
   */
  static _sql_fingerprint(columns, rows) {
    const payload = repr(tuple([[...columns], rows.map(([row_key, row]) => tuple([row_key, row]))]));
    return createHash("sha256").update(Buffer.from(payload, "utf-8")).digest("hex");
  }

  static _write_fingerprint(conn, fingerprint) {
    conn.execute(`CREATE TABLE IF NOT EXISTS "${_SQL_META_TABLE}" (key TEXT PRIMARY KEY, value TEXT)`);
    conn.execute(`INSERT OR REPLACE INTO "${_SQL_META_TABLE}" (key, value) VALUES (?, ?)`, ["fingerprint", fingerprint]);
    conn.commit();
  }

  static _read_fingerprint(conn) {
    let row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [_SQL_META_TABLE]).fetchone();
    if (row === null) return null;
    row = conn.execute(`SELECT value FROM "${_SQL_META_TABLE}" WHERE key = ?`, ["fingerprint"]).fetchone();
    return row === null ? null : row[0];
  }

  static _index_table_exists(conn) {
    const row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [_SQL_INDEX_TABLE]).fetchone();
    return row !== null;
  }

  static _index_row_count(conn) {
    return Number(conn.execute(`SELECT COUNT(*) FROM "${_SQL_INDEX_TABLE}"`).fetchone()[0]);
  }

  static _existing_columns(conn) {
    const rows = conn.execute(`PRAGMA table_info("${_SQL_INDEX_TABLE}")`).fetchall();
    return rows.filter((r) => r[1] !== _SQL_ROW_IDX_COL).map((r) => r[1]);
  }

  static _existing_indexes(conn) {
    const rows = conn.execute(`PRAGMA index_list("${_SQL_INDEX_TABLE}")`).fetchall();
    return new Set(rows.map((r) => r[1]));
  }

  static _create_and_populate(conn, columns, rows) {
    for (const col of columns) Manifest._validate_ident(col);
    const col_defs = columns.map((c) => `, "${c}"`).join("");
    conn.execute(`DROP TABLE IF EXISTS "${_SQL_INDEX_TABLE}"`);
    conn.execute(`CREATE TABLE "${_SQL_INDEX_TABLE}" ` + `("${_SQL_ROW_IDX_COL}" INTEGER PRIMARY KEY${col_defs})`);
    Manifest._insert_rows(conn, _SQL_INDEX_TABLE, columns, rows);
    conn.commit();
  }

  static _insert_rows(conn, table, columns, rows) {
    let statement;
    let data;
    if (columns.length) {
      const col_names = columns.map((c) => `"${c}"`).join(", ");
      const placeholders = Array(columns.length + 1)
        .fill("?")
        .join(", ");
      statement = `INSERT INTO "${table}" ("${_SQL_ROW_IDX_COL}", ${col_names}) VALUES (${placeholders})`;
      data = rows.map(([_row_key, row], idx) => [idx, ...columns.map((c) => dict_get(row, c))]);
    } else {
      statement = `INSERT INTO "${table}" ("${_SQL_ROW_IDX_COL}") VALUES (?)`;
      data = rows.map((_r, idx) => [idx]);
    }
    if (data.length) conn.executemany(statement, data);
  }

  static _apply_indexes(conn, on, composite) {
    const indexed = new Set();
    for (const col of on) {
      Manifest._validate_ident(col);
      conn.execute(`CREATE INDEX IF NOT EXISTS "idx_${_SQL_INDEX_TABLE}_${col}" ` + `ON "${_SQL_INDEX_TABLE}" ("${col}")`);
      indexed.add(col);
    }
    for (const tup of composite) {
      const cols = tuple(tup);
      for (const col of cols) Manifest._validate_ident(col);
      const name = "idx_" + _SQL_INDEX_TABLE + "_" + cols.join("_");
      const cols_sql = cols.map((c) => `"${c}"`).join(", ");
      conn.execute(`CREATE INDEX IF NOT EXISTS "${name}" ON "${_SQL_INDEX_TABLE}" (${cols_sql})`);
      indexed.add(cols);
    }
    conn.commit();
    return indexed;
  }

  static _validate_ident(name) {
    if (!is_str(name) || !_SQL_IDENT_RE.test(name)) throw new ValueError(`Invalid column identifier for indexing: ${repr(name)}`);
  }

  // ----- GC cleanup of temporary index files -----------------------

  /**
   * Ensure the manifest's temporary index file is nuked on GC.
   *
   * ``laila.forget`` cannot cover this -- the manifest object may
   * outlive (or live elsewhere than) the entry that was forgotten --
   * so a per-instance finalizer removes the temporary index file when
   * the manifest is garbage-collected (and at process exit).  Only
   * non-persistent (temporary) indexes are finalized; a user-chosen
   * ``persist=`` file is left untouched.
   */
  _sql_register_gc_cleanup() {
    const finalizer = this._sql_finalizer;
    if (finalizer !== null && finalizer !== undefined) {
      finalizer.detach();
      this._sql_finalizer = null;
    }

    const state = this._sql_state;
    if (state === null || state === undefined || state.is_persistent) return;

    this._sql_finalizer = new _Finalizer(this, Manifest._sql_finalize, state.conn, state.db_path);
  }

  /** Close the connection and delete a temporary index file. */
  static _sql_finalize(conn, db_path) {
    try {
      conn.close();
    } catch {
      // ignore
    }
    Manifest._sql_unlink_db(db_path);
  }

  /** Remove an index db file and its (now-empty) per-manifest dir. */
  static _sql_unlink_db(db_path) {
    if (!db_path) return;
    try {
      if (fs.existsSync(db_path)) fs.rmSync(db_path);
    } catch {
      // ignore
    }
    try {
      const parent = path.dirname(db_path);
      if (parent && fs.statSync(parent, { throwIfNoEntry: false })?.isDirectory() && !fs.readdirSync(parent).length) fs.rmdirSync(parent);
    } catch {
      // ignore
    }
  }

  // ------------------------------------------------------------------
  // Mapping-like API  (top-level blueprint keys for dict(manifest))
  // ------------------------------------------------------------------

  /** Top-level blueprint keys. */
  keys() {
    const bp = this.data;
    if (bp === null || bp === undefined) return [];
    return dict_keys(bp);
  }

  /** Top-level blueprint values. */
  values() {
    const bp = this.data;
    if (bp === null || bp === undefined) return [];
    return dict_values(bp);
  }

  /** Top-level blueprint items. */
  items() {
    const bp = this.data;
    if (bp === null || bp === undefined) return [];
    return dict_items(bp);
  }

  /**
   * Return a new ``Manifest`` containing only the specified top-level keys.
   * @param {string[]} keys
   */
  sub_manifest(keys) {
    const bp = this.data;
    if (bp === null || bp === undefined) throw new RuntimeError("Cannot create sub-manifest — manifest is empty.");
    const missing = keys.filter((k) => !dict_has(bp, k));
    if (missing.length) throw new KeyError(`Keys not found in manifest: ${repr(missing)}`);
    const subset = {};
    for (const k of keys) subset[k] = deepcopy(getitem(bp, k));
    return new Manifest({ blueprint: subset });
  }

  /**
   * Merge another manifest's blueprint into this one in-place.
   * @param {Manifest} other
   * @param {{overwrite?: boolean}} [opts]
   */
  extend(other, opts = {}) {
    const { overwrite = false } = opts;
    if (!(other instanceof Manifest)) throw new PyTypeError(`extend() requires a Manifest, got ${type_name(other)}`);
    const other_bp = other.data;
    if (other_bp === null || other_bp === undefined) return;

    const bp = this.data;
    let new_bp;
    if (bp === null || bp === undefined) {
      new_bp = deepcopy(other_bp);
    } else {
      if (!overwrite) {
        const other_keys = new Set(dict_keys(other_bp));
        const overlap = dict_keys(bp).filter((k) => other_keys.has(k));
        if (overlap.length) throw new KeyError(`Duplicate top-level keys: ${repr(sorted(overlap))}`);
      }
      new_bp = dict_copy(bp);
      for (const [k, v] of dict_items(deepcopy(other_bp))) new_bp[k] = v;
    }

    this._payload = new ComputationalData(new_bp);
    // Same bookkeeping as the ``data`` setter: the blueprint changed in
    // this process, so a later memorize of a variable manifest bumps.
    this._locally_modified = true;

    if (other._pending_entries && other._pending_entries.length) {
      if (this._pending_entries === null || this._pending_entries === undefined) this._pending_entries = [...other._pending_entries];
      else this._pending_entries.push(...other._pending_entries);
    }

    // The blueprint changed: mark any cached SQL index stale so the
    // next sql() rebuilds in place (non-memorizing algorithmic help).
    this.invalidate_index();
  }

  /** ``manifest += other`` -- merge *other* in-place and return this. */
  __iadd__(other) {
    if (!(other instanceof Manifest)) return NotImplemented;
    this.extend(other);
    return this;
  }

  /** ``manifest + other`` -- return a new manifest with merged blueprints. */
  __add__(other) {
    if (!(other instanceof Manifest)) return NotImplemented;

    const bp = this.data;
    const other_bp = other.data;
    if (bp !== null && bp !== undefined && other_bp !== null && other_bp !== undefined) {
      const other_keys = new Set(dict_keys(other_bp));
      const overlap = dict_keys(bp).filter((k) => other_keys.has(k));
      if (overlap.length) throw new KeyError(`Duplicate top-level keys: ${repr(sorted(overlap))}`);
    }

    const merged = {};
    if (bp !== null && bp !== undefined) for (const [k, v] of dict_items(deepcopy(bp))) merged[k] = v;
    if (other_bp !== null && other_bp !== undefined) for (const [k, v] of dict_items(deepcopy(other_bp))) merged[k] = v;

    if (!dict_len(merged)) return new Manifest();

    const result = new Manifest({ blueprint: merged });

    const pending = [];
    if (this._pending_entries && this._pending_entries.length) pending.push(...this._pending_entries);
    if (other._pending_entries && other._pending_entries.length) pending.push(...other._pending_entries);
    if (pending.length) result._pending_entries = pending;

    return result;
  }

  /** Look up a top-level key in the blueprint. */
  __getitem__(key) {
    const bp = this.data;
    if (bp === null || bp === undefined) throw new KeyError(key);
    return getitem(bp, key);
  }

  /** Number of top-level keys in the blueprint. */
  __len__() {
    const bp = this.data;
    if (bp === null || bp === undefined) return 0;
    return dict_len(bp);
  }

  // ------------------------------------------------------------------
  // Iteration / containment  (flattened global_id leaves)
  // ------------------------------------------------------------------

  /** Yield every ``global_id`` string via a depth-first, insertion-order walk. */
  *__iter__() {
    const bp = this.data;
    if (bp === null || bp === undefined) return;
    yield* Manifest._iter_global_ids(bp);
  }

  [Symbol.iterator]() {
    return this.__iter__();
  }

  /** Return ``true`` if *global_id* appears anywhere in the blueprint leaves. */
  __contains__(global_id) {
    if (!is_str(global_id)) return false;
    for (const gid of this) {
      if (gid === global_id) return true;
    }
    return false;
  }

  /**
   * Leaves that resolve at *read* time rather than naming one stored record.
   *
   * A leaf is *floating* when its reference carries a negative
   * ``evolution`` (``@evolution=-1`` = "whatever is latest") or a
   * ``creation_timestamp`` search argument. Such leaves make the
   * manifest's contents depend on the pool state at the moment of
   * ``remember``; pinned manifests (reproducible datasets) should
   * have none. Malformed leaves are ignored here.
   *
   * @returns {string[]}
   */
  floating_leaves() {
    const floating = [];
    for (const gid of this) {
      let attrs;
      try {
        [, attrs] = split_global_id_attributes(gid);
      } catch (e) {
        if (e instanceof ValueError) continue;
        throw e;
      }
      const evolution = dict_get(attrs, EVOLUTION_ATTRIBUTE, "");
      if (evolution.startsWith("-") || dict_has(attrs, CREATION_TIMESTAMP_ATTRIBUTE)) floating.push(gid);
    }
    return floating;
  }

  // ------------------------------------------------------------------
  // String representation
  // ------------------------------------------------------------------

  __str__() {
    return this.global_id;
  }

  __repr__() {
    let n = 0;
    for (const _ of this) n += 1;
    return `Manifest(${this.global_id}, entries=${n})`;
  }

  // ------------------------------------------------------------------
  // Static / class helpers
  // ------------------------------------------------------------------

  /**
   * Extract future IDs from a GroupFuture or a single future identity.
   *
   * The children are re-parented into the manifest-level
   * :class:`GroupFuture` returned to the caller, so an intermediate
   * per-batch group shell is released here (children untouched); the
   * caller releases the returned group, which releases the children.
   *
   * @returns {string[]}
   */
  static _collect_future_ids(ref) {
    if (ref === null || ref === undefined) return [];
    if (ref.future_ids !== undefined) {
        const ids = [...ref.future_ids];
      if (ref instanceof GroupFuture) ref.release({ children: false });
      return ids;
    }
    return [ref.global_id];
  }

  /**
   * Collapse ``pool`` / ``pool_id`` / ``pool_nickname`` into one routing value.
   *
   * Precedence mirrors the top-level shims: an explicit ``pool`` wins,
   * then ``pool_id``, then ``pool_nickname``. ``null`` means the alpha
   * (default) pool. The result is accepted as-is by
   * ``laila.memorize({dst_pool})`` / ``laila.remember({dst_pool})`` /
   * ``laila.forget({pool})``.
   */
  static _resolve_pool(pool = null, pool_nickname = null, pool_id = null) {
    if (pool !== null && pool !== undefined) return pool;
    if (pool_id !== null && pool_id !== undefined) return pool_id;
    return pool_nickname;
  }

  /**
   * Return ``[has_entries, has_strings]`` for leaf values in *data*.
   * @param {object|Map} data
   * @returns {[boolean, boolean]}
   */
  static _classify_data(data) {
    let has_entries = false;
    let has_strings = false;

    const _check = (val) => {
      if (val instanceof Entry) has_entries = true;
      else if (is_str(val)) has_strings = true;
      else if (is_list(val)) for (const item of val) _check(item);
      else if (isdict(val)) for (const v of dict_values(val)) _check(v);
    };

    for (const v of dict_values(data)) _check(v);

    return [has_entries, has_strings];
  }

  /**
   * Yield every ``Entry`` object from a nested dict.
   * @param {object|Map} data
   */
  static *_iter_entries(data) {
    for (const val of dict_values(data)) {
      if (val instanceof Entry) yield val;
      else if (is_list(val)) {
        for (const item of val) if (item instanceof Entry) yield item;
      } else if (isdict(val)) yield* Manifest._iter_entries(val);
    }
  }

  /** Recursively validate that *data* follows the blueprint schema. */
  static _validate_blueprint(data) {
    if (!isdict(data)) throw new ValueError("Blueprint must be a dict.");
    for (const [key, val] of dict_items(data)) {
      if (!is_str(key)) throw new ValueError(`Blueprint keys must be strings, got ${_type_str(key)}`);
      if (is_str(val)) continue;
      else if (is_list(val)) {
        for (const item of val) {
          if (!is_str(item)) throw new ValueError(`List values in blueprint must be strings, got ${_type_str(item)}`);
        }
      } else if (isdict(val)) Manifest._validate_blueprint(val);
      else throw new ValueError(`Blueprint values must be str, list[str], or dict — got ${_type_str(val)}`);
    }
  }

  /** Convert a dict of ``Entry`` objects to a blueprint of ``global_id`` strings. */
  static _extract_blueprint(data) {
    const result = {};
    for (const [key, val] of dict_items(data)) {
      if (val instanceof Entry) result[key] = val.global_id;
      else if (is_str(val)) result[key] = val;
      else if (is_list(val)) result[key] = val.map((item) => (item instanceof Entry ? item.global_id : item));
      else if (isdict(val)) result[key] = Manifest._extract_blueprint(val);
      else throw new ValueError(`Unexpected value type in manifest data: ${_type_str(val)}`);
    }
    return result;
  }

  /** Depth-first, insertion-order walk yielding every leaf ``global_id``. */
  static *_iter_global_ids(blueprint) {
    for (const val of dict_values(blueprint)) {
      if (is_str(val)) yield val;
      else if (is_list(val)) yield* val;
      else if (isdict(val)) yield* Manifest._iter_global_ids(val);
    }
  }

  /**
   * Reconstruct the nested structure replacing ``global_id`` strings with entries.
   * @param {object|Map} blueprint
   * @param {Map<string, any>|object} resolved_map
   */
  static _rebuild_with_entries(blueprint, resolved_map) {
    const result = {};
    for (const [key, val] of dict_items(blueprint)) {
      if (is_str(val)) result[key] = getitem(resolved_map, val);
      else if (is_list(val)) result[key] = val.map((gid) => getitem(resolved_map, gid));
      else if (isdict(val)) result[key] = Manifest._rebuild_with_entries(val, resolved_map);
    }
    return result;
  }
}

export { _SQL_INDEX_TABLE, _SQL_ROW_IDX_COL, _SQL_META_TABLE, _FORBIDDEN_SQL };

register_builder(_MANIFEST_SCOPE, Manifest._build_from_dict_sync.bind(Manifest), Manifest._build_from_dict_async.bind(Manifest));

_register_module("laila.policy.central.memory.schema.manifest", { Manifest, _SqlState, _LockedConnection, _SqlResult });
