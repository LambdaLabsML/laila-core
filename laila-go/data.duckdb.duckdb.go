package laila

import (
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/duckdb/duckdb.py -- DuckDB pool implementation.

// Duckdb_connection is the slice of ``duckdb.DuckDBPyConnection`` the
// pool uses: ``execute(sql, params)`` returning a cursor-like result, and
// ``close()``.
type Duckdb_connection interface {
	Execute(sql string, params ...any) (Sql_rows_, error)
	Close() error
}

// Duckdb_driver is the ``duckdb`` module: ``duckdb.connect(path)``.
// Registered under ``"duckdb"`` by ``drivers/duckdb``.
type Duckdb_driver interface {
	Connect(file_path string) (Duckdb_connection, error)
}

// duckdb_ is the module global (nil when the extra is missing).
func duckdb_() Duckdb_driver {
	d, _ := Driver_("duckdb").(Duckdb_driver)
	return d
}

// Space reclaim. DuckDB only frees the blocks of deleted rows at a
// CHECKPOINT, and even then only for row groups that are *entirely*
// dead: a single long-lived row (a config constant, an index record)
// pins its row group and the deleted neighbours are kept forever, so a
// write/delete churn grows the file and the buffer pool without bound.
// The pool therefore accounts deleted bytes/rows (``DELETE ... RETURNING
// length(value)``) and, past either threshold, arms a deferred
// maintenance step that runs before the next data write: ``CHECKPOINT``,
// then -- when the deleted volume is comparable to what is still live --
// a transactional table rewrite (Compact_) that leaves only live rows,
// followed by another CHECKPOINT.
const (
	DUCKDB_CHECKPOINT_CHURN_BYTES = 8 << 20
	DUCKDB_CHECKPOINT_CHURN_ROWS  = 4096
	// Compact when deleted rows >= live rows, or when deleted bytes >= this
	// floor *and* >= live bytes (the live-bytes scan runs only past the
	// floor).
	DUCKDB_COMPACT_MIN_BYTES = 64 << 20
)

// DuckDBPool is the DuckDB-backed pool storing entries in a local DuckDB
// database file.
//
//   - file_path: database file; defaults to a per-pool file under the
//     laila pools directory.
//   - memory_limit: value for DuckDB's ``memory_limit`` setting (e.g.
//     ``"512MB"``). nil keeps the engine default (80 % of RAM). DuckDB
//     treats the limit as hard: exceeding it raises a *fatal* error that
//     invalidates the connection, so only set it for workloads whose
//     working set you know.
type DuckDBPool struct {
	LAILA_IDENTIFIABLE_POOL_

	File_path    *string `py:"file_path"`
	Memory_limit *string `py:"memory_limit"`

	Conn_               Duckdb_connection `py:"_conn"`
	Churn_bytes_        int               `py:"_churn_bytes"`
	Churn_rows_         int               `py:"_churn_rows"`
	Checkpoint_pending_ bool              `py:"_checkpoint_pending"`
}

// DuckDBPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64).
var DuckDBPool_class = py.NewClass("DuckDBPool", (*DuckDBPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_DuckDBPool is ``DuckDBPool(**data)``.
func New_DuckDBPool(kw ...py.Kw) (*DuckDBPool, error) {
	p := DuckDBPool_class.New().(*DuckDBPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Model_post_init initialises the DuckDB connection and creates the
// entries table.
func (p *DuckDBPool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	if duckdb_() == nil {
		return py.ImportError.New("duckdb is required for DuckDBPool")
	}

	if p.File_path == nil {
		pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
		if err := py.Mkdir_p(pool_dir); err != nil {
			return err
		}
		fp := filepath.Join(pool_dir, "pool.duckdb")
		p.File_path = &fp
	} else {
		fp := py.Expanduser(*p.File_path)
		p.File_path = &fp
		if parent := filepath.Dir(fp); parent != "" && parent != "." {
			if err := py.Mkdir_p(parent); err != nil {
				return err
			}
		}
	}

	conn, err := duckdb_().Connect(*p.File_path)
	if err != nil {
		return err
	}
	p.Conn_ = conn
	if p.Memory_limit != nil && *p.Memory_limit != "" {
		if _, err := conn.Execute("SET memory_limit='" + *p.Memory_limit + "'"); err != nil {
			return err
		}
	}
	rows, err := conn.Execute(
		"SELECT COUNT(*) FROM information_schema.columns " +
			"WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'")
	if err != nil {
		return err
	}
	row, err := rows.Fetchone()
	if err != nil {
		return err
	}
	if row != nil {
		if n, err := py.Int(row[0]); err == nil && n > 0 {
			if _, err := conn.Execute("DROP TABLE laila_pool_entries"); err != nil {
				return err
			}
		}
	}
	_, err = conn.Execute(`
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key VARCHAR NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            `)
	return err
}

// Connection_ returns the active DuckDB connection; RuntimeError once
// closed.
func (p *DuckDBPool) Connection_() (Duckdb_connection, error) {
	if p.Conn_ == nil {
		return nil, py.RuntimeError.New("DuckDBPool is closed.")
	}
	return p.Conn_, nil
}

// Close closes the DuckDB connection.
//
// Runs under the pool lock so an in-flight write finishes before the
// connection goes away; later callers hit the documented
// ``RuntimeError("... is closed")`` from Connection_.
func (p *DuckDBPool) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.Conn_ != nil {
		if p.Checkpoint_pending_ {
			p.Run_pending_checkpoint_()
		}
		err := p.Conn_.Close()
		p.Conn_ = nil
		return err
	}
	return nil
}

// Checkpoint checkpoints the database now; with *compact* also rewrites
// the table.
//
// A plain ``CHECKPOINT`` frees the blocks of row groups that are entirely
// deleted. ``compact=True`` additionally rewrites ``laila_pool_entries``
// with only its live rows (a transactional ``CREATE TABLE ... INSERT ...
// DROP ... RENAME``), which is the only way DuckDB gives back the space
// of deleted rows that share a row group with live ones.
func (p *DuckDBPool) Checkpoint(kw ...py.Kw) error {
	compact := py.Truthy(py.MergeKw(kw...).Get("compact", false))
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("CHECKPOINT"); err != nil {
		return err
	}
	if compact {
		if err := p.Compact_(); err != nil {
			return err
		}
	}
	p.Churn_bytes_ = 0
	p.Churn_rows_ = 0
	p.Checkpoint_pending_ = false
	return nil
}

// Note_churn_ accounts for deleted data; arms deferred maintenance past
// the thresholds (``_note_churn(nbytes, rows=1)``).
func (p *DuckDBPool) Note_churn_(nbytes int, rows ...int) {
	n := 1
	if len(rows) > 0 {
		n = rows[0]
	}
	if nbytes > 0 {
		p.Churn_bytes_ += nbytes
	}
	p.Churn_rows_ += n
	if p.Churn_bytes_ >= DUCKDB_CHECKPOINT_CHURN_BYTES || p.Churn_rows_ >= DUCKDB_CHECKPOINT_CHURN_ROWS {
		p.Checkpoint_pending_ = true
	}
}

// Should_compact_ is "deleted volume comparable to the live volume"
// (rows, or bytes past the floor).
func (p *DuckDBPool) Should_compact_() (bool, error) {
	conn, err := p.Connection_()
	if err != nil {
		return false, err
	}
	live_rows, err := scalar_int_(conn.Execute("SELECT count(*) FROM laila_pool_entries"))
	if err != nil {
		return false, err
	}
	if p.Churn_rows_ >= max(live_rows, 1) {
		return true, nil
	}
	if p.Churn_bytes_ < DUCKDB_COMPACT_MIN_BYTES {
		return false, nil
	}
	live_bytes, err := scalar_int_(conn.Execute(
		"SELECT coalesce(sum(length(value)), 0) FROM laila_pool_entries"))
	if err != nil {
		return false, err
	}
	return p.Churn_bytes_ >= live_bytes, nil
}

// scalar_int_ is ``int(conn.execute(sql).fetchone()[0])``.
func scalar_int_(rows Sql_rows_, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	row, err := rows.Fetchone()
	if err != nil {
		return 0, err
	}
	if row == nil || len(row) == 0 {
		return 0, nil
	}
	return py.Int(row[0])
}

// Compact_ rewrites the entries table with its live rows only (caller
// holds the lock).
func (p *DuckDBPool) Compact_() error {
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("BEGIN"); err != nil {
		return err
	}
	run := func() error {
		for _, sql := range []string{
			"CREATE TABLE laila_pool_entries_compact (" +
				"key VARCHAR NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
			"INSERT INTO laila_pool_entries_compact SELECT key, value FROM laila_pool_entries",
			"DROP TABLE laila_pool_entries",
			"ALTER TABLE laila_pool_entries_compact RENAME TO laila_pool_entries",
			"COMMIT",
		} {
			if _, err := conn.Execute(sql); err != nil {
				return err
			}
		}
		return nil
	}
	if err := run(); err != nil {
		_, _ = conn.Execute("ROLLBACK")
		return err
	}
	_, err = conn.Execute("CHECKPOINT")
	return err
}

// Run_pending_checkpoint_ runs the deferred maintenance (caller holds the
// lock). Never fails the write.
func (p *DuckDBPool) Run_pending_checkpoint_() {
	if !p.Checkpoint_pending_ {
		return
	}
	conn, err := p.Connection_()
	if err != nil {
		return
	}
	if _, err := conn.Execute("CHECKPOINT"); err != nil {
		return
	}
	compact, err := p.Should_compact_()
	if err != nil {
		return
	}
	if compact {
		if err := p.Compact_(); err != nil {
			return
		}
	}
	p.Churn_bytes_ = 0
	p.Churn_rows_ = 0
	p.Checkpoint_pending_ = false
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *DuckDBPool) Read_(key string) (any, error) {
	p.Lock()
	conn, err := p.Connection_()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	rows, err := conn.Execute("SELECT value FROM laila_pool_entries WHERE key = ?", key)
	if err != nil {
		p.Unlock()
		return nil, err
	}
	row, err := rows.Fetchone()
	p.Unlock()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return py.Json_loads(py.Str(row[0]))
}

// Write_ inserts or updates *entry* under *key*.
func (p *DuckDBPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "DuckDBPool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	p.Lock()
	defer p.Unlock()
	// Index-shard writes are interleaved with the deletes that arm the
	// checkpoint; running it there (mid delete phase) does not let DuckDB
	// reuse the freed blocks. Run it on the next *data* write.
	if !Is_index_key(key) {
		p.Run_pending_checkpoint_()
	}
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	_, err = conn.Execute(`
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `, key, value)
	return err
}

// Delete_ deletes the row for *key* (its size feeds the
// deferred-checkpoint accounting).
func (p *DuckDBPool) Delete_(key string) error {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	rows, err := conn.Execute("DELETE FROM laila_pool_entries WHERE key = ? RETURNING length(value)", key)
	if err != nil {
		return err
	}
	row, err := rows.Fetchone()
	if err != nil {
		return err
	}
	if row != nil {
		n, _ := py.Int(row[0])
		p.Note_churn_(n)
	}
	return nil
}

// Empty_ removes all entries from the pool and checkpoints so the space
// is reusable.
func (p *DuckDBPool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("DELETE FROM laila_pool_entries"); err != nil {
		return err
	}
	if _, err := conn.Execute("CHECKPOINT"); err != nil {
		p.Checkpoint_pending_ = true
		return nil
	}
	p.Churn_bytes_ = 0
	p.Churn_rows_ = 0
	p.Checkpoint_pending_ = false
	return nil
}

// Exists_ returns True if *key* is present.
func (p *DuckDBPool) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return false, err
	}
	rows, err := conn.Execute("SELECT 1 FROM laila_pool_entries WHERE key = ?", key)
	if err != nil {
		return false, err
	}
	row, err := rows.Fetchone()
	if err != nil {
		return false, err
	}
	return row != nil, nil
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *DuckDBPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the pool; ``as_generator=True`` yields a lazy
// Iterator instead of a list.
func (p *DuckDBPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	fetch := func() ([]string, error) {
		p.Lock()
		defer p.Unlock()
		conn, err := p.Connection_()
		if err != nil {
			return nil, err
		}
		rows, err := conn.Execute("SELECT key FROM laila_pool_entries ORDER BY key")
		if err != nil {
			return nil, err
		}
		all, err := rows.Fetchall()
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(all))
		for _, row := range all {
			out = append(out, py.Str(row[0]))
		}
		return out, nil
	}
	return keys_list_or_generator_(as_generator, fetch)
}
