package laila

import (
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/sqlite/sqlite.py -- SQLite pool implementation.

// SQLitePool is the SQLite-backed pool storing entries in a local database
// file.
//
// Fields: ``file_path`` (optional; defaults to
// ``<pools>/<uuid>/pool.laila_sqlitedb``) and ``transformations``
// (defaults to Transformation_base64). ``_persistent_storage`` is True.
type SQLitePool struct {
	LAILA_IDENTIFIABLE_POOL_

	File_path *string `py:"file_path"`

	Conn_ *py.Sqlite3Connection `py:"_conn"`
}

// SQLitePool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64,
// ``arbitrary_types_allowed``).
var SQLitePool_class = py.NewClass("SQLitePool", (*SQLitePool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_SQLitePool is ``SQLitePool(**data)``.
func New_SQLitePool(kw ...py.Kw) (*SQLitePool, error) {
	p := SQLitePool_class.New().(*SQLitePool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Sqlite_pool_base_ exposes the embedded SQLitePool to subclasses.
func (p *SQLitePool) Sqlite_pool_base_() *SQLitePool { return p }

// Model_post_init opens the SQLite database and creates the entries
// table.
func (p *SQLitePool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	if p.File_path == nil {
		pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
		if err := py.Mkdir_p(pool_dir); err != nil {
			return err
		}
		fp := filepath.Join(pool_dir, "pool.laila_sqlitedb")
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

	conn, err := py.Sqlite3_connect(*p.File_path)
	if err != nil {
		return err
	}
	p.Conn_ = conn
	cursor, err := conn.Execute(
		"SELECT COUNT(*) FROM pragma_table_info('laila_pool_entries') WHERE name='pool_id'")
	if err != nil {
		return err
	}
	if row := cursor.Fetchone(); row != nil {
		if n, err := py.Int(row[0]); err == nil && n > 0 {
			if _, err := conn.Execute("DROP TABLE laila_pool_entries"); err != nil {
				return err
			}
		}
	}
	if _, err := conn.Execute(`
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            `); err != nil {
		return err
	}
	return conn.Commit()
}

// Connection_ returns the active SQLite connection; RuntimeError once
// closed.
func (p *SQLitePool) Connection_() (*py.Sqlite3Connection, error) {
	if p.Conn_ == nil {
		return nil, py.RuntimeError.New("SQLitePool is closed.")
	}
	return p.Conn_, nil
}

// Close closes the SQLite connection.
//
// Runs under the pool lock so an in-flight write finishes before the
// connection goes away; later callers hit the documented
// ``RuntimeError("... is closed")`` from Connection_.
func (p *SQLitePool) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.Conn_ != nil {
		err := p.Conn_.Close()
		p.Conn_ = nil
		return err
	}
	return nil
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *SQLitePool) Read_(key string) (any, error) {
	p.Lock()
	conn, err := p.Connection_()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	cursor, err := conn.Execute("SELECT value FROM laila_pool_entries WHERE key = ?", key)
	p.Unlock()
	if err != nil {
		return nil, err
	}
	row := cursor.Fetchone()
	if row == nil {
		return nil, nil
	}
	return py.Json_loads(py.Str(row[0]))
}

// Write_ inserts or updates *entry* under *key*. TypeError unless the
// value is a dict (JSON-encoded here) or a serialized JSON string.
func (p *SQLitePool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "SQLitePool expects a serialized JSON string.")
	if err != nil {
		return err
	}

	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute(`
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `, key, value); err != nil {
		return err
	}
	return conn.Commit()
}

// Delete_ deletes the row for *key*.
func (p *SQLitePool) Delete_(key string) error {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("DELETE FROM laila_pool_entries WHERE key = ?", key); err != nil {
		return err
	}
	return conn.Commit()
}

// Empty_ removes all entries from the pool.
func (p *SQLitePool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("DELETE FROM laila_pool_entries"); err != nil {
		return err
	}
	return conn.Commit()
}

// Exists_ returns true if *key* is present in the table.
func (p *SQLitePool) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return false, err
	}
	cursor, err := conn.Execute("SELECT 1 FROM laila_pool_entries WHERE key = ?", key)
	if err != nil {
		return false, err
	}
	return cursor.Fetchone() != nil, nil
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *SQLitePool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the table (``_keys(as_generator=False)``).
// With ``as_generator=True`` a lazy Iterator is returned instead of a
// list.
func (p *SQLitePool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	fetch := func() ([][]any, error) {
		p.Lock()
		defer p.Unlock()
		conn, err := p.Connection_()
		if err != nil {
			return nil, err
		}
		cursor, err := conn.Execute("SELECT key FROM laila_pool_entries ORDER BY key")
		if err != nil {
			return nil, err
		}
		return cursor.Fetchall(), nil
	}
	if !as_generator {
		rows, err := fetch()
		if err != nil {
			return nil, err
		}
		out := py.NewList()
		for _, row := range rows {
			out.Append(row[0])
		}
		return out, nil
	}

	var gen py.Iterator = func(yield func(any, error) bool) {
		rows, err := fetch()
		if err != nil {
			yield(nil, err)
			return
		}
		for _, row := range rows {
			if !yield(row[0], nil) {
				return
			}
		}
	}
	return gen, nil
}
