package py

import (
	"database/sql"
	"regexp"
	"strings"

	_ "modernc.org/sqlite" // pure-Go sqlite driver (no cgo)
)

// sqlite3 -- the subset of the standard-library module used by laila:
// ``sqlite3.connect(path, check_same_thread=False)`` plus
// ``execute`` / ``executemany`` / ``fetchone`` / ``fetchall`` / ``commit``
// / ``close``. Backed by database/sql + modernc.org/sqlite.
//
// A connection is pinned to a single underlying database handle (as in
// CPython) so statements never race each other through a pool of
// handles; callers that want cross-thread use still serialise with their
// own lock (the manifest index and the SQLite pool both do).

// Sqlite3Connection is a ``sqlite3.Connection``.
type Sqlite3Connection struct {
	db     *sql.DB
	closed bool
}

// Sqlite3Cursor is the eagerly fetched result of one ``execute`` (the
// cursor methods laila uses: ``fetchall`` / ``fetchone`` / iteration).
type Sqlite3Cursor struct {
	rows [][]any
}

// Fetchall returns (and drains) every remaining row.
func (c *Sqlite3Cursor) Fetchall() [][]any {
	rows := c.rows
	c.rows = nil
	return rows
}

// Fetchone pops the next row or returns nil when exhausted.
func (c *Sqlite3Cursor) Fetchone() []any {
	if len(c.rows) == 0 {
		return nil
	}
	row := c.rows[0]
	c.rows = c.rows[1:]
	return row
}

// Iter drains the cursor as ``[]any`` rows (``for row in cursor``).
func (c *Sqlite3Cursor) Iter() []any {
	rows := c.Fetchall()
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = Tuple(r)
	}
	return out
}

// Sqlite3_connect is ``sqlite3.connect(path, check_same_thread=False)``.
// ``:memory:`` is honoured. The busy timeout matches CPython's 5 s default.
func Sqlite3_connect(path string) (*Sqlite3Connection, error) {
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, sqlite3_error(err)
	}
	// One handle: ``:memory:`` databases are per-connection and file
	// databases must not interleave statements across handles.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, sqlite3_error(err)
	}
	return &Sqlite3Connection{db: db}, nil
}

var sqlite3_code_re = regexp.MustCompile(`\s*\(\d+\)$`)

// sqlite3_error maps a driver error onto the ``sqlite3`` exception
// hierarchy with CPython's message shape (``no such column: x``).
func sqlite3_error(err error) error {
	if err == nil {
		return nil
	}
	if AsError(err) != nil {
		return err
	}
	msg := err.Error()
	msg = sqlite3_code_re.ReplaceAllString(msg, "")
	for _, prefix := range []string{"SQL logic error: ", "sqlite: "} {
		msg = strings.TrimPrefix(msg, prefix)
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "constraint failed") || strings.Contains(lower, "unique constraint"):
		return IntegrityError.New(msg)
	case strings.Contains(lower, "incomplete input") || strings.Contains(lower, "syntax error") ||
		strings.Contains(lower, "no such column") || strings.Contains(lower, "no such table") ||
		strings.Contains(lower, "unable to open") || strings.Contains(lower, "database is locked") ||
		strings.Contains(lower, "readonly") || strings.Contains(lower, "logic error"):
		return OperationalError.New(msg)
	case strings.Contains(lower, "closed"):
		return ProgrammingError.New("Cannot operate on a closed database.")
	}
	return OperationalError.New(msg)
}

// Execute runs one statement with positional parameters and eagerly
// fetches its rows (``conn.execute(sql, params)``).
func (c *Sqlite3Connection) Execute(query string, params ...any) (*Sqlite3Cursor, error) {
	if c == nil || c.closed {
		return nil, ProgrammingError.New("Cannot operate on a closed database.")
	}
	args := sqlite3_bind(params)
	head := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(head, "SELECT") || strings.HasPrefix(head, "PRAGMA") || strings.HasPrefix(head, "WITH") ||
		strings.HasPrefix(head, "EXPLAIN") || strings.HasPrefix(head, "VALUES") {
		rows, err := c.db.Query(query, args...)
		if err != nil {
			return nil, sqlite3_error(err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return nil, sqlite3_error(err)
		}
		out := [][]any{}
		for rows.Next() {
			cells := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, sqlite3_error(err)
			}
			for i, v := range cells {
				cells[i] = sqlite3_value(v)
			}
			out = append(out, cells)
		}
		if err := rows.Err(); err != nil {
			return nil, sqlite3_error(err)
		}
		return &Sqlite3Cursor{rows: out}, nil
	}
	if _, err := c.db.Exec(query, args...); err != nil {
		return nil, sqlite3_error(err)
	}
	return &Sqlite3Cursor{}, nil
}

// Executemany runs the statement once per parameter row inside a single
// transaction (``conn.executemany(sql, seq_of_params)``).
func (c *Sqlite3Connection) Executemany(query string, seq [][]any) error {
	if c == nil || c.closed {
		return ProgrammingError.New("Cannot operate on a closed database.")
	}
	if len(seq) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return sqlite3_error(err)
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		_ = tx.Rollback()
		return sqlite3_error(err)
	}
	for _, params := range seq {
		if _, err := stmt.Exec(sqlite3_bind(params)...); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return sqlite3_error(err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		return sqlite3_error(err)
	}
	return nil
}

// Commit is ``conn.commit()``. Statements run in autocommit mode here, so
// there is nothing pending; kept for call-site parity.
func (c *Sqlite3Connection) Commit() error {
	if c == nil || c.closed {
		return ProgrammingError.New("Cannot operate on a closed database.")
	}
	return nil
}

// Close is ``conn.close()`` (idempotent).
func (c *Sqlite3Connection) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	return sqlite3_error(c.db.Close())
}

// Closed reports whether Close was called.
func (c *Sqlite3Connection) Closed() bool { return c == nil || c.closed }

// sqlite3_bind converts Python-shaped parameters to driver values.
func sqlite3_bind(params []any) []any {
	out := make([]any, len(params))
	for i, p := range params {
		switch x := p.(type) {
		case nil:
			out[i] = nil
		case bool:
			if x {
				out[i] = int64(1)
			} else {
				out[i] = int64(0)
			}
		case int:
			out[i] = int64(x)
		case int32:
			out[i] = int64(x)
		case int64, float64, string, []byte:
			out[i] = x
		case float32:
			out[i] = float64(x)
		default:
			if n, err := Int(p); err == nil {
				out[i] = int64(n)
			} else {
				out[i] = Str(p)
			}
		}
	}
	return out
}

// sqlite3_value converts a scanned driver value to the port's value
// model (int64 -> int, []byte stays bytes, NULL -> nil).
func sqlite3_value(v any) any {
	switch x := v.(type) {
	case int64:
		return int(x)
	case []byte:
		return Bytes_clone(x)
	}
	return v
}
