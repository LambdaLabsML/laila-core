//go:build cgo

// Package duckdb is the ``duckdb`` extra (``pip install
// laila-core[duckdb]``): it binds laila.DuckDBPool to
// github.com/marcboeker/go-duckdb (cgo; the DuckDB engine is linked
// statically). Import it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/duckdb"
//
// With ``CGO_ENABLED=0`` the package is empty and DuckDBPool raises
// ``ImportError("duckdb is required for DuckDBPool")`` like Python
// without the extra.
package duckdb

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// Error is ``duckdb.Error`` (every engine error).
var Error = py.NewType("Error", py.Exception)

// Driver implements laila.Duckdb_driver.
type Driver struct{}

// Connect is ``duckdb.connect(file_path)``: one pinned session so
// ``BEGIN`` / ``COMMIT`` / ``SET`` apply to the same connection.
func (Driver) Connect(file_path string) (laila.Duckdb_connection, error) {
	db, err := sql.Open("duckdb", file_path)
	if err != nil {
		return nil, wrap(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		return nil, wrap(err)
	}
	return &connection{db: db, conn: conn}, nil
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return Error.New(err.Error()).From(err)
}

type connection struct {
	mu   sync.Mutex
	db   *sql.DB
	conn *sql.Conn
}

// Execute runs one statement; statements that produce rows (SELECT,
// ``RETURNING``) are fetched eagerly like the Python cursor result.
func (c *connection) Execute(sql_ string, params ...any) (laila.Sql_rows_, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, Error.New("Connection already closed!")
	}
	ctx := context.Background()
	if returnsRows(sql_) {
		rows, err := c.conn.QueryContext(ctx, sql_, params...)
		if err != nil {
			return nil, wrap(err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return nil, wrap(err)
		}
		out := [][]any{}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, wrap(err)
			}
			out = append(out, vals)
		}
		if err := rows.Err(); err != nil {
			return nil, wrap(err)
		}
		return &result{rows: out}, nil
	}
	if _, err := c.conn.ExecContext(ctx, sql_, params...); err != nil {
		return nil, wrap(err)
	}
	return &result{}, nil
}

func returnsRows(sql_ string) bool {
	s := strings.ToUpper(strings.TrimSpace(sql_))
	return strings.HasPrefix(s, "SELECT") || strings.HasPrefix(s, "WITH") || strings.Contains(s, " RETURNING ")
}

func (c *connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	if err2 := c.db.Close(); err == nil {
		err = err2
	}
	return wrap(err)
}

type result struct {
	rows [][]any
	pos  int
}

func (r *result) Fetchone() ([]any, error) {
	if r.pos >= len(r.rows) {
		return nil, nil
	}
	row := r.rows[r.pos]
	r.pos++
	return row, nil
}

func (r *result) Fetchall() ([][]any, error) {
	rest := r.rows[r.pos:]
	r.pos = len(r.rows)
	return rest, nil
}

func init() { laila.Register_driver("duckdb", Driver{}) }
