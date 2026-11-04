// Package postgres is the ``postgres`` extra (``pip install
// laila-core[postgres]``): it binds laila.PostgresPool to
// github.com/jackc/pgx. Import it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/postgres"
package postgres

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/jackc/pgx/v5"
)

// OperationalError is ``psycopg.OperationalError`` (connection failures).
var OperationalError = py.NewType("OperationalError", py.Exception)

// Driver implements laila.Psycopg_driver.
type Driver struct{}

// Connect is ``psycopg.connect(..., autocommit=False)``: the connection
// is opened in a transaction that ``commit()`` ends and the next
// statement implicitly reopens, exactly like psycopg's default mode.
func (Driver) Connect(kw laila.Psycopg_connect_kwargs) (laila.Psycopg_connection, error) {
	var dsn string
	if kw.Dsn != nil {
		dsn = *kw.Dsn
	} else {
		parts := []string{}
		add := func(k string, v *string) {
			if v != nil {
				parts = append(parts, k+"="+quoteValue(*v))
			}
		}
		add("host", kw.Host)
		if kw.Port != 0 {
			parts = append(parts, "port="+strconv.Itoa(kw.Port))
		}
		add("dbname", kw.Dbname)
		add("user", kw.User)
		add("password", kw.Password)
		dsn = strings.Join(parts, " ")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, OperationalError.New(err.Error()).From(err)
	}
	if kw.Connect_timeout > 0 {
		cfg.ConnectTimeout = time.Duration(kw.Connect_timeout) * time.Second
	}
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, OperationalError.New(err.Error()).From(err)
	}
	return &connection{conn: conn}, nil
}

func quoteValue(v string) string {
	if v == "" || strings.ContainsAny(v, " '\\") {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	return v
}

type connection struct {
	mu   sync.Mutex
	conn *pgx.Conn
	tx   pgx.Tx
}

// Execute runs one statement inside the current transaction (opening one
// when needed) with psycopg's ``%s`` placeholders translated to pgx's
// ``$n``.
func (c *connection) Execute(sql string, params ...any) (laila.Sql_rows_, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx := context.Background()
	if c.tx == nil {
		tx, err := c.conn.Begin(ctx)
		if err != nil {
			return nil, wrap(err)
		}
		c.tx = tx
	}
	rows, err := c.tx.Query(ctx, translate(sql), params...)
	if err != nil {
		return nil, wrap(err)
	}
	defer rows.Close()
	out := [][]any{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(err)
	}
	return &result{rows: out}, nil
}

func (c *connection) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx == nil {
		return nil
	}
	err := c.tx.Commit(context.Background())
	c.tx = nil
	return wrap(err)
}

func (c *connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx != nil {
		_ = c.tx.Rollback(context.Background())
		c.tx = nil
	}
	return wrap(c.conn.Close(context.Background()))
}

// translate rewrites ``%s`` placeholders into ``$1, $2, ...``.
func translate(sql string) string {
	var sb strings.Builder
	n := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == '%' && i+1 < len(sql) && sql[i+1] == 's' {
			n++
			sb.WriteString("$" + strconv.Itoa(n))
			i++
			continue
		}
		sb.WriteByte(sql[i])
	}
	return sb.String()
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return OperationalError.New(err.Error()).From(err)
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

func init() { laila.Register_driver("psycopg", Driver{}) }
