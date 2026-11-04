package laila

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/postgres/postgres.py -- PostgreSQL pool implementation with
// optional managed local server.

// Psycopg_connection is the slice of ``psycopg.Connection``
// (``autocommit=False``) the pool uses: ``cursor().execute(sql,
// params)`` returning a cursor-like result, ``commit()`` and ``close()``.
type Psycopg_connection interface {
	Execute(sql string, params ...any) (Sql_rows_, error)
	Commit() error
	Close() error
}

// Psycopg_connect_kwargs is the keyword set of ``psycopg.connect``: a
// DSN, or host / port / dbname / user / password, plus the connection
// timeout (0 = driver default).
type Psycopg_connect_kwargs struct {
	Dsn             *string
	Host            *string
	Port            int
	Dbname          *string
	User            *string
	Password        *string
	Connect_timeout int
}

// Psycopg_driver is the ``psycopg`` module: ``psycopg.connect(...)``.
// Registered under ``"psycopg"`` by ``drivers/postgres``.
type Psycopg_driver interface {
	Connect(kw Psycopg_connect_kwargs) (Psycopg_connection, error)
}

// Managed_process_ is the ``subprocess.Popen`` surface the
// server-managing pools drive (``*py.Popen``; tests substitute mocks).
type Managed_process_ interface {
	Poll() *int
	Wait(timeout float64) (int, error)
	Terminate() error
	Kill() error
	Stdout_read() string
	Stderr_read() string
}

// Os_path_exists_ is ``os.path.exists`` as the pools call it (a variable
// so tests can ``patch("os.path.exists")``).
var Os_path_exists_ = func(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// psycopg_ is the module global (nil when the extra is missing).
func psycopg_() Psycopg_driver {
	d, _ := Driver_("psycopg").(Psycopg_driver)
	return d
}

// PostgresPool is the PostgreSQL-backed pool.
//
// Can connect to an existing PostgreSQL via ``dsn`` or explicit ``host``
// / ``port`` / ``dbname`` / ``user`` parameters, or automatically start
// and manage a local ``postgres`` process.
type PostgresPool struct {
	LAILA_IDENTIFIABLE_POOL_

	Dsn                    *string `py:"dsn"`
	Host                   *string `py:"host"`
	Port                   int     `py:"port" default:"5432"`
	Dbname                 *string `py:"dbname"`
	User                   *string `py:"user"`
	Password               *string `py:"password"`
	Server_start_timeout_s float64 `py:"server_start_timeout_s" default:"5.0"`

	Conn_              Psycopg_connection `py:"_conn"`
	Postgres_proc_     Managed_process_   `py:"_postgres_proc"`
	Owns_local_server_ bool               `py:"_owns_local_server"`
	Postgres_dir_      *string            `py:"_postgres_dir"`
	Socket_dir_        *string            `py:"_socket_dir"`
	Local_dbname_      string             `py:"_local_dbname"`
	Local_user_        string             `py:"_local_user"`
}

// PostgresPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64,
// ``_local_dbname = "postgres"``, ``_local_user = "laila"``).
var PostgresPool_class = py.NewClass("PostgresPool", (*PostgresPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Set_private_default("_local_dbname", func() any { return "postgres" }).
	Set_private_default("_local_user", func() any { return "laila" }).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_PostgresPool is ``PostgresPool(**data)``.
func New_PostgresPool(kw ...py.Kw) (*PostgresPool, error) {
	p := PostgresPool_class.New().(*PostgresPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Model_post_init connects to PostgreSQL and creates the entries table.
func (p *PostgresPool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	conn, err := p.Connect_()
	if err != nil {
		return err
	}
	p.Conn_ = conn
	rows, err := conn.Execute(
		"SELECT column_name FROM information_schema.columns " +
			"WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'")
	if err != nil {
		return err
	}
	row, err := rows.Fetchone()
	if err != nil {
		return err
	}
	if row != nil {
		if _, err := conn.Execute("DROP TABLE laila_pool_entries"); err != nil {
			return err
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
	if err := conn.Commit(); err != nil {
		return err
	}
	py.Atexit_register(func() { _ = p.Close() })
	return nil
}

// Connect_ establishes a psycopg connection.
func (p *PostgresPool) Connect_() (Psycopg_connection, error) {
	if psycopg_() == nil {
		return nil, py.ImportError.New("psycopg is required for PostgresPool")
	}
	if p.Dsn != nil {
		return psycopg_().Connect(Psycopg_connect_kwargs{Dsn: p.Dsn})
	}
	if p.Host == nil && p.Dbname == nil && p.User == nil && p.Password == nil {
		if err := p.Configure_local_server_(); err != nil {
			return nil, err
		}
		if err := p.Ensure_local_server_(); err != nil {
			return nil, err
		}
		return p.Connect_local_(1)
	}
	if p.Host == nil || p.Dbname == nil || p.User == nil {
		return nil, py.ValueError.New(
			"PostgresPool requires either dsn, explicit host/dbname/user parameters, or no connection parameters for a managed local server.")
	}
	return psycopg_().Connect(Psycopg_connect_kwargs{
		Host: p.Host, Port: p.Port, Dbname: p.Dbname, User: p.User, Password: p.Password,
	})
}

// Configure_local_server_ sets up data and socket directories for a
// managed local postgres.
func (p *PostgresPool) Configure_local_server_() error {
	if p.Postgres_dir_ != nil && p.Socket_dir_ != nil {
		return nil
	}
	pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
	data_dir := filepath.Join(pool_dir, "data")
	socket_dir := filepath.Join(pool_dir, "socket")
	p.Postgres_dir_ = &data_dir
	p.Socket_dir_ = &socket_dir
	if err := py.Mkdir_p(data_dir); err != nil {
		return err
	}
	if err := py.Mkdir_p(socket_dir); err != nil {
		return err
	}
	p.Port = 20000 + Hashed_port_offset_(p.Pool_id())
	return nil
}

// Hashed_port_offset_ is ``int(sha1(s).hexdigest()[:8], 16) % 20000``.
func Hashed_port_offset_(s string) int {
	n, _ := strconv.ParseUint(py.Sha1_hex([]byte(s))[:8], 16, 64)
	return int(n % 20000)
}

// Connect_local_ connects to the managed local postgres via UNIX socket
// (``_connect_local(*, connect_timeout=1)``).
func (p *PostgresPool) Connect_local_(connect_timeout int) (Psycopg_connection, error) {
	return PostgresPool_connect_local_(p, connect_timeout)
}

// PostgresPool_connect_local_ is the ``_connect_local`` body (patchable
// like ``unittest.mock.patch.object(PostgresPool, "_connect_local")``).
var PostgresPool_connect_local_ = func(p *PostgresPool, connect_timeout int) (Psycopg_connection, error) {
	if p.Socket_dir_ == nil {
		return nil, py.RuntimeError.New("Local Postgres socket directory is not configured.")
	}
	if psycopg_() == nil {
		return nil, py.ImportError.New("psycopg is required for PostgresPool")
	}
	return psycopg_().Connect(Psycopg_connect_kwargs{
		Host: p.Socket_dir_, Port: p.Port, Dbname: &p.Local_dbname_, User: &p.Local_user_,
		Connect_timeout: connect_timeout,
	})
}

// Ensure_local_server_ starts a local postgres if one is not already
// reachable.
func (p *PostgresPool) Ensure_local_server_() error {
	if p.Local_server_available_() {
		p.Owns_local_server_ = false
		return nil
	}
	if err := p.Initdb_if_needed_(); err != nil {
		return err
	}
	return p.Start_local_server_()
}

// Local_server_available_ returns True if the local postgres accepts
// connections.
func (p *PostgresPool) Local_server_available_() bool { return PostgresPool_local_server_available_(p) }

// PostgresPool_local_server_available_ is the ``_local_server_available``
// body (patchable).
var PostgresPool_local_server_available_ = func(p *PostgresPool) bool {
	conn, err := p.Connect_local_(1)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Initdb_if_needed_ runs ``initdb`` if the data directory is
// uninitialised.
func (p *PostgresPool) Initdb_if_needed_() error { return PostgresPool_initdb_if_needed_(p) }

// PostgresPool_initdb_if_needed_ is the ``_initdb_if_needed`` body
// (patchable).
var PostgresPool_initdb_if_needed_ = func(p *PostgresPool) error {
	if p.Postgres_dir_ == nil {
		return py.RuntimeError.New("Local Postgres data directory is not configured.")
	}
	if Os_path_exists_(filepath.Join(*p.Postgres_dir_, "PG_VERSION")) {
		return nil
	}
	return p.Run_command_(
		[]string{"initdb", "-D", *p.Postgres_dir_, "-U", p.Local_user_, "-A", "trust"},
		"initialize local Postgres data directory "+*p.Postgres_dir_)
}

// Run_command_ executes a subprocess command, raising on failure
// (``_run_command(command, *, action)``).
func (p *PostgresPool) Run_command_(command []string, action string) error {
	return PostgresPool_run_command_(p, command, action)
}

// PostgresPool_run_command_ is the ``_run_command`` body (patchable).
var PostgresPool_run_command_ = func(p *PostgresPool, command []string, action string) error {
	return run_command_(command, action)
}

// run_command_ is the shared ``subprocess.run(check=True,
// capture_output=True, text=True)`` wrapper of the server-managing pools.
func run_command_(command []string, action string) error {
	_, _, err := py.Subprocess_run(command)
	if err == nil {
		return nil
	}
	if errors.Is(err, py.FileNotFoundError) {
		return py.RuntimeError.Newf("Unable to %s: command not found: %s", action, command[0]).From(err)
	}
	var e *py.Error
	if errors.As(err, &e) && e.Type.Is(py.CalledProcessError) {
		return py.RuntimeError.Newf("Unable to %s. Stdout: %s Stderr: %s", action,
			strings.TrimSpace(py.Str(e.Attr("stdout"))), strings.TrimSpace(py.Str(e.Attr("stderr")))).From(err)
	}
	return err
}

// Start_local_server_ launches a ``postgres`` subprocess and waits for
// readiness.
func (p *PostgresPool) Start_local_server_() error { return PostgresPool_start_local_server_(p) }

// PostgresPool_start_local_server_ is the ``_start_local_server`` body
// (patchable).
var PostgresPool_start_local_server_ = func(p *PostgresPool) error {
	if p.Postgres_dir_ == nil || p.Socket_dir_ == nil {
		return py.RuntimeError.New("Local Postgres server is not fully configured.")
	}
	cmd := []string{"postgres", "-D", *p.Postgres_dir_, "-k", *p.Socket_dir_, "-p", strconv.Itoa(p.Port), "-h", ""}
	proc, err := py.Popen_(cmd)
	if err != nil {
		return err
	}
	p.Postgres_proc_ = proc
	p.Owns_local_server_ = true
	deadline := py.Time() + p.Server_start_timeout_s
	var last_err error
	for py.Time() < deadline {
		if code := proc.Poll(); code != nil {
			stdout, stderr := proc.Stdout_read(), proc.Stderr_read()
			_ = p.Close()
			return py.RuntimeError.Newf("postgres exited early (code=%d). Stdout: %s Stderr: %s",
				*code, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		}
		conn, err := p.Connect_local_(1)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		last_err = err
		py.Sleep_sync(0.1)
	}
	stdout, stderr := "", ""
	if p.Postgres_proc_ != nil {
		stdout, stderr = p.Postgres_proc_.Stdout_read(), p.Postgres_proc_.Stderr_read()
	}
	_ = p.Close()
	return py.RuntimeError.Newf("postgres failed to start for %s. Last error: %s. Stdout: %s Stderr: %s",
		p.Pool_id(), py.Str(last_err), strings.TrimSpace(stdout), strings.TrimSpace(stderr))
}

// Connection_ returns the active psycopg connection; RuntimeError once
// closed.
func (p *PostgresPool) Connection_() (Psycopg_connection, error) {
	if p.Conn_ == nil {
		return nil, py.RuntimeError.New("PostgresPool is closed.")
	}
	return p.Conn_, nil
}

// Close closes the connection and terminates any managed postgres
// process.
func (p *PostgresPool) Close() error {
	if p.Conn_ != nil {
		_ = p.Conn_.Close()
		p.Conn_ = nil
	}
	proc := p.Postgres_proc_
	p.Postgres_proc_ = nil
	if proc != nil && p.Owns_local_server_ {
		stop_managed_process_(proc, 2.0)
	}
	p.Owns_local_server_ = false
	return nil
}

// stop_managed_process_ is the shared ``terminate(); wait(timeout)``
// with ``kill()`` fallback of the server-managing pools.
func stop_managed_process_(proc Managed_process_, timeout float64) {
	_ = proc.Terminate()
	if _, err := proc.Wait(timeout); err != nil {
		_ = proc.Kill()
		_, _ = proc.Wait(timeout)
	}
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *PostgresPool) Read_(key string) (any, error) {
	p.Lock()
	conn, err := p.Connection_()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	rows, err := conn.Execute("SELECT value FROM laila_pool_entries WHERE key = %s", key)
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
func (p *PostgresPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "PostgresPool expects a serialized JSON string.")
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
                    VALUES (%s, %s)
                    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                    `, key, value); err != nil {
		return err
	}
	return conn.Commit()
}

// Delete_ deletes the row for *key*.
func (p *PostgresPool) Delete_(key string) error {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return err
	}
	if _, err := conn.Execute("DELETE FROM laila_pool_entries WHERE key = %s", key); err != nil {
		return err
	}
	return conn.Commit()
}

// Empty_ removes all entries from the pool.
func (p *PostgresPool) Empty_() error {
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

// Exists_ returns True if *key* is present in the table.
func (p *PostgresPool) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	conn, err := p.Connection_()
	if err != nil {
		return false, err
	}
	rows, err := conn.Execute("SELECT 1 FROM laila_pool_entries WHERE key = %s", key)
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
func (p *PostgresPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the table; ``as_generator=True`` yields a lazy
// Iterator instead of a list.
func (p *PostgresPool) Keys_(kw ...py.Kw) (any, error) {
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
