package laila

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/redis/redis.py -- Redis pool implementation with a managed
// private redis-server.
//
// Python imports ``redis`` unguarded, so without the extra the module
// fails to import and ``laila.data`` has no RedisPool. The Go class
// always exists; ``Driver_available("redis")`` is the ``HAVE_REDISPOOL``
// check and construction raises ModuleNotFoundError when the driver is
// missing.

// Redis_client is the slice of ``redis.Redis(decode_responses=True)``
// the pool uses.
type Redis_client interface {
	// Ping is ``r.ping()``.
	Ping() error
	// Hget is ``r.hget(name, key)`` (nil when absent).
	Hget(name string, key string) (*string, error)
	// Hset is ``r.hset(name, key, value)``.
	Hset(name string, key string, value string) error
	// Hdel is ``r.hdel(name, key)``.
	Hdel(name string, key string) error
	// Delete is ``r.delete(name)``.
	Delete(name string) error
	// Hexists is ``r.hexists(name, key)``.
	Hexists(name string, key string) (bool, error)
	// Hkeys is ``r.hkeys(name)``.
	Hkeys(name string) ([]string, error)
	// Hscan is ``r.hscan(name, cursor)`` -> (next cursor, batch keys).
	Hscan(name string, cursor uint64) (uint64, []string, error)
	// Shutdown is ``r.shutdown(save=save)``.
	Shutdown(save bool) error
	// Close releases the connection pool.
	Close() error
}

// Redis_driver is the ``redis`` module: ``redis.Redis(unix_socket_path,
// password, socket_connect_timeout, decode_responses=True)``.
// ``socket_connect_timeout`` <= 0 means the library default. Registered
// under ``"redis"`` by ``drivers/redis``.
type Redis_driver interface {
	Redis(unix_socket_path string, password *string, socket_connect_timeout float64) (Redis_client, error)
}

// redis_ is the module global (nil when the extra is missing).
func redis_() Redis_driver {
	d, _ := Driver_("redis").(Redis_driver)
	return d
}

// RedisPool is the Redis-backed pool using a private redis-server over a
// UNIX socket.
//
// Persistence:
//   - Data directory: ``<LAILA_DEFAULT_DIRECTORIES["pools"]>/<pool.uuid>/``
//   - Dump file (private): pool.rdb
//   - If pool.rdb exists, Redis loads it automatically.
//   - Dumps are never deleted.
//
// Notes:
//   - No TCP ports
//   - No orphan-process handling
//   - Values stored are serialized JSON strings
type RedisPool struct {
	LAILA_IDENTIFIABLE_POOL_

	// Redis key namespaces
	Key_prefix  string `py:"key_prefix" default:"pool"`
	Lock_prefix string `py:"lock_prefix" default:"pool_lock"`
	// Optional auth
	Redis_password *string `py:"redis_password"`
	// Behavior
	Server_start_timeout_s float64 `py:"server_start_timeout_s" default:"3.0"`
	Lock_timeout           int     `py:"lock_timeout" default:"30"`
	Redis_dir              *string `py:"redis_dir"`

	Client_            Redis_client     `py:"_client"`
	Redis_proc_        Managed_process_ `py:"_redis_proc"`
	Db_dump_name_      string           `py:"_db_dump_name"`
	Redis_socket_name_ string           `py:"_redis_socket_name"`
}

// RedisPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64,
// ``_db_dump_name = "laila_pool.rdb"``, ``_redis_socket_name =
// "laila_redis.sock"``).
var RedisPool_class = py.NewClass("RedisPool", (*RedisPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Set_private_default("_db_dump_name", func() any { return "laila_pool.rdb" }).
	Set_private_default("_redis_socket_name", func() any { return "laila_redis.sock" }).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_RedisPool is ``RedisPool(**data)``.
func New_RedisPool(kw ...py.Kw) (*RedisPool, error) {
	p := RedisPool_class.New().(*RedisPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// ---------------- lifecycle ----------------

// Model_post_init always: 1) establishes redis_dir from pool_id, 2)
// starts redis-server (unix socket only), 3) connects the client.
func (p *RedisPool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	if redis_() == nil {
		return py.ModuleNotFoundError.New("No module named 'redis'")
	}
	pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
	p.Redis_dir = &pool_dir
	if err := py.Mkdir_p(pool_dir); err != nil {
		return err
	}
	// Remove stale socket file (do NOT handle orphan processes yet)
	_ = os.Remove(p.Redis_socket_path_())
	if err := p.Start_redis_server_(); err != nil {
		return err
	}
	client, err := redis_().Redis(p.Redis_socket_path_(), p.Redis_password, 0)
	if err != nil {
		return err
	}
	p.Client_ = client
	py.Atexit_register(func() { _ = p.Close() })
	return nil
}

// Close is the clean shutdown: ask Redis to save and exit; never delete
// dumps.
func (p *RedisPool) Close() error {
	proc := p.Redis_proc_
	p.Redis_proc_ = nil
	if proc == nil {
		return nil
	}
	if p.Client_ != nil {
		_ = p.Client_.Shutdown(true)
		_ = p.Client_.Close()
	}
	stop_managed_process_(proc, 1.5)
	_ = os.Remove(p.Redis_socket_path_())
	return nil
}

// ---------------- redis-server ----------------

// Start_redis_server_ launches a ``redis-server`` subprocess on a UNIX
// socket.
func (p *RedisPool) Start_redis_server_() error {
	if p.Redis_dir == nil || *p.Redis_dir == "" {
		return py.RuntimeError.New("Redis server not fully configured (missing redis_dir)")
	}
	cmd := []string{
		"redis-server",
		"--port", "0", // disable TCP
		"--unixsocket", p.Redis_socket_path_(),
		"--unixsocketperm", "700",
		"--dir", *p.Redis_dir,
		"--dbfilename", p.Db_dump_name_,
		"--save", "900 1",
		"--save", "300 10",
		"--save", "60 10000",
		"--appendonly", "no",
		"--protected-mode", "yes",
	}
	if p.Redis_password != nil && *p.Redis_password != "" {
		cmd = append(cmd, "--requirepass", *p.Redis_password)
	}
	proc, err := py.Popen_(cmd)
	if err != nil {
		return err
	}
	p.Redis_proc_ = proc
	deadline := py.Time() + p.Server_start_timeout_s
	var last_err error
	for py.Time() < deadline {
		if code := proc.Poll(); code != nil {
			stdout, stderr := proc.Stdout_read(), proc.Stderr_read()
			_ = p.Close()
			return py.RuntimeError.Newf("redis-server exited early (code=%d). Stdout: %s Stderr: %s",
				*code, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		}
		r, err := redis_().Redis(p.Redis_socket_path_(), p.Redis_password, 0.25)
		if err == nil {
			err = r.Ping()
			_ = r.Close()
			if err == nil {
				return nil
			}
		}
		last_err = err
		py.Sleep_sync(0.05)
	}
	stdout, stderr := "", ""
	if p.Redis_proc_ != nil {
		stdout, stderr = p.Redis_proc_.Stdout_read(), p.Redis_proc_.Stderr_read()
	}
	_ = p.Close()
	return py.RuntimeError.Newf("redis-server failed to start on unix socket %s. Last error: %s. Stdout: %s Stderr: %s",
		p.Redis_socket_path_(), py.Str(last_err), strings.TrimSpace(stdout), strings.TrimSpace(stderr))
}

// ---------------- private helpers ----------------

// Redis_socket_path_ is the deterministic UNIX socket path derived from
// the pool UUID.
func (p *RedisPool) Redis_socket_path_() string {
	digest := py.Sha1_hex([]byte(p.Uuid()))[:16]
	return "/tmp/laila_redis_" + digest + ".sock"
}

// ---------------- key helpers ----------------

// Redis_hash_key is the Redis hash key used to store all pool entries.
func (p *RedisPool) Redis_hash_key() string { return p.Key_prefix }

// Redis_lock_key is the Redis key used for distributed locking.
func (p *RedisPool) Redis_lock_key() string { return p.Lock_prefix }

// client_ is the ``if self._client is None: raise RuntimeError`` guard.
func (p *RedisPool) client_() (Redis_client, error) {
	if p.Client_ == nil {
		return nil, py.RuntimeError.New("Redis client not initialized.")
	}
	return p.Client_, nil
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *RedisPool) Read_(key string) (any, error) {
	client, err := p.client_()
	if err != nil {
		return nil, err
	}
	value, err := client.Hget(p.Redis_hash_key(), key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return py.Json_loads(*value)
}

// Write_ stores *entry* as a JSON string in the Redis hash.
func (p *RedisPool) Write_(key string, entry any) error {
	value := entry
	if d, ok := asDict(value); ok {
		s, err := py.Json_dumps(d)
		if err != nil {
			return err
		}
		value = s
	}
	client, err := p.client_()
	if err != nil {
		return err
	}
	s, ok := value.(string)
	if !ok {
		return py.TypeError.New("RedisPool expects a serialized JSON string.")
	}
	return client.Hset(p.Redis_hash_key(), key, s)
}

// Delete_ deletes *key* from the Redis hash.
func (p *RedisPool) Delete_(key string) error {
	client, err := p.client_()
	if err != nil {
		return err
	}
	return client.Hdel(p.Redis_hash_key(), key)
}

// Empty_ removes all entries from the pool (deletes the Redis hash).
func (p *RedisPool) Empty_() error {
	client, err := p.client_()
	if err != nil {
		return err
	}
	return client.Delete(p.Redis_hash_key())
}

// Exists_ returns True if *key* exists in the Redis hash.
func (p *RedisPool) Exists_(key string) (bool, error) {
	client, err := p.client_()
	if err != nil {
		return false, err
	}
	return client.Hexists(p.Redis_hash_key(), key)
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *RedisPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the Redis hash; ``as_generator=True`` yields
// a lazy Iterator driven by ``HSCAN``.
func (p *RedisPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	client, err := p.client_()
	if err != nil {
		return nil, err
	}
	if !as_generator {
		keys, err := client.Hkeys(p.Redis_hash_key())
		if err != nil {
			return nil, err
		}
		out := py.NewList()
		for _, k := range keys {
			out.Append(k)
		}
		return out, nil
	}
	var gen py.Iterator = func(yield func(any, error) bool) {
		var cursor uint64
		for {
			next, batch, err := client.Hscan(p.Redis_hash_key(), cursor)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, k := range batch {
				if !yield(k, nil) {
					return
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return gen, nil
}

func init() { py.Register_property("redis_hash_key", "redis_lock_key") }
