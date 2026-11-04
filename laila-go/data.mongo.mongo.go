package laila

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/mongo/mongo.py -- MongoDB-backed pool implementation with
// optional managed local server.

// Pymongo_collection is the slice of ``pymongo.collection.Collection``
// the pool uses; filters / projections / updates are plain dicts exactly
// as passed to pymongo.
type Pymongo_collection interface {
	// Create_index is ``collection.create_index(key, unique=unique)``.
	Create_index(key string, unique bool) error
	// Find_one is ``collection.find_one(filter, projection)`` (nil when
	// no document matches).
	Find_one(filter *py.Dict, projection *py.Dict) (*py.Dict, error)
	// Update_one is ``collection.update_one(filter, update, upsert=upsert)``.
	Update_one(filter *py.Dict, update *py.Dict, upsert bool) error
	// Delete_one is ``collection.delete_one(filter)``.
	Delete_one(filter *py.Dict) error
	// Delete_many is ``collection.delete_many(filter)``.
	Delete_many(filter *py.Dict) error
	// Count_documents is ``collection.count_documents(filter, limit=limit)``.
	Count_documents(filter *py.Dict, limit int) (int, error)
	// Find_sorted is ``collection.find(filter, projection).sort(key,
	// direction)`` fully iterated.
	Find_sorted(filter *py.Dict, projection *py.Dict, sort_key string, direction int) ([]*py.Dict, error)
}

// Pymongo_client is the slice of ``pymongo.MongoClient`` the pool uses.
type Pymongo_client interface {
	// Collection is ``client[dbname][name]``.
	Collection(dbname string, name string) Pymongo_collection
	// Admin_command_ping is ``client.admin.command("ping")``.
	Admin_command_ping() error
	// Close is ``client.close()``.
	Close() error
}

// Pymongo_driver is the ``pymongo`` module: ``MongoClient(uri,
// serverSelectionTimeoutMS=...)`` / ``MongoClient(host=, port=, ...)``.
// Registered under ``"pymongo"`` by ``drivers/mongo``.
type Pymongo_driver interface {
	MongoClient_uri(uri string, server_selection_timeout_ms int) (Pymongo_client, error)
	MongoClient_host(host string, port int, server_selection_timeout_ms int) (Pymongo_client, error)
}

// ServerSelectionTimeoutError is ``pymongo.errors.ServerSelectionTimeoutError``.
var ServerSelectionTimeoutError = py.NewType("ServerSelectionTimeoutError", py.Exception)

// pymongo_ is the module global (nil when the extra is missing).
func pymongo_() Pymongo_driver {
	d, _ := Driver_("pymongo").(Pymongo_driver)
	return d
}

// MongoPool is the MongoDB-backed pool.
//
// Can connect to an existing MongoDB via ``uri`` or ``host`` / ``port``,
// or automatically start and manage a local ``mongod`` process.
type MongoPool struct {
	LAILA_IDENTIFIABLE_POOL_

	Uri                    *string `py:"uri"`
	Host                   *string `py:"host"`
	Port                   int     `py:"port" default:"27017"`
	Dbname                 string  `py:"dbname" default:"laila"`
	Server_start_timeout_s float64 `py:"server_start_timeout_s" default:"5.0"`

	Client_            Pymongo_client   `py:"_client"`
	Mongo_proc_        Managed_process_ `py:"_mongo_proc"`
	Owns_local_server_ bool             `py:"_owns_local_server"`
	Mongo_dir_         *string          `py:"_mongo_dir"`
}

// MongoPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64).
var MongoPool_class = py.NewClass("MongoPool", (*MongoPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_MongoPool is ``MongoPool(**data)``.
func New_MongoPool(kw ...py.Kw) (*MongoPool, error) {
	p := MongoPool_class.New().(*MongoPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Model_post_init connects to MongoDB and ensures the entries collection
// is indexed.
func (p *MongoPool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	client, err := p.Connect_()
	if err != nil {
		return err
	}
	p.Client_ = client
	collection, err := p.Collection_()
	if err != nil {
		return err
	}
	if err := collection.Create_index("key", true); err != nil {
		return err
	}
	py.Atexit_register(func() { _ = p.Close() })
	return nil
}

// Connect_ establishes a ``MongoClient`` connection.
func (p *MongoPool) Connect_() (Pymongo_client, error) {
	if pymongo_() == nil {
		return nil, py.ImportError.New("pymongo is required for MongoPool")
	}
	if p.Uri != nil {
		return pymongo_().MongoClient_uri(*p.Uri, 1000)
	}
	if p.Host == nil {
		if err := p.Configure_local_server_(); err != nil {
			return nil, err
		}
		if err := p.Ensure_local_server_(); err != nil {
			return nil, err
		}
		uri, err := p.Local_uri_()
		if err != nil {
			return nil, err
		}
		return pymongo_().MongoClient_uri(uri, 1000)
	}
	return pymongo_().MongoClient_host(*p.Host, p.Port, 1000)
}

// Configure_local_server_ sets up directory and port for a managed local
// mongod.
func (p *MongoPool) Configure_local_server_() error {
	if p.Mongo_dir_ != nil {
		return nil
	}
	pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
	p.Mongo_dir_ = &pool_dir
	if err := py.Mkdir_p(pool_dir); err != nil {
		return err
	}
	p.Port = 30000 + Hashed_port_offset_(p.Pool_id())
	host := "127.0.0.1"
	p.Host = &host
	return nil
}

// Local_uri_ is the MongoDB connection URI for the managed local server.
func (p *MongoPool) Local_uri_() (string, error) {
	if p.Host == nil {
		return "", py.RuntimeError.New("Local Mongo host is not configured.")
	}
	return "mongodb://" + *p.Host + ":" + strconv.Itoa(p.Port) + "/", nil
}

// Ensure_local_server_ starts a local mongod if one is not already
// reachable.
func (p *MongoPool) Ensure_local_server_() error {
	if p.Local_server_available_() {
		p.Owns_local_server_ = false
		return nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		err := p.Start_local_server_()
		if err == nil {
			return nil
		}
		if !errors.Is(err, py.RuntimeError) || !strings.Contains(py.Str(err), "code=48") {
			return err
		}
		py.Sleep_sync(1)
		if p.Local_server_available_() {
			p.Owns_local_server_ = false
			return nil
		}
		p.Port++
	}
	return nil
}

// Local_server_available_ returns True if the local mongod responds to a
// ping.
func (p *MongoPool) Local_server_available_() bool { return MongoPool_local_server_available_(p) }

// MongoPool_local_server_available_ is the ``_local_server_available``
// body (patchable like ``unittest.mock.patch.object``).
var MongoPool_local_server_available_ = func(p *MongoPool) bool {
	if pymongo_() == nil {
		return false
	}
	uri, err := p.Local_uri_()
	if err != nil {
		return false
	}
	client, err := pymongo_().MongoClient_uri(uri, 500)
	if err != nil {
		return false
	}
	defer client.Close()
	return client.Admin_command_ping() == nil
}

// Start_local_server_ launches a ``mongod`` subprocess and waits for
// readiness.
func (p *MongoPool) Start_local_server_() error { return MongoPool_start_local_server_(p) }

// MongoPool_start_local_server_ is the ``_start_local_server`` body
// (patchable).
var MongoPool_start_local_server_ = func(p *MongoPool) error {
	if p.Mongo_dir_ == nil || p.Host == nil {
		return py.RuntimeError.New("Local Mongo server is not fully configured.")
	}
	cmd := []string{"mongod", "--dbpath", *p.Mongo_dir_, "--port", strconv.Itoa(p.Port), "--bind_ip", *p.Host, "--quiet"}
	proc, err := py.Popen_(cmd)
	if err != nil {
		return err
	}
	p.Mongo_proc_ = proc
	p.Owns_local_server_ = true
	deadline := py.Time() + p.Server_start_timeout_s
	var last_err error
	for py.Time() < deadline {
		if code := proc.Poll(); code != nil {
			stdout, stderr := proc.Stdout_read(), proc.Stderr_read()
			_ = p.Close()
			return py.RuntimeError.Newf("mongod exited early (code=%d). Stdout: %s Stderr: %s",
				*code, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		}
		uri, _ := p.Local_uri_()
		client, err := pymongo_().MongoClient_uri(uri, 500)
		if err == nil {
			err = client.Admin_command_ping()
			_ = client.Close()
			if err == nil {
				return nil
			}
		}
		last_err = err
		py.Sleep_sync(0.1)
	}
	stdout, stderr := "", ""
	if p.Mongo_proc_ != nil {
		stdout, stderr = p.Mongo_proc_.Stdout_read(), p.Mongo_proc_.Stderr_read()
	}
	_ = p.Close()
	return py.RuntimeError.Newf("mongod failed to start for %s. Last error: %s. Stdout: %s Stderr: %s",
		p.Pool_id(), py.Str(last_err), strings.TrimSpace(stdout), strings.TrimSpace(stderr))
}

// Db_ returns the active MongoDB database handle (the client; Go
// collections are addressed by name). RuntimeError once closed.
func (p *MongoPool) Db_() (Pymongo_client, error) {
	if p.Client_ == nil {
		return nil, py.RuntimeError.New("MongoPool is closed.")
	}
	return p.Client_, nil
}

// Collection_ returns the ``laila_pool_entries`` collection.
func (p *MongoPool) Collection_() (Pymongo_collection, error) {
	client, err := p.Db_()
	if err != nil {
		return nil, err
	}
	return client.Collection(p.Dbname, "laila_pool_entries"), nil
}

// Close closes the client and terminates any managed mongod process.
func (p *MongoPool) Close() error {
	if p.Client_ != nil {
		_ = p.Client_.Close()
		p.Client_ = nil
	}
	proc := p.Mongo_proc_
	p.Mongo_proc_ = nil
	if proc != nil && p.Owns_local_server_ {
		stop_managed_process_(proc, 2.0)
	}
	p.Owns_local_server_ = false
	return nil
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *MongoPool) Read_(key string) (any, error) {
	p.Lock()
	collection, err := p.Collection_()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	doc, err := collection.Find_one(py.DictOf("key", key), py.DictOf("_id", 0, "value", 1))
	p.Unlock()
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	value, _ := doc.Get("value")
	return py.Json_loads(py.Str(value))
}

// Write_ upserts *entry* under *key*.
func (p *MongoPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "MongoPool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	p.Lock()
	defer p.Unlock()
	collection, err := p.Collection_()
	if err != nil {
		return err
	}
	return collection.Update_one(py.DictOf("key", key), py.DictOf("$set", py.DictOf("value", value)), true)
}

// Delete_ deletes the document for *key*.
func (p *MongoPool) Delete_(key string) error {
	p.Lock()
	defer p.Unlock()
	collection, err := p.Collection_()
	if err != nil {
		return err
	}
	return collection.Delete_one(py.DictOf("key", key))
}

// Empty_ removes all documents from the entries collection.
func (p *MongoPool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	collection, err := p.Collection_()
	if err != nil {
		return err
	}
	return collection.Delete_many(py.NewDict())
}

// Exists_ returns True if a document for *key* exists.
func (p *MongoPool) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	collection, err := p.Collection_()
	if err != nil {
		return false, err
	}
	n, err := collection.Count_documents(py.DictOf("key", key), 1)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *MongoPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the collection; ``as_generator=True`` yields
// a lazy Iterator instead of a list (the generator form does not hold
// the pool lock, exactly as Python's does not).
func (p *MongoPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	iter_keys := func() ([]string, error) {
		collection, err := p.Collection_()
		if err != nil {
			return nil, err
		}
		docs, err := collection.Find_sorted(py.NewDict(), py.DictOf("_id", 0, "key", 1), "key", 1)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(docs))
		for _, doc := range docs {
			k, _ := doc.Get("key")
			out = append(out, py.Str(k))
		}
		return out, nil
	}
	if !as_generator {
		p.Lock()
		defer p.Unlock()
		return keys_list_or_generator_(false, iter_keys)
	}
	return keys_list_or_generator_(true, iter_keys)
}
