package laila

import (
	"sync"
	"errors"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/boto/boto.py -- abstract boto3-based S3-compatible pool
// implementation.
//
// BotoPool is the shared chassis used by every S3-API-compatible backend
// in laila (AWS S3, Cloudflare R2, BackBlaze B2, ...). Concrete
// subclasses provide the bucket-vendor-specific Get_client_ factory;
// everything else -- key encoding, throttling, async-via-aioboto3,
// connection-pool sizing, multi-loop client caching, and pool teardown
// -- is handled here.
//
// # Sync vs async paths
//
// Every method has both a sync (Read_, Write_, ...) implementation that
// drives a regular boto3 client and an async (Read_async_, ...)
// implementation that drives an aioboto3 client when ``async_default``
// is True. With ``async_default=False`` the async paths fall back to the
// inherited default of running the sync call inline on the calling
// event loop.
//
// # Per-loop async client caching
//
// ``aioboto3`` clients are bound to the event loop they're created on,
// so Python caches one client *per loop* in ``_aio_clients``. Go clients
// are loop-agnostic: the port keeps one shared async client (built from
// Get_aio_session_'s credentials and Aio_client_kwargs_) and drains it
// on Close exactly as Python drains every cached loop client.
//
// # Throttling
//
// Optional simple per-call sleep (Throttle_ / Athrottle_) to keep the
// pool under a request-per-second cap. Defaults to no throttling.

// Boto3_client_config is ``botocore.config.Config(signature_version,
// retries, max_pool_connections)``.
type Boto3_client_config struct {
	Signature_version    string
	Retries_max_attempts int
	Retries_mode         string
	Max_pool_connections int
}

// Boto3_client_kwargs is the keyword set of ``boto3.client("s3", ...)``
// / ``aioboto3.Session(...).client("s3", ...)``: nil pointers mean "not
// given" (the standard credential / region provider chain applies).
type Boto3_client_kwargs struct {
	Endpoint_url      *string
	Access_key_id     *string
	Secret_access_key *string
	Region_name       *string
	Config            Boto3_client_config
}

// Boto3_s3_client is the slice of the boto3 S3 client the pools use.
// Errors from the service are ClientError instances carrying the
// service error code under ``Attr("code")``.
type Boto3_s3_client interface {
	// Get_object is ``get_object(Bucket, Key)["Body"].read()``.
	Get_object(bucket string, key string) ([]byte, error)
	// Put_object is ``put_object(Bucket, Key, Body, ContentType)``.
	Put_object(bucket string, key string, body []byte, content_type string) error
	// Delete_object is ``delete_object(Bucket, Key)``.
	Delete_object(bucket string, key string) error
	// Head_object is ``head_object(Bucket, Key)``; any error means absent.
	Head_object(bucket string, key string) error
	// List_objects_v2 walks ``get_paginator("list_objects_v2")`` and
	// returns every object key in the bucket.
	List_objects_v2(bucket string) ([]string, error)
	// Close releases the client's connection pool.
	Close() error
}

// Boto3_driver is the ``boto3`` / ``aioboto3`` module pair: a factory for
// S3 clients. Registered under ``"boto3"`` by ``drivers/s3``.
type Boto3_driver interface {
	Client(kw Boto3_client_kwargs) (Boto3_s3_client, error)
}

// ClientError is ``botocore.exceptions.ClientError``; the service error
// code (``e.response["Error"]["Code"]``) is ``Attr("code")``.
var ClientError = py.NewType("ClientError", py.Exception)

// Client_error_code_ is ``e.response.get("Error", {}).get("Code")``.
func Client_error_code_(err error) string {
	var e *py.Error
	if errors.As(err, &e) && e.Type.Is(ClientError) {
		return py.Str(e.Attr("code"))
	}
	return ""
}

// boto3_ is the ``boto3`` module global (nil when the extra is missing).
func boto3_() Boto3_driver {
	d, _ := Driver_("boto3").(Boto3_driver)
	return d
}

// aioboto3_ is the ``aioboto3`` module global; the Go driver serves both.
func aioboto3_() Boto3_driver { return boto3_() }

// Boto_hooks_ is the subclass contract of BotoPool reached through
// ``Self()`` (Go has no virtual methods).
type Boto_hooks_ interface {
	Get_client_() (Boto3_s3_client, error)
	Get_aio_session_() (Boto3_client_kwargs, error)
	Aio_client_kwargs_() Boto3_client_kwargs
	No_such_key_codes_() []string
}

// BotoPool is the abstract base for pools backed by a boto3
// S3-compatible client.
//
// Subclasses must define their own fields and implement Get_client_ to
// return a configured boto3 S3 client.
//
// When ``async_default`` is True (the default) the async hooks
// (Read_async_ / Write_async_ / Delete_async_ / Exists_async_) use
// aioboto3 for true non-blocking I/O. When ``async_default`` is False,
// the async hooks fall back to running the sync boto3 call inline on the
// calling loop.
//
// # Connection-pool sizing
//
// ``max_pool_connections`` is forwarded to ``botocore.config.Config`` for
// both the sync and the async client, so a single client can hold open
// many concurrent HTTPS connections to S3 (botocore's default of 10 is
// the bottleneck for high-fanout async workloads).
type BotoPool struct {
	LAILA_IDENTIFIABLE_POOL_

	Bucket_name          string   `py:"bucket_name,required"`
	Max_req_per_second   *float64 `py:"max_req_per_second"`
	Async_default        bool     `py:"async_default" default:"true"`
	Max_pool_connections int      `py:"max_pool_connections,ge=1" default:"128"`

	Client_      Boto3_s3_client      `py:"_client"`
	Aio_session_ *Boto3_client_kwargs `py:"_aio_session"`
	// Per-event-loop cached aioboto3 clients in Python; one shared
	// client here. Drained on Close.
	Aio_clients_     Boto3_s3_client `py:"_aio_clients"`
	Aio_client_lock_ *py.Lock        `py:"_aio_client_locks"`
	// Client_mu_ serialises lazy client construction and Close. In Python
	// the ``if self._client is None`` check runs under the GIL (a lost race
	// at worst builds a second client); in Go the same unsynchronised
	// check is a data race, so the first-use build takes this mutex.
	Client_mu_ sync.Mutex `py:"-"`
}

// BotoPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64,
// ``arbitrary_types_allowed``).
var BotoPool_class = py.NewClass("BotoPool", (*BotoPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Set_private_default("_aio_client_locks", func() any { return py.NewLock() }).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_BotoPool is ``BotoPool(**data)``.
func New_BotoPool(kw ...py.Kw) (*BotoPool, error) {
	p := BotoPool_class.New().(*BotoPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Boto_pool_base_ exposes the embedded BotoPool to subclasses.
func (p *BotoPool) Boto_pool_base_() *BotoPool { return p }

// boto_hooks returns the subclass overrides through Self().
func (p *BotoPool) boto_hooks() Boto_hooks_ {
	if h, ok := p.Self().(Boto_hooks_); ok {
		return h
	}
	return p
}

// No_such_key_codes_ is ``_no_such_key_codes = {"NoSuchKey"}``.
func (p *BotoPool) No_such_key_codes_() []string { return []string{"NoSuchKey"} }

// Get_client_ returns a configured boto3 S3 client. NotImplementedError:
// must be overridden by subclasses.
func (p *BotoPool) Get_client_() (Boto3_s3_client, error) {
	return nil, py.NotImplementedError.New("Subclasses must implement _get_client")
}

// Get_aio_session_ returns (and lazily builds) the cached
// ``aioboto3.Session``: the credential set the async client is built
// from. Subclasses may override to inject credentials. The default
// constructs a session with no explicit credentials so the standard AWS
// credential chain applies.
func (p *BotoPool) Get_aio_session_() (Boto3_client_kwargs, error) {
	if aioboto3_() == nil {
		return Boto3_client_kwargs{}, py.ImportError.New(
			"aioboto3 is required for the async path; install with `pip install aioboto3`")
	}
	if p.Aio_session_ == nil {
		p.Aio_session_ = &Boto3_client_kwargs{}
	}
	return *p.Aio_session_, nil
}

// Aio_client_config_ returns the ``BotocoreConfig`` used for every
// aioboto3 client; honors ``max_pool_connections``.
func (p *BotoPool) Aio_client_config_() Boto3_client_config {
	return Boto3_client_config{
		Signature_version:    "s3v4",
		Retries_max_attempts: 3,
		Retries_mode:         "standard",
		Max_pool_connections: p.Max_pool_connections,
	}
}

// Aio_client_kwargs_ is the extra keyword set for ``session.client("s3",
// ...)`` on the async path. The default is empty (plain AWS S3);
// S3-compatible vendors override this to inject their ``endpoint_url``.
func (p *BotoPool) Aio_client_kwargs_() Boto3_client_kwargs { return Boto3_client_kwargs{} }

// Get_shared_aio_client_ returns (lazily creating) the aioboto3 S3
// client (``_aio_client`` / ``_get_shared_aio_client``).
func (p *BotoPool) Get_shared_aio_client_() (Boto3_s3_client, error) {
	p.Aio_client_lock_.Acquire(true, -1)
	defer p.Aio_client_lock_.Release()
	if p.Aio_clients_ != nil {
		return p.Aio_clients_, nil
	}
	session, err := p.boto_hooks().Get_aio_session_()
	if err != nil {
		return nil, err
	}
	kw := p.boto_hooks().Aio_client_kwargs_()
	if kw.Access_key_id == nil {
		kw.Access_key_id = session.Access_key_id
	}
	if kw.Secret_access_key == nil {
		kw.Secret_access_key = session.Secret_access_key
	}
	if kw.Region_name == nil {
		kw.Region_name = session.Region_name
	}
	kw.Config = p.Aio_client_config_()
	client, err := aioboto3_().Client(kw)
	if err != nil {
		return nil, err
	}
	p.Aio_clients_ = client
	return client, nil
}

// Object_key_ URL-encodes a logical key for S3.
func (p *BotoPool) Object_key_(key string) string { return py.Quote(key, "") }

// Logical_key_ decodes an S3 object key back to the logical key.
func (p *BotoPool) Logical_key_(object_key string) string { return py.Unquote(object_key) }

// Throttle_ sleeps to enforce the per-pool request rate cap.
func (p *BotoPool) Throttle_() {
	if p.Max_req_per_second != nil {
		py.Sleep_sync(1.0 / *p.Max_req_per_second)
	}
}

// Athrottle_ is the async sleep variant of Throttle_ (yields to the
// event loop).
func (p *BotoPool) Athrottle_() error {
	if p.Max_req_per_second != nil {
		return py.Sleep(1.0 / *p.Max_req_per_second)
	}
	return nil
}

// Close releases the boto3 client and tears down all cached aioboto3
// clients.
func (p *BotoPool) Close() error {
	p.Client_mu_.Lock()
	if p.Client_ != nil {
		_ = p.Client_.Close()
	}
	p.Client_ = nil
	p.Client_mu_.Unlock()
	p.Aio_client_lock_.Acquire(true, -1)
	if p.Aio_clients_ != nil {
		func() {
			defer func() { _ = recover() }()
			_ = p.Aio_clients_.Close()
		}()
	}
	p.Aio_clients_ = nil
	p.Aio_client_lock_.Release()
	p.Aio_session_ = nil
	return nil
}

// is_no_such_key_ is ``e.response["Error"]["Code"] in _no_such_key_codes``.
func (p *BotoPool) is_no_such_key_(err error) bool {
	if !errors.Is(err, ClientError) {
		return false
	}
	code := Client_error_code_(err)
	for _, c := range p.boto_hooks().No_such_key_codes_() {
		if c == code {
			return true
		}
	}
	return false
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *BotoPool) Read_(key string) (any, error) {
	p.Throttle_()
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return nil, err
	}
	raw, err := client.Get_object(p.Bucket_name, p.Object_key_(key))
	if err != nil {
		if p.is_no_such_key_(err) {
			return nil, nil
		}
		return nil, err
	}
	return py.Json_loads(string(raw))
}

// Write_ stores *entry* as a JSON object under *key*.
func (p *BotoPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, py.Pytypename(p.Self())+" expects a serialized JSON string.")
	if err != nil {
		return err
	}
	p.Throttle_()
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return err
	}
	return client.Put_object(p.Bucket_name, p.Object_key_(key), []byte(value), "application/json")
}

// Delete_ deletes the object for *key*.
func (p *BotoPool) Delete_(key string) error {
	p.Throttle_()
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return err
	}
	return client.Delete_object(p.Bucket_name, p.Object_key_(key))
}

// Empty_ removes all entries from the pool.
func (p *BotoPool) Empty_() error {
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return err
	}
	keys, err := client.List_objects_v2(p.Bucket_name)
	if err != nil {
		return err
	}
	for _, object_key := range keys {
		p.Throttle_()
		if err := client.Delete_object(p.Bucket_name, object_key); err != nil {
			return err
		}
	}
	return nil
}

// Exists_ returns True if *key* exists in the bucket.
func (p *BotoPool) Exists_(key string) (bool, error) {
	p.Throttle_()
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return false, nil
	}
	return client.Head_object(p.Bucket_name, p.Object_key_(key)) == nil, nil
}

// -------- Async hooks: true async via aioboto3 when async_default=True --------

// Read_async_ retrieves the JSON value for *key* asynchronously, or nil
// if absent.
func (p *BotoPool) Read_async_(key string) (any, error) {
	if !p.Async_default || aioboto3_() == nil {
		return p.LAILA_IDENTIFIABLE_POOL_.Read_async_(key)
	}
	if err := p.Athrottle_(); err != nil {
		return nil, err
	}
	client, err := p.Get_shared_aio_client_()
	if err != nil {
		return nil, err
	}
	raw, err := client.Get_object(p.Bucket_name, p.Object_key_(key))
	if err != nil {
		if p.is_no_such_key_(err) {
			return nil, nil
		}
		return nil, err
	}
	return py.Json_loads(string(raw))
}

// Write_async_ stores *entry* as a JSON object under *key* asynchronously.
func (p *BotoPool) Write_async_(key string, entry any) error {
	if !p.Async_default || aioboto3_() == nil {
		return p.LAILA_IDENTIFIABLE_POOL_.Write_async_(key, entry)
	}
	value, err := json_blob_(entry, py.Pytypename(p.Self())+" expects a serialized JSON string.")
	if err != nil {
		return err
	}
	if err := p.Athrottle_(); err != nil {
		return err
	}
	client, err := p.Get_shared_aio_client_()
	if err != nil {
		return err
	}
	return client.Put_object(p.Bucket_name, p.Object_key_(key), []byte(value), "application/json")
}

// Delete_async_ deletes the object for *key* asynchronously.
func (p *BotoPool) Delete_async_(key string) error {
	if !p.Async_default || aioboto3_() == nil {
		return p.LAILA_IDENTIFIABLE_POOL_.Delete_async_(key)
	}
	if err := p.Athrottle_(); err != nil {
		return err
	}
	client, err := p.Get_shared_aio_client_()
	if err != nil {
		return err
	}
	return client.Delete_object(p.Bucket_name, p.Object_key_(key))
}

// Exists_async_ returns True if *key* exists in the bucket (async).
func (p *BotoPool) Exists_async_(key string) (bool, error) {
	if !p.Async_default || aioboto3_() == nil {
		return p.LAILA_IDENTIFIABLE_POOL_.Exists_async_(key)
	}
	if err := p.Athrottle_(); err != nil {
		return false, err
	}
	client, err := p.Get_shared_aio_client_()
	if err != nil {
		return false, nil
	}
	return client.Head_object(p.Bucket_name, p.Object_key_(key)) == nil, nil
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *BotoPool) Contains(key any) (bool, error) { return p.hooks().Exists_(py.Str(key)) }

// Keys_ returns all keys in the bucket (``_keys(as_generator=False)``);
// with ``as_generator=True`` a lazy Iterator is returned instead.
func (p *BotoPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	client, err := p.boto_hooks().Get_client_()
	if err != nil {
		return nil, err
	}
	iter_keys := func() ([]string, error) {
		object_keys, err := client.List_objects_v2(p.Bucket_name)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(object_keys))
		for _, k := range object_keys {
			out = append(out, p.Logical_key_(k))
		}
		return out, nil
	}
	if !as_generator {
		keys, err := iter_keys()
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
		keys, err := iter_keys()
		if err != nil {
			yield(nil, err)
			return
		}
		for _, k := range keys {
			if !yield(k, nil) {
				return
			}
		}
	}
	return gen, nil
}
