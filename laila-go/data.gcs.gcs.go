package laila

import (
	"sync"
	"errors"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/gcs/gcs.py -- Google Cloud Storage pool implementation.

// Gcs_client is the slice of ``google.cloud.storage.Client`` (and the
// bucket / blob handles hanging off it) the pool uses. Missing objects
// surface as NotFound.
type Gcs_client interface {
	// Download_as_text is ``bucket.blob(name).download_as_text()``.
	Download_as_text(bucket string, name string) (string, error)
	// Upload_from_string is ``blob.upload_from_string(data, content_type)``.
	Upload_from_string(bucket string, name string, data string, content_type string) error
	// Delete is ``blob.delete()``.
	Delete(bucket string, name string) error
	// Exists is ``blob.exists(client)``.
	Exists(bucket string, name string) (bool, error)
	// List_blobs is ``client.list_blobs(bucket)`` -> blob names.
	List_blobs(bucket string) ([]string, error)
	// Close releases the client.
	Close() error
}

// Gcs_driver is the ``google.cloud.storage`` module: ``storage.Client(
// credentials=service_account.Credentials.from_service_account_info(info),
// project=project_id)``. Registered under ``"google.cloud.storage"`` by
// ``drivers/gcs``.
type Gcs_driver interface {
	Client(service_account_info *py.Dict, project_id *string) (Gcs_client, error)
}

// NotFound is ``google.api_core.exceptions.NotFound``.
var NotFound = py.NewType("NotFound", py.Exception)

// gcs_storage_ is the ``storage`` module global (nil when missing).
func gcs_storage_() Gcs_driver {
	d, _ := Driver_("google.cloud.storage").(Gcs_driver)
	return d
}

// GCSPool is the Google Cloud Storage-backed pool.
//
// Entries are stored as JSON objects in a GCS bucket. Authentication can
// use explicit service-account info or Application Default Credentials.
type GCSPool struct {
	LAILA_IDENTIFIABLE_POOL_

	Service_account_info *py.Dict `py:"service_account_info"`
	Project_id           *string  `py:"project_id"`
	Bucket_name          string   `py:"bucket_name,required"`

	Client_ Gcs_client `py:"_client"`
	// Client_mu_ serialises lazy client construction and Close. In Python
	// the ``if self._client is None`` check runs under the GIL (a lost race
	// at worst builds a second client); in Go the same unsynchronised
	// check is a data race, so the first-use build takes this mutex.
	Client_mu_ sync.Mutex `py:"-"`
	Bucket_ *string    `py:"_bucket"`
}

// GCSPool_class is the class descriptor (``_persistent_storage = True``,
// ``transformations`` defaults to Transformation_base64).
var GCSPool_class = py.NewClass("GCSPool", (*GCSPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_GCSPool is ``GCSPool(**data)``.
func New_GCSPool(kw ...py.Kw) (*GCSPool, error) {
	p := GCSPool_class.New().(*GCSPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Get_client_ returns the GCS ``storage.Client``, creating it on first
// call.
func (p *GCSPool) Get_client_() (Gcs_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	return p.get_client_locked_()
}

// get_client_locked_ is Get_client_ with Client_mu_ already held.
func (p *GCSPool) get_client_locked_() (Gcs_client, error) {
	if p.Client_ != nil {
		return p.Client_, nil
	}
	if gcs_storage_() == nil {
		return nil, py.ImportError.New("google-cloud-storage is required for GCSPool")
	}
	if p.Service_account_info != nil && p.Project_id == nil {
		if pid, ok := p.Service_account_info.Get("project_id"); ok && !py.IsNone(pid) {
			s := py.Str(pid)
			p.Project_id = &s
		}
	}
	client, err := gcs_storage_().Client(p.Service_account_info, p.Project_id)
	if err != nil {
		return nil, err
	}
	p.Client_ = client
	return client, nil
}

// Get_bucket_ returns the GCS bucket handle (the bucket name: Go blob
// calls take it explicitly).
func (p *GCSPool) Get_bucket_() (string, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Bucket_ == nil {
		if _, err := p.get_client_locked_(); err != nil {
			return "", err
		}
		b := p.Bucket_name
		p.Bucket_ = &b
	}
	return *p.Bucket_, nil
}

// Object_key_ URL-encodes a logical key for GCS.
func (p *GCSPool) Object_key_(key string) string { return py.Quote(key, "") }

// Logical_key_ decodes a GCS object name back to the logical key.
func (p *GCSPool) Logical_key_(object_key string) string { return py.Unquote(object_key) }

// Close releases client references.
func (p *GCSPool) Close() error {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Client_ != nil {
		_ = p.Client_.Close()
	}
	p.Client_ = nil
	p.Bucket_ = nil
	return nil
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *GCSPool) Read_(key string) (any, error) {
	bucket, err := p.Get_bucket_()
	if err != nil {
		return nil, err
	}
	raw, err := p.Client_.Download_as_text(bucket, p.Object_key_(key))
	if err != nil {
		if errors.Is(err, NotFound) {
			return nil, nil
		}
		return nil, err
	}
	return py.Json_loads(raw)
}

// Write_ stores *entry* as a JSON blob under *key*.
func (p *GCSPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "GCSPool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	bucket, err := p.Get_bucket_()
	if err != nil {
		return err
	}
	return p.Client_.Upload_from_string(bucket, p.Object_key_(key), value, "application/json")
}

// Delete_ deletes the blob for *key*; no-op if absent.
func (p *GCSPool) Delete_(key string) error {
	bucket, err := p.Get_bucket_()
	if err != nil {
		return err
	}
	if err := p.Client_.Delete(bucket, p.Object_key_(key)); err != nil && !errors.Is(err, NotFound) {
		return err
	}
	return nil
}

// Empty_ removes all blobs from the bucket.
func (p *GCSPool) Empty_() error {
	client, err := p.Get_client_()
	if err != nil {
		return err
	}
	blobs, err := client.List_blobs(p.Bucket_name)
	if err != nil {
		return err
	}
	for _, name := range blobs {
		if err := client.Delete(p.Bucket_name, name); err != nil {
			return err
		}
	}
	return nil
}

// Exists_ returns True if a blob for *key* exists.
func (p *GCSPool) Exists_(key string) (bool, error) {
	bucket, err := p.Get_bucket_()
	if err != nil {
		return false, err
	}
	return p.Client_.Exists(bucket, p.Object_key_(key))
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *GCSPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the bucket; ``as_generator=True`` yields a
// lazy Iterator instead of a list.
func (p *GCSPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	client, err := p.Get_client_()
	if err != nil {
		return nil, err
	}
	iter_keys := func() ([]string, error) {
		names, err := client.List_blobs(p.Bucket_name)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(names))
		for _, n := range names {
			out = append(out, p.Logical_key_(n))
		}
		return out, nil
	}
	return keys_list_or_generator_(as_generator, iter_keys)
}

// keys_list_or_generator_ is the shared ``list(_iter_keys())`` /
// ``_iter_keys()`` tail of the cloud pools' ``_keys``.
func keys_list_or_generator_(as_generator bool, iter_keys func() ([]string, error)) (any, error) {
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
