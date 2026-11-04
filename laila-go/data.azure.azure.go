package laila

import (
	"sync"
	"errors"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/azure/azure.py -- Azure Blob Storage pool implementation.

// Azure_container_client is the slice of ``azure.storage.blob``'s
// ContainerClient / BlobClient the pool uses. Missing blobs surface as
// ResourceNotFoundError, an existing container as ResourceExistsError.
type Azure_container_client interface {
	// Create_container is ``container.create_container()``.
	Create_container() error
	// Download_blob is ``get_blob_client(name).download_blob().readall()``.
	Download_blob(name string) ([]byte, error)
	// Upload_blob is ``blob.upload_blob(data, overwrite, content_type)``.
	Upload_blob(name string, data []byte, overwrite bool, content_type string) error
	// Delete_blob is ``blob.delete_blob()``.
	Delete_blob(name string) error
	// Exists is ``blob.exists()``.
	Exists(name string) (bool, error)
	// List_blobs is ``container.list_blobs()`` -> blob names.
	List_blobs() ([]string, error)
}

// Azure_blob_service_client is ``BlobServiceClient``.
type Azure_blob_service_client interface {
	Get_container_client(container_name string) (Azure_container_client, error)
	Close() error
}

// Azure_driver is the ``azure.storage.blob`` module:
// ``BlobServiceClient.from_connection_string``. Registered under
// ``"azure.storage.blob"`` by ``drivers/azure``.
type Azure_driver interface {
	From_connection_string(connection_string string) (Azure_blob_service_client, error)
}

// ResourceNotFoundError / ResourceExistsError are
// ``azure.core.exceptions.*``.
var (
	ResourceNotFoundError = py.NewType("ResourceNotFoundError", py.Exception)
	ResourceExistsError   = py.NewType("ResourceExistsError", py.Exception)
)

// azure_blob_ is the ``BlobServiceClient`` module global (nil when
// missing).
func azure_blob_() Azure_driver {
	d, _ := Driver_("azure.storage.blob").(Azure_driver)
	return d
}

// AzurePool is the Azure Blob Storage-backed pool.
//
// Each entry is stored as a JSON blob in the configured container. Keys
// are URL-encoded to avoid path-separator issues.
type AzurePool struct {
	LAILA_IDENTIFIABLE_POOL_

	Connection_string string `py:"connection_string,required"`
	Container_name    string `py:"container_name,required"`

	Client_    Azure_blob_service_client `py:"_client"`
	Container_ Azure_container_client    `py:"_container"`
	// Client_mu_ serialises lazy client construction and Close. In Python
	// the ``if self._client is None`` check runs under the GIL (a lost race
	// at worst builds a second client); in Go the same unsynchronised
	// check is a data race, so the first-use build takes this mutex.
	Client_mu_ sync.Mutex `py:"-"`
}

// AzurePool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64).
var AzurePool_class = py.NewClass("AzurePool", (*AzurePool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_AzurePool is ``AzurePool(**data)``.
func New_AzurePool(kw ...py.Kw) (*AzurePool, error) {
	p := AzurePool_class.New().(*AzurePool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Get_client_ returns the ``BlobServiceClient``, creating it on first
// call.
func (p *AzurePool) Get_client_() (Azure_blob_service_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	return p.get_client_locked_()
}

// get_client_locked_ is Get_client_ with Client_mu_ already held.
func (p *AzurePool) get_client_locked_() (Azure_blob_service_client, error) {
	if p.Client_ != nil {
		return p.Client_, nil
	}
	if azure_blob_() == nil {
		return nil, py.ImportError.New("azure-storage-blob is required for AzurePool")
	}
	client, err := azure_blob_().From_connection_string(p.Connection_string)
	if err != nil {
		return nil, err
	}
	p.Client_ = client
	return client, nil
}

// Get_container_ returns the container client, creating the container
// if needed.
func (p *AzurePool) Get_container_() (Azure_container_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Container_ == nil {
		client, err := p.get_client_locked_()
		if err != nil {
			return nil, err
		}
		container, err := client.Get_container_client(p.Container_name)
		if err != nil {
			return nil, err
		}
		p.Container_ = container
		if err := container.Create_container(); err != nil && !errors.Is(err, ResourceExistsError) {
			return nil, err
		}
	}
	return p.Container_, nil
}

// Object_key_ URL-encodes a logical key for blob storage.
func (p *AzurePool) Object_key_(key string) string { return py.Quote(key, "") }

// Logical_key_ decodes a blob name back to the logical key.
func (p *AzurePool) Logical_key_(object_key string) string { return py.Unquote(object_key) }

// Close releases client references.
func (p *AzurePool) Close() error {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Client_ != nil {
		_ = p.Client_.Close()
	}
	p.Client_ = nil
	p.Container_ = nil
	return nil
}

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *AzurePool) Read_(key string) (any, error) {
	container, err := p.Get_container_()
	if err != nil {
		return nil, err
	}
	raw, err := container.Download_blob(p.Object_key_(key))
	if err != nil {
		if errors.Is(err, ResourceNotFoundError) {
			return nil, nil
		}
		return nil, err
	}
	return py.Json_loads(string(raw))
}

// Write_ stores *entry* as a JSON blob under *key*.
func (p *AzurePool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "AzurePool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	container, err := p.Get_container_()
	if err != nil {
		return err
	}
	return container.Upload_blob(p.Object_key_(key), []byte(value), true, "application/json")
}

// Delete_ deletes the blob for *key*; no-op if absent.
func (p *AzurePool) Delete_(key string) error {
	container, err := p.Get_container_()
	if err != nil {
		return err
	}
	if err := container.Delete_blob(p.Object_key_(key)); err != nil && !errors.Is(err, ResourceNotFoundError) {
		return err
	}
	return nil
}

// Empty_ removes all blobs from the container.
func (p *AzurePool) Empty_() error {
	container, err := p.Get_container_()
	if err != nil {
		return err
	}
	blobs, err := container.List_blobs()
	if err != nil {
		return err
	}
	for _, name := range blobs {
		if err := container.Delete_blob(name); err != nil {
			return err
		}
	}
	return nil
}

// Exists_ returns True if a blob for *key* exists.
func (p *AzurePool) Exists_(key string) (bool, error) {
	container, err := p.Get_container_()
	if err != nil {
		return false, err
	}
	return container.Exists(p.Object_key_(key))
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *AzurePool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the container; ``as_generator=True`` yields
// a lazy Iterator instead of a list.
func (p *AzurePool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	container, err := p.Get_container_()
	if err != nil {
		return nil, err
	}
	iter_keys := func() ([]string, error) {
		names, err := container.List_blobs()
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
