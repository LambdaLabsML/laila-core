package laila

import (
	"sync"
	"errors"
	"os"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/huggingface/huggingface.py -- Hugging Face Hub pool
// implementation.

// Hf_api is the slice of ``huggingface_hub.HfApi`` the pool uses.
type Hf_api interface {
	// Upload_file is ``api.upload_file(path_or_fileobj=bytes, path_in_repo,
	// repo_id, repo_type, revision, commit_message)``.
	Upload_file(content []byte, path_in_repo string, repo_id string, repo_type string, revision string, commit_message string) error
	// Delete_file is ``api.delete_file(...)``; EntryNotFoundError when
	// absent.
	Delete_file(path_in_repo string, repo_id string, repo_type string, revision string, commit_message string) error
	// List_repo_files is ``api.list_repo_files(repo_id, repo_type,
	// revision, token)``.
	List_repo_files(repo_id string, repo_type string, revision string, token *string) ([]string, error)
}

// Hf_driver is the ``huggingface_hub`` module: ``HfApi(token)`` and
// ``hf_hub_download``. Registered under ``"huggingface_hub"`` by
// ``drivers/huggingface``.
type Hf_driver interface {
	HfApi(token *string) Hf_api
	// Hf_hub_download fetches a file into the local cache and returns its
	// path; EntryNotFoundError when the repo has no such file.
	Hf_hub_download(repo_id string, filename string, repo_type string, revision string, token *string) (string, error)
}

// EntryNotFoundError is ``huggingface_hub.utils.EntryNotFoundError``.
var EntryNotFoundError = py.NewType("EntryNotFoundError", py.Exception)

// huggingface_hub_ is the module global (nil when missing).
func huggingface_hub_() Hf_driver {
	d, _ := Driver_("huggingface_hub").(Hf_driver)
	return d
}

// HuggingFacePool is the Hugging Face Hub-backed pool.
//
// Stores each entry as one JSON object file in a Hub repo (public or
// private). Objects are keyed by entry global_id directly (URL-encoded).
type HuggingFacePool struct {
	LAILA_IDENTIFIABLE_POOL_

	Repo_id     string  `py:"repo_id,required"`
	Repo_type   string  `py:"repo_type" default:"model"`
	Revision    string  `py:"revision" default:"main"`
	Token       *string `py:"token"`
	Path_prefix string  `py:"path_prefix" default:"laila_pool"`

	Api_ Hf_api `py:"_api"`
	// Client_mu_ serialises lazy ``HfApi`` construction and Close. In Python
	// the ``if self._api is None`` check runs under the GIL (a lost race
	// at worst builds a second client); in Go the same unsynchronised
	// check is a data race, so the first-use build takes this mutex.
	Client_mu_ sync.Mutex `py:"-"`
}

// HuggingFacePool_class is the class descriptor (``_persistent_storage
// = True``, ``transformations`` defaults to Transformation_base64).
var HuggingFacePool_class = py.NewClass("HuggingFacePool", (*HuggingFacePool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_HuggingFacePool is ``HuggingFacePool(**data)``.
func New_HuggingFacePool(kw ...py.Kw) (*HuggingFacePool, error) {
	p := HuggingFacePool_class.New().(*HuggingFacePool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Get_api_ returns the ``HfApi`` instance, creating it on first call.
func (p *HuggingFacePool) Get_api_() (Hf_api, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Api_ != nil {
		return p.Api_, nil
	}
	if huggingface_hub_() == nil {
		return nil, py.ImportError.New("huggingface_hub is required for HuggingFacePool")
	}
	p.Api_ = huggingface_hub_().HfApi(p.Token)
	return p.Api_, nil
}

// Prefix_ returns the normalised path prefix (no leading/trailing
// slashes).
func (p *HuggingFacePool) Prefix_() string { return strings.Trim(p.Path_prefix, "/") }

// Object_key_ builds the in-repo file path for *key*.
func (p *HuggingFacePool) Object_key_(key string) string {
	encoded := py.Quote(key, "")
	if prefix := p.Prefix_(); prefix != "" {
		return prefix + "/" + encoded + ".json"
	}
	return encoded + ".json"
}

// Logical_key_ extracts the logical key from an in-repo file path.
func (p *HuggingFacePool) Logical_key_(object_key string) string {
	prefix := p.Prefix_()
	cleaned := object_key
	if prefix != "" && strings.HasPrefix(cleaned, prefix+"/") {
		cleaned = cleaned[len(prefix)+1:]
	}
	cleaned = strings.TrimSuffix(cleaned, ".json")
	return py.Unquote(cleaned)
}

// Close releases the API handle.
func (p *HuggingFacePool) Close() error {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	p.Api_ = nil
	return nil
}

// hf_download_ is ``hf_hub_download(...)`` with the module-missing
// ImportError of ``_get_api``.
func (p *HuggingFacePool) hf_download_(key string) (string, error) {
	if huggingface_hub_() == nil {
		return "", py.ImportError.New("huggingface_hub is required for HuggingFacePool")
	}
	return huggingface_hub_().Hf_hub_download(p.Repo_id, p.Object_key_(key), p.Repo_type, p.Revision, p.Token)
}

// Read_ downloads and parses the JSON file for *key*, or returns nil.
func (p *HuggingFacePool) Read_(key string) (any, error) {
	p.Lock()
	defer p.Unlock()
	path, err := p.hf_download_(key)
	if err != nil {
		if errors.Is(err, EntryNotFoundError) {
			return nil, nil
		}
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, py.FromGo(err)
	}
	return py.Json_loads(string(raw))
}

// Write_ uploads *entry* as a JSON file to the Hub repo.
func (p *HuggingFacePool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "HuggingFacePool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	p.Lock()
	defer p.Unlock()
	api, err := p.Get_api_()
	if err != nil {
		return err
	}
	return api.Upload_file([]byte(value), p.Object_key_(key), p.Repo_id, p.Repo_type, p.Revision, "laila: set "+key)
}

// Delete_ deletes the file for *key* from the Hub repo; no-op if absent.
func (p *HuggingFacePool) Delete_(key string) error {
	p.Lock()
	defer p.Unlock()
	return p.delete_locked_(key)
}

func (p *HuggingFacePool) delete_locked_(key string) error {
	api, err := p.Get_api_()
	if err != nil {
		return err
	}
	err = api.Delete_file(p.Object_key_(key), p.Repo_id, p.Repo_type, p.Revision, "laila: delete "+key)
	if err != nil && !errors.Is(err, EntryNotFoundError) {
		return err
	}
	return nil
}

// Empty_ removes all entries from the pool.
func (p *HuggingFacePool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	keys, err := p.list_keys_()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := p.delete_locked_(key); err != nil {
			return err
		}
	}
	return nil
}

// Exists_ returns True if *key* exists in the Hub repo.
func (p *HuggingFacePool) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	if _, err := p.hf_download_(key); err != nil {
		if errors.Is(err, EntryNotFoundError) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *HuggingFacePool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// list_keys_ is the eager body of ``_keys``: every ``<prefix>/*.json``
// path mapped back to its logical key.
func (p *HuggingFacePool) list_keys_() ([]string, error) {
	prefix := p.Prefix_()
	api, err := p.Get_api_()
	if err != nil {
		return nil, err
	}
	all_files, err := api.List_repo_files(p.Repo_id, p.Repo_type, p.Revision, p.Token)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, path := range all_files {
		if prefix != "" && !strings.HasPrefix(path, prefix+"/") {
			continue
		}
		if !strings.HasSuffix(path, ".json") {
			continue
		}
		out = append(out, p.Logical_key_(path))
	}
	return out, nil
}

// Keys_ returns all keys in the Hub repo under the configured prefix;
// ``as_generator=True`` yields a lazy Iterator instead of a list.
func (p *HuggingFacePool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	// ``list_repo_files`` is called eagerly in Python too; only the
	// filtering is lazy.
	keys, err := p.list_keys_()
	if err != nil {
		return nil, err
	}
	return keys_list_or_generator_(as_generator, func() ([]string, error) { return keys, nil })
}
