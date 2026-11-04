package laila

import (
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/hdf5/hdf5.py -- HDF5 file pool implementation.
//
// Python imports ``h5py`` unguarded, so without the extra the module
// fails to import and ``laila.data`` has no HDF5Pool. The Go class always
// exists; ``Driver_available("h5py")`` is the ``HAVE_HDF5POOL`` check and
// construction raises ImportError when the driver is missing.

// H5py_file is the slice of ``h5py.File`` (the root group) the pool
// uses. Datasets are variable-length UTF-8 strings.
type H5py_file interface {
	// Contains is ``name in root``.
	Contains(name string) (bool, error)
	// Read_string is ``root[name][()]`` decoded as UTF-8.
	Read_string(name string) (string, error)
	// Create_string_dataset is ``root.create_dataset(name, data=value,
	// dtype=h5py.string_dtype("utf-8"))``.
	Create_string_dataset(name string, value string) error
	// Delete is ``del root[name]``.
	Delete(name string) error
	// Keys is ``list(root.keys())``.
	Keys() ([]string, error)
	// Flush is ``root.flush()`` (library-level flush, not fsync).
	Flush() error
	// Close is ``file.close()``.
	Close() error
}

// H5py_driver is the ``h5py`` module: ``h5py.File(path, mode)``.
// Registered under ``"h5py"`` by ``drivers/hdf5``.
type H5py_driver interface {
	File(file_path string, mode string) (H5py_file, error)
}

// h5py_ is the module global (nil when the extra is missing).
func h5py_() H5py_driver {
	d, _ := Driver_("h5py").(H5py_driver)
	return d
}

// HDF5Pool is the HDF5 file-backed pool.
//
// Each entry is stored as a UTF-8 string dataset inside a single HDF5
// file that remains open for the lifetime of the pool.
type HDF5Pool struct {
	LAILA_IDENTIFIABLE_POOL_

	File_path *string `py:"file_path"`

	File_ H5py_file `py:"_file"`
}

// HDF5Pool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64).
var HDF5Pool_class = py.NewClass("HDF5Pool", (*HDF5Pool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_HDF5Pool is ``HDF5Pool(**data)``.
func New_HDF5Pool(kw ...py.Kw) (*HDF5Pool, error) {
	p := HDF5Pool_class.New().(*HDF5Pool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Resolve_memory_global_id_ returns the active policy's memory global ID.
func (p *HDF5Pool) Resolve_memory_global_id_() (string, error) {
	policy, err := Get_active_policy()
	if err != nil {
		return "", err
	}
	return py.Str(getattr(getattr(getattr(policy, "central"), "memory"), "global_id")), nil
}

// Model_post_init opens (or creates) the HDF5 file.
func (p *HDF5Pool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	if h5py_() == nil {
		return py.ModuleNotFoundError.New("No module named 'h5py'")
	}
	if p.File_path == nil {
		pool_dir := filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
		if err := py.Mkdir_p(pool_dir); err != nil {
			return err
		}
		fp := filepath.Join(pool_dir, "pool.h5py")
		p.File_path = &fp
	} else {
		fp := py.Expanduser(*p.File_path)
		if err := py.Mkdir_p(filepath.Dir(fp)); err != nil {
			return err
		}
		p.File_path = &fp
	}
	// Open file and keep it open for the lifetime of the pool.
	file, err := h5py_().File(*p.File_path, "a")
	if err != nil {
		return err
	}
	p.File_ = file
	return nil
}

// Root_ returns the root HDF5 group; RuntimeError once closed.
func (p *HDF5Pool) Root_() (H5py_file, error) {
	if p.File_ == nil {
		return nil, py.RuntimeError.New("HDF5Pool is closed.")
	}
	return p.File_, nil
}

// ---------------- internal helpers ----------------

// Storage_key_ URL-encodes *key* to avoid ``/`` collisions in HDF5 paths.
func (p *HDF5Pool) Storage_key_(key string) string { return py.Quote(key, "") }

// Logical_key_ decodes a storage key back to the logical key.
func (p *HDF5Pool) Logical_key_(key string) string { return py.Unquote(key) }

// Read_raw_ reads the raw UTF-8 string for *key*, or nil.
func (p *HDF5Pool) Read_raw_(key string) (*string, error) {
	skey := p.Storage_key_(key)
	root, err := p.Root_()
	if err != nil {
		return nil, err
	}
	ok, err := root.Contains(skey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	raw, err := root.Read_string(skey)
	if err != nil {
		return nil, err
	}
	return &raw, nil
}

// Close closes the HDF5 file handle.
//
// Runs under the pool lock so an in-flight Write_/Delete_ finishes
// before the handle is released; later writers hit the documented
// ``RuntimeError("HDF5Pool is closed.")`` instead of crashing inside
// h5py.
func (p *HDF5Pool) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.File_ != nil {
		err := p.File_.Close()
		p.File_ = nil
		return err
	}
	return nil
}

// ---------------- mapping API ----------------

// Read_ retrieves the JSON value for *key*, or nil if absent.
func (p *HDF5Pool) Read_(key string) (any, error) {
	p.Lock()
	raw, err := p.Read_raw_(key)
	p.Unlock()
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	return py.Json_loads(*raw)
}

// Write_ stores *entry* as a UTF-8 HDF5 dataset under *key*.
func (p *HDF5Pool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "HDF5Pool expects a serialized JSON string.")
	if err != nil {
		return err
	}
	skey := p.Storage_key_(key)
	p.Lock()
	defer p.Unlock()
	root, err := p.Root_()
	if err != nil {
		return err
	}
	if ok, err := root.Contains(skey); err != nil {
		return err
	} else if ok {
		if err := root.Delete(skey); err != nil {
			return err
		}
	}
	if err := root.Create_string_dataset(skey, value); err != nil {
		return err
	}
	// Push the write through the HDF5 library buffers so a process that
	// dies without ``close()`` (crash, ``os._exit``, SIGKILL) still leaves
	// a readable file. Library-level flush, not fsync.
	return root.Flush()
}

// Delete_ deletes the dataset for *key* if it exists.
func (p *HDF5Pool) Delete_(key string) error {
	skey := p.Storage_key_(key)
	p.Lock()
	defer p.Unlock()
	root, err := p.Root_()
	if err != nil {
		return err
	}
	ok, err := root.Contains(skey)
	if err != nil {
		return err
	}
	if ok {
		if err := root.Delete(skey); err != nil {
			return err
		}
		return root.Flush()
	}
	return nil
}

// Empty_ removes all entries from the pool.
func (p *HDF5Pool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	root, err := p.Root_()
	if err != nil {
		return err
	}
	names, err := root.Keys()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := root.Delete(name); err != nil {
			return err
		}
	}
	return root.Flush()
}

// Exists_ returns True if a dataset for *key* exists.
func (p *HDF5Pool) Exists_(key string) (bool, error) {
	skey := p.Storage_key_(key)
	p.Lock()
	defer p.Unlock()
	root, err := p.Root_()
	if err != nil {
		return false, err
	}
	return root.Contains(skey)
}

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *HDF5Pool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the HDF5 file; ``as_generator=True`` yields a
// lazy Iterator instead of a list.
func (p *HDF5Pool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	fetch := func() ([]string, error) {
		p.Lock()
		defer p.Unlock()
		root, err := p.Root_()
		if err != nil {
			return nil, err
		}
		skeys, err := root.Keys()
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(skeys))
		for _, k := range skeys {
			out = append(out, p.Logical_key_(k))
		}
		return out, nil
	}
	return keys_list_or_generator_(as_generator, fetch)
}
