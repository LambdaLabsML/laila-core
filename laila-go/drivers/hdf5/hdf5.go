//go:build cgo

// Package hdf5 is the ``hdf5`` extra (``pip install laila-core[hdf5]``):
// it binds laila.HDF5Pool to the HDF5 C library (cgo, located through
// ``pkg-config hdf5``) and writes exactly what ``h5py`` writes -- one
// scalar variable-length UTF-8 string dataset per key -- so files are
// interchangeable between the two ports. Import it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/hdf5"
//
// With ``CGO_ENABLED=0`` the package is empty and HDF5Pool is hidden
// (``Driver_available("h5py")`` is False) like Python without h5py.
package hdf5

/*
#cgo pkg-config: hdf5
#include <stdlib.h>
#include <string.h>
#include "hdf5.h"

static void laila_h5_quiet(void) { H5Eset_auto2(H5E_DEFAULT, NULL, NULL); }

// Open read/write, creating the file when it does not exist (h5py "a").
static hid_t laila_h5_open_append(const char *path) {
    hid_t f = -1;
    if (H5Fis_accessible(path, H5P_DEFAULT) > 0) {
        f = H5Fopen(path, H5F_ACC_RDWR, H5P_DEFAULT);
    }
    if (f < 0) {
        f = H5Fcreate(path, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
    }
    return f;
}

static int laila_h5_exists(hid_t file, const char *name) {
    htri_t r = H5Lexists(file, name, H5P_DEFAULT);
    return r > 0 ? 1 : (r == 0 ? 0 : -1);
}

// Write a scalar variable-length UTF-8 string dataset (h5py string_dtype).
static int laila_h5_write_vlen(hid_t file, const char *name, const char *value) {
    hid_t t = H5Tcopy(H5T_C_S1);
    if (t < 0) return -1;
    if (H5Tset_size(t, H5T_VARIABLE) < 0 || H5Tset_cset(t, H5T_CSET_UTF8) < 0) { H5Tclose(t); return -2; }
    hid_t s = H5Screate(H5S_SCALAR);
    if (s < 0) { H5Tclose(t); return -3; }
    hid_t d = H5Dcreate2(file, name, t, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (d < 0) { H5Sclose(s); H5Tclose(t); return -4; }
    const char *buf[1] = { value };
    herr_t w = H5Dwrite(d, t, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf);
    H5Dclose(d); H5Sclose(s); H5Tclose(t);
    return w < 0 ? -5 : 0;
}

// Read a string dataset (variable-length or fixed-length, any charset).
// On success *out is malloc'd (caller frees) and *len set; returns 0.
static int laila_h5_read_string(hid_t file, const char *name, char **out, size_t *len) {
    hid_t d = H5Dopen2(file, name, H5P_DEFAULT);
    if (d < 0) return -1;
    hid_t ft = H5Dget_type(d);
    if (ft < 0) { H5Dclose(d); return -2; }
    if (H5Tget_class(ft) != H5T_STRING) { H5Tclose(ft); H5Dclose(d); return -3; }
    int rc = 0;
    if (H5Tis_variable_str(ft) > 0) {
        hid_t mt = H5Tcopy(H5T_C_S1);
        H5Tset_size(mt, H5T_VARIABLE);
        H5Tset_cset(mt, H5Tget_cset(ft));
        char *buf[1] = { NULL };
        hid_t s = H5Dget_space(d);
        if (H5Dread(d, mt, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0) { rc = -4; }
        else {
            size_t n = buf[0] ? strlen(buf[0]) : 0;
            *out = (char *)malloc(n + 1);
            if (n) memcpy(*out, buf[0], n);
            (*out)[n] = 0;
            *len = n;
            H5Dvlen_reclaim(mt, s, H5P_DEFAULT, buf);
        }
        H5Sclose(s); H5Tclose(mt);
    } else {
        size_t n = H5Tget_size(ft);
        hid_t mt = H5Tcopy(ft);
        char *buf = (char *)calloc(n + 1, 1);
        if (H5Dread(d, mt, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0) { rc = -4; free(buf); }
        else {
            size_t m = strnlen(buf, n);
            *out = buf; *len = m; buf[m] = 0;
        }
        H5Tclose(mt);
    }
    H5Tclose(ft); H5Dclose(d);
    return rc;
}

static int laila_h5_delete(hid_t file, const char *name) {
    return H5Ldelete(file, name, H5P_DEFAULT) < 0 ? -1 : 0;
}

static long long laila_h5_nlinks(hid_t file) {
    H5G_info_t info;
    if (H5Gget_info(file, &info) < 0) return -1;
    return (long long)info.nlinks;
}

// Name of the i-th link in increasing name order (h5py iteration order).
static char *laila_h5_link_name(hid_t file, unsigned long long i) {
    ssize_t n = H5Lget_name_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, (hsize_t)i, NULL, 0, H5P_DEFAULT);
    if (n < 0) return NULL;
    char *buf = (char *)malloc((size_t)n + 1);
    if (H5Lget_name_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, (hsize_t)i, buf, (size_t)n + 1, H5P_DEFAULT) < 0) { free(buf); return NULL; }
    return buf;
}

static int laila_h5_flush(hid_t file) { return H5Fflush(file, H5F_SCOPE_GLOBAL) < 0 ? -1 : 0; }
static int laila_h5_close(hid_t file) { return H5Fclose(file) < 0 ? -1 : 0; }
*/
import "C"

import (
	"sync"
	"unsafe"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// H5Error is the error every failing HDF5 call raises (h5py raises
// OSError / KeyError / ValueError depending on the call; the pool only
// ever propagates them).
var H5Error = py.NewType("H5Error", py.OSError)

// The HDF5 library is not thread-safe unless built with
// ``--enable-threadsafe``; serialise every call process-wide.
var libMu sync.Mutex

// Driver implements laila.H5py_driver.
type Driver struct{}

// File is ``h5py.File(file_path, mode)``; only ``"a"`` (read/write,
// create if missing) and ``"r"`` / ``"r+"`` are supported.
func (Driver) File(file_path string, mode string) (laila.H5py_file, error) {
	libMu.Lock()
	defer libMu.Unlock()
	cpath := C.CString(file_path)
	defer C.free(unsafe.Pointer(cpath))
	var fid C.hid_t
	switch mode {
	case "a", "r+", "w", "":
		if mode == "w" {
			fid = C.H5Fcreate(cpath, C.H5F_ACC_TRUNC, C.H5P_DEFAULT, C.H5P_DEFAULT)
		} else {
			fid = C.laila_h5_open_append(cpath)
		}
	case "r":
		fid = C.H5Fopen(cpath, C.H5F_ACC_RDONLY, C.H5P_DEFAULT)
	default:
		return nil, py.ValueError.Newf("Invalid mode; must be one of r, r+, w, a: %s", py.Repr(mode))
	}
	if fid < 0 {
		return nil, H5Error.Newf("Unable to open file (%s)", file_path)
	}
	return &file{fid: fid, path: file_path}, nil
}

type file struct {
	fid  C.hid_t
	path string
}

func (f *file) check() error {
	if f.fid < 0 {
		return H5Error.New("Closed file")
	}
	return nil
}

func (f *file) Contains(name string) (bool, error) {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return false, err
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	switch C.laila_h5_exists(f.fid, cname) {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, H5Error.Newf("Unable to check link existence (%s)", name)
}

func (f *file) Read_string(name string) (string, error) {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return "", err
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var out *C.char
	var n C.size_t
	if rc := C.laila_h5_read_string(f.fid, cname, &out, &n); rc != 0 {
		if rc == -1 {
			return "", py.KeyError.Newf("Unable to open object (object '%s' doesn't exist)", name)
		}
		return "", H5Error.Newf("Unable to read dataset %s (rc=%d)", name, int(rc))
	}
	defer C.free(unsafe.Pointer(out))
	return C.GoStringN(out, C.int(n)), nil
}

func (f *file) Create_string_dataset(name string, value string) error {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))
	if rc := C.laila_h5_write_vlen(f.fid, cname, cvalue); rc != 0 {
		return H5Error.Newf("Unable to create dataset %s (rc=%d)", name, int(rc))
	}
	return nil
}

func (f *file) Delete(name string) error {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	if C.laila_h5_delete(f.fid, cname) != 0 {
		return py.KeyError.Newf("Couldn't delete link (%s)", name)
	}
	return nil
}

func (f *file) Keys() ([]string, error) {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	n := C.laila_h5_nlinks(f.fid)
	if n < 0 {
		return nil, H5Error.New("Unable to get group info")
	}
	keys := make([]string, 0, int(n))
	for i := C.ulonglong(0); i < C.ulonglong(n); i++ {
		cname := C.laila_h5_link_name(f.fid, i)
		if cname == nil {
			return nil, H5Error.Newf("Unable to get link name (index %d)", uint64(i))
		}
		keys = append(keys, C.GoString(cname))
		C.free(unsafe.Pointer(cname))
	}
	return keys, nil
}

func (f *file) Flush() error {
	libMu.Lock()
	defer libMu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if C.laila_h5_flush(f.fid) != 0 {
		return H5Error.New("Unable to flush file")
	}
	return nil
}

func (f *file) Close() error {
	libMu.Lock()
	defer libMu.Unlock()
	if f.fid < 0 {
		return nil
	}
	rc := C.laila_h5_close(f.fid)
	f.fid = -1
	if rc != 0 {
		return H5Error.New("Unable to close file")
	}
	return nil
}

func init() {
	C.laila_h5_quiet()
	laila.Register_driver("h5py", Driver{})
}
