package laila

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/filesystem/filesystem.py -- loopback-mounted ext4 filesystem pool
// implementation.
//
// A FilesystemPool writes each entry as a single JSON file inside a
// private, loopback-mounted ext4 image. The image is created on first
// use, mounted via ``mount -o loop``, and not unmounted by laila (so
// subsequent process runs can reuse it).
//
// Why an image-backed mount instead of a plain directory?
//
//   - It bounds the pool's footprint to a fixed size (Image_size_bytes_,
//     64 MiB by default) so a runaway producer can't fill the host disk.
//   - It provides POSIX-level isolation -- the pool's contents share an
//     independent inode table from the host filesystem and can be
//     detached/relocated as a single ``.img`` file.
//
// The class is Linux-only (relies on ``mkfs.ext4`` and ``mount``) and
// needs root privileges or appropriate capabilities to mount.

// FilesystemPool is the pool backed by JSON files on a loopback-mounted
// ext4 image.
//
// Each entry lives in a single ``<urlencoded-key>.json`` file inside the
// pool's mount directory. Keys are URL-encoded so any character is safe
// for use in a filename, and decoded on enumeration via Logical_key_.
//
// The image is created with Create_image_file_ (``truncate`` +
// ``mkfs.ext4``) and mounted via Mount_image_ on first construction.
// Subsequent constructions detect the mount via ``/proc/self/mountinfo``
// and skip both steps.
//
// The constructor refuses any explicit ``image_dir`` / ``image_path`` /
// ``mount_dir`` overrides because the pool computes those itself from
// LAILA_DEFAULT_DIRECTORIES and its own UUID -- letting callers override
// them would silently break the "one image file per pool, name derived
// from pool UUID" invariant.
//
// The default ``transformations`` is Transformation_base64, which gives
// the JSON serialiser pure-ASCII payloads (avoiding encoding surprises
// across mount platforms).
type FilesystemPool struct {
	LAILA_IDENTIFIABLE_POOL_

	Pool_dir_         string `py:"_pool_dir"`
	Mount_dir_        string `py:"_mount_dir"`
	Image_path_       string `py:"_image_path"`
	Image_size_bytes_ int    `py:"_image_size_bytes"`
}

// FilesystemPool_class is the class descriptor (``_persistent_storage =
// True``, ``transformations`` defaults to Transformation_base64,
// ``arbitrary_types_allowed``).
var FilesystemPool_class = py.NewClass("FilesystemPool", (*FilesystemPool)(nil), LAILA_IDENTIFIABLE_POOL_class).
	Set_classvar("_persistent_storage", true).
	Set_private_default("_image_size_bytes", func() any { return 64 * 1024 * 1024 }).
	Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
		if !data.Has("transformations") {
			data["transformations"] = Transformation_base64
		}
		return data, nil
	})

// New_FilesystemPool is ``FilesystemPool(**data)``.
func New_FilesystemPool(kw ...py.Kw) (*FilesystemPool, error) {
	p := FilesystemPool_class.New().(*FilesystemPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Filesystem_pool_base_ exposes the embedded FilesystemPool to subclasses.
func (p *FilesystemPool) Filesystem_pool_base_() *FilesystemPool { return p }

// Init_ validates that reserved path fields are not overridden
// (``FilesystemPool.__init__``).
func (p *FilesystemPool) Init_(data py.Kw) error {
	if data.Has("image_dir") {
		return py.ValueError.New("FilesystemPool storage path is fixed and cannot be overridden.")
	}
	if data.Has("image_path") {
		return py.ValueError.New("FilesystemPool image_path is fixed and cannot be overridden.")
	}
	if data.Has("mount_dir") {
		return py.ValueError.New("FilesystemPool mount_dir is fixed and cannot be overridden.")
	}
	return p.LAILA_IDENTIFIABLE_POOL_.Init_(data)
}

// Pool_dir is the root directory for this pool's artefacts.
func (p *FilesystemPool) Pool_dir() string { return p.Pool_dir_ }

// Mount_dir is the mount-point directory where entries are stored.
func (p *FilesystemPool) Mount_dir() string { return p.Mount_dir_ }

// Image_path is the path to the ext4 image file.
func (p *FilesystemPool) Image_path() string { return p.Image_path_ }

// Model_post_init creates or mounts the filesystem image.
func (p *FilesystemPool) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_POOL_.Model_post_init(ctx); err != nil {
		return err
	}
	if p.Image_size_bytes_ == 0 {
		p.Image_size_bytes_ = 64 * 1024 * 1024
	}
	p.Pool_dir_ = p.Resolve_pool_dir_()
	if err := py.Mkdir_p(p.Pool_dir()); err != nil {
		return err
	}
	image_path, err := p.Resolve_image_path_()
	if err != nil {
		return err
	}
	p.Image_path_ = image_path
	p.Mount_dir_ = p.Resolve_mount_dir_()
	if err := py.Mkdir_p(p.Mount_dir()); err != nil {
		return err
	}

	if p.Is_mounted_(p.Mount_dir()) {
		return nil
	}

	if py.Exists(p.Image_path()) {
		return p.Mount_image_()
	}

	if err := p.Create_image_file_(); err != nil {
		return err
	}
	return p.Mount_image_()
}

// Close is a no-op; the mount persists beyond pool lifetime.
func (p *FilesystemPool) Close() error { return nil }

// The mount / image steps are reached through package-level function
// variables so tests can replace them (``patch.object(FilesystemPool,
// "_mount_image", ...)``) without root privileges.
var (
	// FilesystemPool_resolve_pool_dir_ is the ``_resolve_pool_dir`` body.
	FilesystemPool_resolve_pool_dir_ = func(p *FilesystemPool) string {
		return filepath.Join(LAILA_DEFAULT_DIRECTORIES["pools"], p.Uuid())
	}
	// FilesystemPool_is_mounted_ is the ``_is_mounted`` body.
	FilesystemPool_is_mounted_ = func(p *FilesystemPool, path string) bool {
		if py.Ismount(path) {
			return true
		}
		normalized_path := realpath_(path)
		handle, err := os.Open("/proc/self/mountinfo")
		if err != nil {
			return false
		}
		defer handle.Close()
		scanner := bufio.NewScanner(handle)
		for scanner.Scan() {
			parts := strings.Fields(scanner.Text())
			if len(parts) > 4 && realpath_(parts[4]) == normalized_path {
				return true
			}
		}
		return false
	}
	// FilesystemPool_create_image_file_ is the ``_create_image_file`` body.
	FilesystemPool_create_image_file_ = func(p *FilesystemPool) error {
		handle, err := os.OpenFile(p.Image_path(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return py.FromGo(err)
		}
		if err := handle.Truncate(int64(p.Image_size_bytes_)); err != nil {
			_ = handle.Close()
			return py.FromGo(err)
		}
		if err := handle.Close(); err != nil {
			return py.FromGo(err)
		}
		return p.Run_command_([]string{"mkfs.ext4", "-F", p.Image_path()},
			fmt.Sprintf("format filesystem image %s", p.Image_path()))
	}
	// FilesystemPool_mount_image_ is the ``_mount_image`` body.
	FilesystemPool_mount_image_ = func(p *FilesystemPool) error {
		return p.Run_command_([]string{"mount", "-o", "loop", p.Image_path(), p.Mount_dir()},
			fmt.Sprintf("mount filesystem image %s at %s", p.Image_path(), p.Mount_dir()))
	}
)

// realpath_ is ``os.path.realpath``.
func realpath_(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		if abs, err := filepath.Abs(resolved); err == nil {
			return abs
		}
		return resolved
	}
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

// Resolve_pool_dir_ derives the pool directory from default directories.
func (p *FilesystemPool) Resolve_pool_dir_() string { return FilesystemPool_resolve_pool_dir_(p) }

// FilesystemPool_resolve_image_path_ is the ``_resolve_image_path`` body
// (patchable like the mount / image steps above).
var FilesystemPool_resolve_image_path_ = func(p *FilesystemPool) (string, error) {
	img_path := filepath.Join(p.Pool_dir(), p.Pool_id()+".img")
	iso_path := filepath.Join(p.Pool_dir(), p.Pool_id()+".iso")

	if py.Exists(img_path) && py.Exists(iso_path) {
		return "", py.RuntimeError.Newf(
			"Ambiguous filesystem image for %s: both %s and %s exist.", p.Pool_id(), img_path, iso_path)
	}
	if py.Exists(img_path) {
		return img_path, nil
	}
	if py.Exists(iso_path) {
		return iso_path, nil
	}
	return img_path, nil
}

// Resolve_image_path_ resolves the ``.img`` or ``.iso`` image path for
// this pool. RuntimeError when both exist.
func (p *FilesystemPool) Resolve_image_path_() (string, error) {
	return FilesystemPool_resolve_image_path_(p)
}

// Resolve_mount_dir_ returns the mount sub-directory path.
func (p *FilesystemPool) Resolve_mount_dir_() string { return filepath.Join(p.Pool_dir(), "mnt") }

// Is_mounted_ checks whether *path* is currently a mount point.
func (p *FilesystemPool) Is_mounted_(path string) bool { return FilesystemPool_is_mounted_(p, path) }

// Run_command_ executes a subprocess command, raising RuntimeError on
// failure (``_run_command(command, *, action)``).
func (p *FilesystemPool) Run_command_(command []string, action string) error {
	cmd := exec.Command(command[0], command[1:]...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return py.RuntimeError.Newf("Unable to %s: command not found: %s", action, command[0]).From(py.FromGo(err))
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return py.RuntimeError.Newf("Unable to %s. Stdout: %s Stderr: %s",
				action, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String())).From(py.FromGo(err))
		}
		return py.RuntimeError.Newf("Unable to %s: command not found: %s", action, command[0]).From(py.FromGo(err))
	}
	return nil
}

// Create_image_file_ allocates and formats a new ext4 image file.
func (p *FilesystemPool) Create_image_file_() error { return FilesystemPool_create_image_file_(p) }

// Mount_image_ loop-mounts the image file at Mount_dir.
func (p *FilesystemPool) Mount_image_() error { return FilesystemPool_mount_image_(p) }

// Storage_key_ URL-encodes *key* and appends ``.json``.
func (p *FilesystemPool) Storage_key_(key string) string { return py.Quote(key, "") + ".json" }

// Logical_key_ strips the ``.json`` suffix and URL-decodes.
func (p *FilesystemPool) Logical_key_(storage_key string) string {
	return py.Unquote(strings.TrimSuffix(storage_key, ".json"))
}

// Entry_path_ is the full filesystem path for the given entry key.
func (p *FilesystemPool) Entry_path_(key string) string {
	return filepath.Join(p.Mount_dir(), p.Storage_key_(key))
}

// Read_ reads and parses the JSON file for *key*, or returns nil.
func (p *FilesystemPool) Read_(key string) (any, error) {
	path := p.Entry_path_(key)
	if !py.Exists(path) {
		return nil, nil
	}

	p.Lock()
	raw, err := os.ReadFile(path)
	p.Unlock()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, py.FromGo(err)
	}

	if strings.TrimSpace(string(raw)) == "" {
		return nil, nil
	}
	return py.Json_loads(string(raw))
}

// Write_ writes *entry* as a JSON file under *key*. TypeError unless the
// value is a dict (JSON-encoded here) or a serialized JSON string.
func (p *FilesystemPool) Write_(key string, entry any) error {
	value, err := json_blob_(entry, "FilesystemPool expects a serialized JSON string.")
	if err != nil {
		return err
	}

	path := p.Entry_path_(key)
	p.Lock()
	defer p.Unlock()
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		return py.FromGo(err)
	}
	return nil
}

// json_blob_ is the shared ``dict -> json.dumps`` / ``str`` pass-through
// of the file- and row-backed pools; TypeError otherwise.
func json_blob_(entry any, message string) (string, error) {
	value := entry
	if d, ok := asDict(value); ok {
		s, err := py.Json_dumps(d)
		if err != nil {
			return "", err
		}
		value = s
	}
	s, ok := value.(string)
	if !ok {
		return "", py.TypeError.New(message)
	}
	return s, nil
}

// Delete_ removes the JSON file for *key*; no-op if absent.
func (p *FilesystemPool) Delete_(key string) error {
	path := p.Entry_path_(key)
	p.Lock()
	defer p.Unlock()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return py.FromGo(err)
	}
	return nil
}

// Empty_ removes all ``.json`` files from the mount directory.
func (p *FilesystemPool) Empty_() error {
	p.Lock()
	defer p.Unlock()
	names, err := os.ReadDir(p.Mount_dir())
	if err != nil {
		return py.FromGo(err)
	}
	for _, name := range names {
		if !strings.HasSuffix(name.Name(), ".json") {
			continue
		}
		if err := os.Remove(filepath.Join(p.Mount_dir(), name.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return py.FromGo(err)
		}
	}
	return nil
}

// Exists_ returns true if a file for *key* exists on disk.
func (p *FilesystemPool) Exists_(key string) (bool, error) { return py.Exists(p.Entry_path_(key)), nil }

// Contains checks membership (``key in pool``); delegates to Exists_.
func (p *FilesystemPool) Contains(key any) (bool, error) { return p.Exists_(py.Str(key)) }

// Keys_ returns all keys in the mount directory
// (``_keys(as_generator=False)``). With ``as_generator=True`` a lazy
// Iterator is returned instead of a list.
func (p *FilesystemPool) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	if !as_generator {
		p.Lock()
		defer p.Unlock()
		names, err := os.ReadDir(p.Mount_dir())
		if err != nil {
			return nil, py.FromGo(err)
		}
		out := py.NewList()
		for _, name := range names {
			if strings.HasSuffix(name.Name(), ".json") {
				out.Append(p.Logical_key_(name.Name()))
			}
		}
		return out, nil
	}

	var gen py.Iterator = func(yield func(any, error) bool) {
		p.Lock()
		defer p.Unlock()
		names, err := os.ReadDir(p.Mount_dir())
		if err != nil {
			yield(nil, py.FromGo(err))
			return
		}
		for _, name := range names {
			if strings.HasSuffix(name.Name(), ".json") {
				if !yield(p.Logical_key_(name.Name()), nil) {
					return
				}
			}
		}
	}
	return gen, nil
}

func init() { py.Register_property("pool_dir", "mount_dir", "image_path") }
