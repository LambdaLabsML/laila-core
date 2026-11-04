//! Child-process bodies of the Python test tree's inline child scripts
//! (`subprocess.run([sys.executable, "-c", SRC.format(..)])`), the Rust
//! `conftest.py` side of the `child_scripts:` override [P32]. Each is the
//! Python template's body; `subprocess.run` re-executes the test binary
//! on the generated `__laila_child_<CONSTANT>` entry, which calls the
//! function with the `.format(..)` keyword arguments.

use laila::data::HDF5Pool;
use laila::prelude::*;

laila::python! {
    /// `test_persistent_pool_safety._HARD_KILL_CHILD`: write twenty
    /// entries and leave through `os._exit` without `close()` -- the file
    /// must still be readable (F-05: every write is flushed).
    pub fn hard_kill_child(kw: Args) {
        let path = kw["path"];
        let p = HDF5Pool(file_path=path, index_enabled=False);
        for i in range(20) {
            p._write("k%d" % i, {"i": i});
        }
        sys.stdout.write("WROTE\n"); sys.stdout.flush();
        os._exit(0);
    }
}
