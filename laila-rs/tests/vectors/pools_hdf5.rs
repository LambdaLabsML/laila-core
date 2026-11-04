//! On-disk parity of `HDF5Pool` with the Python implementation.
//!
//! `tests/fixtures/pools/hdf5/python_written.h5py` was written by the
//! Python `HDF5Pool` (`gen_fixture.py` beside it) through `h5py`. The Rust
//! pool must open it and read every key back with the same values; a file
//! the Rust pool writes must carry the identical layout -- one scalar
//! variable-length UTF-8 string dataset per URL-encoded key
//! (`gen_fixture.py --check <path>` reads it from Python).

use laila::data::HDF5Pool;
use laila::prelude::*;

const FIXTURE: &[u8] = include_bytes!("../fixtures/pools/hdf5/python_written.h5py");

/// The dataset names in the file's own (name-index) order.
const DATASETS: [&str; 3] = ["key%2Fwith%3Aspecial%3Dchars", "obj", "plain"];

fn expected() -> Vec<(&'static str, Arg)> {
    vec![
        ("plain", "123".into_arg()),
        ("obj", py!({"a": 1, "b": [1, 2.5, "x"], "u": "é\u{2603}"})),
        (
            "key/with:special=chars",
            py!({"nested": {"k": None, "t": True}}),
        ),
    ]
}

fn copy_fixture(dir: &str) -> String {
    let path = format!("{dir}/python_written.h5py");
    std::fs::write(&path, FIXTURE).expect("fixture copy");
    path
}

/// `(name, is_scalar, type descriptor, raw text)` of every dataset.
fn layout_of(path: &str) -> Vec<(String, bool, String, String)> {
    let file = hdf5::File::open(path).expect("open");
    file.member_names()
        .expect("names")
        .into_iter()
        .map(|name| {
            let ds = file.dataset(&name).expect("dataset");
            let descr = ds.dtype().expect("dtype").to_descriptor().expect("descr");
            let text = ds
                .read_scalar::<hdf5::types::VarLenUnicode>()
                .expect("scalar varlen utf-8")
                .as_str()
                .to_string();
            (name, ds.ndim() == 0, format!("{descr:?}"), text)
        })
        .collect()
}

#[test]
fn rust_reads_python_written_file() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_hdf5_vec_"))
        .unwrap();
    let path = copy_fixture(&dir);
    let pool = HDF5Pool::new(args!(
        file_path = path.clone(),
        nickname = "fixture-hdf5",
        index_enabled = false
    ))
    .unwrap();
    let keys: Vec<String> = pool
        .keys(args!())
        .unwrap()
        .__iter__()
        .unwrap()
        .iter()
        .map(|k| k.__str__())
        .collect();
    // `keys()` follows the file's name index -- the same order h5py lists.
    assert_eq!(keys, ["key/with:special=chars", "obj", "plain"]);
    for (key, value) in expected() {
        assert!(pool.exists(key).unwrap());
        assert_eq!(pool.__getitem__(key).unwrap(), value, "{key}");
    }
    pool.close(args!()).unwrap();
    let layout = layout_of(&path);
    let names: Vec<&str> = layout.iter().map(|l| l.0.as_str()).collect();
    assert_eq!(names, DATASETS);
    for (_, scalar, descr, _) in &layout {
        assert!(scalar);
        assert_eq!(descr, "VarLenUnicode");
    }
}

#[test]
fn rust_written_file_matches_python_layout() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_hdf5_vec_"))
        .unwrap();
    let python_path = copy_fixture(&dir);
    let rust_path = format!("{dir}/rust_written.h5py");
    let pool = HDF5Pool::new(args!(
        file_path = rust_path.clone(),
        nickname = "fixture-hdf5",
        index_enabled = false
    ))
    .unwrap();
    pool.__setitem__(
        "plain",
        laila::python::stdlib::json.dumps(args!("123")).unwrap(),
    )
    .unwrap();
    pool.__setitem__("obj", py!({"a": 1, "b": [1, 2.5, "x"], "u": "é\u{2603}"}))
        .unwrap();
    pool.__setitem__(
        "key/with:special=chars",
        laila::python::stdlib::json
            .dumps(args!(py!({"nested": {"k": None, "t": True}})))
            .unwrap(),
    )
    .unwrap();
    pool.close(args!()).unwrap();
    assert_eq!(layout_of(&rust_path), layout_of(&python_path));
}

#[test]
fn closed_pool_raises_runtime_error() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_hdf5_vec_"))
        .unwrap();
    let pool = HDF5Pool::new(args!(file_path = format!("{dir}/p.h5py"))).unwrap();
    pool.close(args!()).unwrap();
    pool.close(args!()).unwrap();
    let err = pool.__getitem__("x").unwrap_err();
    assert_eq!(err.kind, ExcKind::RuntimeError);
    assert_eq!(err.message, "HDF5Pool is closed.");
}
