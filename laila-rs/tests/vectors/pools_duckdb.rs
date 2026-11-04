//! On-disk parity of `DuckDBPool` with the Python implementation.
//!
//! `tests/fixtures/pools/duckdb/python_written.duckdb` was written by the
//! Python `DuckDBPool` (`gen_fixture.py` beside it) through the `duckdb`
//! wheel pinned in `pyproject.toml`. The Rust pool, linked against the
//! same engine version, must open it and read every key back with the
//! same values; a database the Rust pool writes must carry the identical
//! table (columns, primary key) and rows (`gen_fixture.py --check <path>`
//! reads it from Python).

use laila::data::DuckDBPool;
use laila::prelude::*;

const FIXTURE: &[u8] = include_bytes!("../fixtures/pools/duckdb/python_written.duckdb");

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
    let path = format!("{dir}/python_written.duckdb");
    std::fs::write(&path, FIXTURE).expect("fixture copy");
    path
}

/// `(column_name, data_type, is_nullable)` of `laila_pool_entries`, plus
/// the number of primary-key constraints on it.
fn layout_of(path: &str) -> (Vec<(String, String, String)>, i64) {
    let conn = duckdb::Connection::open_with_flags(
        path,
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .expect("config"),
    )
    .expect("open");
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_name = 'laila_pool_entries' ORDER BY ordinal_position",
        )
        .expect("prepare");
    let columns = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("columns");
    let pk: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_constraints() \
             WHERE table_name = 'laila_pool_entries' AND constraint_type = 'PRIMARY KEY'",
            [],
            |r| r.get(0),
        )
        .expect("pk");
    (columns, pk)
}

fn rows_of(path: &str) -> Vec<(String, String)> {
    let conn = duckdb::Connection::open_with_flags(
        path,
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .expect("config"),
    )
    .expect("open");
    let mut stmt = conn
        .prepare("SELECT key, value FROM laila_pool_entries ORDER BY key")
        .expect("prepare");
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
}

fn expected_layout() -> (Vec<(String, String, String)>, i64) {
    (
        vec![
            ("key".into(), "VARCHAR".into(), "NO".into()),
            ("value".into(), "VARCHAR".into(), "NO".into()),
        ],
        1,
    )
}

#[test]
fn rust_reads_python_written_database() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_duckdb_vec_"))
        .unwrap();
    let path = copy_fixture(&dir);
    let pool = DuckDBPool::new(args!(
        file_path = path.clone(),
        nickname = "fixture-duckdb",
        index_enabled = false
    ))
    .unwrap();
    let mut keys: Vec<String> = pool
        .keys(args!())
        .unwrap()
        .__iter__()
        .unwrap()
        .iter()
        .map(|k| k.__str__())
        .collect();
    keys.sort();
    let mut want: Vec<String> = expected().iter().map(|(k, _)| k.to_string()).collect();
    want.sort();
    assert_eq!(keys, want);
    for (key, value) in expected() {
        assert!(pool.exists(key).unwrap());
        assert_eq!(pool.__getitem__(key).unwrap(), value, "{key}");
    }
    pool.close(args!()).unwrap();
    assert_eq!(layout_of(&path), expected_layout());
}

#[test]
fn rust_written_database_matches_python_rows() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_duckdb_vec_"))
        .unwrap();
    let python_path = copy_fixture(&dir);
    let rust_path = format!("{dir}/rust_written.duckdb");
    let pool = DuckDBPool::new(args!(
        file_path = rust_path.clone(),
        nickname = "fixture-duckdb",
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
    assert_eq!(layout_of(&rust_path), expected_layout());
    assert_eq!(rows_of(&rust_path), rows_of(&python_path));
}

#[test]
fn engine_version_matches_the_python_wheel() {
    // The on-disk format is only portable within a storage version: the
    // crate pins the DuckDB release `pyproject.toml` pins for Python.
    assert_eq!(laila::data::duckdb::duckdb.__version__(), "1.5.5");
}

#[test]
fn closed_pool_raises_runtime_error() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_duckdb_vec_"))
        .unwrap();
    let pool = DuckDBPool::new(args!(file_path = format!("{dir}/p.duckdb"))).unwrap();
    pool.close(args!()).unwrap();
    pool.close(args!()).unwrap();
    let err = pool.__getitem__("x").unwrap_err();
    assert_eq!(err.kind, ExcKind::RuntimeError);
    assert_eq!(err.message, "DuckDBPool is closed.");
}
