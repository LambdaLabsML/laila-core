//! On-disk parity of `SQLitePool` with the Python implementation.
//!
//! `tests/fixtures/pools/sqlite/python_written.laila_sqlitedb` was written
//! by the Python `SQLitePool` (`gen_fixture.py` beside it). The Rust pool
//! must open it and read every key back with the same values; a database
//! the Rust pool writes must carry the identical table DDL and rows
//! (`gen_fixture.py --check <path>` reads it from Python).

use laila::data::SQLitePool;
use laila::prelude::*;

const FIXTURE: &[u8] = include_bytes!("../fixtures/pools/sqlite/python_written.laila_sqlitedb");

const DDL: &str = "CREATE TABLE laila_pool_entries (\n                key TEXT NOT NULL PRIMARY KEY,\n                value TEXT NOT NULL\n            )";

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
    let path = format!("{dir}/python_written.laila_sqlitedb");
    std::fs::write(&path, FIXTURE).expect("fixture copy");
    path
}

fn ddl_of(path: &str) -> String {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='laila_pool_entries'",
        [],
        |r| r.get::<_, String>(0),
    )
    .expect("ddl")
}

fn rows_of(path: &str) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(path).expect("open");
    let mut stmt = conn
        .prepare("SELECT key, value FROM laila_pool_entries ORDER BY key")
        .expect("prepare");
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
}

#[test]
fn rust_reads_python_written_database() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_sqlite_vec_"))
        .unwrap();
    let path = copy_fixture(&dir);
    let pool = SQLitePool::new(args!(
        file_path = path.clone(),
        nickname = "fixture-sqlite",
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
    assert_eq!(ddl_of(&path), DDL);
}

#[test]
fn rust_written_database_matches_python_rows() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_sqlite_vec_"))
        .unwrap();
    let python_path = copy_fixture(&dir);
    let rust_path = format!("{dir}/rust_written.laila_sqlitedb");
    let pool = SQLitePool::new(args!(
        file_path = rust_path.clone(),
        nickname = "fixture-sqlite",
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
    assert_eq!(ddl_of(&rust_path), DDL);
    assert_eq!(rows_of(&rust_path), rows_of(&python_path));
}

#[test]
fn closed_pool_raises_runtime_error() {
    let dir = laila::python::stdlib::tempfile
        .mkdtemp(args!(prefix = "laila_sqlite_vec_"))
        .unwrap();
    let pool = SQLitePool::new(args!(file_path = format!("{dir}/p.laila_sqlitedb"))).unwrap();
    pool.close(args!()).unwrap();
    pool.close(args!()).unwrap();
    let err = pool.__getitem__("x").unwrap_err();
    assert_eq!(err.kind, ExcKind::RuntimeError);
    assert_eq!(err.message, "SQLitePool is closed.");
}
