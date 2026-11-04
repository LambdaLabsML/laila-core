"""Writes `python_written.duckdb` with the Python `DuckDBPool`.

Run from the repository parent (so `laila` is the Python package)::

    cd /home/ubuntu && python3 laila/laila-rs/tests/fixtures/pools/duckdb/gen_fixture.py

The Rust side (`tests/vectors/pools_duckdb.rs`) opens the file through
`laila::data::DuckDBPool` and checks keys, values and the table columns;
the reverse direction (Rust writes, Python reads) is the same script with
`--check <path>`.

DuckDB files are only portable across the same storage-format version, so
the Rust crate pins the engine version of the Python wheel (1.5.5).
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 6)))

import duckdb
from laila.data import DuckDBPool

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "python_written.duckdb")

EXPECTED = {
    "plain": "123",
    "obj": {"a": 1, "b": [1, 2.5, "x"], "u": "é\u2603"},
    "key/with:special=chars": {"nested": {"k": None, "t": True}},
}
COLUMNS = [("key", "VARCHAR", "NO"), ("value", "VARCHAR", "NO")]


def write(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    pool = DuckDBPool(file_path=path, nickname="fixture-duckdb", index_enabled=False)
    pool["plain"] = json.dumps("123")
    pool["obj"] = EXPECTED["obj"]
    pool["key/with:special=chars"] = json.dumps(EXPECTED["key/with:special=chars"])
    pool.close()


def check(path: str) -> None:
    conn = duckdb.connect(path, read_only=True)
    columns = conn.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'laila_pool_entries' ORDER BY ordinal_position"
    ).fetchall()
    assert columns == COLUMNS, columns
    (pk,) = conn.execute(
        "SELECT count(*) FROM duckdb_constraints() "
        "WHERE table_name = 'laila_pool_entries' AND constraint_type = 'PRIMARY KEY'"
    ).fetchone()
    assert pk == 1, pk
    conn.close()
    pool = DuckDBPool(file_path=path, nickname="fixture-duckdb", index_enabled=False)
    try:
        assert sorted(pool.keys()) == sorted(EXPECTED), pool.keys()
        for key, value in EXPECTED.items():
            assert pool[key] == value, (key, pool[key])
    finally:
        pool.close()
    print("ok", path)


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--check":
        check(sys.argv[2])
    else:
        write(OUT)
        check(OUT)
