"""Writes `python_written.laila_sqlitedb` with the Python `SQLitePool`.

Run from the repository parent (so `laila` is the Python package)::

    cd /home/ubuntu && python3 laila/laila-rs/tests/fixtures/pools/sqlite/gen_fixture.py

The Rust side (`tests/vectors/pools_sqlite.rs`) opens the file through
`laila::data::SQLitePool` and checks keys, values and the table DDL; the
reverse direction (Rust writes, Python reads) is the same script with
`--check <path>`.
"""

import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 6)))

from laila.data import SQLitePool

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "python_written.laila_sqlitedb")

EXPECTED = {
    "plain": "123",
    "obj": {"a": 1, "b": [1, 2.5, "x"], "u": "é\u2603"},
    "key/with:special=chars": {"nested": {"k": None, "t": True}},
}
DDL = (
    "CREATE TABLE laila_pool_entries (\n"
    "                key TEXT NOT NULL PRIMARY KEY,\n"
    "                value TEXT NOT NULL\n"
    "            )"
)


def write(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    pool = SQLitePool(file_path=path, nickname="fixture-sqlite", index_enabled=False)
    pool["plain"] = json.dumps("123")
    pool["obj"] = EXPECTED["obj"]
    pool["key/with:special=chars"] = json.dumps(EXPECTED["key/with:special=chars"])
    pool.close()


def check(path: str) -> None:
    conn = sqlite3.connect(path)
    (sql,) = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='laila_pool_entries'"
    ).fetchone()
    assert sql == DDL, sql
    conn.close()
    pool = SQLitePool(file_path=path, nickname="fixture-sqlite", index_enabled=False)
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
