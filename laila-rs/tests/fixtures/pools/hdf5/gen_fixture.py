"""Writes `python_written.h5py` with the Python `HDF5Pool`.

Run from the repository parent (so `laila` is the Python package)::

    cd /home/ubuntu && python3 laila/laila-rs/tests/fixtures/pools/hdf5/gen_fixture.py

The Rust side (`tests/vectors/pools_hdf5.rs`) opens the file through
`laila::data::HDF5Pool` and checks keys, values and the dataset layout
(one scalar variable-length UTF-8 string dataset per URL-encoded key); the
reverse direction (Rust writes, Python reads) is the same script with
`--check <path>`.
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *[".."] * 6)))

import h5py
from laila.data import HDF5Pool

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "python_written.h5py")

EXPECTED = {
    "plain": "123",
    "obj": {"a": 1, "b": [1, 2.5, "x"], "u": "é\u2603"},
    "key/with:special=chars": {"nested": {"k": None, "t": True}},
}
DATASETS = ["key%2Fwith%3Aspecial%3Dchars", "obj", "plain"]


def write(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    pool = HDF5Pool(file_path=path, nickname="fixture-hdf5", index_enabled=False)
    pool["plain"] = json.dumps("123")
    pool["obj"] = EXPECTED["obj"]
    pool["key/with:special=chars"] = json.dumps(EXPECTED["key/with:special=chars"])
    pool.close()


def check(path: str) -> None:
    with h5py.File(path, "r") as f:
        assert list(f.keys()) == DATASETS, list(f.keys())
        for name in DATASETS:
            ds = f[name]
            assert ds.shape == (), (name, ds.shape)
            info = h5py.check_string_dtype(ds.dtype)
            assert info is not None and info.encoding == "utf-8" and info.length is None, (
                name,
                ds.dtype,
            )
    pool = HDF5Pool(file_path=path, nickname="fixture-hdf5", index_enabled=False)
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
