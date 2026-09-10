#!/usr/bin/env python3
"""Generate cross-language golden vectors from the real Python `laila` library.

Run with laila importable, e.g.:
    PYTHONPATH=/home/ubuntu python3 laila-c/tests/gen_vectors.py

Writes JSON fixtures into laila-c/tests/vectors/ that the C++ `xlang` suite
reads back, so laila-C is verified against laila's actual output (UUID5 +
Entry.as_dict wire format).
"""

import base64
import json
import os

import laila
from laila import get_active_namespace
from laila.basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT as IO
from laila.entry import transformation_base64

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "vectors")
os.makedirs(OUT, exist_ok=True)


def write(name, obj):
    with open(os.path.join(OUT, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    print("wrote", name)


# --- UUID5 / nickname vectors ---
nicks = ["model", "sensor_calibration", "resnet50_v1", "images", "config", "tenant/data"]
uuid5 = {
    "namespace": str(get_active_namespace()),
    "cases": [
        {
            "nickname": n,
            "uuid": IO.generate_uuid_from_nickname(n),
            "global_id": laila.constant(data=0, nickname=n).global_id,
        }
        for n in nicks
    ],
}
write("uuid5.json", uuid5)

# --- Entry.as_dict wire-format vectors (JSON-friendly payloads) ---
samples = [
    ("string", "hello laila"),
    ("empty_string", ""),
    ("int", 123),
    ("neg_int", -98765),
    ("zero", 0),
    ("float", 3.5),
    ("bool_true", True),
    ("bool_false", False),
    ("dict", {"k": [1, 2, 3], "s": "v"}),
    ("list", [1, 2, 3, 4]),
    ("nested", {"a": {"b": {"c": 1}}, "arr": [{"x": 1}, {"y": 2}]}),
]
entries = []
for name, value in samples:
    e = laila.constant(data=value)
    entries.append(
        {
            "name": name,
            "as_dict": e.as_dict(),
            "global_id": e.global_id,
            "uuid": e.uuid,
        }
    )
write("entries.json", entries)


# --- compdata vectors: entry.serialize(transformation_base64) per type ---
# Each vector carries Python's real serialized form (base64(pickle/msgpack/npy)
# + the recovery codes). laila-C builds it and checks value parity; for the
# deterministic codecs (msgpack/npy) it also checks byte-identity.
def ser(value):
    return laila.constant(data=value).serialize(transformations=transformation_base64)


cd = []


def add(name, check, payload, **extra):
    rec = {"name": name, "check": check, "serialized": ser(payload)}
    rec.update(extra)
    cd.append(rec)


# scalars + str/bytes -> pickle (the Python default serializer)
add("str", "value", "hello laila", value="hello laila")
add("empty_str", "value", "", value="")
add(
    "unicode",
    "value",
    "\u00fcn\u00efc\u00f6d\u00e9 \U0001f600",
    value="\u00fcn\u00efc\u00f6d\u00e9 \U0001f600",
)
add("int_small", "value", 7, value=7)
add("int_neg", "value", -98765, value=-98765)
add("int_max64", "value", 9223372036854775807, value=9223372036854775807)
add("int_min64", "value", -9223372036854775808, value=-9223372036854775808)
add("float", "value", 3.5, value=3.5)
add("float_neg", "value", -2.5e-10, value=-2.5e-10)
add("bool_true", "value", True, value=True)
add("bool_false", "value", False, value=False)
cd.append({"name": "none", "check": "none", "serialized": ser(None)})
add(
    "bytes",
    "bytes",
    b"\x00\x01\x02\xfe\xff",
    b64=base64.b64encode(b"\x00\x01\x02\xfe\xff").decode(),
)

# dict/list/tuple -> msgpack (byte-identical check)
add("list", "value", [1, 2, 3, 4], value=[1, 2, 3, 4], mp_identity=True)
add(
    "dict_ordered",
    "value",
    {"b": 1, "a": 2, "z": 3},
    value={"b": 1, "a": 2, "z": 3},
    mp_identity=True,
)
add(
    "nested",
    "value",
    {"a": {"b": [1, 2, {"c": 3}]}},
    value={"a": {"b": [1, 2, {"c": 3}]}},
    mp_identity=True,
)
add(
    "mixed",
    "value",
    {"i": 1, "f": 2.5, "s": "x", "b": True, "n": None, "l": [1, "two"]},
    value={"i": 1, "f": 2.5, "s": "x", "b": True, "n": None, "l": [1, "two"]},
    mp_identity=True,
)

# nested bytes inside a dict (msgpack bin)
_db = {"k": b"\xde\xad\xbe\xef"}
cd.append(
    {
        "name": "dict_bytes",
        "check": "nested_bytes",
        "serialized": ser(_db),
        "key": "k",
        "b64": base64.b64encode(b"\xde\xad\xbe\xef").decode(),
    }
)

# arbitrary-precision int beyond int64 -> pickle LONG; laila-C must raise UNSUPPORTED
cd.append({"name": "bignum", "check": "bignum_unsupported", "serialized": ser(10**30)})

# numpy arrays -> .npy (byte-identical check)
try:
    import numpy as np

    for nm, arr in [
        ("npy_f64", np.array([[1.5, 2.5], [3.5, 4.5]], dtype="<f8")),
        ("npy_i32", np.array([1, 2, 3, 4, 5], dtype="<i4")),
        ("npy_u8", np.arange(6, dtype="u1").reshape(2, 3)),
    ]:
        cd.append(
            {
                "name": nm,
                "check": "numpy",
                "serialized": ser(arr),
                "dtype": arr.dtype.str,
                "shape": list(arr.shape),
                "raw_b64": base64.b64encode(arr.tobytes()).decode(),
            }
        )
except ModuleNotFoundError:
    print("numpy missing; skipping npy vectors")

write("compdata.json", cd)

print("done ->", OUT)
