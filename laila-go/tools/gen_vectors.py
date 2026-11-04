#!/usr/bin/env python3
"""Generate the Python-side truth for laila-go/py conformance tests.

Run from the repository parent (so ``import laila`` resolves)::

    python3 laila-go/tools/gen_vectors.py > laila-go/py/testdata/vectors.json

Every value is described in a tiny tagged language the Go test decodes
into the py value model, and the expected outputs are recorded exactly as
CPython produces them: repr, json.dumps variants, msgpack.packb, pickle.dumps,
float repr, pickle.encode_long, urllib quoting, inspect.Signature.bind error
texts, Fernet tokens at a fixed timestamp/IV, numpy .npy bytes and uuid5.
"""

import inspect
import json
import pickle
import sys
import urllib.parse
import uuid

import msgpack
import numpy as np
from cryptography.fernet import Fernet


def tag(v):
    if v is None:
        return {"t": "none"}
    if isinstance(v, bool):
        return {"t": "bool", "v": v}
    if isinstance(v, int):
        return {"t": "int", "v": str(v)}
    if isinstance(v, float):
        return {"t": "float", "v": repr(v)}
    if isinstance(v, str):
        return {"t": "str", "v": v}
    if isinstance(v, bytes):
        return {"t": "bytes", "v": v.hex()}
    if isinstance(v, tuple):
        return {"t": "tuple", "v": [tag(x) for x in v]}
    if isinstance(v, list):
        return {"t": "list", "v": [tag(x) for x in v]}
    if isinstance(v, set):
        return {"t": "set", "v": [tag(x) for x in v]}
    if isinstance(v, frozenset):
        return {"t": "frozenset", "v": [tag(x) for x in v]}
    if isinstance(v, dict):
        return {"t": "dict", "v": [[tag(k), tag(x)] for k, x in v.items()]}
    raise TypeError(type(v))


VALUES = [
    None,
    True,
    False,
    0,
    1,
    -1,
    127,
    128,
    255,
    256,
    65535,
    65536,
    -32,
    -33,
    -128,
    -129,
    -32768,
    -32769,
    2**31 - 1,
    2**31,
    -(2**31),
    -(2**31) - 1,
    2**32,
    2**63 - 1,
    2**63,
    2**64 - 1,
    2**64,
    -(2**63),
    -(2**63) - 1,
    2**100,
    -(2**100),
    0.0,
    -0.0,
    1.0,
    1.5,
    0.1,
    1e16,
    1e15,
    123456789012345680.0,
    1e-4,
    1e-5,
    0.0001,
    0.00001,
    1e300,
    1e-300,
    2.5e-7,
    3.14159,
    100.0,
    1e22,
    1e21,
    5e-324,
    1.7976931348623157e308,
    "",
    "a",
    "hello",
    "héllo",
    "日本語",
    "emoji 😀",
    "quote'in",
    'double"in',
    "both'\"",
    "\n\t\r\\",
    "\x00\x01\x1f\x7f",
    "x" * 31,
    "x" * 32,
    "x" * 255,
    "x" * 256,
    "y" * 70000,
    b"",
    b"ab",
    b"\x00\xff",
    b"z" * 255,
    b"z" * 256,
    b"q" * 70000,
    [],
    [1],
    [1, 2, 3],
    [1, "a", None, True, 2.5],
    [[1, [2, [3]]]],
    list(range(1500)),
    (),
    (1,),
    (1, 2),
    (1, 2, 3),
    (1, 2, 3, 4),
    ((1, 2), (3,)),
    {},
    {"a": 1},
    {"a": 1, "b": [1, 2], "c": {"d": None}},
    {"z": 1, "a": 2, "m": 3},
    {1: "one", 2.5: "two-five", True: "t", None: "n"},  # noqa: F601 -- True/1 collision is deliberate
    {("x", 1): "tuple-key"},
    {str(i): i for i in range(1500)},
    set(),
    {1},
    {1, 2, 3},
    frozenset(),
    frozenset({1, 2}),
    [float("inf"), float("-inf")],
    float("nan"),
    {"nested": [{"a": [1, {"b": (2, 3)}]}]},
]


def safe(fn, *a, **k):
    try:
        return {"ok": fn(*a, **k)}
    except Exception as e:
        return {"err": type(e).__name__, "msg": str(e)}


def json_variants(v):
    out = {}
    out["default"] = safe(json.dumps, v)
    out["compact"] = safe(json.dumps, v, separators=(",", ":"))
    out["sorted"] = safe(json.dumps, v, sort_keys=True)
    out["indent2"] = safe(json.dumps, v, indent=2)
    out["noascii"] = safe(json.dumps, v, ensure_ascii=False)
    return out


def hexout(r):
    if "ok" in r:
        return {"ok": r["ok"].hex()}
    return r


vectors = []
for v in VALUES:
    entry = {
        "value": tag(v),
        "repr": repr(v),
        "json": json_variants(v),
        "msgpack_strict": hexout(safe(msgpack.packb, v, use_bin_type=True, strict_types=True)),
        "msgpack_lax": hexout(safe(msgpack.packb, v, use_bin_type=True, strict_types=False)),
        "pickle": hexout(safe(pickle.dumps, v)),
    }
    vectors.append(entry)

# json.loads round trips with type preservation
loads = []
for text in [
    "1",
    "1.0",
    "1e5",
    "-0",
    "-0.0",
    '[1, 2.0, "x", null, true]',
    '{"a": {"b": [1]}}',
    "NaN",
    "Infinity",
    "-Infinity",
    '"\\u00e9\\ud83d\\ude00"',
    "123456789012345678901234567890",
    "  [ ]  ",
    "{}",
    '"a\\nb"',
    "1E+2",
    "0.5e-3",
]:
    try:
        val = json.loads(text)
        loads.append({"text": text, "value": tag(val), "repr": repr(val)})
    except Exception as e:
        loads.append({"text": text, "err": type(e).__name__, "msg": str(e)})
for text in ["", "{", "[1,", '{"a" 1}', "tru", '"abc', "[1 2]", '{"a":1,}', "1 2"]:
    try:
        json.loads(text)
        loads.append({"text": text, "value": tag(None)})
    except Exception as e:
        loads.append({"text": text, "err": type(e).__name__, "msg": str(e)})

# encode_long
longs = [
    {"n": str(n), "hex": pickle.encode_long(n).hex()}
    for n in [
        0,
        1,
        -1,
        127,
        128,
        255,
        256,
        -128,
        -129,
        -256,
        2**63,
        -(2**63),
        2**64,
        2**100,
        -(2**100),
        32767,
        32768,
        -32768,
        -32769,
    ]
]

# urllib quoting
quotes = []
for s in [
    "a b",
    "a/b",
    "é",
    "日本",
    "x?y=z&w",
    "~-_.",
    "100%",
    "",
    "key with spaces/and/slashes",
    "a+b",
]:
    quotes.append(
        {
            "s": s,
            "quote": urllib.parse.quote(s, safe=""),
            "quote_slash": urllib.parse.quote(s),
            "unquote": urllib.parse.unquote(urllib.parse.quote(s, safe="")),
        }
    )


# signature binding messages
def f1(a, b, c=3):
    pass


def f2(a, *args, k, **kw):
    pass


def f3(a, b, /, c, *, d, e=5):
    pass


binds = []
for fn, args, kwargs in [
    (f1, (1,), {}),
    (f1, (), {}),
    (f1, (1, 2, 3, 4), {}),
    (f1, (1, 2), {"d": 1}),
    (f1, (1, 2), {"a": 1}),
    (f1, (1, 2), {}),
    (f1, (1,), {"b": 2, "c": 9}),
    (f2, (1, 2, 3), {"k": 1, "z": 2}),
    (f2, (1,), {}),
    (f3, (1, 2, 3), {"d": 4}),
    (f3, (1, 2), {"c": 3, "d": 4}),
    (f3, (1, 2, 3), {}),
    (f3, (1, 2, 3, 4), {"d": 1}),
    (f3, (1, 2), {"a": 1, "c": 3, "d": 4}),
    (f1, (1, 2, 3), {"x": 1}),
    (f1, (1, 2, 3, 4), {"x": 1}),
]:
    sig = inspect.signature(fn)
    try:
        fn(*args, **kwargs)  # the interpreter's own message is what callers see
        b = sig.bind(*args, **kwargs)
        b.apply_defaults()
        binds.append(
            {
                "fn": fn.__name__,
                "args": [tag(a) for a in args],
                "kwargs": {k: tag(v) for k, v in kwargs.items()},
                "bound": {k: tag(v) for k, v in b.arguments.items()},
            }
        )
    except TypeError as e:
        binds.append(
            {
                "fn": fn.__name__,
                "args": [tag(a) for a in args],
                "kwargs": {k: tag(v) for k, v in kwargs.items()},
                "err": str(e),
            }
        )

# Fernet at fixed time/iv
key = b"cFCmZEBQhbMg4mF4bNpL9KaPThWO5vpwHM9Jb2nX6pA="
f = Fernet(key)
fernets = []
for msg in [b"", b"hello", b"x" * 16, b"y" * 17, "héllo".encode()]:
    iv = bytes(range(16))
    tok = f._encrypt_from_parts(msg, 1700000000, iv)
    fernets.append(
        {
            "msg": msg.hex(),
            "time": 1700000000,
            "iv": iv.hex(),
            "token": tok.decode("ascii"),
            "ts": f.extract_timestamp(tok),
        }
    )

# numpy .npy
npys = []
import io

for arr in [
    np.array([1.0, 2.0, 3.0]),
    np.array([[1, 2], [3, 4]], dtype=np.int64),
    np.array([], dtype=np.float64),
    np.array(5.0),
    np.array([True, False]),
    np.arange(12, dtype=np.float32).reshape(3, 4),
    np.array([1, 2, 3], dtype=np.uint8),
]:
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    npys.append(
        {
            "dtype": arr.dtype.str,
            "shape": list(arr.shape),
            "data": arr.tobytes().hex(),
            "npy": buf.getvalue().hex(),
        }
    )

uuids = [
    {
        "ns": "dns",
        "name": n,
        "uuid5": str(uuid.uuid5(uuid.NAMESPACE_DNS, n)),
        "uuid3": str(uuid.uuid3(uuid.NAMESPACE_DNS, n)),
    }
    for n in ["python.org", "", "laila", "日本"]
]

# repr of exceptions
excs = [
    {
        "type": "KeyError",
        "args": [tag("k")],
        "str": str(KeyError("k")),
        "repr": repr(KeyError("k")),
    },
    {"type": "KeyError", "args": [tag(1)], "str": str(KeyError(1)), "repr": repr(KeyError(1))},
    {
        "type": "ValueError",
        "args": [tag("bad")],
        "str": str(ValueError("bad")),
        "repr": repr(ValueError("bad")),
    },
    {"type": "ValueError", "args": [], "str": str(ValueError()), "repr": repr(ValueError())},
    {
        "type": "TypeError",
        "args": [tag("a"), tag(2)],
        "str": str(TypeError("a", 2)),
        "repr": repr(TypeError("a", 2)),
    },
    {
        "type": "RuntimeError",
        "args": [tag(None)],
        "str": str(RuntimeError(None)),
        "repr": repr(RuntimeError(None)),
    },
]

json.dump(
    {
        "python": sys.version.split()[0],
        "pickle_protocol": pickle.DEFAULT_PROTOCOL,
        "msgpack": msgpack.version,
        "values": vectors,
        "json_loads": loads,
        "encode_long": longs,
        "quote": quotes,
        "bind": binds,
        "fernet": {"key": key.decode(), "cases": fernets},
        "npy": npys,
        "uuid": uuids,
        "exceptions": excs,
    },
    sys.stdout,
    ensure_ascii=False,
    indent=1,
)
