"""Generate the Python-side reference vectors consumed by ``tests/vectors/*.test.js``.

Run from anywhere (the sibling ``laila`` package is put on ``sys.path``)::

    cd laila-js && npm run vectors

Writes ``codecs.json`` next to this file. Everything the JS codecs must be
byte-identical with is produced here by the *real* CPython / msgpack / numpy /
cryptography implementations, so the JS tests never encode expectations by
hand.
"""

from __future__ import annotations

import base64
import io
import json
import os
import pickle
import sys
import zlib

import msgpack
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "codecs.json")

# Make ``import laila`` resolve to the sibling package regardless of cwd
# (tests/vectors -> laila-js -> laila -> its parent), unless already importable.
_PKG_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

cases: dict[str, dict] = {}


def enc(b: bytes) -> str:
    """Hex for small payloads; ``z:`` + base64(zlib) for large ones (keeps the fixture small)."""
    if len(b) <= 2048:
        return b.hex()
    return "z:" + base64.b64encode(zlib.compress(b, 9)).decode()


def add(name, obj, mp=True):
    cases[name] = {"pickle": enc(pickle.dumps(obj))}
    if mp:
        try:
            cases[name]["msgpack"] = enc(msgpack.packb(obj, use_bin_type=True))
        except Exception as e:  # noqa: BLE001
            cases[name]["msgpack_error"] = type(e).__name__


# -- scalars -----------------------------------------------------------------
add("none", None)
add("true", True)
add("false", False)
for v in [0, 1, 255, 256, 65535, 65536, 2**31 - 1, -1, -128, -129, -32768, -32769, -(2**31), -(2**31) - 1,
          2**31, 2**32 - 1, 2**32, 2**53, 2**63 - 1, -(2**63), 2**64 - 1, 2**64, 10**30, -(10**30)]:
    add(f"int_{v}", v)
add("float_3.5", 3.5)
add("float_neg", -2.5e-10)
add("float_1.0", 1.0)
add("float_0.0", 0.0)
add("float_inf", float("inf"))
add("float_nan", float("nan"))

# -- str / bytes -------------------------------------------------------------
add("str_empty", "")
add("str_hello", "hello laila")
add("str_unicode", "\u00fcn\u00efc\u00f6d\u00e9 \U0001f600")
add("str_255", "z" * 255)
add("str_256", "z" * 256)
add("str_300", "x" * 300)
add("str_70000", "y" * 70000)
add("str_surrogate", "a\udcffb", mp=False)
add("bytes_empty", b"")
add("bytes_small", b"\x00\x01\x02\xfe\xff")
add("bytes_255", b"q" * 255)
add("bytes_256", b"q" * 256)
add("bytes_300", bytes(range(256)) + b"\x00" * 44)
add("bytes_65535", b"\x02" * 65535)
add("bytes_65536", b"\x01" * 65536)
add("bytes_70000", b"\x7f" * 70000)
add("bytearray", bytearray(b"\x01\x02\x03"), mp=False)

# -- containers (incl. BATCHSIZE / frame boundaries) -------------------------
add("list_empty", [])
add("list_one", [7])
add("list_1234", [1, 2, 3, 4])
add("list_1000", list(range(1000)))
add("list_1001", list(range(1001)))
add("list_1500", list(range(1500)))
add("list_2000", list(range(2000)))
add("list_2001", list(range(2001)))
add("list_nested", [[1, [2, [3]]], {"k": [None, True]}])
add("list_floats", [1.5, 2.0, -0.0, 1e300])
add("list_same_str", ["shared", "shared"])
shared = [1, 2]
add("list_shared_list", [shared, shared])
add("bool_in_list", [True, False, None, 0, 1])
add("neg_fixint", [-1, -32, -33, -127, -128])
add("uint_edges", [127, 128, 255, 256, 65535, 65536, 4294967295, 4294967296])
add("tuple_empty", ())
add("tuple_1", (1,))
add("tuple_2", (1, 2))
add("tuple_3", (1, 2, 3))
add("tuple_4", (1, 2, 3, 4))
add("tuple_nested", ((1, 2), (3, (4, 5))))
add("dict_empty", {})
add("dict_one", {"k": "v"})
add("dict_ordered", {"b": 1, "a": 2, "z": 3})
add("dict_intkeys", {1: "x", 2: "y"})
add("dict_tuplekey", {(1, 2): "y"}, mp=False)
add("dict_1000", {f"k{i}": i for i in range(1000)})
add("dict_1001", {f"k{i}": i for i in range(1001)})
add("dict_1500", {f"k{i}": i for i in range(1500)})
add("dict_2000", {f"k{i}": i for i in range(2000)})
add("dict_nested", {"a": {"b": [1, 2, {"c": 3}]}})
add("dict_mixed", {"i": 1, "f": 2.5, "s": "x", "b": True, "n": None, "l": [1, "two"]})
add("dict_same_str", {"a": "a"})
add("dict_bytes", {"k": b"\xde\xad\xbe\xef"})
add("deep", {"a": [{"b": ({"c": [1, (2, 3)]},)}]}, mp=False)
# small-int sets iterate in a deterministic order in CPython
add("set_empty", set())
add("set_123", {1, 2, 3})
add("set_1000", set(range(1000)))
add("set_1001", set(range(1001)))
add("set_1500", set(range(1500)))
add("frozenset_empty", frozenset())
add("frozenset_123", frozenset({1, 2, 3}))


# -- numpy -------------------------------------------------------------------
def npy(arr):
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return enc(buf.getvalue())


for name, arr in [
    ("npy_f64", np.array([[1.5, 2.5], [3.5, 4.5]], dtype="<f8")),
    ("npy_i32", np.array([1, 2, 3, 4, 5], dtype="<i4")),
    ("npy_u8", np.arange(6, dtype="u1").reshape(2, 3)),
    ("npy_bool", np.array([True, False, True])),
    ("npy_i64", np.arange(10, dtype="<i8")),
    ("npy_f32_3d", np.zeros((2, 3, 4), dtype="<f4")),
    ("npy_scalar", np.array(7.5)),
    ("npy_empty", np.array([], dtype="<f8")),
    ("npy_fortran", np.asfortranarray(np.array([[1, 2, 3], [4, 5, 6]], dtype="<i4"))),
    ("npy_big_shape", np.zeros((1,) * 30, dtype="u1")),
    ("npy_f16", np.array([1.0, -1.0, 0.1, 65504.0, 6e-8, np.nan, np.inf, -np.inf, 0.0, -0.0, 1 / 3], dtype="<f2")),
    ("npy_f16_lin", np.linspace(-1, 1, 24, dtype="<f2").reshape(2, 3, 4)),
    ("npy_c8", np.array([1 + 2j, -3 + 4j, 0 - 1j], dtype="<c8")),
    ("npy_c16", (np.linspace(-1, 1, 6) + 1j * np.linspace(0.5, -0.5, 6)).astype("<c16").reshape(2, 3)),
    ("npy_be_i4", np.arange(5).astype(">i4")),
    ("npy_be_f8", np.linspace(-1, 1, 6).astype(">f8")),
    ("npy_be_f8_fortran", np.asfortranarray(np.arange(6, dtype=">f8").reshape(2, 3))),
    ("npy_u8_max", np.array([0, 2**63, 2**64 - 1], dtype="<u8")),
    ("npy_i8_minmax", np.array([-(2**63), 0, 2**63 - 1], dtype="<i8")),
]:
    cases[name] = {"npy": npy(arr), "pickle": enc(pickle.dumps(arr)), "pickle4": enc(pickle.dumps(arr, protocol=4))}

out = {"cases": cases}

# -- zlib / base64 -----------------------------------------------------------
text = ("hello hello hello hello laila " * 20).encode()
out["zlib"] = {
    "input": text.decode(),
    "default": base64.b64encode(zlib.compress(text)).decode(),
    "level9": base64.b64encode(zlib.compress(text, 9)).decode(),
    "level1": base64.b64encode(zlib.compress(text, 1)).decode(),
    "level0_abc": base64.b64encode(zlib.compress(b"abc", 0)).decode(),
    "raw_wbits": base64.b64encode(zlib.compress(text, wbits=-15)).decode(),
    "gzip_wbits": base64.b64encode(zlib.compress(text, wbits=31)).decode()[:0],  # gzip carries mtime; not byte-stable
}
out["base64"] = {
    "alt_0_255": base64.b64encode(bytes(range(256)), altchars=b"-_").decode(),
    "std_0_255": base64.b64encode(bytes(range(256))).decode(),
}

# -- laila transformations: recovery codes + fernet ---------------------------
try:
    import laila  # noqa: F401
    from cryptography.fernet import Fernet

    from laila.entry.compdata.transformation.base64.base64 import Base64
    from laila.entry.compdata.transformation.compression.zlib import Zlib
    from laila.entry.compdata.transformation.encryption.encryption import FernetEncryption, key_fingerprint
    from laila.entry.compdata.transformation.jsonstring.jsonstring import JsonString
    from laila.entry.compdata.transformation.serialization.msgpack import MsgpackSerializer
    from laila.entry.compdata.transformation.serialization.numpy import NumpySerializer
    from laila.entry.compdata.transformation.serialization.pickle import PickleSerializer

    key = b"cXdlcnR5dWlvcGFzZGZnaGprbHp4Y3Zibm0xMjM0NTY="
    f = Fernet(key)
    iv = bytes(range(16))
    rc = {
        "base64": Base64().backward_code,
        "base64_kw": Base64(backward_kwargs={"altchars": b"-_", "validate": True}).backward_code,
        "zlib": Zlib().backward_code,
        "zlib_kw": Zlib(backward_kwargs={"wbits": 15, "bufsize": 16384}).backward_code,
        "json_string": JsonString().backward_code,
        "msgpack": MsgpackSerializer().backward_code,
        "msgpack_kw": MsgpackSerializer(backward_kwargs={"use_list": False}).backward_code,
        "numpy": NumpySerializer().backward_code,
        "pickle": PickleSerializer().backward_code,
        "pickle_kw": PickleSerializer(backward_kwargs={"fix_imports": False, "encoding": "ASCII"}).backward_code,
        "fernet": FernetEncryption(key=key).backward_code,
        "fernet_kw": FernetEncryption(key=key, backward_kwargs={"ttl": 60}).backward_code,
    }
    try:
        from laila.entry.compdata.transformation.serialization.torch import TorchSerializer

        rc["torch"] = TorchSerializer().backward_code
    except Exception:  # torch optional
        pass
    out["recovery_codes"] = rc
    out["fernet"] = {
        "key": key.decode(),
        "fingerprint": key_fingerprint(key),
        "time": 1700000000,
        "iv_hex": iv.hex(),
        "tokens": {
            "hello": [f._encrypt_from_parts("hello laila \U0001f600 world".encode(), 1700000000, iv).decode(), "hello laila \U0001f600 world"],
            "empty": [f._encrypt_from_parts(b"", 1700000000, iv).decode(), ""],
            "block16": [f._encrypt_from_parts(b"0123456789abcdef", 1700000000, iv).decode(), "0123456789abcdef"],
        },
    }
    out["transform_forward"] = {
        "zlib_default": Zlib().forward(text.decode()),
        "zlib_level9": Zlib(forward_kwargs={"level": 9}).forward(text.decode()),
        "base64_alt": Base64(forward_kwargs={"altchars": b"-_"}).forward(bytes(range(256))),
    }
except ImportError as e:  # pragma: no cover - laila not importable
    print(f"warning: laila not importable ({e}); recovery-code vectors not regenerated", file=sys.stderr)
    if os.path.exists(OUT):
        prev = json.load(open(OUT))
        for k in ("recovery_codes", "fernet", "transform_forward"):
            if k in prev:
                out[k] = prev[k]

out["meta"] = {
    "python": sys.version.split()[0],
    "pickle_protocol": pickle.DEFAULT_PROTOCOL,
    "msgpack": ".".join(map(str, msgpack.version)),
    "numpy": np.__version__,
}

with open(OUT, "w") as fh:
    json.dump(out, fh, indent=1, sort_keys=True)
print(f"wrote {OUT}: {len(cases)} cases")
