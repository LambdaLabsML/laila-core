#!/usr/bin/env python3
"""Python <-> laila-C interop over the new carrier protocols.

An unmodified Python `laila` policy peers with laila-C policies and exchanges
memorize / remember / forget in both directions, across the connection schemes
both sides can run here (`tcp://` stream carrier and `ws://`). Entries cross the
wire as `serialize(transformation_base64)` blobs, so this also exercises the
compdata mirror (pickle/msgpack/npy) for every value type.

  C_HARNESS=/path/to/c_harness python3 run_interop.py [--qemu-uri tcp://host:port]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

import laila
import numpy as np
from laila.policy.central.communication.protocols.ip_app.tcp import (
    _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL as TCP,
)
from laila.policy.central.communication.protocols.tcpip import (
    _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL as WS,
)

SECRET = "s3cr3t"
HARNESS = os.environ.get("C_HARNESS", "")

# Values spanning the compdata serializers: pickle (scalars/str/bytes),
# msgpack (dict/list), and nesting.
VALUES = [
    "hello",
    "",
    "\u00fcni\U0001f600",
    7,
    -98765,
    9223372036854775807,
    3.5,
    True,
    False,
    b"\x00\x01\xfe\xff",
    [1, 2, 3],
    {"b": 1, "a": 2},
    {"x": {"y": [1, {"z": 2}]}, "n": None},
]

# Entries the laila-C harness builds via its implicit LailaValue constructors
# (memorize_typed <kind>); Python must read each one back as the right type.
# Keep in sync with build_typed() in c_harness.cpp.
TYPED = {
    "int": 1234567,
    "double": 3.5,
    "bool": True,
    "bytes": b"\x00\x01\xfe\xff",
    "dict": {"a": 1, "b": [1, 2, 3]},
    "numpy": np.array([[1.0, 2.0], [3.0, 4.0]], dtype="<f4"),
}

_passes = 0
_failures = 0


def check(cond, label):
    global _passes, _failures
    if cond:
        _passes += 1
        print(f"  PASS  {label}")
    else:
        _failures += 1
        print(f"  FAIL  {label}")


def _servers():
    """Register a TCP stream server and a WS server on the active policy; return
    a {scheme: bound_port} map so a laila-C client can peer INTO Python."""
    comm = laila.get_active_policy().central.communication
    tcp = TCP(host="127.0.0.1", port=0, peer_secret_key=SECRET)
    ws = WS(host="127.0.0.1", port=0, peer_secret_key=SECRET)
    comm.add_connection(tcp)
    comm.add_connection(ws)
    return {"tcp": tcp.bound_port, "ws": ws.bound_port}


class CServer:
    """A laila-C policy serving its memory (subprocess)."""

    def __init__(self, value):
        self.proc = subprocess.Popen(
            [HARNESS, "serve", "0", SECRET, value],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        self.policy = self.alpha = self.store = self.port = None
        deadline = time.time() + 15
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if line.startswith("POLICY "):
                self.policy = line.split(" ", 1)[1]
            elif line.startswith("ALPHA_GID "):
                self.alpha = line.split(" ", 1)[1]
            elif line.startswith("STORE_GID "):
                self.store = line.split(" ", 1)[1]
            elif line.startswith("READY "):
                self.port = int(line.split(" ", 1)[1])
                break
        if self.port is None:
            self.kill()
            raise RuntimeError("C server did not become ready")

    def kill(self):
        try:
            self.proc.kill()
            self.proc.wait(timeout=5)
        except Exception:
            pass


def c_client(mode, uri, *rest):
    out = subprocess.run(
        [HARNESS, mode, uri, SECRET, *rest], capture_output=True, text=True, timeout=30
    )
    return (out.stdout + out.stderr).strip()


def py_client_to_c_server(scheme):
    print(f"[leg] Python client --{scheme}--> laila-C server")
    srv = CServer("c-server-seed")
    try:
        pid = laila.add_peer(f"{scheme}://127.0.0.1:{srv.port}", SECRET)
        # remember the C server's seeded alpha entry
        f = laila.remember(srv.alpha, dst_policy=pid, persist=False)
        laila.wait(f)
        check(f.result.data == "c-server-seed", f"[{scheme}] PY remembers from C")
        # memorize each compdata value into C, read it back
        ok = True
        for v in VALUES:
            e = laila.constant(data=v)
            laila.wait(laila.memorize(e, dst_policy=pid))
            b = laila.remember(e.global_id, dst_policy=pid, persist=False)
            laila.wait(b)
            if b.result.data != v:
                ok = False
                print(f"      mismatch {v!r} -> {b.result.data!r}")
        check(ok, f"[{scheme}] PY->C->PY round-trip for all compdata types")
        # forget on C, then confirm gone
        laila.wait(laila.forget(srv.alpha, policy=pid))
        gone = False
        try:
            g = laila.remember(srv.alpha, dst_policy=pid, persist=False)
            laila.wait(g)
            _ = g.result
        except Exception:
            gone = True
        check(gone, f"[{scheme}] PY forgets on C")
    finally:
        srv.kill()


def c_client_to_py_server(scheme, ports):
    print(f"[leg] laila-C client --{scheme}--> Python server")
    seed = laila.constant(data="py-server-seed")
    laila.wait(laila.memorize(seed))
    uri = f"{scheme}://127.0.0.1:{ports[scheme]}"

    out = c_client("remember", uri, seed.global_id)
    check(out.endswith("VALUE py-server-seed"), f"[{scheme}] C remembers from Python ({out!r})")

    out = c_client("memorize", uri, "c-to-py")
    gid = out.split(" ", 1)[1] if out.startswith("MEMO ") else None
    ok = False
    if gid:
        f = laila.remember(gid, persist=False)
        laila.wait(f)
        ok = f.result.data == "c-to-py"
    check(ok, f"[{scheme}] C memorizes into Python; Python reads it back")

    # C builds an entry of each kind via its implicit constructors and memorizes
    # it into Python; Python must read it back as the right type/value.
    typed_ok = True
    for kind, expected in TYPED.items():
        out = c_client("memorize_typed", uri, kind)
        gid = out.split(" ", 1)[1] if out.startswith("MEMO ") else None
        got_ok = False
        if gid:
            f = laila.remember(gid, persist=False)
            laila.wait(f)
            got = f.result.data
            if kind == "numpy":
                got_ok = isinstance(got, np.ndarray) and np.array_equal(got, expected)
            else:
                got_ok = type(got) is type(expected) and got == expected
        if not got_ok:
            typed_ok = False
            print(f"      typed mismatch {kind}: {out!r}")
    check(
        typed_ok,
        f"[{scheme}] C memorizes each implicit-ctor kind into Python; Python reads them back",
    )

    out = c_client("forget", uri, seed.global_id)
    gone = False
    try:
        f = laila.remember(seed.global_id, persist=False)
        laila.wait(f)
        _ = f.result
    except Exception:
        gone = True
    check(out.startswith("FORGOT") and gone, f"[{scheme}] C forgets on Python")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qemu-uri", default=None)
    ap.add_argument("--device-gid", default=None)
    ap.add_argument("--device-only", action="store_true")
    args = ap.parse_args()

    ports = _servers()
    if not args.device_only:
        for scheme in ("tcp", "ws"):
            py_client_to_c_server(scheme)
            c_client_to_py_server(scheme, ports)

    if args.qemu_uri:
        print(f"[leg] Python client -> emulated ESP32 device ({args.qemu_uri})")
        pid = laila.add_peer(args.qemu_uri, SECRET)
        if args.device_gid:
            g = laila.remember(
                args.device_gid, dst_policy=pid, dst_pool="remote-store", persist=False
            )
            laila.wait(g)
            check(g.result.data == "sensor=42", "PY remembers the device's seeded entry")
        e = laila.constant(data="py-to-device")
        laila.wait(laila.memorize(e, dst_policy=pid))
        b = laila.remember(e.global_id, dst_policy=pid, persist=False)
        laila.wait(b)
        check(b.result.data == "py-to-device", "PY<->ESP32 memorize+remember round-trip")

    print(f"\n=== interop summary: {_passes} passed, {_failures} failed ===")
    return 0 if _failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
