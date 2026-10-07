"""Python ``laila`` peer for the JS <-> Python interop harness.

Two modes, selected by the first argument:

``server [tcpip|tcp]``
    Host a policy with a ``remote-store`` pool holding reference entries and
    listen on the given transport. Prints ``KEY=value`` lines followed by
    ``READY`` and idles until killed. The JS side (client) then peers to it,
    issues RPCs and memory ops, and verifies what landed via the ``probe_*``
    RPC helpers attached to the policy.

``client <uri> <secret> <remote_policy_id> <entry_id>``
    Peer to a JS-hosted policy, run the same memory round-trips from the
    Python side, and print one JSON line per check (``{"check": ..., ...}``)
    followed by ``DONE``.

Both modes are the Python half of the scenarios in
``tests/functional/policy/communication/unit_tests/test_peer_routing.py`` and
``test_emulated_peers_memory.py``, run against the JavaScript port instead
of a second Python process.
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
import uuid

import numpy as np

import laila
from laila.entry.constitution.build_maps import build_by_scope
from laila.macros.defaults import (
    DefaultPolicy,
    DefaultPool,
    DefaultTCPIPProtocol,
    DefaultTCPProtocol,
)

_TRANSPORTS = {"tcpip": DefaultTCPIPProtocol, "tcp": DefaultTCPProtocol}


def _say(**kv):
    for k, v in kv.items():
        print(f"{k}={v}", flush=True)


def _json(**kv):
    print(json.dumps(kv, sort_keys=True), flush=True)


def _reference_payload():
    """A payload exercising every wire-relevant scalar / container kind."""
    return {
        "message": "hello-from-python",
        "int": 7,
        "float": 2.5,
        "one": 1.0,
        "bool": True,
        "none": None,
        "list": [1, "two", 3.0, False, None],
        "nested": {"a": {"b": [1, 2, {"c": "d"}]}},
        "unicode": "ключ 键 🔑",
    }


def _probe_entry(pool, gid):
    """Describe what *pool* holds under *gid* (plain JSON)."""
    if gid not in pool:
        return {"present": False}
    rec = pool[gid]
    entry = rec["entry"] if isinstance(rec, dict) and "entry" in rec else rec
    if isinstance(entry, (dict, str)):
        entry = build_by_scope(entry)
    data = entry.data
    out = {"present": True, "type": type(data).__name__}
    if isinstance(data, np.ndarray):
        out.update(dtype=str(data.dtype), shape=list(data.shape), sum=float(data.sum()))
    elif isinstance(data, (dict, list, str, int, float, bool)) or data is None:
        out["data"] = data
    return out


def _echo_lane(policy, name: str) -> None:
    """Echo every message on stream lane *name* back reversed (``data[::-1]``).

    Waits for the first peer that opened the lane, then relays until the
    lane closes; repeats for the next peer (the JS side re-peers once).
    """
    comm = policy.central.communication
    while True:
        peer_id = None
        while peer_id is None:
            for pid in list(comm.peers.keys()):
                try:
                    if name in comm.peers[pid].channels():
                        peer_id = pid
                        break
                except Exception:
                    pass
            if peer_id is None:
                time.sleep(0.02)
        ch = comm.peers[peer_id][name]
        try:
            for e in laila.relay(ch):
                ch.send(bytes(e.data)[::-1])
        except Exception:
            pass
        time.sleep(0.05)


def serve(transport: str) -> None:
    policy = DefaultPolicy()
    laila.activate_policy(policy)
    pool = DefaultPool()
    laila.memory.extend(pool=pool, pool_nickname="remote-store")

    entry = laila.constant(data=_reference_payload(), nickname="py-ref")
    arr_entry = laila.constant(data=np.arange(12, dtype=np.float32).reshape(3, 4), nickname="py-arr")
    with laila.guarantee:
        laila.memorize(entries=[entry, arr_entry], pool_nickname="remote-store")

    # RPC helpers the JS side calls to verify Python-side state.
    object.__setattr__(policy, "echo", lambda x: x)
    object.__setattr__(policy, "probe_entry", lambda gid: _probe_entry(pool, gid))
    object.__setattr__(policy, "peer_count", lambda: len(policy.central.communication.peers))

    proto = _TRANSPORTS[transport](host="127.0.0.1", port=0, peer_secret_key=uuid.uuid4().hex)
    laila.communication.add_connection(proto)

    if type(proto).supports_channels:
        threading.Thread(target=_echo_lane, args=(policy, "video"), daemon=True).start()

    _say(
        PORT=proto.bound_port,
        SECRET=proto.peer_secret_key,
        ENTRY_ID=entry.global_id,
        ARRAY_ENTRY_ID=arr_entry.global_id,
        POLICY_ID=policy.global_id,
    )
    print("READY", flush=True)
    while True:
        time.sleep(1)


def client(uri: str, secret: str, remote_id: str, entry_id: str) -> None:
    policy = DefaultPolicy()
    laila.activate_policy(policy)
    laila.memory.extend(pool=DefaultPool(), pool_nickname="py-local")
    comm = policy.central.communication
    comm.add_connection(_TRANSPORTS["tcpip" if uri.startswith("ws") else "tcp"](host="127.0.0.1", port=0))

    rid = comm.add_peer(uri, secret)
    _json(check="peered", remote_id=rid, expected=remote_id, ok=rid == remote_id)

    proxy = comm.peers[rid]
    echoed = proxy.echo({"k": [1, 2.5, "s", None, True], "u": "ключ"})
    _json(check="echo", value=echoed)
    _json(check="ping", ok=next(iter(comm.connections.values())).ping(rid))

    # remember from the JS peer: real data crosses the wire
    res = laila.remember(entry_ids=entry_id, pool_nickname="remote-store", policy_id=rid, persist=False)
    data = res.data
    rebuilt = res.wait()
    _json(check="remember", data=data, gid=rebuilt.global_id, active_restored=laila.get_active_policy() is policy)

    # memorize into the JS peer (dict + numpy array)
    e1 = laila.constant(data={"from": "python", "n": [1, 2, 3]}, nickname="py-push")
    e2 = laila.constant(data=np.ones((2, 3), dtype=np.int64) * 4, nickname="py-push-arr")
    f1 = laila.memorize(entries=e1, policy_id=rid, pool_nickname="remote-store")
    f2 = laila.memorize(entries=e2, policy_id=rid, pool_nickname="remote-store")
    _json(check="memorize", gids=[f1.data, f2.data], expected=[e1.global_id, e2.global_id])
    _json(check="probe", pushed=proxy.probe_entry(e1.global_id), pushed_arr=proxy.probe_entry(e2.global_id))

    # remember the numpy entry back from the JS side
    back = laila.remember(entry_ids=e2.global_id, pool_nickname="remote-store", policy_id=rid, persist=False).wait()
    arr = back.data
    _json(
        check="remember_array",
        is_ndarray=isinstance(arr, np.ndarray),
        dtype=str(getattr(arr, "dtype", None)),
        shape=list(getattr(arr, "shape", [])),
        sum=float(np.asarray(arr).sum()),
    )

    if type(next(iter(comm.connections.values()))).supports_channels:
        ch = comm.peers[rid]["video"]
        payloads = [bytes([i % 256 for i in range(n)]) for n in (1, 100, 4096, 65536 + 7)]
        for p in payloads:
            ch.send(p)
        got = [next(laila.relay(ch, timeout=20)) for _ in payloads]
        _json(
            check="stream",
            sent=[hashlib.sha1(p).hexdigest() for p in payloads],
            got=[hashlib.sha1(bytes(e.data)[::-1]).hexdigest() for e in got],
            seqs=[e.stream.seq for e in got],
            channel=sorted({e.stream.channel for e in got}),
            peer=sorted({e.stream.peer_id for e in got}),
            dropped=ch.dropped,
        )

    comm.remove_peer(rid)
    _json(check="removed", has_peer=rid in comm.peers)
    comm.stop()
    print("DONE", flush=True)


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "server":
        serve(sys.argv[2] if len(sys.argv) > 2 else "tcpip")
    elif mode == "client":
        client(*sys.argv[2:6])
    else:
        raise SystemExit(f"unknown mode {mode!r}")
