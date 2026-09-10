# Tutorial 9a: The Transport Zoo — Loopback, TCP, UDP, Unix Sockets, TLS

Tutorial 9 peered two processes over a WebSocket. That is one of 50+ transports under `laila.policy.central.communication.protocols`, all sharing the same surface: register a protocol with `add_connection`, peer with `add_peer(uri, secret)`, and every `memorize` / `remember` / `forget` with `dst_policy=` just works.

You will:

- Peer two in-process policies over **loopback**, **raw TCP**, **UDP**, and a **Unix socket**, and switch the wire codec to **msgpack**
- Run the same flow over **TLS** with a self-signed certificate (optional)
- Hold two transports to the same peer and pin one with `comm=`, `laila.request(...)`, and `proxy.via(...)`
- Look at the **liveness** loop and the **backpressure** knobs
- See how transports are mirrored into `laila.args.environment`

**Prerequisites:** `pip install laila-core` (msgpack ships with it). TLS additionally needs the `openssl` binary. No credentials or external services required.

> **One process, real wire.** `dst_policy=<gid>` is routed *locally* whenever that gid is in `laila.local_policies`. We therefore activate only policy A; each peer B is a plain `DefaultPolicy()` that is never activated, so A can only reach it through its `RemotePolicyProxy` — i.e. over the transport under test.

```python
import json
import os
import subprocess
import tempfile
import time

import laila
from laila.macros.defaults import (
    DefaultPolicy,
    DefaultPool,
    DefaultLoopbackProtocol,
    DefaultTCPProtocol,
    DefaultUDPProtocol,
    DefaultUnixSocketProtocol,
    DefaultTLSProtocol,
)
from laila.policy.central.communication.protocols import iter_comm_protocols

A = DefaultPolicy()
laila.activate_policy(A)

names = sorted(cls.protocol_name for cls in iter_comm_protocols())
print(f"{len(names)} transports registered, e.g.: {names[:12]} ...")
```

## Helpers

`make_peer(*protocols)` builds a peer policy with the given transports and a pool nicknamed `store`. `uri_for(proto, policy)` derives the URI a client uses to reach it:

| Transport | Class | URI |
|---|---|---|
| Loopback (same process) | `DefaultLoopbackProtocol` | `loopback://<policy global_id>` |
| Raw TCP | `DefaultTCPProtocol` | `tcp://host:port` |
| UDP | `DefaultUDPProtocol` | `udp://host:port` |
| Unix domain socket | `DefaultUnixSocketProtocol` | `unix:///path/to.sock` |
| TLS over TCP | `DefaultTLSProtocol` | `tls://host:port` |
| WebSocket (Tutorial 9) | `DefaultWebSocketProtocol` | `ws://host:port` |

Every carrier transport gets a random `peer_secret_key` unless you pass one; `port=0` (or an empty `path`) lets the OS pick, and `bound_port` / `bound_path` tell you what was chosen.

```python
def make_peer(*protocols):
    peer = DefaultPolicy()                       # NOT activated -> reachable only over the wire
    for proto in protocols:
        peer.central.communication.add_connection(proto)
    peer.central.memory.extend(DefaultPool(), pool_nickname="store")
    return peer


def uri_for(proto, policy):
    name = proto.protocol_name
    if name == "loopback":
        return f"loopback://{policy.global_id}"
    if name == "unix":
        return f"unix://{proto.bound_path}"
    return f"{name}://127.0.0.1:{proto.bound_port}"


def round_trip(peer_id, payload):
    e = laila.constant(data=payload)
    laila.memorize(e, dst_policy=peer_id, dst_pool="store").wait()
    return laila.remember(e.global_id, dst_policy=peer_id, dst_pool="store", persist=False).data
```

## Step 1: The same flow over four transports (and two codecs)

The loop body never changes: register A's end of the transport, `add_peer` to B's URI, push an entry, pull it back. The only variable is the protocol constructor. `codec="msgpack"` swaps the JSON-RPC payload encoding for a compact binary one — useful on constrained links.

```python
payload = {"hello": "peer", "values": [1, 2, 3]}

transports = [
    ("loopback",     lambda: DefaultLoopbackProtocol()),
    ("tcp",          lambda: DefaultTCPProtocol(host="127.0.0.1", port=0)),
    ("udp",          lambda: DefaultUDPProtocol(host="127.0.0.1", port=0)),
    ("unix",         lambda: DefaultUnixSocketProtocol()),
    ("tcp/msgpack",  lambda: DefaultTCPProtocol(host="127.0.0.1", port=0, codec="msgpack")),
]

for label, factory in transports:
    proto_a, proto_b = factory(), factory()
    laila.communication.add_connection(proto_a)
    B = make_peer(proto_b)

    uri = uri_for(proto_b, B)
    t0 = time.perf_counter()
    peer_id = laila.add_peer(uri, proto_b.peer_secret_key)
    ok = round_trip(peer_id, payload) == payload
    ms = (time.perf_counter() - t0) * 1000
    print(f"{label:12s} {uri[:44]:46s} codec={proto_a.codec:8s} ok={ok}  {ms:5.1f} ms")

    laila.communication.remove_peer(peer_id)
    laila.communication.remove_connection(proto_a)
    B.central.communication.stop()
```

A mismatched secret is rejected during the handshake:

```python
proto_a, proto_b = DefaultTCPProtocol(host="127.0.0.1", port=0), DefaultTCPProtocol(host="127.0.0.1", port=0)
laila.communication.add_connection(proto_a)
B = make_peer(proto_b)
try:
    laila.add_peer(uri_for(proto_b, B), "wrong-secret")
except ConnectionError as exc:
    print("ConnectionError:", exc)
laila.communication.remove_connection(proto_a)
B.central.communication.stop()
```

## Step 2: TLS (optional)

`DefaultTLSProtocol` is raw TCP wrapped in TLS. The server side needs `certfile` / `keyfile`; a client without `cafile` skips verification, so a self-signed certificate is enough for a demo (pass `cafile=` and `server_hostname=` in production). The cell skips itself if `openssl` is not installed.

```python
tmp_dir = tempfile.mkdtemp(prefix="laila_tls_")
cert, key = os.path.join(tmp_dir, "cert.pem"), os.path.join(tmp_dir, "key.pem")
try:
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-keyout", key, "-out", cert, "-subj", "/CN=localhost"],
        check=True, capture_output=True,
    )
    tls_a = DefaultTLSProtocol()                                       # client only
    tls_b = DefaultTLSProtocol(host="127.0.0.1", port=0, certfile=cert, keyfile=key)
    laila.communication.add_connection(tls_a)
    B = make_peer(tls_b)
    peer_id = laila.add_peer(f"tls://127.0.0.1:{tls_b.bound_port}", tls_b.peer_secret_key)
    print("over TLS:", round_trip(peer_id, "encrypted in transit"))
    laila.communication.remove_peer(peer_id)
    laila.communication.remove_connection(tls_a)
    B.central.communication.stop()
except FileNotFoundError:
    print("openssl not available — skipping the TLS demo")
```

## Step 3: Two transports to one peer, and channel pinning

A and B each register **both** a TCP and a UDP transport, and A peers over both URIs. The peer is the same policy, so `laila.peers` holds a single proxy — but two channels can carry its calls. By default the first transport that holds the peer is used; `comm=` picks a specific one, by protocol token (`"tcp"`, `"udp"`, ...) or by a connection's `global_id`.

The same selector is available on the proxy: `laila.request(peer_id, comm_protocol="udp")` and `laila.peers[peer_id].via("udp")` return a channel-bound proxy whose attribute-chain calls are executed on the peer over that channel.

```python
a_tcp, a_udp = DefaultTCPProtocol(host="127.0.0.1", port=0), DefaultUDPProtocol(host="127.0.0.1", port=0)
b_tcp, b_udp = DefaultTCPProtocol(host="127.0.0.1", port=0), DefaultUDPProtocol(host="127.0.0.1", port=0)
laila.communication.add_connection(a_tcp)
laila.communication.add_connection(a_udp)
B = make_peer(b_tcp, b_udp)

peer_id = laila.add_peer(uri_for(b_tcp, B), b_tcp.peer_secret_key)
same_id = laila.add_peer(uri_for(b_udp, B), b_udp.peer_secret_key)

print("one peer, two channels:", peer_id == same_id, "| peers:", len(laila.peers))
print("tcp holds peer:", a_tcp.has_peer(peer_id), "| udp holds peer:", a_udp.has_peer(peer_id))
print("default channel:", laila.communication._select_protocol_for_peer(peer_id, None).protocol_name)
```

```python
e = laila.constant(data={"channel": "pinned"}, nickname="pinned-entry")

laila.memorize(e, dst_policy=peer_id, dst_pool="store", comm="udp").wait()
print("read over udp:         ", laila.remember(e.global_id, dst_policy=peer_id, dst_pool="store", persist=False, comm="udp").data)
print("read over tcp (by gid):", laila.remember(e.global_id, dst_policy=peer_id, dst_pool="store", persist=False, comm=a_tcp.global_id).data)

# Direct RPC on a channel-bound proxy: the attribute chain runs on B when called.
bound = laila.request(peer_id, comm_protocol="tcp")
print("B's pool nicknames (via tcp):", list(bound.central.memory.pool_router.pools_nicknames.copy()))
print("B's pool nicknames (via udp):", list(laila.peers[peer_id].via("udp").central.memory.pool_router.pools_nicknames.copy()))
print(repr(laila.peers[peer_id].via("udp")))

try:
    laila.remember(e.global_id, dst_policy=peer_id, dst_pool="store", comm="lora")
except ConnectionError as exc:
    print("unknown channel ->", str(exc)[:90], "...")
```

## Step 4: Liveness

The communication hub runs a background loop that pings every peer with a reserved `__comm_ping__` control frame (answered before the request reaches the policy, and exempt from backpressure). Peers that stop answering are disconnected and dropped from `laila.peers`.

- `liveness_enabled` (default `True`) and `liveness_interval` (default 15 s) live on the hub and are CLI-configurable.
- `proto.ping(peer_id)` is the same probe, callable by hand.
- `remove_peer(peer_id)` disconnects through whichever transport holds the peer. UDP is connectionless, so the *other* side only notices once liveness times out — that is exactly what the loop is for.

```python
hub = laila.communication
print("liveness enabled:", hub.liveness_enabled, "| interval:", hub.liveness_interval, "s")
print("ping over tcp:", a_tcp.ping(peer_id), "| over udp:", a_udp.ping(peer_id), "| unknown peer:", a_tcp.ping("nobody"))
```

## Step 5: Backpressure

Inbound RPCs are admitted through a per-policy bounded semaphore sized by `max_inflight_rpcs` (default 1000; embedded devices typically set 32-64). When the budget is exhausted the carrier answers `ERR_BUSY` immediately instead of queueing, and the *sender* retries with exponential backoff and jitter (`rpc_backoff_base`, `rpc_backoff_max`, `max_rpc_retries`) before surfacing a `BackpressureError`. The cell below shrinks B's budget to two slots and shows the third admission being refused.

```python
from laila.policy.central.communication.protocols._carriers.base import BackpressureError

print("A's max_inflight_rpcs:", hub.max_inflight_rpcs)
print("sender backoff (tcp):  base", a_tcp.rpc_backoff_base, "s, max", a_tcp.rpc_backoff_max, "s, retries", a_tcp.max_rpc_retries)

b_hub = B.central.communication
b_hub.max_inflight_rpcs = 2
b_hub._rpc_semaphore = None          # rebuild the gate with the new size
slots = [b_hub._acquire_rpc_slot() for _ in range(3)]
print("admissions with a budget of 2:", slots, "-> the third would be answered ERR_BUSY")
for granted in slots:
    if granted:
        b_hub._release_rpc_slot()
print("BackpressureError is raised on the sender after", a_tcp.max_rpc_retries, "busy retries:", BackpressureError.__name__)
```

## Step 6: Transports in `laila.args.environment`

Every CLI-capable transport dumps to plain JSON, and the live environment mirror lists each registered connection under `central.communication.connections.<protocol_gid>` together with the hub's own knobs. That is what lets Tutorial 8b rebuild a policy *with its transports* from a saved snapshot (runtime state such as `peers` is `CLIExempt` and never mirrored).

```python
env = laila.args.environment.policies[A.global_id].toDict()
comm_env = env["central"]["communication"]

print("hub knobs:", {k: comm_env[k] for k in ("liveness_enabled", "liveness_interval", "max_inflight_rpcs")})
for gid, cfg in comm_env["connections"].items():
    print(f"  {cfg['class_token']:45s} host={cfg.get('host')} port={cfg.get('port')} codec={cfg.get('codec')}")

print("\nJSON-safe transport dump:", json.dumps(a_udp.model_dump(mode="json"))[:90], "...")
```

## Tear down

```python
laila.communication.stop()          # stops A's transports, clears its peers
B.central.communication.stop()
print("A peers:", list(laila.peers))
```

## Summary

- All transports share one surface: `add_connection(proto)`, `add_peer(uri, secret)`, then `dst_policy=` routing. The URI scheme selects the transport; `codec="msgpack"` swaps the wire encoding.
- Loopback needs no socket at all and is the fastest way to exercise the full inter-policy path in one process; TCP, UDP, Unix sockets, and TLS cover the IP and local cases; broker-, bus-, and radio-backed transports (MQTT, AMQP, CAN, I2C, LoRa, BLE, ...) follow the same pattern and import their drivers lazily.
- One peer may be reachable over several channels; pin one with `comm=` on the memory verbs, `laila.request(gid, comm_protocol=...)`, or `proxy.via(...)`.
- Liveness (`liveness_interval`, `ping`) evicts dead peers; backpressure (`max_inflight_rpcs`, `rpc_backoff_*`, `BackpressureError`) keeps a flooded policy's backlog bounded.
- Transports are CLI-capable and appear in `laila.args.environment` under `central.communication.connections`.

Next: [Tutorial 9b — Peer Routing and Relays](09b_peer_routing_and_relay.md).
