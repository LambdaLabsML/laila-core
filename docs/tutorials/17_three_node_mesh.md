# Tutorial 17: Three-Node Mesh on Localhost

Tutorial 9 introduced two-node peering across processes. This tutorial scales to three policies running in the same process, each peered to the other two — a fully connected mesh — and drives everything from one orchestrating node with `dst_policy=` routing.

You will:

- Spin up three policies, each with its own raw-TCP listener and an in-memory pool
- Pair-peer every combination with `add_peer("tcp://...")`
- Seed one entry per node from the orchestrator, then fetch any entry from any node
- Inspect `laila.peers` vs `laila.universe`
- Drop one peer link and confirm the rest of the mesh stays healthy

**Prerequisites:** `pip install laila-core`. No credentials or external services required.

> **Why only one policy is activated.** LAILA routes `dst_policy=<gid>` to a *local* policy directly whenever that gid is registered in `laila.local_policies`. To exercise the real wire path inside one process we activate only node A; B and C exist as plain policy objects that A can reach solely through their `RemotePolicyProxy`.

```python
import time

import laila
from laila.macros.defaults import DefaultPolicy, DefaultPool, DefaultTCPProtocol
```

## Helper functions

`make_node()` builds a policy with its own raw-TCP transport (`port=0` picks a free port; every carrier transport generates a random `peer_secret_key` unless you pass one) and an in-memory pool. `pair(a, b)` connects `a` to `b` through `b`'s bound port and secret using a `tcp://` URI. Peering is symmetric, so one call per pair is enough.

```python
def make_node(label: str):
    policy = DefaultPolicy()
    tcp = DefaultTCPProtocol(host="127.0.0.1", port=0)
    policy.central.communication.add_connection(tcp)          # starts listening
    policy.central.memory.extend(DefaultPool(), pool_nickname=f"{label}_store")
    return policy, tcp


def pair(a, b, tcp_b):
    uri = f"tcp://127.0.0.1:{tcp_b.bound_port}"
    return a.central.communication.add_peer(uri, tcp_b.peer_secret_key)


def short(gid):
    return gid[-8:]
```

## Build three nodes

Only A becomes the active (and therefore locally registered) policy.

```python
A, tcp_A = make_node("A")
B, tcp_B = make_node("B")
C, tcp_C = make_node("C")
laila.activate_policy(A)

for label, p, tcp in (("A", A, tcp_A), ("B", B, tcp_B), ("C", C, tcp_C)):
    print(f"{label}: gid ...{short(p.global_id)}  port {tcp.bound_port}")
print("local policies:", [short(g) for g in laila.local_policies])
```

## Pair-peer every combination

```python
pair(A, B, tcp_B)
pair(A, C, tcp_C)
pair(B, C, tcp_C)
time.sleep(0.3)

for label, p in (("A", A), ("B", B), ("C", C)):
    print(f"{label} peers:", [short(g) for g in p.central.communication.peers])
```

## `laila.peers` vs `laila.universe`

`laila.peers` lists the peers of the **active** policy (A). `laila.universe` is the union of local and remote policies known to this process, keyed by `global_id` — useful when you hold a gid and do not care where its owner lives.

```python
print("laila.peers:   ", [short(g) for g in laila.peers])
print("laila.universe:", [short(g) for g in laila.universe])
print("B reachable from A:", B.global_id in laila.universe)
print("C reachable from A:", C.global_id in laila.universe)
```

## Seed one entry per node from the orchestrator

A writes into its own pool locally and pushes one entry each into B's and C's pools with `dst_policy=`. The push returns a local future whose `.data` is the gid stored on the peer.

```python
entry_A = laila.constant(data="payload from A", nickname="payload_A")
entry_B = laila.constant(data="payload from B", nickname="payload_B")
entry_C = laila.constant(data="payload from C", nickname="payload_C")

laila.memorize(entry_A, dst_pool="A_store").wait()
print("stored on B:", laila.memorize(entry_B, dst_policy=B.global_id, dst_pool="B_store").data)
print("stored on C:", laila.memorize(entry_C, dst_policy=C.global_id, dst_pool="C_store").data)
```

## Fetch any entry from any node

`dst_policy=` accepts a gid **or** the proxy object from `laila.peers`. `persist=False` avoids caching a copy in the peer's alpha pool.

```python
from_B = laila.remember(entry_B.global_id, dst_policy=B.global_id, dst_pool="B_store", persist=False)
from_C = laila.remember(entry_C.global_id, dst_policy=laila.peers[C.global_id], dst_pool="C_store", persist=False)

print("A fetched from B:", from_B.data)
print("A fetched from C:", from_C.data)
print("active policy still A:", laila.active_policy.global_id == A.global_id)
```

## Drop one link — the rest of the mesh stays healthy

`laila.communication.remove_peer(gid)` closes A's link to B (raw TCP and the other carrier transports implement `disconnect`). B disappears from A's peers, so routing to it raises `ConnectionError`, while A -> C and B <-> C are unaffected.

```python
laila.communication.remove_peer(B.global_id)
time.sleep(0.3)

print("A peers now:", [short(g) for g in laila.peers])
print("C peers now:", [short(g) for g in C.central.communication.peers])

try:
    laila.remember(entry_B.global_id, dst_policy=B.global_id, dst_pool="B_store", persist=False)
except ConnectionError as exc:
    print("A -> B:", str(exc)[:60], "...")

still = laila.remember(entry_C.global_id, dst_policy=C.global_id, dst_pool="C_store", persist=False)
print("A -> C still works:", still.data)
```

## Take B fully offline

Stopping B's communication hub closes its listener and every link it holds; C drops B from its peer map as the connection goes away.

```python
B.central.communication.stop()
time.sleep(0.5)

print("B peers:", [short(g) for g in B.central.communication.peers])
print("C peers:", [short(g) for g in C.central.communication.peers])
```

## Tear down

`laila.terminate(wait=True)` shuts down every local policy (transports, taskforces, pools). C was never registered locally, so stop its hub explicitly.

```python
errors = laila.terminate(wait=True)
C.central.communication.stop()
print("teardown errors:", errors)
print("local policies remaining:", len(laila.local_policies))
```

## Summary

- Three (or more) policies can be meshed in one process by giving each its own transport; `add_peer("tcp://host:port", secret)` is symmetric, so one call per pair suffices.
- Activate only the orchestrating node: gids found in `laila.local_policies` are routed locally, everything else goes over the wire via the peer's `RemotePolicyProxy`.
- `laila.memorize` / `laila.remember` with `dst_policy=` (gid or proxy) move entries to and from any node while the active policy stays put.
- `laila.peers` is the active policy's view; `laila.universe` is the process-wide view.
- `remove_peer` drops a single link and `communication.stop()` takes a node offline; the remaining links keep working.

Next: [Tutorial 18 — End-to-End Encrypted Entries](18_encryption.md).
