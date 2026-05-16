# Tutorial 17: Three-Node Mesh on Localhost

[Tutorial 9](09_peer_request_entry.md) introduced two-node peering. This tutorial scales to three policies running in the same process, each peered to the other two — a fully connected mesh. Any node can request any other node's data through a peer proxy.

## Prerequisites

```bash
pip install laila-core
```

## Build three nodes

`_make_policy()` builds a fresh policy with its own TCP listener and an in-memory pool. The pattern mirrors the helpers used in LAILA's own communication tests:

```python
import uuid
import laila
from laila.macros.defaults import DefaultPool, DefaultTCPIPProtocol
from laila.policy.schema.base import _LAILA_IDENTIFIABLE_POLICY

def make_policy(label: str):
    policy = _LAILA_IDENTIFIABLE_POLICY()
    laila._local_policies[policy.global_id] = policy
    laila.active_policy = policy

    tcp = DefaultTCPIPProtocol(host="127.0.0.1", port=0, peer_secret_key=uuid.uuid4().hex)
    policy.central.communication.add_connection(tcp)
    tcp.start()

    pool = DefaultPool()
    policy.central.memory.extend(pool, pool_nickname=f"{label}_store")
    return policy, tcp

A, tcp_A = make_policy("A")
B, tcp_B = make_policy("B")
C, tcp_C = make_policy("C")
```

## Pair-peer every combination

```python
def pair(a, a_tcp, b, b_tcp):
    laila.active_policy = a
    a.central.communication.add_tcpip_peer("127.0.0.1", b_tcp.bound_port, b_tcp.peer_secret_key)

pair(A, tcp_A, B, tcp_B)
pair(A, tcp_A, C, tcp_C)
pair(B, tcp_B, C, tcp_C)
```

After the handshakes, each policy sees the other two in `laila.peers` (when that policy is the active one).

## Store one entry per node

Each node owns a different entry, all under its own pool nickname:

```python
laila.active_policy = A
laila.memorize(laila.constant(data="payload from A", nickname="payload_A"), pool_nickname="A_store").wait()

laila.active_policy = B
laila.memorize(laila.constant(data="payload from B", nickname="payload_B"), pool_nickname="B_store").wait()

laila.active_policy = C
laila.memorize(laila.constant(data="payload from C", nickname="payload_C"), pool_nickname="C_store").wait()
```

## Fetch B's entry from A via active-policy swap

Setting `laila.active_policy` to a peer proxy routes subsequent calls over the wire:

```python
laila.active_policy = A
proxy_B = laila.peers[B.global_id]
laila.active_policy = proxy_B

result = laila.remember(nickname="payload_B", pool_nickname="B_store", persist=False).wait()
```

## `laila.universe` — lookup by gid without picking a host

`laila.universe` is the union of local and remote policies, keyed by `global_id`. Useful when you have a gid in hand and do not want to think about whether it is local or remote:

```python
universe = laila.universe
print(C.global_id in universe)  # True
```

## Disconnect one node — others stay healthy

Stopping `B`'s communication tears down its server thread and drops it from every peer's `peers` map. Subsequent attempts from `A` to reach `B` raise a connection error; `C` is unaffected:

```python
B.central.communication.stop()
```

## Tear down

```python
laila.terminate(wait=True)
```

## Summary

- Three (or more) local policies can be peered in the same process by giving each its own TCP listener.
- `add_tcpip_peer` is symmetric — pair any two nodes and both directions work.
- `laila.peers` lists peers for the **active** policy; `laila.universe` is the cross-policy view.
- Disconnecting one node degrades cleanly: the other peers drop it from their maps without disturbing each other.

Next: the Advanced track, starting with [Tutorial 18 — End-to-End Encrypted Entries](18_encryption.md).
