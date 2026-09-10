# Tutorial 9b: Peer Routing and 3-Party Relays

Tutorials 9 and 9a showed *how* policies connect. This tutorial is about *addressing*: the different ways `dst_policy=` can name a peer, what `persist=` means on a peer read, which arguments must be strings, and how one policy can orchestrate a transfer **between two other policies** with `src_policy=` / `dst_policy=` — a 3-party relay.

You will:

- Address a peer by `global_id`, by its `RemotePolicyProxy`, and by **nickname**
- Read, write, and delete on the peer while the active policy stays local; understand `persist=` and the string-only pool rule
- Run a **push relay** (`memorize`) and a **pull relay** (`remember`) from an orchestrator that is peered to the source but not to the destination
- See the topology rule enforced with a `ConnectionError`

**Prerequisites:** `pip install laila-core`. No credentials or external services required.

```python
import laila
from laila.basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from laila.macros.defaults import (
    DefaultPolicy,
    DefaultPool,
    DefaultLoopbackProtocol,
    DefaultTCPProtocol,
)

A = DefaultPolicy()          # the orchestrator; the only policy we activate
laila.activate_policy(A)
```

## Nicknamed policies

A policy normally gets a random UUID. Deriving it from a nickname with `generate_uuid_from_nickname` makes the policy addressable by **name** from any process that shares the namespace — `dst_policy="edge-sensor"` resolves to the same `global_id` everywhere. The helper below also re-stamps the sub-systems that cache the policy id (`communication.policy_id` is what a peer sees during the handshake).

`pool_of(policy, nickname)` is only used to *inspect* pools in this notebook and prove where data landed.

```python
def named_policy(nickname: str) -> DefaultPolicy:
    p = DefaultPolicy()
    p.uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname)
    p.central.communication.policy_id = p.central.command.policy_id = p.global_id
    return p


def pool_of(policy, nickname):
    router = policy.central.memory.pool_router
    return router.pools[router.pools_nicknames[nickname]]


expected = _LAILA_IDENTIFIABLE_OBJECT.to_global_id(nickname="edge-sensor", scopes=["POLICY"])
B = named_policy("edge-sensor")
print("B:", B.global_id)
print("derived from the nickname:", B.global_id == expected)
```

## Part 1: Two-party routing

B gets a loopback transport and a pool nicknamed `b-store`. B is **not** activated, so A reaches it only through its `RemotePolicyProxy` (a gid present in `laila.local_policies` would be routed in-process instead).

```python
b_loop = DefaultLoopbackProtocol()
B.central.communication.add_connection(b_loop)
B.central.memory.extend(DefaultPool(), pool_nickname="b-store")

laila.communication.add_connection(DefaultLoopbackProtocol())
b_id = laila.add_peer(f"loopback://{B.global_id}", b_loop.peer_secret_key)

print("peered:", b_id == B.global_id)
print("laila.peers:", list(laila.peers))
```

### Three ways to say "that peer"

`dst_policy=` (and `policy=` on `forget`, `src_policy=` on relays) accepts:

1. the peer's **`global_id`** string,
2. the **proxy object** from `laila.peers`,
3. a **nickname** — anything that is not a `LAILA:` gid is treated as a policy nickname and hashed to a gid.

All three resolve to the same peer; the memorize below lands three entries in B's `b-store`.

```python
e1 = laila.constant(data={"celsius": 21.5}, nickname="reading-1")
e2 = laila.constant(data={"celsius": 22.0}, nickname="reading-2")
e3 = laila.constant(data={"celsius": 22.5}, nickname="reading-3")

print("by gid:      ", laila.memorize(e1, dst_policy=b_id,              dst_pool="b-store").data)
print("by proxy:    ", laila.memorize(e2, dst_policy=laila.peers[b_id], dst_pool="b-store").data)
print("by nickname: ", laila.memorize(e3, dst_policy="edge-sensor",     dst_pool="b-store").data)

print("all three in B's b-store:", all(e.global_id in pool_of(B, "b-store") for e in (e1, e2, e3)))
print("active policy still A:   ", laila.active_policy.global_id == A.global_id)
```

### Reading back: single ids, lists, and `persist=`

A single id returns a future whose `.data` is the payload and whose `.wait()` is the rebuilt `Entry`; a list returns a `GroupFuture`. Entries cross the wire as self-describing blobs and are rebuilt on A — no shared pool is involved.

On this `dst_policy=` path the peer serves the read **without** caching it back into its own alpha pool, and A does not cache it either, so nothing grows on either side. `persist=` matters on the two other paths — morph mode (Tutorial 9) and same-process routing to a *local* policy — where the flag does reach the executing policy. Passing `persist=False` on peer reads keeps behaviour identical everywhere.

```python
one = laila.remember(e1.global_id, dst_policy="edge-sensor", dst_pool="b-store", persist=False)
print("single:", one.data, "->", type(one.wait()).__name__)

many = laila.remember([e1.global_id, e2.global_id, e3.global_id],
                      dst_policy="edge-sensor", dst_pool="b-store", persist=False)
print("list:  ", type(many).__name__, [entry.data for entry in many.wait()])

b_alpha = pool_of(B, "_memory")            # B's alpha (default in-memory) pool
laila.remember(e2.global_id, dst_policy="edge-sensor", dst_pool="b-store").wait()   # default persist=True
print("cached in B's alpha pool:", e2.global_id in b_alpha, "| cached in A's alpha pool:", e2.global_id in laila.alpha_pool)
```

### The string-only pool rule, `forget`, and unknown peers

A pool **object** can be routed to locally (Tutorial 14) but cannot be shipped to a peer — for a peer, `dst_pool` / `pool` must be a nickname or gid string that exists **on the peer**. `forget` takes `policy=` / `pool=`. Naming a policy that is neither local nor a connected peer raises `ConnectionError`.

```python
try:
    laila.memorize(e1, dst_policy=b_id, dst_pool=DefaultPool())
except TypeError as exc:
    print("TypeError:", exc)

laila.forget(e3.global_id, policy="edge-sensor", pool="b-store").wait()
print("e3 removed from B:", e3.global_id not in pool_of(B, "b-store"))

try:
    laila.remember(e1.global_id, dst_policy="no-such-node", dst_pool="b-store")
except ConnectionError as exc:
    print("ConnectionError:", str(exc)[:72], "...")
```

## Part 2: Three-party relay

Topology: **A** (orchestrator) is peered to **hub**; **hub** is peered to **archive**; A is *not* peered to archive. A can still move entries between hub and archive by naming both:

- **push**: `laila.memorize(gids, src_policy=hub, src_pool=..., dst_policy=archive, dst_pool=...)` — hub reads from its own pool and memorizes into archive over *its* link.
- **pull**: `laila.remember(gids, src_policy=hub, src_pool=..., dst_policy=archive, dst_pool=...)` — hub fetches from archive and stores into its `src_pool`.

Both return the list of transferred gids once the transfer has completed, and both run on **hub** — which is why hub must be peered to archive, while A only needs a link to hub.

In this notebook hub is a second *local* policy (registered with `activate_policy`) and archive is reached over hub's raw-TCP link. In production each would be its own process; the calls are the same.

```python
hub = named_policy("hub")
laila.activate_policy(hub)                                   # hub is local: register it ...
laila.memory.extend(DefaultPool(), pool_nickname="hub-store")
hub_tcp = DefaultTCPProtocol(host="127.0.0.1", port=0)
laila.communication.add_connection(hub_tcp)
laila.activate_policy(A)                                     # ... and hand control back to A

archive = named_policy("archive")                            # archive is remote-only
arc_tcp = DefaultTCPProtocol(host="127.0.0.1", port=0)
archive.central.communication.add_connection(arc_tcp)
archive.central.memory.extend(DefaultPool(), pool_nickname="archive-store")

hub.central.communication.add_peer(f"tcp://127.0.0.1:{arc_tcp.bound_port}", arc_tcp.peer_secret_key)

print("local policies:", [g[-8:] for g in laila.local_policies], "(A, hub)")
print("hub's peers:   ", [g[-8:] for g in hub.central.communication.peers], "(archive)")
print("A's peers:     ", [g[-8:] for g in laila.peers], "(edge-sensor only — A is not peered to archive)")
```

### Seed the hub

A writes two readings into hub's pool. Because hub is a local policy, `dst_policy="hub"` is executed in-process (no wire).

```python
r1 = laila.constant(data={"reading": 1}, nickname="relay-1")
r2 = laila.constant(data={"reading": 2}, nickname="relay-2")
for r in (r1, r2):
    laila.memorize(r, dst_policy="hub", dst_pool="hub-store").wait()

print("hub has both:", all(r.global_id in pool_of(hub, "hub-store") for r in (r1, r2)))
```

### Push relay: hub -> archive

```python
moved = laila.memorize(
    [r1.global_id, r2.global_id],
    src_policy="hub",     src_pool="hub-store",
    dst_policy="archive", dst_pool="archive-store",
)
print("pushed gids:", [g[-8:] for g in moved])
print("archive has both:", all(r.global_id in pool_of(archive, "archive-store") for r in (r1, r2)))
print("active policy still A:", laila.active_policy.global_id == A.global_id)
```

### Pull relay: archive -> hub

Clear hub's copies first, then have hub pull them back from archive.

```python
for r in (r1, r2):
    laila.forget(r.global_id, policy="hub", pool="hub-store").wait()
print("hub emptied:", not any(r.global_id in pool_of(hub, "hub-store") for r in (r1, r2)))

pulled = laila.remember(
    [r1.global_id, r2.global_id],
    src_policy="hub",     src_pool="hub-store",
    dst_policy="archive", dst_pool="archive-store",
)
print("pulled gids:", [g[-8:] for g in pulled])
print("hub has both again:", all(r.global_id in pool_of(hub, "hub-store") for r in (r1, r2)))
```

### The topology rule

`src_policy` must be a local policy or one of A's connected peers, otherwise A has nobody to hand the relay to:

```python
try:
    laila.memorize(r1.global_id, src_policy="ghost", src_pool="x", dst_policy="archive", dst_pool="archive-store")
except ConnectionError as exc:
    print("ConnectionError:", str(exc)[:90], "...")
```

## Tear down

```python
laila.terminate(wait=True)               # A and hub
B.central.communication.stop()
archive.central.communication.stop()
print("local policies left:", len(laila.local_policies))
```

## Summary

- `dst_policy=` / `policy=` / `src_policy=` accept a gid, a `RemotePolicyProxy`, or a policy nickname; nicknames are hashed to gids, so give a policy a nickname-derived uuid to make it addressable by name.
- Peer-targeted pools must be **strings** (nickname or gid on the peer); pool objects are local-only.
- On the `dst_policy=` wire path nothing is cached on either side; pass `persist=False` on peer reads so morph-mode and same-process routing behave the same.
- A 3-party relay is `memorize` (push) or `remember` (pull) with both `src_policy` and `dst_policy`; it executes on the *source* policy, which must be peered to the destination, while the orchestrator only needs a link to the source.
- Unknown policies fail fast with `ConnectionError`.

Next: [Tutorial 10 — Accessing S3 Through a Remote Peer](10_peer_remote_s3.md).
