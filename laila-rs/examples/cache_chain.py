"""A two-tier cache: a fast in-memory pool fronting a SQLite pool on disk.

Register two pools with the active policy, an in-memory ``DefaultPool``
(``hot``) and an ``SQLitePool`` (``cold``), and wire them with the ``<<``
operator so ``hot`` is a proxy for ``cold``. A read that misses the hot
tier falls through to the cold tier and is cached on the way back, so the
second read of the same entry never touches the database.

    python laila-rs/examples/cache_chain.py
    cargo run -p laila-core --example cache_chain
"""

import shutil
import tempfile

import laila
from laila.data.sqlite import SQLitePool
from laila.macros.defaults import DefaultPolicy, DefaultPool

policy = DefaultPolicy()
laila.activate_policy(policy)

# Keep the SQLite file in a scratch directory so the script leaves no trace.
scratch = tempfile.mkdtemp(prefix="laila-cache-chain-")
laila.set_default_directory(scratch)

hot = DefaultPool()
cold = SQLitePool(name="cold")
laila.memory.extend(pool=hot, affinity=1.0, pool_nickname="hot")
laila.memory.extend(pool=cold, affinity=0.5, pool_nickname="cold")

# ``cache << origin``: reads that miss ``hot`` fall through to ``cold``.
hot << cold
print("hot proxies to:", hot.proxy_to is cold)

# Write the durable copies straight into the cold tier.
entries = [laila.constant(data=i * i, nickname=f"square-{i}") for i in range(5)]
with laila.guarantee:
    laila.memorize(entries=entries, pool_nickname="cold")

print("hot  tier holds:", [e.global_id in hot for e in entries])
print("cold tier holds:", [e.global_id in cold for e in entries])

# First read through the hot tier misses locally, falls through to SQLite
# and caches the blob in memory on the way back.
key = entries[3].global_id
blob = hot[key]
print("fall-through read:", sorted(blob))
print("hot tier now holds it:", key in hot)

# The second read is served from memory; the cold tier is never consulted.
again = hot[key]
print("cached read:", sorted(again) == sorted(blob))

# Deleting from the hot tier is local-only: the cold copy stays put.
del hot[key]
print("after del: hot =", key in hot, "cold =", key in cold)

# The durable copy is still readable through central memory.
remembered = laila.remember(key, pool_nickname="cold").wait()
print("remembered:", remembered.data, "id matches:", remembered.global_id == key)

laila.terminate()
shutil.rmtree(scratch, ignore_errors=True)
