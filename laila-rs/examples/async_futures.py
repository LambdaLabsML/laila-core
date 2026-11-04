"""Futures, guarantees and the async side of the verbs.

Every laila verb returns a future. This script shows the three ways to
collect one: ``.wait()`` on a single future, ``with laila.guarantee:`` to
wait for every future submitted inside the block, and ``await`` from a
coroutine running on the command taskforce, gathered by ``asyncio``.

    python laila-rs/examples/async_futures.py
    cargo run -p laila-core --example async_futures
"""

import asyncio

import laila
from laila.macros.defaults import DefaultPolicy

policy = DefaultPolicy()
laila.activate_policy(policy)

entries = [laila.constant(data=f"value-{i}", nickname=f"item-{i}") for i in range(4)]

# 1. One future, waited on explicitly.
fut = laila.memorize(entries=entries[0])
print("future status before wait:", fut.status)
fut.wait()
print("future status after wait: ", fut.status)

# 2. A guarantee scope: every future submitted inside the block is waited
#    for on exit, and any failure is re-raised at the ``with``.
with laila.guarantee:
    for entry in entries[1:]:
        laila.memorize(entries=entry)
print("all memorized:", all(e.global_id in laila.alpha_pool for e in entries))


# 3. Coroutines: a future is awaitable, so several remembers can run
#    concurrently inside one job submitted to the command taskforce.
async def gather_all():
    futs = [laila.remember(e.global_id) for e in entries]
    return [entry.data for entry in await asyncio.gather(*futs)]


# Submitted jobs hand their result back wrapped in an Entry.
job = laila.command.submit([gather_all])
print("gathered:", job.wait().data)

# 4. Failures surface where the future is collected, not where it is made.
missing = laila.remember("LAILA:ENTRY:00000000-0000-0000-0000-000000000000")
try:
    missing.wait()
except KeyError as err:
    print("remember of a missing entry raised KeyError")

laila.terminate()
