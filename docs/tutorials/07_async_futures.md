# Tutorial 7: Async Operations with Futures

Use Python's `async` / `await` to read numeric entries from S3, double each value, and write the results back — all without blocking the event loop. This tutorial introduces three async patterns:

1. **`await` on a `GroupFuture`** — the future returned by batch `memorize` / `remember` calls is directly awaitable.
2. **`await` on an individual future** via the policy's future bank.
3. **`async with laila.guarantee_async:`** — an async context manager that automatically tracks and awaits every future created inside it.

## Prerequisites

```bash
pip install "laila-core[s3]"
```

You will need an AWS S3 bucket and credentials. Store them in a `secrets.toml` as described in [Tutorial 3](03_remote_pools.md).

## Setup

```python
import asyncio
import laila
from laila.pool import S3Pool

laila.read_args("./secrets.toml")

s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="async_pool",
)
laila.memory.extend(s3_pool, pool_nickname="async_pool")
```

## Step 1: Create and upload numeric entries

Wrap the integers 1 through 10 as LAILA constant entries and push them to S3. `memorize` with a list returns a `GroupFuture` — `await` it directly instead of calling `laila.wait`:

```python
entries = [laila.constant(data=i, nickname=f"number_{i}") for i in range(1, 11)]

upload_future = laila.memorize(entries, pool_nickname="async_pool")
print("Before await:", laila.status(upload_future))

await upload_future

print("After await: ", laila.status(upload_future))
```

## Step 2: Define the async doubling function

An `async` function that remembers entries from S3, doubles every value, and writes the results back. Each I/O call returns a future that you `await`, so the event loop stays free between operations:

```python
async def double_entries(entry_ids, pool_nickname):
    remember_future = laila.remember(entry_ids, pool_nickname=pool_nickname)
    remembered = await remember_future

    doubled = []
    for entry in remembered:
        new_entry = laila.constant(
            data=entry.data * 2,
            nickname=f"doubled_{entry.nickname}",
        )
        doubled.append(new_entry)

    upload_future = laila.memorize(doubled, pool_nickname=pool_nickname)
    await upload_future

    return doubled
```

## Step 3: Run the async function

Jupyter runs its own `asyncio` event loop, so top-level `await` works out of the box:

```python
original_ids = [e.global_id for e in entries]
doubled_entries = await double_entries(original_ids, pool_nickname="async_pool")

for orig, dbl in zip(entries, doubled_entries):
    print(f"  {orig.data} -> {dbl.data}")
```

## Step 4: Verify the results from S3

Delete local references and recall the doubled entries purely by their `global_id` to prove the values actually round-tripped through S3:

```python
doubled_ids = [e.global_id for e in doubled_entries]
del doubled_entries

verify_future = laila.remember(doubled_ids, pool_nickname="async_pool")
verified = await verify_future

for i, entry in enumerate(verified, start=1):
    expected = i * 2
    assert entry.data == expected
    print(f"  number_{i}: {i} -> {entry.data}")
```

## Step 5: Reactive processing with `guarantee_async`

`laila.guarantee_async` is an async context manager that tracks every future created inside its scope and awaits them all on exit. This is useful for a reactive-style loop where entries arrive one at a time:

```python
quadrupled_entries = []
bank = laila.get_active_policy().future_bank

async with laila.guarantee_async:
    for gid in doubled_ids:
        ref = laila.remember(gid, pool_nickname="async_pool")
        recalled = await bank[ref.global_id]

        quad_entry = laila.constant(
            data=recalled.data * 2,
            nickname=f"quadrupled_{recalled.nickname}",
        )
        laila.memorize(quad_entry, pool_nickname="async_pool")
        quadrupled_entries.append(quad_entry)

print(f"Processed {len(quadrupled_entries)} entries inside guarantee_async")
```

## Step 6: Inspect futures

Every future created by LAILA is stored in the active policy's **future bank**. You can query status at any time with `laila.status(future)`, which returns a percentage breakdown for `GroupFuture` objects:

```python
print("upload_future status:", laila.status(upload_future))
print(f"  children: {len(upload_future)}")

future_bank = laila.get_active_policy().future_bank
print(f"Total futures in bank: {len(future_bank)}")
```

## Clean up

```python
all_ids = (
    original_ids
    + doubled_ids
    + [e.global_id for e in quadrupled_entries]
)

async with laila.guarantee_async:
    laila.forget(all_ids, pool_nickname="async_pool")
```

## Summary

- `laila.memorize` and `laila.remember` with lists return a **`GroupFuture`** — it is directly awaitable via `await future`.
- For a single-entry operation, look up the future in the **future bank** (`laila.get_active_policy().future_bank`) and `await` that.
- **`async with laila.guarantee_async:`** tracks every future created in its scope and awaits them all on exit — ideal for reactive loops where entries arrive one at a time.
- `GroupFuture.__await__` uses `asyncio.gather` internally, so all children resolve concurrently.
- `laila.status(future)` returns a percentage breakdown (`finished`, `running`, `error`, etc.) at any point.
- This pattern generalises to any async data pipeline — transform, filter, enrich, or route entries without blocking.

Next: [Tutorial 8a — Saving the Environment to S3](08a_environment_to_s3.md), where you capture and persist the full policy configuration as a manifest.
