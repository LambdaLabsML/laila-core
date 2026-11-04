"""P27 / P26: with, async def / await, lambdas, comprehensions, slices."""

import asyncio

import laila


async def fetch_all(ids):
    results = [await laila.remember_async(i) for i in ids]
    return results[1:]


def run(ids):
    with laila.guarantee:
        fut = laila.command.submit(lambda: 1)
    picked = sorted(ids, key=lambda i: i[-1])
    return asyncio.run(fetch_all(picked)), fut
