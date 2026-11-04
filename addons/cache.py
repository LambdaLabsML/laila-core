"""Default caching process: ``RAM << HDD (HDF5) << CLOUD``.

A single call wires the active policy's alpha pool (RAM) in front of a
local HDF5 pool, which in turn fronts the cloud pool holding the
dataset. Reads routed at the alpha pool (the default for
``laila.remember`` / ``Manifest.realized``) then walk the chain on a
miss and write the blob back into every tier in front:

    alpha misses -> hdd misses -> cloud hits -> hdd caches -> alpha caches

Writes, deletes and ``exists`` stay local to the pool they are issued
on, so ``laila.forget(gids)`` evicts from RAM only and never touches the
HDF5 cache or the cloud origin.
"""

from __future__ import annotations

from typing import Any

import laila


def default_tiers(
    cloud: Any,
    *,
    hdd: Any | None = None,
    hdd_nickname: str = "hdd",
    cloud_nickname: str = "cloud",
    force: bool = False,
) -> tuple[Any, Any]:
    """Register *hdd* and *cloud* and wire ``laila.alpha_pool << hdd << cloud``.

    Parameters
    ----------
    cloud : pool | str
        The origin pool holding the dataset: a live pool, or the gid /
        nickname of a pool already registered with ``laila.memory``.
    hdd : pool | str, optional
        The on-disk tier. Defaults to a fresh ``HDF5Pool(nickname=hdd_nickname)``
        whose file lives under ``<laila root>/pools/<uuid>/pool.h5py``
        (relocate with ``laila.set_default_directory`` *before* calling).
    hdd_nickname, cloud_nickname : str
        Nicknames the two pools are registered under when they are live
        objects (ignored when a string ref is given).
    force : bool, default False
        When the alpha pool is already proxied to a *different* pool,
        re-wire it instead of raising.

    Returns
    -------
    (hdd_pool, cloud_pool)
        The live pool objects, nearest tier first.

    The call is idempotent: wiring the same two pools twice is a no-op.
    """
    memory = laila.memory
    cloud_pool = _ensure_registered(memory, cloud, cloud_nickname)

    if hdd is None:
        from laila.data import HDF5Pool

        hdd = HDF5Pool(nickname=hdd_nickname)
    hdd_pool = _ensure_registered(memory, hdd, hdd_nickname)

    if hdd_pool is cloud_pool:
        raise ValueError("hdd and cloud must be different pools")

    alpha = laila.alpha_pool
    current = alpha.proxy_to
    if current is not None and current is not hdd_pool:
        if not force:
            raise ValueError(
                f"alpha pool is already proxied to {current.global_id}; "
                "pass force=True to re-wire it"
            )
        alpha.proxy_to = None

    if alpha.proxy_to is None:
        alpha.proxy_to = hdd_pool
    if hdd_pool.proxy_to is not cloud_pool:
        hdd_pool.proxy_to = cloud_pool
    return hdd_pool, cloud_pool


def _ensure_registered(memory, pool_ref: Any, nickname: str):
    """Return the live pool for *pool_ref*, registering live objects under *nickname*."""
    if isinstance(pool_ref, str):
        return memory._resolve_pool_ref(pool_ref)
    router = memory.pool_router
    if pool_ref.global_id not in router.pools:
        memory.extend(pool_ref, pool_nickname=nickname)
    elif nickname not in router.pools_nicknames:
        router.pools_nicknames[nickname] = pool_ref.global_id
    return router.pools[pool_ref.global_id]
