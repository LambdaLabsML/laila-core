"""``DataLoader``: a ``torch.utils.data.DataLoader`` with lookahead prefetching.

Everything torch gives you (``batch_size``, ``shuffle``, ``drop_last``,
``sampler`` / ``batch_sampler``, ``collate_fn``, ``generator``,
``pin_memory``, ``len(loader)``) is inherited untouched. Only the fetch
path is replaced: instead of worker processes calling ``dataset[i]``
one item at a time, each index batch is sliced into a sub-manifest and
realized on LAILA's taskforce ``lookahead`` batches ahead of the
training loop, through whatever proxy chain fronts the alpha pool
(``alpha << hdd << cloud``, see :func:`laila.addons.cache.default_tiers`).

The iterator derives from torch's own ``_BaseDataLoaderIter`` so that
sampler iteration, generator consumption (``_base_seed``), pin-memory
device selection and profiling hooks are byte-for-byte the stock
behaviour: with the same ``generator`` seed this loader yields the same
batches in the same order as ``torch.utils.data.DataLoader``.

Memory discipline
-----------------
No LAILA pool evicts on its own, so the loader forgets every consumed
batch from the alpha pool (``evict="auto"``: only when the alpha pool
fronts a proxy chain, i.e. RAM is a cache and not the only copy). A
reference count over the gids of the batches still in flight makes sure
an asset shared by several datapoints (one image, many questions) is
forgotten only once the last pending batch that needs it has been
realized. ``forget_from_hdd=True`` extends the eviction to the on-disk
tier for datasets larger than the disk. The cloud origin is never
written to or deleted from.
"""

from __future__ import annotations

import asyncio
import collections
import functools
import time
from typing import Any

import torch.utils.data
from torch.utils.data.dataloader import _BaseDataLoaderIter

import laila
from laila.policy.central.memory.schema.manifest import Manifest

from .dataset import Dataset, should_evict


class DataLoader(torch.utils.data.DataLoader):
    """torch ``DataLoader`` whose batches are sub-manifests prefetched ``lookahead`` steps ahead.

    Parameters (beyond ``torch.utils.data.DataLoader``'s)
    ------------------------------------------------------
    dataset : Dataset | Manifest
        A :class:`~laila.addons.torch.Dataset`, or a bare ``Manifest``
        which is wrapped in one (no transforms).
    lookahead : int, default 4
        Number of batches kept in flight on LAILA's taskforce.
    evict : bool | "auto", default "auto"
        Forget consumed batches from the alpha (RAM) pool. ``"auto"``
        evicts only when the alpha pool fronts a proxy chain (there is
        an upstream tier to re-read from); ``True`` always; ``False``
        never.
    forget_from_hdd : bool, default False
        Also evict consumed batches from the on-disk tier *hdd_pool*.
    hdd_pool : pool | str, default "hdd"
        The on-disk tier used when *forget_from_hdd* is set.

    ``num_workers`` must stay ``0``: prefetching is LAILA's job, not
    multiprocessing's. Every other torch argument keeps its meaning.
    """

    def __init__(
        self,
        dataset: Dataset | Manifest,
        batch_size: int | None = 1,
        shuffle: bool | None = None,
        sampler=None,
        batch_sampler=None,
        num_workers: int = 0,
        collate_fn=None,
        pin_memory: bool = False,
        drop_last: bool = False,
        timeout: float = 0,
        worker_init_fn=None,
        multiprocessing_context=None,
        generator=None,
        *,
        prefetch_factor: int | None = None,
        persistent_workers: bool = False,
        lookahead: int = 4,
        evict: bool | str = "auto",
        forget_from_hdd: bool = False,
        hdd_pool: Any = "hdd",
        **torch_kwargs: Any,
    ) -> None:
        if num_workers:
            raise ValueError(
                "laila.addons.torch.DataLoader prefetches on LAILA's taskforce; "
                "use lookahead=<n> instead of num_workers"
            )
        if lookahead < 1:
            raise ValueError("lookahead must be >= 1")
        if evict not in (True, False, "auto"):
            raise ValueError("evict must be True, False or 'auto'")
        if isinstance(dataset, Manifest):
            dataset = Dataset(dataset)
        if not isinstance(dataset, Dataset):
            raise TypeError(
                "DataLoader expects a laila.addons.torch.Dataset or a Manifest, "
                f"got {type(dataset).__name__}"
            )
        super().__init__(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            sampler=sampler,
            batch_sampler=batch_sampler,
            num_workers=0,
            collate_fn=collate_fn,
            pin_memory=pin_memory,
            drop_last=drop_last,
            timeout=timeout,
            worker_init_fn=worker_init_fn,
            multiprocessing_context=multiprocessing_context,
            generator=generator,
            prefetch_factor=prefetch_factor,
            persistent_workers=persistent_workers,
            **torch_kwargs,
        )
        self.lookahead = lookahead
        self.evict = evict
        self.forget_from_hdd = forget_from_hdd
        self.hdd_pool = hdd_pool
        self.last_wait_s = 0.0  # seconds blocked in the last __next__

    def _get_iterator(self) -> _BaseDataLoaderIter:
        return _LookaheadIter(self)

    def __iter__(self) -> _BaseDataLoaderIter:
        return self._get_iterator()


class _LookaheadIter(_BaseDataLoaderIter):
    """One epoch: keeps ``lookahead`` sub-manifests in flight on LAILA's taskforce."""

    def __init__(self, loader: DataLoader) -> None:
        super().__init__(loader)
        self.loader = loader
        self.dataset: Dataset = loader.dataset
        self._pending: collections.deque = collections.deque()  # (sub, gids, future), oldest first
        self._inflight: collections.Counter = collections.Counter()  # gid -> pending batches
        self._closed = False
        for _ in range(loader.lookahead):
            self._schedule_next()

    # -- async pipeline, runs on LAILA's taskforce -------------------------
    async def _prepare_batch(self, sub: Manifest, keys: list[str]):
        realized = await sub.async_realized  # {key: {modality: Entry}}, via the proxy chain
        loop = asyncio.get_running_loop()
        # Decoding / collation is CPU-bound: keep it off the loop thread so
        # the other in-flight sub-manifests keep reading.
        return await loop.run_in_executor(None, self._decode_and_collate, realized, keys)

    def _decode_and_collate(self, realized: dict, keys: list[str]):
        items = [self.dataset.apply_transforms(realized[k]) for k in keys]
        batch = self._collate_fn(items) if self._auto_collation else self._collate_fn(items[0])
        if getattr(self, "_pin_memory", False):
            from torch.utils.data._utils.pin_memory import pin_memory

            device = getattr(self, "_pin_memory_device", None)
            batch = pin_memory(batch, device) if device else pin_memory(batch)
        return batch

    # -- scheduling ---------------------------------------------------------
    def _schedule_next(self) -> None:
        try:
            indices = self._next_index()  # shuffle / drop_last already applied
        except StopIteration:
            return
        if not self._auto_collation:
            indices = [indices]
        keys = [self.dataset.keys[i] for i in indices]
        sub = self.dataset.manifest.sub_manifest(keys)
        gids = list(sub)
        self._inflight.update(gids)
        future = laila.command.submit([functools.partial(self._prepare_batch, sub, keys)])
        self._pending.append((sub, gids, future))

    def _forget(self, gids: list[str]) -> None:
        """Forget *gids* no pending batch still references (alpha, optionally hdd)."""
        self._inflight.subtract(gids)
        free = [g for g in dict.fromkeys(gids) if self._inflight[g] <= 0]
        for g in free:
            self._inflight.pop(g, None)
        if not free:
            return
        futures = []
        if should_evict(self.loader.evict):
            futures.append(laila.forget(free))
        if self.loader.forget_from_hdd:
            futures.append(laila.forget(free, pool=self.loader.hdd_pool))
        for f in futures:
            if f is None:
                continue
            try:
                f.wait()
            finally:
                f.release()

    # -- _BaseDataLoaderIter protocol ----------------------------------------
    def _next_data(self):
        if not self._pending:
            self.close()
            raise StopIteration
        _sub, gids, future = self._pending.popleft()

        start = time.perf_counter()
        try:
            future.wait()
            batch = future.data
        except BaseException:
            future.release()
            self.close()
            raise
        finally:
            self.loader.last_wait_s = time.perf_counter() - start
        future.release()

        self._schedule_next()  # keep `lookahead` batches in flight
        self._forget(gids)  # batch is done: free memory (and optionally disk)
        return batch

    # -- teardown -----------------------------------------------------------
    def close(self) -> None:
        """Drain and release every in-flight batch; forget their entries from alpha.

        Called on ``StopIteration`` and from ``__del__`` so a ``break``
        in the training loop leaves neither futures in the future bank
        nor prefetched entries in RAM.
        """
        if self._closed:
            return
        self._closed = True
        while self._pending:
            _sub, gids, future = self._pending.popleft()
            try:
                future.wait()
            except BaseException:
                pass
            finally:
                future.release()
            try:
                self._forget(gids)
            except Exception:
                pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
