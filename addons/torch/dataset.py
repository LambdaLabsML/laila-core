"""``Dataset``: a map-style ``torch.utils.data.Dataset`` over a LAILA Manifest.

The manifest's top-level keys are the datapoints and each datapoint is a
dict of modalities::

    manifest["qa_0001"] == {"image": <gid>, "question": <gid>, "answer": <gid>}

``dataset[i]`` realizes datapoint ``i`` and returns the same dict with
every ``global_id`` replaced by the (transformed) payload. Every fetch
goes through a fresh ``manifest.sub_manifest([...])`` so the read is
routed at the alpha pool and walks whatever proxy chain is wired there
(see :func:`laila.addons.cache.default_tiers`); the parent manifest's
own ``realized`` would try the pool it was remembered from first and
skip the cache.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import torch.utils.data

import laila
from laila.policy.central.memory.schema.manifest import Manifest

from .transforms import identity

Transform = Callable[[Any], Any]


class Dataset(torch.utils.data.Dataset):
    """Map-style view of a :class:`~laila.Manifest`: position -> datapoint dict.

    Parameters
    ----------
    manifest : Manifest | str
        The dataset manifest, or something ``laila.remember`` resolves
        to one: a ``"MANIFEST:<nickname>"`` shorthand or a full
        ``global_id``. String refs are remembered from *pool* with
        ``persist=False``.
    transforms : {modality: callable} | callable, optional
        Applied to the realized payload of the named modality. Missing
        modalities pass through untouched. A modality whose blueprint
        leaf is a list of ids is transformed element-wise. A bare
        callable is applied to every leaf, which is how *flat*
        manifests (``{"image_0000": gid, ...}``, no modality dict) get
        their single payload transformed.
    pool : pool | str, optional
        Where to remember a string *manifest* ref from (nickname, gid
        or live pool). ``None`` means the alpha pool / proxy chain.
    evict : bool | "auto", default "auto"
        Forget a datapoint's entries from the alpha (RAM) pool once
        ``__getitem__`` has decoded them. ``"auto"`` evicts only when
        the alpha pool fronts a proxy chain (so the entries can be
        re-read from disk / cloud); ``True`` always; ``False`` never.
    """

    def __init__(
        self,
        manifest: Manifest | str,
        *,
        transforms: Mapping[str, Transform] | Transform | None = None,
        pool: Any | None = None,
        evict: bool | str = "auto",
    ) -> None:
        if evict not in (True, False, "auto"):
            raise ValueError("evict must be True, False or 'auto'")
        if isinstance(manifest, str):
            manifest = _remember_manifest(manifest, pool)
        if not isinstance(manifest, Manifest):
            raise TypeError(f"Dataset expects a Manifest or a manifest ref, got {type(manifest)!r}")
        self.manifest = manifest
        self.keys: list[str] = list(manifest.keys())
        if callable(transforms):
            self.transforms: dict[str, Transform] = {}
            self._default_transform: Transform = transforms
        else:
            self.transforms = dict(transforms or {})
            self._default_transform = identity
        self.evict = evict

    # ------------------------------------------------------------------
    # torch.utils.data.Dataset protocol
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self.keys)

    def __getitem__(self, index: int) -> dict[str, Any]:
        if torch.utils.data.get_worker_info() is not None:
            raise RuntimeError(
                "laila.addons.torch.Dataset cannot be read from a DataLoader worker "
                "process (no LAILA policy there). Use laila.addons.torch.DataLoader "
                "with lookahead=..., or num_workers=0."
            )
        sub = self.sub_manifest([index])
        realized = sub.realized
        item = self.apply_transforms(realized[self.keys[index]])
        if should_evict(self.evict):
            future = laila.forget(list(sub))
            if future is not None:
                try:
                    future.wait()
                finally:
                    future.release()
        return item

    # ------------------------------------------------------------------
    # helpers shared with DataLoader
    # ------------------------------------------------------------------
    @property
    def modalities(self) -> list[str]:
        """Inner keys of the first datapoint (``[]`` for an empty manifest)."""
        if not self.keys:
            return []
        first = self.manifest[self.keys[0]]
        return list(first.keys()) if isinstance(first, dict) else []

    def sub_manifest(self, indices: Sequence[int]) -> Manifest:
        """Fresh manifest holding only the datapoints at *indices* (no I/O)."""
        return self.manifest.sub_manifest([self.keys[i] for i in indices])

    def apply_transforms(self, datapoint: Any) -> Any:
        """Replace every realized entry in *datapoint* with its transformed payload."""
        default = self._default_transform
        if isinstance(datapoint, dict):
            out: dict[str, Any] = {}
            for modality, value in datapoint.items():
                fn = self.transforms.get(modality, default)
                if isinstance(value, list):
                    out[modality] = [fn(_payload(v)) for v in value]
                else:
                    out[modality] = fn(_payload(value))
            return out
        if isinstance(datapoint, list):
            return [default(_payload(v)) for v in datapoint]
        return default(_payload(datapoint))


def should_evict(evict: bool | str) -> bool:
    """Resolve an ``evict`` setting: ``"auto"`` means "only if RAM is a cache tier"."""
    if evict == "auto":
        return laila.alpha_pool.proxy_to is not None
    return bool(evict)


def _payload(value: Any) -> Any:
    return value.data if isinstance(value, laila.Entry) else value


def _remember_manifest(ref: str, pool: Any | None) -> Manifest:
    future = laila.remember(ref, dst_pool=pool, persist=False)
    try:
        manifest = future.wait()
    finally:
        future.release()
    if not isinstance(manifest, Manifest):
        raise TypeError(f"{ref!r} did not resolve to a Manifest (got {type(manifest).__name__})")
    return manifest
