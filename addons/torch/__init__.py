"""``laila.addons.torch`` -- PyTorch over LAILA manifests.

    from laila.addons.torch import Dataset, DataLoader, Module, Optimizer, default_tiers

Data side: ``Dataset`` wraps a manifest whose top-level keys are
datapoints and whose inner keys are modalities; ``DataLoader`` is a
stock torch ``DataLoader`` with lookahead prefetching on LAILA's
taskforce instead of worker processes. ``default_tiers`` wires the
``RAM << HDF5 << cloud`` read-through chain the loader streams through.

Model side: ``Module`` / ``Optimizer`` wrap a ``torch.nn.Module`` /
``torch.optim.Optimizer`` with the same call API and make
``laila.memorize(obj)`` / ``laila.remember(obj)`` / ``laila.forget(obj)``
work on them: each call turns the object into a ``ModuleManifest`` /
``OptimizerManifest`` (one CPU, detached entry per tensor) and runs that
manifest's operation. No optimizer priming is needed on restore.

Importing this package also registers ``torch.nn.Module`` and
``torch.optim.Optimizer`` with ``laila.ComplexManifest``, so
``everything["model"] = bare_nn_module`` works and comes back bare from
``everything.rebuild()``.
"""

from __future__ import annotations

try:
    import torch as _torch
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError("laila.addons.torch needs PyTorch: pip install 'laila-core[torch]'") from exc

from ..cache import default_tiers as default_tiers
from . import transforms as transforms
from .dataloader import DataLoader as DataLoader
from .dataset import Dataset as Dataset
from .manifests import ModuleManifest as ModuleManifest
from .manifests import OptimizerManifest as OptimizerManifest
from .module import Module as Module
from .optimizer import Optimizer as Optimizer

Dataloader = DataLoader  # alias


def _register_adapters() -> None:
    from laila.policy.central.memory.schema.manifest import ComplexManifest

    def _nick(key: str, container: str | None) -> str:
        return f"{container}/{key}" if container else key

    def _module_adapter(value, key, container):
        return Module(value, nickname=_nick(key, container), bare=True)

    def _optimizer_adapter(value, key, container):
        return Optimizer(value, nickname=_nick(key, container), bare=True)

    # Registered after the wrappers so a wrapper (already ManifestBacked)
    # never goes through an adapter.
    ComplexManifest.register_adapter(_torch.nn.Module, _module_adapter)
    ComplexManifest.register_adapter(_torch.optim.Optimizer, _optimizer_adapter)


_register_adapters()

__all__ = [
    "DataLoader",
    "Dataloader",
    "Dataset",
    "Module",
    "ModuleManifest",
    "Optimizer",
    "OptimizerManifest",
    "default_tiers",
    "transforms",
]
