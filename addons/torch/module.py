"""``Module``: a ``torch.nn.Module`` wrapper that checkpoints through a Manifest.

    model = Module(SimpleCNN(), nickname="cnn")      # same call API as the wrapped model
    laila.memorize(model, dst_pool="cloud")          # state_dict -> one entry per tensor
    laila.remember(model, dst_pool="cloud").wait()   # weights loaded back in place

The wrapper registers the user's model as ``self.module`` (the same
layout as ``DistributedDataParallel``), so ``parameters()``, ``to()``,
``train()`` / ``eval()``, hooks and ``forward`` all go straight to the
wrapped model. ``state_dict()`` / ``load_state_dict()`` are those of the
*inner* model (no ``module.`` prefix), so a checkpoint written through
the wrapper loads into the bare model and vice versa. Attributes and
methods the wrapper does not define fall through to the inner model.

``laila.memorize`` / ``laila.remember`` / ``laila.forget`` accept the
wrapper directly (it is :class:`~laila.ManifestBacked`); each call turns
the model into a :class:`~laila.addons.torch.manifests.ModuleManifest`
and runs that manifest's operation. Tensors are detached and copied to
the CPU before they leave the process (see
:func:`laila.addons.torch.manifests.offload`).
"""

from __future__ import annotations

from typing import Any

import torch

from laila.policy.central.memory.schema.manifest import ManifestBacked

from .manifests import ModuleManifest


class Module(torch.nn.Module, ManifestBacked):
    """Drop-in ``torch.nn.Module`` that knows how to memorize / remember itself.

    Parameters
    ----------
    module : torch.nn.Module
        The model to wrap. Wrapping a ``Module`` unwraps it first.
    nickname : str, optional
        Name the checkpoint is stored under
        (``"MANIFEST:TORCH_MODULE:<nickname>"``). Defaults to the model's
        class name; use something unique per model you keep.
    pool : pool | str, optional
        Default pool for ``memorize`` / ``remember`` / ``forget`` when
        the call does not name one (``None`` = alpha pool / proxy chain).
    strict : bool, default True
        ``strict`` argument of ``load_state_dict`` used by ``remember``.
    bare : bool, default False
        ``True`` when the wrapper only stands in for a bare
        ``nn.Module`` (``everything["model"] = nn_module``): a rebuild of
        the checkpoint then hands back the bare module, not a wrapper.
    """

    def __init__(
        self,
        module: torch.nn.Module,
        *,
        nickname: str | None = None,
        pool: Any = None,
        strict: bool = True,
        bare: bool = False,
    ) -> None:
        if isinstance(module, Module):
            module = module.module
        if not isinstance(module, torch.nn.Module):
            raise TypeError(
                "laila.addons.torch.Module wraps a constructed torch.nn.Module instance, "
                f"got {module!r}"
            )
        super().__init__()
        self.module = module
        self.training = module.training  # mirror the wrapped model's mode
        self.nickname = nickname or type(module).__name__
        self.pool = pool
        self.strict = strict
        self.bare = bare

    # ------------------------------------------------------------------
    # torch.nn.Module surface
    # ------------------------------------------------------------------
    def forward(self, *args: Any, **kwargs: Any) -> Any:
        return self.module(*args, **kwargs)

    def state_dict(self, *args: Any, **kwargs: Any):
        """The wrapped model's ``state_dict`` (keys without a ``module.`` prefix)."""
        return self.module.state_dict(*args, **kwargs)

    def load_state_dict(self, state_dict, *args: Any, **kwargs: Any):
        """Load into the wrapped model (keys without a ``module.`` prefix)."""
        return self.module.load_state_dict(state_dict, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getattr__(name)
        except AttributeError:
            inner = self.__dict__.get("_modules", {}).get("module")
            if inner is None:
                raise
            return getattr(inner, name)

    def extra_repr(self) -> str:
        return f"nickname={self.nickname!r}"

    # ------------------------------------------------------------------
    # ManifestBacked
    # ------------------------------------------------------------------
    def _bound(self) -> ModuleManifest:
        return ModuleManifest.bound(self, self.nickname, pool=self.pool, strict=self.strict)

    def snapshot(self, *, context: dict | None = None) -> ModuleManifest:
        """A fresh manifest with a CPU copy of the current ``state_dict`` as pending entries."""
        return ModuleManifest.snapshot(
            self,
            self.nickname,
            pool=self.pool,
            strict=self.strict,
            context=context,
            wrapped=not self.bare,
        )

    @property
    def manifest(self) -> ModuleManifest:
        """A fresh manifest with a CPU copy of the current ``state_dict`` as pending entries."""
        return self.snapshot()

    @property
    def global_id(self) -> str:
        """``global_id`` of the checkpoint manifest (``LAILA:MANIFEST:TORCH_MODULE:<uuid5(nickname)>``)."""
        return self._bound().global_id

    def memorize(
        self, *, pool: Any = None, batch_size: int = 128, context: dict | None = None, **kwargs: Any
    ):
        """Snapshot and store the model; returns the ``GroupFuture`` of every write."""
        return self.snapshot(context=context).memorize(pool=pool, batch_size=batch_size, **kwargs)

    def remember(self, *, pool: Any = None, persist: bool = True, **kwargs: Any):
        """Load the stored checkpoint into the model in place; ``Future`` whose ``.data`` is this wrapper."""
        return self._bound().remember(pool=pool, persist=persist, **kwargs)

    def forget(self, *, pool: Any = None, **kwargs: Any):
        """Delete the stored checkpoint (manifest and tensors) from *pool*."""
        return self._bound().forget(pool=pool, **kwargs)


__all__ = ["Module"]
