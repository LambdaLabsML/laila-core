"""``Optimizer``: a ``torch.optim.Optimizer`` wrapper that checkpoints through a Manifest.

    optimizer = Optimizer(torch.optim.Adam(model.parameters(), lr=1e-3), nickname="adam")
    laila.memorize(optimizer, dst_pool="cloud")          # state -> one entry per tensor
    laila.remember(optimizer, dst_pool="cloud").wait()   # state loaded back, no priming

The wrapper keeps the user's optimizer as ``self.optimizer`` and forwards
the whole ``torch.optim.Optimizer`` API to it: ``step`` / ``zero_grad`` /
``add_param_group`` / ``state_dict`` / ``load_state_dict`` plus the
``param_groups`` / ``state`` / ``defaults`` attributes (the very same
objects, so LR schedulers built on the wrapper drive the real
optimizer). ``isinstance(wrapper, torch.optim.Optimizer)`` holds.

Restoring needs no "priming": ``torch.optim.Optimizer.load_state_dict``
maps the saved per-parameter state onto the live ``param_groups`` by
index and casts every tensor to its parameter's device, without
touching ``.grad`` or requiring a prior ``step``. The only requirement
is that the optimizer was constructed over the same parameters (same
count and order) -- which is checked before anything is loaded.
"""

from __future__ import annotations

from typing import Any

import torch

from laila.policy.central.memory.schema.manifest import ManifestBacked

from .manifests import OptimizerManifest


class Optimizer(torch.optim.Optimizer, ManifestBacked):
    """Drop-in ``torch.optim.Optimizer`` that knows how to memorize / remember itself.

    Parameters
    ----------
    optimizer : torch.optim.Optimizer
        A *constructed* optimizer (``torch.optim.Adam(model.parameters(), ...)``).
        Wrapping an ``Optimizer`` unwraps it first.
    nickname : str, optional
        Name the checkpoint is stored under
        (``"MANIFEST:TORCH_OPTIMIZER:<nickname>"``). Defaults to the
        optimizer's class name; use something unique.
    pool : pool | str, optional
        Default pool for ``memorize`` / ``remember`` / ``forget`` when
        the call does not name one (``None`` = alpha pool / proxy chain).
    model : laila.addons.torch.Module, optional
        The wrapped model whose parameters this optimizer drives. Lets
        the checkpoint reference each parameter by ``(model gid, name)``
        so ``rebuild`` can bind a fresh optimizer to the rebuilt model.
        Not needed inside a ``ComplexManifest``: sibling models are
        found there automatically.
    bare : bool, default False
        ``True`` when the wrapper only stands in for a bare
        ``torch.optim.Optimizer``: a rebuild then hands back the bare
        optimizer, not a wrapper.
    """

    def __init__(
        self,
        optimizer: torch.optim.Optimizer,
        *,
        nickname: str | None = None,
        pool: Any = None,
        model: Any = None,
        bare: bool = False,
    ) -> None:
        if isinstance(optimizer, Optimizer):
            optimizer = optimizer.optimizer
        if not isinstance(optimizer, torch.optim.Optimizer):
            raise TypeError(
                "laila.addons.torch.Optimizer wraps a constructed torch.optim.Optimizer "
                f"instance (e.g. torch.optim.Adam(model.parameters())), got {optimizer!r}"
            )
        if model is not None and not isinstance(model, ManifestBacked):
            raise TypeError(
                "Optimizer(model=...) expects a laila.addons.torch.Module (the wrapped model), "
                f"got {type(model).__name__}"
            )
        # ``torch.optim.Optimizer.__init__`` is deliberately not called: the
        # wrapped instance owns ``param_groups`` / ``state`` / hooks.
        self.__dict__["optimizer"] = optimizer
        self.nickname = nickname or type(optimizer).__name__
        self.pool = pool
        self.model = model
        self.bare = bare

    # ------------------------------------------------------------------
    # torch.optim.Optimizer surface (forwarded)
    # ------------------------------------------------------------------
    @property
    def param_groups(self):
        return self.optimizer.param_groups

    @property
    def state(self):
        return self.optimizer.state

    @property
    def defaults(self):
        return self.optimizer.defaults

    def step(self, *args: Any, **kwargs: Any):
        return self.optimizer.step(*args, **kwargs)

    def zero_grad(self, *args: Any, **kwargs: Any) -> None:
        return self.optimizer.zero_grad(*args, **kwargs)

    def add_param_group(self, param_group: dict) -> None:
        return self.optimizer.add_param_group(param_group)

    def state_dict(self) -> dict:
        return self.optimizer.state_dict()

    def load_state_dict(self, state_dict: dict) -> None:
        return self.optimizer.load_state_dict(state_dict)

    def __getattr__(self, name: str) -> Any:
        inner = self.__dict__.get("optimizer")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)

    def __getstate__(self) -> dict:
        return dict(self.__dict__)

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)

    def __repr__(self) -> str:
        return f"Optimizer(nickname={self.nickname!r}, {self.optimizer!r})"

    # ------------------------------------------------------------------
    # ManifestBacked
    # ------------------------------------------------------------------
    def _bound(self) -> OptimizerManifest:
        return OptimizerManifest.bound(self, self.nickname, pool=self.pool)

    def snapshot(self, *, context: dict | None = None) -> OptimizerManifest:
        """A fresh manifest with a CPU copy of the current ``state_dict`` as pending entries."""
        return OptimizerManifest.snapshot(
            self, self.nickname, pool=self.pool, context=context, wrapped=not self.bare
        )

    @property
    def manifest(self) -> OptimizerManifest:
        """A fresh manifest with a CPU copy of the current ``state_dict`` as pending entries."""
        return self.snapshot()

    @property
    def global_id(self) -> str:
        """``global_id`` of the checkpoint manifest (``LAILA:MANIFEST:TORCH_OPTIMIZER:<uuid5(nickname)>``)."""
        return self._bound().global_id

    def memorize(
        self, *, pool: Any = None, batch_size: int = 128, context: dict | None = None, **kwargs: Any
    ):
        """Snapshot and store the optimizer state; returns the ``GroupFuture`` of every write."""
        return self.snapshot(context=context).memorize(pool=pool, batch_size=batch_size, **kwargs)

    def remember(self, *, pool: Any = None, persist: bool = True, **kwargs: Any):
        """Load the stored state into the optimizer in place; ``Future`` whose ``.data`` is this wrapper.

        Raises ``RuntimeError`` up front when the optimizer has no
        parameters (was never constructed over ``model.parameters()``)
        and ``ValueError`` when the stored state covers a different
        number of parameters than the live optimizer.
        """
        return self._bound().remember(pool=pool, persist=persist, **kwargs)

    def forget(self, *, pool: Any = None, **kwargs: Any):
        """Delete the stored checkpoint (manifest and tensors) from *pool*."""
        return self._bound().forget(pool=pool, **kwargs)


__all__ = ["Optimizer"]
