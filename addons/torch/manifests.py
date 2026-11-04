"""``Manifest`` subclasses that know how to load themselves into -- and rebuild -- a torch object.

A model checkpoint in LAILA is a :class:`~laila.Manifest`: one constant
entry per tensor (detached, copied to the CPU) plus a small ``meta``
entry, indexed by a blueprint that mirrors ``state_dict()``. The two
subclasses here add the type-specific half of the round trip:

- :class:`ModuleManifest` -- scope ``MANIFEST:TORCH_MODULE``, blueprint
  ``{"state_dict": {key: gid}, "architecture": gid, "meta": gid}`` for a
  ``torch.nn.Module``; ``remember`` calls ``load_state_dict`` on a bound
  target, ``rebuild`` reconstructs the model from scratch
  (``architecture`` is a pickled *skeleton*: the module with every
  parameter / buffer replaced by a meta tensor -- a few KB, no weights).
- :class:`OptimizerManifest` -- scope ``MANIFEST:TORCH_OPTIMIZER``,
  blueprint ``{"state": {"<idx>": {name: gid}}, "param_groups": gid,
  "meta": gid}`` for a ``torch.optim.Optimizer``; ``remember`` rebuilds
  the int-keyed ``state_dict`` and calls ``load_state_dict`` (no priming
  step is needed: torch maps the saved state onto the live parameters by
  index and moves every tensor to the parameter's device). ``meta``
  records, per parameter, *which model* (``ModuleManifest`` gid) and
  *which parameter name* it drives, so ``rebuild`` can bind a fresh
  optimizer to a rebuilt model.

The scopes are part of the gid (``LAILA:MANIFEST:TORCH_MODULE:<uuid>``),
so a stored checkpoint hydrates as the right class wherever it is read
(``laila.remember("MANIFEST:TORCH_MODULE:<nickname>")`` returns a
``ModuleManifest``), and a reader that only knows ``MANIFEST`` still
gets a plain manifest. Every leaf has a deterministic nickname
(``"<nickname>/state_dict/<key>"`` ...), so re-memorizing the same object
overwrites the same gids in place instead of orphaning blobs.

``memorize`` / ``forget`` are inherited from :class:`Manifest`;
``remember`` is overridden to resolve to the *bound target* (the torch
object handed to :meth:`snapshot` / :meth:`bound`) once its state has
been loaded. The wrappers in :mod:`laila.addons.torch.module` /
:mod:`laila.addons.torch.optimizer` delegate to these classes, and a
:class:`~laila.ComplexManifest` holding them (``everything["model"]``)
calls ``rebuild`` on each.
"""

from __future__ import annotations

import copy
import importlib
import pickle
from typing import Any, ClassVar

import torch
from pydantic import PrivateAttr

import laila
from laila.macros.strings import _MANIFEST_SCOPE
from laila.policy.central.memory.schema.manifest import Manifest, ManifestBacked

STATE_DICT_KEY = "state_dict"
ARCHITECTURE_KEY = "architecture"
STATE_KEY = "state"
PARAM_GROUPS_KEY = "param_groups"
META_KEY = "meta"

TORCH_MODULE_SCOPE = "TORCH_MODULE"
TORCH_OPTIMIZER_SCOPE = "TORCH_OPTIMIZER"


def _unwrap(target: Any) -> Any:
    """The bare torch object behind a ``laila.addons.torch`` wrapper (or *target* itself)."""
    if isinstance(target, ManifestBacked):
        for attr in ("module", "optimizer"):
            inner = getattr(target, attr, None)
            if inner is not None and inner is not target:
                return inner
    return target


def offload(value: Any) -> Any:
    """Return a copy of *value* that is safe to hand to an asynchronous upload.

    Tensors (and ``nn.Parameter``\\ s) are detached from the autograd
    graph and copied to the CPU -- ``copy=True`` even when they already
    live there, so the snapshot never aliases a parameter the training
    loop keeps mutating and never pins GPU memory. Containers are walked
    recursively; everything else is deep-copied.
    """
    if isinstance(value, torch.Tensor):
        return value.detach().to("cpu", copy=True)
    if isinstance(value, dict):
        return {k: offload(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(offload(v) for v in value)
    return copy.deepcopy(value)


# ----------------------------------------------------------------------
# architecture skeleton
# ----------------------------------------------------------------------
def _non_persistent_buffer_ids(module: torch.nn.Module) -> set[int]:
    ids: set[int] = set()
    for mod in module.modules():
        for name in getattr(mod, "_non_persistent_buffers_set", ()):
            buf = mod._buffers.get(name)
            if buf is not None:
                ids.add(id(buf))
    return ids


def skeleton(module: torch.nn.Module) -> torch.nn.Module:
    """A deep copy of *module* whose parameters and persistent buffers are meta tensors.

    Shape, dtype, strides, ``requires_grad`` and parameter sharing (tied
    weights) are preserved; no element of a parameter or persistent
    buffer is stored -- ``load_state_dict`` brings those back.
    *Non-persistent* buffers are not part of a ``state_dict`` (RoPE
    ``inv_freq`` tables, causal masks, ...), so they travel inside the
    skeleton as detached CPU copies; they are small by convention. The
    live module is not touched. Pickling the result is how
    :class:`ModuleManifest` records the architecture (the model class
    must be importable at the destination, exactly like
    ``torch.save(model)``).
    """
    memo: dict[int, Any] = {}
    for p in module.parameters():
        memo[id(p)] = torch.nn.Parameter(
            torch.empty_like(p, device="meta"), requires_grad=p.requires_grad
        )
    keep = _non_persistent_buffer_ids(module)
    for b in module.buffers():
        if id(b) in keep:
            memo[id(b)] = b.detach().to("cpu", copy=True)
        else:
            memo[id(b)] = torch.empty_like(b, device="meta")
    return copy.deepcopy(module, memo)


def _shared_paths(pairs) -> list[list[str]]:
    by_id: dict[int, list[str]] = {}
    for path, tensor in pairs:
        by_id.setdefault(id(tensor), []).append(path)
    return [paths for paths in by_id.values() if len(paths) > 1]


def _owner(module: torch.nn.Module, path: str) -> tuple[torch.nn.Module, str]:
    mod_path, _, name = path.rpartition(".")
    return (module.get_submodule(mod_path) if mod_path else module), name


def materialize(module: torch.nn.Module, device: Any = None) -> torch.nn.Module:
    """Give a :func:`skeleton` real (uninitialised) storage on *device*, in place.

    ``to_empty`` allocates every meta tensor, ties that ``to_empty``
    breaks are restored, persistent buffers are zeroed (``load_state_dict``
    overwrites them right after) and the non-persistent buffers the
    skeleton carried as real tensors are moved to *device* with their
    values intact.
    """
    device = device if device is not None else "cpu"
    ties = _shared_paths(module.named_parameters(remove_duplicate=False)) + _shared_paths(
        module.named_buffers(remove_duplicate=False)
    )
    carried = [
        (path, buf) for path, buf in module.named_buffers(remove_duplicate=False) if not buf.is_meta
    ]
    module.to_empty(device=device)
    for paths in ties:
        owner, name = _owner(module, paths[0])
        shared = getattr(owner, name)
        for path in paths[1:]:
            owner, name = _owner(module, path)
            if name in owner._parameters:
                owner._parameters[name] = shared
            else:
                owner._buffers[name] = shared
    with torch.no_grad():
        for buf in module.buffers():
            buf.zero_()
    moved: dict[int, torch.Tensor] = {}
    for path, buf in carried:
        owner, name = _owner(module, path)
        owner._buffers[name] = moved.setdefault(id(buf), buf.to(device))
    return module


_WEIGHT_NORM_COMPAT_HOOK = "weight_norm.<locals>._weight_norm_compat_hook"


def _is_weight_norm_compat_hook(hook: Any) -> bool:
    """``torch.nn.utils.parametrizations.weight_norm`` registers a
    ``load_state_dict`` pre-hook (a closure, so not picklable) that
    translates legacy ``<name>_g`` / ``<name>_v`` keys; it is identified by
    its qualified name so :func:`_strip_parametrizations` can drop it with
    the parametrization and :func:`_restore_parametrizations` put it back."""
    fn = getattr(hook, "hook", hook)  # ``_WrappedHook`` wraps the callable
    return getattr(fn, "__qualname__", None) == _WEIGHT_NORM_COMPAT_HOOK


def _strip_parametrizations(module: torch.nn.Module) -> list[tuple[str, str, list, bool]]:
    """Detach every ``torch.nn.utils.parametrize`` parametrization from *module*, in place.

    torch refuses to pickle a parametrized module (``torch.save(model)``
    has the same restriction), so the skeleton is pickled unparametrized
    and the parametrization modules (``_WeightNorm``, ``_Orthogonal``,
    ``_SpectralNorm``, user classes) ride along as
    ``[(owner path, tensor name, [parametrization, ...], unsafe)]``;
    :func:`_restore_parametrizations` re-registers them after
    :func:`materialize`, before ``load_state_dict`` fills the
    ``parametrizations.<name>.original*`` leaves.

    ``weight_norm``'s legacy-key ``load_state_dict`` pre-hook is a closure
    the standard pickler cannot serialise; it is removed here and
    re-created by :func:`_restore_parametrizations`.
    """
    from torch.nn.utils import parametrize

    stripped: list[tuple[str, str, list, bool]] = []
    for path, mod in list(module.named_modules()):
        if not parametrize.is_parametrized(mod):
            continue
        for name in list(mod.parametrizations.keys()):
            plist = mod.parametrizations[name]
            stripped.append((path, name, list(plist), bool(getattr(plist, "unsafe", False))))
            parametrize.remove_parametrizations(mod, name, leave_parametrized=True)
        hooks = mod._load_state_dict_pre_hooks
        for key in [k for k, h in hooks.items() if _is_weight_norm_compat_hook(h)]:
            del hooks[key]
    return stripped


def _restore_parametrizations(module: torch.nn.Module, stripped: list) -> None:
    from torch.nn.utils import parametrizations, parametrize

    for path, name, plist, unsafe in stripped:
        owner = module.get_submodule(path) if path else module
        for p in plist:
            if type(p) is parametrizations._WeightNorm:
                # Through torch's own entry point so the legacy-key
                # ``load_state_dict`` pre-hook comes back with it.
                parametrizations.weight_norm(owner, name, dim=p.dim)
            else:
                parametrize.register_parametrization(owner, name, p, unsafe=unsafe)


def _dumps_architecture(module: torch.nn.Module) -> tuple[bytes, str]:
    """``(blob, pickler)`` for the skeleton of *module*.

    The blob is ``{"module": skeleton, "parametrizations": [...]}``. The
    standard pickler stores classes by reference (importable at the
    destination, like ``torch.save(model)``). When a class in the module
    tree lives in ``__main__`` (a script, a notebook cell) -- which
    ``pickle`` accepts by reference but another process cannot resolve
    -- or when ``pickle`` fails outright, and ``cloudpickle`` is
    installed, the classes are pickled by value instead.
    """
    sk = skeleton(module)
    payload = {"module": sk, "parametrizations": _strip_parametrizations(sk)}
    by_value = any(type(m).__module__ == "__main__" for m in sk.modules())
    if by_value:
        try:
            import cloudpickle

            return cloudpickle.dumps(payload), "cloudpickle"
        except ImportError:
            pass  # pickle by reference; rebuild only works in this process
    try:
        return pickle.dumps(payload), "pickle"
    except Exception as exc:
        try:
            import cloudpickle
        except ImportError:
            raise RuntimeError(
                f"ModuleManifest: the architecture of {type(module).__name__} cannot be pickled "
                f"({exc!r}); make the model class importable (or pip install cloudpickle) and keep "
                "lambdas / open handles out of the module's attributes"
            ) from exc
        return cloudpickle.dumps(payload), "cloudpickle"


def _loads_architecture(blob: Any, pickler: str) -> tuple[torch.nn.Module, list]:
    """``(skeleton, stripped parametrizations)`` from a :func:`_dumps_architecture` blob."""
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    blob = bytes(blob)
    if pickler == "cloudpickle":
        import cloudpickle

        payload = cloudpickle.loads(blob)
    else:
        payload = pickle.loads(blob)
    if isinstance(payload, torch.nn.Module):  # blobs written before parametrization support
        return payload, []
    return payload["module"], payload.get("parametrizations", [])


def _import_class(module_name: str, qualname: str) -> type:
    try:
        obj: Any = importlib.import_module(module_name)
        for part in qualname.split("."):
            obj = getattr(obj, part)
        return obj
    except Exception as exc:
        raise RuntimeError(
            f"cannot import {module_name}.{qualname}: the class must be importable where the "
            "checkpoint is rebuilt (same as torch.save(model) / torch.load)"
        ) from exc


class _TorchManifest(Manifest):
    """Shared machinery: a manifest bound to a torch *target* and a nickname.

    Parameters (keyword-only, on top of :class:`Manifest`'s)
    ------------------------------------------------------
    target : object, optional
        The torch object ``remember`` loads into. ``None`` makes the
        manifest behave like a plain :class:`Manifest`.
    pool : pool | str, optional
        Default pool for ``memorize`` / ``remember`` / ``forget`` when the
        call does not name one.
    """

    KIND: ClassVar[str] = "torch"
    _OWNS_CHILDREN: ClassVar[bool] = True

    _target: Any = PrivateAttr(default=None)
    _pool: Any = PrivateAttr(default=None)
    _context: Any = PrivateAttr(default=None)
    _wrapped: bool = PrivateAttr(default=True)

    def __init__(self, *, target: Any = None, pool: Any = None, **data: Any) -> None:
        super().__init__(**data)
        self._target = target
        self._pool = pool

    # ------------------------------------------------------------------
    # construction
    # ------------------------------------------------------------------
    @classmethod
    def bound(cls, target: Any, nickname: str, *, pool: Any = None, **kwargs: Any):
        """An empty manifest that only knows its identity and its target (no I/O, no copy)."""
        if not isinstance(nickname, str) or not nickname:
            raise ValueError("a non-empty nickname is required to address the checkpoint")
        return cls(nickname=nickname, target=target, pool=pool, **kwargs)

    @classmethod
    def snapshot(
        cls,
        target: Any,
        nickname: str,
        *,
        pool: Any = None,
        context: dict | None = None,
        wrapped: bool = True,
        **kwargs: Any,
    ):
        """A manifest whose pending entries are a CPU copy of *target*'s current state.

        Parameters
        ----------
        context : dict, optional
            Sibling members of the complex manifest this snapshot is
            taken for (``{key: Module / Optimizer / Manifest}``);
            :class:`OptimizerManifest` resolves its parameters against
            the models in there.
        wrapped : bool, default True
            Whether ``rebuild`` should hand back the wrapper
            (``laila.addons.torch.Module`` / ``Optimizer``) or the bare
            torch object it wraps.
        """
        probe = cls.bound(target, nickname, pool=pool, **kwargs)
        probe._check_target(target)
        probe._context = context
        entries, meta = probe._snapshot_entries(target)
        meta.update(
            {
                "nickname": nickname,
                "wrapped": bool(wrapped),
                "torch": torch.__version__,
                "laila": laila.__version__,
            }
        )
        entries[META_KEY] = laila.constant(data=meta, nickname=f"{nickname}/{META_KEY}")
        out = cls(data=entries, nickname=nickname, target=target, pool=pool, **kwargs)
        out._wrapped = bool(wrapped)
        return out

    # ------------------------------------------------------------------
    # subclass hooks
    # ------------------------------------------------------------------
    def _check_target(self, target: Any) -> None:
        """Raise unless *target* is something this manifest can load into."""
        raise NotImplementedError

    def _snapshot_entries(self, target: Any) -> tuple[dict, dict]:
        """Return ``(blueprint of Entry objects, meta dict)`` for *target*."""
        raise NotImplementedError

    def _load(self, realized: dict, target: Any) -> None:
        """Load the realized payloads into *target* (runs off the loop thread)."""
        raise NotImplementedError

    def _rebuild_target(self, realized: dict, meta: dict, device: Any, context: dict) -> Any:
        """Construct a fresh (unloaded) torch object from *realized* / *meta*."""
        raise NotImplementedError

    def _wrap(self, target: Any, nickname: str) -> Any:
        """The ``laila.addons.torch`` wrapper around a bare *target*."""
        raise NotImplementedError

    def _leaf(self, path: str, value: Any):
        return laila.constant(data=offload(value), nickname=f"{self._nickname}/{path}")

    @staticmethod
    def _payloads(tree: Any) -> Any:
        """Replace every realized ``Entry`` in *tree* with its payload."""
        if isinstance(tree, laila.Entry):
            return tree.data
        if isinstance(tree, dict):
            return {k: _TorchManifest._payloads(v) for k, v in tree.items()}
        if isinstance(tree, list):
            return [_TorchManifest._payloads(v) for v in tree]
        return tree

    # ------------------------------------------------------------------
    # public surface
    # ------------------------------------------------------------------
    @property
    def target(self) -> Any:
        """The torch object this manifest loads into (``None`` when unbound)."""
        return self._target

    @property
    def handle(self) -> Any | None:
        """A wrapper that memorizes / forgets on behalf of the (bare) rebuilt target."""
        if self._target is None or self._nickname is None:
            return None
        if isinstance(self._target, ManifestBacked):
            return self._target
        return self._wrap(self._target, self._nickname)

    def _routed(self, pool, pool_nickname, pool_id):
        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        if routed is None:
            routed = self._pool
        if routed is None:
            # Read back from a pool: that pool is where the leaves are.
            routed = getattr(self, "_source_pool_gid", None)
        return routed

    def memorize(
        self,
        *,
        pool=None,
        pool_nickname=None,
        pool_id=None,
        batch_size: int = 128,
        context: dict | None = None,
    ):
        if self._target is not None:
            self._check_target(self._target)
        return super().memorize(
            pool=self._routed(pool, pool_nickname, pool_id), batch_size=batch_size
        )

    def remember(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
        persist: bool = True,
    ):
        """Load the stored checkpoint into the bound target.

        Returns a :class:`Future` (runs on LAILA's taskforce) whose
        ``.data`` is the target once ``load_state_dict`` has run. The
        stored blueprint always wins over any local snapshot, so calling
        this on a manifest built with :meth:`snapshot` reloads what is in
        the pool. Without a bound target this is plain
        :meth:`Manifest.remember` (fetch the leaves, resolve to them).

        Raises
        ------
        RuntimeError
            Before any I/O, when the target is not something that can be
            loaded into (see the subclass ``_check_target``).
        """
        target = self._target
        if target is None:
            if self.data is None:
                raise RuntimeError(
                    f"{type(self).__name__}.remember needs a live torch object to load into: "
                    "build it with .bound(target, nickname) / .snapshot(target, nickname), "
                    "or wrap the object with laila.addons.torch.Module / Optimizer"
                )
            return super().remember(
                pool=self._routed(pool, pool_nickname, pool_id),
                batch_size=batch_size,
                persist=persist,
            )
        self._check_target(target)
        routed = self._routed(pool, pool_nickname, pool_id)

        async def _task():
            await self._load_into_async(target, routed, batch_size, persist, fetch_stored=True)
            return target

        return laila.command.submit([_task])

    async def _realize_async(self, routed, batch_size: int, persist: bool) -> dict:
        """Fetch every leaf and return the blueprint with entries in place of gids.

        *routed* is tried first; when the leaves are not there (a
        manifest cached into the alpha pool whose tensors stayed in the
        source pool) the manifest's candidate pools are tried in turn
        (source pool, alpha pool, every other registered pool).
        """
        memory = laila.get_active_policy().central.memory
        gids = list(self)
        candidates: list = [routed]
        for candidate in self._candidate_pools(memory):
            if candidate not in candidates:
                candidates.append(candidate)
        last_exc: Exception | None = None
        for candidate in candidates:
            try:
                entries = await memory._read_entries_async(gids, pool=candidate, persist=persist)
                break
            except KeyError as exc:
                last_exc = exc
        else:
            raise KeyError(
                f"{type(self).__name__}: the entries of checkpoint "
                f"{self._nickname or self.global_id!r} were not found in any pool"
            ) from last_exc
        if len(entries) != len(gids):
            raise KeyError(
                f"{type(self).__name__}.remember: expected {len(gids)} entries, got {len(entries)}"
            )
        return Manifest._rebuild_with_entries(self.data, dict(zip(gids, entries)))

    async def _load_into_async(
        self, target: Any, routed, batch_size: int, persist: bool, *, fetch_stored: bool
    ) -> None:
        import asyncio

        if fetch_stored:
            stored = await self._fetch_stored(routed, persist)
            self.data = copy.deepcopy(stored.data)
            self._source_pool_gid = getattr(stored, "_source_pool_gid", None)
        realized = await self._realize_async(routed, batch_size, persist)
        loop = asyncio.get_running_loop()
        # ``load_state_dict`` is a CPU (or H2D) copy: keep it off the loop thread.
        await loop.run_in_executor(None, self._load, realized, target)

    async def _fetch_stored(self, routed, persist: bool):
        """Await the manifest entry stored under ``self.global_id``."""
        ref = laila.remember(self.global_id, dst_pool=routed, persist=persist)
        try:
            await ref
            stored = ref.result
        finally:
            ref.release()
        if not isinstance(stored, Manifest) or stored.data is None:
            raise KeyError(
                f"{self.global_id} did not resolve to a stored manifest "
                f"(nickname {self._nickname!r})"
            )
        return stored

    # ------------------------------------------------------------------
    # rebuild
    # ------------------------------------------------------------------
    def rebuild(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        device: Any = None,
        context: dict | None = None,
        persist: bool = False,
        batch_size: int = 128,
    ) -> Any:
        """Reconstruct the torch object this checkpoint was taken from.

        Synchronous. Fetches the leaves (from *pool*, else the manifest's
        default pool, else the pool it was read from), builds a fresh
        object from the stored architecture / hyper-parameters, loads the
        state into it and returns it -- as the ``laila.addons.torch``
        wrapper when the snapshot was taken through one, as the bare
        torch object otherwise. The result becomes this manifest's
        :attr:`target`.

        Parameters
        ----------
        device : optional
            Where the rebuilt object lives (``ModuleManifest``:
            ``torch.device`` / str; ``None`` = CPU). An optimizer follows
            its parameters.
        context : dict, optional
            ``{gid: live object}`` of already rebuilt siblings (a
            :class:`~laila.ComplexManifest` passes it so the optimizer
            finds its model). Standalone, a missing model is rebuilt from
            its own ``ModuleManifest`` gid.
        """
        if self.data is None:
            raise RuntimeError(
                f"{type(self).__name__}.rebuild: no blueprint; remember the checkpoint first "
                "(laila.remember('MANIFEST:...:<nickname>'))"
            )
        routed = self._routed(pool, pool_nickname, pool_id)
        context = {} if context is None else context

        async def _task():
            import asyncio

            realized = await self._realize_async(routed, batch_size, persist)
            self._meta_cache = dict(self._payloads(realized[META_KEY]))
            meta = self._meta_cache
            loop = asyncio.get_running_loop()

            def _build():
                target = self._rebuild_target(realized, meta, device, context)
                self._load(realized, target)
                return target

            return await loop.run_in_executor(None, _build)

        ref = laila.command.submit([_task])
        try:
            ref.wait()
            bare = ref.result.data if ref.result is not None else None
        finally:
            ref.release()
        if bare is None:
            raise RuntimeError(f"{type(self).__name__}.rebuild produced nothing")

        meta = self._meta_cache or {}
        nickname = self._nickname or meta.get("nickname")
        self._nickname = nickname
        self._wrapped = bool(meta.get("wrapped", True))
        live = self._wrap(bare, nickname) if self._wrapped else bare
        self._target = live
        return live

    _meta_cache: dict | None = PrivateAttr(default=None)

    @property
    def meta(self) -> dict | None:
        """The ``meta`` payload once fetched (``None`` before ``rebuild``)."""
        return None if self._meta_cache is None else dict(self._meta_cache)

    def forget(self, *, pool=None, pool_nickname=None, pool_id=None, batch_size: int = 128):
        """Delete the checkpoint (manifest + every leaf) from the pool.

        A manifest that only knows its identity (:meth:`bound`) first
        fetches the stored blueprint so the leaves can be enumerated.
        """
        routed = self._routed(pool, pool_nickname, pool_id)
        if self.data is None:
            ref = laila.remember(self.global_id, dst_pool=routed, persist=False)
            try:
                ref.wait()
                stored = ref.result
            finally:
                ref.release()
            if not isinstance(stored, Manifest) or stored.data is None:
                raise KeyError(f"{self.global_id} did not resolve to a stored manifest")
            self.data = copy.deepcopy(stored.data)
        return Manifest.forget(self, pool=routed, batch_size=batch_size)

    def __repr__(self) -> str:
        n = sum(1 for _ in self)
        return f"{type(self).__name__}({self.global_id}, nickname={self._nickname!r}, entries={n})"


class ModuleManifest(_TorchManifest):
    """Checkpoint manifest of a ``torch.nn.Module`` (scope ``MANIFEST:TORCH_MODULE``).

    Blueprint: ``{"state_dict": {<key>: gid, ...}, "architecture": gid, "meta": gid}``.
    ``remember`` calls ``target.load_state_dict(..., strict=strict)``; the
    in-place copy keeps the model's device and dtype. ``rebuild``
    unpickles the architecture skeleton, materializes it on *device* and
    loads the weights.
    """

    KIND: ClassVar[str] = "module"
    _DEFAULT_SCOPES: ClassVar[list[str]] = [_MANIFEST_SCOPE, TORCH_MODULE_SCOPE]
    _strict: bool = PrivateAttr(default=True)

    def __init__(self, *, strict: bool = True, **data: Any) -> None:
        super().__init__(**data)
        self._strict = strict

    def _check_target(self, target: Any) -> None:
        if not isinstance(target, torch.nn.Module):
            raise RuntimeError(
                f"ModuleManifest needs a torch.nn.Module to load into, got {type(target).__name__}"
            )

    def _snapshot_entries(self, target: torch.nn.Module) -> tuple[dict, dict]:
        state_dict = target.state_dict()
        entries = {
            key: self._leaf(f"{STATE_DICT_KEY}/{key}", value) for key, value in state_dict.items()
        }
        inner = _unwrap(target)
        blob, pickler = _dumps_architecture(inner)
        architecture = laila.constant(data=blob, nickname=f"{self._nickname}/{ARCHITECTURE_KEY}")
        params = dict(inner.named_parameters())
        meta = {
            "kind": self.KIND,
            "class": type(inner).__qualname__,
            "module": type(inner).__module__,
            "pickler": pickler,
            "keys": list(state_dict.keys()),
            "device": str(next(iter(params.values())).device) if params else "cpu",
            "training": bool(inner.training),
        }
        return {STATE_DICT_KEY: entries, ARCHITECTURE_KEY: architecture}, meta

    def _load(self, realized: dict, target: torch.nn.Module) -> None:
        state_dict = self._payloads(realized.get(STATE_DICT_KEY, {}))
        target.load_state_dict(state_dict, strict=self._strict)

    def _rebuild_target(self, realized: dict, meta: dict, device: Any, context: dict) -> Any:
        if ARCHITECTURE_KEY not in realized:
            raise RuntimeError(
                f"ModuleManifest.rebuild: checkpoint {self._nickname or self.global_id!r} has "
                "no architecture entry; remember it into a live model instead"
            )
        blob = self._payloads(realized[ARCHITECTURE_KEY])
        try:
            module, parametrizations = _loads_architecture(blob, meta.get("pickler", "pickle"))
        except Exception as exc:
            raise RuntimeError(
                f"ModuleManifest.rebuild: cannot unpickle the architecture of "
                f"{meta.get('module')}.{meta.get('class')} -- the class must be importable "
                f"where the checkpoint is rebuilt ({exc!r})"
            ) from exc
        materialize(module, device)
        _restore_parametrizations(module, parametrizations)
        if meta.get("training") is False:
            module.eval()
        return module

    def _wrap(self, target: Any, nickname: str) -> Any:
        from .module import Module

        return Module(target, nickname=nickname, pool=self._pool, strict=self._strict)


class OptimizerManifest(_TorchManifest):
    """Checkpoint manifest of a ``torch.optim.Optimizer`` (scope ``MANIFEST:TORCH_OPTIMIZER``).

    Blueprint: ``{"state": {"<param idx>": {<name>: gid}}, "param_groups": gid, "meta": gid}``.
    ``remember`` rebuilds the int-keyed ``state_dict`` and calls
    ``target.load_state_dict``. The optimizer must already be constructed
    over the model's parameters (same count and order as when it was
    memorized); nothing else -- no backward pass, no prior ``step`` -- is
    required, torch moves each state tensor to its parameter's device.

    ``meta["param_refs"]`` holds, per group, ``[model gid, parameter name]``
    for every parameter (``None`` when the parameter belonged to no
    known model at snapshot time). ``rebuild`` uses them to pick the live
    parameters out of the rebuilt model(s) and constructs
    ``cls(groups, **defaults)`` before loading the state.
    """

    KIND: ClassVar[str] = "optimizer"
    _DEFAULT_SCOPES: ClassVar[list[str]] = [_MANIFEST_SCOPE, TORCH_OPTIMIZER_SCOPE]

    def _check_target(self, target: Any) -> None:
        if not isinstance(target, torch.optim.Optimizer):
            raise RuntimeError(
                "OptimizerManifest needs a torch.optim.Optimizer to load into, "
                f"got {type(target).__name__}"
            )
        groups = getattr(target, "param_groups", None)
        if not groups or not any(len(g.get("params", ())) for g in groups):
            raise RuntimeError(
                "optimizer is not initialized: construct it over model.parameters() "
                "before remembering its state"
            )

    # ----- parameter references -----------------------------------
    @staticmethod
    def _known_models(context: dict | None, target: Any) -> list[tuple[str, torch.nn.Module]]:
        """``(ModuleManifest gid, bare module)`` for every model in reach."""
        models: list[tuple[str, torch.nn.Module]] = []
        explicit = getattr(target, "model", None)
        candidates = [explicit] if explicit is not None else []
        if context:
            candidates.extend(context.values())
        seen: set[int] = set()
        for obj in candidates:
            if isinstance(obj, ManifestBacked) and isinstance(obj, torch.nn.Module):
                inner = _unwrap(obj)
                if id(inner) not in seen:
                    seen.add(id(inner))
                    models.append((obj.global_id, inner))
        return models

    def _snapshot_entries(self, target: torch.optim.Optimizer) -> tuple[dict, dict]:
        state_dict = target.state_dict()
        state = {
            str(idx): {
                name: self._leaf(f"{STATE_KEY}/{idx}/{name}", value)
                for name, value in per_param.items()
            }
            for idx, per_param in state_dict["state"].items()
        }
        param_groups = self._leaf(PARAM_GROUPS_KEY, state_dict["param_groups"])
        inner = _unwrap(target)
        by_id: dict[int, list[str]] = {}
        for gid, model in self._known_models(self._context, target):
            for name, p in model.named_parameters():
                by_id.setdefault(id(p), [gid, name])
        param_refs = [[by_id.get(id(p)) for p in g["params"]] for g in inner.param_groups]
        meta = {
            "kind": self.KIND,
            "class": type(inner).__qualname__,
            "module": type(inner).__module__,
            "n_params": [len(g["params"]) for g in state_dict["param_groups"]],
            "defaults": offload(dict(inner.defaults)),
            "param_refs": param_refs,
        }
        return {STATE_KEY: state, PARAM_GROUPS_KEY: param_groups}, meta

    def _load(self, realized: dict, target: torch.optim.Optimizer) -> None:
        param_groups = self._payloads(realized[PARAM_GROUPS_KEY])
        live = target.param_groups
        stored_sizes = [len(g["params"]) for g in param_groups]
        live_sizes = [len(g["params"]) for g in live]
        if stored_sizes != live_sizes:
            raise ValueError(
                f"stored optimizer state ({self._nickname!r}) covers parameter groups of sizes "
                f"{stored_sizes} but the live optimizer has {live_sizes}; construct the "
                "optimizer over the same parameters (same count and order) before remembering"
            )
        state = {
            int(idx): self._payloads(per_param)
            for idx, per_param in realized.get(STATE_KEY, {}).items()
        }
        target.load_state_dict({"state": state, "param_groups": param_groups})

    def _rebuild_target(self, realized: dict, meta: dict, device: Any, context: dict) -> Any:
        refs = meta.get("param_refs")
        if not refs or any(ref is None for group in refs for ref in group):
            raise RuntimeError(
                f"OptimizerManifest.rebuild: checkpoint {self._nickname or self.global_id!r} "
                "does not reference a model for every parameter (it was memorized without a "
                "sibling model in a complex manifest or an explicit Optimizer(..., model=)); "
                "construct the optimizer over the model's parameters and remember into it instead"
            )
        models: dict[str, torch.nn.Module] = {}
        for group in refs:
            for gid, _ in group:
                if gid in models:
                    continue
                live = context.get(gid)
                if live is None:
                    # Standalone rebuild: pull the model from its own checkpoint.
                    ref = laila.remember(
                        gid, dst_pool=self._routed(None, None, None), persist=False
                    )
                    try:
                        ref.wait()
                        model_manifest = ref.result
                    finally:
                        ref.release()
                    if not isinstance(model_manifest, ModuleManifest):
                        raise RuntimeError(
                            f"OptimizerManifest.rebuild: {gid} is not a ModuleManifest checkpoint"
                        )
                    live = model_manifest.rebuild(
                        pool=self._routed(None, None, None), device=device, context=context
                    )
                    context[gid] = live
                if not isinstance(live, torch.nn.Module):
                    raise RuntimeError(
                        f"OptimizerManifest.rebuild: context[{gid}] is {type(live).__name__}, "
                        "expected the rebuilt model"
                    )
                models[gid] = _unwrap(live)
        named = {gid: dict(model.named_parameters()) for gid, model in models.items()}
        stored_groups = self._payloads(realized[PARAM_GROUPS_KEY])
        groups = []
        for group_refs, stored in zip(refs, stored_groups):
            params = []
            for gid, name in group_refs:
                try:
                    params.append(named[gid][name])
                except KeyError as exc:
                    raise RuntimeError(
                        f"OptimizerManifest.rebuild: model {gid} has no parameter {name!r}"
                    ) from exc
            hyper = {k: v for k, v in stored.items() if k != "params"}
            groups.append({"params": params, **hyper})
        cls = _import_class(meta["module"], meta["class"])
        defaults = dict(meta.get("defaults") or {})
        try:
            return cls(groups, **defaults)
        except TypeError:
            return cls(groups)

    def _wrap(self, target: Any, nickname: str) -> Any:
        from .optimizer import Optimizer

        return Optimizer(target, nickname=nickname, pool=self._pool)


__all__ = [
    "TORCH_MODULE_SCOPE",
    "TORCH_OPTIMIZER_SCOPE",
    "ModuleManifest",
    "OptimizerManifest",
    "materialize",
    "offload",
    "skeleton",
]

from laila.entry.constitution.build_maps import register_builder

register_builder(
    f"{_MANIFEST_SCOPE}:{TORCH_MODULE_SCOPE}",
    ModuleManifest._build_from_dict_sync,
    ModuleManifest._build_from_dict_async,
)
register_builder(
    f"{_MANIFEST_SCOPE}:{TORCH_OPTIMIZER_SCOPE}",
    OptimizerManifest._build_from_dict_sync,
    OptimizerManifest._build_from_dict_async,
)
