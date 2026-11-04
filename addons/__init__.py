"""Optional add-ons built on top of the core ``laila`` API.

Every add-on lives in its own subpackage and only uses the public
``laila.*`` surface (``memorize`` / ``remember`` / ``forget``,
``Manifest``, pool proxy operators, ``laila.command.submit``). Nothing
here touches pool internals.

- :mod:`laila.addons.cache` -- ``default_tiers``: wire the
  ``RAM << HDF5 << cloud`` read-through chain in one call.
- :mod:`laila.addons.torch` -- ``Dataset`` / ``DataLoader``: a
  ``torch.utils.data`` pair over a :class:`~laila.Manifest` with
  lookahead prefetching on LAILA's taskforce; ``Module`` / ``Optimizer``:
  ``torch.nn.Module`` / ``torch.optim.Optimizer`` wrappers that
  ``laila.memorize`` / ``remember`` / ``forget`` accept directly
  (``ModuleManifest`` / ``OptimizerManifest`` under the hood).
- ``addons/skills/data_loading`` -- an agent skill (``SKILL.md``) that
  turns a dataset folder into a manifest + loader.
- ``addons/skills/model_checkpoint`` -- an agent skill that checkpoints
  and resumes a torch model + optimizer through ``Module`` / ``Optimizer``.
"""

from .cache import default_tiers as default_tiers
