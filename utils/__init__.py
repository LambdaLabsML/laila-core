"""Utility helpers shared across the laila code base.

This sub-package holds *cross-cutting* primitives -- pieces of code
that don't belong to any one domain module but are reached for from
many places:

- :mod:`.guarantee` -- :class:`_Guarantee` / :class:`_AsyncGuarantee`
  context-manager classes plus the ``guarantee`` / ``guarantee_async``
  decorators. Together they implement the laila *guarantee scope*
  -- a stack of policies that govern how futures created inside a
  block of code should behave (e.g. "wait synchronously",
  "fan out", "no-submit"). The guarantee stack is consulted by
  ``Command.submit`` and the future lifecycle hooks.
- :mod:`.args` -- the runtime argument loader that produces the
  :data:`laila.args` :class:`AtomicDotMap`. Reads CLI args,
  ``LAILA_*`` environment variables, and ``laila.json`` /
  ``laila.toml`` configuration files into a single nested mapping
  consulted by every CLI-capable class.
- :mod:`.decorators` -- assorted decorator helpers
  (:mod:`.synchronized` for re-entrant locking,
  :mod:`.typecheck` for runtime type assertions).
"""

from .guarantee import _AsyncGuarantee, _Guarantee, guarantee, guarantee_async
