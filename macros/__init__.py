"""Laila macros sub-package -- aliases, defaults, and scope strings.

Catch-all home for *constants* and *factory helpers* that need to be
referenced from many places in laila but don't belong in any one
domain module:

- :mod:`.aliases` -- short-form re-exports for the most-used public
  classes (e.g. ``DefaultPolicy``, ``Entry``).
- :mod:`.defaults` -- factory helpers that produce ready-to-use
  default policies / pools / taskforces, plus the
  :data:`LAILA_DEFAULT_DIRECTORIES` table that other modules
  consult for "where do I put my files?".
- :mod:`.strings` -- the canonical scope-string constants that show
  up in global ids and CLI-args paths (``_POLICY_SCOPE``,
  ``_FUTURE_SCOPE``, ``_POOL_SCOPE``, ...). Centralised here so a
  rename touches exactly one file.
"""
