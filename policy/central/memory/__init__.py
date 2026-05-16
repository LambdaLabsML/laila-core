"""Memory sub-package -- the routing brain behind ``memorize`` / ``remember`` / ``forget``.

Layout:

- :mod:`schema <laila.policy.central.memory.schema>` -- the central
  memory class itself (``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``) plus
  :class:`Manifest`, the structured-references payload type.
- :mod:`router <laila.policy.central.memory.router>` -- the
  :class:`PoolRouter` that resolves a request (by ``pool_id``,
  ``pool_nickname``, or affinity) to a concrete :class:`Pool`.
- :mod:`record <laila.policy.central.memory.record>` -- the
  :class:`Record` envelope that decorates an :class:`Entry` with
  recorder/borrower metadata before it is serialized to the pool.
- :mod:`hint <laila.policy.central.memory.hint>` -- the
  :class:`MemoryHint` knob that lets callers nudge routing towards a
  particular pool, purpose, or affinity.
"""
