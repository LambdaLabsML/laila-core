"""Central sub-systems of a Laila policy.

Each policy owns a small bundle of "central" subsystems that handle
concerns that cut across an entire policy:

- :mod:`command <laila.policy.central.command>` -- task-force
  registry, work submission, futures, and shutdown coordination.
- :mod:`memory <laila.policy.central.memory>` -- the
  ``memorize`` / ``remember`` / ``forget`` API, the pool router, and
  :class:`Manifest` / :class:`Hint` machinery.
- :mod:`communication <laila.policy.central.communication>` --
  transport protocols, peer registry, and the inbound/outbound RPC
  dispatch used by remote policy proxies.
- ``logic`` (placeholder) -- reserved for higher-level orchestration
  hooks; not yet implemented.

These four are bundled inside :class:`_LAILA_IDENTIFIABLE_POLICY.Central`
on every policy instance.
"""
