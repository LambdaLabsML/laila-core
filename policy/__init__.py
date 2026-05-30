"""Policy module -- container for the policy schema and its central sub-systems.

A *policy* is the top-level coordinator that owns three central
sub-systems:

- :class:`central.memory <_LAILA_IDENTIFIABLE_CENTRAL_MEMORY>` --
  routes ``memorize`` / ``remember`` / ``forget`` calls to the
  appropriate :class:`Pool` and applies serialization transformations.
- :class:`central.command <_LAILA_IDENTIFIABLE_CENTRAL_COMMAND>` --
  manages task-forces and the future bank; submits work asynchronously
  and tracks lifetime via :class:`Future` objects.
- :class:`central.communication <_LAILA_IDENTIFIABLE_COMMUNICATION>` --
  registers transport protocols (TCP/IP today) and the resulting peer
  registry of :class:`RemotePolicyProxy` handles.

A process can host any number of policies but only one is *active* at
any moment (see :func:`laila.activate_policy`). Policies are themselves
addressable by their ``global_id``, which is what enables peer-to-peer
RPC: a remote process sees a local policy as a proxy keyed by that gid.
"""
