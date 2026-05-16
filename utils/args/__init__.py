"""Runtime argument-loading utilities for laila.

Exports :class:`ArgReader`, the loader behind :data:`laila.args`.
``ArgReader`` knows how to walk the four configuration sources laila
recognises and merge them into a single :class:`AtomicDotMap`:

1. Command-line arguments parsed by :mod:`argparse`.
2. ``LAILA_*`` environment variables.
3. Project-local ``laila.json`` / ``laila.toml`` files.
4. Programmatic assignments (``laila.args.foo = 1``) made after
   the initial load.

The merged map is consumed by every CLI-capable class during
construction (see :mod:`basics.definitions.cli_capable`), so a
single ``laila.args.policy.central.command.taskforces.<gid>.queue_size``
key can configure a runtime taskforce just as well as the matching
``--policy.central.command.taskforces.<gid>.queue_size=...`` CLI flag.
"""
from .args import ArgReader
