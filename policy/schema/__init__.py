"""Policy schema sub-package -- base class for every Laila policy.

Houses :class:`_LAILA_IDENTIFIABLE_POLICY`, the abstract base that
combines :class:`_LAILA_CLI_CAPABLE_CLASS` (4-tier parameter resolution
from ``laila.args``) with :class:`_LAILA_IDENTIFIABLE_OBJECT` (UUID +
global_id machinery). Every concrete policy subclass -- including the
default :class:`DefaultPolicy` -- inherits from this base.
"""
