"""Pool router -- resolves a memory request to a concrete :class:`Pool`.

The router maintains three coupled data structures:

- ``pools`` -- ``{global_id: Pool}`` -- the authoritative pool registry.
- ``pools_pq`` -- a ``(-affinity, global_id)`` priority queue used as
  the fallback ordering when nothing more specific is requested.
- ``pools_nicknames`` -- ``{nickname: global_id}`` -- a friendly-name
  index so callers can pass ``pool_nickname="cache"`` instead of a
  full UUID-bearing gid.

Routing precedence used by :meth:`PoolRouter.route`:

1. Explicit ``pool_id`` -- always wins.
2. ``pool_nickname`` -- resolved through ``pools_nicknames``.
3. (Future) ``affinity`` -- not yet implemented; falls through to (4).
4. The ``DEFAULT`` nickname's pool, which is auto-registered if missing.
"""

from __future__ import annotations
from typing import Optional, Any, Dict, Iterable, Iterator, Mapping, List
from pydantic import BaseModel, Field, PrivateAttr
from .....basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS
from queue import PriorityQueue

from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _POOL_ROUTER_SCOPE, _DEFAULT_POOL_NICKNAME
from .....pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from .....entry import Entry


class _LAILA_IDENTIFIABLE_POOL_ROUTER(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Selects the destination pool for ``memorize`` / ``remember`` /
    ``forget`` calls.

    Constructed automatically by every
    :class:`_LAILA_IDENTIFIABLE_CENTRAL_MEMORY` and exposed as
    ``policy.central.memory.pool_router``. Users typically interact
    with it indirectly through :meth:`Policy.extend` and the
    ``pool_id`` / ``pool_nickname`` kwargs on the high-level memory
    API, but it is also usable directly for advanced multi-pool setups.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_ROUTER_SCOPE]))
    pools: Optional[Dict[str,_LAILA_IDENTIFIABLE_POOL]] = CLIExempt(default_factory = dict)
    pools_pq: Optional[PriorityQueue] = CLIExempt(default_factory = PriorityQueue)
    pools_nicknames: Optional[Dict[str,str]] = Field(default_factory = dict)


    class Config:
        arbitrary_types_allowed = True


    def model_post_init(self, __context: Any) -> None:
        """Auto-register a :class:`DefaultPool` (in-memory) when no pools were supplied.

        This is what makes a fresh ``DefaultPolicy()`` immediately
        usable without any pool wiring -- the in-memory default is good
        enough for tests and quickstart, and users override it by
        calling :meth:`Policy.extend` with a "real" pool (filesystem,
        S3, postgres, ...).
        """
        if len(self.pools) == 0:
            from .....macros.defaults import DefaultPool
            self.extend(
                pool = DefaultPool(), 
                affinity=1,
                pool_nickname=_DEFAULT_POOL_NICKNAME
            )


    def extend(
        self,
        pool: _LAILA_IDENTIFIABLE_POOL,
        *,
        affinity: Optional[float] = None,
        pool_nickname: Optional[str] = None,
    ):
        """Register a pool with optional affinity priority and nickname.

        Stores *pool* under its ``global_id`` in :attr:`pools`, pushes
        a ``(-affinity, gid)`` entry into the affinity priority queue
        (negated so higher affinity sorts first), and -- if a nickname
        was given -- adds a ``nickname -> gid`` entry to
        :attr:`pools_nicknames`.

        Re-registering an existing nickname overwrites the previous
        binding; re-registering an existing gid overwrites the pool
        instance. That makes hot-swapping a pool implementation
        possible at the cost of users needing to be careful about
        accidentally shadowing a gid.

        Parameters
        ----------
        pool : _LAILA_IDENTIFIABLE_POOL
            The pool instance to register.
        affinity : float, optional
            Routing priority (higher = preferred). Defaults to ``0``,
            which sorts last in the priority queue.
        pool_nickname : str, optional
            Human-readable alias for this pool, usable as the
            ``pool_nickname`` kwarg on memory operations.
        """
        if affinity is None:
            affinity = 0 #farthest away

        self.pools_pq.put((-affinity, pool.global_id))
        self.pools[pool.global_id] = pool
        if pool_nickname is not None:
            self.pools_nicknames[pool_nickname] = pool.global_id


    def route(
        self,
        entries: List[Entry]|List[str],
        *,
        pool_id: Optional[str] = None,
        pool_nickname: Optional[str] = None,
        affinity: Optional[float] = None,
    ):
        """Resolve the destination pool for *entries*.

        Today the routing decision does not actually depend on
        *entries* -- it is purely based on the explicit ``pool_id`` /
        ``pool_nickname`` arguments and the default-pool fallback. The
        *entries* parameter is retained for forward compatibility with
        future strategies (e.g. content-based or affinity-based
        routing that picks per-entry destinations).

        Parameters
        ----------
        entries : list[Entry] or list[str]
            The entries (or entry ids) being routed. Currently unused.
        pool_id : str, optional
            Explicit pool ``global_id`` -- highest precedence.
        pool_nickname : str, optional
            Nickname resolved via :meth:`_route_by_nickname`.
        affinity : float, optional
            Reserved for future affinity-based routing.

        Returns
        -------
        _LAILA_IDENTIFIABLE_POOL
            The selected pool instance.

        Raises
        ------
        KeyError
            If ``pool_id`` is given but not registered, or if
            ``pool_nickname`` does not resolve.
        """
        if pool_id is not None:
            return self.pools[pool_id]
        return self._route_by_nickname(pool_nickname=pool_nickname)


    def _route_by_nickname(
        self, 
        *,
        pool_nickname: Optional[str] = None,
    ) -> _LAILA_IDENTIFIABLE_POOL:
        """Resolve a pool by nickname, falling back to the default pool.

        When *pool_nickname* is ``None`` the fallback is the pool
        registered under ``_DEFAULT_POOL_NICKNAME`` (always present
        because :meth:`model_post_init` auto-registers one if missing).

        Raises
        ------
        KeyError
            If *pool_nickname* is set but not registered, or if the
            default nickname has been removed and no fallback exists.
        """
        if pool_nickname is not None:
            return self.pools[self.pools_nicknames[pool_nickname]]
        else:
            return self.pools[self.pools_nicknames[_DEFAULT_POOL_NICKNAME]]