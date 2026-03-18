from __future__ import annotations
from typing import Optional, Any, Dict, Iterable, Iterator, Mapping, List
from pydantic import BaseModel, Field, PrivateAttr
from queue import PriorityQueue

from .....atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _POOL_ROUTER_SCOPE, _DEFAULT_POOL_NICKNAME
from .....pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from .....entry import Entry


class _LAILA_IDENTIFIABLE_POOL_ROUTER(_LAILA_IDENTIFIABLE_OBJECT):
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_ROUTER_SCOPE]))
    pools: Optional[Dict[str,_LAILA_IDENTIFIABLE_POOL]] = Field(default_factory = dict)
    pools_pq: Optional[PriorityQueue] = Field(default_factory = PriorityQueue)
    pools_nicknames: Optional[Dict[str,str]] = Field(default_factory = dict)


    class Config:
        arbitrary_types_allowed = True


    def model_post_init(self, __context: Any) -> None:
        if len(self.pools) == 0:
            from .....macros.defaults import DefaultPool
            self.add_pool(
                pool = DefaultPool(), 
                affinity=1,
                pool_nickname=_DEFAULT_POOL_NICKNAME
            )
        else:
            raise NotImplementedError


    def add_pool(
        self,
        pool: _LAILA_IDENTIFIABLE_POOL,
        *,
        affinity: Optional[float] = None,
        pool_nickname: Optional[str] = None,
    ):
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
        #immediate route
        if pool_id is not None:
            return self.pools[pool_id]
        #nickname route
        return self._route_by_nickname(pool_nickname=pool_nickname)
        
        #rest is not implemented yet
        raise NotImplementedError


    def _route_by_nickname(
        self, 
        *,
        pool_nickname: Optional[str] = None,
    ) -> _LAILA_IDENTIFIABLE_POOL:
        if pool_nickname is not None:
            return self.pools[self.pools_nicknames[pool_nickname]]
        else:
            return self.pools[self.pools_nicknames[_DEFAULT_POOL_NICKNAME]]

        raise NotImplementedError