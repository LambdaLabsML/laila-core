# entry_graph_nx_test.py

from __future__ import annotations
import uuid
from enum import Enum, auto
from typing import Optional, List, Callable, Dict, Set
import networkx as nx
from pydantic import BaseModel

from ...entry.entry_state import EntryState
from ...entry.entry import EntryMetadataView



Criteria = Optional[Callable[[EntryMetadataView], bool]]


# --------------------------------------------------------------------------- #
#  graph manager
# --------------------------------------------------------------------------- #
class EntryGraphNX:
    """
    Manages EntryMetadataView nodes and their relations via a NetworkX DiGraph.
    Parent edges point **from parent → child** (precedence order).
    """

    # ------------------------------------------------------------------ #
    # construction
    # ------------------------------------------------------------------ #
    def __init__(self, entries: Iterable[EntryMetadataView] | None = None) -> None:
        self.g: nx.DiGraph = nx.DiGraph()
        if entries:
            for e in entries:
                self.g.add_node(e.global_id, entry=e)
            for e in entries:
                for parent in e.precedence or []:
                    if parent not in self.g:
                        raise KeyError(f"Unknown precedence id {parent!r}")
                    self.g.add_edge(parent, e.global_id)

    def __getitem__(self, entry_uuid: str) -> EntryMetadataView:
        """Allow bracket-access to retrieve an entry by its node ID."""
        if entry_uuid not in self.g:
            raise KeyError(f"Node {entry_uuid!r} not found in graph.")
        return self._entry(entry_uuid)
    
    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _accepts(entry: EntryMetadataView, criteria: Criteria) -> bool:
        return criteria(entry) if criteria else True

    def _entry(self, entry_uuid: str) -> EntryMetadataView:
        return self.g.nodes[entry_uuid]["entry"]

    # def _ensure_node(self, entry_uuid: str) -> None:
    #     """Create a stub node if it is not already present."""
    #     if entry_uuid not in self.g:
    #         stub = EntryMetadataView(global_id=entry_uuid)
    #         self.g.add_node(entry_uuid, entry=stub)
            
    # ------------------------------------------------------------------ #
    # core utils
    # ------------------------------------------------------------------ #
    def keys(self, criteria: Criteria = None) -> Iterator[str]:
        """Yield all node IDs (keys) in the graph that meet the optional criteria."""
        return (
            (n for n in self.g.nodes if self._accepts(self._entry(n), criteria))
            if criteria else iter(self.g.nodes)
        )

    def has_key(self, entry_uuid: str) -> bool:
        """Check if the given node ID exists in the graph."""
        return entry_uuid in self.g

    def size(self, criteria: Criteria = None) -> int:
        """Return the number of nodes in the graph that meet the optional criteria."""
        if criteria is None:
            return self.g.number_of_nodes()
        return sum(1 for n in self.g.nodes if self._accepts(self._entry(n), criteria))

    def __len__(self):
        return self.size()

    def __contains__(self, entry_uuid: str) -> bool:
        return entry_uuid in self.g
    
    # ------------------------------------------------------------------ #
    # core upsert
    # ------------------------------------------------------------------ #
    def update_graph(self, view: EntryMetadataView) -> str:
        """Insert or patch a node, syncing precedence edges and mutable attrs."""
        gid: str = view.global_id or str(uuid.uuid4())
        view.global_id = gid
    
        # ---------- update ----------
        if gid in self.g:
            entry = self._entry(gid)
    
            # scalars
            for attr in ("evolution", "pool_id", "alias", "state"):
                val = getattr(view, attr)
                if val is not None and val != getattr(entry, attr):
                    setattr(entry, attr, val)
    
            # precedence delta
            new_parents = set(view.precedence or [])
            cur_parents = set(self.g.predecessors(gid))
    
            for lost in cur_parents - new_parents:
                self.g.remove_edge(lost, gid)
            for gained in new_parents - cur_parents:
                if gained not in self.g:
                    raise KeyError(f"Unknown precedence id {gained!r}")
                self.g.add_edge(gained, gid)
    
            entry.precedence = list(new_parents)
            return gid
    
        # ---------- insert ----------
        self.g.add_node(gid, entry=view)
        for parent in view.precedence or []:
            if parent not in self.g:
                raise KeyError(f"Unknown precedence id {parent!r}")
            self.g.add_edge(parent, gid)
    
        return gid

    # ------------------------------------------------------------------ #
    # mutators
    # ------------------------------------------------------------------ #
    def add_entry_precedence(self, entry_uuid: str, new_precedence: List[str]) -> None:
        ent = self._entry(entry_uuid)
        ent.precedence = sorted(set((ent.precedence or []) + new_precedence))
        for p in new_precedence:
            if p not in self.g:
                raise KeyError(f"Unknown precedence id {p!r}")
            self.g.add_edge(p, entry_uuid)

    def set_entry_precedence(self, entry_uuid: str, precedence: List[str]) -> None:
        self.g.remove_edges_from([(p, entry_uuid) for p in list(self.g.predecessors(entry_uuid))])
        for p in precedence:
            if p not in self.g:
                raise KeyError(f"Unknown precedence id {p!r}")
            self.g.add_edge(p, entry_uuid)
        self._entry(entry_uuid).precedence = list(precedence)

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def get_entry_precedence(
        self,
        entry_uuid: str | None = None,
        criteria: Criteria = None,
    ) -> Union[Dict[str, List[str]], List[str]]:
        """
        • `g.get_entry_precedence()` → dict[entry_uuid -> parents]  
        • `g.get_entry_precedence("B")` → parents of **B** only
        """
        if entry_uuid is not None:
            if entry_uuid not in self.g:
                raise KeyError(entry_uuid)
            if not self._accepts(self._entry(entry_uuid), criteria):
                return []
            return list(self.g.predecessors(entry_uuid))

        return {
            n: list(self.g.predecessors(n))
            for n in self.g.nodes
            if self._accepts(self._entry(n), criteria)
        }

    def get_entry_dependents(self, entry_uuid: str, criteria: Criteria = None) -> List[str]:
        return [
            s for s in self.g.successors(entry_uuid)
            if self._accepts(self._entry(s), criteria)
        ]

    # ------------------------------------------------------------------ #
    # traversals
    # ------------------------------------------------------------------ #
    def get_entry_subgraph(self, entry_uuid: str, criteria: Criteria = None) -> Set[str]:
        nodes = {entry_uuid, *nx.ancestors(self.g, entry_uuid), *nx.descendants(self.g, entry_uuid)}
        return {n for n in nodes if self._accepts(self._entry(n), criteria)}

    def get_dfs_with_criteria(self, entry_uuid: str, criteria: Criteria = None) -> List[str]:
        visited, order = set(), []

        def dfs(n: str) -> None:
            if n in visited:
                return
            visited.add(n)
            for p in self.g.predecessors(n):
                dfs(p)
            if self._accepts(self._entry(n), criteria):
                order.append(n)

        dfs(entry_uuid)
        return order

    def get_bfs_with_criteria(self, entry_uuid: str, criteria: Criteria = None) -> List[str]:
        q: deque[str] = deque([entry_uuid])
        visited: Set[str] = set()
        order: List[str] = []

        while q:
            n = q.popleft()
            if n in visited:
                continue
            visited.add(n)
            if self._accepts(self._entry(n), criteria):
                order.append(n)
            q.extend(self.g.predecessors(n))
        return order

    def get_leaves(self, entry_uuid: str, criteria: Criteria = None) -> List[str]:
        leaves = [
            n for n in self.get_dfs_with_criteria(entry_uuid)
            if self.g.in_degree(n) == 0 and self._accepts(self._entry(n), criteria)
        ]
        return sorted(leaves)

    # ------------------------------------------------------------------ #
    # whole-graph helpers
    # ------------------------------------------------------------------ #
    def get_all_isolated(self, criteria: Criteria = None) -> List[str]:
        return sorted(
            n for n in self.g.nodes
            if self.g.out_degree(n) == 0 and self._accepts(self._entry(n), criteria)
        )

    def get_all_leaves(self, criteria: Criteria = None) -> List[str]:
        return sorted(
            n for n in self.g.nodes
            if self.g.in_degree(n) == 0 and self._accepts(self._entry(n), criteria)
        )

    def get_all_roots(self, criteria: Criteria = None) -> List[str]:
        return sorted(
            n for n in self.g.nodes
            if self.g.in_degree(n) > 0            # has precedents
            and self.g.out_degree(n) == 0         # no dependents
            and self._accepts(self._entry(n), criteria)
        )

    def get_all_pending(self, criteria: Criteria = None) -> List[str]:

        return sorted(
            n for n in self.g.nodes
            if self.g.in_degree(n) > 0                                 # has precedents
            and self._entry(n).state != EntryState.READY               # not READY itself
            and all(
                self._entry(p).state == EntryState.READY               # every parent READY
                for p in self.g.predecessors(n)
            )
            and self._accepts(self._entry(n), criteria)
        )


    def activate(
        self,
        entry_uuid: str,
        criteria: Criteria = None,
        recursive: bool = True
    ) -> None:
        """
        Activates an entry (sets .activated = True), and optionally all its precedents recursively.
    
        Parameters:
        - entry_uuid (str): The ID of the entry to activate.
        - criteria (Optional[Callable]): If provided, only entries passing the filter will be activated.
        - recursive (bool): If True, activates all precedents recursively as well.
        """
        if entry_uuid not in self.g:
            raise KeyError(f"Node {entry_uuid!r} not found in graph.")
        
        if recursive:
            for uid in self.get_dfs_with_criteria(entry_uuid, criteria):
                self._entry(uid).activated = True
        else:
            entry = self._entry(entry_uuid)
            if self._accepts(entry, criteria):
                entry.activated = True


    def protect(
        self,
        entry_uuid: str,
        criteria: Criteria = None,
        recursive: bool = False
    ) -> None:
        """
        Protects an entry (sets .protected = True), and optionally all its precedents recursively.
    
        Parameters:
        - entry_uuid (str): The ID of the entry to protect.
        - criteria (Optional[Callable]): If provided, only entries passing the filter will be protected.
        - recursive (bool): If True, protects all precedents recursively as well.
        """
        if entry_uuid not in self.g:
            raise KeyError(f"Node {entry_uuid!r} not found in graph.")
        
        if recursive:
            for uid in self.get_dfs_with_criteria(entry_uuid, criteria):
                self._entry(uid).protected = True
        else:
            entry = self._entry(entry_uuid)
            if self._accepts(entry, criteria):
                entry.protected = True



    def nuke_node(self, entry_uuid: str) -> None:
        """
        Deletes a node from the graph along with all its incoming and outgoing edges.

        Raises:
            KeyError: If the node does not exist.
            ValueError: If the node is protected.
        """
        if entry_uuid not in self.g:
            raise KeyError(f"Node {entry_uuid!r} not found in graph.")

        entry = self._entry(entry_uuid)
        if getattr(entry, "protected", False):
            raise ValueError(f"Node {entry_uuid!r} is protected and cannot be deleted.")

        self.g.remove_node(entry_uuid)