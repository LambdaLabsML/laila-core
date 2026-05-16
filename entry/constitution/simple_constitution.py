"""Simple constitution: an ordered chain of single-callable code strings.

A :class:`SimpleConstitution` carries a list of Python source strings,
each defining exactly one callable. :meth:`build(payload_input)` threads
``payload_input`` through every callable in order:

.. code-block:: text

    out = code[0](payload_input)
    out = code[1](out)
    ...

The canonical use case is the *inverse* transformation chain emitted by
:meth:`Entry.serialize`. When a pool serializes an entry through
``base64 -> zlib -> msgpack``, the corresponding simple constitution
contains three source strings whose composition decodes the bytes back
into the original Python value -- in the reverse order:
``msgpack-loads`` first, then ``zlib-decompress``, then
``base64-decode``. (The transformation pipeline emits its inverses
already in reverse order, so the constitution itself is just a flat
list applied left-to-right.)
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import PrivateAttr

from .constitution import Constitution, _exec_one_fn, _register_kind


@_register_kind("simple")
class SimpleConstitution(Constitution):
    """An ordered chain of single-callable code strings.

    Each element of :attr:`codes` is a self-contained Python source
    string defining exactly one top-level callable. The chain is
    applied left-to-right at build time. An empty chain is valid and
    returns ``payload_input`` unchanged.
    """

    _codes: List[str] = PrivateAttr(default_factory=list)

    def __init__(self, **data: Any):
        """Accept ``codes=`` as the list of source strings.

        The list is shallow-copied internally so subsequent mutation
        of the caller's list does not affect this constitution.

        Raises
        ------
        TypeError
            If ``codes`` is not a list of strings.
        """
        super().__init__()
        codes = data.get("codes", [])
        if not isinstance(codes, list) or not all(isinstance(c, str) for c in codes):
            raise TypeError("codes must be a list of Python source strings")
        self._codes = list(codes)

    @property
    def codes(self) -> List[str]:
        """A defensive copy of the ordered list of source strings.

        Returning a copy means callers cannot accidentally mutate the
        constitution by editing the returned list -- treat
        :class:`SimpleConstitution` instances as immutable from the
        outside.
        """
        return list(self._codes)

    def build(self, payload_input: Optional[Any] = None) -> Any:
        """Thread *payload_input* through every code in order.

        Each step compiles its source string via :func:`_exec_one_fn`
        and invokes the resulting callable on the running value.
        Compilation happens on every build (no cache), which keeps the
        memory footprint small for constitutions that are rarely re-run
        but does mean tight inner loops should not call ``build`` in a
        hot path.

        Parameters
        ----------
        payload_input : Any, optional
            The starting value (typically the encoded bytes loaded from
            a pool).

        Returns
        -------
        Any
            The result of applying every code string in order.
        """
        current = payload_input
        for code in self._codes:
            current = _exec_one_fn(code)(current)
        return current

    def as_dict(self) -> dict:
        """Serialize as ``{"_kind": "simple", "codes": [...]}``."""
        return {"_kind": "simple", "codes": list(self._codes)}

    @classmethod
    def _from_dict(cls, in_dict: dict) -> "SimpleConstitution":
        """Rebuild a :class:`SimpleConstitution` from its serialized dict.

        Missing ``codes`` is treated as an empty chain (a valid no-op
        constitution) rather than raising, mirroring the constructor's
        default.
        """
        return cls(codes=in_dict.get("codes", []))
