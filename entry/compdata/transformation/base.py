"""Abstract base classes for data transformations and the pipeline that chains them.

A *transformation* is a reversible function on opaque data: an
encoding (base64), a compression (zlib), an encryption (AES), a
serialisation step (msgpack, pickle, numpy, torch), or a wrapper
into a JSON-friendly string. They are the building blocks that
:class:`SimpleConstitution` strings together to recover an entry's
payload from its serialised form.

Two layers live in this module:

- :class:`_data_transformation` -- the base class every concrete
  transformation inherits from. Subclasses register themselves in
  :data:`_data_transformation.REGISTRY` keyed by their ``name``
  field, so the constitution machinery can look up a transformation
  by name when reconstructing it from a serialised dict.
- :class:`TransformationSequence` -- an ordered pipeline of
  transformations. ``forward`` applies them in order on the way
  out (when serialising into a pool); ``backward`` is invoked
  *implicitly* by the constitution by replaying the inverse code
  snippets that ``forward`` collects.

An empty pipeline is an explicit identity, which lets pools default
their ``transformations`` field to ``None`` / ``[]`` without any
special-casing on the call sites.
"""

from __future__ import annotations

import ast
import re
import threading
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

# Sentinel ``backward_kwargs`` used to render a class's recovery-code
# *template*: its ``repr`` is located in the rendered code and replaced
# by a capture group (see ``_data_transformation._backward_code_pattern``).
_SLOT_KWARGS: dict[str, Any] = {"__laila_slot__": 0}
_SLOT_KWARGS_REPR = repr(_SLOT_KWARGS)


def _is_literal_dict(text: str) -> bool:
    """``True`` when *text* is the ``repr`` of a plain literal dict."""
    try:
        return isinstance(ast.literal_eval(text), dict)
    except Exception:
        return False


class _data_transformation(BaseModel, ABC):
    """Abstract base for a single reversible data transformation.

    Concrete subclasses must:

    - Set a stable, unique ``name`` field. The registry uses this
      name as its key, so renaming a transformation is a wire-format
      breaking change.
    - Implement :meth:`forward` (apply on the way out) and
      :meth:`backward` (apply on the way back in). The two must be
      mathematical inverses on the data shape they're declared for.
    - Optionally provide ``backward_code``: a string snippet that
      can be evaluated by the constitution to reconstruct the
      backward step at recovery time without needing to import the
      original transformation class. Used by
      :class:`SimpleConstitution`.

    Subclasses with a non-empty ``name`` are auto-registered in
    :attr:`REGISTRY` via ``__init_subclass__`` so look-up by name
    Just Works without any explicit registration call.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(default_factory=str)
    forward_kwargs: dict[str, Any] = Field(default_factory=dict)
    backward_kwargs: dict[str, Any] = Field(default_factory=dict)
    backward_code: str = Field(default_factory=str)
    REGISTRY: ClassVar[dict[str, type[_data_transformation]]] = {}

    # ------------------------------------------------------------------
    # Recovery-code trust
    # ------------------------------------------------------------------
    # A ``SimpleConstitution`` read back from a pool or a peer is a list
    # of Python source snippets that get ``exec``'d. Only snippets this
    # process could itself have produced are allowed to run:
    #
    # - ``_matches_backward_code`` -- template match against the class's
    #   own recovery code (fixed text identical, the ``backward_kwargs``
    #   slot a literal dict). Records written with other kwargs by other
    #   processes / hosts / ports still match.
    # - ``_KNOWN_BACKWARD_CODES`` -- exact strings generated in this
    #   process (every assignment to ``backward_code`` lands here), the
    #   fallback for third-party transformations whose template cannot
    #   be probed.
    _KNOWN_BACKWARD_CODES: ClassVar[set[str]] = set()
    _PATTERN_CACHE: ClassVar[dict[type, Any]] = {}
    _TRUST_LOCK: ClassVar[threading.Lock] = threading.Lock()

    def __init_subclass__(cls, **kwargs):
        """Register the subclass in ``REGISTRY`` when *name* is set."""
        super().__init_subclass__(**kwargs)
        n = getattr(cls, "name", None)
        if isinstance(n, str) and n:
            _data_transformation.REGISTRY[n] = cls

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name == "backward_code" and isinstance(value, str) and value:
            with _data_transformation._TRUST_LOCK:
                _data_transformation._KNOWN_BACKWARD_CODES.add(value)

    @classmethod
    def _template_probe(cls) -> _data_transformation:
        """Return an instance whose ``backward_code`` embeds the sentinel kwargs.

        Subclasses needing extra constructor arguments (e.g. a key)
        override this; raising means "no template" and only exact,
        locally generated codes are trusted for the class.
        """
        return cls(backward_kwargs=dict(_SLOT_KWARGS))

    @classmethod
    def _template_slots(cls) -> list[tuple[str, Any]]:
        """``(sentinel_text, validator)`` pairs present in the probe's code."""
        return [(_SLOT_KWARGS_REPR, _is_literal_dict)]

    @classmethod
    def _backward_code_pattern(cls):
        """Compiled template for this class's recovery code (cached), or ``None``."""
        cache = _data_transformation._PATTERN_CACHE
        if cls in cache:
            return cache[cls]
        try:
            code = cls._template_probe().backward_code
        except Exception:
            code = ""
        result = None
        if code:
            slots = cls._template_slots()
            token_re = "|".join(re.escape(t) for t, _ in slots)
            parts = re.split(f"({token_re})", code)
            pattern = ""
            validators = []
            for part in parts:
                hit = next((v for t, v in slots if t == part), None)
                if hit is not None:
                    pattern += "(.*?)"
                    validators.append(hit)
                else:
                    pattern += re.escape(part)
            # A slot-less template (e.g. JsonString) is an exact match.
            result = (re.compile(pattern, re.DOTALL), validators)
        with _data_transformation._TRUST_LOCK:
            cache[cls] = result
        return result

    @classmethod
    def _matches_backward_code(cls, code: str) -> bool:
        """``True`` when *code* is an instance of this class's recovery template."""
        compiled = cls._backward_code_pattern()
        if compiled is None:
            return False
        pattern, validators = compiled
        m = pattern.fullmatch(code)
        if m is None:
            return False
        return all(v(g) for v, g in zip(validators, m.groups()))

    @classmethod
    def is_trusted_backward_code(cls, code: Any) -> bool:
        """``True`` when *code* may be executed as a recovery step.

        Trusted means: generated in this process, or matching the
        recovery-code template of a loaded transformation class.
        """
        if not isinstance(code, str) or not code:
            return False
        with _data_transformation._TRUST_LOCK:
            if code in _data_transformation._KNOWN_BACKWARD_CODES:
                return True
        return any(
            sub._matches_backward_code(code) for sub in list(_data_transformation.REGISTRY.values())
        )

    @abstractmethod
    def forward(self, data: Any) -> Any: ...

    @abstractmethod
    def backward(self, data: Any) -> Any: ...


class TransformationSequence(BaseModel):
    """Sequential transformation pipeline.

    Holds an ordered list of :class:`_data_transformation` instances.

    - :meth:`forward` -- applies the transformations left-to-right
      and returns ``(transformed_data, inverse_codes_in_reverse)``.
      The inverse-code list is what the constitution will replay at
      recovery time, so it is built in reverse order on the spot.
    - :meth:`append` -- mutate the pipeline (append one or many),
      returning ``self`` for chaining.
    - Iteration / repr -- standard niceties.
    - An empty pipeline is the identity: ``forward(x)`` returns
      ``(x, [])`` and the constitution simply returns ``x`` on the
      way back.

    Pre-built sequences are exported from :mod:`laila.entry`
    (``transformation_base64``, ``transformation_base64_compression``,
    ``transformation_base64_compression_encryption``,
    ``transformation_encryption``) for the common pool defaults.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    transformations: list[_data_transformation] = Field(default_factory=list)

    def forward(self, data: Any) -> Any:
        """Apply every contained transformation in order and collect their inverses.

        For each transformation ``t`` in :attr:`transformations` (in
        declaration order):

        1. Replace ``current`` with ``t.forward(current)``.
        2. Append ``t.backward_code`` to the inverse-code list.

        At the end, the inverse-code list is reversed so that
        recovery at the constitution layer can simply replay the
        codes top-to-bottom.

        Parameters
        ----------
        data : Any
            Input value -- whatever shape the first transformation
            in the pipeline expects.

        Returns
        -------
        tuple[Any, list[str]]
            ``(transformed_value, inverse_code_snippets)`` where the
            second element is already reversed for replay.
        """
        current = data
        inverse_codes: list[str] = []

        for transformation in self.transformations:
            current = transformation.forward(current)
            inverse_codes.append(transformation.backward_code)

        return current, inverse_codes[::-1]

    def __iter__(self):
        """Iterate over the contained transformations."""
        return iter(self.transformations)

    def append(self, t) -> Any:
        """Append one or more transformations to the pipeline.

        Parameters
        ----------
        t : _data_transformation or list[_data_transformation]
            Transformation(s) to append.

        Returns
        -------
        TransformationSequence
            ``self``, for chaining.

        Raises
        ------
        TypeError
            If *t* is not a valid transformation or list thereof.
        """
        if isinstance(t, _data_transformation):
            self.transformations.append(t)
            return self

        if isinstance(t, list):
            for _t in t:
                if not isinstance(_t, _data_transformation):
                    raise TypeError("All elements must be _data_transformation instances.")
                self.transformations.append(_t)
            return self

        raise TypeError("append expects a _data_transformation or list of them.")

    def __repr__(self) -> str:
        """Return a human-readable pipeline summary."""
        if not self.transformations:
            return f"{self.__class__.__name__}(identity)"
        names = " -> ".join(getattr(t, "name", t.__class__.__name__) for t in self.transformations)
        return f"{self.__class__.__name__}({names})"
