"""Msgpack serialisation / deserialisation transformation.

``backward`` (and the emitted ``backward_code``) unpack with
``strict_map_key=False`` so that dict payloads keyed by ``int`` /
``float`` / ``bool`` -- which msgpack packs without complaint -- round-trip
instead of raising ``ValueError`` on the way back.

``forward`` packs with ``strict_types=True``: msgpack has no tuple type
and no notion of ``dict``/``list``/``int``/``str`` *subclasses*
(``OrderedDict``, ``IntEnum``, ``numpy.float64`` inside a dict, ...), and
the non-strict packer silently coerces them (a nested tuple comes back as
a list, a tuple key is lost, an ``OrderedDict`` becomes a plain dict). In
strict mode the packer raises ``TypeError`` for such values instead, and
:meth:`ComputationalData.serialize` falls back to the universal
:class:`PickleSerializer`, so what you memorize is exactly what you
remember. Payloads made of plain ``dict``/``list``/``str``/``bytes``/
``int``/``float``/``bool``/``None`` are unaffected and stay
language-neutral. Pass ``forward_kwargs={"strict_types": False}`` to opt
back into coercion for a specific transformation instance.
"""

import textwrap
from typing import Any

import msgpack

from ..base import _data_transformation


class MsgpackSerializer(_data_transformation):
    """Reversible msgpack serialiser for Python objects."""

    name: str = "msgpack"

    def model_post_init(self, __context: Any) -> None:
        """Build backward_code dynamically after model creation."""
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                import msgpack
                kwargs = {{"raw": False, "strict_map_key": False, **{self.backward_kwargs!r}}}
                return msgpack.unpackb(inp, **kwargs)
        """)

    def forward(self, inp: Any) -> bytes:
        """Serialize a Python object into bytes using msgpack.

        Parameters
        ----------
        inp : Any
            Object to serialize.

        Returns
        -------
        bytes
            Msgpack-encoded bytes.

        Raises
        ------
        TypeError
            For values msgpack cannot express faithfully (tuples,
            ``dict``/``list``/scalar subclasses, arbitrary objects).
        """
        kwargs = {"use_bin_type": True, "strict_types": True, **self.forward_kwargs}
        return msgpack.packb(inp, **kwargs)

    def backward(self, inp: bytes) -> Any:
        """Deserialize bytes back into a Python object using msgpack.

        Parameters
        ----------
        inp : bytes
            Msgpack-encoded bytes.

        Returns
        -------
        Any
            Deserialized Python object.
        """
        kwargs = {"raw": False, "strict_map_key": False, **self.backward_kwargs}
        return msgpack.unpackb(inp, **kwargs)
