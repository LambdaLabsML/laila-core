"""Exceptions raised by the ``Entry`` subsystem."""

from __future__ import annotations


class EntryNotBuiltError(RuntimeError):
    """Raised when ``Entry.data`` is accessed on an unbuilt entry.

    An entry is *unbuilt* when it has a constitution attached but no
    payload yet. The user must explicitly materialize it (e.g. via
    ``laila.build(entry).wait()`` or ``laila.remember(...)``) before
    its data can be read.
    """
