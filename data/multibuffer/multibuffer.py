"""MultiBuffer -- an integer-indexed ring of records with independent read/write heads.

Where a pool is a *map* keyed by entry ``global_id``, a
:class:`MultiBuffer` is a *list*: a fixed number of slots addressed by
integer index, plus two cursors that walk those slots modulo the
capacity. It is the container a microcontroller puts in front of a
device that produces data faster than it can be persisted -- the
canonical example being a camera with a double (or triple) frame
buffer.

Value contract (shared with pools)
----------------------------------
- ``buf[i] = value`` stores a
  :class:`~laila.policy.central.memory.record.record.Record`. A bare
  :class:`~laila.entry.entry.Entry` is wrapped as ``Record(entry=...)``
  on the way in, exactly as central memory does before a pool write;
  raw payloads are first lifted to a constant entry.
- ``buf[i]`` returns the bare :class:`Entry` (``record.entry``). Raw
  slot contents that were never passed through ``__setitem__`` --
  bytes deposited by hardware -- are wrapped into a constant entry on
  the way out. Empty slots read as ``None``.

Heads
-----
``write()`` targets ``_write_head`` and ``read()`` targets
``_read_head``; each call advances its own head by one, wrapping at
``capacity``. The heads are independent so a producer can run ahead
of the consumer by up to ``capacity`` slots (the usual double-buffer
hand-off), and both are only ever mutated under the instance's atomic
lock.

Mapped mode
-----------
When the buffer stands in for memory that *something else* fills --
a DMA engine, a camera driver, a shared ``bytearray`` -- construct it
with ``mapped=True`` and pass that memory as ``slots``. The list is
used as-is (never copied) so external writes are visible immediately.
In this mode ``write()`` does **not** go through ``__setitem__``: it
only advances the write head (optionally depositing a raw value
directly into the slot first). ``read()`` is unchanged -- it still
goes through ``__getitem__``, which is where the raw bytes at the
read head become an :class:`Entry`.

Typical microcontroller loop::

    frames = MultiBuffer(slots=dma_slots, mapped=True)
    entry = frames.read()        # raw bytes at _read_head -> Entry
    laila.memorize(entry)        # Record into the policy's pool
"""

from typing import Any

from pydantic import Field, PrivateAttr

from ...basics.definitions.cli_capable import CLIExempt
from ...macros.strings import _MULTI_BUFFER_SCOPE
from ..schema.data_container import _LAILA_IDENTIFIABLE_DATA_CONTAINER

_UNSET: Any = object()


class MultiBuffer(_LAILA_IDENTIFIABLE_DATA_CONTAINER):
    """Fixed-capacity ring of records with separate read and write heads.

    Attributes
    ----------
    capacity : int
        Number of slots. Defaults to ``2`` (a double buffer). When
        ``slots`` is supplied explicitly this is overwritten with
        ``len(slots)``.
    mapped : bool
        ``True`` when ``slots`` is externally-owned memory that is
        filled outside laila (see module docstring). Changes only what
        :meth:`write` does; reads always go through the item protocol.
    slots : list-like
        The backing storage. Sized to ``capacity`` with ``None`` when
        not provided. When provided it is used by reference, so a
        caller (or hardware) mutating the same object is observed by
        the buffer. Must support ``len``, integer ``__getitem__`` and
        ``__setitem__``.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_MULTI_BUFFER_SCOPE]))
    _read_head: int = PrivateAttr(default=0)
    _write_head: int = PrivateAttr(default=0)

    capacity: int = Field(default=2, ge=1)
    mapped: bool = Field(default=False)
    # Typed as Any (not list[Any]) so pydantic hands the caller's object
    # through untouched -- copying would break mapped mode.
    slots: Any = CLIExempt(default=None)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.slots is None:
            self.slots = [None] * self.capacity
            return
        try:
            n = len(self.slots)
        except TypeError as exc:
            raise TypeError("MultiBuffer.slots must be a sized, indexable sequence") from exc
        if n == 0:
            raise ValueError("MultiBuffer.slots must contain at least one slot")
        self.capacity = n

    # -------- Heads --------
    @property
    def read_head(self) -> int:
        """Index of the slot the next :meth:`read` will return."""
        return self._read_head

    @property
    def write_head(self) -> int:
        """Index of the slot the next :meth:`write` will fill."""
        return self._write_head

    def _index(self, key: Any) -> int:
        if isinstance(key, bool) or not isinstance(key, int):
            raise TypeError(f"MultiBuffer indices must be integers, not {type(key).__name__}")
        return key % self.capacity

    def __len__(self) -> int:
        return self.capacity

    # -------- Item protocol --------
    def __getitem__(self, key: int) -> Any | None:
        """Return the :class:`Entry` at slot *key* (modulo capacity), or ``None``.

        A stored :class:`Record` yields its ``entry``. Raw contents
        (anything that is neither a record nor an entry -- e.g. bytes
        written by mapped hardware) are wrapped into a constant entry.
        """
        from ...entry.entry import Entry
        from ...policy.central.memory.record.record import Record

        with self.atomic():
            value = self.slots[self._index(key)]

        if value is None:
            return None
        if isinstance(value, Record):
            return value.entry
        if isinstance(value, Entry):
            return value
        return Entry.constant(data=value)

    def __setitem__(self, key: int, value: Any) -> None:
        """Store a :class:`Record` at slot *key* (modulo capacity).

        Accepts a :class:`Record` (stored as-is), an :class:`Entry`
        (wrapped in a fresh record), a raw payload (lifted to a
        constant entry, then wrapped), or ``None`` to clear the slot.
        """
        from ...entry.entry import Entry
        from ...policy.central.memory.record.record import Record

        if value is None:
            record = None
        elif isinstance(value, Record):
            record = value
        elif isinstance(value, Entry):
            record = Record(entry=value)
        else:
            record = Record(entry=Entry.constant(data=value))

        with self.atomic():
            self.slots[self._index(key)] = record

    def empty(self) -> None:
        """Clear every slot and rewind both heads to slot ``0``."""
        with self.atomic():
            for i in range(self.capacity):
                self.slots[i] = None
            self._read_head = 0
            self._write_head = 0

    # -------- Verbs --------
    def write(self, value: Any = _UNSET) -> int:
        """Fill the slot at the write head, then advance the head.

        Unmapped: ``self[write_head] = value`` (so the slot ends up
        holding a :class:`Record`); *value* is required.

        Mapped: :meth:`__setitem__` is bypassed. If *value* is given it
        is deposited raw into the slot; if omitted the slot is assumed
        to have been filled externally and only the head moves.

        Returns
        -------
        int
            The index of the slot that was written.
        """
        with self.atomic():
            idx = self._write_head
            if self.mapped:
                if value is not _UNSET:
                    self.slots[idx] = value
            else:
                if value is _UNSET:
                    raise TypeError("MultiBuffer.write() requires a value when not mapped")
                self[idx] = value
            self._write_head = (idx + 1) % self.capacity
        return idx

    def read(self) -> Any | None:
        """Return the :class:`Entry` at the read head, then advance the head.

        Goes through :meth:`__getitem__`, so in mapped mode this is
        where raw accumulated bytes are wrapped into an entry. An
        empty slot yields ``None`` (the head still advances).
        """
        with self.atomic():
            idx = self._read_head
            entry = self[idx]
            self._read_head = (idx + 1) % self.capacity
        return entry
