"""Record model -- decorates an :class:`Entry` with provenance metadata for storage.

A :class:`Record` is what actually lands in a pool. It wraps an entry
with three pieces of provenance:

- **recorder** -- the policy gid that originated the write (defaults
  to the active policy at construction time);
- **borrower** -- the policy gid that requested the entry on behalf of
  someone else (``None`` unless a caller sets it; reserved for future
  attribution flows);
- **record_timestamp** -- ISO-8601 UTC timestamp of when the record
  was constructed.

Pools never see raw entries; they always see (and persist) a
serialized :class:`Record`. On read, :meth:`Record._build_async` /
:meth:`Record._build_sync` strip the envelope and re-hydrate the
inner entry via the registered scope-specific builder.
"""

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .....entry.compdata.transformation import TransformationSequence


class Record(BaseModel):
    """Immutable wrapper pairing an :class:`Entry` with recorder/borrower metadata.

    Construct with ``Record(entry=...)``; the recorder defaults to
    the active policy's ``global_id`` and the timestamp is captured
    at construction time. Use :meth:`serialize` to turn the record
    into the on-disk dict shape (which embeds the entry's serialized
    form), and :meth:`_build_sync` / :meth:`_build_async` to invert
    that on read.
    """

    entry: Any
    recorder: str | None = None
    borrower: str | None = None
    record_timestamp: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(timespec="milliseconds")
    )

    # ``extra="forbid"`` so a misspelled provenance kwarg (``creator=``)
    # fails loudly instead of being silently dropped.
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    def model_post_init(self, __context: Any) -> None:
        """Default the recorder to the active policy's gid when not provided.

        ``borrower`` is left exactly as given (``None`` unless a caller
        attributes the write to another policy).
        """
        super().model_post_init(__context)
        from ..... import active_policy

        if self.recorder is None:
            self.recorder = active_policy.global_id

    def serialize(self, transformations: TransformationSequence) -> str:
        """Serialize the record into a pool-storable dict.

        The wrapped entry is run through :meth:`Entry.serialize` with
        the provided *transformations* (which is also the pool's
        ``transformations`` attribute) and the resulting dict replaces
        the ``entry`` slot in the record. The pool then persists this
        whole dict atomically.

        Parameters
        ----------
        transformations : TransformationSequence
            The pool's transformation pipeline.

        Returns
        -------
        dict
            A pool-storable dict with ``entry`` replaced by its
            serialized form.
        """
        record_as_dict = self.as_dict
        record_as_dict["entry"] = self.entry.serialize(transformations=transformations)

        return record_as_dict

    @property
    def entry_id(self) -> str:
        """Return the global ID of the wrapped entry."""
        if hasattr(self.entry, "global_id"):
            return self.entry.global_id

        if isinstance(self.entry, Mapping):
            return self.entry["_global_id"]

        raise TypeError(
            "Record.entry must expose a `global_id` or be a serialized entry mapping with "
            f"`_global_id`; got {type(self.entry).__name__}"
        )

    @property
    def as_dict(
        self,
    ) -> dict:
        """Return a plain dict representation, preserving the raw entry object."""
        data = self.model_dump()

        # Since it model_dumps entry in a weird way
        data["entry"] = self.entry
        return data

    @classmethod
    def from_dict(cls, in_dict: Mapping):
        """Construct a Record from the dict shape produced by :attr:`as_dict`.

        ``entry`` may be a live :class:`Entry` or its ``to_dict`` /
        serialized-dict form, in which case it is rebuilt through
        :meth:`Entry.from_dict`. ``recorder``, ``borrower`` and
        ``record_timestamp`` are passed through when present.

        Raises
        ------
        TypeError
            *in_dict* is not a mapping.
        ValueError
            *in_dict* has no ``entry`` slot.
        """
        if not isinstance(in_dict, Mapping):
            raise TypeError(f"Record.from_dict expects a mapping, got {type(in_dict).__name__}")
        if "entry" not in in_dict:
            raise ValueError("Record.from_dict requires an 'entry' slot")

        entry = in_dict["entry"]
        if isinstance(entry, Mapping):
            from .....entry.entry import Entry

            entry = Entry.from_dict(dict(entry))

        kwargs: dict[str, Any] = {"entry": entry}
        for name in ("recorder", "borrower", "record_timestamp"):
            if in_dict.get(name) is not None:
                kwargs[name] = in_dict[name]
        return cls(**kwargs)

    @classmethod
    def _build_sync(cls, record: Any) -> dict:
        """Synchronously hydrate a record dict from a JSON string or raw dict.

        Dispatches to the registered builder for the entry's scope via
        :func:`build_by_scope` (sync path), which returns a fully
        hydrated :class:`Entry`. Mutates *record* in place to replace
        the serialized ``entry`` field with the live entry, and
        returns the same record.

        Returning a dict (rather than a fresh :class:`Record`) is
        intentional: callers in the read pipeline only need the inner
        entry; the metadata fields (recorder/borrower/timestamp) are
        read directly off the dict for logging and audit.
        """
        if isinstance(record, str):
            record = json.loads(record)

        from .....entry.constitution.build_maps import build_by_scope

        record["entry"] = build_by_scope(record["entry"], asynchronous=False)
        return record

    @classmethod
    async def _build_async(cls, record: Any) -> dict:
        """Async variant of :meth:`_build_sync`.

        Awaits the registered builder for the entry's scope (async path),
        allowing nested manifest fetches to yield the loop instead of
        blocking it.
        """
        if isinstance(record, str):
            record = json.loads(record)

        from .....entry.constitution.build_maps import build_by_scope

        record["entry"] = await build_by_scope(record["entry"], asynchronous=True)
        return record

    @classmethod
    def _build(cls, record: Any, *, asynchronous: bool = False):
        """Router: dispatch to :meth:`_build_async` or :meth:`_build_sync`."""
        if asynchronous:
            return cls._build_async(record)
        return cls._build_sync(record)
