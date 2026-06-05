"""Convenience aliases re-exported at the ``laila`` package level.

These short names are the public entry points users actually reach
for in everyday code:

- :func:`constant` -- alias for :meth:`Entry.constant`. Build an
  unversioned entry whose identity does not change as the payload is
  rewritten.
- :func:`variable` -- alias for :meth:`Entry.variable`. Build an
  evolution-tracking entry whose identity bumps on each rewrite.
- :func:`contingent` -- alias for :meth:`Entry.contingent`. Escape
  hatch that forwards raw keyword arguments straight to the
  :class:`Entry` constructor, bypassing the consistency checks done by
  :meth:`Entry.constant` / :meth:`Entry.variable`.
- :class:`future` -- alias for :class:`Future`. Lower-case spelling
  for the common case of "I want a Future-type annotation in user
  code".

Importing this module from ``laila/__init__.py`` re-exports all four
names, so end users can write ``laila.variable(...)`` /
``laila.constant(...)`` / ``laila.future`` directly.
"""

from ..entry import Entry
from ..policy.central.command.schema.future.future import Future

constant = Entry.constant
variable = Entry.variable
contingent = Entry.contingent

future = Future
