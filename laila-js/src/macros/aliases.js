/**
 * Convenience aliases re-exported at the ``laila`` package level.
 *
 * These short names are the public entry points users actually reach
 * for in everyday code:
 *
 * - ``constant`` -- alias for ``Entry.constant``. Build an
 *   unversioned entry whose identity does not change as the payload is
 *   rewritten.
 * - ``variable`` -- alias for ``Entry.variable``. Build an
 *   evolution-tracking entry whose identity bumps on each rewrite.
 * - ``contingent`` -- alias for ``Entry.contingent``. Escape
 *   hatch that forwards raw keyword arguments straight to the
 *   ``Entry`` constructor, bypassing the consistency checks done by
 *   ``Entry.constant`` / ``Entry.variable``.
 * - ``future`` -- alias for ``Future``. Lower-case spelling
 *   for the common case of "I want a Future-type annotation in user
 *   code".
 *
 * Re-exporting this module from ``laila/index.js`` exposes all four
 * names, so end users can write ``laila.variable(...)`` /
 * ``laila.constant(...)`` / ``laila.future`` directly.
 */

import { Entry } from "../entry/index.js";
import { Future } from "../policy/central/command/schema/future/future/future.js";

export const constant = Entry.constant.bind(Entry);
export const variable = Entry.variable.bind(Entry);
export const contingent = Entry.contingent.bind(Entry);

export const future = Future;
