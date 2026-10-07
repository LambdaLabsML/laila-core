/**
 * Laila macros sub-package -- aliases, defaults, and scope strings.
 *
 * Catch-all home for *constants* and *factory helpers* that need to be
 * referenced from many places in laila but don't belong in any one domain
 * module:
 *
 * - ``./aliases.js`` -- short-form re-exports for the most-used public
 *   classes (e.g. ``DefaultPolicy``, ``Entry``).
 * - ``./defaults.js`` -- factory helpers that produce ready-to-use default
 *   policies / pools / taskforces, plus the ``LAILA_DEFAULT_DIRECTORIES``
 *   table that other modules consult for "where do I put my files?".
 * - ``./strings.js`` -- the canonical scope-string constants that show up in
 *   global ids and CLI-args paths (``_POLICY_SCOPE``, ``_FUTURE_SCOPE``,
 *   ``_POOL_SCOPE``, ...). Centralised here so a rename touches exactly one
 *   file.
 *
 * Like the Python package ``__init__``, this module exports nothing itself;
 * import the sub-modules directly.
 */

export {};
