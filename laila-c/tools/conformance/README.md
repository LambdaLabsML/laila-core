# Mirror Law conformance harness

Python `laila` and generic `laila-C` are meant to be the **same API in two
languages** -- identical symbols, identical logic/process/order, identical
arguments, at *every* level of the class hierarchy. The only permitted
differences are pure language syntax (`.` -> `->`/`::`, `import` -> `#include`,
kwargs -> `*Opts` structs / overloads, object -> `shared_ptr` factory of the
same name, value/blocking property `X` -> method `X()`).

This harness mechanically enforces that invariant. Scope: **Python => generic
laila-C** only (the `py2c` hop). The generic-C => architecture-C (`c2target`)
hop is out of scope.

## Layers

- **Layer A -- surface + hierarchy parity** (`check.py`, implemented): compares
  the symbol/class surfaces of Python `laila` and `laila-c/include/laila/**`,
  *including base/internal classes* (`_LAILA_IDENTIFIABLE_OBJECT`,
  `_LAILA_CLI_CAPABLE_CLASS`, the `_LAILA_IDENTIFIABLE_*` family). Flags:
  - `RENAMED`  -- Python symbol exists in C under a different name
    (e.g. `_LAILA_IDENTIFIABLE_OBJECT` -> `Identifiable`).
  - `MISSING`  -- Python symbol has no C twin (e.g. `guarantee`, `read_args`,
    `DefaultTCPIPProtocol`, `bound_port`).
  - `INVENTED` -- public C symbol Python never had (e.g. `listen`, `poll`).
- **Layer B -- per-tutorial sequence/order conformance** (planned): translate
  every notebook in `tutorials/` and assert the ordered laila-symbol sequence of
  the C twin matches the Python source (names + order; arg names vs the C
  declaration). Deterministic golden gate + optional token-gated `py2c` LLM lint.

## Usage

```bash
python laila-c/tools/conformance/check.py     # exit != 0 on any violation
```

Inspect the two extractors directly:

```bash
python laila-c/tools/conformance/extract_py.py   # Python surface summary
python laila-c/tools/conformance/extract_c.py    # laila-c header surface summary
```

## Files

- `extract_py.py` -- AST walk of the Python `laila` tree (no `import laila`, so no
  side effects). Collects classes (+ bases), public methods, facade verbs/
  properties, and `Default*` aliases.
- `extract_c.py` -- tolerant regex/token scan of `include/laila/**.hpp`. Collects
  class/struct names (+ bases), `LAILA_DECLARE_PROTOCOL` classes, `using`
  aliases, and a generous set of callable identifiers.
- `check.py` -- the Layer A checker + report + CLI exit code.

## First-version caveats

- The anchor set in `check.py` is **curated, not exhaustive** -- it covers the
  full identity hierarchy plus the violations found so far, and is intended to
  grow until it spans the entire surface.
- `extract_c.py` over-approximates callable names (a token scan, not a C++
  parser). That is safe for presence/absence checks; libclang is a future
  upgrade.
- Layer B (tutorial translation conformance) is not yet implemented here.
