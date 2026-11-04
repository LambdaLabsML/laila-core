#!/usr/bin/env python3
"""Mirror-law conformance checker for the Go port (surface parity, exhaustive).

Python ``laila`` and ``laila-go`` are the same API in two languages: one
Python module <-> one Go file, one class <-> one struct, one function / method
<-> one func / method, every name mapped by the single mechanical rule in
``laila-go/README.md``:

- capitalise the first letter, keep snake_case (``memorize`` -> ``Memorize``);
- leading underscores move to the end (``_read`` -> ``Read_``,
  ``_LAILA_IDENTIFIABLE_POLICY`` -> ``LAILA_IDENTIFIABLE_POLICY_``);
- dunders drop both underscores (``__getitem__`` -> ``Getitem``,
  ``__str__`` -> ``String``); ``__init__`` is the ``New_<Class>`` constructor;
- classmethods / staticmethods may be ``<Class>_<name>`` functions
  (``Entry.constant`` -> ``Entry_constant``), properties are ``X()`` /
  ``Set_x()`` methods, and a method may live on the embedded base struct the
  Python class inherits from (Go embedding promotes it).

This script walks the Python tree (``extract_py``) and the Go flat package
(``extract_go``) and reports every Python symbol without a Go twin
(``MISSING``) and every exported Go name Python never had (``INVENTED``).
Documented deviations live in ``allow.txt`` next to this file (one
``kind symbol  # reason`` per line); anything else is a violation and the
exit code is non-zero so drift cannot be merged.

Run from anywhere: ``python3 laila-go/tools/conformance/check.py``
(``--verbose`` lists allowed deviations too).
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import extract_go
import extract_py

REPO_ROOT = HERE.parents[2]  # the ``laila`` package directory
GO_ROOT = HERE.parents[1]  # laila-go/

DUNDER = {
    "__str__": "String",
    "__repr__": "Repr",
    "__init__": None,  # constructor: New_<Class>
    "__new__": None,
    "__init_subclass__": None,
    "__class_getitem__": None,
    "__get_pydantic_core_schema__": None,
    "__pydantic_init_subclass__": None,
    "__set_name__": None,
    "__reduce__": None,
    "__reduce_ex__": None,
    "__getstate__": None,
    "__setstate__": None,
    "__del__": None,
    "__weakref__": None,
    "__slots__": None,
}


def goname(name: str) -> str | None:
    """The Go identifier for a Python name under the port's rule (None = no
    Go twin is expected)."""
    if name.startswith("__") and name.endswith("__") and len(name) > 4:
        if name in DUNDER:
            return DUNDER[name]
        core = name[2:-2]
        return core[0].upper() + core[1:]
    core = name.lstrip("_")
    if not core:
        return None
    k = len(name) - len(core)
    return core[0].upper() + core[1:] + "_" * k


def pyname_candidates(go: str) -> set[str]:
    """Python names that would map to Go identifier *go* (inverse of goname)."""
    core = go.rstrip("_")
    k = len(go) - len(core)
    if not core:
        return set()
    out = {"_" * k + core, "_" * k + core[0].lower() + core[1:]}
    if k == 0:
        out.add("__" + core[0].lower() + core[1:] + "__")
    return out


def go_file_for(module_key: str) -> str:
    return "laila.go" if module_key == "laila" else module_key + ".go"


@dataclass
class Violation:
    kind: str
    symbol: str
    detail: str


# --------------------------------------------------------------------------- #
# allow-list: documented deviations (laila-go/tools/conformance/allow.txt)
# --------------------------------------------------------------------------- #


def load_allow(path: Path) -> dict[str, str]:
    """``{"KIND symbol": reason}`` from allow.txt; ``*`` globs allowed in symbol."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        body, _, reason = line.partition("#")
        parts = body.split()
        if len(parts) != 2:
            raise SystemExit(f"allow.txt: expected 'KIND symbol  # reason', got: {raw!r}")
        out[f"{parts[0]} {parts[1]}"] = reason.strip()
    return out


def allowed_key(allow: dict[str, str], kind: str, symbol: str) -> str | None:
    key = f"{kind} {symbol}"
    if key in allow:
        return key
    for k in allow:
        akind, _, pat = k.partition(" ")
        if (
            akind == kind
            and "*" in pat
            and re.fullmatch(re.escape(pat).replace("\\*", ".*"), symbol)
        ):
            return k
    return None


def allowed(allow: dict[str, str], kind: str, symbol: str) -> str | None:
    k = allowed_key(allow, kind, symbol)
    return None if k is None else allow[k]


# --------------------------------------------------------------------------- #
# checks
# --------------------------------------------------------------------------- #

# Go top-level names that are port conventions rather than Python symbols:
# constructors / class descriptors / typed accessors, the driver split
# (``pip install laila-core[extra]`` -> ``drivers/<extra>`` modules binding
# through ``*_driver`` interfaces, ``Register_driver`` / ``Driver_available``)
# and the hook interfaces that stand in for Python's virtual dispatch.
CONVENTION_PREFIXES = (
    "New_",
    "As_",
    "Is_",
    "Register_",
    "Unregister_",
    "Registered_",
    "Driver_",
    "Available_",
)
CONVENTION_SUFFIXES = (
    "_class",
    "_driver",
    "_hooks",
    "_client",
    "_connection",
    "_collection",
    "_kwargs",
    "_config",
)


def _is_exception(cls: extract_py.PyClass, classes: dict[str, extract_py.PyClass]) -> bool:
    seen: set[str] = set()
    stack = list(cls.bases)
    while stack:
        b = stack.pop()
        if b in seen:
            continue
        seen.add(b)
        if b.endswith("Error") or b.endswith("Exception") or b == "BaseException":
            return True
        if b in classes:
            stack.extend(classes[b].bases)
    return False


def check(verbose: bool = False) -> tuple[list[Violation], list[tuple[Violation, str]]]:
    py = extract_py.extract(REPO_ROOT)
    go = extract_go.extract(GO_ROOT)
    allow = load_allow(HERE / "allow.txt")
    used_allow: set[str] = set()

    violations: list[Violation] = []
    waived: list[tuple[Violation, str]] = []

    def report(kind: str, symbol: str, detail: str) -> None:
        v = Violation(kind, symbol, detail)
        reason = allowed(allow, kind, symbol)
        if reason is None:
            violations.append(v)
        else:
            waived.append((v, reason))
            used_allow.add(allowed_key(allow, kind, symbol))

    classes = py.classes()
    go_class_names = {c.name: goname(c.name) for c in classes.values()}

    # Receivers a Python class's methods may legitimately live on in Go: the
    # class itself and (through embedding) every Python base in the tree.
    def receivers(cls: extract_py.PyClass) -> list[str]:
        seen: list[str] = []
        stack = [cls.name]
        while stack:
            n = stack.pop(0)
            if n in seen or n not in classes:
                continue
            seen.append(n)
            stack.extend(classes[n].bases)
        return [go_class_names[n] for n in seen if go_class_names.get(n)]

    for key in sorted(py.modules):
        mod = py.modules[key]
        if not mod.defines_anything():
            continue
        gofile = go_file_for(key)
        if gofile not in go.files:
            report("MODULE", key, f"{mod.path} has no Go twin {gofile}")
            continue

        for fn in mod.functions:
            g = goname(fn)
            if g is None:
                continue
            if not (go.has_func(g) or go.has_type(g)):
                where = " (exists as a method)" if go.any_method(g) else ""
                report("FUNCTION", f"{key}.{fn}", f"no Go func/var {g}{where}")

        for name in mod.assignments:
            g = goname(name)
            if g is None:
                continue
            if go.has_name(g):
                continue
            # laila-go is one flat package: module-private constants that
            # collide across modules carry a ``_<module>`` / ``<Class>_``
            # disambiguator (``INSTALL_HINT_amqp_``, ``Constitution_REGISTRY_``,
            # ``NOT_IMPLEMENTED_lora_``, ``STREAM_HEADER_LEN``).
            stem = g.rstrip("_")
            modname = key.rsplit(".", 1)[-1]
            disamb = {f"{stem}_{modname}_", f"{stem}_{modname}"}
            disamb |= {f"{t}_{g}" for t in go.types}
            if disamb & go.all_names() or any(
                n.startswith(stem + "_") and n != g for n in go.all_names()
            ):
                continue
            report("ASSIGN", f"{key}.{name}", f"module-level '{name}' has no Go var/func/type {g}")

        for cls in mod.classes:
            gc = goname(cls.name)
            if key == "laila" and cls.name == "_LailaModule":
                # ``sys.modules[__name__].__class__ = _LailaModule``: its
                # properties are module attributes (``laila.alpha_pool``),
                # so each one is a package-level ``X()`` / ``Set_x()``.
                for meth in cls.methods:
                    g = goname(meth.name)
                    if g is None:
                        continue
                    if meth.kind == "setter":
                        cands = ["Set_" + g[0].lower() + g[1:], "Set_" + g]
                    else:
                        cands = [g, "Get_" + g[0].lower() + g[1:]]
                    if not any(go.has_func(c) for c in cands):
                        report(
                            "FUNCTION",
                            f"laila.{meth.name}",
                            f"module property: no Go func {' / '.join(cands)}",
                        )
                continue
            if gc is not None and gc in go.vars and _is_exception(cls, classes):
                continue  # exception classes are ``var X = py.NewType(...)`` values
            if gc is None or not go.has_type(gc):
                report("CLASS", f"{key}.{cls.name}", f"no Go type {gc}")
                continue
            recvs = receivers(cls)
            for meth in cls.methods:
                g = goname(meth.name)
                if g is None:
                    continue
                candidates = [g]
                if meth.kind == "setter":
                    candidates = ["Set_" + g[0].lower() + g[1:], "Set_" + g]
                elif meth.name.startswith("__") and meth.name.endswith("__"):
                    candidates = [g, g + "_"]
                ok = any(go.has_method(r, c) for r in recvs for c in candidates)
                # abstract Python methods (``raise NotImplementedError``) become
                # hook-interface methods the concrete structs implement
                ok = ok or any(go.any_iface_method(c) for c in candidates)
                if not ok and meth.kind in ("classmethod", "staticmethod"):
                    # ``Entry.constant`` -> ``Entry_constant``; ``Manifest._apply_indexes``
                    # -> ``Manifest_apply_indexes_``; or a package func taking ``cls``.
                    lower = g[0].lower() + g[1:]
                    ok = any(
                        go.has_func(f"{r}_{lower}")
                        or go.has_func(f"{r}_{g}")
                        or go.has_func(f"{r.rstrip('_')}_{lower}")
                        or go.has_func(f"{r.rstrip('_')}_{g}")
                        for r in recvs
                    )
                    ok = ok or go.has_func(g)
                if not ok:
                    elsewhere = sorted(
                        r for r, ms in go.methods.items() if any(c in ms for c in candidates)
                    )
                    hint = f"; defined on {', '.join(elsewhere[:4])}" if elsewhere else ""
                    report(
                        "METHOD",
                        f"{cls.name}.{meth.name}",
                        f"{meth.kind}: no Go method {gc}.{'/'.join(candidates)}{hint}",
                    )
            for fld in cls.fields:
                g = goname(fld)
                tagged = fld in go.all_tags
                plain = any(
                    g in go.struct_fields.get(r, ()) for r in recvs
                )  # dataclass-style structs
                if (
                    not tagged
                    and not plain
                    and not any(go.has_method(r, g) or go.has_method(r, g + "_") for r in recvs)
                ):
                    report(
                        "FIELD",
                        f"{cls.name}.{fld}",
                        f'no Go struct tag py:"{fld}" and no accessor {gc}.{g}()',
                    )

    # INVENTED: exported Go top-level names (funcs / types / vars) without a
    # Python origin and outside the port's constructor / class-descriptor /
    # typed-accessor conventions.
    py_names = py.all_names()
    go_top = set(go.funcs) | set(go.types) | set(go.vars)
    # ``Entry_constant`` and, for ``X_`` (Python ``_X``) classes, ``X_member``
    type_prefixes = tuple(sorted({t.rstrip("_") + "_" for t in go.types}, key=len, reverse=True))
    py_names.add("__version__")
    for g in sorted(go_top):
        if not g[0].isupper() or g.endswith("_"):
            # lowercase = unexported; trailing underscore = the Go spelling of a
            # Python-private helper (module-local code Python keeps inline)
            continue
        if any(g.startswith(p) for p in CONVENTION_PREFIXES) or any(
            g.endswith(s) for s in CONVENTION_SUFFIXES
        ):
            continue
        if pyname_candidates(g) & py_names:
            continue
        if g == "Version":
            continue  # ``__version__``
        # ``laila.active_policy = p`` / ``laila.logger = l`` setters
        if g.startswith("Set_") and pyname_candidates(g[4:]) & py_names:
            continue
        # ``<Class>_<member>`` forms (classmethods, enum members, class-level
        # helpers of a known class)
        if "_" in g and g.startswith(type_prefixes):
            continue
        report("INVENTED", g, "exported Go name with no Python origin")

    # An allow.txt line that no longer waives anything is a stale deviation
    # record: the port caught up (delete the line) or the symbol was renamed.
    for k in allow:
        if k not in used_allow:
            violations.append(Violation("STALE", k, "allow.txt entry waives nothing -- remove it"))

    return violations, waived


def _format(violations: list[Violation], waived: list[tuple[Violation, str]], verbose: bool) -> str:
    lines: list[str] = []
    if verbose and waived:
        lines.append(f"Documented deviations ({len(waived)}):")
        for v, reason in waived:
            lines.append(f"  [{v.kind:8}] {v.symbol}: {reason}")
        lines.append("")
    if not violations:
        lines.append(
            f"Mirror law: OK -- Python surface == Go surface ({len(waived)} documented deviations)."
        )
        return "\n".join(lines)
    order = {
        "MODULE": 0,
        "CLASS": 1,
        "FUNCTION": 2,
        "METHOD": 3,
        "FIELD": 4,
        "ASSIGN": 5,
        "INVENTED": 6,
        "STALE": 7,
    }
    violations = sorted(violations, key=lambda v: (order.get(v.kind, 9), v.symbol))
    lines.append("Mirror law violations:\n")
    counts: dict[str, int] = {}
    for v in violations:
        counts[v.kind] = counts.get(v.kind, 0) + 1
        lines.append(f"  [{v.kind:8}] {v.symbol}")
        lines.append(f"             {v.detail}")
    summary = ", ".join(f"{k}={counts[k]}" for k in sorted(counts))
    lines.append("")
    lines.append(
        f"Total: {len(violations)} violation(s)  ({summary}); {len(waived)} documented deviations waived"
    )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    verbose = "--verbose" in argv or "-v" in argv
    violations, waived = check(verbose)
    print(_format(violations, waived, verbose))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
