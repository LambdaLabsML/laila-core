#!/usr/bin/env python3
"""Mirror-law conformance checker for the Rust port (Layer A: surface parity).

Python ``laila`` and ``laila-rs`` are the same API in two languages: one
Python module <-> one Rust source file (``a/b/c.py`` <-> ``src/a/b/c.rs``,
``a/b/__init__.py`` <-> ``src/a/b/mod.rs``, the root ``__init__.py`` <->
``src/lib.rs``), one class <-> one struct / trait / class descriptor, one
function or method <-> one ``fn`` of the same name (an ``Args``-taking one
carrying ``#[pysig("name(params)")]``, the Python signature verbatim), one
pydantic field <-> the field a class descriptor declares. Names are kept
exactly: ``memorize`` is ``memorize``, ``_read`` is ``_read``,
``__getitem__`` is ``__getitem__`` (``r#type`` for a Rust keyword).

This script walks the Python tree (``extract_py``) and the crate
(``extract_rs``) and reports:

- ``MODULE``    a Python module with no Rust twin file;
- ``CLASS``     a Python class the twin file does not declare;
- ``FUNCTION``  a module-level ``def`` the twin file has no ``fn`` for;
- ``METHOD``    a method with no ``fn`` (or name-dispatched string) in the
  twin file or the twin files of the class's Python bases;
- ``FIELD``     a pydantic field the twin files neither declare nor read;
- ``ASSIGN``    a module-level constant / alias with no twin item;
- ``SIGNATURE`` a ``#[pysig]`` whose parameter list differs from the Python
  ``def`` (names, order, ``*`` / ``*args`` / ``**kwargs``, defaults);
- ``INVENTED``  a ``#[pysig]`` function whose name Python never had.

Three language-forced mappings are built in rather than waived: a package's
namesake module (``entry/entry.py``, ``x/schema/base.py``) folds into the
nearest ``mod.rs`` that declares its symbols; a Python exception class is
one ``ExcKind`` variant in ``python/errors.rs``; the closed-set class
families the port keeps as one handle with a kind (the futures, the policy,
central memory) have their members' methods in the host file (``HOSTED_IN``).
``#[pysig]``s in the Rust-only runtime packages (``python::``, ``_compat::``,
``_codecs::`` -- shims of CPython and third-party libraries) are never
INVENTED.

Leading-underscore (non-dunder) names are implementation details whose
decomposition is language-forced (asyncio loop callbacks become threads,
``PrivateAttr`` state becomes struct fields); a missing twin for one is
reported as ``INTERNAL`` -- advisory, listed by ``--internals``, never a
failure. Every public name must match exactly.

Documented deviations live in ``allow.txt`` next to this file (one
``KIND symbol  # reason`` per line, ``*`` globs allowed); anything else is a
violation and the exit code is non-zero so drift cannot be merged. Layer B
(the test suites, deep-eval suites and examples) is ``py2rs --check``: the
checked-in Rust is regenerated from the Python and must reproduce
byte-exactly (``.github/workflows/laila-rs.yml``).

Run from anywhere: ``python3 laila-rs/tools/conformance/check.py``
(``--verbose`` lists the waived deviations too, ``--internals`` the
advisory internals).
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import extract_py
import extract_rs

REPO_ROOT = HERE.parents[2]  # the ``laila`` package directory
RS_ROOT = HERE.parents[1]  # laila-rs/

# Python dunders / pydantic hooks that have no Rust twin by construction.
NO_TWIN = {
    "__init__",  # ``new`` / the class descriptor
    "__new__",
    "__init_subclass__",
    "__class_getitem__",
    "__get_pydantic_core_schema__",
    "__pydantic_init_subclass__",
    "__set_name__",
    "__reduce__",
    "__reduce_ex__",
    "__getstate__",
    "__setstate__",
    "__del__",
    "__weakref__",
    "__slots__",
    "__post_init__",
}

# Closed-set class families the port folds into one handle (an enum of
# kinds), as the plan's class model prescribes: the methods and fields of
# every member live in the host file, dispatched on the kind.
HOSTED_IN = {
    "_LAILA_IDENTIFIABLE_FUTURE": "policy.central.command.schema.future.future.future",
    "Future": "policy.central.command.schema.future.future.future",
    "ConcurrentPackageFuture": "policy.central.command.schema.future.future.future",
    "ProcessPackageFuture": "policy.central.command.schema.future.future.future",
    "ComplexFuture": "policy.central.command.schema.future.future.future",
    "GroupFuture": "policy.central.command.schema.future.future.future",
    "RemoteFuture": "policy.central.command.schema.future.future.future",
    "_LAILA_IDENTIFIABLE_POLICY": "policy",
    "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY": "policy.central.memory",
}


def is_internal(name: str) -> bool:
    """A leading-underscore, non-dunder name: an implementation detail.

    Internals are reported as ``INTERNAL`` (advisory; ``--internals`` lists
    them) rather than as violations: their decomposition is language-forced
    (asyncio loop callbacks become threads, ``PrivateAttr`` state becomes
    struct fields, generator helpers become closures), whereas every public
    name -- the API the Mirror Law protects -- must match exactly.
    """
    return name.startswith("_") and not (name.startswith("__") and name.endswith("__"))


# Rust-only packages: the Python object model (``python::``), the pydantic /
# numpy / stdlib compatibility shims (``_compat::``, ``_codecs::``). They
# mirror CPython and third-party libraries, not ``laila``, so a ``#[pysig]``
# there is never INVENTED.
RUNTIME_PACKAGES = {"python", "_compat", "_codecs"}

# Rust keywords a Python name may collide with: ``fn r#type`` keeps the name.
RUST_KEYWORDS = {
    "type",
    "match",
    "move",
    "ref",
    "loop",
    "mod",
    "use",
    "where",
    "struct",
    "trait",
    "impl",
    "self",
    "super",
    "crate",
    "in",
    "as",
    "fn",
    "let",
    "mut",
    "pub",
    "enum",
    "const",
    "static",
    "unsafe",
    "async",
    "await",
    "dyn",
    "box",
    "yield",
    "try",
    "macro",
}


@dataclass
class Violation:
    kind: str
    symbol: str
    detail: str


# --------------------------------------------------------------------------- #
# allow-list
# --------------------------------------------------------------------------- #


def load_allow(path: Path) -> dict[str, str]:
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


# --------------------------------------------------------------------------- #
# signatures
# --------------------------------------------------------------------------- #


def _params_of(sig_text: str) -> list[tuple[str, str, str]] | None:
    """``[(kind, name, default)]`` of a parameter list (``a, b=1, *c, d, **e``)."""
    try:
        tree = ast.parse(f"def __f({sig_text}): pass")
    except SyntaxError:
        return None
    fn = tree.body[0]
    assert isinstance(fn, ast.FunctionDef)
    a = fn.args
    out: list[tuple[str, str, str]] = []
    pos = list(a.posonlyargs) + list(a.args)
    first = len(pos) - len(a.defaults)
    for i, arg in enumerate(pos):
        out.append(("pos", arg.arg, ast.unparse(a.defaults[i - first]) if i >= first else ""))
    if a.vararg:
        out.append(("*", a.vararg.arg, ""))
    elif a.kwonlyargs:
        out.append(("*", "", ""))
    for arg, d in zip(a.kwonlyargs, a.kw_defaults):
        out.append(("kw", arg.arg, "" if d is None else ast.unparse(d)))
    if a.kwarg:
        out.append(("**", a.kwarg.arg, ""))
    return out


def _norm_default(text: str) -> str:
    # ``PydanticUndefined()`` (Rust spelling of the sentinel) == ``PydanticUndefined``
    return text.replace("()", "") if text.endswith("()") else text


def signatures_differ(py_sig: str, rs_sig: str) -> str | None:
    """A description of the first difference, or None when they agree."""
    p = _params_of(py_sig)
    r = _params_of(rs_sig)
    if p is None or r is None:
        return f"unparseable: python={py_sig!r} rust={rs_sig!r}"
    if len(p) != len(r):
        return f"{len(p)} python parameters vs {len(r)} in #[pysig]"
    for (pk, pn, pd), (rk, rn, rd) in zip(p, r):
        if (pk, pn) != (rk, rn):
            return f"parameter {pn or pk!r} vs {rn or rk!r}"
        if _norm_default(pd) != _norm_default(rd):
            return f"default of {pn!r}: python {pd} vs rust {rd}"
    return None


# --------------------------------------------------------------------------- #
# checks
# --------------------------------------------------------------------------- #


def rs_name(name: str) -> str:
    return name  # the port keeps every name; ``r#`` is stripped by the extractor


def check(
    verbose: bool = False,
) -> tuple[list[Violation], list[tuple[Violation, str]], list[Violation]]:
    py = extract_py.extract(REPO_ROOT)
    rs = extract_rs.extract(RS_ROOT)
    allow = load_allow(HERE / "allow.txt")
    used_allow: set[str] = set()

    violations: list[Violation] = []
    waived: list[tuple[Violation, str]] = []

    internals: list[Violation] = []

    def report(kind: str, symbol: str, detail: str) -> None:
        v = Violation(kind, symbol, detail)
        k = allowed_key(allow, kind, symbol)
        if k is not None:
            waived.append((v, allow[k]))
            used_allow.add(k)
        elif is_internal(symbol.rsplit(".", 1)[-1]):
            internals.append(Violation("INTERNAL", f"{kind} {symbol}", detail))
        else:
            violations.append(v)

    classes = py.classes()
    class_module = {c.name: c.module for c in classes.values()}
    exc_kinds = rs.exception_kinds()

    def twin_of(key: str) -> extract_rs.RsFile | None:
        """The Rust file standing for Python module *key*.

        ``src/<key>.rs`` (or ``<key>/mod.rs``); a package's *namesake* module
        (``entry/entry.py``, ``utils/args/args.py``, ``x/schema/base.py`` --
        the one module a Python package exists to hold) is folded into the
        ``mod.rs`` of the nearest enclosing package that declares one of its
        symbols when the crate has no separate file for it, as ``mod.rs`` is
        where Rust puts a module's own body.
        """
        f = rs.files.get(key)
        if f is not None:
            return f
        parts = key.split(".")
        if len(parts) < 2 or parts[-1] not in (parts[-2], "base"):
            return None
        mod = py.modules.get(key)
        names = {c.name for c in mod.classes} | set(mod.functions) if mod else set()
        cands = [".".join(parts[:i]) for i in range(len(parts) - 1, 0, -1)]
        found = [rs.files[c] for c in cands if c in rs.files]
        for g in found:
            if any(g.declares(n) for n in names):
                return g
        return found[0] if found else None

    def twin_files(cls: extract_py.PyClass) -> list[extract_rs.RsFile]:
        """The twin file of *cls* and, through inheritance, of every Python base."""
        out: list[extract_rs.RsFile] = []
        seen: list[str] = []
        stack = [cls.name]
        while stack:
            n = stack.pop(0)
            if n in seen or n not in classes:
                continue
            seen.append(n)
            for key in (class_module[n], HOSTED_IN.get(n)):
                f = twin_of(key) if key else None
                if f is not None and f not in out:
                    out.append(f)
            stack.extend(classes[n].bases)
        return out

    def is_exception(cls: extract_py.PyClass) -> bool:
        names = [cls.name, *cls.bases]
        return any(n.endswith(("Error", "Exception", "Warning")) for n in names)

    def hint(name: str) -> str:
        where = [k for k in rs.where_declared(name)]
        return f"; declared in {', '.join(where[:3])}" if where else ""

    for key in sorted(py.modules):
        mod = py.modules[key]
        if not mod.defines_anything():
            continue
        f = twin_of(key)
        if f is None:
            report("MODULE", key, f"{mod.path} has no Rust twin src/{key.replace('.', '/')}.rs")
            continue

        for fn in mod.functions:
            if fn in NO_TWIN:
                continue
            if fn not in f.fns and fn not in f.items:
                report("FUNCTION", f"{key}.{fn}", f"no fn {fn} in {f.path}{hint(fn)}")

        for name in mod.assignments:
            if name in f.items or name in f.fns or name in f.classes or name in f.strings:
                continue
            report(
                "ASSIGN",
                f"{key}.{name}",
                f"module-level '{name}' has no item / fn / string in {f.path}{hint(name)}",
            )

        for cls in mod.classes:
            if is_exception(cls) and cls.name in exc_kinds:
                continue  # one exception class <-> one ``ExcKind`` variant
            if not (f.declares(cls.name) or cls.name in f.strings):
                report(
                    "CLASS",
                    f"{key}.{cls.name}",
                    f"no struct / trait / descriptor {cls.name} in {f.path}{hint(cls.name)}",
                )
                continue
            files = twin_files(cls)
            for meth in cls.methods:
                if meth.name in NO_TWIN:
                    continue
                cands = [meth.name]
                if meth.kind == "setter":
                    cands = [meth.name, "set_" + meth.name, "__setattr__"]
                ok = any(g.declares(c) or c in g.strings for g in files for c in cands)
                if not ok:
                    report(
                        "METHOD",
                        f"{cls.name}.{meth.name}",
                        f"{meth.kind}: no fn / dispatched name {meth.name} in {', '.join(g.path for g in files)}{hint(meth.name)}",
                    )
            for fld in cls.fields:
                if not any(g.mentions(fld) for g in files):
                    report(
                        "FIELD",
                        f"{cls.name}.{fld}",
                        f"field '{fld}' neither declared nor read in {', '.join(g.path for g in files)}",
                    )

    # SIGNATURE / INVENTED: every #[pysig] against the Python def of that name
    # in the twin module (a method of one of the module's classes, or a
    # module-level function); the same name may carry several Python defs
    # (overridden in a subclass of the same module), any match is fine.
    folded: dict[str, list[extract_py.PyModule]] = {}
    for key, mod in py.modules.items():
        f = twin_of(key)
        if f is not None:
            folded.setdefault(f.key, []).append(mod)
    for cname, host in HOSTED_IN.items():
        mod = py.modules.get(class_module.get(cname, ""))
        if mod is not None and mod not in folded.setdefault(host, []):
            folded[host].append(mod)
    for key, f in sorted(rs.files.items()):
        mods = folded.get(key, [])
        for name, sigs in sorted(f.pysigs.items()):
            py_sigs: list[str] = []
            for mod in mods:
                if name in mod.signatures:
                    py_sigs.append(mod.signatures[name])
                for cls in mod.classes:
                    for meth in cls.methods:
                        if meth.name == name:
                            py_sigs.append(meth.signature)
                    if name == cls.name:
                        # a constructor pysig ``Cls(**kwargs)``: Python ``__init__``
                        for meth in cls.methods:
                            if meth.name == "__init__":
                                py_sigs.append(meth.signature)
            if not py_sigs:
                if key.split(".")[0] in RUNTIME_PACKAGES:
                    continue  # the object model / stdlib shims have no Python twin
                origin = [c for c in classes if c == name] or [
                    c.name
                    for m in py.modules.values()
                    for c in m.classes
                    for mm in c.methods
                    if mm.name == name
                ]
                if name in py.all_names() or origin:
                    continue  # the Python def lives in another module (inherited / re-exported)
                report(
                    "INVENTED",
                    f"{key}.{name}",
                    f"#[pysig] {sigs[0]!r} has no Python def of that name",
                )
                continue
            for s in sigs:
                rs_params = s.split("(", 1)[1].rsplit(")", 1)[0]
                rs_params = re.sub(
                    r"^\s*(self|cls)\s*(,\s*|$)", "", rs_params
                )  # a method's receiver
                diffs = [signatures_differ(p, rs_params) for p in py_sigs]
                if all(diffs):
                    report(
                        "SIGNATURE",
                        f"{key}.{name}",
                        f"#[pysig({s!r})] vs python ({py_sigs[0]}): {diffs[0]}",
                    )

    for k in allow:
        if k not in used_allow:
            violations.append(Violation("STALE", k, "allow.txt entry waives nothing -- remove it"))

    return violations, waived, internals


def _format(
    violations: list[Violation],
    waived: list[tuple[Violation, str]],
    internals: list[Violation],
    verbose: bool,
    show_internals: bool,
) -> str:
    lines: list[str] = []
    if verbose and waived:
        lines.append(f"Documented deviations ({len(waived)}):")
        for v, reason in waived:
            lines.append(f"  [{v.kind:9}] {v.symbol}: {reason}")
        lines.append("")
    if show_internals and internals:
        lines.append(
            f"Internal (leading-underscore) names without a same-named Rust twin -- advisory ({len(internals)}):"
        )
        for v in sorted(internals, key=lambda v: v.symbol):
            lines.append(f"  [{v.kind:9}] {v.symbol}")
            lines.append(f"              {v.detail}")
        lines.append("")
    if not violations:
        lines.append(
            f"Mirror law: OK -- Python surface == Rust surface ({len(waived)} documented deviations; {len(internals)} internals differ, see --internals)."
        )
        return "\n".join(lines)
    order = {
        "MODULE": 0,
        "CLASS": 1,
        "FUNCTION": 2,
        "METHOD": 3,
        "FIELD": 4,
        "ASSIGN": 5,
        "SIGNATURE": 6,
        "INVENTED": 7,
        "STALE": 8,
    }
    violations = sorted(violations, key=lambda v: (order.get(v.kind, 9), v.symbol))
    lines.append("Mirror law violations:\n")
    counts: dict[str, int] = {}
    for v in violations:
        counts[v.kind] = counts.get(v.kind, 0) + 1
        lines.append(f"  [{v.kind:9}] {v.symbol}")
        lines.append(f"              {v.detail}")
    summary = ", ".join(f"{k}={counts[k]}" for k in sorted(counts))
    lines.append("")
    lines.append(
        f"Total: {len(violations)} violation(s)  ({summary}); {len(waived)} documented deviations waived; {len(internals)} internals differ (advisory)"
    )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    verbose = "--verbose" in argv or "-v" in argv
    show_internals = "--internals" in argv
    violations, waived, internals = check(verbose)
    print(_format(violations, waived, internals, verbose, show_internals))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
