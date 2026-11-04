#!/usr/bin/env python3
"""Generate the facade alias packages that reproduce the Python import paths.

laila-go is one flat Go package (``laila``) because the Python package is one
big import cycle. Python code still spells ``from laila.entry import Entry``
or ``from laila.policy.central.communication import Channel``; this script
turns every Python *package* (a directory with ``__init__.py``) into a Go
package at the same path whose only content is aliases into the flat root:

    laila/entry/__init__.py       ->  laila-go/entry/entry.go        (package entry)
    laila/policy/central/memory/  ->  laila-go/policy/central/memory/memory.go

For each name the ``__init__.py`` binds (``from .x import A``, ``from .x
import A as B``, ``__all__``, top-level ``def`` / ``class`` / assignments,
also inside ``try: ... except ModuleNotFoundError``) the generator emits:

    type A = laila.A                         Python class  -> Go type
    var New_A = laila.New_A                  its constructor(s), ``A_class`` descriptor
    var A_member = laila.A_member            classmethods / enum members (``A_<x>``)
    var f = laila.F                          function  (generic functions cannot be
                                             aliased without instantiation -> skipped,
                                             listed in the file header)
    const C = laila.C / var V = laila.V      module-level constants / values

Directories whose Python name starts with ``_`` (``_carriers``) are emitted
as ``carriers_`` because ``go build ./...`` ignores ``_``-prefixed
directories; the package name follows the same rule.

Run from anywhere:  python3 laila-go/tools/facades/gen_facades.py [--check]
``--check`` regenerates into memory and exits 1 if any file on disk differs
(CI gate).
"""

from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
GO_ROOT = HERE.parent.parent
REPO_ROOT = GO_ROOT.parent
sys.path.insert(0, str(HERE.parent / "conformance"))

import extract_go
import extract_py
from check import goname

MODULE_PATH = "github.com/amirzadehlambda/laila/laila-go"
EXCLUDED_DIRS = set(extract_py.EXCLUDED_DIRS)

RE_GENERIC_FUNC = re.compile(r"^func\s+([A-Za-z_]\w*)\[", re.M)
RE_CONST_LINE = re.compile(r"^const\s+([A-Za-z_]\w*)\b", re.M)
RE_CONST_BLOCK = re.compile(r"^const\s*\((.*?)^\)", re.M | re.S)
RE_GENERIC_TYPE = re.compile(r"^type\s+([A-Za-z_]\w*)\[", re.M)
RE_BUILD_TAG = re.compile(r"^//go:build\s+(.*)$", re.M)
# package vars a facade must not copy: a copy of a patch point (``var X =
# func(...)``) or of mutable state (mutex, scalar) would silently diverge
# from the root -- ``postgres.PostgresPool_start_local_server_ = f`` would
# not patch what the root calls.
RE_VAR_DECL = re.compile(
    r"^(?:var\s+)?([A-Za-z_]\w*)\s*(?:[A-Za-z_][\w.\[\]*]*\s*)?=\s*(.*)$", re.M
)
RE_VAR_TYPED = re.compile(
    r"^\s*([A-Za-z_]\w*)\s+((?:\*?sync\.|\*?atomic\.|int|uint|bool|string|float)\S*)\s*$", re.M
)


def go_index(go_root: Path):
    """Surface + generic funcs + generic types + consts + build tags per file
    + package vars that must not be copied (patch points / mutable state)."""
    surface = extract_go.extract(go_root)
    generic_funcs: set[str] = set()
    generic_types: set[str] = set()
    consts: set[str] = set()
    tags: dict[str, str] = {}
    no_copy: dict[str, str] = {}
    for p in sorted(go_root.glob("*.go")):
        if p.name.endswith("_test.go"):
            continue
        text = p.read_text(encoding="utf-8")
        m = RE_BUILD_TAG.search(text)
        if m:
            tags[p.name] = m.group(1).strip()
        generic_funcs.update(RE_GENERIC_FUNC.findall(text))
        generic_types.update(RE_GENERIC_TYPE.findall(text))
        consts.update(RE_CONST_LINE.findall(text))
        for block in RE_CONST_BLOCK.findall(text):
            for line in block.splitlines():
                line = line.strip()
                if not line or line.startswith("//"):
                    continue
                m2 = re.match(r"([A-Za-z_]\w*)\b", line)
                if m2:
                    consts.add(m2.group(1))
        for m3 in RE_VAR_DECL.finditer(text):
            name, rhs = m3.group(1), m3.group(2).strip()
            if name not in surface.vars:
                continue
            if re.match(r"^func\(\)\s*\S+\s*\{$", rhs):
                continue  # ``var X = func() T { ... }()`` -- an initializer, not a func value
            if rhs.startswith("func(") or rhs.startswith("func ("):
                no_copy[name] = "patch point (var holding a func)"
            elif re.match(r'^(-?\d|"|`|true\b|false\b|nil\b)', rhs):
                no_copy[name] = "mutable package state"
        for m4 in RE_VAR_TYPED.finditer(text):
            if m4.group(1) in surface.vars:
                no_copy[m4.group(1)] = f"mutable package state ({m4.group(2)})"
    return surface, generic_funcs, generic_types, consts, tags, no_copy


def _bound_names(tree: ast.Module) -> list[str]:
    """Names an ``__init__.py`` binds at import time (first-seen order)."""
    out: list[str] = []
    seen: set[str] = set()

    def add(n: str) -> None:
        if n in seen or n == "__all__" or n == "annotations" or n.startswith("__"):
            return
        seen.add(n)
        out.append(n)

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, ast.ImportFrom):
                # relative (``from .x import A``) or absolute ``laila.`` imports only
                if node.level == 0 and not (node.module or "").startswith("laila"):
                    continue
                for alias in node.names:
                    if alias.name == "*":
                        continue  # resolved through __all__ of the target below
                    add(alias.asname or alias.name)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                add(node.name)
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        add(t.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                add(node.target.id)
            elif isinstance(node, ast.Try):
                visit(node.body)
                for h in node.handlers:
                    visit(h.body)
                visit(node.orelse)
                visit(node.finalbody)
            elif isinstance(node, ast.If):
                visit(node.body)
                visit(node.orelse)

    visit(tree.body)
    # ``__all__`` entries (covers ``from .x import *`` re-exports)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            if isinstance(node.value, (ast.List, ast.Tuple)):
                for elt in node.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        add(elt.value)
    return out


def star_targets(tree: ast.Module, pkg_dir: Path) -> list[Path]:
    """``from .x import *`` sources (resolved to files) for __all__ expansion."""
    out: list[Path] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and any(a.name == "*" for a in node.names)
            and node.level
        ):
            base = pkg_dir
            for _ in range(node.level - 1):
                base = base.parent
            mod = (node.module or "").split(".")
            p = base.joinpath(*mod)
            if p.with_suffix(".py").exists():
                out.append(p.with_suffix(".py"))
            elif (p / "__init__.py").exists():
                out.append(p / "__init__.py")
    return out


def go_dir_name(py_name: str) -> str:
    return py_name[1:] + "_" if py_name.startswith("_") else py_name


def packages(repo_root: Path) -> list[Path]:
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = sorted(
            d for d in dirnames if d not in EXCLUDED_DIRS and not d.startswith(".")
        )
        d = Path(dirpath)
        if d == repo_root:
            continue
        if "__init__.py" not in filenames:
            dirnames[:] = []
            continue
        out.append(d)
    return out


def render(pkg: Path, names: list[str], idx) -> tuple[str | None, list[str], list[str]]:
    surface, generic_funcs, generic_types, consts, tags, no_copy = idx
    rel = pkg.relative_to(REPO_ROOT)
    parts = [go_dir_name(p) for p in rel.parts]
    pkgname = parts[-1]
    lines: list[str] = []
    skipped: list[str] = []
    missing: list[str] = []
    used_tags: set[str] = set()

    def files_of(table: dict[str, set[str]], name: str) -> set[str]:
        return table.get(name, set())

    def tag_of(files: set[str]) -> str | None:
        t = {tags[f] for f in files if f in tags}
        return next(iter(t)) if len(t) == 1 and len(files) == 1 else None

    emitted: set[str] = set()

    def emit(kind: str, g: str, files: set[str]) -> None:
        if g in emitted:
            return
        emitted.add(g)
        t = tag_of(files)
        if t:
            used_tags.add(g)
            skipped.append(f"{g} (//go:build {t}; import the root package under that tag)")
            return
        if kind == "type":
            if g in generic_types:
                skipped.append(f"{g} (generic type)")
                return
            lines.append(f"type {g} = laila.{g}")
        elif kind == "func":
            if g in generic_funcs:
                skipped.append(f"{g} (generic function)")
                return
            lines.append(f"var {g} = laila.{g}")
        elif kind == "const":
            lines.append(f"const {g} = laila.{g}")
        else:
            if g in no_copy:
                skipped.append(f"{g} ({no_copy[g]}; read/patch laila.{g} directly)")
                return
            lines.append(f"var {g} = laila.{g}")

    all_go = surface.all_names()
    for n in names:
        g = goname(n)
        if g is None:
            continue
        if surface.has_type(g):
            emit("type", g, files_of(surface.types, g))
            # constructor(s), class descriptor, classmethods / enum members
            for cand in sorted(all_go):
                if (
                    cand == f"New_{g}"
                    or cand == f"{g}_class"
                    or (cand.startswith(g.rstrip("_") + "_") and cand != g)
                ):
                    if cand in surface.types:
                        continue  # a sibling type (e.g. Future vs Future_callback) is aliased on its own
                    if cand in consts:
                        emit("const", cand, files_of(surface.vars, cand))
                    elif cand in surface.funcs:
                        emit("func", cand, files_of(surface.funcs, cand))
                    else:
                        emit("var", cand, files_of(surface.vars, cand))
        elif g in consts:
            emit("const", g, files_of(surface.vars, g))
        elif g in surface.funcs:
            emit("func", g, files_of(surface.funcs, g))
        elif g in surface.vars:
            emit("var", g, files_of(surface.vars, g))
        else:
            missing.append(n)

    if not lines:
        return None, skipped, missing

    header = [
        "// Code generated by tools/facades/gen_facades.py; DO NOT EDIT.",
        "",
        f"// Package {pkgname} is the Go twin of the Python package `laila.{'.'.join(rel.parts)}`:",
        "// every name its `__init__.py` binds is re-exported here as an alias into the",
        f"// flat root package, so `from laila.{'.'.join(rel.parts)} import X` becomes",
        f'// `import "{MODULE_PATH}/{"/".join(parts)}"` + `{pkgname}.X`.',
    ]
    if skipped:
        header.append("//")
        header.append("// Not aliased (use the root package directly):")
        for s_ in skipped:
            header.append(f"//   - {s_}")
    if missing:
        header.append("//")
        header.append("// Bound by __init__.py but with no Go twin (documented deviation):")
        for m_ in missing:
            header.append(f"//   - {m_}")
    body = (
        "\n".join(header)
        + f'\npackage {pkgname}\n\nimport laila "{MODULE_PATH}"\n\n'
        + "\n".join(lines)
        + "\n"
    )
    return gofmt(body), skipped, missing


def gofmt(src: str) -> str:
    """Format through ``gofmt`` when one is on PATH (CI and dev boxes); the
    generator's own output is already gofmt-clean modulo blank-line grouping."""
    import shutil
    import subprocess

    exe = shutil.which("gofmt") or next(
        (
            str(p)
            for p in (Path.home() / ".local/go/bin/gofmt", Path("/usr/local/go/bin/gofmt"))
            if p.exists()
        ),
        None,
    )
    if exe is None:
        return src
    r = subprocess.run([exe], input=src, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else src


def generate() -> dict[Path, str]:
    idx = go_index(GO_ROOT)
    out: dict[Path, str] = {}
    for pkg in packages(REPO_ROOT):
        tree = ast.parse((pkg / "__init__.py").read_text(encoding="utf-8"))
        names = _bound_names(tree)
        for src in star_targets(tree, pkg):
            names += [
                n
                for n in _bound_names(ast.parse(src.read_text(encoding="utf-8")))
                if n not in names
            ]
        body, _, _ = render(pkg, names, idx)
        if body is None:
            continue
        rel = pkg.relative_to(REPO_ROOT)
        parts = [go_dir_name(p) for p in rel.parts]
        out[GO_ROOT.joinpath(*parts) / f"{parts[-1]}.go"] = body
    return out


def main(argv: list[str]) -> int:
    check = "--check" in argv
    files = generate()
    stale: list[Path] = []
    for path, body in sorted(files.items()):
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == body:
            continue
        if check:
            stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
            print(f"wrote {path.relative_to(GO_ROOT)}")
    # facades on disk that no Python package produces any more
    for p in GO_ROOT.rglob("*.go"):
        if (
            p.name.endswith("_test.go")
            or p in files
            or GO_ROOT / "py" in p.parents
            or GO_ROOT / "tests" in p.parents
        ):
            continue
        if (
            GO_ROOT / "drivers" in p.parents
            or GO_ROOT / "examples" in p.parents
            or GO_ROOT / "tools" in p.parents
        ):
            continue
        if p.parent == GO_ROOT:
            continue
        if p.read_text(encoding="utf-8").startswith(
            "// Code generated by tools/facades/gen_facades.py"
        ):
            if check:
                stale.append(p)
            else:
                p.unlink()
                print(f"removed {p.relative_to(GO_ROOT)}")
    if check and stale:
        print("facades out of date (run python3 laila-go/tools/facades/gen_facades.py):")
        for p in stale:
            print(f"  {p.relative_to(GO_ROOT)}")
        return 1
    print(f"{len(files)} facade packages {'up to date' if check else 'generated'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
