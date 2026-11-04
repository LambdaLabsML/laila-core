"""Static extraction of the Python ``laila`` API surface (no import side effects).

Walks the Python ``laila`` source tree with :mod:`ast` and records, per
module, everything the Go port must mirror file-for-file:

- the module itself (``a/b/c.py`` -> Go ``a.b.c.go``; a package
  ``a/b/__init__.py`` that defines anything -> ``a.b.go``; the root
  ``__init__.py`` -> ``laila.go``);
- every top-level ``def`` / ``async def`` (public *and* private -- the port
  keeps private names under the trailing-underscore rule);
- every ``class`` with its bases, its methods (with their kind: instance,
  classmethod, staticmethod, property getter / setter) and its annotated
  class-level fields (pydantic fields -> Go struct tags ``py:"name"``);
- module-level assignments of aliases / constants (``DefaultPolicy = ...``,
  ``ERR_X = ...``).

Static analysis instead of ``import laila`` because importing the package
auto-initializes a policy, touches ``~/.laila`` and has other side effects a
conformance checker must not trigger.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass, field
from pathlib import Path

# Directories under the repository root that are NOT the importable library.
EXCLUDED_DIRS = {
    "laila-c",
    "laila-js",
    "laila-go",
    "laila-rs",
    "tests",
    "tutorials",
    "docs",
    "examples",
    "agentic",
    "hooks",
    "vault",
    "site",
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "build",
    "laila_core.egg-info",
}


@dataclass
class PyMethod:
    name: str
    kind: str  # instance | classmethod | staticmethod | property | setter
    lineno: int


@dataclass
class PyClass:
    name: str
    bases: list[str]
    module: str
    methods: list[PyMethod] = field(default_factory=list)
    fields: list[str] = field(default_factory=list)  # annotated class attributes
    classvars: list[str] = field(default_factory=list)  # ClassVar[...] annotations
    lineno: int = 0


@dataclass
class PyModule:
    key: str  # dotted, e.g. "entry.compdata.taxonomy.cd_numpy" or "laila"
    path: str  # relative to the repo root
    functions: list[str] = field(default_factory=list)
    classes: list[PyClass] = field(default_factory=list)
    assignments: list[str] = field(default_factory=list)

    def defines_anything(self) -> bool:
        return bool(self.functions or self.classes or self.assignments)


@dataclass
class PySurface:
    modules: dict[str, PyModule]

    def classes(self) -> dict[str, PyClass]:
        out: dict[str, PyClass] = {}
        for m in self.modules.values():
            for c in m.classes:
                out[c.name] = c
        return out

    def all_names(self) -> set[str]:
        names: set[str] = set()
        for m in self.modules.values():
            names.update(m.functions)
            names.update(m.assignments)
            for c in m.classes:
                names.add(c.name)
                names.update(meth.name for meth in c.methods)
                names.update(c.fields)
                names.update(c.classvars)
        return names


def _base_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):  # Generic[T]
        return _base_name(node.value)
    return ast.unparse(node)


def _decorator_names(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    out = []
    for d in fn.decorator_list:
        if isinstance(d, ast.Name):
            out.append(d.id)
        elif isinstance(d, ast.Attribute):
            out.append(ast.unparse(d))
        elif isinstance(d, ast.Call):
            out.append(ast.unparse(d.func))
    return out


def _method_kind(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    decos = _decorator_names(fn)
    if "staticmethod" in decos:
        return "staticmethod"
    if "classmethod" in decos:
        return "classmethod"
    if "property" in decos or any(
        d in ("cached_property", "functools.cached_property") for d in decos
    ):
        return "property"
    if any(d.endswith(".setter") for d in decos):
        return "setter"
    if any(d.endswith(".deleter") for d in decos):
        return "deleter"
    return "instance"


def _is_classvar(annotation: ast.expr | None) -> bool:
    if annotation is None:
        return False
    text = ast.unparse(annotation)
    return text.startswith("ClassVar") or text.startswith("typing.ClassVar")


def _parse_class(node: ast.ClassDef, module: str) -> PyClass:
    cls = PyClass(
        name=node.name, bases=[_base_name(b) for b in node.bases], module=module, lineno=node.lineno
    )
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            cls.methods.append(PyMethod(item.name, _method_kind(item), item.lineno))
        elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            if _is_classvar(item.annotation):
                cls.classvars.append(item.target.id)
            else:
                cls.fields.append(item.target.id)
    return cls


def _module_key(rel: Path) -> str:
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else "laila"


def _walk(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            d for d in dirnames if d not in EXCLUDED_DIRS and not d.startswith(".")
        )
        # only packages (directories with __init__.py) are part of the library
        if Path(dirpath) != root and not (Path(dirpath) / "__init__.py").exists():
            dirnames[:] = []
            continue
        for fn in sorted(filenames):
            if fn.endswith(".py"):
                yield Path(dirpath) / fn


def extract(repo_root: Path) -> PySurface:
    modules: dict[str, PyModule] = {}
    for path in _walk(repo_root):
        rel = path.relative_to(repo_root)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        key = _module_key(rel)
        mod = PyModule(key=key, path=str(rel))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                mod.functions.append(node.name)
            elif isinstance(node, ast.ClassDef):
                mod.classes.append(_parse_class(node, key))
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id not in ("__all__", "__version__"):
                        mod.assignments.append(target.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                mod.assignments.append(node.target.id)
        modules[key] = mod
    return PySurface(modules=modules)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    surface = extract(root)
    n_funcs = sum(len(m.functions) for m in surface.modules.values())
    n_classes = sum(len(m.classes) for m in surface.modules.values())
    n_methods = sum(len(c.methods) for m in surface.modules.values() for c in m.classes)
    print(
        f"modules: {len(surface.modules)}  functions: {n_funcs}  classes: {n_classes}  methods: {n_methods}"
    )
    for key in sorted(surface.modules):
        m = surface.modules[key]
        if m.defines_anything():
            print(
                f"  {key:60} funcs={len(m.functions):3} classes={len(m.classes):2} assigns={len(m.assignments):3}"
            )
