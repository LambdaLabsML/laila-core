"""Static extraction of the Python ``laila`` API surface (no import side effects).

This walks the Python ``laila`` source tree with :mod:`ast` and collects the
symbols the Mirror Law cares about:

- every ``class`` definition (name + base-class names + the file it lives in),
  so base/internal classes like ``_LAILA_IDENTIFIABLE_OBJECT`` are visible at
  *every* level, not just the public leaf API;
- public method names per class;
- top-level facade verbs and properties declared in the package ``__init__.py``
  (``constant``/``memorize``/``read_args``/...);
- the ``Default*`` aliases declared in ``macros/defaults.py``.

We deliberately use static analysis instead of ``import laila`` because importing
the package auto-initializes a policy, touches ``~/.laila``, and otherwise has
side effects we do not want in a conformance checker.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass, field
from pathlib import Path

# Directories under the repo root that are NOT the importable library surface.
_EXCLUDED_DIRS = {
    "laila-c",
    "tests",
    "tutorials",
    "docs",
    "examples",
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "build",
}


@dataclass
class PyClass:
    name: str
    bases: list[str]
    file: str
    methods: list[str] = field(default_factory=list)


@dataclass
class PySurface:
    classes: dict[str, PyClass]
    facade_functions: set[str]
    facade_properties: set[str]
    facade_imports: set[str]
    default_aliases: set[str]

    def all_callable_names(self) -> set[str]:
        names: set[str] = set(self.facade_functions)
        names |= self.facade_properties
        names |= self.facade_imports
        for cls in self.classes.values():
            names.update(cls.methods)
        return names

    def all_symbol_names(self) -> set[str]:
        return (
            set(self.classes)
            | self.all_callable_names()
            | self.default_aliases
        )


def _base_name(node: ast.expr) -> str:
    """Best-effort dotted/simple name for a base-class expression."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):  # e.g. Generic[T]
        return _base_name(node.value)
    return ast.unparse(node) if hasattr(ast, "unparse") else "<expr>"


def _public_methods(cls_node: ast.ClassDef) -> list[str]:
    out: list[str] = []
    for item in cls_node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = item.name
            # Keep public names and the dunders that form part of the surface.
            if not name.startswith("_") or name in {"__iter__", "__getitem__", "__len__", "__lshift__", "__rshift__"}:
                out.append(name)
    return out


def _walk_python_files(library_root: Path):
    for dirpath, dirnames, filenames in os.walk(library_root):
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDED_DIRS]
        for fn in filenames:
            if fn.endswith(".py"):
                yield Path(dirpath) / fn


def _parse_init(init_path: Path) -> tuple[set[str], set[str], set[str]]:
    """Return (top-level def names, @property names, names imported into laila)."""
    funcs: set[str] = set()
    props: set[str] = set()
    imports: set[str] = set()
    if not init_path.exists():
        return funcs, props, imports
    tree = ast.parse(init_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                funcs.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported = alias.asname or alias.name
                if not imported.startswith("_") and imported != "*":
                    imports.add(imported)
        elif isinstance(node, ast.ClassDef):
            # The module facade class may expose properties (laila.memory, ...).
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    is_property = any(
                        isinstance(d, ast.Name) and d.id == "property"
                        for d in item.decorator_list
                    )
                    if is_property and not item.name.startswith("_"):
                        props.add(item.name)
    return funcs, props, imports


def _parse_defaults(defaults_path: Path) -> set[str]:
    aliases: set[str] = set()
    if not defaults_path.exists():
        return aliases
    tree = ast.parse(defaults_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.startswith("Default"):
                    aliases.add(target.id)
    return aliases


def extract(repo_root: Path) -> PySurface:
    library_root = repo_root
    classes: dict[str, PyClass] = {}

    for path in _walk_python_files(library_root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = str(path.relative_to(repo_root))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes[node.name] = PyClass(
                    name=node.name,
                    bases=[_base_name(b) for b in node.bases],
                    file=rel,
                    methods=_public_methods(node),
                )

    funcs, props, imports = _parse_init(repo_root / "__init__.py")
    aliases = _parse_defaults(repo_root / "macros" / "defaults.py")

    return PySurface(
        classes=classes,
        facade_functions=funcs,
        facade_properties=props,
        facade_imports=imports,
        default_aliases=aliases,
    )


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    surface = extract(root)
    print(f"classes: {len(surface.classes)}")
    print(f"facade functions: {sorted(surface.facade_functions)}")
    print(f"facade properties: {sorted(surface.facade_properties)}")
    print(f"default aliases: {sorted(surface.default_aliases)}")
