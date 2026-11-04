"""Tolerant scan of the Go port's flat ``laila`` package (no Go toolchain needed).

Collects, from every ``laila-go/*.go`` file (the single flat package; ``py/``,
``drivers/``, ``tests/`` and ``tools/`` are not part of the mirrored
surface):

- top-level ``func Name(`` declarations;
- methods ``func (r *Recv) Name(`` / ``func (r Recv) Name(`` keyed by receiver
  type;
- ``type Name`` declarations (structs, aliases, interfaces, named types);
- ``var`` / ``const`` names (single-line and grouped blocks);
- struct field tags ``py:"name"`` keyed by the enclosing struct type.

A line-oriented regex scan is enough for presence / absence checks because
``gofmt``-style layout is used throughout the port (one declaration per
line at column 0).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

RE_FUNC = re.compile(r"^func\s+([A-Za-z_][A-Za-z0-9_]*)\s*[(\[]")
RE_METHOD = re.compile(
    r"^func\s+\(\s*[A-Za-z_][A-Za-z0-9_]*\s+\*?([A-Za-z_][A-Za-z0-9_]*)(?:\[[^\]]*\])?\s*\)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
RE_TYPE = re.compile(r"^type\s+([A-Za-z_][A-Za-z0-9_]*)")
RE_VAR = re.compile(r"^(?:var|const)\s+([A-Za-z_][A-Za-z0-9_]*)")
RE_BLOCK_OPEN = re.compile(r"^(?:var|const)\s*\(\s*$")
RE_BLOCK_NAME = re.compile(r"^\t([A-Za-z_][A-Za-z0-9_]*)\b")
RE_TYPE_BLOCK_OPEN = re.compile(r"^type\s*\(\s*$")
RE_TYPE_BLOCK_NAME = re.compile(r"^\t([A-Za-z_][A-Za-z0-9_]*)\s")
RE_STRUCT_OPEN = re.compile(r"^type\s+([A-Za-z_][A-Za-z0-9_]*)\s+struct\s*\{\s*$")
RE_IFACE_OPEN = re.compile(r"^type\s+([A-Za-z_][A-Za-z0-9_]*)\s+interface\s*\{\s*$")
RE_IFACE_METHOD = re.compile(r"^\t([A-Za-z_][A-Za-z0-9_]*)\s*\(")
RE_TAG = re.compile(r"`[^`]*\bpy:\"([^\",]+)")
RE_STRUCT_FIELD = re.compile(r"^\t([A-Za-z_][A-Za-z0-9_]*)(?:,\s*[A-Za-z_][A-Za-z0-9_]*)*\s+\S")


@dataclass
class GoSurface:
    files: set[str] = field(default_factory=set)
    funcs: dict[str, set[str]] = field(default_factory=dict)  # name -> files
    methods: dict[str, dict[str, set[str]]] = field(default_factory=dict)  # recv -> name -> files
    types: dict[str, set[str]] = field(default_factory=dict)
    vars: dict[str, set[str]] = field(default_factory=dict)
    tags: dict[str, set[str]] = field(default_factory=dict)  # struct -> py field names
    all_tags: set[str] = field(default_factory=set)
    iface_methods: dict[str, set[str]] = field(default_factory=dict)  # interface -> method names
    struct_fields: dict[str, set[str]] = field(default_factory=dict)  # struct -> Go field names

    def has_func(self, name: str) -> bool:
        return name in self.funcs or name in self.vars

    def has_method(self, recv: str, name: str) -> bool:
        return name in self.methods.get(recv, {})

    def any_method(self, name: str) -> bool:
        return any(name in ms for ms in self.methods.values())

    def any_iface_method(self, name: str) -> bool:
        return any(name in ms for ms in self.iface_methods.values())

    def has_type(self, name: str) -> bool:
        return name in self.types

    def has_name(self, name: str) -> bool:
        return self.has_func(name) or self.has_type(name) or self.any_method(name)

    def all_names(self) -> set[str]:
        return set(self.funcs) | set(self.vars) | set(self.types)


def _add(d: dict[str, set[str]], name: str, file: str) -> None:
    d.setdefault(name, set()).add(file)


def extract(go_root: Path) -> GoSurface:
    s = GoSurface()
    for fn in sorted(os.listdir(go_root)):
        if not fn.endswith(".go") or fn.endswith("_test.go"):
            continue
        s.files.add(fn)
        in_block: str | None = None  # "var" | "type" | "struct:<Name>"
        for raw in (go_root / fn).read_text(encoding="utf-8").splitlines():
            line = raw.rstrip()
            if in_block:
                if line == ")" or line == "}":
                    in_block = None
                    continue
                if in_block == "var":
                    m = RE_BLOCK_NAME.match(line)
                    if m:
                        _add(s.vars, m.group(1), fn)
                elif in_block == "type":
                    m = RE_TYPE_BLOCK_NAME.match(line)
                    if m:
                        _add(s.types, m.group(1), fn)
                elif in_block.startswith("struct:"):
                    m = RE_TAG.search(line)
                    if m:
                        s.tags.setdefault(in_block[7:], set()).add(m.group(1))
                        s.all_tags.add(m.group(1))
                    m = RE_STRUCT_FIELD.match(line)
                    if m:
                        for name in [m.group(1)] + re.findall(
                            r",\s*([A-Za-z_][A-Za-z0-9_]*)",
                            line.split("\t", 1)[1].split()[0] if False else "",
                        ):
                            s.struct_fields.setdefault(in_block[7:], set()).add(name)
                elif in_block.startswith("iface:"):
                    m = RE_IFACE_METHOD.match(line)
                    if m:
                        s.iface_methods.setdefault(in_block[6:], set()).add(m.group(1))
                continue
            m = RE_METHOD.match(line)
            if m:
                s.methods.setdefault(m.group(1), {}).setdefault(m.group(2), set()).add(fn)
                continue
            m = RE_FUNC.match(line)
            if m:
                _add(s.funcs, m.group(1), fn)
                continue
            m = RE_STRUCT_OPEN.match(line)
            if m:
                _add(s.types, m.group(1), fn)
                in_block = "struct:" + m.group(1)
                continue
            m = RE_IFACE_OPEN.match(line)
            if m:
                _add(s.types, m.group(1), fn)
                in_block = "iface:" + m.group(1)
                continue
            if RE_TYPE_BLOCK_OPEN.match(line):
                in_block = "type"
                continue
            m = RE_TYPE.match(line)
            if m:
                _add(s.types, m.group(1), fn)
                continue
            if RE_BLOCK_OPEN.match(line):
                in_block = "var"
                continue
            m = RE_VAR.match(line)
            if m:
                _add(s.vars, m.group(1), fn)
    return s


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    s = extract(root)
    n_methods = sum(len(ms) for ms in s.methods.values())
    print(
        f"files: {len(s.files)}  funcs: {len(s.funcs)}  types: {len(s.types)}  vars: {len(s.vars)}  methods: {n_methods}  tagged fields: {len(s.all_tags)}"
    )
