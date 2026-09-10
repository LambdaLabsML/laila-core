"""Tolerant extraction of the ``laila-c`` public API surface from headers.

This is a regex/token scan (not a full C++ parser) over ``include/laila/**.hpp``.
It is intentionally generous: for membership questions ("does symbol X exist in
C?", "is invented symbol Y present?") an over-approximation of callable names is
fine and avoids the cost/fragility of libclang for a first version.

Collected:
- class/struct names and their base classes (``class X : public Y`` and the
  ``LAILA_DECLARE_PROTOCOL(Name, ...)`` macro form);
- ``using Alias = ...;`` type aliases (where ``Default*`` aliases would live);
- a generous set of callable identifiers (anything of the form ``name(``), used
  for presence/absence checks of methods and free functions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

_CLASS_RE = re.compile(
    r"\b(?:class|struct)\s+(\w+)\s*(?:final\s*)?(?::\s*(?P<bases>[^{]+))?\{",
    re.MULTILINE,
)
_BASE_RE = re.compile(r"(?:public|protected|private|virtual)\s+([\w:]+)")
_MACRO_PROTO_RE = re.compile(r"LAILA_DECLARE_PROTOCOL\(\s*(\w+)\s*,\s*(\w+)", re.MULTILINE)
_USING_RE = re.compile(r"\busing\s+(\w+)\s*=", re.MULTILINE)
_CALLABLE_RE = re.compile(r"\b([a-zA-Z_]\w*)\s*\(")

# Tokens that are never API names even though they match ``name(``.
_CALLABLE_STOPWORDS = {
    "if",
    "for",
    "while",
    "switch",
    "return",
    "sizeof",
    "static_cast",
    "reinterpret_cast",
    "const_cast",
    "dynamic_cast",
    "decltype",
    "noexcept",
    "explicit",
    "catch",
    "throw",
    "and",
    "or",
    "not",
}


@dataclass
class CClass:
    name: str
    bases: list[str]
    file: str


@dataclass
class CSurface:
    classes: dict[str, CClass]
    using_aliases: set[str]
    callables: set[str]
    files: list[str] = field(default_factory=list)

    def has(self, name: str) -> bool:
        return name in self.classes or name in self.using_aliases or name in self.callables


def _strip_block_comments(text: str) -> str:
    # Drop /* ... */ and // ... so comments don't pollute the token scan.
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"//[^\n]*", " ", text)
    return text


def extract(include_root: Path) -> CSurface:
    classes: dict[str, CClass] = {}
    using_aliases: set[str] = set()
    callables: set[str] = set()
    files: list[str] = []

    for path in sorted(include_root.rglob("*.hpp")):
        raw = path.read_text(encoding="utf-8")
        text = _strip_block_comments(raw)
        rel = str(path)
        files.append(rel)

        for m in _CLASS_RE.finditer(text):
            name = m.group(1)
            bases_raw = m.group("bases") or ""
            bases = _BASE_RE.findall(bases_raw)
            # Normalize namespace-qualified bases (hal::Connection -> Connection).
            bases = [b.split("::")[-1] for b in bases]
            classes[name] = CClass(name=name, bases=bases, file=rel)

        for m in _MACRO_PROTO_RE.finditer(text):
            name = m.group(1)
            classes.setdefault(name, CClass(name=name, bases=["CommProtocol"], file=rel))

        using_aliases.update(_USING_RE.findall(text))

        for m in _CALLABLE_RE.finditer(text):
            tok = m.group(1)
            if tok not in _CALLABLE_STOPWORDS:
                callables.add(tok)

    return CSurface(
        classes=classes,
        using_aliases=using_aliases,
        callables=callables,
        files=files,
    )


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]  # laila-c/
    surface = extract(root / "include" / "laila")
    print(f"headers: {len(surface.files)}")
    print(f"classes: {sorted(surface.classes)}")
    print(f"using aliases: {sorted(surface.using_aliases)}")
