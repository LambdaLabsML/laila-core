"""Static extraction of the Rust ``laila`` crate surface (tolerant token scan).

Walks ``laila-rs/laila/src/**/*.rs`` and records, per file, every name the
Python twin's symbols may be mirrored by:

- declared items: ``pub struct`` / ``pub trait`` / ``pub enum`` / ``pub type``
  / ``pub static`` / ``pub const``, and every ``fn`` (``pub`` or not: a
  Python method of a base class is a trait method the backend ``impl``
  provides without ``pub``);
- the class a descriptor declares: the first string argument of
  ``pool_class("Name", ..)`` / ``model_class("Name", ..)``, the name in
  ``laila_pool_handle!(Name, Backend)`` and ``py_model!(Name: Base, ..)``;
- every ``#[pysig("name(params)")]`` attribute (the machine-readable
  Python signature of an ``Args``-taking function);
- identifier-shaped string literals (``"file_path"``, ``"close"``): the
  field names a class descriptor declares, the method names a backend
  dispatches by name (``method_names`` / ``__call_method__`` arms) -- the
  dynamic half of the Python surface.

No ``cargo`` / ``syn``: a regex scan over the source text is enough for
presence checks and keeps the checker runnable anywhere Python runs.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

_ITEM = re.compile(
    r"\bpub(?:\([^)]*\))?\s+(?:unsafe\s+)?(struct|trait|enum|type|static|const)\s+([A-Za-z_][A-Za-z0-9_]*)"
)
_FN = re.compile(
    r"\b(pub(?:\([^)]*\))?\s+)?(?:async\s+|const\s+|unsafe\s+)*fn\s+(r#)?([A-Za-z_][A-Za-z0-9_]*)"
)
_PYSIG = re.compile(r"#\[pysig\(\s*\"((?:[^\"\\]|\\.)*)\"\s*,?\s*\)\]", re.S)
_DESC = re.compile(r"\b(?:pool_class|model_class|protocol_class)\(\s*\"([A-Za-z_][A-Za-z0-9_]*)\"")
_HANDLE = re.compile(r"laila_pool_handle!\(\s*(?:///[^\n]*\n\s*)*([A-Za-z_][A-Za-z0-9_]*)\s*,")
_PY_MODEL = re.compile(r"py_model!\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*:")
# any crate macro declaring the item named by its first argument
# (``cd_subclass!(CD_dict, ..)``, ``exc_class!(Name)``, ...); std macros excluded.
_MACRO_DECL = re.compile(
    r"\b([a-z_][a-z0-9_]*)!\s*[\(\{]\s*(?:#\[[^\]]*\]\s*)*(?:pub\s+)?([A-Z_][A-Za-z0-9_]*)\s*[,:\)\}]"
)
_STD_MACROS = {
    "vec",
    "format",
    "matches",
    "assert",
    "assert_eq",
    "assert_ne",
    "debug_assert",
    "write",
    "writeln",
    "println",
    "print",
    "eprintln",
    "eprint",
    "panic",
    "unreachable",
    "todo",
    "unimplemented",
    "cfg",
    "env",
    "concat",
    "stringify",
    "include_str",
    "include_bytes",
    "compile_error",
    "thread_local",
    "dbg",
    "line",
    "file",
    "column",
    "module_path",
    "option_env",
    "quote",
    "format_args",
    "lazy_static",
}
_PUB_USE = re.compile(r"\bpub(?:\([^)]*\))?\s+use\s+([^;]*);", re.S)
_USE_AS = re.compile(
    r"\bpub(?:\([^)]*\))?\s+use\s+[^;]*?\bas\s+(r#)?([A-Za-z_][A-Za-z0-9_]*)\s*[;,\}]"
)
_ENUM_BODY = re.compile(r"\bpub\s+enum\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{(.*?)\n\}", re.S)
_VARIANT = re.compile(r"^\s*([A-Z][A-Za-z0-9_]*)\s*(?:[,({=]|$)", re.M)
_STRING = re.compile(r"\"([A-Za-z_][A-Za-z0-9_]*)\"")
_LINE_COMMENT = re.compile(r"//[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)


@dataclass
class RsFile:
    key: str  # module key the file stands for (``entry.entry``, ``laila``)
    path: str  # relative to laila-rs/
    items: set[str] = field(default_factory=set)  # struct / trait / enum / type / static / const
    fns: set[str] = field(default_factory=set)  # every fn name
    pub_fns: set[str] = field(default_factory=set)
    classes: set[str] = field(default_factory=set)  # descriptor-declared class names
    pysigs: dict[str, list[str]] = field(default_factory=dict)  # name -> signature texts
    strings: set[str] = field(default_factory=set)  # identifier-shaped string literals
    variants: dict[str, set[str]] = field(default_factory=dict)  # pub enum -> its variants

    def declares(self, name: str) -> bool:
        return name in self.items or name in self.fns or name in self.classes

    def mentions(self, name: str) -> bool:
        return self.declares(name) or name in self.strings


@dataclass
class RsSurface:
    files: dict[str, RsFile]  # by module key

    def where_declared(self, name: str) -> list[str]:
        return sorted(k for k, f in self.files.items() if f.declares(name))

    def where_mentioned(self, name: str) -> list[str]:
        return sorted(k for k, f in self.files.items() if f.mentions(name))

    def exception_kinds(self) -> set[str]:
        """The variants of ``python::errors::ExcKind`` -- every Python exception
        class is one variant (``LailaError::new(ExcKind::X, msg)`` raises it)."""
        f = self.files.get("python.errors")
        return set(f.variants.get("ExcKind", set())) if f else set()

    def all_pysigs(self) -> dict[str, list[tuple[str, str]]]:
        """name -> [(module key, signature text)]"""
        out: dict[str, list[tuple[str, str]]] = {}
        for k, f in self.files.items():
            for name, sigs in f.pysigs.items():
                for s in sigs:
                    out.setdefault(name, []).append((k, s))
        return out


def _strip_comments(text: str) -> str:
    text = _BLOCK_COMMENT.sub("", text)
    return _LINE_COMMENT.sub("", text)


def module_key(rel: Path) -> str:
    """``src/a/b/c.rs`` -> ``a.b.c``; ``src/a/b/mod.rs`` -> ``a.b``; ``src/lib.rs`` -> ``laila``."""
    parts = list(rel.with_suffix("").parts)
    if parts and parts[0] == "src":
        parts = parts[1:]
    if parts and parts[-1] in ("mod", "lib"):
        parts = parts[:-1]
    return ".".join(parts) if parts else "laila"


def parse_file(path: Path, rel: Path) -> RsFile:
    raw = path.read_text(encoding="utf-8")
    f = RsFile(key=module_key(rel), path=str(rel))
    # pysigs first: the attribute text is a string literal, scanned on the raw source
    for m in _PYSIG.finditer(raw):
        sig = m.group(1).replace('\\"', '"')
        name = sig.split("(", 1)[0].strip()
        f.pysigs.setdefault(name, []).append(sig)
    text = _strip_comments(raw)
    for m in _ITEM.finditer(text):
        f.items.add(m.group(2))
    for m in _FN.finditer(text):
        f.fns.add(m.group(3))
        if m.group(1):
            f.pub_fns.add(m.group(3))
    for rx in (_DESC, _HANDLE, _PY_MODEL):
        for m in rx.finditer(text):
            f.classes.add(m.group(1))
    for m in _MACRO_DECL.finditer(text):
        if m.group(1) not in _STD_MACROS:
            f.classes.add(m.group(2))
    for m in _USE_AS.finditer(text):
        f.items.add(m.group(2))
    for m in _PUB_USE.finditer(text):
        # ``pub use a::b::{x, y as z, w::*};`` re-exports ``x``, ``z`` (``*`` ignored)
        body = m.group(1)
        inner = (
            body[body.index("{") + 1 : body.rindex("}")]
            if "{" in body
            else body.rsplit("::", 1)[-1]
        )
        for part in inner.split(","):
            part = part.strip()
            if not part or part.endswith("*"):
                continue
            name = part.split(" as ")[-1].strip() if " as " in part else part.rsplit("::", 1)[-1]
            name = name.removeprefix("r#").strip()
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                f.items.add(name)
    for m in _ENUM_BODY.finditer(text):
        f.variants[m.group(1)] = {v.group(1) for v in _VARIANT.finditer(m.group(2))}
    for m in _STRING.finditer(text):
        f.strings.add(m.group(1))
    return f


def extract(rs_root: Path) -> RsSurface:
    """*rs_root* is ``laila-rs/``; the crate is ``laila-rs/laila``."""
    crate = rs_root / "laila"
    files: dict[str, RsFile] = {}
    for dirpath, dirnames, filenames in os.walk(crate / "src"):
        dirnames[:] = sorted(dirnames)
        for fn in sorted(filenames):
            if not fn.endswith(".rs"):
                continue
            p = Path(dirpath) / fn
            rel = p.relative_to(crate)
            f = parse_file(p, rel)
            if f.key in files:
                # ``a/b.rs`` next to ``a/b/mod.rs`` cannot both exist in Rust;
                # keep the first and merge defensively.
                g = files[f.key]
                g.items |= f.items
                g.fns |= f.fns
                g.pub_fns |= f.pub_fns
                g.classes |= f.classes
                g.strings |= f.strings
                g.variants.update(f.variants)
                for k, v in f.pysigs.items():
                    g.pysigs.setdefault(k, []).extend(v)
            else:
                files[f.key] = f
    return RsSurface(files=files)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    s = extract(root)
    n_items = sum(len(f.items) for f in s.files.values())
    n_fns = sum(len(f.fns) for f in s.files.values())
    n_sigs = sum(len(v) for f in s.files.values() for v in f.pysigs.values())
    print(f"files: {len(s.files)}  items: {n_items}  fns: {n_fns}  pysigs: {n_sigs}")
    for key in sorted(s.files):
        f = s.files[key]
        print(
            f"  {key:60} items={len(f.items):3} fns={len(f.fns):4} classes={len(f.classes):2} pysigs={sum(len(v) for v in f.pysigs.values()):3}"
        )
