"""Mirror Law conformance checker (Layer A: surface + hierarchy parity).

Enforces the foundational invariant that Python ``laila`` and generic ``laila-C``
are the same API at *every* level. This first version implements Layer A: it
compares the symbol/class surfaces (including base/internal classes) and reports
three kinds of violation:

- RENAMED   -- a Python symbol exists in C under a different ("cleaner") name
              (e.g. ``_LAILA_IDENTIFIABLE_OBJECT`` -> ``Identifiable``).
- MISSING   -- a Python symbol has no C twin at all (e.g. ``guarantee``,
              ``read_args``, ``DefaultTCPIPProtocol``, ``bound_port``).
- INVENTED  -- a public C symbol Python never had (e.g. ``listen``, ``poll``).

Allowed mechanical differences (``.`` -> ``->``/``::``, kwargs -> ``*Opts``,
property -> ``X()``, ``import`` -> ``#include``, object -> ``shared_ptr``
factory of the same name) preserve the *name*, so a name-level check is the
right primitive: only renames/missing/invented are flagged.

Run: ``python laila-c/tools/conformance/check.py``  (exit code != 0 on violation)

NOTE (first version): the anchor set below is curated, not exhaustive. It covers
the violations surfaced so far plus the full identity hierarchy; it is meant to
grow until it spans the entire surface. Layer B (per-tutorial sequence/order
conformance) is a separate, planned module.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import extract_c
import extract_py

# --------------------------------------------------------------------------- #
# Anchors: Python symbols that MUST have an identically named generic-C twin.
# --------------------------------------------------------------------------- #

# Base/internal classes -- the "faithful at every level" requirement.
ANCHOR_BASE_CLASSES = [
    "_LAILA_IDENTIFIABLE_OBJECT",
    "_LAILA_CLI_CAPABLE_CLASS",
    "_LAILA_IDENTIFIABLE_POLICY",
    "_LAILA_IDENTIFIABLE_POOL",
    "_LAILA_IDENTIFIABLE_POOL_ROUTER",
    "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY",
    "_LAILA_IDENTIFIABLE_CENTRAL_COMMAND",
    "_LAILA_IDENTIFIABLE_COMMUNICATION",
    "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
    "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
]

# Public leaf classes a user names directly.
ANCHOR_PUBLIC_CLASSES = [
    "Entry",
    "Manifest",
    "FilesystemPool",
    "S3Pool",
]

# Top-level facade verbs/properties a user writes as ``laila.X(...)`` / ``laila.X``.
ANCHOR_FACADE = [
    "constant",
    "variable",
    "memorize",
    "remember",
    "forget",
    "build",
    "read_args",
    "manifest",
    "guarantee",
    "add_peer",
    "memory",
    "communication",
    "command",
    "peers",
    "args",
]

# Default* aliases (macros/defaults.py) that must exist as C aliases too.
ANCHOR_DEFAULTS = [
    "DefaultPolicy",
    "DefaultPool",
    "DefaultTCPIPProtocol",
    "DefaultTCPProtocol",
]

# Protocol-level members the serving twin requires (Python protocol attributes).
ANCHOR_PROTOCOL_MEMBERS = [
    "bound_port",
    "peer_secret_key",
    "add_connection",
]

# Manifest surface (the blueprint Entry API a user touches).
ANCHOR_MANIFEST_MEMBERS = [
    "keys",
    "realized",
    "blueprint",
    "global_ids",
    "remember",
]

# When a Python anchor is absent by name in C, this annotates the likely rename
# so the report is actionable. Value is the current C name.
KNOWN_RENAMES = {
    "_LAILA_IDENTIFIABLE_OBJECT": "Identifiable",
    "_LAILA_IDENTIFIABLE_POLICY": "Policy",
    "_LAILA_IDENTIFIABLE_POOL": "Pool",
    "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY": "CentralMemory",
    "_LAILA_IDENTIFIABLE_CENTRAL_COMMAND": "CentralCommand",
    "_LAILA_IDENTIFIABLE_COMMUNICATION": "CentralCommunication",
    "_LAILA_IDENTIFIABLE_COMM_PROTOCOL": "CommProtocol",
    "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL": "TCPIPProtocol",
    "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL": "TCPProtocol",
}

# Public C symbols that must NOT exist (Python has no such symbol).
INVENTED_C = ["listen", "poll", "listen_port", "set_peer_secret_key", "RamPool"]


@dataclass
class Violation:
    kind: str  # RENAMED | MISSING | INVENTED
    symbol: str
    detail: str


def _c_has(c: extract_c.CSurface, name: str) -> bool:
    return c.has(name)


def check(repo_root: Path) -> list[Violation]:
    py = extract_py.extract(repo_root)
    c = extract_c.extract(repo_root / "laila-c" / "include" / "laila")

    py_symbols = py.all_symbol_names()
    violations: list[Violation] = []

    anchors = (
        [("base class", n) for n in ANCHOR_BASE_CLASSES]
        + [("public class", n) for n in ANCHOR_PUBLIC_CLASSES]
        + [("facade", n) for n in ANCHOR_FACADE]
        + [("Default alias", n) for n in ANCHOR_DEFAULTS]
        + [("protocol member", n) for n in ANCHOR_PROTOCOL_MEMBERS]
        + [("manifest member", n) for n in ANCHOR_MANIFEST_MEMBERS]
    )

    for kind_label, name in anchors:
        if _c_has(c, name):
            continue
        rename = KNOWN_RENAMES.get(name)
        if rename and _c_has(c, rename):
            violations.append(
                Violation(
                    "RENAMED",
                    name,
                    f"{kind_label}: present in C as '{rename}' (must keep Python name '{name}')",
                )
            )
        else:
            violations.append(Violation("MISSING", name, f"{kind_label}: no C twin found"))

    for name in INVENTED_C:
        if _c_has(c, name) and name not in py_symbols:
            violations.append(
                Violation(
                    "INVENTED",
                    name,
                    "present in C public surface but Python laila has no such symbol",
                )
            )

    # Focused inheritance-edge check: Python's Manifest is `class Manifest(Entry)`,
    # so the C twin must derive from Entry (not Identifiable). This is a first slice
    # of Layer-A edge checking, scoped to Manifest to avoid surfacing unrelated
    # structural diffs.
    cm = c.classes.get("Manifest")
    if cm is not None and "Entry" not in cm.bases:
        violations.append(
            Violation(
                "RENAMED",
                "Manifest",
                f"C Manifest must derive from Entry (Python: class Manifest(Entry)); bases are {cm.bases or '[]'}",
            )
        )

    return violations


def _format_report(violations: list[Violation]) -> str:
    if not violations:
        return "Mirror Law (Layer A): OK -- no surface/hierarchy violations found."

    order = {"RENAMED": 0, "MISSING": 1, "INVENTED": 2}
    violations = sorted(violations, key=lambda v: (order.get(v.kind, 9), v.symbol))

    lines = ["Mirror Law (Layer A) violations:\n"]
    counts: dict[str, int] = {}
    for v in violations:
        counts[v.kind] = counts.get(v.kind, 0) + 1
        lines.append(f"  [{v.kind:8}] {v.symbol}")
        lines.append(f"             {v.detail}")
    summary = ", ".join(f"{k}={counts[k]}" for k in sorted(counts))
    lines.append("")
    lines.append(f"Total: {len(violations)} violation(s)  ({summary})")
    return "\n".join(lines)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    violations = check(repo_root)
    print(_format_report(violations))
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
