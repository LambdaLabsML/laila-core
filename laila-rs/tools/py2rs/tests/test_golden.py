"""py2rs golden tests: one fixture per statement-layer rule of
``docs/PORT_RULES.md`` (P2 .. P33).

``golden/<case>.py`` is transpiled with ``py2rs.convert`` and must reproduce
``golden/<case>.rs`` byte for byte; ``p33_script`` runs in ``--script`` mode.
A deliberate generator change regenerates the goldens with
``PY2RS_GOLDEN=overwrite python3 -m unittest tools/py2rs/tests/test_golden.py``
(review the diff: the goldens are the specification of the lowering).
The second half of every case checks determinism -- converting twice gives
the same text -- which is what makes the CI regenerate-and-diff of the
suites and examples meaningful.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import py2rs

GOLDEN = HERE / "golden"
RS_ROOT = HERE.parents[2]  # laila-rs/
REPO_ROOT = RS_ROOT.parent


def transpile(py_path: Path) -> str:
    source = py_path.read_text(encoding="utf-8")
    rel = py_path.relative_to(REPO_ROOT).as_posix()
    text, em = py2rs.convert(
        source,
        rel,
        src_path=str(py_path),
        overrides_dir=None,
        root=str(REPO_ROOT),
        script=py_path.stem.startswith("p33_"),
    )
    if em.unsupported:
        raise AssertionError(f"{py_path.name}: unsupported constructs: {em.unsupported}")
    return text


class GoldenTests(unittest.TestCase):
    maxDiff = None

    def check(self, name: str) -> None:
        py_path = GOLDEN / f"{name}.py"
        rs_path = GOLDEN / f"{name}.rs"
        got = transpile(py_path)
        self.assertEqual(got, transpile(py_path), f"{name}: py2rs is not deterministic")
        if os.environ.get("PY2RS_GOLDEN") == "overwrite" or not rs_path.exists():
            rs_path.write_text(got, encoding="utf-8")
        expected = rs_path.read_text(encoding="utf-8")
        self.assertEqual(
            expected,
            got,
            f"{name}.rs differs from the py2rs output (PY2RS_GOLDEN=overwrite to accept)",
        )

    def test_every_fixture_has_a_golden(self) -> None:
        pys = {p.stem for p in GOLDEN.glob("*.py")}
        rss = {p.stem for p in GOLDEN.glob("*.rs")}
        self.assertEqual(pys, rss)


def _make(name: str):
    def test(self: GoldenTests) -> None:
        self.check(name)

    test.__name__ = f"test_{name}"
    return test


for _p in sorted(GOLDEN.glob("*.py")):
    setattr(GoldenTests, f"test_{_p.stem}", _make(_p.stem))


if __name__ == "__main__":
    unittest.main()
