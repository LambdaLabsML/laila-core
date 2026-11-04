"""Agent skills bundled with ``laila-core``.

Each subdirectory is one skill (a ``SKILL.md`` with a ``name`` /
``description`` frontmatter, a ``reference.md`` and optional helper
scripts). They ship inside the wheel next to :mod:`laila.addons.torch`,
but an agent only picks them up from a skills folder (``.cursor/skills``
in a project, or ``~/.cursor/skills`` for the user). :func:`install`
copies them there; the ``laila-skills`` console script wraps it::

    pip install "laila-core[torch-skills]"
    laila-skills install            # -> ./.cursor/skills/<name>/
    laila-skills install --user     # -> ~/.cursor/skills/<name>/

Bundled skills:

- ``data-loading`` -- turn a dataset folder into a manifest + the
  ``RAM << HDF5 << cloud`` cache + a ``laila.addons.torch.DataLoader``.
- ``model-checkpoint`` -- checkpoint / restore a torch model + optimizer
  through ``laila.addons.torch.Module`` / ``Optimizer``.
"""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Iterable
from pathlib import Path

SKILLS_DIR = Path(__file__).resolve().parent
"""Directory the bundled skills are read from (inside the installed package)."""

SKILLS_SUBDIR = Path(".cursor") / "skills"
"""Where agents look for skills, relative to a project root or the home dir."""

_NAME_RE = re.compile(r"^name:\s*(?P<name>\S+)\s*$", re.MULTILINE)
_IGNORE = shutil.ignore_patterns("__pycache__", "*.py[cod]")


def skill_name(skill_dir: str | os.PathLike[str]) -> str:
    """Name of the skill in ``skill_dir`` (``name:`` from the ``SKILL.md`` frontmatter).

    Falls back to the directory name with ``_`` -> ``-`` when the frontmatter
    has no ``name`` line, so ``data_loading`` -> ``data-loading``.
    """
    skill_dir = Path(skill_dir)
    skill_md = skill_dir / "SKILL.md"
    if skill_md.is_file():
        text = skill_md.read_text(encoding="utf-8")
        if text.startswith("---\n"):
            end = text.find("\n---", 4)
            match = _NAME_RE.search(text[4:end]) if end != -1 else None
            if match:
                return match.group("name")
    return skill_dir.name.replace("_", "-")


def available() -> dict[str, Path]:
    """Bundled skills, as ``{name: directory}`` (directories holding a ``SKILL.md``)."""
    return {
        skill_name(d): d
        for d in sorted(SKILLS_DIR.iterdir())
        if d.is_dir() and (d / "SKILL.md").is_file()
    }


def target_dir(dest: str | os.PathLike[str] | None = None, *, user: bool = False) -> Path:
    """Resolve the skills folder :func:`install` writes to.

    ``dest`` wins when given; ``user=True`` picks ``~/.cursor/skills``;
    otherwise ``./.cursor/skills`` under the current working directory.
    """
    if dest is not None:
        return Path(dest).expanduser().resolve()
    root = Path.home() if user else Path.cwd()
    return root / SKILLS_SUBDIR


def install(
    dest: str | os.PathLike[str] | None = None,
    *,
    user: bool = False,
    names: Iterable[str] | None = None,
    link: bool = False,
    force: bool = False,
) -> dict[str, Path]:
    """Copy the bundled skills into a skills folder and return ``{name: installed path}``.

    One subfolder per skill named after the skill (``<dest>/data-loading/``,
    ...). ``names`` restricts which skills are installed (default: all).
    ``link=True`` symlinks to the package copy instead of copying -- handy
    for a checkout where the skills are being edited. An existing target is
    left alone (and omitted from the result) unless ``force=True``, in which
    case it is replaced.

    Raises :class:`KeyError` for an unknown skill name.
    """
    skills = available()
    if names is None:
        chosen = skills
    else:
        unknown = sorted(set(names) - skills.keys())
        if unknown:
            raise KeyError(f"unknown skill(s) {unknown}; available: {sorted(skills)}")
        chosen = {n: skills[n] for n in names}

    root = target_dir(dest, user=user)
    root.mkdir(parents=True, exist_ok=True)
    installed: dict[str, Path] = {}
    for name, src in chosen.items():
        target = root / name
        if target.is_symlink() or target.exists():
            if not force:
                continue
            if target.is_symlink() or target.is_file():
                target.unlink()
            else:
                shutil.rmtree(target)
        if link:
            target.symlink_to(src, target_is_directory=True)
        else:
            shutil.copytree(src, target, ignore=_IGNORE)
        installed[name] = target
    return installed


__all__ = ["SKILLS_DIR", "SKILLS_SUBDIR", "available", "install", "skill_name", "target_dir"]
