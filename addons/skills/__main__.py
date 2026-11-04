"""``laila-skills`` / ``python -m laila.addons.skills`` -- install the bundled agent skills.

::

    laila-skills list                         # bundled skills and where they live
    laila-skills install                      # all skills -> ./.cursor/skills/
    laila-skills install --user               # all skills -> ~/.cursor/skills/
    laila-skills install --dest path/to/dir   # explicit skills folder
    laila-skills install data-loading --link  # one skill, symlinked (dev checkout)
    laila-skills install --force              # replace what is already there
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from . import SKILLS_DIR, available, install, target_dir


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="laila-skills",
        description="Install the agent skills bundled with laila-core.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show the bundled skills")

    inst = sub.add_parser("install", help="copy the skills into a skills folder")
    inst.add_argument("names", nargs="*", help="skills to install (default: all)")
    where = inst.add_mutually_exclusive_group()
    where.add_argument("--user", action="store_true", help="install into ~/.cursor/skills")
    where.add_argument("--dest", help="install into this folder instead of ./.cursor/skills")
    inst.add_argument("--link", action="store_true", help="symlink instead of copy")
    inst.add_argument("--force", action="store_true", help="replace existing skills")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    skills = available()

    if args.command == "list":
        print(f"bundled in {SKILLS_DIR}")
        for name, path in skills.items():
            print(f"  {name:<20} {path.relative_to(SKILLS_DIR)}")
        return 0

    try:
        installed = install(
            args.dest, user=args.user, names=args.names or None, link=args.link, force=args.force
        )
    except KeyError as exc:
        print(f"laila-skills: {exc.args[0]}", file=sys.stderr)
        return 2

    root = target_dir(args.dest, user=args.user)
    wanted = args.names or list(skills)
    for name in wanted:
        if name in installed:
            verb = "linked" if args.link else "installed"
            print(f"{verb:<10} {name:<20} -> {installed[name]}")
        else:
            print(f"{'exists':<10} {name:<20}    {root / name}  (use --force to replace)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
