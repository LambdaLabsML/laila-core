"""Auto-generate the Tutorials sidebar from docs/tutorials/*.md.

Scans for tutorial markdown files, extracts the first ``# Title`` line
from each, and writes a ``tutorials/SUMMARY.md`` consumed by
mkdocs-literate-nav.  Tutorials 01–07 are grouped under **Basics** and
08+ under **Intermediate**.
"""

from pathlib import Path
import mkdocs_gen_files

TUTORIALS_DIR = Path("docs/tutorials")

nav = mkdocs_gen_files.Nav()

for md_path in sorted(TUTORIALS_DIR.glob("*.md")):
    title = None
    with open(md_path) as f:
        for line in f:
            if line.startswith("# "):
                title = line.removeprefix("# ").strip()
                break
    if title is None:
        title = md_path.stem.replace("_", " ").title()

    stem = md_path.stem
    num = stem.split("_", 1)[0]
    if num.rstrip("ab").isdigit() and int(num.rstrip("ab")) >= 8:
        nav[("Intermediate", title)] = md_path.name
    else:
        nav[("Basics", title)] = md_path.name

with mkdocs_gen_files.open("tutorials/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
