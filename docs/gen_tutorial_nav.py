"""Auto-generate the Tutorials sidebar from docs/tutorials/*.md.

Scans for tutorial markdown files, extracts the first ``# Title`` line
from each, and writes a ``tutorials/SUMMARY.md`` consumed by
mkdocs-literate-nav.  Tutorials 01-07 are grouped under **Basics**,
08-17 under **Intermediate**, and 18+ under **Advanced**.

Also copies the matching Jupyter notebooks into the virtual
``tutorials/notebooks/`` directory so each tutorial page can offer a
download link.
"""

from pathlib import Path
import mkdocs_gen_files

TUTORIALS_DIR = Path("docs/tutorials")
NOTEBOOK_ROOTS = [
    Path("tutorials/01_basics"),
    Path("tutorials/02_intermediate"),
    Path("tutorials/03_advanced"),
]

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
    if num.rstrip("ab").isdigit():
        n = int(num.rstrip("ab"))
        if n >= 18:
            nav[("Advanced", title)] = md_path.name
        elif n >= 8:
            nav[("Intermediate", title)] = md_path.name
        else:
            nav[("Basics", title)] = md_path.name
    else:
        nav[("Basics", title)] = md_path.name

    # Copy the matching .ipynb into tutorials/notebooks/<name>.ipynb
    for nb_root in NOTEBOOK_ROOTS:
        matches = sorted(nb_root.glob(f"{num}_*.ipynb"))
        if matches:
            nb_path = matches[0]
            dest = f"tutorials/notebooks/{nb_path.name}"
            with mkdocs_gen_files.open(dest, "wb") as out:
                out.write(nb_path.read_bytes())
            break

with mkdocs_gen_files.open("tutorials/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
