"""Auto-generate the Examples sidebar from docs/examples/*.md.

Scans for example markdown files, extracts the first ``# Title`` line
from each, and writes an ``examples/SUMMARY.md`` consumed by
mkdocs-literate-nav.  Unlike the tutorials, examples form a single flat
list ordered by their numeric prefix.  ``index.md`` is emitted first so
Material's ``navigation.indexes`` renders it as the section landing page.

Also copies the matching Jupyter notebooks into the virtual
``examples/notebooks/`` directory so each example page can offer a
download link.
"""

from pathlib import Path
import mkdocs_gen_files

EXAMPLES_DIR = Path("docs/examples")
NOTEBOOK_ROOTS = [
    Path("examples/image_dataset"),
]

nav = mkdocs_gen_files.Nav()
nav["Overview"] = "index.md"

for md_path in sorted(EXAMPLES_DIR.glob("*.md")):
    if md_path.name == "index.md":
        continue

    title = None
    with open(md_path) as f:
        for line in f:
            if line.startswith("# "):
                title = line.removeprefix("# ").strip()
                break
    if title is None:
        title = md_path.stem.replace("_", " ").title()

    nav[title] = md_path.name

    # Copy the matching .ipynb into examples/notebooks/<name>.ipynb
    num = md_path.stem.split("_", 1)[0]
    for nb_root in NOTEBOOK_ROOTS:
        matches = sorted(nb_root.glob(f"{num}_*.ipynb"))
        if matches:
            nb_path = matches[0]
            dest = f"examples/notebooks/{nb_path.name}"
            with mkdocs_gen_files.open(dest, "wb") as out:
                out.write(nb_path.read_bytes())
            break

with mkdocs_gen_files.open("examples/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
