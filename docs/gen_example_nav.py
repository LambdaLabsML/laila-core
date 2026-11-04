"""Auto-generate the Examples sidebar from docs/examples/<group>/*.md.

Examples are nested one level: every entry in ``GROUPS`` is a
sub-directory of ``docs/examples`` rendered as a collapsible section,
and the pages inside it are listed by their numeric prefix. The first
``# Title`` line of each page is its sidebar label. ``index.md`` is
emitted first so Material's ``navigation.indexes`` renders it as the
section landing page. The result is an ``examples/SUMMARY.md`` consumed
by mkdocs-literate-nav.

Also copies each page's Jupyter notebook (matched by numeric prefix in
the group's notebook directory) into the virtual ``examples/notebooks/``
directory so the page can offer a download link.
"""

from pathlib import Path
import mkdocs_gen_files

EXAMPLES_DIR = Path("docs/examples")

# (docs sub-directory, sidebar title, directory holding the source notebooks)
GROUPS = [
    ("simple_data_loading", "Simple Data Loading", Path("examples/image_dataset")),
    ("torch_wrappers", "Torch Wrappers", Path("examples/torch_wrappers")),
]

nav = mkdocs_gen_files.Nav()
nav["Overview"] = "index.md"

for subdir, group_title, nb_root in GROUPS:
    for md_path in sorted((EXAMPLES_DIR / subdir).glob("*.md")):
        title = None
        with open(md_path) as f:
            for line in f:
                if line.startswith("# "):
                    title = line.removeprefix("# ").strip()
                    break
        if title is None:
            title = md_path.stem.replace("_", " ").title()

        nav[(group_title, title)] = f"{subdir}/{md_path.name}"

        # Copy the matching .ipynb into examples/notebooks/<name>.ipynb
        num = md_path.stem.split("_", 1)[0]
        matches = sorted(nb_root.glob(f"{num}_*.ipynb"))
        if matches:
            nb_path = matches[0]
            dest = f"examples/notebooks/{nb_path.name}"
            with mkdocs_gen_files.open(dest, "wb") as out:
                out.write(nb_path.read_bytes())

with mkdocs_gen_files.open("examples/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
