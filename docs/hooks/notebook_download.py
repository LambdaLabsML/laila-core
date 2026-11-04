"""MkDocs hook: inject a 'Download as Notebook' button at the top of tutorial
and example pages.

The button links to the matching ``.ipynb`` file that ``gen_tutorial_nav.py``
(for ``tutorials/``) or ``gen_example_nav.py`` (for ``examples/``) copies into
the section's ``notebooks/`` directory at build time.
"""

from __future__ import annotations

import re
from pathlib import Path

# Docs section prefix -> repo directories that hold the source notebooks.
_NOTEBOOK_ROOTS: dict[str, list[Path]] = {
    "tutorials/": [
        Path("tutorials/01_basics"),
        Path("tutorials/02_intermediate"),
        Path("tutorials/03_advanced"),
    ],
    "examples/": [
        Path("examples/image_dataset"),
        Path("examples/torch_wrappers"),
    ],
}

# ``{up}`` climbs from the page's directory URL back to the section root,
# where ``notebooks/`` lives: one ``../`` per path component under the section
# (``tutorials/02_x.md`` -> ``../``, ``examples/torch_wrappers/03_x.md`` -> ``../../``).
_BUTTON_TEMPLATE = (
    '<a class="md-button md-button--primary notebook-download" '
    'href="{up}notebooks/{filename}" download>{filename}</a>\n\n'
)


def _find_notebook(section: str, num: str) -> str | None:
    """Return the notebook filename whose number prefix matches *num*."""
    for root in _NOTEBOOK_ROOTS[section]:
        for nb in sorted(root.glob(f"{num}_*.ipynb")):
            return nb.name
    return None


def on_page_markdown(markdown: str, *, page, config, files) -> str:
    """Prepend a download button when the page is a tutorial/example with a notebook."""
    src = page.file.src_path
    if not src.endswith(".md"):
        return markdown

    section = next((s for s in _NOTEBOOK_ROOTS if src.startswith(s)), None)
    if section is None:
        return markdown

    stem = Path(src).stem
    num = stem.split("_", 1)[0]
    nb_name = _find_notebook(section, num)
    if nb_name is None:
        return markdown

    up = "../" * (len(Path(src).parts) - 1)
    button_html = _BUTTON_TEMPLATE.format(up=up, filename=nb_name)

    heading_re = re.compile(r"^(# .+)$", re.MULTILINE)
    match = heading_re.search(markdown)
    if match:
        insert_pos = match.end()
        return markdown[:insert_pos] + "\n\n" + button_html + markdown[insert_pos:]

    return button_html + markdown
