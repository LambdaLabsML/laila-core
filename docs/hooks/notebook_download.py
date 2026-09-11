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
    ],
}

_BUTTON_TEMPLATE = (
    '<a class="md-button md-button--primary notebook-download" '
    'href="../notebooks/{filename}" download>{filename}</a>\n\n'
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

    heading_re = re.compile(r"^(# .+)$", re.MULTILINE)
    match = heading_re.search(markdown)
    if match:
        insert_pos = match.end()
        button_html = "\n\n" + _BUTTON_TEMPLATE.format(filename=nb_name)
        return markdown[:insert_pos] + button_html + markdown[insert_pos:]

    return _BUTTON_TEMPLATE.format(filename=nb_name) + markdown
