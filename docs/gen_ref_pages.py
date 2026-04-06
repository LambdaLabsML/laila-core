"""Auto-generate API reference pages for mkdocs-gen-files.

This script walks the laila package tree and creates one stub page per
module/subpackage.  It also writes a SUMMARY.md consumed by
mkdocs-literate-nav to build the sidebar navigation.
"""

from pathlib import Path
import mkdocs_gen_files

PACKAGES = [
    "laila",
    "laila.atomic",
    "laila.atomic.definitions",
    "laila.atomic.types",
    "laila.basics",
    "laila.basics.definitions",
    "laila.entry",
    "laila.entry.compdata",
    "laila.entry.compdata.taxonomy",
    "laila.entry.compdata.transformation",
    "laila.pool",
    "laila.pool.s3",
    "laila.pool.gcs",
    "laila.pool.azure",
    "laila.pool.cloudflare",
    "laila.pool.backblaze",
    "laila.pool.filesystem",
    "laila.pool.hdf5",
    "laila.pool.redis",
    "laila.pool.duckdb",
    "laila.pool.postgres",
    "laila.pool.mongo",
    "laila.pool.sqlite",
    "laila.pool.huggingface",
    "laila.pool.schema",
    "laila.pool.boto",
    "laila.policy",
    "laila.policy.schema",
    "laila.policy.central",
    "laila.policy.central.command",
    "laila.policy.central.command.schema",
    "laila.policy.central.command.schema.future",
    "laila.policy.central.command.schema.future.future",
    "laila.policy.central.command.taskforce",
    "laila.policy.central.communication",
    "laila.policy.central.communication.schema",
    "laila.policy.central.memory",
    "laila.policy.central.memory.hint",
    "laila.policy.central.memory.record",
    "laila.policy.central.memory.router",
    "laila.policy.central.memory.schema",
    "laila.utils",
    "laila.utils.args",
    "laila.utils.decorators",
    "laila.macros",
]

nav = mkdocs_gen_files.Nav()

for dotted in sorted(PACKAGES):
    parts = dotted.split(".")
    doc_path = Path(*parts, "index.md")
    full_doc_path = Path("reference", *parts, "index.md")

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        fd.write(f"# `{dotted}`\n\n::: {dotted}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, ".." * len(parts))

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
