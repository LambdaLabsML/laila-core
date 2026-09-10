# Tutorial 14a: Querying a Manifest with SQL

A `Manifest` is a blueprint of string keys mapping to `global_id`s — or to nested dicts of strings. When those nested dicts carry **metadata** (owner, format, size, ...), the manifest becomes a small catalog, and `Manifest.sql()` lets you filter it with a `SELECT ... FROM ... WHERE ...` query.

Under the hood the manifest materialises its rows into a private **sqlite** index. This index is LAILA's sanctioned *non-memorizing algorithmic helper*: it never goes through `central.memory`, is never memorized, never registered with a pool router, and never travels with the manifest. It is a local, disposable file under `<laila_root>/indices/` that can be rebuilt at any time.

You will:

- Build a catalog manifest and filter it with `sql()`
- Learn the supported grammar (and what is rejected)
- Control the index explicitly: `build_index(on=..., composite=...)`, `invalidate_index()`, `clear_index()`
- Reuse an index across manifests with `persist=`
- Flatten list-valued metadata with a `_sql_rows` override
- Realize only the matched entries through normal `laila.remember`

**Prerequisites:** `pip install laila-core`. No credentials or external services required.

```python
import os
import tempfile

import laila
from laila.macros.defaults import LAILA_DEFAULT_DIRECTORIES
from laila.policy.central.memory.schema.manifest import Manifest

# Keep index files out of your real ~/.laila for this tutorial.
root = tempfile.mkdtemp(prefix="laila_sql_tutorial_")
laila.set_default_directory(root)
print("indices live under:", LAILA_DEFAULT_DIRECTORIES["indices"])
```

## Step 1: A catalog manifest

Each top-level value is a dict of **string** metadata (blueprint values must be `str`, `list[str]`, or nested dicts of those). One column — here `gid` — holds the entry the row describes. Note that `ds_d` has no `fmt` key: missing columns become SQL `NULL`.

```python
datasets = {name: laila.constant(data=f"<{name} payload>", nickname=name) for name in ("a", "b", "c", "d")}
laila.memorize(list(datasets.values())).wait()

catalog = Manifest(data={
    "ds_a": {"gid": datasets["a"].global_id, "owner": "alice", "rows": "120", "fmt": "parquet"},
    "ds_b": {"gid": datasets["b"].global_id, "owner": "bob",   "rows": "30",  "fmt": "csv"},
    "ds_c": {"gid": datasets["c"].global_id, "owner": "alice", "rows": "900", "fmt": "csv"},
    "ds_d": {"gid": datasets["d"].global_id, "owner": "carol", "rows": "7"},
}, nickname="dataset_catalog")

print("catalog:", catalog.global_id)
print("keys:   ", list(catalog.data))
```

## Step 2: `sql()`

`sql()` returns a **new** manifest of the same type containing only the matched top-level entries; the original is never mutated. The index is built lazily on the first call and reused afterwards.

Grammar:

- `SELECT <items> FROM <name> [WHERE <predicate>]` — nothing else.
- `<name>` is an alias and is ignored; `<items>` may be `*`, `col`, or `alias.col` (the alias is stripped; the default projection keeps whole rows regardless).
- `==` is normalised to `=`; a trailing `;` is fine.
- The `WHERE` predicate is passed to sqlite, so `AND` / `OR` / `NOT`, `IN (...)`, `LIKE`, `IS NULL`, comparisons, and functions such as `CAST(...)` all work.
- **String literals must be single-quoted.**

```python
def show(label, m):
    print(f"{label:48s} -> {list(m.data)}")

show("owner = 'alice'",             catalog.sql("SELECT * FROM ds WHERE owner = 'alice'"))
show("ds.gid ... owner == 'alice'", catalog.sql("SELECT ds.gid FROM ds WHERE owner == 'alice' AND fmt = 'csv'"))
show("fmt IS NULL",                 catalog.sql("SELECT * FROM catalog WHERE fmt IS NULL"))
show("CAST(rows AS INTEGER) > 100", catalog.sql("SELECT * FROM t WHERE CAST(rows AS INTEGER) > 100;"))
show("owner LIKE 'c%'",             catalog.sql("SELECT * FROM t WHERE owner LIKE 'c%'"))
show("no WHERE",                    catalog.sql("SELECT * FROM t"))

result = catalog.sql("SELECT * FROM ds WHERE owner = 'alice'")
print("\nresult is a", type(result).__name__, "| distinct object:", result.global_id != catalog.global_id)
print("original untouched:", list(catalog.data))
```

### What is rejected

`ORDER BY`, `LIMIT`, `GROUP BY`, `HAVING`, `JOIN`, and `UNION` raise `ValueError` before anything touches sqlite; so does any statement that is not a `SELECT`. An unquoted bareword on the right-hand side is a *column name* to SQL — the error is re-raised as a `ValueError` with a hint.

```python
for query in (
    "SELECT * FROM t ORDER BY rows",
    "SELECT * FROM t LIMIT 1",
    "DELETE FROM t",
    "SELECT * FROM t WHERE owner = alice",      # forgot the quotes
):
    try:
        catalog.sql(query)
    except ValueError as exc:
        print(f"{query:42s} -> ValueError: {str(exc)[:70]} ...")
```

## Step 3: The index behind `sql()`

The first `sql()` created a sqlite file under `<laila_root>/indices/<manifest-uuid>/`. `build_index()` lets you create it explicitly and add sqlite indexes on hot columns (`on=`) or column combinations (`composite=`). It is idempotent while the index is fresh.

```python
state = catalog._sql_state                      # private handle, shown only for illustration
print("index file:", os.path.relpath(state.db_path, root))
print("exists:", os.path.exists(state.db_path), "| columns:", state.columns, "| sqlite indexes:", state.indexed)

catalog.clear_index()                            # close + delete the temporary file
print("after clear_index: file exists:", os.path.exists(state.db_path), "| state:", catalog._sql_state)

catalog.build_index(on=["owner"], composite=[("owner", "fmt")])
print("rebuilt with indexes:", catalog._sql_state.indexed)
```

### Invalidation is automatic on mutation

`extend()` / `+=` mark the index **stale** (cheap — the connection stays open). The next `sql()` re-inserts the rows in place, keeping the sqlite indexes, and widens the table when new columns appear. Subclasses that mutate their own blueprint should call `invalidate_index()` after writing.

```python
datasets["e"] = laila.constant(data="<e payload>", nickname="e")
datasets["f"] = laila.constant(data="<f payload>", nickname="f")
laila.memorize([datasets["e"], datasets["f"]]).wait()

catalog.extend(Manifest(data={"ds_e": {"gid": datasets["e"].global_id, "owner": "alice", "rows": "1", "fmt": "csv"}}))
print("stale after extend:", catalog._sql_state.stale)
show("owner = 'alice' (after extend)", catalog.sql("SELECT * FROM t WHERE owner = 'alice'"))
print("stale after sql:  ", catalog._sql_state.stale)

catalog += Manifest(data={"ds_f": {"gid": datasets["f"].global_id, "owner": "dave", "rows": "2", "fmt": "csv", "region": "eu"}})
show("region = 'eu' (new column)", catalog.sql("SELECT * FROM t WHERE region = 'eu'"))
print("columns now:", catalog._sql_state.columns, "| indexes kept:", catalog._sql_state.indexed)
```

## Step 4: Persisting an index with `persist=`

By default the index file is temporary and deleted by `clear_index()` (or when the manifest is garbage-collected). Pass `persist="<path>"` to `build_index` to use a file of your choice instead. A second manifest with the same blueprint **attaches** to that file without rebuilding — handy for large catalogs that are re-opened in every process. Persistent files survive `clear_index()` unless you pass `remove_persisted=True`.

```python
index_path = os.path.join(root, "dataset_catalog.sqlite")

catalog.clear_index()
catalog.build_index(persist=index_path)
print("persistent file exists:", os.path.exists(index_path), "| is_persistent:", catalog._sql_state.is_persistent)

reopened = Manifest(data=catalog.data, nickname="dataset_catalog")      # e.g. in a new process
reopened.build_index(persist=index_path)
print("attached without rebuild:", reopened._sql_state.db_path == index_path)
show("reopened: owner = 'bob'", reopened.sql("SELECT * FROM t WHERE owner = 'bob'"))

reopened.clear_index()
print("file survives a plain clear_index():", os.path.exists(index_path))
catalog.clear_index(remove_persisted=True)
print("removed with remove_persisted=True:  ", not os.path.exists(index_path))
```

## Step 5: Non-primitive metadata and the `_sql_rows` hook

Index columns must be primitive scalars (`str`, `int`, `float`, `bool`, `bytes`, `None`). A list-valued key such as `tags` is rejected at build time. Subclass `Manifest` and override `_sql_rows()` to flatten it — the hook returns `(rows, columns)` where each row is `(row_key, {column: scalar})`. Row order must be deterministic. (`_sql_project()` is the matching hook if your rows do not map 1:1 to top-level keys.)

```python
tagged = Manifest(data={
    "x": {"gid": datasets["a"].global_id, "tags": ["vision", "prod"]},
    "y": {"gid": datasets["b"].global_id, "tags": ["nlp"]},
})
try:
    tagged.sql("SELECT * FROM t")
except TypeError as exc:
    print("TypeError:", str(exc)[:96], "...")


class TaggedManifest(Manifest):
    def _sql_rows(self):
        rows, columns = [], set()
        for key, meta in (self.data or {}).items():
            row = {k: (",".join(v) if isinstance(v, list) else v) for k, v in meta.items()}
            rows.append((key, row))
            columns.update(row)
        return rows, columns


tagged = TaggedManifest(data=tagged.data)
show("tags LIKE '%prod%'", tagged.sql("SELECT * FROM t WHERE tags LIKE '%prod%'"))
print("result type:", type(tagged.sql("SELECT * FROM t")).__name__)
```

## Step 6: From matched rows to entries

The query only touched the index; the entries themselves still live in memory. Collect the `gid` column of the result and go through `laila.remember` as usual — the index never replaces the memory path, it only narrows what you ask for. (A manifest whose leaves are metadata strings cannot use `.realized`, which treats every leaf as a gid.)

```python
matched = catalog.sql("SELECT * FROM t WHERE owner = 'alice' AND fmt = 'csv'")
gids = [row["gid"] for row in matched.data.values()]

fetched = laila.remember(gids, persist=False).wait()
entries = fetched if isinstance(fetched, list) else [fetched]
for key, entry in zip(matched.data, entries):
    print(f"{key}: {entry.data}")
```

## Clean up

```python
catalog.clear_index()
tagged.clear_index()
print("index dirs left:", os.listdir(LAILA_DEFAULT_DIRECTORIES["indices"]))
```

## Summary

- `Manifest.sql("SELECT ... FROM ... WHERE ...")` filters a manifest whose top-level values are metadata dicts and returns a new manifest; `ORDER BY` / `LIMIT` / `GROUP BY` / `HAVING` / `JOIN` / `UNION` are rejected, string literals need single quotes.
- The index is a private sqlite file under `LAILA_DEFAULT_DIRECTORIES["indices"]` — a *non-memorizing algorithmic helper* that never enters `central.memory` or the wire.
- `build_index(on=, composite=)` adds sqlite indexes; `extend` / `+=` invalidate; `clear_index()` closes and deletes temporary files; `persist=` keeps a reusable file that a matching manifest can attach to.
- Row values must be primitive scalars; override `_sql_rows()` (and `_sql_project()`) to flatten or reshape.
- Use the result's `gid` column with `laila.remember` to fetch only what matched.

Next: [Tutorial 15 — Migrating Entries Between Pools](15_pool_migration.md).
