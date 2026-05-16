# Tutorial 20: Structured Backends — DuckDB, SQLite, Postgres, Mongo

Object stores work well for opaque blobs, but sometimes you want LAILA entries to live alongside structured data — a Postgres database that already holds your application state, a DuckDB file you can query analytically, a SQLite file for a small embedded app, or MongoDB for document storage.

## Prerequisites

```bash
pip install "laila-core[duckdb,postgres,mongo]"
```

SQLite needs no extra (stdlib only).

## DuckDB — embedded analytical store

`DuckDBPool` writes to a `.duckdb` file. Without `file_path`, LAILA picks a default under the configured `pools/` directory:

```python
from laila.pool import DuckDBPool

duck = DuckDBPool(nickname="duck")
laila.memory.extend(duck, pool_nickname="duck")
```

## SQLite — stdlib embedded store

`SQLitePool` needs no third-party extras — it uses the stdlib `sqlite3` module:

```python
from laila.pool import SQLitePool

sqlite = SQLitePool(nickname="sqlite")
laila.memory.extend(sqlite, pool_nickname="sqlite")
```

## PostgresPool — managed-local or remote

If you do **not** pass any connection parameters, `PostgresPool` starts a managed local `postgres` server in a subprocess. Pass `host` / `port` / `dbname` / `user` / `password` (or a `dsn`) to point at an existing server:

```python
from laila.pool import PostgresPool

# Managed local server:
pg = PostgresPool(nickname="pg")

# Remote server:
# pg = PostgresPool(host="db.example.com", port=5432, dbname="prod", user="laila", password="...")

laila.memory.extend(pg, pool_nickname="pg")
```

## MongoPool — managed-local or remote

`MongoPool` mirrors the Postgres pattern: managed local `mongod` when no `uri`/`host` is given, otherwise connects to your existing cluster:

```python
from laila.pool import MongoPool

mongo = MongoPool(nickname="mongo")
laila.memory.extend(mongo, pool_nickname="mongo")
```

## When to pick a structured backend

| Backend | Reach for it when |
|---|---|
| DuckDB | You want to run analytical SQL across your entries on a single machine. |
| SQLite | You're shipping an embedded app and want a zero-dependency pool. |
| Postgres | You already run Postgres and want entries to live alongside business data. |
| Mongo | You already run MongoDB and prefer document semantics over rows. |

## Summary

- The `memorize` / `remember` API is unchanged across backends.
- DuckDB and SQLite are file-based; Postgres and Mongo can self-host or attach to existing servers.
- Pool routing (see [Tutorial 14](14_multi_pool_routing.md)) lets you mix structured and object backends in the same workflow.

Next: [Tutorial 21 — Publishing to the Hugging Face Hub](21_huggingface_publishing.md).
