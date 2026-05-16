"""Pool implementations for every supported storage backend.

This package is the catalogue of concrete :class:`_LAILA_IDENTIFIABLE_POOL`
subclasses. Each backend lives in its own sub-module and exposes a
single ``*Pool`` class:

================  ==============================================
Backend           Class
================  ==============================================
Redis             :class:`RedisPool`
HDF5              :class:`HDF5Pool`
Cloudflare R2     :class:`CloudflarePool`
S3 (boto3)        :class:`S3Pool`
HuggingFace Hub   :class:`HuggingFacePool`
Filesystem        :class:`FilesystemPool`
Google Cloud      :class:`GCSPool`
Azure Blob        :class:`AzurePool`
SQLite            :class:`SQLitePool`
Postgres          :class:`PostgresPool`
MongoDB           :class:`MongoPool`
DuckDB            :class:`DuckDBPool`
BackBlaze B2      :class:`BackblazePool`
================  ==============================================

Each import is guarded by ``try/except ModuleNotFoundError`` so the
absence of an optional dependency (e.g. ``redis``, ``h5py``, ``boto3``)
only hides that backend's ``*Pool`` class instead of breaking
``import laila``. Install the matching extra (for example
``pip install laila-core[redis]``) to make a backend available.

Pools are designed to be composed via the proxy operators
(``cache << origin`` / ``origin >> cache``) so users can stack a fast
local tier in front of a slower remote tier without the rest of the
codebase ever knowing.
"""

try:
    from .redis.redis import RedisPool
except ModuleNotFoundError:
    pass
try:
    from .hdf5.hdf5 import HDF5Pool
except ModuleNotFoundError:
    pass
try:
    from .cloudflare.cloudflare import CloudflarePool
except ModuleNotFoundError:
    pass
try:
    from .s3.s3 import S3Pool
except ModuleNotFoundError:
    pass
try:
    from .huggingface.huggingface import HuggingFacePool
except ModuleNotFoundError:
    pass
try:
    from .filesystem.filesystem import FilesystemPool
except ModuleNotFoundError:
    pass
try:
    from .gcs.gcs import GCSPool
except ModuleNotFoundError:
    pass
try:
    from .azure.azure import AzurePool
except ModuleNotFoundError:
    pass
try:
    from .sqlite.sqlite import SQLitePool
except ModuleNotFoundError:
    pass
try:
    from .postgres.postgres import PostgresPool
except ModuleNotFoundError:
    pass
try:
    from .mongo.mongo import MongoPool
except ModuleNotFoundError:
    pass
try:
    from .duckdb.duckdb import DuckDBPool
except ModuleNotFoundError:
    pass
try:
    from .backblaze.backblaze import BackblazePool
except ModuleNotFoundError:
    pass
