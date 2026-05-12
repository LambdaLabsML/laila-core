"""Pool implementations for all supported storage backends.

Each backend is imported behind a ``try/except ModuleNotFoundError`` guard so
the absence of an optional dependency (e.g. ``redis``, ``h5py``, ``boto3``)
only hides that backend's ``*Pool`` class instead of breaking ``import laila``.
Install the matching extra (e.g. ``pip install laila-core[redis]``) to enable
a backend.
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
