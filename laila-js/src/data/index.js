/**
 * Data containers: storage pools for every supported backend, plus MultiBuffer.
 *
 * Everything here derives from the virtual
 * ``_LAILA_IDENTIFIABLE_DATA_CONTAINER`` (``laila/data/schema/data_container``).
 * Two families exist:
 *
 * - **Pools** (``_LAILA_IDENTIFIABLE_POOL`` subclasses) -- maps keyed by
 *   entry ``global_id``; the persistence tier behind ``memorize`` /
 *   ``remember`` / ``forget``.
 * - ``MultiBuffer`` -- an integer-indexed ring with independent
 *   read/write heads, used as the proxy to a device's own buffer (e.g.
 *   a camera's double-buffered frames on a microcontroller).
 *
 * Each pool backend lives in its own sub-module and exposes a single
 * ``*Pool`` class:
 *
 * ================  ==============================================
 * Backend           Class
 * ================  ==============================================
 * Redis             ``RedisPool``
 * HDF5              ``HDF5Pool``
 * Cloudflare R2     ``CloudflarePool``
 * S3 (boto3)        ``S3Pool``
 * HuggingFace Hub   ``HuggingFacePool``
 * Filesystem        ``FilesystemPool``
 * Google Cloud      ``GCSPool``
 * Azure Blob        ``AzurePool``
 * SQLite            ``SQLitePool``
 * Postgres          ``PostgresPool``
 * MongoDB           ``MongoPool``
 * DuckDB            ``DuckDBPool``
 * BackBlaze B2      ``BackblazePool``
 * ================  ==============================================
 *
 * Every backend guards its client-library import (``optional_import``), so
 * the absence of an optional dependency (e.g. ``redis``, ``h5wasm``,
 * ``@aws-sdk/client-s3``) only makes that backend's ``*Pool`` raise
 * ``ImportError`` on construction instead of breaking ``import "laila"``.
 * Install the matching peer dependency to make a backend available.
 *
 * Pools are designed to be composed via the proxy operators
 * (``cache << origin`` / ``origin >> cache`` -- ``__lshift__`` / ``__rshift__``)
 * so users can stack a fast local tier in front of a slower remote tier
 * without the rest of the codebase ever knowing.
 */

export { MultiBuffer } from "./multibuffer/multibuffer.js";

export { RedisPool } from "./redis/redis.js";
export { HDF5Pool } from "./hdf5/hdf5.js";
export { CloudflarePool } from "./cloudflare/cloudflare.js";
export { S3Pool } from "./s3/s3.js";
export { HuggingFacePool } from "./huggingface/huggingface.js";
export { FilesystemPool } from "./filesystem/filesystem.js";
export { GCSPool } from "./gcs/gcs.js";
export { AzurePool } from "./azure/azure.js";
export { SQLitePool } from "./sqlite/sqlite.js";
export { PostgresPool } from "./postgres/postgres.js";
export { MongoPool } from "./mongo/mongo.js";
export { DuckDBPool } from "./duckdb/duckdb.js";
export { BackblazePool } from "./backblaze/backblaze.js";
