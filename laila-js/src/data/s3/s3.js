/**
 * AWS S3 pool implementation.
 *
 * Thin specialisation of ``BotoPool`` that points at the AWS S3
 * endpoint with AWS-style v4 signatures. The bulk of the read/write/
 * delete logic lives in the parent class -- this file just plugs in
 * the right ``boto3.client`` factory and the matching
 * ``aioboto3.Session`` for the async path.
 */
import { ImportError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { Field, define_fields } from "../../_compat/pydantic.js";
import { BotoPool, BotocoreConfig, aioboto3, boto3 } from "../boto/boto.js";

/**
 * AWS S3-backed pool.
 *
 * Uploads and downloads serialised entries against a single S3
 * bucket -- one bucket per pool, objects keyed by entry
 * ``global_id``. Sync paths use the standard ``boto3`` client;
 * async paths (``_read_async`` / ``_write_async`` / ...) use
 * ``aioboto3`` for true non-blocking I/O.
 *
 * Fields
 * ------
 * access_key_id : str, optional
 *     AWS access key id. Falls back to the standard boto3 credential
 *     provider chain (env vars, ``~/.aws/credentials``, IAM role
 *     metadata, ...) when omitted.
 * secret_access_key : str, optional
 *     AWS secret access key. See ``access_key_id`` for the fallback rules.
 * region_name : str, optional
 *     AWS region for the bucket. When omitted, boto3 picks the
 *     region from its standard configuration sources.
 */
export class S3Pool extends BotoPool {
  static {
    define_fields(this, {
      access_key_id: ["str | None", Field({ default: null })],
      secret_access_key: ["str | None", Field({ default: null })],
      region_name: ["str | None", Field({ default: null })],
    });
  }

  /**
   * Return a cached ``boto3.client`` instance for AWS S3.
   *
   * Configured with SigV4 signing, a three-attempt standard retry
   * policy, and a connection pool sized by ``max_pool_connections``.
   * Subsequent calls return the cached client.
   */
  _get_client() {
    if (this._client !== null && this._client !== undefined) return this._client;
    if (boto3 === null || BotocoreConfig === null) throw new ImportError("boto3 is required for S3Pool");
    const kwargs = {
      config: BotocoreConfig({
        signature_version: "s3v4",
        retries: { max_attempts: 3, mode: "standard" },
        max_pool_connections: this.max_pool_connections,
      }),
    };
    if (this.access_key_id !== null && this.access_key_id !== undefined) kwargs.aws_access_key_id = this.access_key_id;
    if (this.secret_access_key !== null && this.secret_access_key !== undefined) kwargs.aws_secret_access_key = this.secret_access_key;
    if (this.region_name !== null && this.region_name !== undefined) kwargs.region_name = this.region_name;
    this._client = boto3.client("s3", kwargs);
    return this._client;
  }

  /**
   * Return a cached ``aioboto3.Session`` carrying this pool's AWS credentials.
   *
   * Used by the async path. Raises ``ImportError`` with an actionable
   * hint if the optional dependency is missing.
   */
  _get_aio_session() {
    if (aioboto3 === null) {
      throw new ImportError("aioboto3 is required for the async S3 path; install with `pip install aioboto3`");
    }
    if (this._aio_session === null || this._aio_session === undefined) {
      const kwargs = {};
      if (this.access_key_id !== null && this.access_key_id !== undefined) kwargs.aws_access_key_id = this.access_key_id;
      if (this.secret_access_key !== null && this.secret_access_key !== undefined) kwargs.aws_secret_access_key = this.secret_access_key;
      if (this.region_name !== null && this.region_name !== undefined) kwargs.region_name = this.region_name;
      this._aio_session = new aioboto3.Session(kwargs);
    }
    return this._aio_session;
  }
}

register("laila.data.s3.s3", { S3Pool });
