/** Cloudflare R2 pool implementation. */
import { ImportError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { Field, define_fields } from "../../_compat/pydantic.js";
import { BotoPool, BotocoreConfig, aioboto3, boto3 } from "../boto/boto.js";

/**
 * Cloudflare R2-backed pool. Uploads and downloads key-value data from R2.
 *
 * Uses boto3 S3 API with R2 endpoint. Objects are keyed by entry global_id
 * directly (no pool directory). Use one bucket per pool.
 */
export class CloudflarePool extends BotoPool {
  static {
    define_fields(this, {
      account_id: ["str"],
      access_key_id: ["str"],
      secret_access_key: ["str"],
    });
  }

  /** The R2 S3-compatible endpoint derived from ``account_id``. */
  get endpoint_url() {
    return `https://${this.account_id}.r2.cloudflarestorage.com`;
  }

  /** Return a boto3 S3 client configured for Cloudflare R2. */
  _get_client() {
    if (this._client !== null && this._client !== undefined) return this._client;
    if (boto3 === null || BotocoreConfig === null) throw new ImportError("boto3 is required for CloudflarePool");
    this._client = boto3.client("s3", {
      endpoint_url: this.endpoint_url,
      aws_access_key_id: this.access_key_id,
      aws_secret_access_key: this.secret_access_key,
      config: BotocoreConfig({
        signature_version: "s3v4",
        retries: { max_attempts: 3, mode: "standard" },
      }),
    });
    return this._client;
  }

  /** Return a cached ``aioboto3.Session`` carrying the R2 API token. */
  _get_aio_session() {
    if (aioboto3 === null) {
      throw new ImportError("aioboto3 is required for the async R2 path; install with `pip install aioboto3`");
    }
    if (this._aio_session === null || this._aio_session === undefined) {
      this._aio_session = new aioboto3.Session({
        aws_access_key_id: this.access_key_id,
        aws_secret_access_key: this.secret_access_key,
      });
    }
    return this._aio_session;
  }

  /** Point the aioboto3 client at the R2 endpoint instead of AWS. */
  _aio_client_kwargs() {
    return { endpoint_url: this.endpoint_url };
  }
}

register("laila.data.cloudflare.cloudflare", { CloudflarePool });
