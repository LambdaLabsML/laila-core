/** Backblaze B2 pool implementation using the S3-compatible API. */
import { ImportError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { BotoPool, BotocoreConfig, aioboto3, boto3 } from "../boto/boto.js";

/**
 * Backblaze B2-backed pool using the S3-compatible API.
 */
export class BackblazePool extends BotoPool {
  static {
    define_fields(this, {
      application_key_id: ["str"],
      application_key: ["str"],
      endpoint_url: ["str", Field({ default: "https://s3.us-west-004.backblazeb2.com" })],
    });
    define_private(this, {
      _no_such_key_codes: PrivateAttr({ default_factory: () => new Set(["NoSuchKey", "404"]) }),
    });
  }

  /** Return a boto3 S3 client configured for Backblaze B2. */
  _get_client() {
    if (this._client !== null && this._client !== undefined) return this._client;
    if (boto3 === null || BotocoreConfig === null) throw new ImportError("boto3 is required for BackblazePool");
    this._client = boto3.client("s3", {
      endpoint_url: this.endpoint_url,
      aws_access_key_id: this.application_key_id,
      aws_secret_access_key: this.application_key,
      config: BotocoreConfig({
        signature_version: "s3v4",
        retries: { max_attempts: 3, mode: "standard" },
      }),
    });
    return this._client;
  }

  /** Return a cached ``aioboto3.Session`` carrying the B2 application key. */
  _get_aio_session() {
    if (aioboto3 === null) {
      throw new ImportError("aioboto3 is required for the async B2 path; install with `pip install aioboto3`");
    }
    if (this._aio_session === null || this._aio_session === undefined) {
      this._aio_session = new aioboto3.Session({
        aws_access_key_id: this.application_key_id,
        aws_secret_access_key: this.application_key,
      });
    }
    return this._aio_session;
  }

  /** Point the aioboto3 client at the B2 endpoint instead of AWS. */
  _aio_client_kwargs() {
    return { endpoint_url: this.endpoint_url };
  }
}

register("laila.data.backblaze.backblaze", { BackblazePool });
