/**
 * Abstract boto3-based S3-compatible pool implementation.
 *
 * ``BotoPool`` is the shared chassis used by every S3-API-compatible
 * backend in laila (AWS S3, Cloudflare R2, BackBlaze B2, ...).
 * Concrete subclasses provide the bucket-vendor-specific
 * ``_get_client`` factory; everything else -- key encoding, throttling,
 * async-via-aioboto3, connection-pool sizing, multi-loop client
 * caching, and pool teardown -- is handled here.
 *
 * Sync vs async paths
 * -------------------
 * Every method has both a sync (``_read``, ``_write``, ...)
 * implementation that drives a regular boto3 client and an async
 * (``_read_async``, ...) implementation that drives an aioboto3
 * client when ``async_default`` is ``true``. With
 * ``async_default=false`` the async paths fall back to the inherited
 * default of running the sync call inline on the calling event loop.
 *
 * Per-loop async client caching
 * -----------------------------
 * ``aioboto3`` clients are bound to the event loop they're created on,
 * so we cache one client *per loop* in ``_aio_clients``. That keeps
 * the underlying connector pool warm (and respects
 * ``max_pool_connections``) without leaking clients across loops --
 * critical when laila spins up multiple async taskforces, each with
 * its own loop.
 *
 * Throttling
 * ----------
 * Optional simple per-call sleep (``_throttle`` / ``_athrottle``)
 * to keep the pool under a request-per-second cap. Defaults to no
 * throttling.
 *
 * JavaScript client library
 * -------------------------
 * ``boto3`` / ``aioboto3`` map onto ``@aws-sdk/client-s3``. The SDK is
 * promise-only, so the "boto3" client used by the sync hooks is a thin
 * adapter (``_Boto3Client``) that drives the same ``S3Client`` through
 * ``block_on`` -- the sync client call that would block the thread in
 * Python -- while the "aioboto3" client (``_AioBotoClient``) exposes the
 * promise-returning operations directly. Both speak the boto3 operation
 * vocabulary (``get_object`` / ``put_object`` / ``delete_object`` /
 * ``head_object`` / ``get_paginator("list_objects_v2")``) so the hook
 * bodies read exactly like the Python ones.
 */
import https from "node:https";

import * as asyncio from "../../_compat/asyncio.js";
import { block_on } from "../../_compat/blocking.js";
import { asynccontextmanager, with_async } from "../../_compat/contextlib.js";
import { ImportError, NotImplementedError, PyException, TypeError as PyTypeError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { optional_import } from "../../_compat/optional.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { id, is_str, isdict } from "../../_compat/pytypes.js";
import * as time from "../../_compat/time.js";
import { quote, unquote } from "../../_compat/urlquote.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

// ``try: import boto3 / from botocore.config import Config / except ImportError``
const _client_s3 = optional_import("@aws-sdk/client-s3");
// ``@smithy/node-http-handler`` ships with the SDK; it sizes the socket pool.
const _node_http_handler = _client_s3 ? optional_import("@smithy/node-http-handler") : null;

/**
 * ``botocore.exceptions.ClientError``: wraps an SDK service error so callers
 * can inspect ``e.response["Error"]["Code"]`` exactly as with botocore.
 */
export class ClientError extends PyException {
  /**
   * @param {any} error the ``@aws-sdk`` error
   * @param {string} operation_name boto3 operation name (``"GetObject"``)
   */
  constructor(error, operation_name) {
    const code = ClientError._code_of(error);
    const message = error && error.message !== undefined ? String(error.message) : String(error);
    super(`An error occurred (${code}) when calling the ${operation_name} operation: ${message}`);
    this.operation_name = operation_name;
    this.response = {
      Error: { Code: code, Message: message },
      ResponseMetadata: { HTTPStatusCode: error && error.$metadata ? error.$metadata.httpStatusCode : undefined },
    };
    this.original = error;
  }

  /** botocore's error code: the parsed XML ``<Code>``, else the HTTP status. */
  static _code_of(error) {
    if (error && typeof error.Code === "string") return error.Code;
    const status = error && error.$metadata ? error.$metadata.httpStatusCode : undefined;
    if (error && typeof error.name === "string" && error.name !== "Error" && !/^\d+$/.test(error.name)) {
      // botocore reports bare HEAD failures (no body to parse) by status code.
      if (error.name === "NotFound" && status === 404) return "404";
      return error.name;
    }
    if (status !== undefined && status !== null) return String(status);
    return "Unknown";
  }
}

/** ``botocore.config.Config(...)`` -- plain options object. */
export function BotocoreConfig(opts = {}) {
  return {
    signature_version: opts.signature_version ?? null,
    retries: opts.retries ?? null,
    max_pool_connections: opts.max_pool_connections ?? 10,
  };
}

// Region lookup chain (env vars, shared config file) -- also ships with the
// SDK: ``@smithy/core/config`` on current releases, two separate packages on
// older ones.
function _region_chain_module() {
  if (_client_s3 === null) return null;
  const core = optional_import("@smithy/core", { file: "dist-cjs/submodules/config/index.js" });
  if (core && core.loadConfig && core.NODE_REGION_CONFIG_OPTIONS) return core;
  const provider = optional_import("@smithy/node-config-provider");
  const resolver = optional_import("@smithy/config-resolver");
  if (provider && resolver && provider.loadConfig && resolver.NODE_REGION_CONFIG_OPTIONS) {
    return {
      loadConfig: provider.loadConfig,
      NODE_REGION_CONFIG_OPTIONS: resolver.NODE_REGION_CONFIG_OPTIONS,
      NODE_REGION_CONFIG_FILE_OPTIONS: resolver.NODE_REGION_CONFIG_FILE_OPTIONS,
    };
  }
  return null;
}
const _region_config = _region_chain_module();

/**
 * botocore's region resolution for S3: the configured region when one is
 * set (``AWS_REGION`` / ``AWS_DEFAULT_REGION`` / ``~/.aws/config``), else
 * ``us-east-1``. Returned as an SDK region *provider*.
 */
function _default_region_provider() {
  const chain =
    _region_config !== null
      ? _region_config.loadConfig(_region_config.NODE_REGION_CONFIG_OPTIONS, _region_config.NODE_REGION_CONFIG_FILE_OPTIONS)
      : null;
  return async () => {
    if (chain !== null) {
      try {
        const region = await chain();
        if (region) return region;
      } catch {
        /* no configured region */
      }
    }
    return "us-east-1";
  };
}

/** ``isinstance(x, ClientError)`` for both wrapped and raw SDK errors. */
function _is_client_error(e) {
  return e instanceof ClientError;
}

/**
 * Translate boto3 ``client("s3", **kwargs)`` keyword arguments into an
 * ``S3Client`` configuration.
 *
 * @param {{config?: object, endpoint_url?: string, aws_access_key_id?: string,
 *   aws_secret_access_key?: string, region_name?: string}} kwargs
 */
export function _s3_client_config(kwargs) {
  const config = kwargs.config ?? BotocoreConfig();
  const cfg = {};
  if (kwargs.endpoint_url !== undefined && kwargs.endpoint_url !== null) cfg.endpoint = kwargs.endpoint_url;
  // botocore: explicit ``region_name`` > env / ``~/.aws/config`` > ``us-east-1`` (S3's global default).
  cfg.region = kwargs.region_name !== undefined && kwargs.region_name !== null ? kwargs.region_name : _default_region_provider();
  if (kwargs.aws_access_key_id !== undefined && kwargs.aws_access_key_id !== null) {
    cfg.credentials = {
      accessKeyId: kwargs.aws_access_key_id,
      secretAccessKey: kwargs.aws_secret_access_key,
    };
  }
  if (config.retries && config.retries.max_attempts !== undefined) {
    cfg.maxAttempts = config.retries.max_attempts;
    if (config.retries.mode) cfg.retryMode = config.retries.mode;
  }
  const max_sockets = config.max_pool_connections;
  const agent_opts = { keepAlive: true, maxSockets: max_sockets };
  if (_node_http_handler && _node_http_handler.NodeHttpHandler) {
    cfg.requestHandler = new _node_http_handler.NodeHttpHandler({ httpsAgent: new https.Agent(agent_opts) });
  } else {
    cfg.requestHandler = { httpsAgent: new https.Agent(agent_opts) };
  }
  return cfg;
}

/**
 * Promise-based S3 operations in boto3 vocabulary (the ``aioboto3`` client).
 */
export class _AioBotoClient {
  /** @param {any} s3 an ``S3Client`` */
  constructor(s3) {
    this._s3 = s3;
  }

  async _send(operation_name, Command, params) {
    try {
      return await this._s3.send(new Command(params));
    } catch (e) {
      if (e && typeof e === "object" && "$metadata" in e) throw new ClientError(e, operation_name);
      throw e;
    }
  }

  /** ``await client.get_object(Bucket=..., Key=...)`` -> ``{"Body": body}`` with ``await body.read()``. */
  async get_object(params) {
    const resp = await this._send("GetObject", _client_s3.GetObjectCommand, params);
    const body = resp.Body;
    return {
      ...resp,
      Body: {
        async read() {
          return Buffer.from(await body.transformToByteArray());
        },
      },
    };
  }

  put_object(params) {
    return this._send("PutObject", _client_s3.PutObjectCommand, params);
  }

  delete_object(params) {
    return this._send("DeleteObject", _client_s3.DeleteObjectCommand, params);
  }

  head_object(params) {
    return this._send("HeadObject", _client_s3.HeadObjectCommand, params);
  }

  list_objects_v2(params) {
    return this._send("ListObjectsV2", _client_s3.ListObjectsV2Command, params);
  }

  /** ``client.get_paginator("list_objects_v2")`` -- ``paginate`` is an async generator. */
  get_paginator(operation_name) {
    if (operation_name !== "list_objects_v2") throw new NotImplementedError(`paginator for ${operation_name}`);
    const self = this;
    return {
      async *paginate(params) {
        let token;
        for (;;) {
          const page = await self.list_objects_v2(token === undefined ? params : { ...params, ContinuationToken: token });
          yield page;
          if (!page.IsTruncated || !page.NextContinuationToken) return;
          token = page.NextContinuationToken;
        }
      },
    };
  }

  /** Release the socket pool (``S3Client.destroy``). */
  close() {
    this._s3.destroy();
  }
}

/**
 * Blocking S3 operations in boto3 vocabulary (the ``boto3`` client): every
 * call drives the promise-based SDK through ``block_on``.
 */
export class _Boto3Client {
  /** @param {any} s3 an ``S3Client`` */
  constructor(s3) {
    this._aio = new _AioBotoClient(s3);
  }

  /** ``client.get_object(...)["Body"].read()`` returns ``bytes`` (a Buffer). */
  get_object(params) {
    const resp = block_on(this._aio.get_object(params));
    const body = resp.Body;
    return {
      ...resp,
      Body: {
        read() {
          return block_on(body.read());
        },
      },
    };
  }

  put_object(params) {
    return block_on(this._aio.put_object(params));
  }

  delete_object(params) {
    return block_on(this._aio.delete_object(params));
  }

  head_object(params) {
    return block_on(this._aio.head_object(params));
  }

  list_objects_v2(params) {
    return block_on(this._aio.list_objects_v2(params));
  }

  /** ``client.get_paginator("list_objects_v2")`` -- ``paginate`` is a sync generator. */
  get_paginator(operation_name) {
    if (operation_name !== "list_objects_v2") throw new NotImplementedError(`paginator for ${operation_name}`);
    const self = this;
    return {
      *paginate(params) {
        let token;
        for (;;) {
          const page = self.list_objects_v2(token === undefined ? params : { ...params, ContinuationToken: token });
          yield page;
          if (!page.IsTruncated || !page.NextContinuationToken) return;
          token = page.NextContinuationToken;
        }
      },
    };
  }

  close() {
    this._aio.close();
  }
}

/** ``boto3.client("s3", **kwargs)`` */
export const boto3 =
  _client_s3 === null
    ? null
    : {
        client(service_name, kwargs = {}) {
          if (service_name !== "s3") throw new NotImplementedError(`boto3.client(${JSON.stringify(service_name)})`);
          return new _Boto3Client(new _client_s3.S3Client(_s3_client_config(kwargs)));
        },
      };

/**
 * ``aioboto3.Session(**credentials)``: ``session.client("s3", **kwargs)``
 * returns an async context manager yielding an ``_AioBotoClient``.
 */
export class _AioSession {
  constructor(kwargs = {}) {
    this._kwargs = { ...kwargs };
  }

  client(service_name, kwargs = {}) {
    if (service_name !== "s3") throw new NotImplementedError(`Session.client(${JSON.stringify(service_name)})`);
    const merged = { ...this._kwargs, ...kwargs };
    let client = null;
    return {
      async __aenter__() {
        client = new _AioBotoClient(new _client_s3.S3Client(_s3_client_config(merged)));
        return client;
      },
      async __aexit__() {
        if (client !== null) client.close();
        client = null;
        return false;
      },
    };
  }
}

/** ``aioboto3`` module stand-in (``null`` when the SDK is not installed). */
export const aioboto3 = _client_s3 === null ? null : { Session: _AioSession };

/**
 * Abstract base for pools backed by a boto3 S3-compatible client.
 *
 * Subclasses must define their own fields and implement ``_get_client``
 * to return a configured ``boto3`` S3 client.
 *
 * When ``async_default`` is ``true`` (the default) the async hooks
 * (``_read_async`` / ``_write_async`` / ``_delete_async`` /
 * ``_exists_async``) use ``aioboto3`` for true non-blocking I/O --
 * hundreds of concurrent in-flight HTTP requests on a single thread
 * via a shared connection pool. When ``async_default`` is
 * ``false``, the async hooks fall back to running the sync ``boto3``
 * call inline on the calling loop (which blocks that loop for the
 * duration of the HTTP round-trip).
 *
 * Connection-pool sizing
 * ----------------------
 * ``max_pool_connections`` is forwarded to ``botocore.config.Config``
 * for both the sync and the async client, so a single client can hold
 * open many concurrent HTTPS connections to S3 (botocore's default of
 * 10 is the bottleneck for high-fanout async workloads). For the async
 * path, the ``aioboto3`` client is cached *per event loop*: every
 * coroutine running on the same loop reuses one client, and therefore
 * one warmed connection pool, rather than constructing a new one per
 * call.
 */
export class BotoPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      bucket_name: ["str"],
      max_req_per_second: ["float | None", Field({ default: null })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
      async_default: [
        "bool",
        Field({
          default: true,
          description:
            "When True, async hooks use aioboto3 (true non-blocking I/O via aiohttp). " +
            "When False, async hooks fall back to sync boto3 inline, blocking the calling loop.",
        }),
      ],
      max_pool_connections: [
        "int",
        Field({
          default: 128,
          ge: 1,
          description:
            "Maximum number of concurrent HTTPS connections the underlying " +
            "botocore/aiohttp pool is allowed to keep open per client. " +
            "Forwarded to botocore.config.Config(max_pool_connections=...). " +
            "The botocore default is 10, which caps high-fanout async I/O.",
        }),
      ],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
      _aio_session: PrivateAttr({ default: null }),
      _aio_session_lock: PrivateAttr({ default: null }),
      // Per-event-loop cached aioboto3 clients: loop_id -> [client, ctx_mgr, loop].
      // Cached in the loop's own thread, drained on close().
      _aio_clients: PrivateAttr({ default_factory: () => new Map() }),
      _aio_client_locks: PrivateAttr({ default_factory: () => new Map() }),
      _no_such_key_codes: PrivateAttr({ default_factory: () => new Set(["NoSuchKey"]) }),
    });
  }

  /**
   * Return a configured boto3 S3 client.
   *
   * @throws {NotImplementedError} Must be overridden by subclasses.
   */
  _get_client() {
    throw new NotImplementedError("Subclasses must implement _get_client");
  }

  /**
   * Return (and lazily build) the cached ``aioboto3.Session``.
   *
   * Subclasses may override to inject credentials. The default
   * constructs a session with no explicit credentials so the standard
   * AWS credential chain (env vars, ~/.aws/credentials, instance role)
   * applies.
   */
  _get_aio_session() {
    if (aioboto3 === null) {
      throw new ImportError("aioboto3 is required for the async path; install with `pip install aioboto3`");
    }
    if (this._aio_session === null || this._aio_session === undefined) {
      this._aio_session = new aioboto3.Session();
    }
    return this._aio_session;
  }

  /**
   * Return the ``BotocoreConfig`` used for every aioboto3 client.
   *
   * Honors ``max_pool_connections`` so the underlying connection pool is
   * sized to actually accommodate the configured concurrency.
   */
  _aio_client_config() {
    return BotocoreConfig({
      signature_version: "s3v4",
      retries: { max_attempts: 3, mode: "standard" },
      max_pool_connections: this.max_pool_connections,
    });
  }

  /**
   * Extra keyword arguments for ``session.client("s3", ...)`` on the async path.
   *
   * The default is empty (plain AWS S3). S3-compatible vendors override
   * this to inject their ``endpoint_url`` so the aioboto3 client talks to
   * the same host as the sync ``_get_client``.
   *
   * @returns {Record<string, any>}
   */
  _aio_client_kwargs() {
    return {};
  }

  /**
   * Yield a shared aioboto3 S3 client cached on the running event loop.
   *
   * The first call from a given loop enters an ``async with
   * session.client("s3", ...)`` context and stores the client in
   * ``_aio_clients``; subsequent calls on the same loop reuse it
   * directly so the connection pool stays warm and obeys the
   * configured ``max_pool_connections`` cap. The client is *not*
   * torn down on context exit -- ``close()`` is responsible for
   * draining all cached clients.
   */
  _aio_client() {
    const self = this;
    return asynccontextmanager(async function* _aio_client() {
      const client = await self._get_shared_aio_client();
      yield client;
    })();
  }

  /** Return (lazily creating) the aioboto3 S3 client for the current loop. */
  async _get_shared_aio_client() {
    const loop = asyncio.get_running_loop();
    const loop_id = id(loop);

    let cached = this._aio_clients.get(loop_id);
    if (cached !== undefined) return cached[0];

    // asyncio.Lock is bound to the loop it's first awaited on; create
    // one per loop. setdefault is GIL-atomic so two coroutines on the
    // same loop will agree on the same lock instance.
    let lock = this._aio_client_locks.get(loop_id);
    if (lock === undefined) {
      lock = new asyncio.Lock();
      this._aio_client_locks.set(loop_id, lock);
    }

    return await with_async(lock, async () => {
      cached = this._aio_clients.get(loop_id);
      if (cached !== undefined) return cached[0];

      const session = this._get_aio_session();
      const ctx = session.client("s3", { config: this._aio_client_config(), ...this._aio_client_kwargs() });
      const client = await ctx.__aenter__();
      this._aio_clients.set(loop_id, [client, ctx, loop]);
      return client;
    });
  }

  /** URL-encode a logical key for S3. */
  _object_key(key) {
    return quote(key, { safe: "" });
  }

  /** Decode an S3 object key back to the logical key. */
  _logical_key(object_key) {
    return unquote(object_key);
  }

  /** Sleep to enforce the per-pool request rate cap. */
  _throttle() {
    if (this.max_req_per_second !== null && this.max_req_per_second !== undefined) {
      time.sleep(1.0 / this.max_req_per_second);
    }
  }

  /** Async sleep variant of ``_throttle`` -- yields to the event loop. */
  async _athrottle() {
    if (this.max_req_per_second !== null && this.max_req_per_second !== undefined) {
      await asyncio.sleep(1.0 / this.max_req_per_second);
    }
  }

  /**
   * Release the boto3 client and tear down all cached aioboto3 clients.
   *
   * Each cached aioboto3 client lives on its own event loop, so we
   * schedule ``ctx.__aexit__`` back onto that loop. If the loop is no
   * longer running (e.g. ``laila.terminate`` shut the taskforces
   * first), fall back to draining the client on a private throwaway
   * loop so the connector doesn't leak.
   */
  close() {
    if (this._client !== null && this._client !== undefined && typeof this._client.close === "function") {
      try {
        this._client.close();
      } catch {
        /* best effort */
      }
    }
    this._client = null;
    for (const [, [, ctx, loop]] of [...this._aio_clients.entries()]) {
      try {
        if (loop !== null && loop !== undefined && loop.is_running()) {
          try {
            block_on(ctx.__aexit__(null, null, null), 5);
          } catch {
            /* pass */
          }
        } else {
          // Loop is already stopped; the aioboto3 client+connector still
          // hold sockets. Drain on a fresh loop so we close cleanly
          // instead of leaking.
          try {
            block_on(ctx.__aexit__(null, null, null));
          } catch {
            /* pass */
          }
        }
      } catch {
        /* pass */
      }
    }
    this._aio_clients.clear();
    this._aio_client_locks.clear();
    this._aio_session = null;
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    this._throttle();
    try {
      const resp = this._get_client().get_object({
        Bucket: this.bucket_name,
        Key: this._object_key(key),
      });
      const raw = resp.Body.read().toString("utf-8");
      return json.loads(raw);
    } catch (e) {
      if (_is_client_error(e)) {
        if (this._no_such_key_codes.has((e.response.Error ?? {}).Code)) return null;
      }
      throw e;
    }
  }

  /** Store *entry* as a JSON object under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError(`${this.constructor.name} expects a serialized JSON string.`);

    this._throttle();
    this._get_client().put_object({
      Bucket: this.bucket_name,
      Key: this._object_key(key),
      Body: Buffer.from(value, "utf-8"),
      ContentType: "application/json",
    });
  }

  /** Delete the object for *key*. */
  _delete(key) {
    this._throttle();
    this._get_client().delete_object({
      Bucket: this.bucket_name,
      Key: this._object_key(key),
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    const paginator = this._get_client().get_paginator("list_objects_v2");
    for (const page of paginator.paginate({ Bucket: this.bucket_name })) {
      for (const obj of page.Contents ?? []) {
        this._throttle();
        this._get_client().delete_object({
          Bucket: this.bucket_name,
          Key: obj.Key,
        });
      }
    }
  }

  /** Return ``true`` if *key* exists in the bucket. */
  _exists(key) {
    this._throttle();
    try {
      this._get_client().head_object({
        Bucket: this.bucket_name,
        Key: this._object_key(key),
      });
      return true;
    } catch {
      return false;
    }
  }

  // -------- Async hooks: true async via aioboto3 when async_default=True --------

  /**
   * The ``async_default=False`` fallback: drive the *sync* boto3 client
   * from the async hooks.
   *
   * Python runs the sync call inline, blocking the loop. The JS boto3
   * client is a ``block_on`` adapter over the promise-based SDK and a
   * blocking wait is impossible from inside a promise job, so the same
   * client is driven through its promise view instead -- same client,
   * same configuration, no aioboto3 session, nothing cached in
   * ``_aio_clients``. A custom ``_get_client`` returning a foreign client
   * keeps the inherited inline behaviour.
   */
  async _fallback_read_async(key) {
    const client = this._get_client();
    if (!(client instanceof _Boto3Client)) return await super._read_async(key);
    await this._athrottle();
    try {
      const resp = await client._aio.get_object({ Bucket: this.bucket_name, Key: this._object_key(key) });
      const body = await resp.Body.read();
      return json.loads(body.toString("utf-8"));
    } catch (e) {
      if (_is_client_error(e)) {
        if (this._no_such_key_codes.has((e.response.Error ?? {}).Code)) return null;
      }
      throw e;
    }
  }

  async _fallback_write_async(key, entry) {
    const client = this._get_client();
    if (!(client instanceof _Boto3Client)) return await super._write_async(key, entry);
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError(`${this.constructor.name} expects a serialized JSON string.`);
    await this._athrottle();
    await client._aio.put_object({
      Bucket: this.bucket_name,
      Key: this._object_key(key),
      Body: Buffer.from(value, "utf-8"),
      ContentType: "application/json",
    });
  }

  async _fallback_delete_async(key) {
    const client = this._get_client();
    if (!(client instanceof _Boto3Client)) return await super._delete_async(key);
    await this._athrottle();
    await client._aio.delete_object({ Bucket: this.bucket_name, Key: this._object_key(key) });
  }

  async _fallback_exists_async(key) {
    const client = this._get_client();
    if (!(client instanceof _Boto3Client)) return await super._exists_async(key);
    await this._athrottle();
    try {
      await client._aio.head_object({ Bucket: this.bucket_name, Key: this._object_key(key) });
      return true;
    } catch {
      return false;
    }
  }

  /** Retrieve the JSON value for *key* asynchronously, or ``null`` if absent. */
  async _read_async(key) {
    if (!this.async_default || aioboto3 === null) return await this._fallback_read_async(key);
    await this._athrottle();
    try {
      return await with_async(this._aio_client(), async (client) => {
        const resp = await client.get_object({
          Bucket: this.bucket_name,
          Key: this._object_key(key),
        });
        const body = await resp.Body.read();
        return json.loads(body.toString("utf-8"));
      });
    } catch (e) {
      if (_is_client_error(e)) {
        if (this._no_such_key_codes.has((e.response.Error ?? {}).Code)) return null;
      }
      throw e;
    }
  }

  /** Store *entry* as a JSON object under *key* asynchronously. */
  async _write_async(key, entry) {
    if (!this.async_default || aioboto3 === null) return await this._fallback_write_async(key, entry);
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError(`${this.constructor.name} expects a serialized JSON string.`);
    await this._athrottle();
    await with_async(this._aio_client(), async (client) => {
      await client.put_object({
        Bucket: this.bucket_name,
        Key: this._object_key(key),
        Body: Buffer.from(value, "utf-8"),
        ContentType: "application/json",
      });
    });
  }

  /** Delete the object for *key* asynchronously. */
  async _delete_async(key) {
    if (!this.async_default || aioboto3 === null) return await this._fallback_delete_async(key);
    await this._athrottle();
    await with_async(this._aio_client(), async (client) => {
      await client.delete_object({
        Bucket: this.bucket_name,
        Key: this._object_key(key),
      });
    });
  }

  /** Return ``true`` if *key* exists in the bucket (async). */
  async _exists_async(key) {
    if (!this.async_default || aioboto3 === null) return await this._fallback_exists_async(key);
    await this._athrottle();
    try {
      return await with_async(this._aio_client(), async (client) => {
        await client.head_object({
          Bucket: this.bucket_name,
          Key: this._object_key(key),
        });
        return true;
      });
    } catch {
      return false;
    }
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the bucket.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const paginator = this._get_client().get_paginator("list_objects_v2");
    const self = this;

    function* _iter_keys() {
      for (const page of paginator.paginate({ Bucket: self.bucket_name })) {
        for (const obj of page.Contents ?? []) yield self._logical_key(obj.Key);
      }
    }

    if (!as_generator) return [..._iter_keys()];
    return _iter_keys();
  }
}

register("laila.data.boto.boto", { BotoPool, ClientError, BotocoreConfig, boto3, aioboto3 });
