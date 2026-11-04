//! Abstract boto3-based S3-compatible pool implementation.
//!
//! `BotoPool` is the shared chassis used by every S3-API-compatible
//! backend in laila (AWS S3, Cloudflare R2, BackBlaze B2, ...). Concrete
//! subclasses provide the bucket-vendor-specific `_get_client` factory
//! (here: [`BotoVendor::client_config`]); everything else -- key encoding,
//! throttling, connection reuse and pool teardown -- is handled here.
//!
//! Where Python drives a `boto3` client, the port drives [`S3Client`]: the
//! S3 REST API (`GetObject`, `PutObject`, `DeleteObject`, `HeadObject`,
//! `ListObjectsV2`) over one keep-alive HTTPS connection pool, every
//! request signed with AWS Signature Version 4 the way botocore signs it
//! (`signature_version="s3v4"`, `retries={"max_attempts": 3, "mode":
//! "standard"}`). Credentials and region follow boto3's provider chain:
//! explicit fields, then `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
//! `AWS_SESSION_TOKEN` / `AWS_DEFAULT_REGION`, then the shared
//! `~/.aws/credentials` / `~/.aws/config` files (`AWS_PROFILE`).
//!
//! Sync vs async paths
//! -------------------
//! Python's `async_default=True` routes the `_*_async` hooks through
//! `aioboto3` (true non-blocking I/O on a per-event-loop cached client);
//! `async_default=False` runs the sync call inline on the calling loop.
//! The port keeps both fields for constructor parity and always takes
//! the inline path: the pool's async hooks are the sync hooks awaited
//! on the caller's task (see `PARITY.md`).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::marker::PhantomData;

use parking_lot::Mutex;

use crate::_compat::pydantic::{LazyClass, ModelClass};
use crate::data::http::{self, Client, Response};
use crate::data::schema::base::{
    field_default, field_required, pool_class, private_default, PoolBackend, PoolObject,
    _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::{Arg, Args, IntoArg, LailaError, Result};

// ---------------------------------------------------------------------------
// The S3 REST client

/// What a vendor's `_get_client` configures on the boto3 client.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// `endpoint_url=` (S3-compatible vendors); `None` for AWS S3.
    pub endpoint_url: Option<String>,
    /// `region_name=`; resolved through the boto3 chain when `None`.
    pub region_name: Option<String>,
    /// `aws_access_key_id=`; resolved through the boto3 chain when `None`.
    pub access_key_id: Option<String>,
    /// `aws_secret_access_key=`.
    pub secret_access_key: Option<String>,
    /// `Config(max_pool_connections=..)`.
    pub max_pool_connections: i64,
}

/// `botocore.exceptions.ClientError`: the S3 error document of a failed
/// call (`e.response["Error"]["Code"]`).
#[derive(Debug, Clone)]
pub struct S3Error {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub operation: &'static str,
}

impl S3Error {
    fn from_response(resp: &Response, operation: &'static str) -> S3Error {
        let text = resp.text();
        let code = http::xml_text(&text, "Code")
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| resp.status.to_string());
        let message = http::xml_text(&text, "Message").unwrap_or_else(|| match resp.status {
            404 => String::from("Not Found"),
            403 => String::from("Forbidden"),
            _ => String::from("Unknown"),
        });
        S3Error {
            status: resp.status,
            code,
            message,
            operation,
        }
    }
    /// The exception botocore raises for this document.
    pub fn into_error(self) -> LailaError {
        LailaError::new(
            ExcKind::Exception,
            alloc::format!(
                "botocore.exceptions.ClientError: An error occurred ({}) when calling the {} operation: {}",
                self.code, self.operation, self.message
            ),
        )
    }
}

/// A result whose service-side failure is an [`S3Error`] (transport and
/// signing failures are the outer `LailaError`).
pub type S3Result<T> = Result<core::result::Result<T, S3Error>>;

/// The configured `boto3.client("s3")`: one bucket, SigV4 signing,
/// keep-alive connections.
pub struct S3Client {
    http: Client,
    bucket: String,
    /// `scheme://host[:port]` the requests go to (bucket already folded
    /// in for virtual-hosted addressing).
    base: String,
    /// The `Host` header value (what the signature covers).
    host: String,
    region: String,
    credentials: Option<Credentials>,
    path_style: bool,
}

impl core::fmt::Debug for S3Client {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("S3Client")
            .field("bucket", &self.bucket)
            .field("base", &self.base)
            .field("region", &self.region)
            .finish()
    }
}

#[derive(Clone)]
struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// `[profile]` sections of an AWS ini file -> `key = value` pairs.
fn aws_ini_section(path: &str, profile: &str) -> Vec<(String, String)> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut in_section = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = name.trim();
            in_section = name == profile
                || name == alloc::format!("profile {profile}")
                || (profile == "default" && name == "default");
            continue;
        }
        if in_section {
            if let Some((k, v)) = line.split_once('=') {
                out.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
    }
    out
}

fn ini_get(pairs: &[(String, String)], key: &str) -> Option<String> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
}

fn home_path(rel: &str) -> String {
    let home = env("HOME").unwrap_or_else(|| String::from("/"));
    alloc::format!("{home}/{rel}")
}

fn is_ip_or_localhost(host: &str) -> bool {
    host == "localhost"
        || host.parse::<core::net::IpAddr>().is_ok()
        || host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .map(|h| h.parse::<core::net::Ipv6Addr>().is_ok())
            .unwrap_or(false)
}

/// botocore's `check_dns_name`: a bucket that can be a hostname label
/// (virtual-hosted addressing); dotted names stay path-style over HTTPS
/// (the wildcard certificate covers one label only).
fn dns_compatible_bucket(bucket: &str, https: bool) -> bool {
    let n = bucket.len();
    if !(3..=63).contains(&n) {
        return false;
    }
    if bucket.contains("..") || bucket.starts_with('-') || bucket.ends_with('-') {
        return false;
    }
    if https && bucket.contains('.') {
        return false;
    }
    bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
}

impl S3Client {
    /// `boto3.client("s3", **kwargs)` for *bucket*.
    pub fn new(cfg: &S3Config, bucket: &str) -> Result<S3Client> {
        let profile = env("AWS_PROFILE").unwrap_or_else(|| String::from("default"));
        let credentials_file =
            env("AWS_SHARED_CREDENTIALS_FILE").unwrap_or_else(|| home_path(".aws/credentials"));
        let config_file = env("AWS_CONFIG_FILE").unwrap_or_else(|| home_path(".aws/config"));
        let shared_credentials = aws_ini_section(&credentials_file, &profile);
        let shared_config = aws_ini_section(&config_file, &profile);

        let credentials = match (&cfg.access_key_id, &cfg.secret_access_key) {
            (Some(ak), Some(sk)) => Some(Credentials {
                access_key: ak.clone(),
                secret_key: sk.clone(),
                session_token: env("AWS_SESSION_TOKEN"),
            }),
            _ => match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
                (Some(ak), Some(sk)) => Some(Credentials {
                    access_key: ak,
                    secret_key: sk,
                    session_token: env("AWS_SESSION_TOKEN"),
                }),
                _ => match (
                    ini_get(&shared_credentials, "aws_access_key_id"),
                    ini_get(&shared_credentials, "aws_secret_access_key"),
                ) {
                    (Some(ak), Some(sk)) => Some(Credentials {
                        access_key: ak,
                        secret_key: sk,
                        session_token: ini_get(&shared_credentials, "aws_session_token"),
                    }),
                    _ => None,
                },
            },
        };
        let region = cfg
            .region_name
            .clone()
            .or_else(|| env("AWS_REGION"))
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .or_else(|| ini_get(&shared_config, "region"))
            .unwrap_or_else(|| String::from("us-east-1"));

        let endpoint = cfg
            .endpoint_url
            .clone()
            .unwrap_or_else(|| alloc::format!("https://s3.{region}.amazonaws.com"));
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let (scheme, authority) = endpoint.split_once("://").ok_or_else(|| {
            LailaError::value_error(alloc::format!("Invalid endpoint: {endpoint}"))
        })?;
        let authority = authority.split('/').next().unwrap_or(authority);
        let host_only = authority
            .rsplit_once(':')
            .filter(|(h, p)| !h.contains(']') || p.chars().all(|c| c.is_ascii_digit()))
            .map(|(h, _)| h)
            .unwrap_or(authority);
        let https = scheme == "https";
        let path_style = is_ip_or_localhost(host_only) || !dns_compatible_bucket(bucket, https);
        let host = if path_style {
            authority.to_string()
        } else {
            alloc::format!("{bucket}.{authority}")
        };
        Ok(S3Client {
            http: Client::new(),
            bucket: bucket.to_string(),
            base: alloc::format!("{scheme}://{host}"),
            host,
            region,
            credentials,
            path_style,
        })
    }

    /// The bucket this client is bound to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Path-style (`host/bucket/key`) rather than virtual-hosted
    /// (`bucket.host/key`) addressing.
    pub fn path_style(&self) -> bool {
        self.path_style
    }

    fn url(&self, object_key: Option<&str>, query: &[(String, String)]) -> (String, String) {
        let mut path = if self.path_style {
            alloc::format!("/{}", http::uri_encode(&self.bucket, false))
        } else {
            String::new()
        };
        match object_key {
            Some(k) => {
                path.push('/');
                path.push_str(&http::uri_encode(k, false));
            }
            None => path.push('/'),
        }
        let qs = http::canonical_query(query);
        let mut url = alloc::format!("{}{path}", self.base);
        if !qs.is_empty() {
            url.push('?');
            url.push_str(&qs);
        }
        (url, path)
    }

    /// SigV4-sign one request: the headers to send.
    fn sign(
        &self,
        method: &str,
        canonical_uri: &str,
        query: &[(String, String)],
        content_type: Option<&str>,
        payload: &[u8],
    ) -> Result<Vec<(String, String)>> {
        self.sign_at(
            &http::amz_date(),
            &[],
            method,
            canonical_uri,
            query,
            content_type,
            payload,
        )
    }

    /// [`sign`](Self::sign) at a given `x-amz-date`, with `extra` headers
    /// also covered by the signature (the AWS reference vectors sign
    /// `range`); the production path passes now and none.
    #[allow(clippy::too_many_arguments)]
    fn sign_at(
        &self,
        amz_date: &str,
        extra: &[(String, String)],
        method: &str,
        canonical_uri: &str,
        query: &[(String, String)],
        content_type: Option<&str>,
        payload: &[u8],
    ) -> Result<Vec<(String, String)>> {
        let Some(creds) = &self.credentials else {
            return Err(LailaError::new(
                ExcKind::Exception,
                "botocore.exceptions.NoCredentialsError: Unable to locate credentials",
            ));
        };
        let amz_date = String::from(amz_date);
        let date = &amz_date[..8];
        let payload_hash = http::sha256_hex(payload);

        let mut headers: Vec<(String, String)> = Vec::new();
        headers.push((String::from("host"), self.host.clone()));
        if let Some(ct) = content_type {
            headers.push((String::from("content-type"), ct.to_string()));
        }
        headers.extend(extra.iter().cloned());
        headers.push((String::from("x-amz-content-sha256"), payload_hash.clone()));
        headers.push((String::from("x-amz-date"), amz_date.clone()));
        if let Some(token) = &creds.session_token {
            headers.push((String::from("x-amz-security-token"), token.clone()));
        }
        headers.sort();
        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| alloc::format!("{k}:{}\n", v.trim()))
            .collect();
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = alloc::format!(
            "{method}\n{canonical_uri}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            http::canonical_query(query)
        );
        let scope = alloc::format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = alloc::format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            http::sha256_hex(canonical_request.as_bytes())
        );
        let k_date = http::hmac_sha256(
            alloc::format!("AWS4{}", creds.secret_key).as_bytes(),
            date.as_bytes(),
        );
        let k_region = http::hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = http::hmac_sha256(&k_region, b"s3");
        let k_signing = http::hmac_sha256(&k_service, b"aws4_request");
        let signature = http::hex(&http::hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let authorization = alloc::format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            creds.access_key
        );
        // `host` is set by the transport from the URL.
        let mut out: Vec<(String, String)> =
            headers.into_iter().filter(|(k, _)| k != "host").collect();
        out.push((String::from("authorization"), authorization));
        Ok(out)
    }

    fn call(
        &self,
        method: &str,
        object_key: Option<&str>,
        query: &[(String, String)],
        content_type: Option<&str>,
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let (url, canonical_uri) = self.url(object_key, query);
        let payload = body.unwrap_or(&[]);
        self.http.send_with_retry(method, &url, body, || {
            self.sign(method, &canonical_uri, query, content_type, payload)
        })
    }

    fn check(resp: Response, operation: &'static str) -> core::result::Result<Response, S3Error> {
        if resp.ok() {
            Ok(resp)
        } else {
            Err(S3Error::from_response(&resp, operation))
        }
    }

    /// `client.get_object(Bucket=.., Key=..)["Body"].read()`
    pub fn get_object(&self, object_key: &str) -> S3Result<Vec<u8>> {
        let resp = self.call("GET", Some(object_key), &[], None, None)?;
        Ok(Self::check(resp, "GetObject").map(|r| r.body))
    }

    /// `client.put_object(Bucket=.., Key=.., Body=.., ContentType=..)`
    pub fn put_object(&self, object_key: &str, body: &[u8], content_type: &str) -> S3Result<()> {
        let resp = self.call("PUT", Some(object_key), &[], Some(content_type), Some(body))?;
        Ok(Self::check(resp, "PutObject").map(|_| ()))
    }

    /// `client.delete_object(Bucket=.., Key=..)` (204 whether or not the
    /// object existed).
    pub fn delete_object(&self, object_key: &str) -> S3Result<()> {
        let resp = self.call("DELETE", Some(object_key), &[], None, None)?;
        Ok(Self::check(resp, "DeleteObject").map(|_| ()))
    }

    /// `client.head_object(Bucket=.., Key=..)`
    pub fn head_object(&self, object_key: &str) -> S3Result<()> {
        let resp = self.call("HEAD", Some(object_key), &[], None, None)?;
        Ok(Self::check(resp, "HeadObject").map(|_| ()))
    }

    /// `client.get_paginator("list_objects_v2").paginate(Bucket=..)`:
    /// every object key of the bucket, page after page.
    pub fn list_object_keys(&self) -> S3Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = alloc::vec![(String::from("list-type"), String::from("2"))];
            if let Some(t) = &token {
                query.push((String::from("continuation-token"), t.clone()));
            }
            let resp = self.call("GET", None, &query, None, None)?;
            let resp = match Self::check(resp, "ListObjectsV2") {
                Ok(r) => r,
                Err(e) => return Ok(Err(e)),
            };
            let text = resp.text();
            // `<Key>` elements live only under `<Contents>`; `<CommonPrefixes>`
            // carries `<Prefix>`.
            keys.extend(http::xml_texts(&text, "Key"));
            let truncated = http::xml_text(&text, "IsTruncated")
                .map(|t| t.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            token = http::xml_text(&text, "NextContinuationToken").filter(|t| !t.is_empty());
            if !truncated || token.is_none() {
                break;
            }
        }
        Ok(Ok(keys))
    }
}

// ---------------------------------------------------------------------------
// The chassis

/// What distinguishes one S3-compatible vendor's pool class from another:
/// the class name, its `_get_client` configuration, its `NoSuchKey` codes
/// and any extra property (`CloudflarePool.endpoint_url`).
pub trait BotoVendor: Default + Send + Sync + 'static {
    /// `type(self).__name__`
    const CLASS_NAME: &'static str;
    /// The subclass's `_get_client`: how its fields configure the client.
    fn client_config(pool: &PoolObject) -> Result<S3Config>;
    /// `_no_such_key_codes` -- error codes meaning "the key is absent".
    fn no_such_key_codes() -> &'static [&'static str] {
        &["NoSuchKey"]
    }
    /// A vendor-specific property (`None`: not defined).
    fn getattr(_pool: &PoolObject, _name: &str) -> Option<Result<Arg>> {
        None
    }
}

/// `BotoPool`'s state: the cached client (`self._client`; `None` until the
/// first call and again after `close()`).
pub struct BotoBackend<V: BotoVendor> {
    client: Mutex<Option<Arc<S3Client>>>,
    _vendor: PhantomData<V>,
}

impl<V: BotoVendor> Default for BotoBackend<V> {
    fn default() -> Self {
        BotoBackend {
            client: Mutex::new(None),
            _vendor: PhantomData,
        }
    }
}

impl<V: BotoVendor> core::fmt::Debug for BotoBackend<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(V::CLASS_NAME)
    }
}

fn opt_str(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

impl<V: BotoVendor> BotoBackend<V> {
    /// `self._get_client()`: the cached client, created on first use.
    pub fn _get_client(&self, pool: &PoolObject) -> Result<Arc<S3Client>> {
        if let Some(c) = self.client.lock().as_ref() {
            return Ok(c.clone());
        }
        let cfg = V::client_config(pool)?;
        let bucket = opt_str(pool, "bucket_name").unwrap_or_default();
        let client = Arc::new(S3Client::new(&cfg, &bucket)?);
        *self.client.lock() = Some(client.clone());
        Ok(client)
    }

    /// `self._object_key(key)`: URL-encode a logical key for S3.
    pub fn _object_key(&self, key: &str) -> String {
        http::quote_key(key)
    }

    /// `self._logical_key(object_key)`: decode an S3 object key back to the
    /// logical key.
    pub fn _logical_key(&self, object_key: &str) -> String {
        http::unquote_key(object_key)
    }

    /// `self._throttle()`: sleep to enforce the per-pool request rate cap.
    pub fn _throttle(&self, pool: &PoolObject) {
        if let Some(rate) = pool.field("max_req_per_second").and_then(|v| v.as_f64()) {
            if rate > 0.0 {
                std::thread::sleep(core::time::Duration::from_secs_f64(1.0 / rate));
            }
        }
    }

    fn is_no_such_key(e: &S3Error) -> bool {
        V::no_such_key_codes().contains(&e.code.as_str())
    }
}

impl<V: BotoVendor> PoolBackend for BotoBackend<V> {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Release the boto3 client (and, in Python, drain the cached aioboto3
    /// clients).
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        *self.client.lock() = None;
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        self._throttle(pool);
        let client = self._get_client(pool)?;
        match client.get_object(&self._object_key(key))? {
            Ok(raw) => {
                let text = core::str::from_utf8(&raw)
                    .map_err(|e| LailaError::new(ExcKind::UnicodeDecodeError, e.to_string()))?;
                crate::_compat::pyjson::loads(text)
            }
            Err(e) if Self::is_no_such_key(&e) => Ok(Arg::None),
            Err(e) => Err(e.into_error()),
        }
    }

    /// Store *entry* as a JSON object under *key*.
    fn _write(&self, pool: &PoolObject, key: &str, entry: Arg) -> Result<()> {
        let value = match entry {
            Arg::Dict(_) => Arg::Str(
                crate::_compat::pyjson::dumps(
                    &entry,
                    &crate::_compat::pyjson::DumpsOptions::default(),
                )?
                .into(),
            ),
            other => other,
        };
        let Arg::Str(value) = value else {
            return Err(LailaError::type_error(alloc::format!(
                "{} expects a serialized JSON string.",
                V::CLASS_NAME
            )));
        };
        self._throttle(pool);
        let client = self._get_client(pool)?;
        client
            .put_object(&self._object_key(key), value.as_bytes(), "application/json")?
            .map_err(S3Error::into_error)
    }

    /// Delete the object for *key*.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        self._throttle(pool);
        let client = self._get_client(pool)?;
        client
            .delete_object(&self._object_key(key))?
            .map_err(S3Error::into_error)
    }

    /// Remove all entries from the pool.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let client = self._get_client(pool)?;
        let keys = client.list_object_keys()?.map_err(S3Error::into_error)?;
        for object_key in keys {
            self._throttle(pool);
            client
                .delete_object(&object_key)?
                .map_err(S3Error::into_error)?;
        }
        Ok(())
    }

    /// `True` if *key* exists in the bucket.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        self._throttle(pool);
        let client = self._get_client(pool)?;
        Ok(matches!(
            client.head_object(&self._object_key(key)),
            Ok(Ok(()))
        ))
    }

    /// Return all keys in the bucket.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let client = self._get_client(pool)?;
        let keys = client.list_object_keys()?.map_err(S3Error::into_error)?;
        Ok(keys.iter().map(|k| self._logical_key(k)).collect())
    }

    fn __getattr__(&self, pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_client" => Some(Ok(if self.client.lock().is_some() {
                Arg::Str("<botocore.client.S3>".into())
            } else {
                Arg::None
            })),
            "_no_such_key_codes" => Some(crate::python::builtins::set(Args::from(
                V::no_such_key_codes()
                    .iter()
                    .map(|c| Arg::Str((*c).into()))
                    .collect::<Vec<_>>()
                    .into_arg(),
            ))),
            "_aio_session" => Some(Ok(Arg::None)),
            "_aio_clients" | "_aio_client_locks" => {
                Some(Ok(crate::python::PyDictBuilder::new().finish()))
            }
            _ => V::getattr(pool, name),
        }
    }

    fn __call_method__(&self, pool: &PoolObject, name: &str, args: Args) -> Option<Result<Arg>> {
        let first = || -> Result<String> {
            args.positional
                .first()
                .map(|a| a.__str__())
                .ok_or_else(|| LailaError::type_error("missing 1 required positional argument"))
        };
        match name {
            "_object_key" => Some(first().map(|k| self._object_key(&k).into_arg())),
            "_logical_key" => Some(first().map(|k| self._logical_key(&k).into_arg())),
            "_throttle" => {
                self._throttle(pool);
                Some(Ok(Arg::None))
            }
            "_get_client" => Some(
                self._get_client(pool)
                    .map(|_| Arg::Str("<botocore.client.S3>".into())),
            ),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_get_client",
            "_object_key",
            "_logical_key",
            "_throttle",
        ]
    }
}

// ---------------------------------------------------------------------------
// The abstract class

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "BotoPool",
        "laila.data.boto.boto",
        alloc::vec![],
        alloc::vec![
            (String::from("bucket_name"), field_required("str")),
            (
                String::from("max_req_per_second"),
                field_default("float | None", Arg::None)
            ),
            (
                String::from("transformations"),
                field_default(
                    "TransformationSequence | None",
                    crate::entry::transformation_base64()
                )
            ),
            (
                String::from("async_default"),
                field_default("bool", Arg::Bool(true))
            ),
            (
                String::from("max_pool_connections"),
                field_default("int", Arg::Int(128))
            ),
        ],
        alloc::vec![
            (String::from("_client"), private_default(Arg::None)),
            (String::from("_aio_session"), private_default(Arg::None)),
            (
                String::from("_aio_session_lock"),
                private_default(Arg::None)
            ),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

/// `BotoPool` -- the abstract base class (no handle: `_get_client` raises
/// `NotImplementedError` in Python; here only subclasses have a backend).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BotoPool;

impl BotoPool {
    /// The class descriptor (the base of `S3Pool`, `CloudflarePool`,
    /// `BackblazePool`).
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
}

/// `BotoPool` as a class object.
pub fn class_object() -> Arg {
    BotoPool::__class__().as_pyclass().into_arg()
}

/// The vendor-neutral `Field(...)` of a `max_pool_connections` check
/// (`ge=1`), applied by the subclasses after construction.
pub fn validate_max_pool_connections(pool: &PoolObject) -> Result<()> {
    if let Some(n) = pool.field("max_pool_connections").and_then(|v| v.as_i64()) {
        if n < 1 {
            return Err(LailaError::new(
                ExcKind::ValidationError,
                alloc::format!(
                    "1 validation error for {}\nmax_pool_connections\n  Input should be greater than or equal to 1 [type=greater_than_equal, input_value={n}, input_type=int]",
                    pool.class_name()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS "Signature Calculations for the Authorization Header:
    /// Transferring Payload in a Single Chunk", example *GET Object*:
    /// `GET /test.txt` on `examplebucket.s3.amazonaws.com` with
    /// `Range: bytes=0-9` at `20130524T000000Z`, region `us-east-1`.
    #[test]
    fn sigv4_matches_the_aws_reference_vector() {
        let client = S3Client {
            http: Client::new(),
            bucket: String::from("examplebucket"),
            base: String::from("https://examplebucket.s3.amazonaws.com"),
            host: String::from("examplebucket.s3.amazonaws.com"),
            region: String::from("us-east-1"),
            credentials: Some(Credentials {
                access_key: String::from("AKIAIOSFODNN7EXAMPLE"),
                secret_key: String::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
                session_token: None,
            }),
            path_style: false,
        };
        let headers = client
            .sign_at(
                "20130524T000000Z",
                &[(String::from("range"), String::from("bytes=0-9"))],
                "GET",
                "/test.txt",
                &[],
                None,
                b"",
            )
            .unwrap();
        let authorization = headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }
}
