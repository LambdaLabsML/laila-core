//! Google Cloud Storage pool implementation.
//!
//! Twin of `laila/data/gcs/gcs.py`: entries are JSON objects of one
//! bucket, named by the URL-encoded logical key. Where Python drives
//! `google.cloud.storage.Client`, the port drives [`GcsClient`]: the Cloud
//! Storage JSON API (`storage/v1/b/{bucket}/o/...`) with the same
//! authentication the Google SDK performs -- explicit service-account
//! info (an RS256-signed JWT exchanged for an OAuth2 access token) or
//! Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the
//! gcloud user credentials file, the GCE metadata server) -- and, like the
//! SDK, anonymous requests against `STORAGE_EMULATOR_HOST` when set.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use parking_lot::Mutex;
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::RsaPrivateKey;
use sha2::Sha256;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::data::http::{self, Client, Response};
use crate::data::schema::base::{
    field_default, field_required, pool_class, private_default, Pool, PoolBackend, PoolObject,
    _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

const SCOPES: &str = "https://www.googleapis.com/auth/devstorage.full_control https://www.googleapis.com/auth/devstorage.read_only https://www.googleapis.com/auth/devstorage.read_write";
const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// `google.api_core.exceptions.GoogleAPICallError` for a failed call
/// (`NotFound` for 404, which the pool treats as "absent").
fn api_error(resp: &Response, method: &str, url: &str) -> LailaError {
    let class = match resp.status {
        400 => "BadRequest",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "NotFound",
        409 => "Conflict",
        412 => "PreconditionFailed",
        429 => "TooManyRequests",
        500 => "InternalServerError",
        503 => "ServiceUnavailable",
        _ => "GoogleAPICallError",
    };
    let body = crate::_compat::pyjson::loads(&resp.text()).ok();
    let message = body
        .as_ref()
        .and_then(|b| http::json_get(b, "error"))
        .and_then(|e| http::json_str(&e, "message"))
        .unwrap_or_else(|| resp.text());
    LailaError::new(
        ExcKind::Exception,
        alloc::format!(
            "google.api_core.exceptions.{class}: {} {method} {url}: {message}",
            resp.status
        ),
    )
}

fn is_not_found(e: &LailaError) -> bool {
    e.message
        .starts_with("google.api_core.exceptions.NotFound:")
}

/// How the client authenticates.
enum Auth {
    /// `STORAGE_EMULATOR_HOST`: anonymous.
    Anonymous,
    /// A service account: self-signed JWT exchanged at `token_uri`.
    ServiceAccount {
        client_email: String,
        private_key: Box<RsaPrivateKey>,
        token_uri: String,
    },
    /// gcloud user credentials: a refresh token.
    AuthorizedUser {
        client_id: String,
        client_secret: String,
        refresh_token: String,
        token_uri: String,
    },
    /// The GCE / Cloud Run metadata server.
    Metadata,
}

/// `google.cloud.storage.Client`: the JSON API endpoint, the credentials
/// and the cached access token.
pub struct GcsClient {
    http: Client,
    base: String,
    auth: Auth,
    token: Mutex<Option<(String, i64)>>,
    project: Option<String>,
}

impl core::fmt::Debug for GcsClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GcsClient")
            .field("base", &self.base)
            .field("project", &self.project)
            .finish()
    }
}

fn default_credentials_error(detail: &str) -> LailaError {
    LailaError::new(
        ExcKind::Exception,
        alloc::format!(
            "google.auth.exceptions.DefaultCredentialsError: Your default credentials were not found. {detail} To set up Application Default Credentials, see https://cloud.google.com/docs/authentication/external/set-up-adc for more information."
        ),
    )
}

fn parse_private_key(pem: &str) -> Result<RsaPrivateKey> {
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::DecodePrivateKey;
    let pem = pem.replace("\\n", "\n");
    let parsed = if pem.contains("BEGIN RSA PRIVATE KEY") {
        RsaPrivateKey::from_pkcs1_pem(&pem).map_err(|e| e.to_string())
    } else {
        RsaPrivateKey::from_pkcs8_pem(&pem).map_err(|e| e.to_string())
    };
    parsed
        .map_err(|e| LailaError::value_error(alloc::format!("Could not deserialize key data. {e}")))
}

/// Credentials from a service-account / authorized-user JSON document.
fn auth_from_info(info: &Arg) -> Result<Auth> {
    let kind = http::json_str(info, "type").unwrap_or_else(|| String::from("service_account"));
    match kind.as_str() {
        "service_account" => {
            let client_email = http::json_str(info, "client_email").ok_or_else(|| {
                LailaError::value_error(
                    "Service account info was not in the expected format, missing fields client_email.",
                )
            })?;
            let private_key = http::json_str(info, "private_key").ok_or_else(|| {
                LailaError::value_error(
                    "Service account info was not in the expected format, missing fields private_key.",
                )
            })?;
            Ok(Auth::ServiceAccount {
                client_email,
                private_key: Box::new(parse_private_key(&private_key)?),
                token_uri: http::json_str(info, "token_uri")
                    .unwrap_or_else(|| String::from(TOKEN_URI)),
            })
        }
        "authorized_user" => Ok(Auth::AuthorizedUser {
            client_id: http::json_str(info, "client_id").unwrap_or_default(),
            client_secret: http::json_str(info, "client_secret").unwrap_or_default(),
            refresh_token: http::json_str(info, "refresh_token").ok_or_else(|| {
                LailaError::value_error(
                    "Authorized user info was not in the expected format, missing fields refresh_token.",
                )
            })?,
            token_uri: http::json_str(info, "token_uri").unwrap_or_else(|| String::from(TOKEN_URI)),
        }),
        other => Err(LailaError::value_error(alloc::format!(
            "The file does not have a valid type. Type is {other}, expected one of ('authorized_user', 'service_account', 'external_account', 'external_account_authorized_user', 'impersonated_service_account', 'gdch_service_account')."
        ))),
    }
}

impl GcsClient {
    /// `storage.Client(credentials=.., project=..)`: explicit
    /// service-account info, else Application Default Credentials.
    pub fn new(service_account_info: Option<&Arg>, project: Option<String>) -> Result<GcsClient> {
        if let Some(host) = env("STORAGE_EMULATOR_HOST") {
            let base = if host.contains("://") {
                host
            } else {
                alloc::format!("http://{host}")
            };
            return Ok(GcsClient {
                http: Client::new(),
                base: base.trim_end_matches('/').to_string(),
                auth: Auth::Anonymous,
                token: Mutex::new(None),
                project: project.or_else(|| Some(String::from("<none>"))),
            });
        }
        let (auth, project) = match service_account_info {
            Some(info) => (
                auth_from_info(info)?,
                project.or_else(|| http::json_str(info, "project_id")),
            ),
            None => {
                let mut project = project
                    .or_else(|| env("GOOGLE_CLOUD_PROJECT"))
                    .or_else(|| env("GCLOUD_PROJECT"));
                let path = env("GOOGLE_APPLICATION_CREDENTIALS").or_else(|| {
                    let p = alloc::format!(
                        "{}/.config/gcloud/application_default_credentials.json",
                        env("HOME").unwrap_or_default()
                    );
                    std::path::Path::new(&p).is_file().then_some(p)
                });
                let auth = match path {
                    Some(p) => {
                        let text = std::fs::read_to_string(&p).map_err(|e| {
                            default_credentials_error(&alloc::format!(
                                "File {p} was not found or could not be read: {e}."
                            ))
                        })?;
                        let info = crate::_compat::pyjson::loads(&text)?;
                        if project.is_none() {
                            project = http::json_str(&info, "project_id")
                                .or_else(|| http::json_str(&info, "quota_project_id"));
                        }
                        auth_from_info(&info)?
                    }
                    None => Auth::Metadata,
                };
                (auth, project)
            }
        };
        let project = match project {
            Some(p) => Some(p),
            None if matches!(auth, Auth::Metadata) => None,
            None => {
                return Err(LailaError::new(
                    ExcKind::OSError,
                    "Project was not passed and could not be determined from the environment.",
                ))
            }
        };
        Ok(GcsClient {
            http: Client::new(),
            base: String::from("https://storage.googleapis.com"),
            auth,
            token: Mutex::new(None),
            project,
        })
    }

    /// `client.project`
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    fn form(pairs: &[(&str, &str)]) -> Vec<u8> {
        pairs
            .iter()
            .map(|(k, v)| {
                alloc::format!(
                    "{}={}",
                    http::uri_encode(k, false),
                    http::uri_encode(v, false)
                )
            })
            .collect::<Vec<_>>()
            .join("&")
            .into_bytes()
    }

    fn token_response(
        &self,
        url: &str,
        body: &[u8],
        headers: &[(String, String)],
    ) -> Result<(String, i64)> {
        let resp = self.http.send("POST", url, headers, Some(body))?;
        if !resp.ok() {
            return Err(LailaError::new(
                ExcKind::Exception,
                alloc::format!(
                    "google.auth.exceptions.RefreshError: {} {}",
                    resp.status,
                    resp.text()
                ),
            ));
        }
        let json = crate::_compat::pyjson::loads(&resp.text())?;
        let token = http::json_str(&json, "access_token").ok_or_else(|| {
            LailaError::new(
                ExcKind::Exception,
                "google.auth.exceptions.RefreshError: No access token in response.",
            )
        })?;
        let expires_in = http::json_get(&json, "expires_in")
            .and_then(|v| v.as_i64())
            .unwrap_or(3600);
        Ok((token, http::unix_now() + expires_in))
    }

    /// A valid bearer token (refreshed when within a minute of expiry).
    fn access_token(&self) -> Result<Option<String>> {
        if matches!(self.auth, Auth::Anonymous) {
            return Ok(None);
        }
        if let Some((tok, exp)) = self.token.lock().as_ref() {
            if *exp - 60 > http::unix_now() {
                return Ok(Some(tok.clone()));
            }
        }
        let fresh = match &self.auth {
            Auth::Anonymous => unreachable!(),
            Auth::ServiceAccount {
                client_email,
                private_key,
                token_uri,
            } => {
                let now = http::unix_now();
                let header = http::base64url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
                let claims = alloc::format!(
                    r#"{{"iss":"{client_email}","scope":"{SCOPES}","aud":"{token_uri}","iat":{now},"exp":{}}}"#,
                    now + 3600
                );
                let signing_input =
                    alloc::format!("{header}.{}", http::base64url_encode(claims.as_bytes()));
                let signing_key = SigningKey::<Sha256>::new((**private_key).clone());
                let signature = signing_key.sign(signing_input.as_bytes()).to_vec();
                let assertion =
                    alloc::format!("{signing_input}.{}", http::base64url_encode(&signature));
                self.token_response(
                    token_uri,
                    &Self::form(&[
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", &assertion),
                    ]),
                    &[(
                        String::from("content-type"),
                        String::from("application/x-www-form-urlencoded"),
                    )],
                )?
            }
            Auth::AuthorizedUser {
                client_id,
                client_secret,
                refresh_token,
                token_uri,
            } => self.token_response(
                token_uri,
                &Self::form(&[
                    ("grant_type", "refresh_token"),
                    ("client_id", client_id),
                    ("client_secret", client_secret),
                    ("refresh_token", refresh_token),
                ]),
                &[(
                    String::from("content-type"),
                    String::from("application/x-www-form-urlencoded"),
                )],
            )?,
            Auth::Metadata => {
                let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
                let resp = self
                    .http
                    .send(
                        "GET",
                        url,
                        &[(String::from("metadata-flavor"), String::from("Google"))],
                        None,
                    )
                    .map_err(|_| {
                        default_credentials_error("Was not able to reach the metadata server.")
                    })?;
                if !resp.ok() {
                    return Err(default_credentials_error(
                        "The metadata server did not return a token.",
                    ));
                }
                let json = crate::_compat::pyjson::loads(&resp.text())?;
                let token = http::json_str(&json, "access_token").unwrap_or_default();
                let expires_in = http::json_get(&json, "expires_in")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(3600);
                (token, http::unix_now() + expires_in)
            }
        };
        *self.token.lock() = Some(fresh.clone());
        Ok(Some(fresh.0))
    }

    fn call(
        &self,
        method: &str,
        url: &str,
        content_type: Option<&str>,
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let resp = self.http.send_with_retry(method, url, body, || {
            let mut headers: Vec<(String, String)> = Vec::new();
            if let Some(tok) = self.access_token()? {
                headers.push((
                    String::from("authorization"),
                    alloc::format!("Bearer {tok}"),
                ));
            }
            if let Some(ct) = content_type {
                headers.push((String::from("content-type"), ct.to_string()));
            }
            Ok(headers)
        })?;
        if resp.ok() {
            Ok(resp)
        } else {
            Err(api_error(&resp, method, url))
        }
    }

    fn object_url(&self, bucket: &str, object_name: &str) -> String {
        alloc::format!(
            "{}/storage/v1/b/{}/o/{}",
            self.base,
            http::uri_encode(bucket, false),
            http::uri_encode(object_name, false)
        )
    }

    /// `blob.download_as_text()`: the object's bytes, `NotFound` when absent.
    pub fn download(&self, bucket: &str, object_name: &str) -> Result<Vec<u8>> {
        let url = alloc::format!("{}?alt=media", self.object_url(bucket, object_name));
        Ok(self.call("GET", &url, None, None)?.body)
    }

    /// `blob.upload_from_string(data, content_type=..)`
    pub fn upload(
        &self,
        bucket: &str,
        object_name: &str,
        data: &[u8],
        content_type: &str,
    ) -> Result<()> {
        let url = alloc::format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.base,
            http::uri_encode(bucket, false),
            http::uri_encode(object_name, false)
        );
        self.call("POST", &url, Some(content_type), Some(data))
            .map(|_| ())
    }

    /// `blob.delete()`: `NotFound` when absent.
    pub fn delete(&self, bucket: &str, object_name: &str) -> Result<()> {
        self.call("DELETE", &self.object_url(bucket, object_name), None, None)
            .map(|_| ())
    }

    /// `blob.exists()`
    pub fn exists(&self, bucket: &str, object_name: &str) -> Result<bool> {
        let url = alloc::format!("{}?fields=name", self.object_url(bucket, object_name));
        match self.call("GET", &url, None, None) {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// `client.list_blobs(bucket)`: every object name, page after page.
    pub fn list_blob_names(&self, bucket: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = alloc::format!(
                "{}/storage/v1/b/{}/o?fields=items%2Fname%2CnextPageToken",
                self.base,
                http::uri_encode(bucket, false)
            );
            if let Some(t) = &page_token {
                url.push_str("&pageToken=");
                url.push_str(&http::uri_encode(t, false));
            }
            let resp = self.call("GET", &url, None, None)?;
            let json = crate::_compat::pyjson::loads(&resp.text())?;
            if let Some(items) = http::json_get(&json, "items") {
                for item in items.__iter__()? {
                    if let Some(name) = http::json_str(&item, "name") {
                        names.push(name);
                    }
                }
            }
            page_token = http::json_str(&json, "nextPageToken").filter(|t| !t.is_empty());
            if page_token.is_none() {
                break;
            }
        }
        Ok(names)
    }
}

// ---------------------------------------------------------------------------
// The backend

/// `GCSPool`'s state: the client (`self._client`) and the bucket handle
/// (`self._bucket`, here the bucket name once resolved).
#[derive(Default)]
pub struct GCSBackend {
    client: Mutex<Option<Arc<GcsClient>>>,
    bucket: Mutex<Option<String>>,
}

impl core::fmt::Debug for GCSBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("GCSBackend")
    }
}

fn str_field(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

impl GCSBackend {
    /// Return the GCS `storage.Client`, creating it on first call.
    pub fn _get_client(&self, pool: &PoolObject) -> Result<Arc<GcsClient>> {
        if let Some(c) = self.client.lock().as_ref() {
            return Ok(c.clone());
        }
        let info = pool.field("service_account_info").filter(|v| !v.is_none());
        let mut project_id = str_field(pool, "project_id");
        if project_id.is_none() {
            if let Some(info) = &info {
                project_id = http::json_str(info, "project_id");
                if let Some(p) = &project_id {
                    pool.set_field("project_id", p.clone());
                }
            }
        }
        let client = Arc::new(GcsClient::new(info.as_ref(), project_id)?);
        *self.client.lock() = Some(client.clone());
        Ok(client)
    }

    /// Return the GCS bucket handle (`self._bucket`).
    pub fn _get_bucket(&self, pool: &PoolObject) -> Result<String> {
        if let Some(b) = self.bucket.lock().as_ref() {
            return Ok(b.clone());
        }
        self._get_client(pool)?;
        let bucket = str_field(pool, "bucket_name").unwrap_or_default();
        *self.bucket.lock() = Some(bucket.clone());
        Ok(bucket)
    }

    /// URL-encode a logical key for GCS.
    pub fn _object_key(&self, key: &str) -> String {
        http::quote_key(key)
    }

    /// Decode a GCS object name back to the logical key.
    pub fn _logical_key(&self, object_key: &str) -> String {
        http::unquote_key(object_key)
    }
}

impl PoolBackend for GCSBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Release client references.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        *self.client.lock() = None;
        *self.bucket.lock() = None;
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let bucket = self._get_bucket(pool)?;
        let client = self._get_client(pool)?;
        match client.download(&bucket, &self._object_key(key)) {
            Ok(raw) => crate::_compat::pyjson::loads(&String::from_utf8_lossy(&raw)),
            Err(e) if is_not_found(&e) => Ok(Arg::None),
            Err(e) => Err(e),
        }
    }

    /// Store *entry* as a JSON blob under *key*.
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
            return Err(LailaError::type_error(
                "GCSPool expects a serialized JSON string.",
            ));
        };
        let bucket = self._get_bucket(pool)?;
        let client = self._get_client(pool)?;
        client.upload(
            &bucket,
            &self._object_key(key),
            value.as_bytes(),
            "application/json",
        )
    }

    /// Delete the blob for *key*; no-op if absent.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let bucket = self._get_bucket(pool)?;
        let client = self._get_client(pool)?;
        match client.delete(&bucket, &self._object_key(key)) {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Remove all blobs from the bucket.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let bucket = str_field(pool, "bucket_name").unwrap_or_default();
        let client = self._get_client(pool)?;
        for name in client.list_blob_names(&bucket)? {
            client.delete(&bucket, &name)?;
        }
        Ok(())
    }

    /// `True` if a blob for *key* exists.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let bucket = self._get_bucket(pool)?;
        let client = self._get_client(pool)?;
        client.exists(&bucket, &self._object_key(key))
    }

    /// Return all keys in the bucket.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let bucket = str_field(pool, "bucket_name").unwrap_or_default();
        let client = self._get_client(pool)?;
        Ok(client
            .list_blob_names(&bucket)?
            .iter()
            .map(|n| self._logical_key(n))
            .collect())
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_client" => Some(Ok(if self.client.lock().is_some() {
                Arg::Str("<google.cloud.storage.Client>".into())
            } else {
                Arg::None
            })),
            "_bucket" => Some(Ok(self
                .bucket
                .lock()
                .clone()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None))),
            _ => None,
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
            "_get_client" => Some(
                self._get_client(pool)
                    .map(|_| Arg::Str("<google.cloud.storage.Client>".into())),
            ),
            "_get_bucket" => Some(self._get_bucket(pool).map(IntoArg::into_arg)),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_get_client",
            "_get_bucket",
            "_object_key",
            "_logical_key",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "GCSPool",
        "laila.data.gcs.gcs",
        alloc::vec![],
        alloc::vec![
            (
                String::from("service_account_info"),
                field_default("dict[str, Any] | None", Arg::None)
            ),
            (
                String::from("project_id"),
                field_default("str | None", Arg::None)
            ),
            (String::from("bucket_name"), field_required("str")),
            (
                String::from("transformations"),
                field_default(
                    "TransformationSequence | None",
                    crate::entry::transformation_base64()
                )
            ),
        ],
        alloc::vec![
            (String::from("_client"), private_default(Arg::None)),
            (String::from("_bucket"), private_default(Arg::None)),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// Google Cloud Storage-backed pool.
    ///
    /// Entries are stored as JSON objects in a GCS bucket. Authentication
    /// can use explicit service-account info or Application Default
    /// Credentials.
    GCSPool,
    GCSBackend
);

impl ModelBase for GCSPool {
    fn __class__() -> &'static ModelClass {
        GCSPool::__class__()
    }
}

impl GCSPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `GCSPool(bucket_name, *, service_account_info=None, project_id=None, **kwargs)`
    #[pysig("GCSPool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<GCSPool> {
        Ok(GCSPool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(GCSBackend::default())),
        )?))
    }
    /// `pool.bucket_name`
    pub fn bucket_name(&self) -> Arg {
        self.0.field("bucket_name").unwrap_or(Arg::None)
    }
    /// `pool.project_id`
    pub fn project_id(&self) -> Arg {
        self.0.field("project_id").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for GCSPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        GCSPool::new(args).map(IntoArg::into_arg)
    }
}

/// `GCSPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    GCSPool::__class__().as_pyclass().into_arg()
}

impl PyArg for GCSBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
