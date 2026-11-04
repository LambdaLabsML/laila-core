//! Azure Blob Storage pool implementation.
//!
//! Twin of `laila/data/azure/azure.py`: each entry is a JSON blob of one
//! container, named by the URL-encoded logical key. Where Python drives
//! `azure.storage.blob.BlobServiceClient`, the port drives
//! [`BlobServiceClient`]: the Blob service REST API with the
//! authentication the SDK derives from a connection string -- SharedKey
//! (an HMAC-SHA256 over the canonicalized request), a shared access
//! signature appended to every URL, or anonymous access -- against the
//! endpoint the string names (Azure, or Azurite's
//! `BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1`).

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use parking_lot::Mutex;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::data::http::{self, Client, Response};
use crate::data::schema::base::{
    field_default, field_required, pool_class, private_default, Pool, PoolBackend, PoolObject,
    _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// The service version the requests are made against (`x-ms-version`).
const API_VERSION: &str = "2023-11-03";

/// How requests are authenticated.
enum Credential {
    /// `AccountName=..;AccountKey=..`
    SharedKey { account: String, key: Vec<u8> },
    /// `SharedAccessSignature=..`: the query appended to every URL.
    Sas(String),
    /// A public container.
    Anonymous,
}

/// `azure.storage.blob.BlobServiceClient`: the account endpoint and the
/// credential parsed from a connection string.
pub struct BlobServiceClient {
    http: Client,
    endpoint: String,
    credential: Credential,
}

impl core::fmt::Debug for BlobServiceClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlobServiceClient")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// The `azure.core.exceptions` class the SDK raises for a response.
fn error_class(status: u16, code: &str) -> &'static str {
    match status {
        404 => "ResourceNotFoundError",
        409 if code.ends_with("AlreadyExists") || code == "ResourceTypeMismatch" => {
            "ResourceExistsError"
        }
        412 => "ResourceModifiedError",
        401 | 403 => "ClientAuthenticationError",
        _ => "HttpResponseError",
    }
}

/// `azure.core.exceptions.*` for a failed response.
fn storage_error(resp: &Response) -> LailaError {
    let text = resp.text();
    let code = resp
        .header("x-ms-error-code")
        .map(String::from)
        .or_else(|| http::xml_text(&text, "Code"))
        .unwrap_or_default();
    let message = http::xml_text(&text, "Message").unwrap_or_else(|| {
        alloc::format!("Operation returned an invalid status '{}'", resp.status)
    });
    let class = error_class(resp.status, &code);
    let mut msg = alloc::format!("azure.core.exceptions.{class}: {message}");
    if !code.is_empty() {
        msg.push_str("\nErrorCode:");
        msg.push_str(&code);
    }
    LailaError::new(ExcKind::Exception, msg)
}

fn is_class(e: &LailaError, class: &str) -> bool {
    e.message
        .starts_with(&alloc::format!("azure.core.exceptions.{class}:"))
}

/// `isinstance(e, ResourceNotFoundError)`
pub fn is_resource_not_found(e: &LailaError) -> bool {
    is_class(e, "ResourceNotFoundError")
}

/// `isinstance(e, ResourceExistsError)`
pub fn is_resource_exists(e: &LailaError) -> bool {
    is_class(e, "ResourceExistsError")
}

impl BlobServiceClient {
    /// `BlobServiceClient.from_connection_string(conn_str)`
    pub fn from_connection_string(conn_str: &str) -> Result<BlobServiceClient> {
        let conn_str = conn_str.trim_end_matches(';');
        let mut settings: Vec<(String, String)> = Vec::new();
        for part in conn_str.split(';') {
            match part.split_once('=') {
                Some((k, v)) => settings.push((k.to_ascii_uppercase(), v.to_string())),
                None => {
                    return Err(LailaError::value_error(
                        "Connection string is either blank or malformed.",
                    ))
                }
            }
        }
        let get = |name: &str| -> Option<String> {
            settings
                .iter()
                .rev()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let account_name = get("ACCOUNTNAME");
        let credential = match (account_name.clone(), get("ACCOUNTKEY")) {
            (Some(account), Some(key)) => Credential::SharedKey {
                account,
                key: http::base64_decode(&key)?,
            },
            _ => match get("SHAREDACCESSSIGNATURE") {
                Some(sas) => Credential::Sas(sas.trim_start_matches('?').to_string()),
                None => Credential::Anonymous,
            },
        };
        let endpoint = match get("BLOBENDPOINT") {
            Some(primary) => primary,
            None => {
                if get("BLOBSECONDARYENDPOINT").is_some() {
                    return Err(LailaError::value_error(
                        "Connection string specifies only secondary endpoint.",
                    ));
                }
                match (
                    get("DEFAULTENDPOINTSPROTOCOL"),
                    &account_name,
                    get("ENDPOINTSUFFIX"),
                ) {
                    (Some(proto), Some(account), Some(suffix)) => {
                        alloc::format!("{proto}://{account}.blob.{suffix}")
                    }
                    (_, Some(account), suffix) => alloc::format!(
                        "https://{account}.blob.{}",
                        suffix.unwrap_or_else(|| String::from("core.windows.net"))
                    ),
                    _ => {
                        return Err(LailaError::value_error(
                            "Connection string missing required connection details.",
                        ))
                    }
                }
            }
        };
        Ok(BlobServiceClient {
            http: Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            credential,
        })
    }

    /// `client.url`
    pub fn url(&self) -> &str {
        &self.endpoint
    }

    /// The URL's path component (already percent-encoded).
    fn path_of(url: &str) -> &str {
        let rest = url.split("://").nth(1).unwrap_or(url);
        match rest.find('/') {
            Some(i) => &rest[i..],
            None => "/",
        }
    }

    /// The `Authorization: SharedKey` header for a request.
    fn shared_key_header(
        account: &str,
        key: &[u8],
        method: &str,
        path: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        content_length: usize,
    ) -> String {
        let header = |name: &str| -> String {
            headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        let mut ms: Vec<&(String, String)> = headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-ms-"))
            .collect();
        ms.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers: String = ms
            .iter()
            .map(|(k, v)| alloc::format!("{k}:{}\n", v.trim()))
            .collect();
        let mut sorted_query: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();
        sorted_query.sort();
        let mut canonical_resource = alloc::format!("/{account}{path}");
        for (k, v) in &sorted_query {
            canonical_resource.push('\n');
            canonical_resource.push_str(k);
            canonical_resource.push(':');
            canonical_resource.push_str(v);
        }
        let content_length = if content_length == 0 {
            String::new()
        } else {
            content_length.to_string()
        };
        let string_to_sign = alloc::format!(
            "{method}\n{}\n{}\n{content_length}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{canonical_headers}{canonical_resource}",
            header("content-encoding"),
            header("content-language"),
            header("content-md5"),
            header("content-type"),
            header("date"),
            header("if-modified-since"),
            header("if-match"),
            header("if-none-match"),
            header("if-unmodified-since"),
            header("range"),
        );
        let signature = http::base64_encode(&http::hmac_sha256(key, string_to_sign.as_bytes()));
        alloc::format!("SharedKey {account}:{signature}")
    }

    /// One authenticated request against `{endpoint}{path}?{query}`.
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        extra_headers: &[(String, String)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let mut url = alloc::format!("{}{path}", self.endpoint);
        let mut query_string: String = query
            .iter()
            .map(|(k, v)| {
                alloc::format!(
                    "{}={}",
                    http::uri_encode(k, false),
                    http::uri_encode(v, false)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        if let Credential::Sas(sas) = &self.credential {
            if !query_string.is_empty() {
                query_string.push('&');
            }
            query_string.push_str(sas);
        }
        if !query_string.is_empty() {
            url.push('?');
            url.push_str(&query_string);
        }
        let resp = self.http.send_with_retry(method, &url, body, || {
            let mut headers: Vec<(String, String)> = Vec::new();
            headers.push((String::from("x-ms-date"), http::rfc1123_now()));
            headers.push((String::from("x-ms-version"), String::from(API_VERSION)));
            headers.extend(extra_headers.iter().cloned());
            if let Credential::SharedKey { account, key } = &self.credential {
                let auth = Self::shared_key_header(
                    account,
                    key,
                    method,
                    Self::path_of(&url).split('?').next().unwrap_or("/"),
                    query,
                    &headers,
                    body.map(<[u8]>::len).unwrap_or(0),
                );
                headers.push((String::from("authorization"), auth));
            }
            Ok(headers)
        })?;
        if resp.ok() {
            Ok(resp)
        } else {
            Err(storage_error(&resp))
        }
    }

    fn container_path(container: &str) -> String {
        alloc::format!("/{}", http::uri_encode(container, false))
    }

    fn blob_path(container: &str, blob_name: &str) -> String {
        alloc::format!(
            "/{}/{}",
            http::uri_encode(container, false),
            http::uri_encode(blob_name, true)
        )
    }

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (String::from(*k), String::from(*v)))
            .collect()
    }

    /// `container_client.create_container()`: `ResourceExistsError` when
    /// it already exists.
    pub fn create_container(&self, container: &str) -> Result<()> {
        self.request(
            "PUT",
            &Self::container_path(container),
            &Self::q(&[("restype", "container")]),
            &[],
            Some(&[]),
        )
        .map(|_| ())
    }

    /// `blob_client.download_blob().readall()`: `ResourceNotFoundError`
    /// when absent.
    pub fn download_blob(&self, container: &str, blob_name: &str) -> Result<Vec<u8>> {
        Ok(self
            .request(
                "GET",
                &Self::blob_path(container, blob_name),
                &[],
                &[],
                None,
            )?
            .body)
    }

    /// `blob_client.upload_blob(data, overwrite=True, content_type=..)`
    pub fn upload_blob(
        &self,
        container: &str,
        blob_name: &str,
        data: &[u8],
        content_type: &str,
    ) -> Result<()> {
        self.request(
            "PUT",
            &Self::blob_path(container, blob_name),
            &[],
            &[
                (String::from("x-ms-blob-type"), String::from("BlockBlob")),
                (
                    String::from("x-ms-blob-content-type"),
                    content_type.to_string(),
                ),
                (
                    String::from("content-type"),
                    String::from("application/octet-stream"),
                ),
            ],
            Some(data),
        )
        .map(|_| ())
    }

    /// `blob_client.delete_blob()`: `ResourceNotFoundError` when absent.
    pub fn delete_blob(&self, container: &str, blob_name: &str) -> Result<()> {
        self.request(
            "DELETE",
            &Self::blob_path(container, blob_name),
            &[],
            &[],
            None,
        )
        .map(|_| ())
    }

    /// `blob_client.exists()`
    pub fn blob_exists(&self, container: &str, blob_name: &str) -> Result<bool> {
        match self.request(
            "HEAD",
            &Self::blob_path(container, blob_name),
            &[],
            &[],
            None,
        ) {
            Ok(_) => Ok(true),
            Err(e) if is_resource_not_found(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// `container_client.list_blobs()`: every blob name, page after page.
    pub fn list_blob_names(&self, container: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut query = Self::q(&[("restype", "container"), ("comp", "list")]);
            if let Some(m) = &marker {
                query.push((String::from("marker"), m.clone()));
            }
            let resp = self.request("GET", &Self::container_path(container), &query, &[], None)?;
            let xml = resp.text();
            for blob in http::xml_texts(&xml, "Blob") {
                if let Some(name) = http::xml_text(&blob, "Name") {
                    names.push(name);
                }
            }
            marker = http::xml_text(&xml, "NextMarker").filter(|m| !m.is_empty());
            if marker.is_none() {
                break;
            }
        }
        Ok(names)
    }
}

// ---------------------------------------------------------------------------
// The backend

/// `AzurePool`'s state: the service client (`self._client`) and the
/// container handle (`self._container`, here the container name once the
/// container has been created).
#[derive(Default)]
pub struct AzureBackend {
    client: Mutex<Option<Arc<BlobServiceClient>>>,
    container: Mutex<Option<String>>,
}

impl core::fmt::Debug for AzureBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("AzureBackend")
    }
}

fn str_field(pool: &PoolObject, name: &str) -> String {
    match pool.field(name) {
        Some(Arg::Str(s)) => String::from(&*s),
        Some(Arg::None) | None => String::new(),
        Some(other) => other.__str__(),
    }
}

impl AzureBackend {
    /// Return the `BlobServiceClient`, creating it on first call.
    pub fn _get_client(&self, pool: &PoolObject) -> Result<Arc<BlobServiceClient>> {
        if let Some(c) = self.client.lock().as_ref() {
            return Ok(c.clone());
        }
        let client = Arc::new(BlobServiceClient::from_connection_string(&str_field(
            pool,
            "connection_string",
        ))?);
        *self.client.lock() = Some(client.clone());
        Ok(client)
    }

    /// Return the container client, creating the container if needed.
    pub fn _get_container(&self, pool: &PoolObject) -> Result<String> {
        if let Some(c) = self.container.lock().as_ref() {
            return Ok(c.clone());
        }
        let container = str_field(pool, "container_name");
        match self._get_client(pool)?.create_container(&container) {
            Ok(()) => {}
            Err(e) if is_resource_exists(&e) => {}
            Err(e) => return Err(e),
        }
        *self.container.lock() = Some(container.clone());
        Ok(container)
    }

    /// URL-encode a logical key for blob storage.
    pub fn _object_key(&self, key: &str) -> String {
        http::quote_key(key)
    }

    /// Decode a blob name back to the logical key.
    pub fn _logical_key(&self, object_key: &str) -> String {
        http::unquote_key(object_key)
    }
}

impl PoolBackend for AzureBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Release client references.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        *self.client.lock() = None;
        *self.container.lock() = None;
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        match client.download_blob(&container, &self._object_key(key)) {
            Ok(raw) => crate::_compat::pyjson::loads(&String::from_utf8_lossy(&raw)),
            Err(e) if is_resource_not_found(&e) => Ok(Arg::None),
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
                "AzurePool expects a serialized JSON string.",
            ));
        };
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        client.upload_blob(
            &container,
            &self._object_key(key),
            value.as_bytes(),
            "application/json",
        )
    }

    /// Delete the blob for *key*; no-op if absent.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        match client.delete_blob(&container, &self._object_key(key)) {
            Ok(()) => Ok(()),
            Err(e) if is_resource_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Remove all blobs from the container.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        let blobs = client.list_blob_names(&container)?;
        for name in blobs {
            client.delete_blob(&container, &name)?;
        }
        Ok(())
    }

    /// `True` if a blob for *key* exists.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        client.blob_exists(&container, &self._object_key(key))
    }

    /// Return all keys in the container.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let container = self._get_container(pool)?;
        let client = self._get_client(pool)?;
        Ok(client
            .list_blob_names(&container)?
            .iter()
            .map(|n| self._logical_key(n))
            .collect())
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_client" => Some(Ok(match self.client.lock().as_ref() {
                Some(c) => Arg::Str(alloc::format!("<BlobServiceClient {}>", c.url()).into()),
                None => Arg::None,
            })),
            "_container" => Some(Ok(self
                .container
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
                    .map(|c| Arg::Str(alloc::format!("<BlobServiceClient {}>", c.url()).into())),
            ),
            "_get_container" => Some(self._get_container(pool).map(IntoArg::into_arg)),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_get_client",
            "_get_container",
            "_object_key",
            "_logical_key",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "AzurePool",
        "laila.data.azure.azure",
        alloc::vec![],
        alloc::vec![
            (String::from("connection_string"), field_required("str")),
            (String::from("container_name"), field_required("str")),
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
            (String::from("_container"), private_default(Arg::None)),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// Azure Blob Storage-backed pool.
    ///
    /// Each entry is stored as a JSON blob in the configured container.
    /// Keys are URL-encoded to avoid path-separator issues.
    AzurePool,
    AzureBackend
);

impl ModelBase for AzurePool {
    fn __class__() -> &'static ModelClass {
        AzurePool::__class__()
    }
}

impl AzurePool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `AzurePool(connection_string, container_name, **kwargs)`
    #[pysig("AzurePool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<AzurePool> {
        Ok(AzurePool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(AzureBackend::default())),
        )?))
    }
    /// `pool.connection_string`
    pub fn connection_string(&self) -> Arg {
        self.0.field("connection_string").unwrap_or(Arg::None)
    }
    /// `pool.container_name`
    pub fn container_name(&self) -> Arg {
        self.0.field("container_name").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for AzurePool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        AzurePool::new(args).map(IntoArg::into_arg)
    }
}

/// `AzurePool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    AzurePool::__class__().as_pyclass().into_arg()
}

impl PyArg for AzureBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
