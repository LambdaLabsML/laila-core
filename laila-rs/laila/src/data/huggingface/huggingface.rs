//! Hugging Face Hub pool implementation.
//!
//! Twin of `laila/data/huggingface/huggingface.py`: each entry is one
//! JSON file of a Hub repo (`{path_prefix}/{url-encoded key}.json`).
//! Where Python drives `huggingface_hub.HfApi` / `hf_hub_download`, the
//! port drives [`HfApi`]: the Hub's HTTP API -- `resolve/` to read,
//! `tree/` to list, `preupload/` + the git-LFS batch endpoint + `commit/`
//! to write and delete -- with the token resolution of `huggingface_hub`
//! (the explicit token, `HF_TOKEN`, the cached login) and its error
//! classes (`EntryNotFoundError`, `RepositoryNotFoundError`, ...).

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

const DEFAULT_ENDPOINT: &str = "https://huggingface.co";
const REPO_TYPES: [&str; 3] = ["model", "dataset", "space"];

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// `huggingface_hub.utils.get_token()`: `HF_TOKEN`, else the token file
/// written by `huggingface-cli login`.
fn get_token() -> Option<String> {
    if let Some(t) = env("HF_TOKEN") {
        return Some(t);
    }
    let path = env("HF_TOKEN_PATH").unwrap_or_else(|| {
        let home = env("HF_HOME").unwrap_or_else(|| {
            alloc::format!("{}/.cache/huggingface", env("HOME").unwrap_or_default())
        });
        alloc::format!("{home}/token")
    });
    std::fs::read_to_string(path)
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

/// `huggingface_hub.utils.hf_raise_for_status`: the error class the SDK
/// raises for a failed response.
fn hub_error(resp: &Response, url: &str) -> LailaError {
    let error_code = resp.header("x-error-code").unwrap_or("");
    let error_message = resp.header("x-error-message").unwrap_or("");
    let request_id = resp
        .header("x-request-id")
        .map(|id| alloc::format!(" (Request ID: {id})"))
        .unwrap_or_default();
    let kind = if resp.status >= 500 {
        "Server"
    } else {
        "Client"
    };
    let (class, detail) = match error_code {
        "RevisionNotFound" => (
            "RevisionNotFoundError",
            alloc::format!("Revision Not Found for url: {url}."),
        ),
        "EntryNotFound" => (
            "EntryNotFoundError",
            alloc::format!("Entry Not Found for url: {url}."),
        ),
        "GatedRepo" => (
            "GatedRepoError",
            alloc::format!("Cannot access gated repo for url {url}."),
        ),
        _ if error_message == "Access to this resource is disabled." => (
            "DisabledRepoError",
            alloc::format!("Cannot access repository for url {url}."),
        ),
        "RepoNotFound" => (
            "RepositoryNotFoundError",
            alloc::format!("Repository Not Found for url: {url}."),
        ),
        _ if resp.status == 401 => (
            "RepositoryNotFoundError",
            alloc::format!("Repository Not Found for url: {url}."),
        ),
        _ if resp.status == 400 => ("BadRequestError", String::new()),
        _ => ("HfHubHTTPError", String::new()),
    };
    let server = {
        let text = resp.text();
        crate::_compat::pyjson::loads(&text)
            .ok()
            .and_then(|j| http::json_str(&j, "error"))
            .unwrap_or(text)
    };
    let mut msg = alloc::format!(
        "huggingface_hub.errors.{class}: {} {kind} Error.{request_id}",
        resp.status
    );
    if !detail.is_empty() {
        msg.push_str("\n\n");
        msg.push_str(&detail);
    }
    if !server.is_empty() {
        msg.push_str("\n\n");
        msg.push_str(&server);
    }
    LailaError::new(ExcKind::Exception, msg)
}

/// `isinstance(e, EntryNotFoundError)`
pub fn is_entry_not_found(e: &LailaError) -> bool {
    e.message
        .starts_with("huggingface_hub.errors.EntryNotFoundError:")
}

/// A JSON string literal.
fn js(s: &str) -> String {
    crate::_compat::pyjson::dumps(
        &Arg::Str(s.into()),
        &crate::_compat::pyjson::DumpsOptions::default(),
    )
    .unwrap_or_else(|_| alloc::format!("\"{s}\""))
}

/// One operation of a commit (`CommitOperationAdd` / `CommitOperationDelete`).
enum CommitOp {
    /// A regular (non-LFS) file: inlined base64.
    File { path: String, content: Vec<u8> },
    /// An LFS file already uploaded to the LFS store.
    LfsFile {
        path: String,
        oid: String,
        size: usize,
    },
    /// `CommitOperationDelete`
    Deleted { path: String },
}

/// `huggingface_hub.HfApi`: endpoint, token and the repo helpers.
pub struct HfApi {
    http: Client,
    endpoint: String,
    token: Option<String>,
}

impl core::fmt::Debug for HfApi {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HfApi")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl HfApi {
    /// `HfApi(token=token)`
    pub fn new(token: Option<String>) -> HfApi {
        HfApi {
            http: Client::new(),
            endpoint: env("HF_ENDPOINT")
                .unwrap_or_else(|| String::from(DEFAULT_ENDPOINT))
                .trim_end_matches('/')
                .to_string(),
            token,
        }
    }

    /// `api.endpoint`
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The token for a request: the call's, else the API's, else the
    /// environment / cached login.
    fn resolve_token(&self, token: Option<&str>) -> Option<String> {
        token
            .map(String::from)
            .or_else(|| self.token.clone())
            .or_else(get_token)
    }

    /// `repo_type` validated (`ValueError` otherwise).
    fn check_repo_type(repo_type: &str) -> Result<()> {
        if REPO_TYPES.contains(&repo_type) {
            Ok(())
        } else {
            Err(LailaError::value_error(alloc::format!(
                "Invalid repo type: {repo_type}. Accepted repo types are: ['model', 'dataset', 'space']"
            )))
        }
    }

    /// `constants.REPO_TYPES_URL_PREFIXES`: the `resolve/` URL prefix.
    fn url_prefix(repo_type: &str) -> &'static str {
        match repo_type {
            "dataset" => "datasets/",
            "space" => "spaces/",
            _ => "",
        }
    }

    /// The `/api/{models|datasets|spaces}/` segment.
    fn api_plural(repo_type: &str) -> &'static str {
        match repo_type {
            "dataset" => "datasets",
            "space" => "spaces",
            _ => "models",
        }
    }

    fn headers(&self, token: Option<&str>, content_type: Option<&str>) -> Vec<(String, String)> {
        let mut headers: Vec<(String, String)> = alloc::vec![(
            String::from("user-agent"),
            alloc::format!("laila/{}; rust", env!("CARGO_PKG_VERSION")),
        )];
        if let Some(t) = self.resolve_token(token) {
            headers.push((String::from("authorization"), alloc::format!("Bearer {t}")));
        }
        if let Some(ct) = content_type {
            headers.push((String::from("content-type"), ct.to_string()));
        }
        headers
    }

    fn call(
        &self,
        method: &str,
        url: &str,
        token: Option<&str>,
        content_type: Option<&str>,
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let resp = self
            .http
            .send_with_retry(method, url, body, || Ok(self.headers(token, content_type)))?;
        if resp.ok() {
            Ok(resp)
        } else {
            Err(hub_error(&resp, url))
        }
    }

    /// `hf_hub_url(repo_id, filename, repo_type=.., revision=..)`
    pub fn hf_hub_url(
        &self,
        repo_id: &str,
        filename: &str,
        repo_type: &str,
        revision: &str,
    ) -> Result<String> {
        Self::check_repo_type(repo_type)?;
        Ok(alloc::format!(
            "{}/{}{repo_id}/resolve/{}/{}",
            self.endpoint,
            Self::url_prefix(repo_type),
            http::uri_encode(revision, false),
            http::uri_encode(filename, true)
        ))
    }

    /// `hf_hub_download(repo_id, filename, repo_type, revision, token)`:
    /// the file's bytes (`EntryNotFoundError` when absent).
    pub fn hf_hub_download(
        &self,
        repo_id: &str,
        filename: &str,
        repo_type: &str,
        revision: &str,
        token: Option<&str>,
    ) -> Result<Vec<u8>> {
        let url = self.hf_hub_url(repo_id, filename, repo_type, revision)?;
        Ok(self.call("GET", &url, token, None, None)?.body)
    }

    /// `api.list_repo_files(repo_id, repo_type=.., revision=.., token=..)`:
    /// every file path of the revision (the recursive tree, page after
    /// page).
    pub fn list_repo_files(
        &self,
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        token: Option<&str>,
    ) -> Result<Vec<String>> {
        Self::check_repo_type(repo_type)?;
        let mut files = Vec::new();
        let mut url = alloc::format!(
            "{}/api/{}/{repo_id}/tree/{}?recursive=True&expand=False",
            self.endpoint,
            Self::api_plural(repo_type),
            http::uri_encode(revision, false)
        );
        loop {
            let resp = self.call("GET", &url, token, None, None)?;
            let next = resp.header("link").and_then(|link| {
                link.split(',').find_map(|part| {
                    let part = part.trim();
                    part.contains("rel=\"next\"")
                        .then(|| part.split('<').nth(1)?.split('>').next().map(String::from))
                        .flatten()
                })
            });
            let json = crate::_compat::pyjson::loads(&resp.text())?;
            for item in json.__iter__()? {
                if http::json_str(&item, "type").as_deref() == Some("file") {
                    if let Some(path) = http::json_str(&item, "path") {
                        files.push(path);
                    }
                }
            }
            match next {
                Some(n) => url = n,
                None => break,
            }
        }
        Ok(files)
    }

    /// `POST /api/{type}/{repo}/preupload/{revision}`: whether the Hub
    /// wants *path* as a regular (inlined) or an LFS file, or ignores it.
    fn preupload(
        &self,
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        path: &str,
        content: &[u8],
        token: Option<&str>,
    ) -> Result<(String, bool)> {
        let url = alloc::format!(
            "{}/api/{}/{repo_id}/preupload/{}",
            self.endpoint,
            Self::api_plural(repo_type),
            http::uri_encode(revision, false)
        );
        let sample = http::base64_encode(&content[..content.len().min(512)]);
        let body = alloc::format!(
            r#"{{"files":[{{"path":{},"sample":"{sample}","size":{}}}]}}"#,
            js(path),
            content.len()
        );
        let resp = self.call(
            "POST",
            &url,
            token,
            Some("application/json"),
            Some(body.as_bytes()),
        )?;
        let json = crate::_compat::pyjson::loads(&resp.text())?;
        let files = http::json_get(&json, "files").unwrap_or(Arg::None);
        let mut mode = String::from("regular");
        let mut ignore = false;
        if !files.is_none() {
            for f in files.__iter__()? {
                if http::json_str(&f, "path").as_deref() == Some(path) {
                    mode = http::json_str(&f, "uploadMode").unwrap_or(mode);
                    ignore = http::json_get(&f, "shouldIgnore")
                        .map(|v| v.__bool__())
                        .unwrap_or(false);
                }
            }
        }
        Ok((mode, ignore))
    }

    /// The git-LFS batch + upload of one file; its `(oid, size)`.
    fn lfs_upload(
        &self,
        repo_id: &str,
        repo_type: &str,
        content: &[u8],
        token: Option<&str>,
    ) -> Result<(String, usize)> {
        let oid = http::sha256_hex(content);
        let size = content.len();
        let url = alloc::format!(
            "{}/{}{repo_id}.git/info/lfs/objects/batch",
            self.endpoint,
            Self::url_prefix(repo_type)
        );
        let body = alloc::format!(
            r#"{{"operation":"upload","transfers":["basic","multipart"],"objects":[{{"oid":"{oid}","size":{size}}}],"hash_algo":"sha256"}}"#
        );
        let resp = self
            .http
            .send_with_retry("POST", &url, Some(body.as_bytes()), || {
                let mut h = self.headers(token, Some("application/vnd.git-lfs+json"));
                h.push((
                    String::from("accept"),
                    String::from("application/vnd.git-lfs+json"),
                ));
                Ok(h)
            })?;
        if !resp.ok() {
            return Err(hub_error(&resp, &url));
        }
        let json = crate::_compat::pyjson::loads(&resp.text())?;
        let objects = http::json_get(&json, "objects").unwrap_or(Arg::None);
        let object = objects
            .__iter__()
            .ok()
            .and_then(|items| items.into_iter().next())
            .ok_or_else(|| LailaError::value_error("Malformed LFS batch response: no objects."))?;
        if let Some(err) = http::json_get(&object, "error") {
            return Err(LailaError::value_error(alloc::format!(
                "Error while uploading to LFS store: {}",
                http::json_str(&err, "message").unwrap_or_default()
            )));
        }
        let upload = http::json_get(&object, "actions").and_then(|a| http::json_get(&a, "upload"));
        let Some(upload) = upload else {
            // Already on the LFS store: nothing to upload.
            return Ok((oid, size));
        };
        let href = http::json_str(&upload, "href").unwrap_or_default();
        let header = http::json_get(&upload, "header").unwrap_or(Arg::None);
        let header_pairs: Vec<(String, String)> = match &header {
            Arg::Dict(d) => d
                .read()
                .iter()
                .map(|(k, v)| (k.__str__().to_ascii_lowercase(), v.__str__()))
                .collect(),
            _ => Vec::new(),
        };
        let chunk_size = header_pairs
            .iter()
            .find(|(k, _)| k == "chunk_size")
            .and_then(|(_, v)| v.parse::<usize>().ok());
        match chunk_size {
            None => {
                // Basic transfer: one PUT of the bytes to the signed URL.
                let resp = self
                    .http
                    .send_with_retry("PUT", &href, Some(content), || Ok(header_pairs.clone()))?;
                if !resp.ok() {
                    return Err(hub_error(&resp, &href));
                }
            }
            Some(chunk) => {
                // Multipart transfer: numbered part URLs, then completion.
                let mut parts: Vec<String> = Vec::new();
                for (i, piece) in content.chunks(chunk.max(1)).enumerate() {
                    let part_url = header_pairs
                        .iter()
                        .find(|(k, _)| *k == (i + 1).to_string())
                        .map(|(_, v)| v.clone())
                        .ok_or_else(|| {
                            LailaError::value_error(
                                "Malformed LFS multipart response: missing part URL.",
                            )
                        })?;
                    let resp = self
                        .http
                        .send_with_retry("PUT", &part_url, Some(piece), || Ok(Vec::new()))?;
                    if !resp.ok() {
                        return Err(hub_error(&resp, &part_url));
                    }
                    let etag = resp.header("etag").unwrap_or("").to_string();
                    parts.push(alloc::format!(
                        r#"{{"partNumber":{},"etag":{}}}"#,
                        i + 1,
                        js(&etag)
                    ));
                }
                let body = alloc::format!(r#"{{"oid":"{oid}","parts":[{}]}}"#, parts.join(","));
                let resp =
                    self.http
                        .send_with_retry("POST", &href, Some(body.as_bytes()), || {
                            Ok(self.headers(token, Some("application/json")))
                        })?;
                if !resp.ok() {
                    return Err(hub_error(&resp, &href));
                }
            }
        }
        Ok((oid, size))
    }

    /// `api.create_commit(...)`: one commit of *operations*.
    fn create_commit(
        &self,
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        commit_message: &str,
        operations: &[CommitOp],
        token: Option<&str>,
    ) -> Result<()> {
        let url = alloc::format!(
            "{}/api/{}/{repo_id}/commit/{}",
            self.endpoint,
            Self::api_plural(repo_type),
            http::uri_encode(revision, false)
        );
        let mut lines: Vec<String> = alloc::vec![alloc::format!(
            r#"{{"key":"header","value":{{"summary":{},"description":""}}}}"#,
            js(commit_message)
        )];
        for op in operations {
            lines.push(match op {
                CommitOp::File { path, content } => alloc::format!(
                    r#"{{"key":"file","value":{{"content":"{}","path":{},"encoding":"base64"}}}}"#,
                    http::base64_encode(content),
                    js(path)
                ),
                CommitOp::LfsFile { path, oid, size } => alloc::format!(
                    r#"{{"key":"lfsFile","value":{{"path":{},"algo":"sha256","oid":"{oid}","size":{size}}}}}"#,
                    js(path)
                ),
                CommitOp::Deleted { path } => alloc::format!(
                    r#"{{"key":"deletedFile","value":{{"path":{}}}}}"#,
                    js(path)
                ),
            });
        }
        let body = lines.join("\n");
        self.call(
            "POST",
            &url,
            token,
            Some("application/x-ndjson"),
            Some(body.as_bytes()),
        )
        .map(|_| ())
    }

    /// `api.upload_file(path_or_fileobj, path_in_repo, repo_id, repo_type,
    /// revision, commit_message)`
    pub fn upload_file(
        &self,
        content: &[u8],
        path_in_repo: &str,
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        commit_message: &str,
    ) -> Result<()> {
        Self::check_repo_type(repo_type)?;
        let (mode, ignore) =
            self.preupload(repo_id, repo_type, revision, path_in_repo, content, None)?;
        if ignore {
            // The Hub's `.gitignore` excludes the path: the SDK skips it.
            return Ok(());
        }
        let op = if mode == "lfs" {
            let (oid, size) = self.lfs_upload(repo_id, repo_type, content, None)?;
            CommitOp::LfsFile {
                path: path_in_repo.to_string(),
                oid,
                size,
            }
        } else {
            CommitOp::File {
                path: path_in_repo.to_string(),
                content: content.to_vec(),
            }
        };
        self.create_commit(repo_id, repo_type, revision, commit_message, &[op], None)
    }

    /// `api.delete_file(path_in_repo, repo_id, repo_type, revision,
    /// commit_message)`
    pub fn delete_file(
        &self,
        path_in_repo: &str,
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        commit_message: &str,
    ) -> Result<()> {
        Self::check_repo_type(repo_type)?;
        self.create_commit(
            repo_id,
            repo_type,
            revision,
            commit_message,
            &[CommitOp::Deleted {
                path: path_in_repo.to_string(),
            }],
            None,
        )
    }
}

// ---------------------------------------------------------------------------
// The backend

/// `HuggingFacePool`'s state: the API handle (`self._api`).
#[derive(Default)]
pub struct HuggingFaceBackend {
    api: Mutex<Option<Arc<HfApi>>>,
}

impl core::fmt::Debug for HuggingFaceBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("HuggingFaceBackend")
    }
}

fn str_field(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

/// The pool's repo coordinates, read once per call.
struct Repo {
    repo_id: String,
    repo_type: String,
    revision: String,
    token: Option<String>,
}

fn repo(pool: &PoolObject) -> Repo {
    Repo {
        repo_id: str_field(pool, "repo_id").unwrap_or_default(),
        repo_type: str_field(pool, "repo_type").unwrap_or_else(|| String::from("model")),
        revision: str_field(pool, "revision").unwrap_or_else(|| String::from("main")),
        token: str_field(pool, "token"),
    }
}

impl HuggingFaceBackend {
    /// Return the `HfApi` instance, creating it on first call.
    pub fn _get_api(&self, pool: &PoolObject) -> Result<Arc<HfApi>> {
        if let Some(a) = self.api.lock().as_ref() {
            return Ok(a.clone());
        }
        let api = Arc::new(HfApi::new(str_field(pool, "token")));
        *self.api.lock() = Some(api.clone());
        Ok(api)
    }

    /// Return the normalised path prefix (no leading/trailing slashes).
    pub fn _prefix(&self, pool: &PoolObject) -> String {
        str_field(pool, "path_prefix")
            .unwrap_or_default()
            .trim_matches('/')
            .to_string()
    }

    /// Build the in-repo file path for *key*.
    pub fn _object_key(&self, pool: &PoolObject, key: &str) -> String {
        let encoded = http::quote_key(key);
        let prefix = self._prefix(pool);
        if !prefix.is_empty() {
            return alloc::format!("{prefix}/{encoded}.json");
        }
        alloc::format!("{encoded}.json")
    }

    /// Extract the logical key from an in-repo file path.
    pub fn _logical_key(&self, pool: &PoolObject, object_key: &str) -> String {
        let prefix = self._prefix(pool);
        let mut cleaned = object_key;
        if !prefix.is_empty() {
            if let Some(rest) = cleaned.strip_prefix(&alloc::format!("{prefix}/")) {
                cleaned = rest;
            }
        }
        if let Some(rest) = cleaned.strip_suffix(".json") {
            cleaned = rest;
        }
        http::unquote_key(cleaned)
    }

    /// `hf_hub_download(...)` for *key*, `None` when the entry is absent.
    fn download(&self, pool: &PoolObject, key: &str) -> Result<Option<Vec<u8>>> {
        let r = repo(pool);
        let api = self._get_api(pool)?;
        match api.hf_hub_download(
            &r.repo_id,
            &self._object_key(pool, key),
            &r.repo_type,
            &r.revision,
            r.token.as_deref(),
        ) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if is_entry_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl PoolBackend for HuggingFaceBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Release the API handle.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        *self.api.lock() = None;
        Some(Ok(()))
    }

    /// Download and parse the JSON file for *key*, or return `None`.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held = pool.held();
        match self.download(pool, key)? {
            Some(raw) => crate::_compat::pyjson::loads(&String::from_utf8_lossy(&raw)),
            None => Ok(Arg::None),
        }
    }

    /// Upload *entry* as a JSON file to the Hub repo.
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
                "HuggingFacePool expects a serialized JSON string.",
            ));
        };
        let _held = pool.held();
        let r = repo(pool);
        self._get_api(pool)?.upload_file(
            value.as_bytes(),
            &self._object_key(pool, key),
            &r.repo_id,
            &r.repo_type,
            &r.revision,
            &alloc::format!("laila: set {key}"),
        )
    }

    /// Delete the file for *key* from the Hub repo; no-op if absent.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        let r = repo(pool);
        match self._get_api(pool)?.delete_file(
            &self._object_key(pool, key),
            &r.repo_id,
            &r.repo_type,
            &r.revision,
            &alloc::format!("laila: delete {key}"),
        ) {
            Ok(()) => Ok(()),
            Err(e) if is_entry_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Remove all entries from the pool.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        for key in self._keys(pool)? {
            self._delete(pool, &key)?;
        }
        Ok(())
    }

    /// `True` if *key* exists in the Hub repo.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let _held = pool.held();
        Ok(self.download(pool, key)?.is_some())
    }

    /// Return all keys in the Hub repo under the configured prefix.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let prefix = self._prefix(pool);
        let r = repo(pool);
        let all_files = self._get_api(pool)?.list_repo_files(
            &r.repo_id,
            &r.repo_type,
            &r.revision,
            r.token.as_deref(),
        )?;
        let mut keys = Vec::new();
        for path in all_files {
            if !prefix.is_empty() && !path.starts_with(&alloc::format!("{prefix}/")) {
                continue;
            }
            if !path.ends_with(".json") {
                continue;
            }
            keys.push(self._logical_key(pool, &path));
        }
        Ok(keys)
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_api" => Some(Ok(match self.api.lock().as_ref() {
                Some(a) => Arg::Str(alloc::format!("<HfApi {}>", a.endpoint()).into()),
                None => Arg::None,
            })),
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
            "_prefix" => Some(Ok(self._prefix(pool).into_arg())),
            "_object_key" => Some(first().map(|k| self._object_key(pool, &k).into_arg())),
            "_logical_key" => Some(first().map(|k| self._logical_key(pool, &k).into_arg())),
            "_get_api" => Some(
                self._get_api(pool)
                    .map(|a| Arg::Str(alloc::format!("<HfApi {}>", a.endpoint()).into())),
            ),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_get_api",
            "_prefix",
            "_object_key",
            "_logical_key",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "HuggingFacePool",
        "laila.data.huggingface.huggingface",
        alloc::vec![],
        alloc::vec![
            (String::from("repo_id"), field_required("str")),
            (
                String::from("repo_type"),
                field_default("str", Arg::Str("model".into()))
            ),
            (
                String::from("revision"),
                field_default("str", Arg::Str("main".into()))
            ),
            (
                String::from("token"),
                field_default("str | None", Arg::None)
            ),
            (
                String::from("path_prefix"),
                field_default("str", Arg::Str("laila_pool".into()))
            ),
            (
                String::from("transformations"),
                field_default(
                    "TransformationSequence | None",
                    crate::entry::transformation_base64()
                )
            ),
        ],
        alloc::vec![(String::from("_api"), private_default(Arg::None))],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// Hugging Face Hub-backed pool.
    ///
    /// Stores each entry as one JSON object file in a Hub repo (public or
    /// private). Objects are keyed by entry global_id directly
    /// (URL-encoded).
    HuggingFacePool,
    HuggingFaceBackend
);

impl ModelBase for HuggingFacePool {
    fn __class__() -> &'static ModelClass {
        HuggingFacePool::__class__()
    }
}

impl HuggingFacePool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `HuggingFacePool(repo_id, *, repo_type="model", revision="main", token=None, path_prefix="laila_pool", **kwargs)`
    #[pysig("HuggingFacePool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<HuggingFacePool> {
        Ok(HuggingFacePool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(HuggingFaceBackend::default())),
        )?))
    }
    /// `pool.repo_id`
    pub fn repo_id(&self) -> Arg {
        self.0.field("repo_id").unwrap_or(Arg::None)
    }
    /// `pool.repo_type`
    pub fn repo_type(&self) -> Arg {
        self.0.field("repo_type").unwrap_or(Arg::None)
    }
    /// `pool.revision`
    pub fn revision(&self) -> Arg {
        self.0.field("revision").unwrap_or(Arg::None)
    }
    /// `pool.path_prefix`
    pub fn path_prefix(&self) -> Arg {
        self.0.field("path_prefix").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for HuggingFacePool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        HuggingFacePool::new(args).map(IntoArg::into_arg)
    }
}

/// `HuggingFacePool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    HuggingFacePool::__class__().as_pyclass().into_arg()
}

impl PyArg for HuggingFaceBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
