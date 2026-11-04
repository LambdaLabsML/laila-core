//! Trust gate for constitutions rebuilt from storage or the wire.
//!
//! A constitution is Python source that gets `exec`'d when an entry is
//! materialised. Entries are rebuilt from pool files and from peer replies,
//! i.e. from inputs the reading process did not produce. Two gates make sure
//! only code the reader could have produced itself ever runs:
//!
//! - **Simple constitutions** (transformation inverse chains) are accepted
//!   only when every snippet matches the recovery-code *template* of a
//!   loaded transformation class, or is byte-identical to a snippet this
//!   process generated (see `is_trusted_backward_code`). Templates are
//!   parametrised on `backward_kwargs` (a literal dict) and, for Fernet, the
//!   key fingerprint, so records written by other processes, hosts or the
//!   JS/C/Rust ports still match.
//! - **Complex constitutions** (user builders) carry an HMAC-SHA256 over the
//!   canonical `{code, manifest_global_id}` pair, keyed by a per-user secret.
//!   The secret lives in `~/.laila/constitution.key` (created on first use,
//!   mode `0600`); it is deliberately *not* derived from
//!   `laila.encryption_key` so rotating the data-encryption key never
//!   invalidates stored constitutions, and it is independent of
//!   `laila.set_default_directory` so every process of the same user on a
//!   host trusts the same code. To let another host (or user) read complex
//!   constitutions you wrote, copy that file.
//!
//! On `no_std` there is no home directory: the secret is process-local
//! random bytes (constitutions written are only readable by this process).
//!
//! Residual risk, documented: `PickleSerializer` and `allow_pickle` /
//! `weights_only` kwargs on the numpy / torch templates remain a
//! deserialisation vector for hostile pool *payloads* -- that is pickle's
//! contract, not a constitution hole.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::_compat::pyjson::{self, DumpsOptions};
use crate::python::sync::Mutex;
use crate::python::{Arg, IntoArg, LailaError, PyDictBuilder, Result};

use super::super::compdata::transformation::base::is_trusted_backward_code;

pub const _SECRET_ENV: &str = "LAILA_CONSTITUTION_KEY_FILE";
pub const _SECRET_FILENAME: &str = "constitution.key";
pub const _SECRET_NBYTES: usize = 32;
static _LOCK_AND_CACHE: Mutex<Option<Vec<u8>>> = Mutex::new(None);

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&alloc::format!("{b:02x}"));
    }
    out
}

/// `secrets.token_hex(n).encode("ascii")`
#[cfg(feature = "std")]
fn token_hex(n: usize) -> Vec<u8> {
    hex(&crate::_compat::random::urandom(n)).into_bytes()
}

/// `secrets.token_bytes(n)`
fn token_bytes(n: usize) -> Vec<u8> {
    crate::_compat::random::urandom(n)
}

/// `bytes.strip()` -- ASCII whitespace at both ends.
#[cfg(feature = "std")]
fn strip(data: &[u8]) -> Vec<u8> {
    let ws = |b: &u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0b' | b'\x0c');
    let start = data.iter().position(|b| !ws(b)).unwrap_or(data.len());
    let end = data.iter().rposition(|b| !ws(b)).map_or(start, |i| i + 1);
    data[start..end].to_vec()
}

/// `_secret_path()`: `$LAILA_CONSTITUTION_KEY_FILE` (user-expanded) or
/// `~/.laila/constitution.key`.
#[cfg(feature = "std")]
pub fn _secret_path() -> std::path::PathBuf {
    use crate::python::stdlib::os;
    if let Ok(over) = std::env::var(_SECRET_ENV) {
        if !over.is_empty() {
            let expanded = os.path().expanduser(over.as_str()).unwrap_or(over);
            return std::path::PathBuf::from(expanded);
        }
    }
    let home = os
        .path()
        .expanduser("~")
        .ok()
        .filter(|h| h != "~")
        .unwrap_or_else(|| ".".to_string());
    std::path::PathBuf::from(home)
        .join(".laila")
        .join(_SECRET_FILENAME)
}

#[cfg(feature = "std")]
fn read_or_create_secret(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    use std::io::Write;
    if path.exists() {
        let data = strip(&std::fs::read(path)?);
        if data.len() >= 16 {
            return Ok(data);
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let fresh = token_hex(_SECRET_NBYTES);
    let tmp = path.with_extension("tmp");
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut fh = opts.open(&tmp)?;
        let written = fh.write_all(&fresh);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // ``os.chmod(tmp, 0o600)`` -- errors ignored.
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        written?;
    }
    std::fs::rename(&tmp, path)?;
    // Another process may have raced us: honour whatever won.
    Ok(strip(&std::fs::read(path)?))
}

/// Return the per-user HMAC secret, creating it on first use.
///
/// Falls back to a process-local random secret (with a logged warning) when
/// the home directory is not writable; constitutions written in that mode
/// are only readable by the same process.
pub fn constitution_secret() -> Vec<u8> {
    let mut cached = _LOCK_AND_CACHE.lock();
    if let Some(s) = &*cached {
        return s.clone();
    }
    #[cfg(feature = "std")]
    let secret = {
        let path = _secret_path();
        match read_or_create_secret(&path) {
            Ok(s) => s,
            Err(_exc) => {
                // MERGE: ``get_logger().warning(f"constitution secret file {path} is
                // not usable ({exc}); using a process-local secret -- complex
                // constitutions written now are only readable by this process")``
                // -- no logger shim exists in laila-rs yet.
                token_bytes(_SECRET_NBYTES)
            }
        }
    };
    #[cfg(not(feature = "std"))]
    let secret = token_bytes(_SECRET_NBYTES);
    *cached = Some(secret.clone());
    secret
}

/// Forget the cached secret (tests that switch `LAILA_CONSTITUTION_KEY_FILE`).
pub fn _reset_secret_cache() {
    *_LOCK_AND_CACHE.lock() = None;
}

/// `json.dumps({"code": code, "manifest_global_id": gid}, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")`
fn _canonical(code: &Arg, manifest_global_id: &Arg) -> Vec<u8> {
    let mut d = PyDictBuilder::new();
    d.insert("code", code.clone());
    d.insert("manifest_global_id", manifest_global_id.clone());
    let opts = DumpsOptions {
        sort_keys: true,
        separators: Some((",".to_string(), ":".to_string())),
        ensure_ascii: false,
        ..Default::default()
    };
    pyjson::dumps(&d.finish(), &opts)
        .map(String::into_bytes)
        .unwrap_or_default()
}

fn _sign(payload: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(&constitution_secret())
        .expect("HMAC accepts any key length");
    mac.update(payload);
    hex(&mac.finalize().into_bytes())
}

/// `hmac.compare_digest(a, b)` for two `str`s.
fn compare_digest(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b) {
        acc |= x ^ y;
    }
    acc == 0
}

fn _verify(payload: &[u8], signature: &Arg) -> bool {
    let Arg::Str(sig) = signature else {
        return false;
    };
    if sig.is_empty() {
        return false;
    }
    compare_digest(&_sign(payload), sig)
}

/// HMAC-SHA256 (hex) over the canonical complex-constitution payload.
pub fn sign_complex(code: impl IntoArg, manifest_global_id: impl IntoArg) -> String {
    _sign(&_canonical(
        &code.into_arg(),
        &manifest_global_id.into_arg(),
    ))
}

/// Constant-time check of *signature* against the local secret.
pub fn verify_complex(
    code: impl IntoArg,
    manifest_global_id: impl IntoArg,
    signature: impl IntoArg,
) -> bool {
    _verify(
        &_canonical(&code.into_arg(), &manifest_global_id.into_arg()),
        &signature.into_arg(),
    )
}

/// `list(codes or ())`
fn codes_list(codes: &Arg) -> Result<Vec<Arg>> {
    if !codes.__bool__() {
        return Ok(Vec::new());
    }
    codes.__iter__()
}

/// `json.dumps(list(codes or ()), separators=(",", ":"), ensure_ascii=False).encode("utf-8")`
fn _canonical_codes(codes: &Arg) -> Result<Vec<u8>> {
    let items = codes_list(codes)?;
    let opts = DumpsOptions {
        separators: Some((",".to_string(), ":".to_string())),
        ensure_ascii: false,
        ..Default::default()
    };
    Ok(pyjson::dumps(&items.into_arg(), &opts)?.into_bytes())
}

/// HMAC-SHA256 (hex) over a simple constitution's code list.
pub fn sign_simple(codes: impl IntoArg) -> Result<String> {
    Ok(_sign(&_canonical_codes(&codes.into_arg())?))
}

/// Raise `ValueError` unless *codes* may run in this process.
///
/// Accepted when either every snippet matches a loaded transformation's
/// recovery-code template (or was generated here) -- the normal case for
/// pipeline-emitted constitutions, including ones written by other hosts
/// and by the JS/C/Python ports -- or the list as a whole carries a valid
/// `_hmac` from a writer sharing this user's secret (hand-written simple
/// constitutions).
pub fn check_simple_codes(codes: impl IntoArg, signature: impl IntoArg) -> Result<()> {
    let codes = codes.into_arg();
    let signature = signature.into_arg();
    if !signature.is_none() && _verify(&_canonical_codes(&codes)?, &signature) {
        return Ok(());
    }
    for code in codes_list(&codes)? {
        if !is_trusted_backward_code(&code) {
            let text = match &code {
                Arg::Str(s) => s.to_string(),
                other => other.__repr__(),
            };
            // ``head = text.strip().splitlines()[:1]``
            let head: Vec<Arg> = text
                .trim()
                .lines()
                .take(1)
                .map(|l| Arg::Str(l.into()))
                .collect();
            return Err(LailaError::value_error(alloc::format!(
                "untrusted constitution: recovery code does not match any loaded \
                 transformation template and was not generated by this process ({})",
                head.into_arg().__repr__()
            )));
        }
    }
    Ok(())
}
