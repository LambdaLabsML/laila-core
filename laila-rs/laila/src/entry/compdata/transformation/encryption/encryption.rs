//! Fernet symmetric encryption / decryption data transformation.
//!
//! The [`FernetEncryption`] transformation slots into a transformation
//! pipeline as the encryption step. It wraps a UTF-8 string in a Fernet
//! token (AES-128-CBC + HMAC-SHA256, time-stamped, URL-safe base64 encoded)
//! using the `cryptography` port in `crate::_codecs::fernet`.
//!
//! A pipeline that compresses then encrypts (e.g.
//! `[Json, Zlib, FernetEncryption, Base64]`) gives you compact,
//! ciphertext-only blobs that can sit safely in any text-only pool backend.
//!
//! # Optional TTL
//! `backward_kwargs={"ttl": seconds}` instructs `backward` to reject tokens
//! older than `seconds` (raising `ValueError`). The same machinery is
//! exposed in the standalone recovery snippet emitted into `backward_code`.
//!
//! # Key handling
//! The key is **never** written into `backward_code` or into any serialized
//! form of the transformation. Like every other secret in laila it lives in
//! `laila.args`:
//!
//! - canonical location: `laila.args.encryption.key`
//! - convenience alias:  `laila.encryption_key` (getter / setter)
//!
//! Both the writer and the reader are expected to configure the *same* key
//! in their own process. Construct the transformation with no `key=` to
//! pick it up from there; an explicit `key=` still wins and is useful for
//! ad-hoc / test pipelines.
//!
//! The recovery snippet carries only a short SHA-256 fingerprint of the
//! write-time key, so a reader configured with a *different* key fails with
//! a descriptive `ValueError` instead of a bare `InvalidToken`.
//!
//! [`get_encryption_key`] / [`set_encryption_key`] are the
//! `laila.encryption_key` property (the `Laila` facade forwards to them);
//! both read and write `laila.args.encryption.key`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::fernet::Fernet;
use crate::_codecs::recovery_codes::{self, emit_fernet, Backward, Recognized};
use crate::python::methods::utf8_decode;
use crate::python::object::bytes;
use crate::python::sync::RwLock;
use crate::python::{Arg, Args, ExcKind, IntoArg, LailaError, PyDictBuilder, Result};

use super::super::base::{
    _is_literal_dict, kw_pairs, pydantic_validation_error, transformation_object,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS, _SLOT_KWARGS_REPR,
};

pub const _FINGERPRINT_HEX_LEN: usize = 12;

/// `_FINGERPRINT_SLOT = "0" * _FINGERPRINT_HEX_LEN`
pub fn _FINGERPRINT_SLOT() -> String {
    "0".repeat(_FINGERPRINT_HEX_LEN)
}

/// `_FINGERPRINT_RE = ^[0-9a-f]{12}$`
fn _fingerprint_matches(g: &str) -> bool {
    g.len() == _FINGERPRINT_HEX_LEN
        && g.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Standalone recovery code: no key material, only its fingerprint.
///
/// The reader resolves the key from its own `laila.args`. Kept as a
/// module-level function so the recovery-code *template* (used by the
/// constitution trust gate) and the emitted code share one source.
pub fn _render_backward_code(fingerprint: &str, backward_kwargs: &Arg) -> String {
    emit_fernet(fingerprint, Some(backward_kwargs))
}

/// `_coerce_key_bytes(v)`: `str` -> UTF-8 bytes, `bytes`/`bytearray` as-is,
/// anything else `TypeError("Encryption.key must be bytes or str")`.
pub fn _coerce_key_bytes(v: &Arg) -> Result<Vec<u8>> {
    match v {
        Arg::Str(s) => Ok(s.as_bytes().to_vec()),
        other => other
            .as_bytes_like()
            .ok_or_else(|| LailaError::type_error("Encryption.key must be bytes or str")),
    }
}

/// Return the short SHA-256 fingerprint laila embeds in recovery code.
pub fn key_fingerprint(key: &Arg) -> Result<String> {
    Ok(crate::_codecs::fernet::key_fingerprint(&_coerce_key_bytes(
        key,
    )?))
}

/// `laila.encryption_key = value`: store *value* under
/// `laila.args.encryption.key` (`args.encryption.key = value`; the
/// `DotMap` autovivifies the `encryption` section).
pub fn set_encryption_key(key: impl IntoArg) {
    let args = crate::basics::definitions::cli_capable::args();
    if let Ok(section) = args.__getattr__("encryption") {
        let _ = section.__setattr__("key", key);
    }
}

/// `laila.encryption_key` -- the process-wide Fernet key used by
/// `FernetEncryption` when no explicit `key=` is given; alias of
/// `laila.args.encryption.key`. `None` when unset (or set to `""` / `{}`).
pub fn get_encryption_key() -> Arg {
    let args = crate::basics::definitions::cli_capable::args();
    // ``args.get("encryption")`` -- ``laila.args`` is a ``_LailaArgs`` (a
    // ``DotMap`` subclass); ``unittest.mock.patch("laila.args", ..)`` may
    // have swapped in a plain mapping.
    let Ok(section) = args.get(Args::from(Arg::Str("encryption".into()))) else {
        return Arg::None;
    };
    // ``not hasattr(section, "get")``: a plain value under ``encryption``.
    let Some(section) = crate::_compat::dotmap::DotMap::downcast(&section) else {
        return Arg::None;
    };
    let Ok(key) = section.get("key", Arg::None) else {
        return Arg::None;
    };
    if key.is_none() || key == "" || key == PyDictBuilder::new().finish() {
        Arg::None
    } else {
        key
    }
}

/// Return the process-wide encryption key configured in `laila.args`.
///
/// Looks up `laila.args.encryption.key` (also reachable as
/// `laila.encryption_key`) without auto-creating DotMap nodes.
///
/// `RuntimeError` when no key is configured in this process; `ValueError`
/// when a key is configured but does not match *expected_fingerprint*.
pub fn resolve_encryption_key(expected_fingerprint: Option<&str>) -> Result<Vec<u8>> {
    // ``section = laila.args.get("encryption"); key = section.get("key")``
    let key = get_encryption_key();
    if key.is_none() {
        return Err(LailaError::runtime_error(
            "no encryption key configured in this process; set \
             `laila.encryption_key = <fernet key>` (i.e. laila.args.encryption.key) \
             on both the writing and the reading side",
        ));
    }
    let key_bytes = _coerce_key_bytes(&key)?;
    if let Some(expected) = expected_fingerprint {
        if crate::_codecs::fernet::key_fingerprint(&key_bytes) != expected {
            return Err(LailaError::value_error(alloc::format!(
                "configured encryption key (laila.encryption_key) does not match the key used \
                 at write time (fingerprint {expected}); set the same key as the writer"
            )));
        }
    }
    Ok(key_bytes)
}

/// `ttl` keyword -> `Fernet.decrypt(token, ttl=...)` argument.
fn ttl_of(kwargs: &Arg) -> Result<Option<u64>> {
    let ttl = kw_pairs(kwargs)?
        .into_iter()
        .find(|(k, _)| k == "ttl")
        .map(|(_, v)| v)
        .unwrap_or(Arg::None);
    match ttl {
        Arg::None => Ok(None),
        Arg::Int(i) if i >= 0 => Ok(Some(i as u64)),
        Arg::Int(_) => Ok(Some(0)),
        other => Err(LailaError::type_error(alloc::format!(
            "ttl must be an int or None, not {}",
            other.type_name()
        ))),
    }
}

/// `f.decrypt(token, ttl=ttl)` with `InvalidToken` mapped to the laila
/// `ValueError`, then `.decode("utf-8")`.
fn decrypt_text(f: &Fernet, data: &str, ttl: Option<u64>) -> Result<Arg> {
    let token = bytes(data.as_bytes());
    match f.decrypt_token(&token, ttl) {
        Ok(out) => Ok(Arg::Str(utf8_decode(&out)?.into())),
        Err(e) if e.kind == ExcKind::InvalidToken => Err(LailaError::value_error(
            "Invalid Fernet token or TTL expired",
        )),
        Err(e) => Err(e),
    }
}

/// Reversible Fernet symmetric encryption transformation.
///
/// `key`: Fernet-compatible encryption key (`str` or `bytes`). When omitted
/// the key is read from `laila.args.encryption.key` / `laila.encryption_key`.
/// Never serialized or shown in `repr`.
pub struct FernetEncryption {
    fields: TransformationFields,
    key: RwLock<Vec<u8>>,
    _fernet: RwLock<Fernet>,
    // ``True`` when the caller pinned a key; ``False`` means "follow
    // ``laila.encryption_key``", re-resolved on every forward/backward so
    // a rotation mid-run is honoured by pools built before it.
    _explicit_key: bool,
}

impl Clone for FernetEncryption {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.clone(),
            key: RwLock::new(self.key.read().clone()),
            _fernet: RwLock::new(self._fernet.read().clone()),
            _explicit_key: self._explicit_key,
        }
    }
}

impl FernetEncryption {
    pub const NAME: &'static str = "fernet";

    /// `FernetEncryption(key=None, **fields)`: initialise the Fernet
    /// encryptor and build the backward recovery code (`model_post_init`).
    pub fn new(args: impl Into<Args>) -> Result<FernetEncryption> {
        ensure_backend();
        let all: Args = args.into();
        let mut a = all.clone();
        let key = a.pop_or("key", Arg::None);
        // ``key: str | bytes | None`` -- pydantic validates the union first.
        if !matches!(
            key,
            Arg::None | Arg::Str(_) | Arg::Bytes(_) | Arg::ByteArray(_)
        ) {
            return Err(pydantic_validation_error(
                "FernetEncryption",
                &[
                    (
                        "key.str",
                        "Input should be a valid string",
                        "string_type",
                        &key,
                    ),
                    (
                        "key.bytes",
                        "Input should be a valid bytes",
                        "bytes_type",
                        &key,
                    ),
                ],
            ));
        }
        let fields = TransformationFields::from_args("FernetEncryption", Self::NAME, &a)?;
        // ``_coerce_key``: string keys to bytes; ``None`` defers to ``laila.args``.
        let explicit = !key.is_none();
        let key_bytes = if explicit {
            _coerce_key_bytes(&key)?
        } else {
            // Fail fast when no key is configured at all.
            resolve_encryption_key(None)?
        };
        // ``model_post_init``: a ``ValueError`` raised there (a malformed
        // Fernet key) is reported by pydantic as a model-level validation
        // error whose input is the whole constructor mapping.
        let fernet = match Fernet::from_key(&bytes(&key_bytes)) {
            Ok(f) => f,
            Err(e) if e.kind.is_subclass_of(ExcKind::ValueError) => {
                let msg = alloc::format!("Value error, {}", e.__str__());
                return Err(pydantic_validation_error(
                    "FernetEncryption",
                    &[("", &msg, "value_error", &all.as_dict())],
                ));
            }
            Err(e) => return Err(e),
        };
        let t = FernetEncryption {
            fields,
            key: RwLock::new(Vec::new()),
            _fernet: RwLock::new(fernet),
            _explicit_key: explicit,
        };
        t.model_post_init(Arg::None, key_bytes)?;
        Ok(t)
    }

    /// Initialise the Fernet encryptor and build backward recovery code
    /// (`self.key` already resolved -- `resolve_encryption_key()` when no
    /// key was given -- by `new`, as pydantic validates before the hook).
    pub fn model_post_init(&self, __context: Arg, key: Vec<u8>) -> Result<()> {
        self._bind_key(key)
    }

    /// Point the encryptor and the recovery code at *key*.
    fn _bind_key(&self, key: Vec<u8>) -> Result<()> {
        let fernet = Fernet::from_key(&bytes(&key))?;
        let fp = crate::_codecs::fernet::key_fingerprint(&key);
        *self.key.write() = key;
        *self._fernet.write() = fernet;
        self.set_backward_code(&_render_backward_code(&fp, &self.backward_kwargs()));
        Ok(())
    }

    /// Re-resolve `laila.encryption_key` unless a key was pinned.
    fn _sync_key(&self) -> Result<()> {
        if self._explicit_key {
            return Ok(());
        }
        let live = resolve_encryption_key(None)?;
        if live != *self.key.read() {
            self._bind_key(live)?;
        }
        Ok(())
    }

    /// `self.key` (bytes).
    pub fn key(&self) -> Arg {
        bytes(&*self.key.read())
    }

    /// `self._explicit_key`
    pub fn _explicit_key(&self) -> bool {
        self._explicit_key
    }

    // --- recovery-code template (constitution trust gate) ---

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("key", Fernet::generate_key());
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        let probe = FernetEncryption::new(a)?;
        probe.set_backward_code(&_render_backward_code(
            &_FINGERPRINT_SLOT(),
            &_SLOT_KWARGS(),
        ));
        Ok(probe.backward_code())
    }

    /// The fingerprint is rendered via `!r` inside quotes, so the slot text
    /// is the bare hex and the captured group must be hex too.
    pub fn _template_slots() -> Vec<Slot> {
        alloc::vec![
            Slot {
                text: _FINGERPRINT_SLOT(),
                validate: _fingerprint_matches,
            },
            Slot {
                text: _SLOT_KWARGS_REPR(),
                validate: _is_literal_dict,
            },
        ]
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "FernetEncryption",
            probe: FernetEncryption::_template_probe,
            slots: FernetEncryption::_template_slots,
        }
    }
}

impl DataTransformation for FernetEncryption {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "FernetEncryption"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Encrypt a UTF-8 string and return the Fernet token as a string.
    ///
    /// `TypeError` if *data* is not a `str`.
    fn forward(&self, data: &Arg) -> Result<Arg> {
        let Arg::Str(text) = data else {
            return Err(LailaError::type_error(
                "Encryption.forward expects a str (e.g. Base64 text); place Base64() before \
                 FernetEncryption() -- see the transformation_encryption preset",
            ));
        };
        self._sync_key()?;
        let token = self._fernet.read().encrypt_bytes(text.as_bytes());
        Ok(Arg::Str(utf8_decode(&token)?.into()))
    }

    /// Decrypt a Fernet token back to the original string.
    ///
    /// `TypeError` if *data* is not a `str`; `ValueError` if the token is
    /// invalid or the TTL has expired.
    fn backward(&self, data: &Arg) -> Result<Arg> {
        let Arg::Str(text) = data else {
            return Err(LailaError::type_error(
                "Encryption.backward expects a Fernet token string (str)",
            ));
        };
        self._sync_key()?;
        let ttl = ttl_of(&self.backward_kwargs())?;
        let f = self._fernet.read().clone();
        decrypt_text(&f, text, ttl)
    }

    /// Two encryptors are never equal (pydantic compares the private
    /// `_fernet` objects, which have identity equality).
    fn __eq__(&self, other: &dyn DataTransformation) -> bool {
        other
            .as_any()
            .downcast_ref::<FernetEncryption>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }

    /// `self.key` is a field (excluded from `repr`), `self._explicit_key`
    /// a private attribute.
    fn __getattr_extra__(&self, name: &str) -> Option<Result<Arg>> {
        match name {
            "key" => Some(Ok(self.key())),
            "_explicit_key" => Some(Ok(Arg::Bool(self._explicit_key))),
            _ => None,
        }
    }
}

transformation_object!(FernetEncryption);

/// The native backend for the `fernet` recovery template: the Rust twin of
/// the emitted `backward(inp)` snippet. Registered with
/// `recovery_codes::register_backend` by the transformation registry seed.
fn fernet_backend(info: &Recognized) -> Result<Backward> {
    let fingerprint = info.fingerprint.clone().unwrap_or_default();
    let ttl = ttl_of(&info.kwargs)?;
    Ok(Arc::new(move |inp: &Arg| {
        let Arg::Str(text) = inp else {
            return Err(LailaError::type_error(
                "Encryption.backward expects a Fernet token string (str)",
            ));
        };
        let key = resolve_encryption_key(Some(&fingerprint))?;
        let f = Fernet::from_key(&bytes(&key))?;
        decrypt_text(&f, text, ttl)
    }))
}

/// Register the `fernet` backend (idempotent).
pub(crate) fn ensure_backend() {
    recovery_codes::register_backend("fernet", Arc::new(fernet_backend));
}
