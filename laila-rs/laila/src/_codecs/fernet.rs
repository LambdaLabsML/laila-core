//! `cryptography.fernet` -- byte-exact Fernet (spec version 0x80).
//!
//! ```text
//! token = urlsafe_b64( 0x80 | timestamp(8, BE) | IV(16) | AES-128-CBC/PKCS7(data) | HMAC-SHA256(32) )
//! key   = urlsafe_b64( signing_key(16) | encryption_key(16) )
//! ```
//!
//! Mirrors `cryptography/fernet.py`: `Fernet.generate_key`, `encrypt`,
//! `encrypt_at_time`, `_encrypt_from_parts`, `decrypt(token, ttl=None)`,
//! `decrypt_at_time`, `extract_timestamp` and `MultiFernet` (`encrypt`,
//! `decrypt`, `rotate`), raising `InvalidToken` under exactly the same
//! conditions (bad base64, wrong version byte, bad HMAC, expired TTL,
//! timestamp more than 60 s in the future, bad padding).
//!
//! The base64 layer is `crate::_codecs::base64` (the faithful `binascii`
//! port), so `Fernet(key)` / `decrypt(token)` accept `str` and `bytes`
//! exactly as the Python class does.

use alloc::string::String;
use alloc::vec::Vec;

use cbc::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::_codecs::base64::{urlsafe_b64decode, urlsafe_b64encode};
use crate::python::errors::ExcKind;
use crate::python::object::Arg;
use crate::python::{LailaError, Result};

type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
type HmacSha256 = Hmac<Sha256>;

/// `cryptography.fernet._MAX_CLOCK_SKEW`
const MAX_CLOCK_SKEW: u64 = 60;

const KEY_ERROR: &str = "Fernet key must be 32 url-safe base64-encoded bytes.";

/// `raise InvalidToken` (the class is raised bare in `cryptography`, so the
/// message is empty).
fn invalid_token() -> LailaError {
    LailaError::new(ExcKind::InvalidToken, "")
}

/// `os.urandom(n)` -- the platform entropy source (`_compat::random`).
fn random_bytes(n: usize) -> Vec<u8> {
    crate::_compat::random::urandom(n)
}

/// `int(time.time())`
fn now() -> u64 {
    #[cfg(feature = "std")]
    {
        let t = crate::python::stdlib::time.time();
        if t > 0.0 {
            t as u64
        } else {
            0
        }
    }
    #[cfg(not(feature = "std"))]
    {
        0
    }
}

/// `cryptography.fernet.Fernet(key, backend=None)`
#[derive(Clone)]
pub struct Fernet {
    _signing_key: [u8; 16],
    _encryption_key: [u8; 16],
}

impl core::fmt::Debug for Fernet {
    /// Key material is never printed (`<cryptography.fernet.Fernet object>`).
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("<cryptography.fernet.Fernet object>")
    }
}

impl Fernet {
    /// `Fernet(key, backend=None)` -- the Python-shaped constructor
    /// (`laila::python!` lowers `Fernet(key)` to `Fernet::new(args!(key))`).
    pub fn new(args: impl Into<crate::python::Args>) -> Result<Fernet> {
        let a: crate::python::Args = args.into();
        let key = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("key"));
        if key.is_none() {
            return Err(LailaError::type_error(
                "Fernet.__init__() missing 1 required positional argument: 'key'",
            ));
        }
        Self::from_key(&key)
    }

    /// `Fernet(key)` -- `key` is `str` or `bytes`, url-safe base64 of 32 bytes.
    ///
    /// `ValueError("Fernet key must be 32 url-safe base64-encoded bytes.")`
    /// on bad base64 or a wrong length; a non-`str`/`bytes` key raises the
    /// `TypeError` that `base64.urlsafe_b64decode` raises.
    pub fn from_key(key: &Arg) -> Result<Fernet> {
        let raw = match urlsafe_b64decode(key) {
            Ok(raw) => raw,
            Err(e) if e.kind == ExcKind::BinasciiError => {
                return Err(LailaError::value_error(KEY_ERROR))
            }
            Err(e) => return Err(e),
        };
        if raw.len() != 32 {
            return Err(LailaError::value_error(KEY_ERROR));
        }
        let mut signing = [0u8; 16];
        let mut encryption = [0u8; 16];
        signing.copy_from_slice(&raw[..16]);
        encryption.copy_from_slice(&raw[16..]);
        Ok(Fernet {
            _signing_key: signing,
            _encryption_key: encryption,
        })
    }

    /// `Fernet.generate_key()` -> `base64.urlsafe_b64encode(os.urandom(32))`
    pub fn generate_key() -> Arg {
        crate::python::object::bytes(Self::generate_key_bytes())
    }

    /// `Fernet.generate_key()` as raw bytes (typed Rust callers).
    pub fn generate_key_bytes() -> Vec<u8> {
        urlsafe_b64encode(&random_bytes(32))
    }

    /// `Fernet.encrypt(data)`
    pub fn encrypt_bytes(&self, data: &[u8]) -> Vec<u8> {
        self.encrypt_bytes_at_time(data, now())
    }

    /// `Fernet.encrypt_at_time(data, current_time)`
    pub fn encrypt_bytes_at_time(&self, data: &[u8], current_time: u64) -> Vec<u8> {
        let mut iv = [0u8; 16];
        iv.copy_from_slice(&random_bytes(16));
        self._encrypt_from_parts(data, current_time, &iv)
    }

    /// `Fernet._encrypt_from_parts(data, current_time, iv)`
    pub fn _encrypt_from_parts(&self, data: &[u8], current_time: u64, iv: &[u8; 16]) -> Vec<u8> {
        let ciphertext = Aes128CbcEnc::new((&self._encryption_key).into(), iv.into())
            .encrypt_padded_vec_mut::<Pkcs7>(data);
        let mut basic_parts = Vec::with_capacity(1 + 8 + 16 + ciphertext.len() + 32);
        basic_parts.push(0x80);
        basic_parts.extend_from_slice(&current_time.to_be_bytes());
        basic_parts.extend_from_slice(iv);
        basic_parts.extend_from_slice(&ciphertext);
        let mut h =
            HmacSha256::new_from_slice(&self._signing_key).expect("HMAC accepts any key length");
        h.update(&basic_parts);
        basic_parts.extend_from_slice(&h.finalize().into_bytes());
        urlsafe_b64encode(&basic_parts)
    }

    /// `Fernet.decrypt(token, ttl=None)`
    pub fn decrypt_token(&self, token: &Arg, ttl: Option<u64>) -> Result<Vec<u8>> {
        let (timestamp, data) = Self::_get_unverified_token_data(token)?;
        let time_info = ttl.map(|ttl| (ttl, now()));
        self._decrypt_data(&data, timestamp, time_info)
    }

    /// `Fernet.decrypt_at_time(token, ttl, current_time)` -- `ttl` must not be `None`.
    pub fn decrypt_token_at_time(
        &self,
        token: &Arg,
        ttl: Option<u64>,
        current_time: u64,
    ) -> Result<Vec<u8>> {
        let ttl = ttl.ok_or_else(|| {
            LailaError::value_error("decrypt_at_time() can only be used with a non-None ttl")
        })?;
        let (timestamp, data) = Self::_get_unverified_token_data(token)?;
        self._decrypt_data(&data, timestamp, Some((ttl, current_time)))
    }

    /// `Fernet.extract_timestamp(token)`
    pub fn extract_timestamp_of(&self, token: &Arg) -> Result<u64> {
        let (timestamp, data) = Self::_get_unverified_token_data(token)?;
        self._verify_signature(&data)?;
        Ok(timestamp)
    }

    /// `Fernet._get_unverified_token_data(token)` -> `(timestamp, data)`
    fn _get_unverified_token_data(token: &Arg) -> Result<(u64, Vec<u8>)> {
        if !matches!(token, Arg::Str(_) | Arg::Bytes(_)) {
            return Err(LailaError::type_error("token must be bytes or str"));
        }
        let data = urlsafe_b64decode(token).map_err(|_| invalid_token())?;
        if data.is_empty() || data[0] != 0x80 {
            return Err(invalid_token());
        }
        if data.len() < 9 {
            return Err(invalid_token());
        }
        let mut ts = [0u8; 8];
        ts.copy_from_slice(&data[1..9]);
        Ok((u64::from_be_bytes(ts), data))
    }

    /// `Fernet._verify_signature(data)` (constant-time HMAC compare).
    fn _verify_signature(&self, data: &[u8]) -> Result<()> {
        if data.len() < 32 {
            return Err(invalid_token());
        }
        let (body, given) = data.split_at(data.len() - 32);
        let mut h =
            HmacSha256::new_from_slice(&self._signing_key).expect("HMAC accepts any key length");
        h.update(body);
        h.verify_slice(given).map_err(|_| invalid_token())
    }

    /// `Fernet._decrypt_data(data, timestamp, time_info)`
    fn _decrypt_data(
        &self,
        data: &[u8],
        timestamp: u64,
        time_info: Option<(u64, u64)>,
    ) -> Result<Vec<u8>> {
        if let Some((ttl, current_time)) = time_info {
            if timestamp.saturating_add(ttl) < current_time {
                return Err(invalid_token());
            }
            if current_time.saturating_add(MAX_CLOCK_SKEW) < timestamp {
                return Err(invalid_token());
            }
        }
        self._verify_signature(data)?;
        // 1 + 8 + 16 (iv) + ciphertext + 32 (hmac)
        if data.len() < 25 + 32 {
            return Err(invalid_token());
        }
        let iv = &data[9..25];
        let ciphertext = &data[25..data.len() - 32];
        // whole AES blocks only (`& 15`: the crate's MSRV predates `is_multiple_of`)
        if ciphertext.is_empty() || ciphertext.len() & 15 != 0 {
            return Err(invalid_token());
        }
        Aes128CbcDec::new((&self._encryption_key).into(), iv.into())
            .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
            .map_err(|_| invalid_token())
    }
}

/// `cryptography.fernet.MultiFernet(fernets)`
#[derive(Clone)]
pub struct MultiFernet {
    _fernets: Vec<Fernet>,
}

impl core::fmt::Debug for MultiFernet {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "<cryptography.fernet.MultiFernet object ({} keys)>",
            self._fernets.len()
        )
    }
}

impl MultiFernet {
    /// `MultiFernet(fernets)` -- Python-shaped constructor over an iterable
    /// of `Fernet` objects.
    pub fn new(args: impl Into<crate::python::Args>) -> Result<Self> {
        let a: crate::python::Args = args.into();
        let seq = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("fernets"));
        let mut fernets = Vec::new();
        for f in seq.__iter__()? {
            match f.downcast_ref::<Fernet>() {
                Some(f) => fernets.push(f.clone()),
                None => {
                    return Err(LailaError::type_error(
                        "MultiFernet requires a list of Fernet instances",
                    ))
                }
            }
        }
        Self::from_fernets(fernets)
    }

    /// `MultiFernet(fernets)` -- `ValueError` when the list is empty.
    pub fn from_fernets(fernets: Vec<Fernet>) -> Result<Self> {
        if fernets.is_empty() {
            return Err(LailaError::value_error(
                "MultiFernet requires at least one Fernet instance",
            ));
        }
        Ok(MultiFernet { _fernets: fernets })
    }

    /// `MultiFernet.encrypt(msg)`
    pub fn encrypt_bytes(&self, msg: &[u8]) -> Vec<u8> {
        self.encrypt_bytes_at_time(msg, now())
    }

    /// `MultiFernet.encrypt_at_time(msg, current_time)` -- under the primary key.
    pub fn encrypt_bytes_at_time(&self, msg: &[u8], current_time: u64) -> Vec<u8> {
        self._fernets[0].encrypt_bytes_at_time(msg, current_time)
    }

    /// `MultiFernet.rotate(msg)` -- re-encrypts under the primary key,
    /// keeping the token's original timestamp.
    pub fn rotate_token(&self, msg: &Arg) -> Result<Vec<u8>> {
        let (timestamp, data) = Fernet::_get_unverified_token_data(msg)?;
        let mut p: Option<Vec<u8>> = None;
        for f in &self._fernets {
            match f._decrypt_data(&data, timestamp, None) {
                Ok(v) => {
                    p = Some(v);
                    break;
                }
                Err(e) if e.kind == ExcKind::InvalidToken => continue,
                Err(e) => return Err(e),
            }
        }
        let p = p.ok_or_else(invalid_token)?;
        let mut iv = [0u8; 16];
        iv.copy_from_slice(&random_bytes(16));
        Ok(self._fernets[0]._encrypt_from_parts(&p, timestamp, &iv))
    }

    /// `MultiFernet.decrypt(msg, ttl=None)`
    pub fn decrypt_token(&self, msg: &Arg, ttl: Option<u64>) -> Result<Vec<u8>> {
        for f in &self._fernets {
            match f.decrypt_token(msg, ttl) {
                Ok(v) => return Ok(v),
                Err(e) if e.kind == ExcKind::InvalidToken => continue,
                Err(e) => return Err(e),
            }
        }
        Err(invalid_token())
    }

    /// `MultiFernet.decrypt_at_time(msg, ttl, current_time)`
    pub fn decrypt_token_at_time(
        &self,
        msg: &Arg,
        ttl: Option<u64>,
        current_time: u64,
    ) -> Result<Vec<u8>> {
        for f in &self._fernets {
            match f.decrypt_token_at_time(msg, ttl, current_time) {
                Ok(v) => return Ok(v),
                Err(e) if e.kind == ExcKind::InvalidToken => continue,
                Err(e) => return Err(e),
            }
        }
        Err(invalid_token())
    }
}

/// `laila.entry.compdata.transformation.encryption.encryption.key_fingerprint(key)`:
/// `hashlib.sha256(key).hexdigest()[:12]` of the key *as given* (the
/// base64 text, not the decoded 32 bytes).
pub fn key_fingerprint(key: &[u8]) -> String {
    let digest = Sha256::digest(key);
    let mut out = String::with_capacity(12);
    for b in &digest[..6] {
        out.push_str(&alloc::format!("{b:02x}"));
    }
    out
}

// --------------------------------------------------------------------------
// Python-shaped methods (`f.encrypt(data)`, `f.decrypt(token, ttl=None)`)
// and the object protocol, so `Fernet` values work both typed and inside
// `Arg` containers (`MultiFernet([Fernet(k1), Fernet(k2)])`).
// --------------------------------------------------------------------------

fn bytes_arg(a: &Arg, what: &str) -> Result<Vec<u8>> {
    a.as_bytes_like().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "{what} must be bytes, not {}",
            a.type_name()
        ))
    })
}

fn ttl_arg(a: &Arg) -> Result<Option<u64>> {
    match a {
        Arg::None => Ok(None),
        Arg::Int(i) if *i >= 0 => Ok(Some(*i as u64)),
        Arg::Int(_) => Ok(Some(0)),
        other => Err(LailaError::type_error(alloc::format!(
            "ttl must be an int or None, not {}",
            other.type_name()
        ))),
    }
}

fn time_arg(a: &Arg) -> Result<u64> {
    match a {
        Arg::Int(i) => Ok((*i).max(0) as u64),
        Arg::Float(f) => Ok(if *f > 0.0 { *f as u64 } else { 0 }),
        other => Err(LailaError::type_error(alloc::format!(
            "current_time must be an int, not {}",
            other.type_name()
        ))),
    }
}

fn first_or_kw(args: &crate::python::Args, i: usize, name: &str) -> Arg {
    args.positional
        .get(i)
        .cloned()
        .unwrap_or_else(|| args.get(name))
}

impl Fernet {
    /// `Fernet.encrypt(data)` -> `bytes` token.
    pub fn encrypt(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let data = bytes_arg(&first_or_kw(&a, 0, "data"), "data")?;
        Ok(crate::python::object::bytes(self.encrypt_bytes(&data)))
    }
    /// `Fernet.encrypt_at_time(data, current_time)`
    pub fn encrypt_at_time(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let data = bytes_arg(&first_or_kw(&a, 0, "data"), "data")?;
        let t = time_arg(&first_or_kw(&a, 1, "current_time"))?;
        Ok(crate::python::object::bytes(
            self.encrypt_bytes_at_time(&data, t),
        ))
    }
    /// `Fernet.decrypt(token, ttl=None)` -> `bytes`.
    pub fn decrypt(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let token = first_or_kw(&a, 0, "token");
        let ttl = ttl_arg(&first_or_kw(&a, 1, "ttl"))?;
        Ok(crate::python::object::bytes(
            self.decrypt_token(&token, ttl)?,
        ))
    }
    /// `Fernet.decrypt_at_time(token, ttl, current_time)`
    pub fn decrypt_at_time(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let token = first_or_kw(&a, 0, "token");
        let ttl = ttl_arg(&first_or_kw(&a, 1, "ttl"))?;
        let t = time_arg(&first_or_kw(&a, 2, "current_time"))?;
        Ok(crate::python::object::bytes(
            self.decrypt_token_at_time(&token, ttl, t)?,
        ))
    }
    /// `Fernet.extract_timestamp(token)` -> `int`.
    pub fn extract_timestamp(&self, args: impl Into<crate::python::Args>) -> Result<i64> {
        let a: crate::python::Args = args.into();
        Ok(self.extract_timestamp_of(&first_or_kw(&a, 0, "token"))? as i64)
    }
}

impl MultiFernet {
    /// `MultiFernet.encrypt(msg)`
    pub fn encrypt(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let msg = bytes_arg(&first_or_kw(&a, 0, "msg"), "msg")?;
        Ok(crate::python::object::bytes(self.encrypt_bytes(&msg)))
    }
    /// `MultiFernet.encrypt_at_time(msg, current_time)`
    pub fn encrypt_at_time(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let msg = bytes_arg(&first_or_kw(&a, 0, "msg"), "msg")?;
        let t = time_arg(&first_or_kw(&a, 1, "current_time"))?;
        Ok(crate::python::object::bytes(
            self.encrypt_bytes_at_time(&msg, t),
        ))
    }
    /// `MultiFernet.rotate(msg)`
    pub fn rotate(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        Ok(crate::python::object::bytes(
            self.rotate_token(&first_or_kw(&a, 0, "msg"))?,
        ))
    }
    /// `MultiFernet.decrypt(msg, ttl=None)`
    pub fn decrypt(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let msg = first_or_kw(&a, 0, "msg");
        let ttl = ttl_arg(&first_or_kw(&a, 1, "ttl"))?;
        Ok(crate::python::object::bytes(self.decrypt_token(&msg, ttl)?))
    }
    /// `MultiFernet.decrypt_at_time(msg, ttl, current_time)`
    pub fn decrypt_at_time(&self, args: impl Into<crate::python::Args>) -> Result<Arg> {
        let a: crate::python::Args = args.into();
        let msg = first_or_kw(&a, 0, "msg");
        let ttl = ttl_arg(&first_or_kw(&a, 1, "ttl"))?;
        let t = time_arg(&first_or_kw(&a, 2, "current_time"))?;
        Ok(crate::python::object::bytes(
            self.decrypt_token_at_time(&msg, ttl, t)?,
        ))
    }
}

impl crate::python::object::PyObject for Fernet {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Fernet"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn crate::python::object::PyObject) -> bool {
        other.as_any().downcast_ref::<Fernet>().is_some_and(|o| {
            o._signing_key == self._signing_key && o._encryption_key == self._encryption_key
        })
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self as *const Fernet as usize >> 4) as i64)
    }
    fn __call_method__(&self, name: &str, args: crate::python::Args) -> Result<Arg> {
        match name {
            "encrypt" => self.encrypt(args),
            "encrypt_at_time" => self.encrypt_at_time(args),
            "decrypt" => self.decrypt(args),
            "decrypt_at_time" => self.decrypt_at_time(args),
            "extract_timestamp" => self.extract_timestamp(args).map(Arg::Int),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'Fernet' object has no attribute '{name}'"
            ))),
        }
    }
}

impl crate::python::IntoArg for Fernet {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

impl crate::python::builtins::IsInstance for Fernet {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Fernet>())
    }
}

impl crate::python::object::PyObject for MultiFernet {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "MultiFernet"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn crate::python::object::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<MultiFernet>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self as *const MultiFernet as usize >> 4) as i64)
    }
    fn __call_method__(&self, name: &str, args: crate::python::Args) -> Result<Arg> {
        match name {
            "encrypt" => self.encrypt(args),
            "encrypt_at_time" => self.encrypt_at_time(args),
            "rotate" => self.rotate(args),
            "decrypt" => self.decrypt(args),
            "decrypt_at_time" => self.decrypt_at_time(args),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'MultiFernet' object has no attribute '{name}'"
            ))),
        }
    }
}

impl crate::python::IntoArg for MultiFernet {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

impl crate::python::builtins::IsInstance for MultiFernet {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<MultiFernet>())
    }
}
