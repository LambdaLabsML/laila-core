//! Encryption transformation sub-package.

#[allow(clippy::module_inception)]
pub mod encryption;

pub use self::encryption::{
    get_encryption_key, key_fingerprint, resolve_encryption_key, set_encryption_key,
    FernetEncryption,
};
