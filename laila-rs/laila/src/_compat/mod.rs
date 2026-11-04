//! Python-stdlib compatibility shims (`uuid`, `datetime`, `json`, ...).
//! Phase 0 carries only what the spike needs; Phase 1 ports the rest.

pub mod datetime;
pub mod dotmap;
pub mod fmath;
pub mod hash;
pub mod ndarray;
pub mod nprandom;
pub mod pydantic;
pub mod pyjson;
pub mod pyliteral;
pub mod random;
pub mod re_lite;
pub mod uuid;

/// `laila.get_active_namespace()` / `laila.set_active_namespace()` state.
pub mod uuid_ns {
    use alloc::string::String;

    use crate::python::sync::RwLock;
    use crate::python::{LailaError, Result};

    static ACTIVE: RwLock<Option<[u8; 16]>> = RwLock::new(None);

    /// `laila.macros.defaults.LAILA_UNIVERSAL_NAMESPACE = uuid5(NAMESPACE_DNS, "laila")`
    pub fn universal_namespace() -> [u8; 16] {
        super::uuid::parse(&super::uuid::uuid5(&super::uuid::NAMESPACE_DNS, "laila")).unwrap()
    }

    pub fn active_bytes() -> [u8; 16] {
        if let Some(ns) = *ACTIVE.read() {
            return ns;
        }
        let ns = universal_namespace();
        *ACTIVE.write() = Some(ns);
        ns
    }

    pub fn get_active_namespace() -> String {
        hyphenate(&active_bytes())
    }

    fn hyphenate(b: &[u8; 16]) -> String {
        alloc::format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        )
    }

    /// `laila.set_active_namespace(namespace_key)`: the active namespace
    /// becomes `uuid5(NAMESPACE_DNS, namespace_key)`.
    pub fn set_active_namespace(namespace_key: &str) -> Result<()> {
        let derived = super::uuid::uuid5(&super::uuid::NAMESPACE_DNS, namespace_key);
        set_active_namespace_uuid(&derived)
    }

    /// `laila._active_namespace = None`: back to the universal namespace
    /// (resolved again on the next read).
    pub fn reset_active_namespace() {
        *ACTIVE.write() = None;
    }

    /// Activate a namespace *UUID* directly (`laila._active_namespace =
    /// uuid.UUID(ns)`): what the cross-implementation vectors do to pin
    /// the namespace their cases were generated under.
    pub fn set_active_namespace_uuid(ns: &str) -> Result<()> {
        match super::uuid::parse(ns) {
            Some(b) => {
                *ACTIVE.write() = Some(b);
                Ok(())
            }
            None => Err(LailaError::value_error(alloc::format!(
                "badly formed hexadecimal UUID string: {ns}"
            ))),
        }
    }
}
