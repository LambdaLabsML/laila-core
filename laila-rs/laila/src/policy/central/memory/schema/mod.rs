//! `laila.policy.central.memory.schema` -- the central-memory schema
//! objects: [`manifest::Manifest`] (the blueprint entry) and, under
//! [`base`], the central-memory class itself
//! (`_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`, the [`super::Memory`] handle).

pub mod manifest;

/// `laila.policy.central.memory.schema.base`
pub mod base {
    pub use crate::policy::central::memory::{Memory, _LAILA_IDENTIFIABLE_CENTRAL_MEMORY};
}

pub use manifest::Manifest;
