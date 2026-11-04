//! Hint model used to guide memory routing decisions.
//!
//! A [`MemoryHint`] is a soft, declarative knob the caller can pass
//! into a memory operation to bias the `PoolRouter`'s pool selection
//! without hard-coding a specific `pool_id`. The router is free to
//! ignore any field it doesn't understand -- hints are advisory, never
//! authoritative. (For an authoritative selection, pass `pool_id` or
//! `pool_nickname` directly to `memorize` / `remember` / `forget`.)

use alloc::string::String;
use alloc::vec::Vec;

use crate::_compat::pydantic::{FieldInfo, LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::python::{Arg, Args, Result};

fn field(ann: &str, description: &str, bounds: &[(&str, f64)]) -> FieldInfo {
    let mut extra: Vec<(String, Arg)> = bounds
        .iter()
        .map(|(k, v)| (String::from(*k), Arg::Float(*v)))
        .collect();
    extra.push((String::from("description"), Arg::Str(description.into())));
    FieldInfo {
        default: Some(Arg::None),
        default_factory: None,
        json_schema_extra: Arg::None,
        annotation: String::from(ann),
        extra,
    }
}

/// Routing hints that bias which pools are searched or written to.
///
/// All fields are optional. Each is a soft preference; the `PoolRouter`
/// may honor or ignore them based on the routing strategy currently in
/// effect.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryHint;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "MemoryHint",
    module: "laila.policy.central.memory.hint.hint",
    bases: Vec::new(),
    fields: alloc::vec![
        (
            String::from("pools"),
            field("set[str] | None", "Which pools to search first.", &[])
        ),
        (
            String::from("keywords"),
            field(
                "set[str] | None",
                "Keywords that help find the entry in memory.",
                &[]
            )
        ),
        (
            String::from("semantics"),
            field(
                "Any | None",
                "Vector semantics that help find the entry in memory.",
                &[]
            )
        ),
        (
            String::from("purpose"),
            field(
                "str | None",
                "If certain pools are uniquely used for a special purpose.",
                &[]
            )
        ),
        (
            String::from("affinity"),
            field(
                "float | None",
                "Closeness to local/nearby memory caches (0\u{2013}1).",
                &[("ge", 0.0), ("le", 1.0)]
            )
        ),
    ],
    private_attrs: Vec::new(),
    class_vars: Vec::new(),
    hooks: None,
});

impl ModelBase for MemoryHint {
    fn __class__() -> &'static ModelClass {
        &CLASS
    }
}

impl MemoryHint {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }
    /// `MemoryHint(pools=None, keywords=None, semantics=None, purpose=None, affinity=None)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args.into())
    }
}
