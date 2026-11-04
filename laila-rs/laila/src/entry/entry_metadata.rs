//! Lightweight read-only views over Entry identity and state.

use alloc::string::String;
use alloc::vec::Vec;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance, PrivateAttrInfo};
use crate::basics::definitions::identifiable_object::_LAILA_IDENTIFIABLE_OBJECT;
use crate::macros::strings::_ENTRY_SCOPE;
use crate::python::{Arg, Args, IntoArg, Result};

fn entry_scopes() -> Arg {
    alloc::vec![Arg::Str(_ENTRY_SCOPE.into())].into_arg()
}

fn private(name: &str) -> (String, PrivateAttrInfo) {
    (
        String::from(name),
        PrivateAttrInfo {
            default: Some(Arg::None),
            default_factory: None,
        },
    )
}

/// Read-only projection exposing only the identity and state of an Entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntryIdentityView;

static IDENTITY_VIEW: LazyClass = LazyClass::new(|| ModelClass {
    name: "EntryIdentityView",
    module: "laila.entry.entry_metadata",
    bases: alloc::vec![_LAILA_IDENTIFIABLE_OBJECT::__class__()],
    fields: Vec::new(),
    private_attrs: alloc::vec![private("_state")],
    class_vars: alloc::vec![(String::from("_DEFAULT_SCOPES"), entry_scopes())],
    hooks: None,
});

impl ModelBase for EntryIdentityView {
    fn __class__() -> &'static ModelClass {
        &IDENTITY_VIEW
    }
}

impl EntryIdentityView {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &IDENTITY_VIEW
    }
    /// `EntryIdentityView(uuid=None, scopes=None, evolution=None, nickname=None)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(&IDENTITY_VIEW, args.into())
    }
}

/// Read-only projection exposing identity, state, and constitution of an Entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntryHolisticView;

static HOLISTIC_VIEW: LazyClass = LazyClass::new(|| ModelClass {
    name: "EntryHolisticView",
    module: "laila.entry.entry_metadata",
    bases: alloc::vec![_LAILA_IDENTIFIABLE_OBJECT::__class__()],
    fields: Vec::new(),
    private_attrs: alloc::vec![private("_state"), private("_constitution")],
    class_vars: alloc::vec![(String::from("_DEFAULT_SCOPES"), entry_scopes())],
    hooks: None,
});

impl ModelBase for EntryHolisticView {
    fn __class__() -> &'static ModelClass {
        &HOLISTIC_VIEW
    }
}

impl EntryHolisticView {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &HOLISTIC_VIEW
    }
    /// `EntryHolisticView(uuid=None, scopes=None, evolution=None, nickname=None)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(&HOLISTIC_VIEW, args.into())
    }
}
