//! Record model -- decorates an `Entry` with provenance metadata for storage.
//!
//! A [`Record`] is what actually lands in a pool. It wraps an entry with
//! three pieces of provenance:
//!
//! - **recorder** -- the policy gid that originated the write (defaults
//!   to the active policy at construction time);
//! - **borrower** -- the policy gid that requested the entry on behalf of
//!   someone else (`None` unless a caller sets it; reserved for future
//!   attribution flows);
//! - **record_timestamp** -- ISO-8601 UTC timestamp of when the record
//!   was constructed.
//!
//! Pools never see raw entries; they always see (and persist) a
//! serialized `Record`. On read, [`Record::_build_async`] /
//! [`Record::_build_sync`] strip the envelope and re-hydrate the inner
//! entry via the registered scope-specific builder.
//!
//! Rust shape: [`Record`] is the class (a `pydantic.BaseModel` descriptor
//! plus the class methods); `Record(entry=..)` is a model instance
//! (`Arg`) whose `serialize` / `entry_id` / `as_dict` are served by the
//! class hooks.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    validation_error, FieldInfo, InitState, Kwargs, LazyClass, ModelBase, ModelClass, ModelHooks,
    ModelInstance,
};
use crate::basics::definitions::laila_object::_now_creation_timestamp;
use crate::python::builtins::IsInstance;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

struct Hooks;

impl ModelHooks for Hooks {
    /// `extra="forbid"`: a misspelled provenance kwarg (`creator=`) fails
    /// loudly instead of being silently dropped.
    fn init_head(
        &self,
        cls: &'static ModelClass,
        data: &mut Kwargs,
        _st: &mut InitState,
    ) -> Result<()> {
        let fields: Vec<String> = cls.all_fields().into_iter().map(|(n, _)| n).collect();
        for (k, v) in data.iter() {
            if !fields.iter().any(|f| f == k) {
                return Err(validation_error(
                    cls.name,
                    k,
                    "Extra inputs are not permitted",
                    "extra_forbidden",
                    v,
                ));
            }
        }
        Ok(())
    }

    /// Default the recorder to the active policy's gid when not provided.
    /// `borrower` is left exactly as given.
    fn model_post_init(&self, inst: &ModelInstance, _st: &InitState) -> Result<()> {
        if inst.field("recorder").unwrap_or(Arg::None).is_none() {
            inst.set_field(
                "recorder",
                Arg::Str(crate::policy::get_active_policy().global_id().into()),
            );
        }
        Ok(())
    }

    fn getattr(&self, inst: &ModelInstance, name: &str) -> Option<Result<Arg>> {
        match name {
            "entry_id" => Some(Record::entry_id_of(inst)),
            "as_dict" => Some(Record::as_dict_of(inst)),
            _ => None,
        }
    }

    fn call_method(&self, inst: &ModelInstance, name: &str, args: Args) -> Option<Result<Arg>> {
        match name {
            "serialize" => {
                let transformations = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("transformations"));
                Some(Record::serialize_inst(inst, transformations))
            }
            _ => None,
        }
    }

    fn property_getters(&self) -> &'static [&'static str] {
        &["entry_id", "as_dict"]
    }
    fn method_names(&self) -> &'static [&'static str] {
        &[
            "serialize",
            "from_dict",
            "_build_sync",
            "_build_async",
            "_build",
        ]
    }
}

static HOOKS: Hooks = Hooks;

fn optional_str(default: Arg) -> FieldInfo {
    FieldInfo {
        default: Some(default),
        default_factory: None,
        json_schema_extra: Arg::None,
        annotation: "str | None".to_string(),
        extra: Vec::new(),
    }
}

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "Record",
    module: "laila.policy.central.memory.record.record",
    bases: Vec::new(),
    fields: alloc::vec![
        (
            String::from("entry"),
            FieldInfo {
                default: None,
                default_factory: None,
                json_schema_extra: Arg::None,
                annotation: "Any".to_string(),
                extra: Vec::new(),
            }
        ),
        (String::from("recorder"), optional_str(Arg::None)),
        (String::from("borrower"), optional_str(Arg::None)),
        (
            String::from("record_timestamp"),
            FieldInfo {
                default: None,
                default_factory: Some(Arc::new(|_a: Args| {
                    Ok(Arg::Str(_now_creation_timestamp().into()))
                })),
                json_schema_extra: Arg::None,
                annotation: "str".to_string(),
                extra: Vec::new(),
            }
        ),
    ],
    private_attrs: Vec::new(),
    class_vars: Vec::new(),
    hooks: Some(&HOOKS),
});

/// Immutable wrapper pairing an `Entry` with recorder/borrower metadata.
///
/// Construct with `Record(entry=...)`; the recorder defaults to the active
/// policy's `global_id` and the timestamp is captured at construction
/// time. Use `serialize` to turn the record into the on-disk dict shape
/// (which embeds the entry's serialized form), and [`_build_sync`] /
/// [`_build_async`] to invert that on read.
///
/// [`_build_sync`]: Record::_build_sync
/// [`_build_async`]: Record::_build_async
#[derive(Clone, Copy, Debug, Default)]
pub struct Record;

impl ModelBase for Record {
    fn __class__() -> &'static ModelClass {
        Record::__class__()
    }
}

impl Record {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }
    /// `Record(entry=..., recorder=None, borrower=None, record_timestamp=...)`
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        ModelInstance::construct(Self::__class__(), args.into())
    }
    /// `Record.model_fields`
    pub fn model_fields() -> Arg {
        Self::__class__().model_fields()
    }

    fn entry_of(inst: &ModelInstance) -> Arg {
        inst.field("entry").unwrap_or(Arg::None)
    }

    /// `record.entry_id`: the global ID of the wrapped entry.
    fn entry_id_of(inst: &ModelInstance) -> Result<Arg> {
        let entry = Self::entry_of(inst);
        if let Ok(gid) = entry.__getattr__("global_id") {
            return Ok(gid);
        }
        if let Arg::Dict(_) = &entry {
            return entry.__getitem__("_global_id");
        }
        Err(LailaError::type_error(alloc::format!(
            "Record.entry must expose a `global_id` or be a serialized entry mapping with `_global_id`; got {}",
            entry.type_name()
        )))
    }

    /// `record.as_dict`: a plain dict representation, preserving the raw entry object.
    fn as_dict_of(inst: &ModelInstance) -> Result<Arg> {
        let data = inst.model_dump();
        // Since it model_dumps entry in a weird way
        data.__setitem__("entry", Self::entry_of(inst))?;
        Ok(data)
    }

    fn serialize_inst(inst: &ModelInstance, transformations: Arg) -> Result<Arg> {
        let record_as_dict = Self::as_dict_of(inst)?;
        let entry = Self::entry_of(inst);
        let serialized = match &entry {
            Arg::Entry(e) => {
                let mut a = Args::new();
                a.push_keyword("transformations", transformations);
                e.serialize(a)?
            }
            other => {
                let mut a = Args::new();
                a.push_keyword("transformations", transformations);
                other.__call_method__("serialize", a)?
            }
        };
        record_as_dict.__setitem__("entry", serialized)?;
        Ok(record_as_dict)
    }

    /// `record.serialize(transformations)` on a record instance (typed):
    /// the wrapped entry is run through `Entry.serialize` with the pool's
    /// *transformations* and the resulting dict replaces the `entry` slot
    /// in the record. The pool then persists this whole dict atomically.
    pub fn serialize_record(record: &Arg, transformations: Arg) -> Result<Arg> {
        match ModelInstance::downcast(record) {
            Some(inst) if inst.class.is_subclass("Record") => {
                Self::serialize_inst(inst, transformations)
            }
            _ => Err(LailaError::type_error(alloc::format!(
                "Record.serialize expects a Record, got {}",
                record.type_name()
            ))),
        }
    }

    /// `record.entry` on a record instance (typed).
    pub fn entry(record: &Arg) -> Arg {
        ModelInstance::downcast(record)
            .map(Self::entry_of)
            .unwrap_or(Arg::None)
    }

    /// Construct a Record from the dict shape produced by `as_dict`.
    ///
    /// `entry` may be a live `Entry` or its `to_dict` / serialized-dict
    /// form, in which case it is rebuilt through `Entry.from_dict`.
    /// `recorder`, `borrower` and `record_timestamp` are passed through
    /// when present. `TypeError` if *in_dict* is not a mapping;
    /// `ValueError` if it has no `entry` slot.
    pub fn from_dict(in_dict: impl IntoArg) -> Result<Arg> {
        let in_dict = in_dict.into_arg();
        if !matches!(in_dict, Arg::Dict(_)) {
            return Err(LailaError::type_error(alloc::format!(
                "Record.from_dict expects a mapping, got {}",
                in_dict.type_name()
            )));
        }
        if !in_dict.__contains__("entry")? {
            return Err(LailaError::value_error(
                "Record.from_dict requires an 'entry' slot",
            ));
        }
        let mut entry = in_dict.__getitem__("entry")?;
        if let Arg::Dict(_) = &entry {
            entry = Arg::Entry(crate::entry::Entry::from_dict(
                crate::python::builtins::dict(Args::from(entry))?,
            )?);
        }
        let mut kwargs = Args::new();
        kwargs.push_keyword("entry", entry);
        for name in ["recorder", "borrower", "record_timestamp"] {
            let v = in_dict.get(Args::from(Arg::Str(name.into())))?;
            if !v.is_none() {
                kwargs.push_keyword(name, v);
            }
        }
        Self::new(kwargs)
    }

    /// Synchronously hydrate a record dict from a JSON string or raw dict.
    ///
    /// Dispatches to the registered builder for the entry's scope via
    /// `build_by_scope` (sync path), which returns a fully hydrated
    /// `Entry`. Returns a *shallow copy* of the record dict with the
    /// serialized `entry` slot replaced by the live entry: the input is
    /// never mutated, because in-memory pools hand out their stored dict
    /// by reference and rewriting it would turn every later read into the
    /// same already-built entry object.
    pub fn _build_sync(record: impl IntoArg) -> Result<Arg> {
        let mut record = record.into_arg();
        if let Arg::Str(s) = &record {
            record = crate::_compat::pyjson::loads(s)?;
        }
        let built = crate::python::builtins::dict(Args::from(record.clone()))?;
        let mut a = Args::new();
        a.push_positional(record.__getitem__("entry")?);
        a.push_keyword("asynchronous", false);
        built.__setitem__(
            "entry",
            crate::entry::constitution::build_maps::build_by_scope(a)?,
        )?;
        Ok(built)
    }

    /// Async variant of [`_build_sync`](Record::_build_sync):
    /// `_build_async(record)` (the record as the single positional).
    pub async fn _build_async(record: impl Into<Args>) -> Result<Arg> {
        let mut record: Args = record.into();
        let mut record = record.positional.pop().unwrap_or(Arg::None);
        if let Arg::Str(s) = &record {
            record = crate::_compat::pyjson::loads(s)?;
        }
        let built = crate::python::builtins::dict(Args::from(record.clone()))?;
        let mut a = Args::new();
        a.push_positional(record.__getitem__("entry")?);
        a.push_keyword("asynchronous", true);
        let entry = crate::entry::constitution::build_maps::build_by_scope(a)?.await?;
        built.__setitem__("entry", entry)?;
        Ok(built)
    }

    /// Router: dispatch to `_build_async` or `_build_sync`.
    #[pysig("_build(record, *, asynchronous=False)")]
    pub fn _build(args: impl Into<Args>) -> Result<Arg> {
        if asynchronous.__bool__() {
            return Ok(Arg::Future(
                crate::policy::central::command::Future::from_result(
                    crate::python::stdlib::asyncio::run(Self::_build_async(record)),
                ),
            ));
        }
        Self::_build_sync(record)
    }
}

impl PyArg for Record {
    fn to_arg(&self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl IntoArg for Record {
    fn into_arg(self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl crate::python::builtins::CallValue for Record {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args)
    }
}
impl IsInstance for Record {
    fn isinstance(x: &Arg) -> bool {
        crate::_compat::pydantic::isinstance_model(x, "Record")
    }
}

/// `isinstance(x, Record)` on a dynamic value.
pub fn is_record(x: &Arg) -> bool {
    Record::isinstance(x)
}
