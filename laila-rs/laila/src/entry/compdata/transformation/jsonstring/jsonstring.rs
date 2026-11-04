//! JSON string serialisation / deserialisation data transformation.
//!
//! The [`JsonString`] transformation is the canonical "round-trip through
//! JSON text" step. `forward` accepts any JSON-serialisable Python object
//! and emits a compact JSON string (no whitespace, unicode-preserving);
//! `backward` parses the string back to the equivalent Python value.
//!
//! This is most useful as the final step of a pipeline whose pool backend
//! stores plain strings (filesystem JSON files, S3 JSON objects, postgres
//! TEXT columns, ...). For binary serialisation of arbitrary Python
//! objects, prefer `PickleSerializer`; for compact binary representations
//! of JSON-shaped data, prefer `MsgpackSerializer`.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_json_string;
use crate::_compat::pyjson;
use crate::python::{Arg, Args, ExcKind, LailaError, Result};

use super::super::base::{
    kw_pairs, transformation_object, unexpected_keyword, DataTransformation, Slot, TemplateSpec,
    TransformationFields,
};

/// Reversible JSON string transformation.
///
/// Forward serialises a Python object to a compact JSON string; backward
/// parses it back.
#[derive(Clone)]
pub struct JsonString {
    fields: TransformationFields,
}

impl JsonString {
    pub const NAME: &'static str = "json_string";

    /// `JsonString(**fields)`; builds the standalone backward recovery code.
    pub fn new(args: impl Into<Args>) -> Result<JsonString> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("JsonString", Self::NAME, &a)?;
        let t = JsonString { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        // Standalone recovery code mirroring `backward()` with embedded kwargs
        self.set_backward_code(&emit_json_string());
    }

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", super::super::base::_SLOT_KWARGS());
        Ok(JsonString::new(a)?.backward_code())
    }

    /// The template has no slot: an exact match (the default slot list is
    /// kept, as in Python; the sentinel never appears in the code).
    pub fn _template_slots() -> Vec<Slot> {
        super::super::base::default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "JsonString",
            probe: JsonString::_template_probe,
            slots: JsonString::_template_slots,
        }
    }
}

impl DataTransformation for JsonString {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "JsonString"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Serialize *data* to a compact JSON string.
    ///
    /// `TypeError` if *data* is not JSON-serialisable.
    fn forward(&self, data: &Arg) -> Result<Arg> {
        let mut opts = pyjson::DumpsOptions {
            separators: Some((",".to_string(), ":".to_string())),
            ensure_ascii: false,
            ..Default::default()
        };
        let mut dump = || -> Result<String> {
            for (k, v) in kw_pairs(&self.forward_kwargs())? {
                match k.as_str() {
                    "indent" => opts.indent = Some(v),
                    "sort_keys" => opts.sort_keys = v.__bool__(),
                    "ensure_ascii" => opts.ensure_ascii = v.__bool__(),
                    "allow_nan" => opts.allow_nan = v.__bool__(),
                    "separators" => {
                        let parts = v.__iter__()?;
                        if parts.len() != 2 {
                            return Err(LailaError::value_error(
                                "too many values to unpack (expected 2)",
                            ));
                        }
                        opts.separators = Some((parts[0].__str__(), parts[1].__str__()));
                    }
                    "default" => {
                        opts.default = match v {
                            Arg::None => None,
                            Arg::Callable(c) => Some(c),
                            other => {
                                return Err(LailaError::type_error(alloc::format!(
                                    "'{}' object is not callable",
                                    other.type_name()
                                )))
                            }
                        }
                    }
                    "skipkeys" | "check_circular" | "cls" => {}
                    other => return Err(unexpected_keyword("dumps", other)),
                }
            }
            pyjson::dumps(data, &opts)
        };
        match dump() {
            Ok(s) => Ok(Arg::Str(s.into())),
            Err(e)
                if e.kind.is_subclass_of(ExcKind::TypeError)
                    || e.kind.is_subclass_of(ExcKind::ValueError) =>
            {
                Err(LailaError::type_error(alloc::format!(
                    "JsonString.forward: object not JSON serializable: {}",
                    e.__str__()
                )))
            }
            Err(e) => Err(e),
        }
    }

    /// Deserialize a JSON string back to a Python object.
    ///
    /// `TypeError` if *data* is not a string or contains invalid JSON.
    fn backward(&self, data: &Arg) -> Result<Arg> {
        let Arg::Str(text) = data else {
            return Err(LailaError::type_error(
                "JsonString.backward expects a JSON string (str)",
            ));
        };
        for (k, _) in kw_pairs(&self.backward_kwargs())? {
            match k.as_str() {
                "cls" | "object_hook" | "parse_float" | "parse_int" | "parse_constant"
                | "object_pairs_hook" => {}
                other => return Err(unexpected_keyword("loads", other)),
            }
        }
        match pyjson::loads(text) {
            Ok(v) => Ok(v),
            Err(e) if e.kind == ExcKind::JSONDecodeError => {
                // ``e.msg``: the text before ``: line L column C (char N)``.
                let full = e.__str__();
                let msg = match full.rfind(": line ") {
                    Some(i) => full[..i].to_string(),
                    None => full,
                };
                Err(LailaError::type_error(alloc::format!(
                    "JsonString.backward: invalid JSON string: {msg}"
                )))
            }
            Err(e) => Err(e),
        }
    }
}

transformation_object!(JsonString);
