//! Recovery-code emitter + recognizer.
//!
//! Every laila transformation stores a *Python* source snippet
//! (`backward_code`) inside the entry's `SimpleConstitution` so that the
//! payload can be rebuilt without importing the transformation class. The
//! Rust port must (a) emit the exact same strings (constitutions are compared
//! and hashed byte-for-byte across runtimes) and (b) be able to *execute*
//! those snippets -- which it does by recognising the finite set of templates
//! laila emits and dispatching to native Rust implementations.
//!
//! Templates (one per transformation, kwargs embedded via `repr`):
//! `base64`, `zlib`, `json_string`, `msgpack`, `numpy`, `pickle`, `torch`,
//! `fernet`.
//!
//! Unrecognised code raises `ValueError` -- laila-rs never evaluates
//! arbitrary Python.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pyliteral::literal_eval;
use crate::python::methods::utf8_decode;
use crate::python::pyrepr::str_repr;
use crate::python::sync::RwLock;
use crate::python::{Arg, LailaError, Result};

// --------------------------------------------------------------------------
// Emitters -- string-identical to the Python `model_post_init` bodies
// --------------------------------------------------------------------------

/// `repr(dict)` of a kwargs mapping. `None` / `Arg::None` emit `{}`.
pub fn kwargs_repr(kwargs: Option<&Arg>) -> String {
    match kwargs {
        None | Some(Arg::None) => "{}".into(),
        Some(k) => k.__repr__(),
    }
}

fn base64_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push_str("def backward(inp):\n");
    s.push_str("    import base64\n");
    s.push_str("    kwargs = ");
    s.push_str(kw);
    s.push('\n');
    s.push_str("    if isinstance(inp, memoryview):\n");
    s.push_str("        inp = inp.tobytes()\n");
    s.push_str("    return base64.b64decode(inp, **kwargs)\n");
    s
}

fn zlib_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(data):\n");
    s.push_str("    import zlib, base64\n");
    s.push_str("    if not isinstance(data, str):\n");
    s.push_str("        raise TypeError(\"Zlib.backward expects a Base64 string (str)\")\n");
    s.push_str("    compressed = base64.b64decode(data, validate=True)\n");
    s.push_str("    return zlib.decompress(compressed, **");
    s.push_str(kw);
    s.push_str(").decode(\"utf-8\")\n");
    s
}

fn json_string_tpl() -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(data):\n");
    s.push_str("    import json\n");
    s.push_str("    if not isinstance(data, str):\n");
    s.push_str("        raise TypeError(\"JsonString.backward expects a JSON string (str)\")\n");
    s.push_str("    return json.loads(data)\n");
    s
}

fn msgpack_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(inp):\n");
    s.push_str("    import msgpack\n");
    s.push_str("    kwargs = {\"raw\": False, \"strict_map_key\": False, **");
    s.push_str(kw);
    s.push_str("}\n");
    s.push_str("    return msgpack.unpackb(inp, **kwargs)\n");
    s
}

fn numpy_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(inp):\n");
    s.push_str("    import io\n");
    s.push_str("    import numpy as np\n");
    s.push_str("    buf = io.BytesIO(inp)\n");
    s.push_str("    kwargs = {'allow_pickle': False, **");
    s.push_str(kw);
    s.push_str("}\n");
    s.push_str("    return np.load(buf, **kwargs)\n");
    s
}

fn pickle_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(inp):\n");
    s.push_str("    import pickle\n");
    s.push_str("    kwargs = ");
    s.push_str(kw);
    s.push('\n');
    s.push_str("    return pickle.loads(inp, **kwargs)\n");
    s
}

fn torch_tpl(kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(inp):\n");
    s.push_str("    import io\n");
    s.push_str("    import torch\n");
    s.push_str("    buf = io.BytesIO(inp)\n");
    s.push_str("    kwargs = ");
    s.push_str(kw);
    s.push('\n');
    s.push_str("    return torch.load(buf, **kwargs)\n");
    s
}

fn fernet_tpl(fp: &str, kw: &str) -> String {
    let mut s = String::new();
    s.push('\n');
    s.push_str("def backward(inp):\n");
    s.push_str("    from cryptography.fernet import Fernet, InvalidToken\n");
    s.push_str("    from laila.entry.compdata.transformation.encryption.encryption import (\n");
    s.push_str("        resolve_encryption_key,\n");
    s.push_str("    )\n");
    s.push_str("    if not isinstance(inp, str):\n");
    s.push_str(
        "        raise TypeError(\"Encryption.backward expects a Fernet token string (str)\")\n",
    );
    s.push_str("    f = Fernet(resolve_encryption_key(expected_fingerprint=");
    s.push_str(fp);
    s.push_str("))\n");
    s.push_str("    token = inp.encode(\"utf-8\")\n");
    s.push_str("    kwargs = ");
    s.push_str(kw);
    s.push('\n');
    s.push_str("    ttl = kwargs.get(\"ttl\", None)\n");
    s.push_str("    try:\n");
    s.push_str("        out = f.decrypt(token, ttl=ttl)\n");
    s.push_str("    except InvalidToken as e:\n");
    s.push_str("        raise ValueError(\"Invalid Fernet token or TTL expired\") from e\n");
    s.push_str("    return out.decode(\"utf-8\")\n");
    s
}

/// `Base64.backward_code` for the given `backward_kwargs`.
pub fn emit_base64(backward_kwargs: Option<&Arg>) -> String {
    base64_tpl(&kwargs_repr(backward_kwargs))
}

/// `Zlib.backward_code`.
pub fn emit_zlib(backward_kwargs: Option<&Arg>) -> String {
    zlib_tpl(&kwargs_repr(backward_kwargs))
}

/// `JsonString.backward_code`.
pub fn emit_json_string() -> String {
    json_string_tpl()
}

/// `MsgpackSerializer.backward_code`.
pub fn emit_msgpack(backward_kwargs: Option<&Arg>) -> String {
    msgpack_tpl(&kwargs_repr(backward_kwargs))
}

/// `NumpySerializer.backward_code`.
pub fn emit_numpy(backward_kwargs: Option<&Arg>) -> String {
    numpy_tpl(&kwargs_repr(backward_kwargs))
}

/// `PickleSerializer.backward_code`.
pub fn emit_pickle(backward_kwargs: Option<&Arg>) -> String {
    pickle_tpl(&kwargs_repr(backward_kwargs))
}

/// `TorchSerializer.backward_code`.
pub fn emit_torch(backward_kwargs: Option<&Arg>) -> String {
    torch_tpl(&kwargs_repr(backward_kwargs))
}

/// `FernetEncryption.backward_code` (`fingerprint` is embedded via `repr`).
pub fn emit_fernet(fingerprint: &str, backward_kwargs: Option<&Arg>) -> String {
    fernet_tpl(&str_repr(fingerprint), &kwargs_repr(backward_kwargs))
}

// --------------------------------------------------------------------------
// Recognizer
// --------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Field {
    Kwargs,
    Fingerprint,
}

struct Template {
    name: &'static str,
    /// Literal segments between placeholders (`nargs + 1` entries).
    segments: Vec<String>,
    fields: &'static [Field],
}

const MARK0: &str = "\u{0}0\u{0}";
const MARK1: &str = "\u{0}1\u{0}";

fn split_marks(src: &str, marks: &[&str]) -> Vec<String> {
    let mut segments = Vec::with_capacity(marks.len() + 1);
    let mut rest = src;
    for m in marks {
        let at = rest.find(m).expect("template placeholder present");
        segments.push(rest[..at].to_string());
        rest = &rest[at + m.len()..];
    }
    segments.push(rest.to_string());
    segments
}

fn templates() -> Vec<Template> {
    alloc::vec![
        Template {
            name: "base64",
            segments: split_marks(&base64_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "zlib",
            segments: split_marks(&zlib_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "json_string",
            segments: split_marks(&json_string_tpl(), &[]),
            fields: &[],
        },
        Template {
            name: "msgpack",
            segments: split_marks(&msgpack_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "numpy",
            segments: split_marks(&numpy_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "pickle",
            segments: split_marks(&pickle_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "torch",
            segments: split_marks(&torch_tpl(MARK0), &[MARK0]),
            fields: &[Field::Kwargs],
        },
        Template {
            name: "fernet",
            segments: split_marks(&fernet_tpl(MARK0, MARK1), &[MARK0, MARK1]),
            fields: &[Field::Fingerprint, Field::Kwargs],
        },
    ]
}

/// Anchored match of `^seg0([\s\S]*?)seg1([\s\S]*?)...segN$` with the same
/// backtracking semantics as the JS `RegExp`: captures are tried shortest
/// first, longer only if the remainder fails. Shared with the transformation
/// trust gate (`_data_transformation._matches_backward_code`).
pub(crate) fn match_segments<'a>(segments: &[String], code: &'a str) -> Option<Vec<&'a str>> {
    let head = &segments[0];
    if !code.starts_with(head.as_str()) {
        return None;
    }
    let mut caps = Vec::with_capacity(segments.len() - 1);
    if go(segments, 1, code, head.len(), &mut caps) {
        Some(caps)
    } else {
        None
    }
}

fn go<'a>(
    segments: &[String],
    i: usize,
    code: &'a str,
    pos: usize,
    caps: &mut Vec<&'a str>,
) -> bool {
    if i == segments.len() {
        return pos == code.len();
    }
    let seg = segments[i].as_str();
    let last = i + 1 == segments.len();
    if last {
        // Final literal must be a suffix: capture is everything in between.
        let rest = &code[pos..];
        if rest.len() >= seg.len() && rest.ends_with(seg) {
            caps.push(&rest[..rest.len() - seg.len()]);
            return true;
        }
        return false;
    }
    let mut search_from = pos;
    while let Some(off) = code[search_from..].find(seg) {
        let at = search_from + off;
        caps.push(&code[pos..at]);
        if go(segments, i + 1, code, at + seg.len(), caps) {
            return true;
        }
        caps.pop();
        // Advance by one char to find the next (later) occurrence.
        let step = code[at..].chars().next().map_or(1, char::len_utf8);
        search_from = at + step;
        if search_from > code.len() {
            break;
        }
    }
    false
}

/// Result of [`recognize`]: which template matched and its embedded values.
#[derive(Clone, Debug)]
pub struct Recognized {
    /// Template name (`"base64"`, `"zlib"`, ... `"fernet"`).
    pub name: &'static str,
    /// `backward_kwargs` as a Python `dict` (`{}` when the template has none).
    pub kwargs: Arg,
    /// `fernet` only: the expected key fingerprint.
    pub fingerprint: Option<String>,
}

/// Recognise a recovery-code string. `Ok(None)` when no template matches;
/// `Err` only if a matched placeholder is not a Python literal.
pub fn recognize(code: &str) -> Result<Option<Recognized>> {
    for t in templates() {
        let Some(caps) = match_segments(&t.segments, code) else {
            continue;
        };
        let mut out = Recognized {
            name: t.name,
            kwargs: crate::python::builtins::dict(crate::python::Args::new())?,
            fingerprint: None,
        };
        for (f, txt) in t.fields.iter().zip(caps) {
            match f {
                Field::Kwargs => out.kwargs = literal_eval(txt)?,
                Field::Fingerprint => {
                    out.fingerprint = Some(literal_eval(txt)?.__str__());
                }
            }
        }
        return Ok(Some(out));
    }
    Ok(None)
}

// --------------------------------------------------------------------------
// Backends -- Rust implementations of each `backward`
// --------------------------------------------------------------------------

/// A compiled `backward(inp)`.
pub type Backward = Arc<dyn Fn(&Arg) -> Result<Arg> + Send + Sync>;
/// Builds a [`Backward`] from the recognised template info.
pub type BackendFactory = Arc<dyn Fn(&Recognized) -> Result<Backward> + Send + Sync>;

static BACKENDS: RwLock<Vec<(&'static str, BackendFactory)>> = RwLock::new(Vec::new());

/// Register (or override) the Rust implementation for a recognised template.
/// The encryption transformation registers the `fernet` backend (it needs
/// `laila.args` for key resolution).
pub fn register_backend(name: &'static str, factory: BackendFactory) {
    let mut g = BACKENDS.write();
    if let Some(slot) = g.iter_mut().find(|(n, _)| *n == name) {
        slot.1 = factory;
    } else {
        g.push((name, factory));
    }
}

fn backend(name: &str) -> Option<BackendFactory> {
    let g = BACKENDS.read();
    g.iter().find(|(n, _)| *n == name).map(|(_, f)| f.clone())
}

/// `kwargs` dict -> `(str key, value)` pairs (keys are `str()`-ed like the
/// JS `_kw_obj`).
fn kw_pairs(kwargs: &Arg) -> Result<Vec<(String, Arg)>> {
    let mut out = Vec::new();
    if matches!(kwargs, Arg::None) {
        return Ok(out);
    }
    for k in kwargs.__iter__()? {
        let v = kwargs.__getitem__(k.clone())?;
        out.push((k.__str__(), v));
    }
    Ok(out)
}

fn kw_get<'a>(pairs: &'a [(String, Arg)], name: &str) -> Option<&'a Arg> {
    pairs.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

fn kw_int(pairs: &[(String, Arg)], name: &str, default: i64) -> Result<i64> {
    match kw_get(pairs, name) {
        None | Some(Arg::None) => Ok(default),
        Some(v) => v.as_i64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as an integer",
                v.type_name()
            ))
        }),
    }
}

fn bytes_input(inp: &Arg, what: &str) -> Result<Vec<u8>> {
    inp.as_bytes_like().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "a bytes-like object is required, not '{}'",
            if what.is_empty() {
                inp.type_name().into()
            } else {
                what.to_string()
            }
        ))
    })
}

fn builtin_backend(info: &Recognized) -> Result<Option<Backward>> {
    let kw = kw_pairs(&info.kwargs)?;
    Ok(Some(match info.name {
        "base64" => {
            let altchars = match kw_get(&kw, "altchars") {
                None | Some(Arg::None) => None,
                Some(a) => Some(a.as_bytes_like().ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "argument should be a bytes-like object or ASCII string, not '{}'",
                        a.type_name()
                    ))
                })?),
            };
            let validate = kw_get(&kw, "validate").is_some_and(Arg::__bool__);
            Arc::new(move |inp: &Arg| {
                let out = crate::_codecs::base64::b64decode(inp, altchars.as_deref(), validate)?;
                Ok(crate::python::object::bytes(&out))
            })
        }
        "zlib" => {
            let wbits = kw_int(&kw, "wbits", 15)? as i32;
            let bufsize = kw_int(&kw, "bufsize", 16384)?;
            if bufsize < 0 {
                return Err(LailaError::value_error("bufsize must be non-negative"));
            }
            let bufsize = bufsize as usize;
            Arc::new(move |data: &Arg| {
                if !matches!(data, Arg::Str(_)) {
                    return Err(LailaError::type_error(
                        "Zlib.backward expects a Base64 string (str)",
                    ));
                }
                let compressed = crate::_codecs::base64::b64decode(data, None, true)?;
                let raw = crate::_codecs::zlib::decompress(&compressed, wbits, bufsize)?;
                Ok(Arg::Str(utf8_decode(&raw)?.into()))
            })
        }
        "json_string" => Arc::new(|data: &Arg| match data {
            Arg::Str(s) => crate::_compat::pyjson::loads(s),
            _ => Err(LailaError::type_error(
                "JsonString.backward expects a JSON string (str)",
            )),
        }),
        "msgpack" => {
            let mut opts = crate::_codecs::msgpack::UnpackOptions {
                raw: false,
                strict_map_key: false,
                ..Default::default()
            };
            if let Some(v) = kw_get(&kw, "raw") {
                opts.raw = v.__bool__();
            }
            if let Some(v) = kw_get(&kw, "strict_map_key") {
                opts.strict_map_key = v.__bool__();
            }
            if let Some(v) = kw_get(&kw, "use_list") {
                opts.use_list = v.__bool__();
            }
            Arc::new(move |inp: &Arg| {
                let b = bytes_input(inp, "")?;
                crate::_codecs::msgpack::unpackb(&b, &opts)
            })
        }
        "numpy" => {
            let _allow_pickle = kw_get(&kw, "allow_pickle").is_some_and(Arg::__bool__);
            Arc::new(move |inp: &Arg| {
                let b = bytes_input(inp, "")?;
                let arr = crate::_codecs::npy::load(&b)?;
                Ok(Arg::NDArray(Arc::new(arr)))
            })
        }
        "pickle" => Arc::new(|inp: &Arg| {
            let b = bytes_input(inp, "")?;
            crate::_codecs::pickle::loads(&b)
        }),
        "torch" => Arc::new(|_inp: &Arg| {
            Err(LailaError::not_implemented(
                "torch payloads cannot be rebuilt in laila-rs (no torch runtime)",
            ))
        }),
        _ => return Ok(None),
    }))
}

/// Rust counterpart of `laila.entry.constitution.constitution._exec_one_fn`:
/// turn a recovery-code string into a callable `backward(inp)`.
pub fn compile_backward(code: &str) -> Result<Backward> {
    let Some(info) = recognize(code)? else {
        return Err(LailaError::value_error(
            "constitution code is not a recognised laila recovery snippet; laila-rs cannot execute arbitrary Python",
        ));
    };
    if info.name == "fernet" {
        // The encryption transformation owns the key resolution; make sure
        // its backend is registered even if nothing imported it yet.
        crate::entry::compdata::transformation::encryption::encryption::ensure_backend();
    }
    if let Some(factory) = backend(info.name) {
        return factory(&info);
    }
    match builtin_backend(&info)? {
        Some(f) => Ok(f),
        None => Err(LailaError::value_error(alloc::format!(
            "no Rust backend registered for recovery code '{}'",
            info.name
        ))),
    }
}

/// Names of the recognised templates, in match order.
pub const TEMPLATE_NAMES: &[&str] = &[
    "base64",
    "zlib",
    "json_string",
    "msgpack",
    "numpy",
    "pickle",
    "torch",
    "fernet",
];
