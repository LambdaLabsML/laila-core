//! Wire codec shared by every carrier: payload encoding (`json` /
//! `msgpack`), the 4-byte big-endian length prefix of stream transports
//! and the stream-lane frame helpers re-exported from `wire`.
//!
//! Byte-for-byte identical to `laila.policy.central.communication.protocols._carriers.codec`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use super::super::super::protocol as _json_protocol;
pub use super::super::super::wire::{
    is_reserved_frame, is_stream_frame, pack_stream_frame, unpack_stream_header, FLAG_END,
    FLAG_START, RESERVED_MARKER_MAX, SEQ_MODULUS, STREAM_HEADER_LEN, STREAM_MARKER,
};

/// `CODECS = ("json", "msgpack")`
pub const CODECS: [&str; 2] = ["json", "msgpack"];
/// `MAX_FRAME_BYTES = 256 MiB` -- the inbound frame-length sanity cap.
pub const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;
/// `_LENGTH_PREFIX.size`
pub const LENGTH_PREFIX_LEN: usize = 4;

fn unknown_codec(codec_name: &str) -> LailaError {
    LailaError::value_error(alloc::format!(
        "Unknown codec {}; expected one of ('json', 'msgpack').",
        crate::python::pyrepr::str_repr(codec_name)
    ))
}

/// `encode(obj, codec="json")` -> the payload bytes.
pub fn encode(obj: &Arg, codec_name: &str) -> Result<Vec<u8>> {
    match codec_name {
        "json" => Ok(_json_protocol::encode(obj)?.into_bytes()),
        "msgpack" => {
            let opts = crate::_codecs::msgpack::PackOptions {
                use_bin_type: true,
                default: Some(Arc::new(|a: Args| -> Result<Arg> {
                    let o = a.positional.first().cloned().unwrap_or(Arg::None);
                    _json_protocol::json_default(&o)
                })),
                ..Default::default()
            };
            crate::_codecs::msgpack::packb(obj, &opts)
        }
        other => Err(unknown_codec(other)),
    }
}

/// `decode(data, codec="json")` -> the message.
pub fn decode(data: &[u8], codec_name: &str) -> Result<Arg> {
    match codec_name {
        "json" => {
            let text = crate::python::methods::utf8_decode(data)?;
            _json_protocol::decode(&text)
        }
        "msgpack" => {
            let opts = crate::_codecs::msgpack::UnpackOptions {
                raw: false,
                ..Default::default()
            };
            crate::_codecs::msgpack::unpackb(data, &opts)
        }
        other => Err(unknown_codec(other)),
    }
}

/// `frame(payload)`: the 4-byte big-endian length prefix + payload.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + LENGTH_PREFIX_LEN);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// `encode_frame(obj, codec)`: `frame(encode(obj, codec))`.
pub fn encode_frame(obj: &Arg, codec_name: &str) -> Result<Vec<u8>> {
    Ok(frame(&encode(obj, codec_name)?))
}

/// Parse one length-prefixed frame from the head of *buf*: `Ok(Some((payload,
/// consumed)))` when complete, `Ok(None)` when more bytes are needed,
/// `ValueError` when the announced length exceeds [`MAX_FRAME_BYTES`].
pub fn split_frame(buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>> {
    if buf.len() < LENGTH_PREFIX_LEN {
        return Ok(None);
    }
    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(LailaError::value_error(alloc::format!(
            "Frame length {length} exceeds cap {MAX_FRAME_BYTES}."
        )));
    }
    if buf.len() < LENGTH_PREFIX_LEN + length {
        return Ok(None);
    }
    Ok(Some((
        buf[LENGTH_PREFIX_LEN..LENGTH_PREFIX_LEN + length].to_vec(),
        LENGTH_PREFIX_LEN + length,
    )))
}

/// `read_frame(reader)` over a blocking `std::io::Read`: `None` at a clean
/// EOF (or a truncated frame), the payload otherwise.
#[cfg(feature = "std")]
pub fn read_frame<R: std::io::Read>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut header = [0u8; LENGTH_PREFIX_LEN];
    if read_exact_or_eof(reader, &mut header)?.is_none() {
        return Ok(None);
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(LailaError::value_error(alloc::format!(
            "Frame length {length} exceeds cap {MAX_FRAME_BYTES}."
        )));
    }
    if length == 0 {
        return Ok(Some(Vec::new()));
    }
    let mut payload = alloc::vec![0u8; length];
    Ok(read_exact_or_eof(reader, &mut payload)?.map(|()| payload))
}

#[cfg(feature = "std")]
fn read_exact_or_eof<R: std::io::Read>(reader: &mut R, buf: &mut [u8]) -> Result<Option<()>> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => return Ok(None),
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(LailaError::connection_error(alloc::format!(
                    "stream read failed: {e}"
                )))
            }
        }
    }
    Ok(Some(()))
}

// ---------------------------------------------------------------------------
// module object

/// `laila.policy.central.communication.protocols._carriers.codec` as a value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct codec_mod;
pub static codec: codec_mod = codec_mod;

fn codec_name_of(name: Arg) -> String {
    if name.is_none() {
        String::from("json")
    } else {
        name.__str__()
    }
}

impl codec_mod {
    /// `codec.encode(obj, codec)` (positional parameters on purpose:
    /// `str.encode()` / `bytes.decode()` are zero-argument `Arg` methods and
    /// the macro must not see an `Args`-taking `encode`/`decode`).
    pub fn encode(&self, obj: impl IntoArg, codec_name: impl IntoArg) -> Result<Arg> {
        let obj = obj.into_arg();
        Ok(crate::python::object::bytes(encode(
            &obj,
            &codec_name_of(codec_name.into_arg()),
        )?))
    }
    /// `codec.decode(data, codec)`
    pub fn decode(&self, data: impl IntoArg, codec_name: impl IntoArg) -> Result<Arg> {
        let bytes = data.into_arg().as_bytes_like().unwrap_or_default();
        decode(&bytes, &codec_name_of(codec_name.into_arg()))
    }
    pub fn frame(&self, payload: impl IntoArg) -> Result<Arg> {
        let b = payload.into_arg().as_bytes_like().unwrap_or_default();
        Ok(crate::python::object::bytes(frame(&b)))
    }
    pub fn encode_frame(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let obj = a.positional.first().cloned().unwrap_or(Arg::None);
        let codec_arg = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("codec"));
        let codec_name = if codec_arg.is_none() {
            String::from("json")
        } else {
            codec_arg.__str__()
        };
        Ok(crate::python::object::bytes(encode_frame(
            &obj,
            &codec_name,
        )?))
    }
    /// `read_frame(reader)` for an in-memory `asyncio.StreamReader` shim:
    /// *reader* is a bytes-like buffer object exposing `read(n)`.
    pub fn read_frame(&self, reader: impl IntoArg) -> Result<Arg> {
        let reader = reader.into_arg();
        let header = reader.__call_method__("read", Args::from(LENGTH_PREFIX_LEN as i64))?;
        let header = header.as_bytes_like().unwrap_or_default();
        if header.len() < LENGTH_PREFIX_LEN {
            return Ok(Arg::None);
        }
        let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(LailaError::value_error(alloc::format!(
                "Frame length {length} exceeds cap {MAX_FRAME_BYTES}."
            )));
        }
        if length == 0 {
            return Ok(crate::python::object::bytes([]));
        }
        let body = reader.__call_method__("read", Args::from(length as i64))?;
        let body = body.as_bytes_like().unwrap_or_default();
        if body.len() < length {
            return Ok(Arg::None);
        }
        Ok(crate::python::object::bytes(body))
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "CODECS" => Arg::Tuple(Arc::from(
                CODECS
                    .iter()
                    .map(|c| Arg::Str((*c).into()))
                    .collect::<Vec<_>>(),
            )),
            "MAX_FRAME_BYTES" => (MAX_FRAME_BYTES as i64).into_arg(),
            "STREAM_MARKER" => (STREAM_MARKER as i64).into_arg(),
            "STREAM_HEADER_LEN" => (STREAM_HEADER_LEN as i64).into_arg(),
            "FLAG_START" => (FLAG_START as i64).into_arg(),
            "FLAG_END" => (FLAG_END as i64).into_arg(),
            "RESERVED_MARKER_MAX" => (RESERVED_MARKER_MAX as i64).into_arg(),
            "SEQ_MODULUS" => (SEQ_MODULUS as i64).into_arg(),
            // re-exports of the `wire` functions (the same callables)
            "is_stream_frame"
            | "is_reserved_frame"
            | "pack_stream_frame"
            | "unpack_stream_header" => crate::python::PyObject::__getattr__(
                &crate::policy::central::communication::wire::wire,
                name,
            )?,
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "module 'codec' has no attribute '{other}'"
                )))
            }
        })
    }
}
