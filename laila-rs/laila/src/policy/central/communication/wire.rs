//! Stream-lane wire format shared by `channel` and the carriers
//! (`laila.policy.central.communication.wire`).
//!
//! Frame layout (payload of one outer length-prefixed frame on stream
//! carriers, or one whole datagram on packet carriers):
//!
//! ```text
//! [0x01][lane u8][seq u32 BE][flags u8][data ...]
//! ```
//!
//! - `0x01` is [`STREAM_MARKER`]. Every first byte up to
//!   [`RESERVED_MARKER_MAX`] (`0x1F`) is reserved for binary control and is
//!   never an RPC payload (JSON starts with `{` = 0x7B, msgpack maps with
//!   0x80-0x8F / 0xDE / 0xDF). Unknown reserved markers are dropped silently
//!   by the receiver.
//! - `lane` is the *receiver's* lane id; `0` is reserved for RPC/control
//!   and never allocated.
//! - `seq` is a per-lane **chunk** counter. The receiver uses gaps to
//!   detect a lost chunk and discards the in-progress message whole.
//! - `flags`: [`FLAG_START`] marks the first chunk of a message,
//!   [`FLAG_END`] the last. Both are set on a single-chunk message
//!   (including the empty message `b""`).

use alloc::vec::Vec;

use crate::python::{LailaError, Result};

/// First payload byte of a stream-lane frame.
pub const STREAM_MARKER: u8 = 0x01;
/// Every first byte `<= RESERVED_MARKER_MAX` is a binary control marker.
pub const RESERVED_MARKER_MAX: u8 = 0x1F;
/// `STREAM_HEADER = struct.Struct(">BBIB")` -- `[marker u8][lane u8][seq u32 BE][flags u8]`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StreamHeaderStruct;
pub static STREAM_HEADER: StreamHeaderStruct = StreamHeaderStruct;
/// `STREAM_HEADER_LEN = STREAM_HEADER.size`
pub const STREAM_HEADER_LEN: usize = 7;

impl StreamHeaderStruct {
    /// `STREAM_HEADER.size`
    pub fn size(&self) -> usize {
        STREAM_HEADER_LEN
    }
    /// `STREAM_HEADER.pack(marker, lane, seq, flags)`
    pub fn pack(&self, marker: u8, lane: u8, seq: u32, flags: u8) -> [u8; STREAM_HEADER_LEN] {
        let mut h = [0u8; STREAM_HEADER_LEN];
        h[0] = marker;
        h[1] = lane;
        h[2..6].copy_from_slice(&seq.to_be_bytes());
        h[6] = flags;
        h
    }
    /// `STREAM_HEADER.unpack(raw[:STREAM_HEADER_LEN])` -> `(marker, lane, seq, flags)`.
    pub fn unpack(&self, raw: &[u8]) -> Option<(u8, u8, u32, u8)> {
        if raw.len() < STREAM_HEADER_LEN {
            return None;
        }
        Some((
            raw[0],
            raw[1],
            u32::from_be_bytes([raw[2], raw[3], raw[4], raw[5]]),
            raw[6],
        ))
    }
}
/// Last chunk of a logical message.
pub const FLAG_END: u8 = 0x01;
/// First chunk of a logical message.
pub const FLAG_START: u8 = 0x02;
/// Chunk counters wrap at 2**32.
pub const SEQ_MODULUS: u64 = 1 << 32;

/// `True` if *raw* is a stream-lane frame (first byte is the marker).
pub fn is_stream_frame(raw: &[u8]) -> bool {
    raw.first() == Some(&STREAM_MARKER)
}

/// `True` if *raw* starts with any reserved binary marker (`< 0x20`).
///
/// Such a frame must never be handed to the RPC codec; stream frames are
/// routed to the lane machinery and unknown markers are dropped.
pub fn is_reserved_frame(raw: &[u8]) -> bool {
    raw.first().is_some_and(|b| *b <= RESERVED_MARKER_MAX)
}

/// Build one stream-lane frame payload (without the outer length prefix).
pub fn pack_stream_frame(lane: u8, seq: u64, flags: u8, data: &[u8]) -> Vec<u8> {
    let seq = (seq % SEQ_MODULUS) as u32;
    let mut out = Vec::with_capacity(STREAM_HEADER_LEN + data.len());
    out.extend_from_slice(&STREAM_HEADER.pack(STREAM_MARKER, lane, seq, flags));
    out.extend_from_slice(data);
    out
}

/// Parse `(lane, seq, flags)` from a stream frame.
///
/// `ValueError` if *raw* is shorter than the header, carries the wrong
/// marker, or names the reserved lane `0`.
pub fn unpack_stream_header(raw: &[u8]) -> Result<(u8, u64, u8)> {
    if raw.len() < STREAM_HEADER_LEN {
        return Err(LailaError::value_error(alloc::format!(
            "Stream frame too short: {} < {STREAM_HEADER_LEN} bytes.",
            raw.len()
        )));
    }
    let Some((marker, lane, seq, flags)) = STREAM_HEADER.unpack(raw) else {
        unreachable!("length checked above")
    };
    if marker != STREAM_MARKER {
        return Err(LailaError::value_error(alloc::format!(
            "Not a stream frame: marker 0x{marker:02x}."
        )));
    }
    let seq = u64::from(seq);
    if lane == 0 {
        return Err(LailaError::value_error(
            "Lane 0 is reserved for RPC/control.",
        ));
    }
    Ok((lane, seq, flags))
}

/// `BackpressureError(RuntimeError)`: a peer rejected an RPC with
/// `ERR_BUSY` (see `protocols::_carriers::base`).
pub use crate::python::errors::BackpressureError;

// ---------------------------------------------------------------------------
// The module as a value

use alloc::string::String;
use alloc::sync::Arc;

use crate::python::{Arg, Args, IntoArg, PyArg, PyObject};

/// `laila.policy.central.communication.wire` as a Python module value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct wire_mod;

/// `from laila.policy.central.communication import wire`
#[allow(non_upper_case_globals)]
pub static wire: wire_mod = wire_mod;

fn bytes_of(a: Arg) -> Vec<u8> {
    a.as_bytes_like().unwrap_or_default()
}

impl wire_mod {
    /// `wire.is_stream_frame(raw)`
    pub fn is_stream_frame(&self, raw: impl IntoArg) -> bool {
        is_stream_frame(&bytes_of(raw.into_arg()))
    }
    /// `wire.is_reserved_frame(raw)`
    pub fn is_reserved_frame(&self, raw: impl IntoArg) -> bool {
        is_reserved_frame(&bytes_of(raw.into_arg()))
    }
    /// `wire.pack_stream_frame(lane, seq, flags, data)` -> bytes
    pub fn pack_stream_frame(
        &self,
        lane: impl IntoArg,
        seq: impl IntoArg,
        flags: impl IntoArg,
        data: impl IntoArg,
    ) -> Result<Arg> {
        let lane = lane.into_arg().as_i64().unwrap_or(0);
        let seq = seq.into_arg().as_i64().unwrap_or(0);
        let flags = flags.into_arg().as_i64().unwrap_or(0);
        if !(0..=255).contains(&lane) || !(0..=255).contains(&flags) {
            return Err(LailaError::value_error("Lane and flags must fit one byte."));
        }
        Ok(crate::python::object::bytes(pack_stream_frame(
            lane as u8,
            seq.rem_euclid(SEQ_MODULUS as i64) as u64,
            flags as u8,
            &bytes_of(data.into_arg()),
        )))
    }
    /// `wire.unpack_stream_header(raw)` -> `(lane, seq, flags)`
    pub fn unpack_stream_header(&self, raw: impl IntoArg) -> Result<Arg> {
        let (lane, seq, flags) = unpack_stream_header(&bytes_of(raw.into_arg()))?;
        Ok(Arg::Tuple(
            alloc::vec![
                Arg::Int(lane as i64),
                Arg::Int(seq as i64),
                Arg::Int(flags as i64)
            ]
            .into(),
        ))
    }
    /// `getattr(wire, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        PyObject::__getattr__(self, name)
    }
}

static FN_IS_STREAM_FRAME: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
    crate::python::object::callable(|args: Args| -> Result<Arg> {
        let p = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(Arg::Bool(wire.is_stream_frame(p)))
    })
});
static FN_IS_RESERVED_FRAME: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
    crate::python::object::callable(|args: Args| -> Result<Arg> {
        let p = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(Arg::Bool(wire.is_reserved_frame(p)))
    })
});
static FN_PACK_STREAM_FRAME: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
    crate::python::object::callable(|args: Args| -> Result<Arg> {
        let p = |i: usize| args.positional.get(i).cloned().unwrap_or(Arg::None);
        wire.pack_stream_frame(p(0), p(1), p(2), p(3))
    })
});
static FN_UNPACK_STREAM_HEADER: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
    crate::python::object::callable(|args: Args| -> Result<Arg> {
        let p = args.positional.first().cloned().unwrap_or(Arg::None);
        wire.unpack_stream_header(p)
    })
});

impl PyObject for wire_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 'laila.policy.central.communication.wire'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<wire_mod>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x77_69_72_65)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            // the module functions as *values* (`codec.pack_stream_frame is
            // wire.pack_stream_frame`): one callable per function
            "is_stream_frame" => FN_IS_STREAM_FRAME.clone(),
            "is_reserved_frame" => FN_IS_RESERVED_FRAME.clone(),
            "pack_stream_frame" => FN_PACK_STREAM_FRAME.clone(),
            "unpack_stream_header" => FN_UNPACK_STREAM_HEADER.clone(),
            "STREAM_MARKER" => Arg::Int(STREAM_MARKER as i64),
            "RESERVED_MARKER_MAX" => Arg::Int(RESERVED_MARKER_MAX as i64),
            "STREAM_HEADER_LEN" => Arg::Int(STREAM_HEADER_LEN as i64),
            "FLAG_END" => Arg::Int(FLAG_END as i64),
            "FLAG_START" => Arg::Int(FLAG_START as i64),
            "SEQ_MODULUS" => Arg::Int(SEQ_MODULUS as i64),
            "__name__" => Arg::Str("laila.policy.central.communication.wire".into()),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "module 'laila.policy.central.communication.wire' has no attribute '{other}'"
                )))
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let p = |i: usize| args.positional.get(i).cloned().unwrap_or(Arg::None);
        match name {
            "is_stream_frame" => Ok(Arg::Bool(self.is_stream_frame(p(0)))),
            "is_reserved_frame" => Ok(Arg::Bool(self.is_reserved_frame(p(0)))),
            "pack_stream_frame" => self.pack_stream_frame(p(0), p(1), p(2), p(3)),
            "unpack_stream_header" => self.unpack_stream_header(p(0)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'laila.policy.central.communication.wire' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for wire_mod {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for wire_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl crate::python::builtins::ModuleValue for wire_mod {
    fn module_value(self) -> Arg {
        self.into_arg()
    }
}
