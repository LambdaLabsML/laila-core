//! Synthetic H.265 / HEVC Annex B elementary stream: the hand-written twin
//! of `tests/functional/policy/communication/streaming/unit_tests/_hevc.py`.
//!
//! A byte stream with the *shape* of real H.265 -- NAL unit framing,
//! start codes, parameter sets, IDR / trailing pictures, size distribution,
//! emulation-prevention-safe payloads -- so stream framing, message
//! boundaries, reassembly and parsing all get exercised without a codec.
//! The payload bits are random; nothing here decodes video.
//!
//! The module is imported as a value (`import _hevc as H` ->
//! `use laila_tests::hevc as H;`), so the generator / parser entry points
//! are methods of the [`hevc`] module value; the per-AU records are typed
//! (`au.size()`) *and* dynamic (`self.aus[0].size` through `__getattr__`)
//! like Python dataclasses.

#![allow(non_upper_case_globals, non_snake_case, clippy::new_without_default)]

use std::sync::Arc;

use laila::python::builtins::dict;
use laila::python::{Arg, Args, IntoArg, LailaError, PyObject, Result};

pub const START_CODE: &[u8] = b"\x00\x00\x00\x01";

pub const NAL_TRAIL_N: i64 = 0;
pub const NAL_TRAIL_R: i64 = 1;
pub const NAL_IDR_W_RADL: i64 = 19;
pub const NAL_VPS: i64 = 32;
pub const NAL_SPS: i64 = 33;
pub const NAL_PPS: i64 = 34;
pub const NAL_AUD: i64 = 35;
pub const NAL_SEI_PREFIX: i64 = 39;

fn is_param_type(t: i64) -> bool {
    matches!(t, NAL_VPS | NAL_SPS | NAL_PPS)
}

/// VCL NAL unit types.
fn is_vcl(t: i64) -> bool {
    (0..32).contains(&t)
}

// ----------------------------------------------------------------------
// A small deterministic PRNG standing in for `random.Random(seed)`: the
// tests only need *reproducibility* for a seed, not CPython's sequence.
// ----------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: i64) -> Self {
        let mut r = Rng((seed as u64) ^ 0x9E37_79B9_7F4A_7C15);
        r.next_u64();
        r
    }
    /// splitmix64
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// `rng.random()`: a float in `[0, 1)`.
    fn random(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// `rng.randint(a, b)`: inclusive.
    fn randint(&mut self, a: i64, b: i64) -> i64 {
        if b <= a {
            return a;
        }
        let span = (b - a + 1) as u64;
        a + (self.next_u64() % span) as i64
    }
    /// `rng.randbytes(n)`.
    fn randbytes(&mut self, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let w = self.next_u64().to_le_bytes();
            let take = (n - out.len()).min(8);
            out.extend_from_slice(&w[..take]);
        }
        out
    }
    /// `rng.lognormvariate(mu, sigma)` (Box-Muller).
    fn lognormvariate(&mut self, mu: f64, sigma: f64) -> f64 {
        let u1 = loop {
            let u = self.random();
            if u > 0.0 {
                break u;
            }
        };
        let u2 = self.random();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * core::f64::consts::PI * u2).cos();
        (mu + sigma * z).exp()
    }
}

// ----------------------------------------------------------------------
// Generator
// ----------------------------------------------------------------------

/// Two-byte HEVC NAL header: f=0, type, layer=0, tid_plus1=1.
pub fn nal_header(nal_type: i64) -> Result<[u8; 2]> {
    if !(0..64).contains(&nal_type) {
        return Err(LailaError::value_error(nal_type.to_string()));
    }
    Ok([((nal_type << 1) & 0x7E) as u8, 0x01])
}

/// `nal_unit_type` from a NAL unit (header + payload, no start code).
pub fn nal_type_of(nal: &[u8]) -> i64 {
    ((nal[0] >> 1) & 0x3F) as i64
}

/// Insert `0x03` after any `00 00` followed by `00..03` (H.265 7.4.2).
pub fn emulation_prevention(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + payload.len() / 64);
    let mut zeros = 0usize;
    for &b in payload {
        if zeros >= 2 && b <= 0x03 {
            out.push(0x03);
            zeros = 0;
        }
        out.push(b);
        if b == 0 {
            zeros += 1;
        } else {
            zeros = 0;
        }
    }
    out
}

/// Random EP-safe payload of about *n* bytes with a non-zero last byte.
fn rbsp(rng: &mut Rng, n: usize, first_slice: Option<bool>) -> Vec<u8> {
    let mut raw = rng.randbytes(n.max(1));
    if let Some(first) = first_slice {
        // first_slice_segment_in_pic_flag lives in the MSB of byte 0;
        // keep byte 0 outside 0x00..0x03 so EP insertion never touches it.
        raw[0] = if first {
            0x80 | (raw[0] & 0x7F)
        } else {
            0x04 | (raw[0] & 0x7B)
        };
    }
    if *raw.last().unwrap() == 0 {
        *raw.last_mut().unwrap() = 0x80;
    }
    let mut out = emulation_prevention(&raw);
    if *out.last().unwrap() == 0 {
        *out.last_mut().unwrap() = 0x80;
    }
    out
}

/// `start code + header + EP-safe payload` of roughly *size* bytes.
fn make_nal(
    rng: &mut Rng,
    nal_type: i64,
    size: usize,
    first_slice: Option<bool>,
) -> Result<Vec<u8>> {
    let payload = rbsp(rng, size.saturating_sub(2).max(1), first_slice);
    let mut out = Vec::with_capacity(payload.len() + 6);
    out.extend_from_slice(START_CODE);
    out.extend_from_slice(&nal_header(nal_type)?);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// One frame: an ordered list of `(nal_type, nal_bytes_with_start_code)`.
#[derive(Clone, Debug)]
pub struct AccessUnit(Arc<AccessUnitInner>);

#[derive(Debug)]
pub struct AccessUnitInner {
    pub index: i64,
    /// `"IDR" | "P" | "B"`
    pub kind: String,
    pub nals: Vec<(i64, Arc<[u8]>)>,
}

impl AccessUnit {
    pub fn index(&self) -> i64 {
        self.0.index
    }
    pub fn kind(&self) -> String {
        self.0.kind.clone()
    }
    /// The `(nal_type, bytes)` pairs as Python tuples.
    pub fn nals(&self) -> Vec<Arg> {
        self.0
            .nals
            .iter()
            .map(|(t, b)| (Arg::Int(*t), Arg::Bytes(b.clone())).into_arg())
            .collect()
    }
    pub fn data(&self) -> Arg {
        let mut out = Vec::with_capacity(self.size_usize());
        for (_, b) in &self.0.nals {
            out.extend_from_slice(b);
        }
        Arg::Bytes(out.into())
    }
    fn size_usize(&self) -> usize {
        self.0.nals.iter().map(|(_, b)| b.len()).sum()
    }
    pub fn size(&self) -> i64 {
        self.size_usize() as i64
    }
    pub fn types(&self) -> Vec<i64> {
        self.0.nals.iter().map(|(t, _)| *t).collect()
    }
    pub fn is_idr(&self) -> bool {
        self.0.kind == "IDR"
    }
}

impl PyObject for AccessUnit {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "AccessUnit"
    }
    fn __repr__(&self) -> String {
        format!(
            "AccessUnit(index={}, kind={:?}, nals=<{} nals, {} bytes>)",
            self.0.index,
            self.0.kind,
            self.0.nals.len(),
            self.size()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AccessUnit>()
            .is_some_and(|o| {
                o.0.index == self.0.index && o.0.kind == self.0.kind && o.0.nals == self.0.nals
            })
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AccessUnit'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "index" => Arg::Int(self.index()),
            "kind" => self.kind().into_arg(),
            "nals" => self.nals().into_arg(),
            "data" => self.data(),
            "size" => Arg::Int(self.size()),
            "types" => self.types().into_arg(),
            "is_idr" => Arg::Bool(self.is_idr()),
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'AccessUnit' object has no attribute '{other}'"
                )))
            }
        })
    }
}

impl IntoArg for AccessUnit {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl IntoArg for &AccessUnit {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// Summary of a generated stream the receiver must reproduce.
#[derive(Clone, Debug)]
pub struct GroundTruth {
    pub au_count: i64,
    pub idr_count: i64,
    pub nal_histogram: Arg,
    pub au_sizes: Vec<i64>,
    pub total_bytes: i64,
    pub nal_count: i64,
}

impl GroundTruth {
    pub fn au_count(&self) -> i64 {
        self.au_count
    }
    pub fn idr_count(&self) -> i64 {
        self.idr_count
    }
    pub fn nal_histogram(&self) -> Arg {
        self.nal_histogram.clone()
    }
    pub fn au_sizes(&self) -> Vec<i64> {
        self.au_sizes.clone()
    }
    pub fn total_bytes(&self) -> i64 {
        self.total_bytes
    }
    pub fn nal_count(&self) -> i64 {
        self.nal_count
    }
}

impl PyObject for GroundTruth {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "GroundTruth"
    }
    fn __repr__(&self) -> String {
        format!(
            "GroundTruth(au_count={}, idr_count={}, total_bytes={}, nal_count={})",
            self.au_count, self.idr_count, self.total_bytes, self.nal_count
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<GroundTruth>()
            .is_some_and(|o| {
                o.au_count == self.au_count
                    && o.idr_count == self.idr_count
                    && o.au_sizes == self.au_sizes
                    && o.total_bytes == self.total_bytes
                    && o.nal_count == self.nal_count
                    && o.nal_histogram == self.nal_histogram
            })
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'GroundTruth'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "au_count" => Arg::Int(self.au_count),
            "idr_count" => Arg::Int(self.idr_count),
            "nal_histogram" => self.nal_histogram.clone(),
            "au_sizes" => self.au_sizes.clone().into_arg(),
            "total_bytes" => Arg::Int(self.total_bytes),
            "nal_count" => Arg::Int(self.nal_count),
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'GroundTruth' object has no attribute '{other}'"
                )))
            }
        })
    }
}

impl IntoArg for GroundTruth {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl IntoArg for &GroundTruth {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `collections.Counter` of NAL types: a dict `{nal_type: count}`.
fn counter(types: impl IntoIterator<Item = i64>) -> Result<Arg> {
    let hist = dict(Args::new())?;
    for t in types {
        let cur = hist
            .get(Args::from((Arg::Int(t), Arg::Int(0))))?
            .as_i64()
            .unwrap_or(0);
        hist.__setitem__(Arg::Int(t), Arg::Int(cur + 1))?;
    }
    Ok(hist)
}

/// `size` of a parsed access unit by the receiver / `size` of a generated one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedAU {
    pub types: Vec<i64>,
    pub size: i64,
}

impl ParsedAU {
    pub fn types(&self) -> Vec<i64> {
        self.types.clone()
    }
    pub fn size(&self) -> i64 {
        self.size
    }
    pub fn is_idr(&self) -> bool {
        self.types.iter().any(|t| (16..=23).contains(t))
    }
}

impl PyObject for ParsedAU {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "ParsedAU"
    }
    fn __repr__(&self) -> String {
        format!("ParsedAU(types={:?}, size={})", self.types, self.size)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ParsedAU>()
            .is_some_and(|o| o == self)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'ParsedAU'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "types" => self.types().into_arg(),
            "size" => Arg::Int(self.size),
            "is_idr" => Arg::Bool(self.is_idr()),
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'ParsedAU' object has no attribute '{other}'"
                )))
            }
        })
    }
}

impl IntoArg for ParsedAU {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl IntoArg for &ParsedAU {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// A list of access units handed to the module functions: a typed `Vec`
/// (by value or reference) or a dynamic list of `AccessUnit` objects
/// (`self.aus` set in `setUpClass`).
pub trait AuList {
    fn au_list(self) -> Result<Vec<AccessUnit>>;
}

impl AuList for Vec<AccessUnit> {
    fn au_list(self) -> Result<Vec<AccessUnit>> {
        Ok(self)
    }
}

impl AuList for &Vec<AccessUnit> {
    fn au_list(self) -> Result<Vec<AccessUnit>> {
        Ok(self.clone())
    }
}

impl AuList for &[AccessUnit] {
    fn au_list(self) -> Result<Vec<AccessUnit>> {
        Ok(self.to_vec())
    }
}

impl AuList for Arg {
    fn au_list(self) -> Result<Vec<AccessUnit>> {
        (&self).au_list()
    }
}

impl AuList for &Arg {
    fn au_list(self) -> Result<Vec<AccessUnit>> {
        self.__iter__()?
            .into_iter()
            .map(|x| match &x {
                Arg::Object(o) => o
                    .as_any()
                    .downcast_ref::<AccessUnit>()
                    .cloned()
                    .ok_or_else(|| LailaError::type_error("expected an AccessUnit")),
                _ => Err(LailaError::type_error("expected an AccessUnit")),
            })
            .collect()
    }
}

/// The same for parsed access units.
pub trait ParsedList {
    fn parsed_list(self) -> Result<Vec<ParsedAU>>;
}

impl ParsedList for Vec<ParsedAU> {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        Ok(self)
    }
}

impl ParsedList for &Vec<ParsedAU> {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        Ok(self.clone())
    }
}

impl ParsedList for Vec<Arg> {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        (&self).parsed_list()
    }
}

impl ParsedList for &Vec<Arg> {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        self.iter()
            .map(|p| {
                p.downcast_ref::<ParsedAU>()
                    .cloned()
                    .ok_or_else(|| LailaError::type_error("ParsedAU expected"))
            })
            .collect()
    }
}

impl ParsedList for Arg {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        (&self).parsed_list()
    }
}

impl ParsedList for &Arg {
    fn parsed_list(self) -> Result<Vec<ParsedAU>> {
        self.__iter__()?
            .into_iter()
            .map(|x| match &x {
                Arg::Object(o) => o
                    .as_any()
                    .downcast_ref::<ParsedAU>()
                    .cloned()
                    .ok_or_else(|| LailaError::type_error("expected a ParsedAU")),
                _ => Err(LailaError::type_error("expected a ParsedAU")),
            })
            .collect()
    }
}

/// A byte stream argument: `bytes` / `bytearray` as an `Arg`, or a slice.
pub trait ByteStream {
    fn byte_stream(self) -> Result<Vec<u8>>;
}

impl ByteStream for Arg {
    fn byte_stream(self) -> Result<Vec<u8>> {
        self.as_bytes_like()
            .ok_or_else(|| LailaError::type_error("a bytes-like object is required"))
    }
}

impl ByteStream for &Arg {
    fn byte_stream(self) -> Result<Vec<u8>> {
        self.as_bytes_like()
            .ok_or_else(|| LailaError::type_error("a bytes-like object is required"))
    }
}

impl ByteStream for Vec<u8> {
    fn byte_stream(self) -> Result<Vec<u8>> {
        Ok(self)
    }
}

impl ByteStream for &Vec<u8> {
    fn byte_stream(self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
}

impl ByteStream for &[u8] {
    fn byte_stream(self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
}

impl<const N: usize> ByteStream for &[u8; N] {
    fn byte_stream(self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
}

fn lognormal_size(rng: &mut Rng, lo: i64, hi: i64, median: i64, sigma: f64) -> i64 {
    let mu = (median as f64).ln();
    let v = rng.lognormvariate(mu, sigma) as i64;
    v.clamp(lo, hi)
}

/// Generate *n_frames* access units (an IDR every *gop* frames).
pub fn generate(
    seed: i64,
    n_frames: i64,
    gop: i64,
    idr_bytes: (i64, i64),
    p_bytes: (i64, i64),
    p_median: i64,
) -> Result<Vec<AccessUnit>> {
    let mut rng = Rng::new(seed);
    let mut aus = Vec::with_capacity(n_frames.max(0) as usize);
    for i in 0..n_frames.max(0) {
        let is_idr = i % gop.max(1) == 0;
        let kind = if is_idr {
            "IDR"
        } else if rng.random() < 0.3 {
            "B"
        } else {
            "P"
        };
        let mut nals: Vec<(i64, Arc<[u8]>)> = Vec::new();
        if is_idr || rng.random() < 0.15 {
            nals.push((NAL_AUD, make_nal(&mut rng, NAL_AUD, 3, None)?.into()));
        }
        if is_idr {
            nals.push((NAL_VPS, make_nal(&mut rng, NAL_VPS, 30, None)?.into()));
            nals.push((NAL_SPS, make_nal(&mut rng, NAL_SPS, 60, None)?.into()));
            nals.push((NAL_PPS, make_nal(&mut rng, NAL_PPS, 10, None)?.into()));
        }
        if rng.random() < 0.2 {
            nals.push((
                NAL_SEI_PREFIX,
                make_nal(&mut rng, NAL_SEI_PREFIX, 100, None)?.into(),
            ));
        }
        let (total, n_slices, slice_type) = if is_idr {
            let total = rng.randint(idr_bytes.0, idr_bytes.1);
            (total, rng.randint(1, 3), NAL_IDR_W_RADL)
        } else {
            let total = lognormal_size(&mut rng, p_bytes.0, p_bytes.1, p_median, 0.6);
            let st = if kind == "B" {
                NAL_TRAIL_N
            } else {
                NAL_TRAIL_R
            };
            (total, rng.randint(1, 2), st)
        };
        let mut cuts: Vec<i64> = (0..n_slices - 1)
            .map(|_| rng.randint(1, total - 1))
            .collect();
        cuts.sort_unstable();
        let mut bounds = Vec::with_capacity(cuts.len() + 2);
        bounds.push(0);
        bounds.extend(cuts);
        bounds.push(total);
        for s in 0..n_slices as usize {
            let size = (bounds[s + 1] - bounds[s]).max(16) as usize;
            nals.push((
                slice_type,
                make_nal(&mut rng, slice_type, size, Some(s == 0))?.into(),
            ));
        }
        aus.push(AccessUnit(Arc::new(AccessUnitInner {
            index: i,
            kind: kind.to_string(),
            nals,
        })));
    }
    Ok(aus)
}

pub fn ground_truth(aus: &[AccessUnit]) -> Result<GroundTruth> {
    let hist = counter(aus.iter().flat_map(|au| au.types()))?;
    Ok(GroundTruth {
        au_count: aus.len() as i64,
        idr_count: aus.iter().filter(|au| au.is_idr()).count() as i64,
        nal_histogram: hist,
        au_sizes: aus.iter().map(|au| au.size()).collect(),
        total_bytes: aus.iter().map(|au| au.size()).sum(),
        nal_count: aus.iter().map(|au| au.0.nals.len() as i64).sum(),
    })
}

/// One message per access unit (all its NALs concatenated, Annex B).
pub fn packetize_per_au(aus: &[AccessUnit]) -> Vec<Arg> {
    aus.iter().map(|au| au.data()).collect()
}

/// One message per NAL unit (with its start code).
pub fn packetize_per_nal(aus: &[AccessUnit]) -> Vec<Arg> {
    aus.iter()
        .flat_map(|au| au.0.nals.iter().map(|(_, b)| Arg::Bytes(b.clone())))
        .collect()
}

// ----------------------------------------------------------------------
// Reference parser (independent of the generator)
// ----------------------------------------------------------------------

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > hay.len() || needle.is_empty() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Split an Annex B byte stream on `00 00 01` start codes.
///
/// Returns NAL units (header + payload) without start codes. A
/// `zero_byte` belonging to a following 4-byte start code is stripped from
/// the end of the preceding NAL (RBSP payloads never end in `0x00`).
pub fn split_nals(stream: &[u8]) -> Vec<Vec<u8>> {
    let sc: &[u8] = b"\x00\x00\x01";
    let mut nals = Vec::new();
    let n = stream.len();
    let mut idx = find(stream, sc, 0);
    while let Some(i) = idx {
        let start = i + 3;
        let nxt = find(stream, sc, start);
        let end = nxt.unwrap_or(n);
        let mut nal = &stream[start..end];
        while nal.last() == Some(&0) {
            nal = &nal[..nal.len() - 1];
        }
        if !nal.is_empty() {
            nals.push(nal.to_vec());
        }
        idx = nxt;
    }
    nals
}

fn first_slice_flag(nal: &[u8]) -> bool {
    nal.len() > 2 && (nal[2] & 0x80) != 0
}

/// Group NAL units into access units.
///
/// A new access unit starts at an AUD, at a VPS/SPS/PPS/SEI that follows a
/// VCL NAL, or at a VCL NAL with `first_slice_segment_in_pic_flag` set when
/// the previous NAL was also VCL.
pub fn group_access_units(nals: &[Vec<u8>]) -> Vec<ParsedAU> {
    let mut aus: Vec<ParsedAU> = Vec::new();
    let mut prev_vcl = false;
    for nal in nals {
        let t = nal_type_of(nal);
        let new_au = aus.is_empty()
            || t == NAL_AUD
            || ((is_param_type(t) || t == NAL_SEI_PREFIX) && prev_vcl)
            || (is_vcl(t) && prev_vcl && first_slice_flag(nal));
        if new_au {
            aus.push(ParsedAU {
                types: Vec::new(),
                size: 0,
            });
        }
        let cur = aus.last_mut().unwrap();
        cur.types.push(t);
        cur.size += nal.len() as i64 + 4; // account for the 4-byte start code
        prev_vcl = is_vcl(t);
    }
    aus
}

/// Full reference parse: NAL split + AU grouping.
pub fn parse_annexb(stream: &[u8]) -> Vec<ParsedAU> {
    group_access_units(&split_nals(stream))
}

pub fn histogram(aus: &[ParsedAU]) -> Result<Arg> {
    counter(aus.iter().flat_map(|au| au.types.iter().copied()))
}

// ----------------------------------------------------------------------
// The module value: `import _hevc as H` -> `H.generate(..)`.
// ----------------------------------------------------------------------

/// The `_hevc` module as a value.
#[derive(Clone, Copy, Debug, Default)]
pub struct HevcModule;

/// `import _hevc as H`: the module value.
pub const hevc: HevcModule = HevcModule;

impl HevcModule {
    /// `H.generate(seed=.., n_frames=.., gop=.., idr_bytes=.., p_bytes=.., p_median=..)`.
    pub fn generate(&self, args: impl Into<Args>) -> Result<Vec<AccessUnit>> {
        let mut a: Args = args.into();
        let mut pos = core::mem::take(&mut a.positional).into_iter();
        let mut take = |name: &str, default: Arg| -> Arg {
            if let Some(v) = pos.next() {
                return v;
            }
            a.pop_or(name, default)
        };
        let seed = take("seed", Arg::Int(1234));
        let n_frames = take("n_frames", Arg::Int(300));
        let gop = take("gop", Arg::Int(30));
        let pair = |v: Arg, d: (i64, i64)| -> Result<(i64, i64)> {
            if v.is_none() {
                return Ok(d);
            }
            let items = v.__iter__()?;
            match items.as_slice() {
                [lo, hi] => Ok((
                    lo.as_i64()
                        .ok_or_else(|| LailaError::type_error("int expected"))?,
                    hi.as_i64()
                        .ok_or_else(|| LailaError::type_error("int expected"))?,
                )),
                _ => Err(LailaError::value_error("expected a (lo, hi) pair")),
            }
        };
        let idr_bytes = pair(a.pop_or("idr_bytes", Arg::None), (60_000, 250_000))?;
        let p_bytes = pair(a.pop_or("p_bytes", Arg::None), (4_000, 40_000))?;
        let p_median = a.pop_or("p_median", Arg::Int(12_000));
        let int = |v: Arg| {
            v.as_i64()
                .ok_or_else(|| LailaError::type_error("int expected"))
        };
        generate(
            int(seed)?,
            int(n_frames)?,
            int(gop)?,
            idr_bytes,
            p_bytes,
            int(p_median)?,
        )
    }
    pub fn ground_truth(&self, aus: impl AuList) -> Result<GroundTruth> {
        ground_truth(&aus.au_list()?)
    }
    pub fn packetize_per_au(&self, aus: impl AuList) -> Result<Vec<Arg>> {
        Ok(packetize_per_au(&aus.au_list()?))
    }
    pub fn packetize_per_nal(&self, aus: impl AuList) -> Result<Vec<Arg>> {
        Ok(packetize_per_nal(&aus.au_list()?))
    }
    pub fn split_nals(&self, stream: impl ByteStream) -> Result<Vec<Arg>> {
        Ok(split_nals(&stream.byte_stream()?)
            .into_iter()
            .map(|n| Arg::Bytes(n.into()))
            .collect())
    }
    /// `H.parse_annexb(stream)`: a Python list of `ParsedAU`s (dynamic, so
    /// `p[0].types` / `[p.size for p in parsed]` read as in Python).
    pub fn parse_annexb(&self, stream: impl ByteStream) -> Result<Vec<Arg>> {
        Ok(parse_annexb(&stream.byte_stream()?)
            .into_iter()
            .map(IntoArg::into_arg)
            .collect())
    }
    pub fn histogram(&self, aus: impl ParsedList) -> Result<Arg> {
        histogram(&aus.parsed_list()?)
    }
    pub fn nal_header(&self, nal_type: i64) -> Result<Arg> {
        Ok(Arg::Bytes(nal_header(nal_type)?.to_vec().into()))
    }
    pub fn nal_type_of(&self, nal: impl ByteStream) -> Result<i64> {
        Ok(nal_type_of(&nal.byte_stream()?))
    }
    pub fn emulation_prevention(&self, payload: impl ByteStream) -> Result<Arg> {
        Ok(Arg::Bytes(
            emulation_prevention(&payload.byte_stream()?).into(),
        ))
    }
    pub fn START_CODE(&self) -> Arg {
        Arg::Bytes(START_CODE.into())
    }
    pub fn NAL_AUD(&self) -> i64 {
        NAL_AUD
    }
    pub fn NAL_IDR_W_RADL(&self) -> i64 {
        NAL_IDR_W_RADL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let aus = generate(1234, 61, 30, (60_000, 250_000), (4_000, 40_000), 12_000).unwrap();
        let gt = ground_truth(&aus).unwrap();
        assert_eq!(gt.au_count, 61);
        assert_eq!(gt.idr_count, 3);
        let mut stream = Vec::new();
        for m in packetize_per_au(&aus) {
            stream.extend_from_slice(&m.as_bytes_like().unwrap());
        }
        let parsed = parse_annexb(&stream);
        assert_eq!(parsed.len(), 61);
        assert_eq!(
            parsed.iter().map(|p| p.size).collect::<Vec<_>>(),
            gt.au_sizes
        );
        assert_eq!(
            parsed.iter().map(|p| p.types.clone()).collect::<Vec<_>>(),
            aus.iter().map(|a| a.types()).collect::<Vec<_>>()
        );
        assert!(histogram(&parsed).unwrap() == gt.nal_histogram);
        // no NAL body contains an unescaped 00 00 0x (x <= 3)
        for au in &aus {
            for (_, nal) in &au.0.nals {
                let body = &nal[4..];
                assert!(!body.windows(3).any(|w| w[0] == 0 && w[1] == 0 && w[2] <= 3));
                assert_ne!(*body.last().unwrap(), 0);
            }
        }
    }
}
