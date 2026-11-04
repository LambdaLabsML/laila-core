//! `.npy` -- byte-exact `numpy.save` / `numpy.load` (`allow_pickle=False`)
//! for [`NDArray`] (`numpy.lib.format`, versions 1.0 / 2.0 / 3.0).
//!
//! ```text
//! save(arr) -> bytes                       # np.save(buf, arr, allow_pickle=False)
//! load(data) -> NDArray                    # np.load(io.BytesIO(data), allow_pickle=False)
//! write_array(arr, version=None) -> bytes  # numpy.lib.format.write_array
//! read_magic(data) -> (major, minor)
//! read_array_header(data, version) -> (shape, fortran_order, dtype, offset)
//! ```
//!
//! Writer (`_write_array_header` + `write_array`):
//!   magic `\x93NUMPY` + version + header-length (`<H` for 1.0, `<I` for
//!   2.0/3.0) + header text + spaces + `\n` such that the whole prefix is a
//!   multiple of ARRAY_ALIGN (64) bytes. Header text is the repr of a dict
//!   with sorted keys:
//!     `{'descr': '<f8', 'fortran_order': False, 'shape': (2, 3), }`
//!   followed by `GROWTH_AXIS_MAX_DIGITS - len(repr(growth axis))` spaces
//!   (growth axis = shape[-1] when fortran_order else shape[0]; none for 0-d).
//!   Data follows in C order, or F order when the array is Fortran-contiguous
//!   and not C-contiguous (so 1-d / 0-d arrays are always `fortran_order`
//!   False).
//!
//! Reader: the header is parsed with the `ast.literal_eval` subset numpy
//! needs (a dict of `'str'`, `True`/`False`, and a tuple of ints), then
//! validated with numpy's own messages.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::_compat::ndarray::{dtype_spec, normalize_dtype, NDArray};
use crate::python::pyrepr::{bytes_repr, str_repr};
use crate::python::{LailaError, Result};

/// `numpy.lib.format.MAGIC_PREFIX`.
pub const MAGIC_PREFIX: &[u8; 6] = b"\x93NUMPY";
/// `numpy.lib.format.MAGIC_LEN` (prefix + 2 version bytes).
pub const MAGIC_LEN: usize = MAGIC_PREFIX.len() + 2;
/// `numpy.lib.format.ARRAY_ALIGN`.
pub const ARRAY_ALIGN: usize = 64;
const GROWTH_AXIS_MAX_DIGITS: usize = 21;
const MAX_HEADER_SIZE: usize = 10000;

/// Header-length field size and text encoding per format version.
fn header_size_info(version: (u8, u8)) -> Option<(usize, bool)> {
    // (struct size, utf8?)
    match version {
        (1, 0) => Some((2, false)),
        (2, 0) => Some((4, false)),
        (3, 0) => Some((4, true)),
        _ => None,
    }
}

/// Python's `repr(tuple_of_ints)`: `()`, `(5,)`, `(2, 3)`.
fn py_tuple_repr(shape: &[usize]) -> String {
    match shape.len() {
        0 => "()".into(),
        1 => alloc::format!("({},)", shape[0]),
        _ => alloc::format!(
            "({})",
            shape
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn version_repr(version: (u8, u8)) -> String {
    alloc::format!("({}, {})", version.0, version.1)
}

/// `numpy.lib.format.magic(major, minor)`.
pub fn magic(major: i64, minor: i64) -> Result<Vec<u8>> {
    if !(0..256).contains(&major) {
        return Err(LailaError::value_error(
            "major version must be 0 <= major < 256",
        ));
    }
    if !(0..256).contains(&minor) {
        return Err(LailaError::value_error(
            "minor version must be 0 <= minor < 256",
        ));
    }
    let mut out = MAGIC_PREFIX.to_vec();
    out.push(major as u8);
    out.push(minor as u8);
    Ok(out)
}

/// `numpy.lib.format._check_version`.
fn check_version(version: (u8, u8)) -> Result<(usize, bool)> {
    header_size_info(version).ok_or_else(|| {
        LailaError::value_error(alloc::format!(
            "we only support format version (1,0), (2,0), and (3,0), not {}",
            version_repr(version)
        ))
    })
}

/// Outcome of `_wrap_header` for one version (the Python code distinguishes
/// `ValueError` (too big for 1.0) from `UnicodeEncodeError` (not latin-1)).
enum WrapError {
    TooBig(LailaError),
    NotLatin1,
}

/// `numpy.lib.format._wrap_header(header, version)`.
fn wrap_header(header: &str, version: (u8, u8)) -> core::result::Result<Vec<u8>, WrapError> {
    let (hsize, utf8) = header_size_info(version).expect("known version");
    let hbytes: Vec<u8> = if utf8 {
        header.as_bytes().to_vec()
    } else {
        let mut v = Vec::with_capacity(header.len());
        for ch in header.chars() {
            let c = ch as u32;
            if c > 0xff {
                return Err(WrapError::NotLatin1);
            }
            v.push(c as u8);
        }
        v
    };
    let hlen = hbytes.len() + 1;
    let padlen = ARRAY_ALIGN - ((MAGIC_LEN + hsize + hlen) % ARRAY_ALIGN);
    let total = hlen + padlen;
    if hsize == 2 && total > 0xffff {
        return Err(WrapError::TooBig(LailaError::value_error(alloc::format!(
            "Header length {hlen} too big for version={}",
            version_repr(version)
        ))));
    }
    let mut out = magic(version.0 as i64, version.1 as i64).expect("u8 versions");
    if hsize == 2 {
        out.extend_from_slice(&(total as u16).to_le_bytes());
    } else {
        out.extend_from_slice(&(total as u32).to_le_bytes());
    }
    out.extend_from_slice(&hbytes);
    out.resize(out.len() + padlen, b' ');
    out.push(b'\n');
    Ok(out)
}

/// `numpy.lib.format._wrap_header_guess_version(header)`.
fn wrap_header_guess_version(header: &str) -> Result<Vec<u8>> {
    match wrap_header(header, (1, 0)) {
        Ok(v) => return Ok(v),
        Err(WrapError::TooBig(_)) => {}
        Err(WrapError::NotLatin1) => return Err(latin1_error(header)),
    }
    match wrap_header(header, (2, 0)) {
        Ok(v) => return Ok(v),
        Err(WrapError::TooBig(e)) => return Err(e),
        Err(WrapError::NotLatin1) => {}
    }
    wrap_header(header, (3, 0)).map_err(|e| match e {
        WrapError::TooBig(e) => e,
        WrapError::NotLatin1 => latin1_error(header),
    })
}

fn latin1_error(header: &str) -> LailaError {
    let (pos, ch) = header
        .char_indices()
        .find(|(_, c)| *c as u32 > 0xff)
        .map(|(i, c)| (header[..i].chars().count(), c))
        .unwrap_or((0, '\0'));
    LailaError::new(
        crate::python::errors::ExcKind::UnicodeEncodeError,
        alloc::format!(
            "'latin-1' codec can't encode character '\\u{:04x}' in position {pos}: ordinal not in range(256)",
            ch as u32
        ),
    )
}

/// `numpy.lib.format.header_data_from_array_1_0(array)` ->
/// `(shape, fortran_order, descr)`.
pub fn header_data_from_array_1_0(array: &NDArray) -> (Vec<usize>, bool, String) {
    // 0-d and 1-d arrays are both C- and F-contiguous -> C wins.
    let fortran_order = array.fortran_order && array.shape.len() > 1;
    (array.shape.clone(), fortran_order, array.dtype.clone())
}

/// The header dict's repr plus the growth-axis reserve, as
/// `_write_array_header` builds it.
fn header_text(shape: &[usize], fortran_order: bool, descr: &str) -> String {
    // repr of a dict with sorted keys: descr, fortran_order, shape
    let mut header = alloc::format!(
        "{{'descr': {}, 'fortran_order': {}, 'shape': {}, }}",
        str_repr(descr),
        if fortran_order { "True" } else { "False" },
        py_tuple_repr(shape)
    );
    if !shape.is_empty() {
        let growth = if fortran_order {
            shape[shape.len() - 1]
        } else {
            shape[0]
        };
        let digits = growth.to_string().len();
        header.push_str(&" ".repeat(GROWTH_AXIS_MAX_DIGITS.saturating_sub(digits)));
    }
    header
}

/// `numpy.lib.format.write_array(fp, array, version=None, allow_pickle=False)`
/// into a byte vector.
pub fn write_array(array: &NDArray, version: Option<(u8, u8)>) -> Result<Vec<u8>> {
    let (shape, fortran_order, descr) = header_data_from_array_1_0(array);
    let header = header_text(&shape, fortran_order, &descr);
    let mut out = match version {
        None => wrap_header_guess_version(&header)?,
        Some(v) => {
            check_version(v)?;
            wrap_header(&header, v).map_err(|e| match e {
                WrapError::TooBig(e) => e,
                WrapError::NotLatin1 => latin1_error(&header),
            })?
        }
    };
    // data: F order only when fortran_order; NDArray stores data in its own
    // declared layout, so re-layout if the header disagrees.
    if fortran_order == array.fortran_order {
        out.extend_from_slice(array.raw_bytes());
    } else {
        out.extend_from_slice(array.to_c_order().raw_bytes());
    }
    Ok(out)
}

/// `np.save(buf, arr, allow_pickle=False)` -> bytes.
pub fn save(arr: &NDArray) -> Result<Vec<u8>> {
    write_array(arr, None)
}

/// `numpy.lib.format.read_magic(fp)` -> `(major, minor)`.
pub fn read_magic(buf: &[u8]) -> Result<(u8, u8)> {
    if buf.len() < MAGIC_LEN {
        return Err(LailaError::value_error(alloc::format!(
            "EOF: reading magic string, expected {MAGIC_LEN} bytes got {}",
            buf.len()
        )));
    }
    if &buf[..6] != MAGIC_PREFIX {
        return Err(LailaError::value_error(alloc::format!(
            "the magic string is not correct; expected {}, got {}",
            bytes_repr(MAGIC_PREFIX),
            bytes_repr(&buf[..6])
        )));
    }
    Ok((buf[6], buf[7]))
}

/// `numpy.lib.format.descr_to_dtype(descr)` (simple, non-structured descrs
/// only): the canonical dtype string.
pub fn descr_to_dtype(descr: &str) -> Result<String> {
    normalize_dtype(Some(descr)).map_err(|_| {
        LailaError::value_error(alloc::format!(
            "descr is not a valid dtype descriptor: {}",
            str_repr(descr)
        ))
    })
}

// --------------------------------------------------------------------------
// header literal parsing (the `ast.literal_eval` subset numpy headers use)
// --------------------------------------------------------------------------

/// A parsed header value.
#[derive(Clone, Debug, PartialEq)]
enum Lit {
    Str(String),
    Bool(bool),
    None,
    Int(i128),
    Tuple(Vec<Lit>),
}

impl Lit {
    /// Python's `repr`.
    fn repr(&self) -> String {
        match self {
            Lit::Str(s) => str_repr(s),
            Lit::Bool(true) => "True".into(),
            Lit::Bool(false) => "False".into(),
            Lit::None => "None".into(),
            Lit::Int(i) => i.to_string(),
            Lit::Tuple(items) => match items.len() {
                0 => "()".into(),
                1 => alloc::format!("({},)", items[0].repr()),
                _ => alloc::format!(
                    "({})",
                    items.iter().map(Lit::repr).collect::<Vec<_>>().join(", ")
                ),
            },
        }
    }
}

struct LitParser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> LitParser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.s.len() && matches!(self.s[self.pos], b' ' | b'\t' | b'\n' | b'\r') {
            self.pos += 1;
        }
    }
    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.s.get(self.pos).copied()
    }
    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, c: u8) -> Option<()> {
        self.eat(c).then_some(())
    }

    /// `'...'` / `"..."` with Python's simple escapes (adjacent literals concatenate).
    fn string(&mut self) -> Option<String> {
        let mut out = String::new();
        while let Some(quote @ (b'\'' | b'"')) = self.peek() {
            self.pos += 1;
            let mut piece: Vec<u8> = Vec::new();
            loop {
                let c = *self.s.get(self.pos)?;
                self.pos += 1;
                if c == quote {
                    break;
                }
                if c == b'\\' {
                    let e = *self.s.get(self.pos)?;
                    self.pos += 1;
                    match e {
                        b'n' => piece.push(b'\n'),
                        b't' => piece.push(b'\t'),
                        b'r' => piece.push(b'\r'),
                        b'0' => piece.push(0),
                        b'\\' | b'\'' | b'"' => piece.push(e),
                        b'x' => {
                            let h = self.s.get(self.pos..self.pos + 2)?;
                            self.pos += 2;
                            let v = u8::from_str_radix(core::str::from_utf8(h).ok()?, 16).ok()?;
                            let ch = char::from_u32(v as u32)?;
                            let mut b = [0u8; 4];
                            piece.extend_from_slice(ch.encode_utf8(&mut b).as_bytes());
                        }
                        _ => return None,
                    }
                    continue;
                }
                piece.push(c);
            }
            out.push_str(core::str::from_utf8(&piece).ok()?);
        }
        Some(out)
    }

    fn word(&mut self) -> &'a str {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.s.len()
            && (self.s[self.pos].is_ascii_alphanumeric() || self.s[self.pos] == b'_')
        {
            self.pos += 1;
        }
        core::str::from_utf8(&self.s[start..self.pos]).unwrap_or("")
    }

    fn value(&mut self) -> Option<Lit> {
        match self.peek()? {
            b'\'' | b'"' => self.string().map(Lit::Str),
            b'(' => {
                self.pos += 1;
                let mut items = Vec::new();
                let mut trailing_comma = false;
                while !self.eat(b')') {
                    items.push(self.value()?);
                    trailing_comma = self.eat(b',');
                    if !trailing_comma {
                        self.expect(b')')?;
                        break;
                    }
                }
                // `(1)` is the int 1, not a tuple
                if items.len() == 1 && !trailing_comma {
                    return items.pop();
                }
                Some(Lit::Tuple(items))
            }
            b'-' | b'+' | b'0'..=b'9' => {
                let start = self.pos;
                self.pos += 1;
                while self.pos < self.s.len() && self.s[self.pos].is_ascii_digit() {
                    self.pos += 1;
                }
                let text = core::str::from_utf8(&self.s[start..self.pos]).ok()?;
                let v = text.parse::<i128>().ok()?;
                // Python 2 `long` suffix (numpy's `_filter_header` strips it)
                if self.s.get(self.pos) == Some(&b'L') {
                    self.pos += 1;
                }
                Some(Lit::Int(v))
            }
            _ => match self.word() {
                "True" => Some(Lit::Bool(true)),
                "False" => Some(Lit::Bool(false)),
                "None" => Some(Lit::None),
                _ => None,
            },
        }
    }

    /// `{'key': value, ...}` with string keys (insertion order kept).
    fn dict(&mut self) -> Option<Vec<(String, Lit)>> {
        self.expect(b'{')?;
        let mut out = Vec::new();
        while !self.eat(b'}') {
            let key = match self.peek()? {
                b'\'' | b'"' => self.string()?,
                _ => return None,
            };
            self.expect(b':')?;
            let v = self.value()?;
            out.push((key, v));
            if !self.eat(b',') {
                self.expect(b'}')?;
                break;
            }
        }
        self.skip_ws();
        (self.pos == self.s.len()).then_some(out)
    }
}

/// `ast.literal_eval(header)` for the dict shape numpy writes, then numpy's
/// key/type validation -> `(shape, fortran_order, descr)`.
fn parse_header(header: &str) -> Result<(Vec<usize>, bool, String)> {
    let mut p = LitParser {
        s: header.as_bytes(),
        pos: 0,
    };
    let d = p.dict().ok_or_else(|| {
        LailaError::value_error(alloc::format!("Cannot parse header: {}", str_repr(header)))
    })?;
    let mut keys: Vec<&str> = d.iter().map(|(k, _)| k.as_str()).collect();
    keys.sort_unstable();
    if keys != ["descr", "fortran_order", "shape"] {
        // numpy: `sorted(d.keys())`
        let shown: Vec<String> = keys.iter().map(|k| str_repr(k)).collect();
        return Err(LailaError::value_error(alloc::format!(
            "Header does not contain the correct keys: [{}]",
            shown.join(", ")
        )));
    }
    let get = |name: &str| {
        d.iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .expect("key checked")
    };
    let shape_lit = get("shape");
    let shape: Vec<usize> = match &shape_lit {
        Lit::Tuple(items) if items.iter().all(|x| matches!(x, Lit::Int(i) if *i >= 0)) => items
            .iter()
            .map(|x| match x {
                Lit::Int(i) => usize::try_from(*i).ok(),
                _ => None,
            })
            .collect::<Option<Vec<usize>>>()
            .ok_or_else(|| {
                LailaError::value_error(alloc::format!("shape is not valid: {}", shape_lit.repr()))
            })?,
        other => {
            return Err(LailaError::value_error(alloc::format!(
                "shape is not valid: {}",
                other.repr()
            )));
        }
    };
    let fortran_order = match get("fortran_order") {
        Lit::Bool(b) => b,
        other => {
            return Err(LailaError::value_error(alloc::format!(
                "fortran_order is not a valid bool: {}",
                other.repr()
            )));
        }
    };
    let descr = match get("descr") {
        Lit::Str(s) => s,
        other => {
            return Err(LailaError::value_error(alloc::format!(
                "descr is not a valid dtype descriptor: {}",
                other.repr()
            )));
        }
    };
    Ok((shape, fortran_order, descr))
}

/// `numpy.lib.format.read_array_header_1_0 / 2_0` on the whole buffer ->
/// `(shape, fortran_order, dtype, data offset)`.
pub fn read_array_header(
    buf: &[u8],
    version: (u8, u8),
) -> Result<(Vec<usize>, bool, String, usize)> {
    let (shape, fortran_order, descr, offset) = read_header_raw(buf, version, MAX_HEADER_SIZE)?;
    let dtype = descr_to_dtype(&descr)?;
    Ok((shape, fortran_order, dtype, offset))
}

fn read_header_raw(
    buf: &[u8],
    version: (u8, u8),
    max_header_size: usize,
) -> Result<(Vec<usize>, bool, String, usize)> {
    let (hsize, utf8) = check_version(version)?;
    let mut pos = MAGIC_LEN;
    if buf.len() < pos + hsize {
        return Err(LailaError::value_error(alloc::format!(
            "EOF: reading array header length, expected {hsize} bytes got {}",
            buf.len().saturating_sub(pos)
        )));
    }
    let hlen = if hsize == 2 {
        u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize
    } else {
        u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize
    };
    pos += hsize;
    if buf.len() < pos + hlen {
        return Err(LailaError::value_error(alloc::format!(
            "EOF: reading array header, expected {hlen} bytes got {}",
            buf.len() - pos
        )));
    }
    let raw = &buf[pos..pos + hlen];
    pos += hlen;
    let header: String = if utf8 {
        core::str::from_utf8(raw)
            .map_err(|e| {
                LailaError::new(
                    crate::python::errors::ExcKind::UnicodeDecodeError,
                    alloc::format!(
                        "'utf-8' codec can't decode byte 0x{:02x} in position {}: invalid start byte",
                        raw.get(e.valid_up_to()).copied().unwrap_or(0),
                        e.valid_up_to()
                    ),
                )
            })?
            .into()
    } else {
        raw.iter().map(|b| *b as char).collect()
    };
    if header.chars().count() > max_header_size {
        return Err(LailaError::value_error(alloc::format!(
            "Header info length ({}) is large and may not be safe to load securely.\n\
             To allow loading, adjust `max_header_size` or fully trust the `.npy` file using `allow_pickle=True`.\n\
             For safety against large resource use or crashes, sandboxing may be necessary.",
            header.chars().count()
        )));
    }
    let (shape, fortran_order, descr) = parse_header(&header)?;
    Ok((shape, fortran_order, descr, pos))
}

/// `np.load(io.BytesIO(data), allow_pickle=False)` -> [`NDArray`].
pub fn load(data: &[u8]) -> Result<NDArray> {
    load_with(data, MAX_HEADER_SIZE)
}

/// `np.load(io.BytesIO(data), allow_pickle=False, max_header_size=..)`.
pub fn load_with(data: &[u8], max_header_size: usize) -> Result<NDArray> {
    let version = read_magic(data)?;
    let (shape, fortran_order, descr, offset) = read_header_raw(data, version, max_header_size)?;
    // object arrays need pickle, which `allow_pickle=False` forbids
    if descr
        .trim_start_matches(['<', '>', '|', '='])
        .starts_with('O')
    {
        return Err(LailaError::value_error(
            "Object arrays cannot be loaded when allow_pickle=False",
        ));
    }
    let dtype = descr_to_dtype(&descr)?;
    let spec = dtype_spec(&dtype)?;
    let count: usize = shape.iter().product();
    let nbytes = count * spec.size;
    if data.len() - offset < nbytes {
        return Err(LailaError::value_error(alloc::format!(
            "EOF: reading array data, expected {nbytes} bytes got {}",
            data.len() - offset
        )));
    }
    // raw bytes stay in the descr's byte order; NDArray keeps dtype.str intact
    NDArray::from_bytes(
        &dtype,
        shape,
        fortran_order,
        data[offset..offset + nbytes].to_vec(),
    )
}

/// `numpy.lib.format.read_array` is `load` for in-memory data.
pub fn read_array(data: &[u8]) -> Result<NDArray> {
    load(data)
}
