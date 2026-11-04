//! `import pickle`, `import msgpack`, `import base64`, `import binascii`,
//! `import zlib`, `import io` -- Python-spelled facades over `crate::_codecs`
//! so transpiled code keeps `pickle.dumps(obj)`, `msgpack.unpackb(b, raw=False)`,
//! `io.BytesIO(data)` verbatim.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use laila_macros::pysig;

use super::super::args::Args;
use super::super::errors::{ExcKind, LailaError, Result};
use super::super::object::{bytes, Arg, IntoArg, PyObject};
use super::super::sync::RwLock;
use crate::_codecs;

fn bytes_like(a: &Arg, fname: &str) -> Result<Vec<u8>> {
    a.as_bytes_like().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "{fname}: a bytes-like object is required, not '{}'",
            a.type_name()
        ))
    })
}

fn int_arg(a: &Arg, what: &str) -> Result<i64> {
    match a {
        Arg::Int(i) => Ok(*i),
        Arg::Bool(b) => Ok(i64::from(*b)),
        other => Err(LailaError::type_error(alloc::format!(
            "'{}' object cannot be interpreted as an integer{}",
            other.type_name(),
            what
        ))),
    }
}

// --------------------------------------------------------------------------
// io
// --------------------------------------------------------------------------

#[derive(Debug, Default)]
struct BytesIOState {
    buf: Vec<u8>,
    pos: usize,
    closed: bool,
}

/// `io.BytesIO(initial_bytes=b"")` -- an in-memory binary stream. Cloning
/// shares the buffer (Python reference semantics).
#[derive(Clone, Debug, Default)]
pub struct BytesIO(Arc<RwLock<BytesIOState>>);

impl BytesIO {
    pub fn new(initial: &[u8]) -> Self {
        BytesIO(Arc::new(RwLock::new(BytesIOState {
            buf: initial.to_vec(),
            pos: 0,
            closed: false,
        })))
    }
    fn check_open(&self) -> Result<()> {
        if self.0.read().closed {
            return Err(LailaError::value_error("I/O operation on closed file."));
        }
        Ok(())
    }
    /// `BytesIO.getvalue()` -> `bytes` of the whole buffer.
    pub fn getvalue(&self) -> Result<Arg> {
        self.check_open()?;
        Ok(bytes(&self.0.read().buf))
    }
    /// `BytesIO.getbuffer()` (returns `bytes` in place of a `memoryview`).
    pub fn getbuffer(&self) -> Result<Arg> {
        self.getvalue()
    }
    /// `BytesIO.read(size=-1)`
    pub fn read(&self, args: impl Into<Args>) -> Result<Arg> {
        self.check_open()?;
        let a: Args = args.into();
        let size = match a.positional.first() {
            None | Some(Arg::None) => -1,
            Some(s) => int_arg(s, "")?,
        };
        let mut st = self.0.write();
        let start = st.pos.min(st.buf.len());
        let end = if size < 0 {
            st.buf.len()
        } else {
            (start + size as usize).min(st.buf.len())
        };
        st.pos = end;
        Ok(bytes(&st.buf[start..end]))
    }
    /// `BytesIO.write(b)` -> number of bytes written.
    pub fn write(&self, b: impl IntoArg) -> Result<i64> {
        self.check_open()?;
        let b = b.into_arg();
        let data = bytes_like(&b, "write")?;
        let mut st = self.0.write();
        let pos = st.pos;
        let end = pos + data.len();
        if st.buf.len() < pos {
            st.buf.resize(pos, 0);
        }
        if st.buf.len() < end {
            st.buf.resize(end, 0);
        }
        st.buf[pos..end].copy_from_slice(&data);
        st.pos = end;
        Ok(data.len() as i64)
    }
    /// `BytesIO.seek(pos, whence=0)`
    pub fn seek(&self, args: impl Into<Args>) -> Result<i64> {
        self.check_open()?;
        let a: Args = args.into();
        let off = int_arg(a.positional.first().unwrap_or(&Arg::None), "")?;
        let whence = match a.positional.get(1) {
            Some(w) => int_arg(w, "")?,
            None => int_arg(&a.get("whence"), "").unwrap_or(0),
        };
        let mut st = self.0.write();
        let base: i64 = match whence {
            0 => {
                if off < 0 {
                    return Err(LailaError::value_error(alloc::format!(
                        "negative seek value {off}"
                    )));
                }
                0
            }
            1 => st.pos as i64,
            2 => st.buf.len() as i64,
            w => {
                return Err(LailaError::value_error(alloc::format!(
                    "invalid whence ({w}, should be 0, 1 or 2)"
                )))
            }
        };
        st.pos = (base + off).max(0) as usize;
        Ok(st.pos as i64)
    }
    /// `BytesIO.tell()`
    pub fn tell(&self) -> Result<i64> {
        self.check_open()?;
        Ok(self.0.read().pos as i64)
    }
    /// `BytesIO.truncate(size=None)`
    pub fn truncate(&self, args: impl Into<Args>) -> Result<i64> {
        self.check_open()?;
        let a: Args = args.into();
        let mut st = self.0.write();
        let size = match a.positional.first() {
            None | Some(Arg::None) => st.pos,
            Some(s) => int_arg(s, "")?.max(0) as usize,
        };
        st.buf.truncate(size);
        Ok(size as i64)
    }
    /// `BytesIO.close()`
    pub fn close(&self) {
        self.0.write().closed = true;
    }
    /// `BytesIO.closed`
    pub fn closed(&self) -> bool {
        self.0.read().closed
    }
    pub fn readable(&self) -> bool {
        true
    }
    pub fn writable(&self) -> bool {
        true
    }
    pub fn seekable(&self) -> bool {
        true
    }
    pub fn flush(&self) -> Result<()> {
        self.check_open()
    }
    /// `with io.BytesIO() as buf:` support.
    pub fn __enter__(&self) -> BytesIO {
        self.clone()
    }
    pub fn __exit__(&self) {
        self.close();
    }
}

impl PyObject for BytesIO {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "BytesIO"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<_io.BytesIO object at {:#x}>",
            Arc::as_ptr(&self.0) as *const () as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<BytesIO>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((Arc::as_ptr(&self.0) as *const () as usize >> 4) as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "getvalue" => self.getvalue(),
            "getbuffer" => self.getbuffer(),
            "read" => self.read(args),
            "write" => self
                .write(args.positional.into_iter().next().unwrap_or(Arg::None))
                .map(Arg::Int),
            "seek" => self.seek(args).map(Arg::Int),
            "tell" => self.tell().map(Arg::Int),
            "truncate" => self.truncate(args).map(Arg::Int),
            "close" => {
                self.close();
                Ok(Arg::None)
            }
            "flush" => self.flush().map(|()| Arg::None),
            _ => Err(LailaError::new(
                ExcKind::AttributeError,
                alloc::format!("'_io.BytesIO' object has no attribute '{name}'"),
            )),
        }
    }
}

impl IntoArg for BytesIO {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl crate::python::PyArg for BytesIO {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

impl crate::python::builtins::IsInstance for BytesIO {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<BytesIO>())
    }
}

/// Bytes of a `bytes`-like *or* a `BytesIO` (what `np.load` / `pickle.load`
/// accept as "file").
pub fn file_bytes(a: &Arg) -> Result<Vec<u8>> {
    if let Arg::Object(o) = a {
        if let Some(b) = o.as_any().downcast_ref::<BytesIO>() {
            let st = b.0.read();
            let start = st.pos.min(st.buf.len());
            return Ok(st.buf[start..].to_vec());
        }
    }
    bytes_like(a, "file")
}

/// `import io`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_mod;
#[allow(non_upper_case_globals)]
pub static io: io_mod = io_mod;
impl io_mod {
    /// `io.BytesIO(initial_bytes=b"")`
    pub fn BytesIO(&self, args: impl Into<Args>) -> Result<BytesIO> {
        let a: Args = args.into();
        match a.positional.first() {
            None | Some(Arg::None) => Ok(BytesIO::new(b"")),
            Some(b) => Ok(BytesIO::new(&bytes_like(b, "BytesIO")?)),
        }
    }
    /// `io.StringIO(initial_value="")`
    pub fn StringIO(&self, args: impl Into<Args>) -> Result<StringIO> {
        let a: Args = args.into();
        match a.positional.first() {
            None | Some(Arg::None) => Ok(StringIO::new("")),
            Some(Arg::Str(s)) => Ok(StringIO::new(s)),
            Some(other) => Err(LailaError::type_error(alloc::format!(
                "initial_value must be str or None, not {}",
                other.type_name()
            ))),
        }
    }
}

// --------------------------------------------------------------------------
// io.StringIO
// --------------------------------------------------------------------------

struct StringIOState {
    buf: String,
    pos: usize,
    closed: bool,
}

/// `io.StringIO`: an in-memory text stream (`write`, `getvalue`, `read`,
/// `seek`, `tell`, `truncate`, `close`, `flush`).  The buffer is shared by
/// every clone, so a `StringIO` handed to a `logging.StreamHandler` is read
/// back through the original handle.
#[derive(Clone)]
pub struct StringIO(Arc<RwLock<StringIOState>>);

impl StringIO {
    pub fn new(initial: &str) -> StringIO {
        StringIO(Arc::new(RwLock::new(StringIOState {
            buf: initial.into(),
            pos: 0,
            closed: false,
        })))
    }
    fn check_open(&self) -> Result<()> {
        if self.0.read().closed {
            return Err(LailaError::value_error("I/O operation on closed file."));
        }
        Ok(())
    }
    /// `StringIO.getvalue()` -> the whole buffer.
    pub fn getvalue(&self) -> Result<Arg> {
        self.check_open()?;
        Ok(Arg::Str(self.0.read().buf.clone().into()))
    }
    /// `StringIO.read(size=-1)`
    pub fn read(&self, args: impl Into<Args>) -> Result<Arg> {
        self.check_open()?;
        let a: Args = args.into();
        let size = match a.positional.first() {
            None | Some(Arg::None) => -1,
            Some(s) => int_arg(s, "")?,
        };
        let mut st = self.0.write();
        let chars: Vec<char> = st.buf.chars().collect();
        let start = st.pos.min(chars.len());
        let end = if size < 0 {
            chars.len()
        } else {
            (start + size as usize).min(chars.len())
        };
        st.pos = end;
        Ok(Arg::Str(
            chars[start..end].iter().collect::<String>().into(),
        ))
    }
    /// `StringIO.write(s)` -> number of characters written.
    pub fn write(&self, s: impl IntoArg) -> Result<i64> {
        self.check_open()?;
        let s = s.into_arg();
        let text = match &s {
            Arg::Str(t) => alloc::string::String::from(&**t),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "string argument expected, got '{}'",
                    other.type_name()
                )))
            }
        };
        let mut st = self.0.write();
        let mut chars: Vec<char> = st.buf.chars().collect();
        let pos = st.pos;
        let new: Vec<char> = text.chars().collect();
        let end = pos + new.len();
        if chars.len() < pos {
            chars.resize(pos, '\0');
        }
        if chars.len() < end {
            chars.resize(end, '\0');
        }
        chars[pos..end].copy_from_slice(&new);
        st.buf = chars.into_iter().collect();
        st.pos = end;
        Ok(new.len() as i64)
    }
    /// `StringIO.seek(pos, whence=0)`
    pub fn seek(&self, args: impl Into<Args>) -> Result<i64> {
        self.check_open()?;
        let a: Args = args.into();
        let off = a
            .positional
            .first()
            .map(|p| int_arg(p, ""))
            .transpose()?
            .unwrap_or(0);
        let whence = a
            .positional
            .get(1)
            .map(|w| int_arg(w, ""))
            .transpose()?
            .unwrap_or(0);
        let mut st = self.0.write();
        let len = st.buf.chars().count() as i64;
        let base = match whence {
            0 => 0,
            1 => st.pos as i64,
            2 => len,
            _ => return Err(LailaError::value_error("invalid whence")),
        };
        let target = base + off;
        if target < 0 {
            return Err(LailaError::value_error("negative seek position"));
        }
        st.pos = target as usize;
        Ok(target)
    }
    /// `StringIO.tell()`
    pub fn tell(&self) -> Result<i64> {
        self.check_open()?;
        Ok(self.0.read().pos as i64)
    }
    /// `StringIO.truncate(size=None)`
    pub fn truncate(&self, args: impl Into<Args>) -> Result<i64> {
        self.check_open()?;
        let a: Args = args.into();
        let mut st = self.0.write();
        let size = match a.positional.first() {
            None | Some(Arg::None) => st.pos,
            Some(s) => int_arg(s, "")?.max(0) as usize,
        };
        st.buf = st.buf.chars().take(size).collect();
        Ok(size as i64)
    }
    /// `StringIO.close()`
    pub fn close(&self) {
        self.0.write().closed = true;
    }
    /// `StringIO.closed`
    pub fn closed(&self) -> bool {
        self.0.read().closed
    }
    pub fn flush(&self) -> Result<()> {
        self.check_open()
    }
    pub fn __enter__(&self) -> StringIO {
        self.clone()
    }
    pub fn __exit__(&self) {
        self.close();
    }
}

impl PyObject for StringIO {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "StringIO"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<_io.StringIO object at {:#x}>",
            Arc::as_ptr(&self.0) as *const () as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<StringIO>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((Arc::as_ptr(&self.0) as *const () as usize >> 4) as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "getvalue" => self.getvalue(),
            "read" => self.read(args),
            "write" => self
                .write(args.positional.into_iter().next().unwrap_or(Arg::None))
                .map(Arg::Int),
            "seek" => self.seek(args).map(Arg::Int),
            "tell" => self.tell().map(Arg::Int),
            "truncate" => self.truncate(args).map(Arg::Int),
            "close" => {
                self.close();
                Ok(Arg::None)
            }
            "flush" => self.flush().map(|()| Arg::None),
            _ => Err(LailaError::new(
                ExcKind::AttributeError,
                alloc::format!("'_io.StringIO' object has no attribute '{name}'"),
            )),
        }
    }
}

impl IntoArg for StringIO {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl crate::python::PyArg for StringIO {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

impl crate::python::builtins::IsInstance for StringIO {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<StringIO>())
    }
}

// --------------------------------------------------------------------------
// pickle
// --------------------------------------------------------------------------

/// `import pickle`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct pickle_mod;
#[allow(non_upper_case_globals)]
pub static pickle: pickle_mod = pickle_mod;
impl pickle_mod {
    pub fn HIGHEST_PROTOCOL(&self) -> i64 {
        _codecs::pickle::HIGHEST_PROTOCOL as i64
    }
    pub fn DEFAULT_PROTOCOL(&self) -> i64 {
        _codecs::pickle::DEFAULT_PROTOCOL as i64
    }
    /// `pickle.dumps(obj, protocol=None, *, fix_imports=True, buffer_callback=None)`
    #[pysig("dumps(obj, protocol=None, *, fix_imports=True, buffer_callback=None)")]
    pub fn dumps(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = (fix_imports, buffer_callback);
        let protocol = match protocol {
            Arg::None => None,
            p => Some(int_arg(&p, "")? as i32),
        };
        Ok(bytes(&_codecs::pickle::dumps(&obj, protocol)?))
    }
    /// `pickle.loads(data, /, *, fix_imports=True, encoding="ASCII", errors="strict", buffers=())`
    #[pysig("loads(data, *, fix_imports=True, encoding='ASCII', errors='strict', buffers=())")]
    pub fn loads(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = (fix_imports, encoding, errors, buffers);
        _codecs::pickle::loads(&bytes_like(&data, "loads")?)
    }
    /// `pickle.dump(obj, file, protocol=None)` -- `file` is an `io.BytesIO`.
    #[pysig("dump(obj, file, protocol=None, *, fix_imports=True, buffer_callback=None)")]
    pub fn dump(&self, args: impl Into<Args>) -> Result<()> {
        let _ = (fix_imports, buffer_callback);
        let protocol = match protocol {
            Arg::None => None,
            p => Some(int_arg(&p, "")? as i32),
        };
        let out = _codecs::pickle::dumps(&obj, protocol)?;
        write_to_file(&file, &out)
    }
    /// `pickle.load(file)`
    #[pysig("load(file, *, fix_imports=True, encoding='ASCII', errors='strict', buffers=())")]
    pub fn load(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = (fix_imports, encoding, errors, buffers);
        _codecs::pickle::loads(&file_bytes(&file)?)
    }
}

fn write_to_file(file: &Arg, data: &[u8]) -> Result<()> {
    match file {
        Arg::Object(o) => match o.as_any().downcast_ref::<BytesIO>() {
            Some(b) => b.write(bytes(data)).map(|_| ()),
            None => Err(LailaError::type_error(alloc::format!(
                "file must have a 'write' attribute, not '{}'",
                file.type_name()
            ))),
        },
        other => Err(LailaError::type_error(alloc::format!(
            "file must have a 'write' attribute, not '{}'",
            other.type_name()
        ))),
    }
}

// --------------------------------------------------------------------------
// msgpack
// --------------------------------------------------------------------------

/// `import msgpack`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct msgpack_mod;
#[allow(non_upper_case_globals)]
pub static msgpack: msgpack_mod = msgpack_mod;

fn pack_options(kwargs: &mut Args) -> Result<_codecs::msgpack::PackOptions> {
    let mut o = _codecs::msgpack::PackOptions::default();
    if kwargs.__contains__("use_bin_type")? {
        o.use_bin_type = kwargs.pop("use_bin_type")?.__bool__();
    }
    if kwargs.__contains__("use_single_float")? {
        o.use_single_float = kwargs.pop("use_single_float")?.__bool__();
    }
    if kwargs.__contains__("default")? {
        o.default = match kwargs.pop("default")? {
            Arg::None => None,
            Arg::Callable(c) => Some(c),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "default must be a callable, not '{}'",
                    other.type_name()
                )))
            }
        };
    }
    reject_unknown(kwargs, "Packer")?;
    Ok(o)
}

fn unpack_options(kwargs: &mut Args) -> Result<_codecs::msgpack::UnpackOptions> {
    let mut o = _codecs::msgpack::UnpackOptions::default();
    if kwargs.__contains__("raw")? {
        o.raw = kwargs.pop("raw")?.__bool__();
    }
    if kwargs.__contains__("strict_map_key")? {
        o.strict_map_key = kwargs.pop("strict_map_key")?.__bool__();
    }
    if kwargs.__contains__("use_list")? {
        o.use_list = kwargs.pop("use_list")?.__bool__();
    }
    reject_unknown(kwargs, "Unpacker")?;
    Ok(o)
}

fn reject_unknown(kwargs: &Args, who: &str) -> Result<()> {
    if let Some((k, _)) = kwargs.keywords.first() {
        return Err(LailaError::type_error(alloc::format!(
            "{who}() got an unexpected keyword argument '{k}'"
        )));
    }
    Ok(())
}

impl msgpack_mod {
    pub fn version(&self) -> Arg {
        let (a, b, c) = _codecs::msgpack::VERSION;
        Arg::Tuple(alloc::vec![Arg::Int(a as i64), Arg::Int(b as i64), Arg::Int(c as i64)].into())
    }
    /// `msgpack.ExtType(code, data)`
    pub fn ExtType(&self, code: impl IntoArg, data: impl IntoArg) -> Result<Arg> {
        let code = code.into_arg();
        let data = data.into_arg();
        let c = int_arg(&code, "")?;
        if !matches!(data, Arg::Bytes(_)) {
            return Err(LailaError::type_error("data must be bytes"));
        }
        if !(-128..=127).contains(&c) {
            return Err(LailaError::value_error("code must be 0~127 or -128~-1"));
        }
        Ok(_codecs::msgpack::ExtType::new(c as i8, bytes_like(&data, "ExtType")?).into_arg())
    }
    /// `msgpack.packb(o, **kwargs)`
    #[pysig("packb(o, **kwargs)")]
    pub fn packb(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut kwargs = kwargs;
        let opts = pack_options(&mut kwargs)?;
        Ok(bytes(&_codecs::msgpack::packb(&o, &opts)?))
    }
    /// `msgpack.dumps` is an alias of `packb`.
    pub fn dumps(&self, args: impl Into<Args>) -> Result<Arg> {
        self.packb(args)
    }
    /// `msgpack.unpackb(packed, **kwargs)`
    #[pysig("unpackb(packed, **kwargs)")]
    pub fn unpackb(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut kwargs = kwargs;
        let opts = unpack_options(&mut kwargs)?;
        _codecs::msgpack::unpackb(&bytes_like(&packed, "unpackb")?, &opts)
    }
    /// `msgpack.loads` is an alias of `unpackb`.
    pub fn loads(&self, args: impl Into<Args>) -> Result<Arg> {
        self.unpackb(args)
    }
    /// `msgpack.Unpacker(file_like=None, **kwargs)` (feed-mode only).
    #[pysig("Unpacker(file_like=None, **kwargs)")]
    pub fn Unpacker(&self, args: impl Into<Args>) -> Result<Unpacker> {
        let mut kwargs = kwargs;
        let opts = unpack_options(&mut kwargs)?;
        let mut u = _codecs::msgpack::Unpacker::new(opts);
        if !file_like.is_none() {
            u.feed(&file_bytes(&file_like)?);
        }
        Ok(Unpacker(Arc::new(RwLock::new(u))))
    }
}

/// `msgpack.Unpacker` handle (shared, so `for obj in unpacker:` works after
/// `unpacker.feed(...)`).
#[derive(Clone, Debug)]
pub struct Unpacker(Arc<RwLock<_codecs::msgpack::Unpacker>>);

impl Unpacker {
    /// `Unpacker.feed(next_bytes)`
    pub fn feed(&self, data: impl IntoArg) -> Result<()> {
        let d = data.into_arg();
        self.0.write().feed(&bytes_like(&d, "feed")?);
        Ok(())
    }
    /// `next(unpacker)` / `unpacker.unpack()`
    pub fn unpack(&self) -> Result<Arg> {
        match self.0.write().next() {
            Some(r) => r,
            None => Err(LailaError::new(
                ExcKind::ValueError,
                "No more data to unpack.",
            )),
        }
    }
    /// `list(unpacker)` -- every complete object currently buffered.
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        let mut g = self.0.write();
        let mut out = Vec::new();
        for r in g.by_ref() {
            out.push(r?);
        }
        Ok(out)
    }
}

impl IntoIterator for Unpacker {
    type Item = Result<Arg>;
    type IntoIter = alloc::vec::IntoIter<Result<Arg>>;
    fn into_iter(self) -> Self::IntoIter {
        let mut g = self.0.write();
        let out: Vec<Result<Arg>> = g.by_ref().collect();
        out.into_iter()
    }
}

// --------------------------------------------------------------------------
// base64 / binascii
// --------------------------------------------------------------------------

/// `import base64`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct base64_mod;
#[allow(non_upper_case_globals)]
pub static base64: base64_mod = base64_mod;

fn altchars_of(a: &Arg) -> Result<Option<Vec<u8>>> {
    match a {
        Arg::None => Ok(None),
        Arg::Str(s) => {
            if !s.is_ascii() {
                return Err(LailaError::value_error(
                    "string argument should contain only ASCII characters",
                ));
            }
            Ok(Some(s.as_bytes().to_vec()))
        }
        other => Ok(Some(other.as_bytes_like().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "argument should be a bytes-like object or ASCII string, not '{}'",
                other.type_name()
            ))
        })?)),
    }
}

impl base64_mod {
    /// `base64.b64encode(s, altchars=None)`
    #[pysig("b64encode(s, altchars=None)")]
    pub fn b64encode(&self, args: impl Into<Args>) -> Result<Arg> {
        let alt = altchars_of(&altchars)?;
        Ok(bytes(&_codecs::base64::b64encode(
            &bytes_like(&s, "b64encode")?,
            alt.as_deref(),
        )?))
    }
    /// `base64.b64decode(s, altchars=None, validate=False)`
    #[pysig("b64decode(s, altchars=None, validate=False)")]
    pub fn b64decode(&self, args: impl Into<Args>) -> Result<Arg> {
        let alt = altchars_of(&altchars)?;
        Ok(bytes(&_codecs::base64::b64decode(
            &s,
            alt.as_deref(),
            validate.__bool__(),
        )?))
    }
    /// `base64.standard_b64encode(s)`
    pub fn standard_b64encode(&self, s: impl IntoArg) -> Result<Arg> {
        let s = s.into_arg();
        Ok(bytes(_codecs::base64::standard_b64encode(&bytes_like(
            &s,
            "standard_b64encode",
        )?)))
    }
    /// `base64.standard_b64decode(s)`
    pub fn standard_b64decode(&self, s: impl IntoArg) -> Result<Arg> {
        Ok(bytes(&_codecs::base64::standard_b64decode(&s.into_arg())?))
    }
    /// `base64.urlsafe_b64encode(s)`
    pub fn urlsafe_b64encode(&self, s: impl IntoArg) -> Result<Arg> {
        let s = s.into_arg();
        Ok(bytes(_codecs::base64::urlsafe_b64encode(&bytes_like(
            &s,
            "urlsafe_b64encode",
        )?)))
    }
    /// `base64.urlsafe_b64decode(s)`
    pub fn urlsafe_b64decode(&self, s: impl IntoArg) -> Result<Arg> {
        Ok(bytes(&_codecs::base64::urlsafe_b64decode(&s.into_arg())?))
    }
}

/// `import binascii`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct binascii_mod;
#[allow(non_upper_case_globals)]
pub static binascii: binascii_mod = binascii_mod;
impl binascii_mod {
    /// `binascii.a2b_base64(data, /, *, strict_mode=False)`
    #[pysig("a2b_base64(data, *, strict_mode=False)")]
    pub fn a2b_base64(&self, args: impl Into<Args>) -> Result<Arg> {
        let buf = match &data {
            Arg::Str(s) => {
                if !s.is_ascii() {
                    return Err(LailaError::value_error(
                        "string argument should contain only ASCII characters",
                    ));
                }
                s.as_bytes().to_vec()
            }
            other => bytes_like(other, "a2b_base64")?,
        };
        Ok(bytes(&_codecs::base64::a2b_base64(
            &buf,
            strict_mode.__bool__(),
        )?))
    }
    /// `binascii.b2a_base64(data, /, *, newline=True)`
    #[pysig("b2a_base64(data, *, newline=True)")]
    pub fn b2a_base64(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut out = _codecs::base64::b2a_base64(&bytes_like(&data, "b2a_base64")?);
        if !newline.__bool__() && out.last() == Some(&b'\n') {
            out.pop();
        }
        Ok(bytes(&out))
    }
    /// `binascii.crc32(data, crc=0)`
    #[pysig("crc32(data, crc=0)")]
    pub fn crc32(&self, args: impl Into<Args>) -> Result<i64> {
        let v = int_arg(&crc, "")? as u32;
        Ok(_codecs::zlib::crc32(&bytes_like(&data, "crc32")?, v) as i64)
    }
    /// `binascii.hexlify(data)` / `bytes.hex()`
    pub fn hexlify(&self, data: impl IntoArg) -> Result<Arg> {
        let d = data.into_arg();
        let mut s = String::new();
        for b in bytes_like(&d, "hexlify")? {
            s.push_str(&alloc::format!("{b:02x}"));
        }
        Ok(bytes(s.as_bytes()))
    }
    /// `binascii.unhexlify(hexstr)`
    pub fn unhexlify(&self, hexstr: impl IntoArg) -> Result<Arg> {
        let h = hexstr.into_arg();
        let raw = match &h {
            Arg::Str(s) => s.as_bytes().to_vec(),
            other => bytes_like(other, "unhexlify")?,
        };
        if raw.len() % 2 != 0 {
            return Err(LailaError::new(ExcKind::BinasciiError, "Odd-length string"));
        }
        let mut out = Vec::with_capacity(raw.len() / 2);
        for pair in raw.chunks(2) {
            let hi = (pair[0] as char).to_digit(16);
            let lo = (pair[1] as char).to_digit(16);
            match (hi, lo) {
                (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
                _ => {
                    return Err(LailaError::new(
                        ExcKind::BinasciiError,
                        "Non-hexadecimal digit found",
                    ))
                }
            }
        }
        Ok(bytes(&out))
    }
}

// --------------------------------------------------------------------------
// zlib
// --------------------------------------------------------------------------

/// `import zlib`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct zlib_mod;
#[allow(non_upper_case_globals)]
pub static zlib: zlib_mod = zlib_mod;
impl zlib_mod {
    pub fn MAX_WBITS(&self) -> i64 {
        15
    }
    pub fn DEF_BUF_SIZE(&self) -> i64 {
        16384
    }
    pub fn DEF_MEM_LEVEL(&self) -> i64 {
        8
    }
    pub fn Z_DEFAULT_COMPRESSION(&self) -> i64 {
        -1
    }
    pub fn Z_NO_COMPRESSION(&self) -> i64 {
        0
    }
    pub fn Z_BEST_SPEED(&self) -> i64 {
        1
    }
    pub fn Z_BEST_COMPRESSION(&self) -> i64 {
        9
    }
    /// `zlib.compress(data, /, level=-1, wbits=15)`
    #[pysig("compress(data, level=-1, wbits=15)")]
    pub fn compress(&self, args: impl Into<Args>) -> Result<Arg> {
        let level = int_arg(&level, "")?;
        let wbits = int_arg(&wbits, "")?;
        Ok(bytes(&_codecs::zlib::compress(
            &bytes_like(&data, "compress")?,
            level as i32,
            wbits as i32,
        )?))
    }
    /// `zlib.decompress(data, /, wbits=15, bufsize=16384)`
    #[pysig("decompress(data, wbits=15, bufsize=16384)")]
    pub fn decompress(&self, args: impl Into<Args>) -> Result<Arg> {
        let wbits = int_arg(&wbits, "")?;
        let bufsize = int_arg(&bufsize, "")?;
        if bufsize < 0 {
            return Err(LailaError::value_error("bufsize must be non-negative"));
        }
        Ok(bytes(&_codecs::zlib::decompress(
            &bytes_like(&data, "decompress")?,
            wbits as i32,
            bufsize as usize,
        )?))
    }
    /// `zlib.crc32(data, value=0)`
    #[pysig("crc32(data, value=0)")]
    pub fn crc32(&self, args: impl Into<Args>) -> Result<i64> {
        let v = int_arg(&value, "")? as u32;
        Ok(_codecs::zlib::crc32(&bytes_like(&data, "crc32")?, v) as i64)
    }
    /// `zlib.adler32(data, value=1)`
    #[pysig("adler32(data, value=1)")]
    pub fn adler32(&self, args: impl Into<Args>) -> Result<i64> {
        let v = int_arg(&value, "")? as u32;
        Ok(_codecs::zlib::adler32(&bytes_like(&data, "adler32")?, v) as i64)
    }
}
