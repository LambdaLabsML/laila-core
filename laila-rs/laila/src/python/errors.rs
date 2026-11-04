//! Python exceptions as one Rust error type.
//!
//! `LailaError` mirrors Python's exception classes (same names, same message
//! texts). Each Python class also exists as a zero-sized *marker type*
//! (`ValueError`, `KeyError`, ...) so `isinstance(e, ValueError)`,
//! `pytest.raises(ValueError)` and `raise ValueError("msg")` keep Python's
//! spelling; the marker encodes Python's class hierarchy (`KeyError` is a
//! `LookupError`, `ValidationError` is a `ValueError`, ...).

use alloc::string::String;
use core::fmt;

/// A Python exception.
///
/// Clones are handles on one Python exception *object*: `identity` is
/// assigned at construction and shared by every clone, so `e is err` holds
/// across the `Arg::Exception` / `LailaError` boundary. Equality is by
/// class and message.
#[derive(Clone, Debug)]
pub struct LailaError {
    pub kind: ExcKind,
    pub message: String,
    identity: u64,
}

impl PartialEq for LailaError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.message == other.message
    }
}
impl Eq for LailaError {}

/// Python exception class of a `LailaError`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::enum_variant_names)]
pub enum ExcKind {
    Exception,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    AttributeError,
    NotImplementedError,
    ImportError,
    RuntimeError,
    AssertionError,
    LookupError,
    OSError,
    ConnectionError,
    TimeoutError,
    PermissionError,
    FileNotFoundError,
    FileExistsError,
    StopIteration,
    ZeroDivisionError,
    OverflowError,
    ValidationError,
    EntryNotBuiltError,
    BlockingNotPossibleError,
    // stdlib / third-party classes the codecs raise
    SyntaxError,
    MemoryError,
    UnicodeDecodeError,
    UnicodeEncodeError,
    /// `binascii.Error` (base64).
    BinasciiError,
    /// `zlib.error`.
    ZlibError,
    /// `json.JSONDecodeError`.
    JSONDecodeError,
    /// `pickle.PicklingError`.
    PicklingError,
    /// `pickle.UnpicklingError`.
    UnpicklingError,
    /// `msgpack.ExtraData`.
    ExtraData,
    /// `msgpack.FormatError` / `StackError` family (`msgpack.exceptions.UnpackValueError`).
    UnpackValueError,
    /// `cryptography.fernet.InvalidToken`.
    InvalidToken,
    /// `cryptography.exceptions.InvalidSignature`.
    InvalidSignature,
    /// `asyncio.CancelledError` / `concurrent.futures.CancelledError`.
    CancelledError,
    /// `laila.policy.central.command.schema.exceptions.LoopBlockingWaitError`.
    LoopBlockingWaitError,
    /// `laila.policy.central.command.schema.exceptions.NestedCommandSubmitError`.
    NestedCommandSubmitError,
    /// `laila.policy.central.command.schema.parking.CyclicDependencyError`.
    CyclicDependencyError,
    /// `concurrent.futures.process.BrokenProcessPool`.
    BrokenProcessPool,
    /// `threading.BrokenBarrierError`.
    BrokenBarrierError,
    /// `sqlite3.OperationalError` (the port derives it from `OSError`, the
    /// kind the file-backed pools raised before the sqlite classes existed).
    OperationalError,
    /// `sqlite3.ProgrammingError` (a closed connection; derives from `OSError`
    /// here, as `OperationalError` does).
    ProgrammingError,
    /// `tomllib.TOMLDecodeError` (a `ValueError`).
    TOMLDecodeError,
    /// `xml.etree.ElementTree.ParseError` (a `SyntaxError`).
    ParseError,
    /// `laila.policy.central.communication.wire.BackpressureError`: a
    /// communication path refused a message (`ERR_BUSY` from the peer, a
    /// full stream lane, a closed channel).
    BackpressureError,
}

impl ExcKind {
    /// Python's class name.
    pub fn name(self) -> &'static str {
        match self {
            ExcKind::Exception => "Exception",
            ExcKind::ValueError => "ValueError",
            ExcKind::TypeError => "TypeError",
            ExcKind::KeyError => "KeyError",
            ExcKind::IndexError => "IndexError",
            ExcKind::AttributeError => "AttributeError",
            ExcKind::NotImplementedError => "NotImplementedError",
            ExcKind::ImportError => "ImportError",
            ExcKind::RuntimeError => "RuntimeError",
            ExcKind::AssertionError => "AssertionError",
            ExcKind::LookupError => "LookupError",
            ExcKind::OSError => "OSError",
            ExcKind::ConnectionError => "ConnectionError",
            ExcKind::TimeoutError => "TimeoutError",
            ExcKind::PermissionError => "PermissionError",
            ExcKind::FileNotFoundError => "FileNotFoundError",
            ExcKind::FileExistsError => "FileExistsError",
            ExcKind::StopIteration => "StopIteration",
            ExcKind::ZeroDivisionError => "ZeroDivisionError",
            ExcKind::OverflowError => "OverflowError",
            ExcKind::ValidationError => "ValidationError",
            ExcKind::EntryNotBuiltError => "EntryNotBuiltError",
            ExcKind::BlockingNotPossibleError => "BlockingNotPossibleError",
            ExcKind::SyntaxError => "SyntaxError",
            ExcKind::MemoryError => "MemoryError",
            ExcKind::UnicodeDecodeError => "UnicodeDecodeError",
            ExcKind::UnicodeEncodeError => "UnicodeEncodeError",
            ExcKind::BinasciiError => "Error",
            ExcKind::ZlibError => "error",
            ExcKind::JSONDecodeError => "JSONDecodeError",
            ExcKind::PicklingError => "PicklingError",
            ExcKind::UnpicklingError => "UnpicklingError",
            ExcKind::ExtraData => "ExtraData",
            ExcKind::UnpackValueError => "UnpackValueError",
            ExcKind::InvalidToken => "InvalidToken",
            ExcKind::InvalidSignature => "InvalidSignature",
            ExcKind::CancelledError => "CancelledError",
            ExcKind::LoopBlockingWaitError => "LoopBlockingWaitError",
            ExcKind::NestedCommandSubmitError => "NestedCommandSubmitError",
            ExcKind::CyclicDependencyError => "CyclicDependencyError",
            ExcKind::BrokenProcessPool => "BrokenProcessPool",
            ExcKind::BrokenBarrierError => "BrokenBarrierError",
            ExcKind::OperationalError => "OperationalError",
            ExcKind::ProgrammingError => "ProgrammingError",
            ExcKind::TOMLDecodeError => "TOMLDecodeError",
            ExcKind::ParseError => "ParseError",
            ExcKind::BackpressureError => "BackpressureError",
        }
    }

    /// Python's `__module__ + "." + __qualname__` where it differs from the
    /// bare name (`binascii.Error`, `zlib.error`, `msgpack.ExtraData`, ...).
    pub fn qualified_name(self) -> &'static str {
        match self {
            ExcKind::BinasciiError => "binascii.Error",
            ExcKind::ZlibError => "zlib.error",
            ExcKind::JSONDecodeError => "json.decoder.JSONDecodeError",
            ExcKind::PicklingError => "_pickle.PicklingError",
            ExcKind::UnpicklingError => "_pickle.UnpicklingError",
            ExcKind::ExtraData => "msgpack.exceptions.ExtraData",
            ExcKind::UnpackValueError => "msgpack.exceptions.UnpackValueError",
            ExcKind::InvalidToken => "cryptography.fernet.InvalidToken",
            ExcKind::InvalidSignature => "cryptography.exceptions.InvalidSignature",
            ExcKind::CancelledError => "asyncio.exceptions.CancelledError",
            ExcKind::BrokenProcessPool => "concurrent.futures.process.BrokenProcessPool",
            ExcKind::BrokenBarrierError => "threading.BrokenBarrierError",
            ExcKind::OperationalError => "sqlite3.OperationalError",
            ExcKind::ProgrammingError => "sqlite3.ProgrammingError",
            ExcKind::TOMLDecodeError => "tomllib.TOMLDecodeError",
            ExcKind::ParseError => "xml.etree.ElementTree.ParseError",
            other => other.name(),
        }
    }

    /// Python's `__bases__` walk: the direct base class, `None` for `Exception`.
    pub fn base(self) -> Option<ExcKind> {
        use ExcKind::*;
        Some(match self {
            Exception => return None,
            KeyError | IndexError => LookupError,
            ConnectionError | TimeoutError | PermissionError | FileNotFoundError
            | FileExistsError => OSError,
            ValidationError => ValueError,
            NotImplementedError => RuntimeError,
            ZeroDivisionError | OverflowError => Exception, // ArithmeticError, folded
            EntryNotBuiltError | BlockingNotPossibleError => RuntimeError,
            LoopBlockingWaitError | NestedCommandSubmitError | CyclicDependencyError => {
                RuntimeError
            }
            BrokenProcessPool => RuntimeError, // BrokenExecutor, folded
            BrokenBarrierError => RuntimeError,
            OperationalError | ProgrammingError => OSError,
            TOMLDecodeError => ValueError,
            ParseError => SyntaxError,
            BackpressureError => RuntimeError,
            CancelledError => Exception, // BaseException in Python
            UnicodeDecodeError | UnicodeEncodeError | JSONDecodeError | UnpackValueError => {
                ValueError
            }
            PicklingError | UnpicklingError => Exception, // pickle.PickleError, folded
            _ => Exception,
        })
    }

    /// `issubclass(self, other)`.
    pub fn is_subclass_of(self, other: ExcKind) -> bool {
        let mut cur = Some(self);
        while let Some(k) = cur {
            if k == other {
                return true;
            }
            cur = k.base();
        }
        false
    }
}

impl LailaError {
    pub fn new(kind: ExcKind, message: impl Into<String>) -> Self {
        static NEXT: super::sync::IdCounter = super::sync::IdCounter::new();
        LailaError {
            kind,
            message: message.into(),
            identity: NEXT.next(),
        }
    }
    /// `a is b`: the same exception object (clones share the identity).
    pub fn is_same(&self, other: &LailaError) -> bool {
        self.identity == other.identity
    }
    /// `isinstance(self, T)` for an exception marker type.
    pub fn is<T: ExcMarker>(&self) -> bool {
        self.kind.is_subclass_of(T::KIND)
    }
    /// Python's `str(exc)`. For `KeyError`, `message` already holds
    /// `repr(key)` (Python's `str(KeyError(k)) == repr(k)`).
    pub fn __str__(&self) -> String {
        self.message.clone()
    }
}

/// `fut.set_exception(err)` with a still-live `err` binding (`assertIs(..., err)`).
impl From<&LailaError> for alloc::sync::Arc<LailaError> {
    fn from(e: &LailaError) -> Self {
        alloc::sync::Arc::new(e.clone())
    }
}

impl fmt::Display for LailaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.name(), self.__str__())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for LailaError {}

/// `Result` with laila's exception type.
pub type Result<T, E = LailaError> = core::result::Result<T, E>;

/// Implemented by the exception marker types (`ValueError`, ...).
pub trait ExcMarker {
    const KIND: ExcKind;
}

/// The second argument of `assertRaises` / `pytest.raises` / `except`: a
/// marker or a tuple of markers (`(TypeError, ValueError)`).
pub trait ExcMatch {
    /// `isinstance(e, T)` for the marker(s).
    fn matches(e: &LailaError) -> bool;
    /// The text `"<T> not raised"` names.
    fn name() -> String;
}
impl<T: ExcMarker> ExcMatch for T {
    fn matches(e: &LailaError) -> bool {
        e.is::<T>()
    }
    fn name() -> String {
        T::KIND.name().into()
    }
}
macro_rules! exc_match_tuple {
    ($($t:ident),+) => {
        impl<$($t: ExcMatch + ?Sized),+> ExcMatch for ($(&$t,)+) {
            fn matches(e: &LailaError) -> bool { $( $t::matches(e) )||+ }
            fn name() -> String {
                let names: alloc::vec::Vec<String> = alloc::vec![$( $t::name() ),+];
                alloc::format!("({})", names.join(", "))
            }
        }
    };
}
exc_match_tuple!(A);
exc_match_tuple!(A, B);
exc_match_tuple!(A, B, C);

macro_rules! exc_markers {
    ($($name:ident),* $(,)?) => {
        $(
            #[doc = concat!("Marker for Python's `", stringify!($name), "`; also a constructor: `", stringify!($name), "::new(args!(\"msg\"))`.")]
            #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
            pub struct $name;
            impl ExcMarker for $name {
                const KIND: ExcKind = ExcKind::$name;
            }
            impl $name {
                /// Python `Name("message")` -- a `LailaError` value, as every
                /// exception class is in Python.
                #[allow(clippy::new_ret_no_self)]
                pub fn new(args: impl Into<crate::python::Args>) -> LailaError {
                    let a: crate::python::Args = args.into();
                    let msg = match ExcKind::$name {
                        // KeyError("k") prints as 'k' in Python.
                        ExcKind::KeyError => a.positional.first().map(|m| m.__repr__()).unwrap_or_default(),
                        _ => a.positional.first().map(|m| m.__str__()).unwrap_or_default(),
                    };
                    LailaError::new(ExcKind::$name, msg)
                }
            }
            impl From<$name> for LailaError {
                fn from(_: $name) -> LailaError {
                    LailaError::new(ExcKind::$name, "")
                }
            }
            /// The exception *class* as a value (`(RuntimeError, OSError)`
            /// in a module constant, `raise cls(..)`).
            impl crate::python::PyArg for $name {
                fn to_arg(&self) -> crate::python::Arg {
                    exception_class(ExcKind::$name)
                }
            }
            /// `issubclass(Name, Other)` on exception classes.
            impl<B: ExcMarker> crate::python::builtins::IsSubclassOf<B> for $name {
                fn issubclass() -> bool {
                    ExcKind::$name.is_subclass_of(B::KIND)
                }
            }
        )*
        impl ExcKind {
            /// The kind named `name` (an exception class name), if any.
            pub fn from_name(name: &str) -> Option<ExcKind> {
                $( if name == stringify!($name) { return Some(ExcKind::$name); } )*
                None
            }
        }
    };
}

/// An exception class as a class object: calling it constructs the
/// exception value.
pub fn exception_class(kind: ExcKind) -> crate::python::Arg {
    use crate::python::IntoArg;
    let ctor: crate::python::Callable = alloc::sync::Arc::new(move |a: crate::python::Args| {
        let msg = match kind {
            ExcKind::KeyError => a
                .positional
                .first()
                .map(|m| m.__repr__())
                .unwrap_or_default(),
            _ => a
                .positional
                .first()
                .map(|m| m.__str__())
                .unwrap_or_default(),
        };
        Ok(LailaError::new(kind, msg).into_arg())
    });
    crate::python::PyClass::with_ctor(kind.name(), "builtins", ctor).into_arg()
}

/// `isinstance(e, cls)` with *cls* an exception class value (or a tuple of
/// them): `false` for anything else.
pub fn exception_matches_value(e: &LailaError, cls: &crate::python::Arg) -> bool {
    use crate::python::Arg;
    match cls {
        Arg::Tuple(t) => t.iter().any(|c| exception_matches_value(e, c)),
        Arg::List(l) => l.read().iter().any(|c| exception_matches_value(e, c)),
        Arg::Object(o) => match o.as_any().downcast_ref::<crate::python::PyClass>() {
            Some(c) => ExcKind::from_name(c.name).is_some_and(|k| e.kind.is_subclass_of(k)),
            None => false,
        },
        _ => false,
    }
}

exc_markers!(
    Exception,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    AttributeError,
    NotImplementedError,
    ImportError,
    RuntimeError,
    AssertionError,
    LookupError,
    OSError,
    ConnectionError,
    TimeoutError,
    PermissionError,
    FileNotFoundError,
    FileExistsError,
    StopIteration,
    ZeroDivisionError,
    OverflowError,
    ValidationError,
    EntryNotBuiltError,
    BlockingNotPossibleError,
    SyntaxError,
    MemoryError,
    UnicodeDecodeError,
    UnicodeEncodeError,
    BinasciiError,
    ZlibError,
    JSONDecodeError,
    PicklingError,
    UnpicklingError,
    ExtraData,
    UnpackValueError,
    InvalidToken,
    InvalidSignature,
    CancelledError,
    LoopBlockingWaitError,
    NestedCommandSubmitError,
    CyclicDependencyError,
    BrokenProcessPool,
    BrokenBarrierError,
    OperationalError,
    ProgrammingError,
    TOMLDecodeError,
    ParseError,
    BackpressureError,
);

/// Marker for Python's `BaseException`. The port folds `BaseException`
/// into `Exception` (the root of `ExcKind`), so `except BaseException`
/// catches everything, as in Python.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BaseException;
impl ExcMarker for BaseException {
    const KIND: ExcKind = ExcKind::Exception;
}
impl BaseException {
    /// Python `BaseException("message")`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<crate::python::Args>) -> LailaError {
        let a: crate::python::Args = args.into();
        let msg = a
            .positional
            .first()
            .map(|m| m.__str__())
            .unwrap_or_default();
        LailaError::new(ExcKind::Exception, msg)
    }
}

/// Shorthand constructors used throughout the port (Python's message texts).
impl LailaError {
    pub fn type_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::TypeError, msg)
    }
    pub fn value_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::ValueError, msg)
    }
    pub fn key_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::KeyError, msg)
    }
    pub fn attribute_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::AttributeError, msg)
    }
    pub fn not_implemented(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::NotImplementedError, msg)
    }
    pub fn runtime_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::RuntimeError, msg)
    }
    pub fn connection_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::ConnectionError, msg)
    }
    pub fn timeout_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::TimeoutError, msg)
    }
    pub fn backpressure_error(msg: impl Into<String>) -> Self {
        Self::new(ExcKind::BackpressureError, msg)
    }
}
