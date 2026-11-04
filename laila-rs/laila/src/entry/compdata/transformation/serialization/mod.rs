//! Serializer transformations for various Python data formats.
//!
//! Each serializer is a `DataTransformation` whose `forward` turns a Python
//! object into bytes and whose `backward` does the inverse. They are
//! typically the *first* step in a transformation pipeline (the rest is
//! encoding / compression / encryption on top of the resulting bytes).
//!
//! | Class               | When to use |
//! |---------------------|-------------|
//! | `PickleSerializer`  | Catch-all; works for arbitrary Python objects but produces opaque, version-coupled blobs. Preferred fall-back when no native serializer fits. |
//! | `MsgpackSerializer` | Compact, language-agnostic. Good for dicts / lists of JSON-shaped values; faster and smaller than pickle for those payloads. |
//! | `NumpySerializer`   | Optimised for `numpy.ndarray` -- preserves dtype, shape, and byte order. |
//! | `TorchSerializer`   | Optimised for `torch.Tensor`. No torch runtime in laila-rs: constructing it raises `ModuleNotFoundError`. |

pub mod msgpack;
pub mod numpy;
pub mod pickle;
pub mod torch;

pub use self::msgpack::MsgpackSerializer;
pub use self::numpy::NumpySerializer;
pub use self::pickle::PickleSerializer;
pub use self::torch::TorchSerializer;
