//! `laila.data.hdf5` -- HDF5 file pool backend.
//!
//! ```python
//! from .hdf5 import HDF5Pool
//! ```
//!
//! Host only (`hdf5` feature, the `laila-core[hdf5]` extra): links the
//! system `libhdf5` (the library `h5py` wraps).

#[allow(clippy::module_inception)]
pub mod hdf5;

pub use hdf5::{HDF5Backend, HDF5Pool};
