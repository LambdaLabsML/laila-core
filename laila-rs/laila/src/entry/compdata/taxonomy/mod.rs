//! Register all concrete `ComputationalData` subclasses for type dispatch.

pub mod cd_dict;
pub mod cd_list;
pub mod cd_numpy;
pub mod cd_object;
pub mod cd_torch;
#[allow(clippy::module_inception)]
pub mod compdata;

pub use self::cd_dict::CD_dict;
pub use self::cd_list::CD_list;
pub use self::cd_numpy::CD_numpyarray;
pub use self::cd_object::CD_generic;
pub use self::cd_torch::CD_torchtensor;
pub use self::compdata::{register_cdtype, CDKind, ComputationalData, TYPE_TO_WRAPPER};
