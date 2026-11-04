//! Cellular WAN transports (GSM/GPRS, LTE, 5G NR): a TCP data path over a modem link.
//!
//! Python: `laila.policy.central.communication.protocols.cellular`.

#[allow(clippy::module_inception)]
pub mod cellular;
pub mod gsm_gprs;
pub mod lte;
pub mod nr5g;

pub use cellular::_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL;
pub use gsm_gprs::_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL;
pub use lte::_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL;
pub use nr5g::_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL;
