//! Default implementations and compile-time constants for laila.
//!
//! Single source of truth for the "what does laila ship with by default?"
//! question. The `Default*` names at the top of the module name the
//! *concrete* class that backs each abstract role (`DefaultPolicy` is
//! `_LAILA_IDENTIFIABLE_POLICY`, `DefaultPool` is `_LAILA_IDENTIFIABLE_POOL`,
//! `DefaultTaskForce` is `PythonAsyncThreadPoolTaskForce`, ...). The full
//! table is [`DEFAULT_CLASS_ALIASES`]; the aliases whose Rust twin exists
//! are type aliases as well (`DefaultPool`), the others are reachable as
//! class objects through the module object (`defaults.DefaultPolicy`) and
//! gain a Rust type as their phase lands.
//!
//! Other constants:
//!
//! - [`AUTO_INITIALIZE_POLICY`] -- whether `import laila` should spin up a
//!   default policy automatically.
//! - [`LAILA_UNIVERSAL_NAMESPACE`] -- the UUID-5 namespace used as the
//!   *root* of laila's deterministic-id tree
//!   (`uuid.uuid5(uuid.NAMESPACE_DNS, "laila")`). Changing it invalidates
//!   every nickname-derived id ever produced.
//! - [`LAILA_DEFAULT_DIRECTORIES`] -- the on-disk layout under `~/.laila`:
//!   per-pool storage under `pools/`, log files under `logs/`, key material
//!   under `secrets/`, and non-memorizing query helpers under `indices/`.
//!   Each directory is created on first use if missing (Python: at import).

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::python::stdlib::UUID;
use crate::python::{Arg, IntoArg, PyDictBuilder};

/// `DefaultMultiBuffer = MultiBuffer`
pub use crate::data::multibuffer::multibuffer::MultiBuffer as DefaultMultiBuffer;
/// `DefaultPool = _LAILA_IDENTIFIABLE_POOL` -- the same class object
/// (`laila.DefaultPool is _LAILA_IDENTIFIABLE_POOL`).
pub use crate::data::schema::base::_LAILA_IDENTIFIABLE_POOL as DefaultPool;
/// `DefaultCentralCommand = _LAILA_IDENTIFIABLE_CENTRAL_COMMAND`
pub use crate::policy::central::command::schema::base::_LAILA_IDENTIFIABLE_CENTRAL_COMMAND as DefaultCentralCommand;
/// `DefaultTaskForce = PythonAsyncThreadPoolTaskForce`
pub use crate::policy::central::command::taskforce::TaskForce as DefaultTaskForce;
/// `DefaultCellularProtocol = _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::cellular::cellular::_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL as DefaultCellularProtocol;
/// `DefaultGSMProtocol = _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::cellular::gsm_gprs::_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL as DefaultGSMProtocol;
/// `DefaultLTEProtocol = _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::cellular::lte::_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL as DefaultLTEProtocol;
/// `DefaultNR5GProtocol = _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::cellular::nr5g::_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL as DefaultNR5GProtocol;
/// `DefaultAMQPProtocol = _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::amqp::_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL as DefaultAMQPProtocol;
/// `DefaultCoAPProtocol = _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::coap::_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL as DefaultCoAPProtocol;
/// `DefaultDDSProtocol = _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::dds::_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL as DefaultDDSProtocol;
/// `DefaultDTLSProtocol = _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::dtls::_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL as DefaultDTLSProtocol;
/// `DefaultEthernetProtocol = _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::ethernet::_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL as DefaultEthernetProtocol;
/// `DefaultGRPCProtocol = _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::grpc::_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL as DefaultGRPCProtocol;
/// `DefaultHTTP2Protocol = _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::http2::_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL as DefaultHTTP2Protocol;
/// `DefaultHTTP3Protocol = _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::http3::_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL as DefaultHTTP3Protocol;
/// `DefaultModbusTCPProtocol = _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::modbus_tcp::_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL as DefaultModbusTCPProtocol;
/// `DefaultMQTTProtocol = _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::mqtt::_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL as DefaultMQTTProtocol;
/// `DefaultOPCUAProtocol = _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::opcua::_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL as DefaultOPCUAProtocol;
/// `DefaultTCPProtocol = _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::tcp::_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL as DefaultTCPProtocol;
/// `DefaultTLSProtocol = _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::tls::_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL as DefaultTLSProtocol;
/// `DefaultUDPProtocol = _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::udp::_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL as DefaultUDPProtocol;
/// `DefaultXMPPProtocol = _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::xmpp::_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL as DefaultXMPPProtocol;
/// `DefaultZeroMQProtocol = _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::ip_app::zeromq::_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL as DefaultZeroMQProtocol;
/// `DefaultLoopbackProtocol = _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::local::loopback::_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL as DefaultLoopbackProtocol;
/// `DefaultUnixSocketProtocol = _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::local::unixsocket::_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL as DefaultUnixSocketProtocol;
/// `DefaultLoRaProtocol = _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::lora::_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL as DefaultLoRaProtocol;
/// `DefaultLoRaWANProtocol = _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::lorawan::_LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL as DefaultLoRaWANProtocol;
/// `DefaultLTEMProtocol = _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::ltem::_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL as DefaultLTEMProtocol;
/// `DefaultNBIoTProtocol = _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::nbiot::_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL as DefaultNBIoTProtocol;
/// `DefaultSatelliteProtocol = _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::satellite::_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL as DefaultSatelliteProtocol;
/// `DefaultSigfoxProtocol = _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::lpwan::sigfox::_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL as DefaultSigfoxProtocol;
/// `DefaultANTProtocol = _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::ant::_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL as DefaultANTProtocol;
/// `DefaultBluetoothProtocol = _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::bluetooth::_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL as DefaultBluetoothProtocol;
/// `DefaultESPNowProtocol = _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::espnow::_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL as DefaultESPNowProtocol;
/// `DefaultIrDAProtocol = _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::irda::_LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL as DefaultIrDAProtocol;
/// `DefaultMatterProtocol = _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::matter::_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL as DefaultMatterProtocol;
/// `DefaultNFCProtocol = _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::nfc::_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL as DefaultNFCProtocol;
/// `DefaultRFIDProtocol = _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::rfid::_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL as DefaultRFIDProtocol;
/// `DefaultSixLoWPANProtocol = _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::sixlowpan::_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL as DefaultSixLoWPANProtocol;
/// `DefaultThreadProtocol = _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::thread::_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL as DefaultThreadProtocol;
/// `DefaultUWBProtocol = _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::uwb::_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL as DefaultUWBProtocol;
/// `DefaultWiFiProtocol = _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::wifi::_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL as DefaultWiFiProtocol;
/// `DefaultWiFiDirectProtocol = _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::wifi_direct::_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL as DefaultWiFiDirectProtocol;
/// `DefaultZigbeeProtocol = _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::zigbee::_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL as DefaultZigbeeProtocol;
/// `DefaultZWaveProtocol = _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::short_range::zwave::_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL as DefaultZWaveProtocol;
/// `DefaultTCPIPProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::tcpip::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL as DefaultTCPIPProtocol;
/// `DefaultWebSocketProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::tcpip::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL as DefaultWebSocketProtocol;
/// `DefaultCANProtocol = _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::can::_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL as DefaultCANProtocol;
/// `DefaultENIPProtocol = _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::enip::_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL as DefaultENIPProtocol;
/// `DefaultEtherCATProtocol = _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::ethercat::_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL as DefaultEtherCATProtocol;
/// `DefaultI2CProtocol = _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::i2c::_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL as DefaultI2CProtocol;
/// `DefaultI2SProtocol = _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::i2s::_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL as DefaultI2SProtocol;
/// `DefaultLINProtocol = _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::lin::_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL as DefaultLINProtocol;
/// `DefaultModbusRTUProtocol = _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::modbus_rtu::_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL as DefaultModbusRTUProtocol;
/// `DefaultOneWireProtocol = _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::one_wire::_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL as DefaultOneWireProtocol;
/// `DefaultProfinetProtocol = _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::profinet::_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL as DefaultProfinetProtocol;
/// `DefaultRS232Protocol = _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::rs232::_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL as DefaultRS232Protocol;
/// `DefaultRS485Protocol = _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::rs485::_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL as DefaultRS485Protocol;
/// `DefaultSDIOProtocol = _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::sdio::_LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL as DefaultSDIOProtocol;
/// `DefaultSPIProtocol = _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::spi::_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL as DefaultSPIProtocol;
/// `DefaultUARTProtocol = _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::uart::_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL as DefaultUARTProtocol;
/// `DefaultUSBProtocol = _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::protocols::wired::usb::_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL as DefaultUSBProtocol;
/// `DefaultCentralCommunication = _LAILA_IDENTIFIABLE_COMMUNICATION`
#[cfg(feature = "std")]
pub use crate::policy::central::communication::schema::base::_LAILA_IDENTIFIABLE_COMMUNICATION as DefaultCentralCommunication;
/// `DefaultPoolRouter = _LAILA_IDENTIFIABLE_POOL_ROUTER`
pub use crate::policy::central::memory::router::pool_router::_LAILA_IDENTIFIABLE_POOL_ROUTER as DefaultPoolRouter;
/// `DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY`
pub use crate::policy::schema::base::_LAILA_IDENTIFIABLE_POLICY as DefaultPolicy;

/// `AUTO_INITIALIZE_POLICY = True`
pub const AUTO_INITIALIZE_POLICY: bool = true;

/// `LAILA_UNIVERSAL_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "laila")`
///
/// DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING.
#[allow(non_snake_case)]
pub fn LAILA_UNIVERSAL_NAMESPACE() -> UUID {
    UUID {
        bytes: crate::_compat::uuid_ns::universal_namespace(),
    }
}

/// `_DEFAULT_ROOT = os.path.expanduser("~/.laila")`
#[allow(non_snake_case)]
pub fn _DEFAULT_ROOT() -> String {
    #[cfg(feature = "std")]
    {
        if let Some(home) = std::env::var_os("HOME") {
            let mut p = std::path::PathBuf::from(home);
            p.push(".laila");
            return p.to_string_lossy().into_owned();
        }
    }
    String::from("~/.laila")
}

/// The keys of `LAILA_DEFAULT_DIRECTORIES`, in order.
pub const DEFAULT_DIRECTORY_KEYS: &[&str] = &["root", "pools", "logs", "secrets", "indices"];

/// The layout rooted at *root*: `(key, path)` pairs in
/// [`DEFAULT_DIRECTORY_KEYS`] order.
fn directories_under(root: &str) -> Vec<(&'static str, String)> {
    let join = |sub: &str| {
        #[cfg(feature = "std")]
        {
            let mut p = std::path::PathBuf::from(root);
            p.push(sub);
            p.to_string_lossy().into_owned()
        }
        #[cfg(not(feature = "std"))]
        {
            alloc::format!("{root}/{sub}")
        }
    };
    alloc::vec![
        ("root", root.to_string()),
        ("pools", join("pools")),
        ("logs", join("logs")),
        ("secrets", join("secrets")),
        ("indices", join("indices")),
    ]
}

/// The one `LAILA_DEFAULT_DIRECTORIES` dict (Python's module-level dict:
/// `set_default_directory` and `.update(..)` / `.clear()` mutate it in
/// place and every reader sees the change).
#[cfg(feature = "std")]
static DEFAULT_DIRECTORIES: std::sync::LazyLock<Arg> = std::sync::LazyLock::new(|| {
    let mut d = PyDictBuilder::new();
    for (k, v) in directories_under(&_DEFAULT_ROOT()) {
        let _ = std::fs::create_dir_all(&v);
        d.insert(k, v);
    }
    d.finish()
});

/// `LAILA_DEFAULT_DIRECTORIES` as `(key, path)` pairs -- read from the
/// live dict, so a `set_default_directory` is honoured.
pub fn default_directories() -> Vec<(&'static str, String)> {
    #[cfg(feature = "std")]
    {
        let d = LAILA_DEFAULT_DIRECTORIES();
        let fallback = directories_under(&_DEFAULT_ROOT());
        fallback
            .into_iter()
            .map(|(k, v)| match d.__getitem__(k) {
                Ok(Arg::Str(s)) => (k, s.to_string()),
                _ => (k, v),
            })
            .collect()
    }
    #[cfg(not(feature = "std"))]
    {
        directories_under(&_DEFAULT_ROOT())
    }
}

/// `for _dir in LAILA_DEFAULT_DIRECTORIES.values(): os.makedirs(_dir, exist_ok=True)`
pub fn ensure_default_directories() {
    #[cfg(feature = "std")]
    {
        for (_, dir) in default_directories() {
            let _ = std::fs::create_dir_all(dir);
        }
    }
}

/// `LAILA_DEFAULT_DIRECTORIES = {"root": .., "pools": .., "logs": .., "secrets": .., "indices": ..}`
/// -- the shared module dict (the directories are created on first access,
/// as Python does at import).
#[allow(non_snake_case)]
pub fn LAILA_DEFAULT_DIRECTORIES() -> Arg {
    #[cfg(feature = "std")]
    {
        DEFAULT_DIRECTORIES.clone()
    }
    #[cfg(not(feature = "std"))]
    {
        let mut d = PyDictBuilder::new();
        for (k, v) in directories_under(&_DEFAULT_ROOT()) {
            d.insert(k, v);
        }
        d.finish()
    }
}

/// `laila.set_default_directory(directory)`: relocate every on-disk
/// sub-directory laila uses by default. Mutates `LAILA_DEFAULT_DIRECTORIES`
/// in place; pools created *after* this call resolve their backing
/// directories from the new root, pools that already opened files keep
/// their paths. `~` / `~user` are expanded.
pub fn set_default_directory(directory: impl IntoArg) -> crate::python::Result<()> {
    let directory = directory.into_arg().__str__();
    #[cfg(feature = "std")]
    let directory = crate::python::stdlib::os
        .path()
        .expanduser(directory.as_str())?;
    let d = LAILA_DEFAULT_DIRECTORIES();
    let mut update = PyDictBuilder::new();
    for (k, v) in directories_under(&directory) {
        update.insert(k, v);
    }
    d.update(crate::python::Args::from(update.finish()))?;
    ensure_default_directories();
    Ok(())
}

/// `(alias, concrete class name, concrete class module)` for every
/// `Default* = <class>` assignment of the Python module, in source order.
pub const DEFAULT_CLASS_ALIASES: &[(&str, &str, &str)] = &[
    (
        "DefaultTaskForce",
        "PythonAsyncThreadPoolTaskForce",
        "laila.policy.central.command.taskforce.async_thread_pool_executor",
    ),
    (
        "DefaultCentralCommand",
        "_LAILA_IDENTIFIABLE_CENTRAL_COMMAND",
        "laila.policy.central.command.schema.base",
    ),
    (
        "DefaultCentralCommunication",
        "_LAILA_IDENTIFIABLE_COMMUNICATION",
        "laila.policy.central.communication.schema.base",
    ),
    (
        "DefaultTCPIPProtocol",
        "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.tcpip",
    ),
    (
        "DefaultWebSocketProtocol",
        "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.tcpip",
    ),
    (
        "DefaultTCPProtocol",
        "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.tcp",
    ),
    (
        "DefaultUDPProtocol",
        "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.udp",
    ),
    (
        "DefaultTLSProtocol",
        "_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.tls",
    ),
    (
        "DefaultEthernetProtocol",
        "_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.ethernet",
    ),
    (
        "DefaultWiFiProtocol",
        "_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.wifi",
    ),
    (
        "DefaultWiFiDirectProtocol",
        "_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.wifi_direct",
    ),
    (
        "DefaultCellularProtocol",
        "_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.cellular.cellular",
    ),
    (
        "DefaultGSMProtocol",
        "_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.cellular.gsm_gprs",
    ),
    (
        "DefaultLTEProtocol",
        "_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.cellular.lte",
    ),
    (
        "DefaultNR5GProtocol",
        "_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.cellular.nr5g",
    ),
    (
        "DefaultNBIoTProtocol",
        "_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.nbiot",
    ),
    (
        "DefaultLTEMProtocol",
        "_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.ltem",
    ),
    (
        "DefaultMatterProtocol",
        "_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.matter",
    ),
    (
        "DefaultSigfoxProtocol",
        "_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.sigfox",
    ),
    (
        "DefaultSatelliteProtocol",
        "_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.satellite",
    ),
    (
        "DefaultUSBProtocol",
        "_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.usb",
    ),
    (
        "DefaultLoRaWANProtocol",
        "_LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.lorawan",
    ),
    (
        "DefaultSixLoWPANProtocol",
        "_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.sixlowpan",
    ),
    (
        "DefaultZigbeeProtocol",
        "_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.zigbee",
    ),
    (
        "DefaultThreadProtocol",
        "_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.thread",
    ),
    (
        "DefaultZWaveProtocol",
        "_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.zwave",
    ),
    (
        "DefaultNFCProtocol",
        "_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.nfc",
    ),
    (
        "DefaultRFIDProtocol",
        "_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.rfid",
    ),
    (
        "DefaultUWBProtocol",
        "_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.uwb",
    ),
    (
        "DefaultIrDAProtocol",
        "_LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.irda",
    ),
    (
        "DefaultANTProtocol",
        "_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.ant",
    ),
    (
        "DefaultESPNowProtocol",
        "_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.espnow",
    ),
    (
        "DefaultUnixSocketProtocol",
        "_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.local.unixsocket",
    ),
    (
        "DefaultLoopbackProtocol",
        "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.local.loopback",
    ),
    (
        "DefaultMQTTProtocol",
        "_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.mqtt",
    ),
    (
        "DefaultAMQPProtocol",
        "_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.amqp",
    ),
    (
        "DefaultZeroMQProtocol",
        "_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.zeromq",
    ),
    (
        "DefaultUARTProtocol",
        "_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.uart",
    ),
    (
        "DefaultRS232Protocol",
        "_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.rs232",
    ),
    (
        "DefaultRS485Protocol",
        "_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.rs485",
    ),
    (
        "DefaultCANProtocol",
        "_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.can",
    ),
    (
        "DefaultModbusTCPProtocol",
        "_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.modbus_tcp",
    ),
    (
        "DefaultModbusRTUProtocol",
        "_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.modbus_rtu",
    ),
    (
        "DefaultI2CProtocol",
        "_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.i2c",
    ),
    (
        "DefaultSPIProtocol",
        "_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.spi",
    ),
    (
        "DefaultENIPProtocol",
        "_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.enip",
    ),
    (
        "DefaultLINProtocol",
        "_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.lin",
    ),
    (
        "DefaultOneWireProtocol",
        "_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.one_wire",
    ),
    (
        "DefaultI2SProtocol",
        "_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.i2s",
    ),
    (
        "DefaultSDIOProtocol",
        "_LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.sdio",
    ),
    (
        "DefaultProfinetProtocol",
        "_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.profinet",
    ),
    (
        "DefaultEtherCATProtocol",
        "_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.wired.ethercat",
    ),
    (
        "DefaultCoAPProtocol",
        "_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.coap",
    ),
    (
        "DefaultHTTP2Protocol",
        "_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.http2",
    ),
    (
        "DefaultHTTP3Protocol",
        "_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.http3",
    ),
    (
        "DefaultDTLSProtocol",
        "_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.dtls",
    ),
    (
        "DefaultXMPPProtocol",
        "_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.xmpp",
    ),
    (
        "DefaultDDSProtocol",
        "_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.dds",
    ),
    (
        "DefaultOPCUAProtocol",
        "_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.opcua",
    ),
    (
        "DefaultGRPCProtocol",
        "_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.ip_app.grpc",
    ),
    (
        "DefaultLoRaProtocol",
        "_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.lpwan.lora",
    ),
    (
        "DefaultBluetoothProtocol",
        "_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL",
        "laila.policy.central.communication.protocols.short_range.bluetooth",
    ),
    (
        "DefaultCentralMemory",
        "_LAILA_IDENTIFIABLE_CENTRAL_MEMORY",
        "laila.policy.central.memory.schema.base",
    ),
    (
        "DefaultPolicy",
        "_LAILA_IDENTIFIABLE_POLICY",
        "laila.policy.schema.base",
    ),
    (
        "DefaultPool",
        "_LAILA_IDENTIFIABLE_POOL",
        "laila.data.schema.base",
    ),
    (
        "DefaultPoolRouter",
        "_LAILA_IDENTIFIABLE_POOL_ROUTER",
        "laila.policy.central.memory.router.pool_router",
    ),
    (
        "DefaultMultiBuffer",
        "MultiBuffer",
        "laila.data.multibuffer.multibuffer",
    ),
];

/// The concrete class behind a `Default*` alias, as `(name, module)`.
pub fn default_class(alias: &str) -> Option<(&'static str, &'static str)> {
    DEFAULT_CLASS_ALIASES
        .iter()
        .find(|(a, _, _)| *a == alias)
        .map(|(_, n, m)| (*n, *m))
}

/// `__name__` of the Python module.
pub const __name__: &str = "laila.macros.defaults";
/// `__doc__` of the Python module.
pub const __doc__: &str = "Default implementations and compile-time constants for laila.";

/// `AUTO_INITIALIZE_POLICY` as a Python value.
pub fn auto_initialize_policy_arg() -> Arg {
    AUTO_INITIALIZE_POLICY.into_arg()
}

/// `str(_DEFAULT_ROOT)` helper for callers that want the root only.
pub fn default_root() -> String {
    _DEFAULT_ROOT().to_string()
}
