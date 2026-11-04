//! Wired field buses and serial links (UART, RS-232/485, USB-CDC, CAN, I2C, SPI, Modbus, ...).
//!
//! Python: `laila.policy.central.communication.protocols.wired`.

pub mod can;
pub mod enip;
pub mod ethercat;
pub mod i2c;
pub mod i2s;
pub mod lin;
pub mod modbus_rtu;
pub mod one_wire;
pub mod profinet;
pub mod rs232;
pub mod rs485;
pub mod sdio;
pub mod spi;
pub mod uart;
pub mod usb;

pub use can::_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL;
pub use enip::_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL;
pub use ethercat::_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL;
pub use i2c::_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL;
pub use i2s::_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL;
pub use lin::_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL;
pub use modbus_rtu::_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL;
pub use one_wire::_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL;
pub use profinet::_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL;
pub use rs232::_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL;
pub use rs485::_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL;
pub use sdio::_LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL;
pub use spi::_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL;
pub use uart::_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL;
pub use usb::_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL;
