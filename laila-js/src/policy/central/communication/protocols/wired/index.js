/**
 * Wired / bus transports (board + field level).
 *
 * Working serial-line transports (UART, RS-232, RS-485) on the
 * point-to-point stream carrier, plus the CAN/CAN-FD transport (ISO-TP over
 * SocketCAN). Drivers are imported lazily so importing laila never requires
 * them.
 */
export { _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL } from "./can.js";
export { _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL } from "./enip.js";
export { _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL } from "./i2c.js";
export { _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL } from "./modbus_rtu.js";
export { _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL } from "./rs232.js";
export { _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL } from "./rs485.js";
export { _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL } from "./spi.js";
export { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL } from "./uart.js";
export { _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL } from "./usb.js";
// Autoloaded transports (Python's ``_autoload_transports`` import walk).
export { _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL } from "./ethercat.js";
export { _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL } from "./i2s.js";
export { _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL } from "./lin.js";
export { _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL } from "./one_wire.js";
export { _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL } from "./profinet.js";
export { _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL } from "./sdio.js";

export const __all__ = Object.freeze([
  "_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL",
  "_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL",
]);
