package laila

import (
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// macros/defaults.py -- default implementations and compile-time constants.
//
// The ``Default*`` aliases name the concrete class that backs each abstract
// role. They are class descriptors (``*py.Class``) so that
// ``DefaultPolicy()`` in Python is ``New_DefaultPolicy()`` here and
// ``isinstance(x, DefaultPool)`` is ``py.Isinstance(x, DefaultPool)``.
// Each alias is assigned in the file that declares its class (later
// phases); the ones below are the roots every other module relies on.

// AUTO_INITIALIZE_POLICY -- whether ``import laila`` should spin up a default
// policy automatically.
var AUTO_INITIALIZE_POLICY = true

// ============================================================
// DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING
var LAILA_UNIVERSAL_NAMESPACE = py.Uuid5(py.NAMESPACE_DNS, "laila")

// ============================================================

var DEFAULT_ROOT_ = py.Expanduser("~/.laila")

// LAILA_DEFAULT_DIRECTORIES is the on-disk layout under ``~/.laila``. Each
// directory is created at import (package init) time if missing.
var LAILA_DEFAULT_DIRECTORIES = map[string]string{
	"root":    DEFAULT_ROOT_,
	"pools":   filepath.Join(DEFAULT_ROOT_, "pools"),
	"logs":    filepath.Join(DEFAULT_ROOT_, "logs"),
	"secrets": filepath.Join(DEFAULT_ROOT_, "secrets"),
	"indices": filepath.Join(DEFAULT_ROOT_, "indices"),
}

func init() {
	for _, dir := range LAILA_DEFAULT_DIRECTORIES {
		_ = py.Mkdir_p(dir)
	}
}

// ------------------------------------------------------------------
// Default*Protocol aliases (macros/defaults.py lines 229-285).
//
// ``DefaultTCPIPProtocol`` / ``DefaultWebSocketProtocol`` live next to
// their class in policy.central.communication.protocols.tcpip.go; every
// other transport alias is declared here with a matching
// ``New_Default*Protocol`` constructor so ``DefaultXProtocol(**kw)`` in
// Python is ``New_DefaultXProtocol(kw)`` in Go.
// ------------------------------------------------------------------

var (
	DefaultTCPProtocol = LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class
	DefaultUDPProtocol = LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class
	DefaultTLSProtocol = LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_class
	DefaultEthernetProtocol = LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_class
	DefaultWiFiProtocol = LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_class
	DefaultWiFiDirectProtocol = LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_class
	DefaultCellularProtocol = LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class
	DefaultGSMProtocol = LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_class
	DefaultLTEProtocol = LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_class
	DefaultNR5GProtocol = LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_class
	DefaultNBIoTProtocol = LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_class
	DefaultLTEMProtocol = LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL_class
	DefaultMatterProtocol = LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_class
	DefaultSigfoxProtocol = LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_class
	DefaultSatelliteProtocol = LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_class
	DefaultUSBProtocol = LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_class
	DefaultLoRaWANProtocol = LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL_class
	DefaultSixLoWPANProtocol = LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_class
	DefaultZigbeeProtocol = LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_class
	DefaultThreadProtocol = LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_class
	DefaultZWaveProtocol = LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_class
	DefaultNFCProtocol = LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL_class
	DefaultRFIDProtocol = LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_class
	DefaultUWBProtocol = LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_class
	DefaultIrDAProtocol = LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL_class
	DefaultANTProtocol = LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_class
	DefaultESPNowProtocol = LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_class
	DefaultUnixSocketProtocol = LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class
	DefaultLoopbackProtocol = LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class
	DefaultMQTTProtocol = LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_class
	DefaultAMQPProtocol = LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_class
	DefaultZeroMQProtocol = LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_class
	DefaultUARTProtocol = LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class
	DefaultRS232Protocol = LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_class
	DefaultRS485Protocol = LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_class
	DefaultCANProtocol = LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_class
	DefaultModbusTCPProtocol = LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_class
	DefaultModbusRTUProtocol = LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_class
	DefaultI2CProtocol = LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_class
	DefaultSPIProtocol = LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_class
	DefaultENIPProtocol = LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_class
	DefaultLINProtocol = LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_class
	DefaultOneWireProtocol = LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_class
	DefaultI2SProtocol = LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL_class
	DefaultSDIOProtocol = LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL_class
	DefaultProfinetProtocol = LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL_class
	DefaultEtherCATProtocol = LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_class
	DefaultCoAPProtocol = LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_class
	DefaultHTTP2Protocol = LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_class
	DefaultHTTP3Protocol = LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_class
	DefaultDTLSProtocol = LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_class
	DefaultXMPPProtocol = LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_class
	DefaultDDSProtocol = LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_class
	DefaultOPCUAProtocol = LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_class
	DefaultGRPCProtocol = LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_class
	DefaultLoRaProtocol = LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_class
	DefaultBluetoothProtocol = LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_class
)

// New_DefaultTCPProtocol is ``DefaultTCPProtocol(**kw)``.
func New_DefaultTCPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_(kw...)
}

// New_DefaultUDPProtocol is ``DefaultUDPProtocol(**kw)``.
func New_DefaultUDPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_(kw...)
}

// New_DefaultTLSProtocol is ``DefaultTLSProtocol(**kw)``.
func New_DefaultTLSProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_(kw...)
}

// New_DefaultEthernetProtocol is ``DefaultEthernetProtocol(**kw)``.
func New_DefaultEthernetProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_(kw...)
}

// New_DefaultWiFiProtocol is ``DefaultWiFiProtocol(**kw)``.
func New_DefaultWiFiProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_(kw...)
}

// New_DefaultWiFiDirectProtocol is ``DefaultWiFiDirectProtocol(**kw)``.
func New_DefaultWiFiDirectProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_(kw...)
}

// New_DefaultCellularProtocol is ``DefaultCellularProtocol(**kw)``.
func New_DefaultCellularProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_(kw...)
}

// New_DefaultGSMProtocol is ``DefaultGSMProtocol(**kw)``.
func New_DefaultGSMProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_(kw...)
}

// New_DefaultLTEProtocol is ``DefaultLTEProtocol(**kw)``.
func New_DefaultLTEProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_(kw...)
}

// New_DefaultNR5GProtocol is ``DefaultNR5GProtocol(**kw)``.
func New_DefaultNR5GProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_(kw...)
}

// New_DefaultNBIoTProtocol is ``DefaultNBIoTProtocol(**kw)``.
func New_DefaultNBIoTProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_(kw...)
}

// New_DefaultLTEMProtocol is ``DefaultLTEMProtocol(**kw)``.
func New_DefaultLTEMProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL_(kw...)
}

// New_DefaultMatterProtocol is ``DefaultMatterProtocol(**kw)``.
func New_DefaultMatterProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_(kw...)
}

// New_DefaultSigfoxProtocol is ``DefaultSigfoxProtocol(**kw)``.
func New_DefaultSigfoxProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_(kw...)
}

// New_DefaultSatelliteProtocol is ``DefaultSatelliteProtocol(**kw)``.
func New_DefaultSatelliteProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_(kw...)
}

// New_DefaultUSBProtocol is ``DefaultUSBProtocol(**kw)``.
func New_DefaultUSBProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_(kw...)
}

// New_DefaultLoRaWANProtocol is ``DefaultLoRaWANProtocol(**kw)``.
func New_DefaultLoRaWANProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL_(kw...)
}

// New_DefaultSixLoWPANProtocol is ``DefaultSixLoWPANProtocol(**kw)``.
func New_DefaultSixLoWPANProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_(kw...)
}

// New_DefaultZigbeeProtocol is ``DefaultZigbeeProtocol(**kw)``.
func New_DefaultZigbeeProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_(kw...)
}

// New_DefaultThreadProtocol is ``DefaultThreadProtocol(**kw)``.
func New_DefaultThreadProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_(kw...)
}

// New_DefaultZWaveProtocol is ``DefaultZWaveProtocol(**kw)``.
func New_DefaultZWaveProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_(kw...)
}

// New_DefaultNFCProtocol is ``DefaultNFCProtocol(**kw)``.
func New_DefaultNFCProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL_(kw...)
}

// New_DefaultRFIDProtocol is ``DefaultRFIDProtocol(**kw)``.
func New_DefaultRFIDProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_(kw...)
}

// New_DefaultUWBProtocol is ``DefaultUWBProtocol(**kw)``.
func New_DefaultUWBProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_(kw...)
}

// New_DefaultIrDAProtocol is ``DefaultIrDAProtocol(**kw)``.
func New_DefaultIrDAProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL_(kw...)
}

// New_DefaultANTProtocol is ``DefaultANTProtocol(**kw)``.
func New_DefaultANTProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_(kw...)
}

// New_DefaultESPNowProtocol is ``DefaultESPNowProtocol(**kw)``.
func New_DefaultESPNowProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_(kw...)
}

// New_DefaultUnixSocketProtocol is ``DefaultUnixSocketProtocol(**kw)``.
func New_DefaultUnixSocketProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_(kw...)
}

// New_DefaultLoopbackProtocol is ``DefaultLoopbackProtocol(**kw)``.
func New_DefaultLoopbackProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_(kw...)
}

// New_DefaultMQTTProtocol is ``DefaultMQTTProtocol(**kw)``.
func New_DefaultMQTTProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_(kw...)
}

// New_DefaultAMQPProtocol is ``DefaultAMQPProtocol(**kw)``.
func New_DefaultAMQPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_(kw...)
}

// New_DefaultZeroMQProtocol is ``DefaultZeroMQProtocol(**kw)``.
func New_DefaultZeroMQProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_(kw...)
}

// New_DefaultUARTProtocol is ``DefaultUARTProtocol(**kw)``.
func New_DefaultUARTProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_(kw...)
}

// New_DefaultRS232Protocol is ``DefaultRS232Protocol(**kw)``.
func New_DefaultRS232Protocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_(kw...)
}

// New_DefaultRS485Protocol is ``DefaultRS485Protocol(**kw)``.
func New_DefaultRS485Protocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_(kw...)
}

// New_DefaultCANProtocol is ``DefaultCANProtocol(**kw)``.
func New_DefaultCANProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_(kw...)
}

// New_DefaultModbusTCPProtocol is ``DefaultModbusTCPProtocol(**kw)``.
func New_DefaultModbusTCPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_(kw...)
}

// New_DefaultModbusRTUProtocol is ``DefaultModbusRTUProtocol(**kw)``.
func New_DefaultModbusRTUProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_(kw...)
}

// New_DefaultI2CProtocol is ``DefaultI2CProtocol(**kw)``.
func New_DefaultI2CProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_(kw...)
}

// New_DefaultSPIProtocol is ``DefaultSPIProtocol(**kw)``.
func New_DefaultSPIProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_(kw...)
}

// New_DefaultENIPProtocol is ``DefaultENIPProtocol(**kw)``.
func New_DefaultENIPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_(kw...)
}

// New_DefaultLINProtocol is ``DefaultLINProtocol(**kw)``.
func New_DefaultLINProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_(kw...)
}

// New_DefaultOneWireProtocol is ``DefaultOneWireProtocol(**kw)``.
func New_DefaultOneWireProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_(kw...)
}

// New_DefaultI2SProtocol is ``DefaultI2SProtocol(**kw)``.
func New_DefaultI2SProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL_(kw...)
}

// New_DefaultSDIOProtocol is ``DefaultSDIOProtocol(**kw)``.
func New_DefaultSDIOProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL_(kw...)
}

// New_DefaultProfinetProtocol is ``DefaultProfinetProtocol(**kw)``.
func New_DefaultProfinetProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL_(kw...)
}

// New_DefaultEtherCATProtocol is ``DefaultEtherCATProtocol(**kw)``.
func New_DefaultEtherCATProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_(kw...)
}

// New_DefaultCoAPProtocol is ``DefaultCoAPProtocol(**kw)``.
func New_DefaultCoAPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_(kw...)
}

// New_DefaultHTTP2Protocol is ``DefaultHTTP2Protocol(**kw)``.
func New_DefaultHTTP2Protocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_(kw...)
}

// New_DefaultHTTP3Protocol is ``DefaultHTTP3Protocol(**kw)``.
func New_DefaultHTTP3Protocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_(kw...)
}

// New_DefaultDTLSProtocol is ``DefaultDTLSProtocol(**kw)``.
func New_DefaultDTLSProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_(kw...)
}

// New_DefaultXMPPProtocol is ``DefaultXMPPProtocol(**kw)``.
func New_DefaultXMPPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_(kw...)
}

// New_DefaultDDSProtocol is ``DefaultDDSProtocol(**kw)``.
func New_DefaultDDSProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_(kw...)
}

// New_DefaultOPCUAProtocol is ``DefaultOPCUAProtocol(**kw)``.
func New_DefaultOPCUAProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_(kw...)
}

// New_DefaultGRPCProtocol is ``DefaultGRPCProtocol(**kw)``.
func New_DefaultGRPCProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_(kw...)
}

// New_DefaultLoRaProtocol is ``DefaultLoRaProtocol(**kw)``.
func New_DefaultLoRaProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_(kw...)
}

// New_DefaultBluetoothProtocol is ``DefaultBluetoothProtocol(**kw)``.
func New_DefaultBluetoothProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_(kw...)
}

// Macros_defaults_protocols is ``{name: getattr(defaults, name) for name in
// dir(defaults) if name.startswith("Default") and name.endswith("Protocol")}``
// -- the alias -> class table tests walk exactly like the module namespace.
var Macros_defaults_protocols = map[string]*py.Class{
	"DefaultTCPIPProtocol": DefaultTCPIPProtocol,
	"DefaultWebSocketProtocol": DefaultWebSocketProtocol,
	"DefaultTCPProtocol": DefaultTCPProtocol,
	"DefaultUDPProtocol": DefaultUDPProtocol,
	"DefaultTLSProtocol": DefaultTLSProtocol,
	"DefaultEthernetProtocol": DefaultEthernetProtocol,
	"DefaultWiFiProtocol": DefaultWiFiProtocol,
	"DefaultWiFiDirectProtocol": DefaultWiFiDirectProtocol,
	"DefaultCellularProtocol": DefaultCellularProtocol,
	"DefaultGSMProtocol": DefaultGSMProtocol,
	"DefaultLTEProtocol": DefaultLTEProtocol,
	"DefaultNR5GProtocol": DefaultNR5GProtocol,
	"DefaultNBIoTProtocol": DefaultNBIoTProtocol,
	"DefaultLTEMProtocol": DefaultLTEMProtocol,
	"DefaultMatterProtocol": DefaultMatterProtocol,
	"DefaultSigfoxProtocol": DefaultSigfoxProtocol,
	"DefaultSatelliteProtocol": DefaultSatelliteProtocol,
	"DefaultUSBProtocol": DefaultUSBProtocol,
	"DefaultLoRaWANProtocol": DefaultLoRaWANProtocol,
	"DefaultSixLoWPANProtocol": DefaultSixLoWPANProtocol,
	"DefaultZigbeeProtocol": DefaultZigbeeProtocol,
	"DefaultThreadProtocol": DefaultThreadProtocol,
	"DefaultZWaveProtocol": DefaultZWaveProtocol,
	"DefaultNFCProtocol": DefaultNFCProtocol,
	"DefaultRFIDProtocol": DefaultRFIDProtocol,
	"DefaultUWBProtocol": DefaultUWBProtocol,
	"DefaultIrDAProtocol": DefaultIrDAProtocol,
	"DefaultANTProtocol": DefaultANTProtocol,
	"DefaultESPNowProtocol": DefaultESPNowProtocol,
	"DefaultUnixSocketProtocol": DefaultUnixSocketProtocol,
	"DefaultLoopbackProtocol": DefaultLoopbackProtocol,
	"DefaultMQTTProtocol": DefaultMQTTProtocol,
	"DefaultAMQPProtocol": DefaultAMQPProtocol,
	"DefaultZeroMQProtocol": DefaultZeroMQProtocol,
	"DefaultUARTProtocol": DefaultUARTProtocol,
	"DefaultRS232Protocol": DefaultRS232Protocol,
	"DefaultRS485Protocol": DefaultRS485Protocol,
	"DefaultCANProtocol": DefaultCANProtocol,
	"DefaultModbusTCPProtocol": DefaultModbusTCPProtocol,
	"DefaultModbusRTUProtocol": DefaultModbusRTUProtocol,
	"DefaultI2CProtocol": DefaultI2CProtocol,
	"DefaultSPIProtocol": DefaultSPIProtocol,
	"DefaultENIPProtocol": DefaultENIPProtocol,
	"DefaultLINProtocol": DefaultLINProtocol,
	"DefaultOneWireProtocol": DefaultOneWireProtocol,
	"DefaultI2SProtocol": DefaultI2SProtocol,
	"DefaultSDIOProtocol": DefaultSDIOProtocol,
	"DefaultProfinetProtocol": DefaultProfinetProtocol,
	"DefaultEtherCATProtocol": DefaultEtherCATProtocol,
	"DefaultCoAPProtocol": DefaultCoAPProtocol,
	"DefaultHTTP2Protocol": DefaultHTTP2Protocol,
	"DefaultHTTP3Protocol": DefaultHTTP3Protocol,
	"DefaultDTLSProtocol": DefaultDTLSProtocol,
	"DefaultXMPPProtocol": DefaultXMPPProtocol,
	"DefaultDDSProtocol": DefaultDDSProtocol,
	"DefaultOPCUAProtocol": DefaultOPCUAProtocol,
	"DefaultGRPCProtocol": DefaultGRPCProtocol,
	"DefaultLoRaProtocol": DefaultLoRaProtocol,
	"DefaultBluetoothProtocol": DefaultBluetoothProtocol,
}
