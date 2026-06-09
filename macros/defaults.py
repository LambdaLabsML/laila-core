"""Default implementations and compile-time constants for laila.

Single source of truth for the "what does laila ship with by
default?" question. The aliases at the top of the module name the
*concrete* class that backs each abstract role:

==========================  ==================================================
Default name                Concrete class
==========================  ==================================================
DefaultTaskForce            :class:`PythonAsyncThreadPoolTaskForce`
DefaultCentralCommand       :class:`_LAILA_IDENTIFIABLE_CENTRAL_COMMAND`
DefaultCentralCommunication :class:`_LAILA_IDENTIFIABLE_COMMUNICATION`
DefaultTCPIPProtocol        :class:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`
DefaultLoRaProtocol         :class:`_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL` (scaffold)
DefaultBluetoothProtocol    :class:`_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL` (scaffold)
DefaultCentralMemory        :class:`_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
DefaultPolicy               :class:`_LAILA_IDENTIFIABLE_POLICY`
DefaultPool                 :class:`_LAILA_IDENTIFIABLE_POOL`
DefaultPoolRouter           :class:`_LAILA_IDENTIFIABLE_POOL_ROUTER`
==========================  ==================================================

Other constants:

- :data:`AUTO_INITIALIZE_POLICY` -- whether ``import laila`` should
  spin up a default policy automatically. Toggling this off is
  useful for unit-test rigs that want full control over policy
  lifecycle.
- :data:`LAILA_UNIVERSAL_NAMESPACE` -- the UUID-5 namespace used as
  the *root* of laila's deterministic-id tree. Touch only if you
  know exactly what you're doing -- changing it invalidates every
  nickname-derived id ever produced.
- :data:`LAILA_DEFAULT_DIRECTORIES` -- the on-disk layout under
  ``~/.laila``: per-pool storage under ``pools/``, log files under
  ``logs/``, key material under ``secrets/``, and non-memorizing
  query helpers (e.g. Manifest SQL indices) under ``indices/``. Each
  directory is created at import time if missing.
"""

import os
import uuid

from ..policy.central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ..policy.central.command.taskforce.async_thread_pool_executor import (
    PythonAsyncThreadPoolTaskForce,
)
from ..policy.central.communication.protocols.cellular.cellular import (
    _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.cellular.gsm_gprs import (
    _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.cellular.lte import (
    _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.cellular.nr5g import (
    _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.amqp import (
    _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.coap import (
    _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.dds import (
    _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.dtls import (
    _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.ethernet import (
    _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.grpc import (
    _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.http2 import (
    _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.http3 import (
    _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.modbus_tcp import (
    _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.mqtt import (
    _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.opcua import (
    _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.tcp import (
    _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.tls import (
    _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.udp import (
    _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.xmpp import (
    _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.ip_app.zeromq import (
    _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.local.loopback import (
    _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.local.unixsocket import (
    _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.lora import (
    _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.lorawan import (
    _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.ltem import (
    _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.nbiot import (
    _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.satellite import (
    _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lpwan.sigfox import (
    _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.ant import (
    _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.bluetooth import (
    _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.espnow import (
    _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.irda import (
    _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.matter import (
    _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.nfc import (
    _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.rfid import (
    _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.sixlowpan import (
    _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.thread import (
    _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.uwb import (
    _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.wifi import (
    _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.wifi_direct import (
    _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.zigbee import (
    _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.short_range.zwave import (
    _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
from ..policy.central.communication.protocols.wired.can import (
    _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.enip import (
    _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.ethercat import (
    _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.i2c import (
    _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.i2s import (
    _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.lin import (
    _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.modbus_rtu import (
    _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.one_wire import (
    _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.profinet import (
    _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.rs232 import (
    _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.rs485 import (
    _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.sdio import (
    _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.spi import (
    _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.uart import (
    _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.wired.usb import (
    _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL,
)
from ..policy.central.communication.schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION
from ..policy.central.memory.router.pool_router import _LAILA_IDENTIFIABLE_POOL_ROUTER
from ..policy.central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ..policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from ..pool.schema.base import _LAILA_IDENTIFIABLE_POOL

DefaultTaskForce = PythonAsyncThreadPoolTaskForce
DefaultCentralCommand = _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
DefaultCentralCommunication = _LAILA_IDENTIFIABLE_COMMUNICATION
DefaultTCPIPProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
DefaultWebSocketProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
DefaultTCPProtocol = _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL
DefaultUDPProtocol = _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL
DefaultTLSProtocol = _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL
DefaultEthernetProtocol = _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL
DefaultWiFiProtocol = _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL
DefaultWiFiDirectProtocol = _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL
DefaultCellularProtocol = _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL
DefaultGSMProtocol = _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL
DefaultLTEProtocol = _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL
DefaultNR5GProtocol = _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL
DefaultNBIoTProtocol = _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL
DefaultLTEMProtocol = _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL
DefaultMatterProtocol = _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL
DefaultSigfoxProtocol = _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL
DefaultSatelliteProtocol = _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL
DefaultUSBProtocol = _LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL
DefaultLoRaWANProtocol = _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL
DefaultSixLoWPANProtocol = _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL
DefaultZigbeeProtocol = _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL
DefaultThreadProtocol = _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL
DefaultZWaveProtocol = _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL
DefaultNFCProtocol = _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL
DefaultRFIDProtocol = _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL
DefaultUWBProtocol = _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL
DefaultIrDAProtocol = _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL
DefaultANTProtocol = _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL
DefaultESPNowProtocol = _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL
DefaultUnixSocketProtocol = _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL
DefaultLoopbackProtocol = _LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL
DefaultMQTTProtocol = _LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL
DefaultAMQPProtocol = _LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL
DefaultZeroMQProtocol = _LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL
DefaultUARTProtocol = _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL
DefaultRS232Protocol = _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL
DefaultRS485Protocol = _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL
DefaultCANProtocol = _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL
DefaultModbusTCPProtocol = _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL
DefaultModbusRTUProtocol = _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL
DefaultI2CProtocol = _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL
DefaultSPIProtocol = _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL
DefaultENIPProtocol = _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL
DefaultLINProtocol = _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL
DefaultOneWireProtocol = _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL
DefaultI2SProtocol = _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL
DefaultSDIOProtocol = _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL
DefaultProfinetProtocol = _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL
DefaultEtherCATProtocol = _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL
DefaultCoAPProtocol = _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL
DefaultHTTP2Protocol = _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL
DefaultHTTP3Protocol = _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL
DefaultDTLSProtocol = _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL
DefaultXMPPProtocol = _LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL
DefaultDDSProtocol = _LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL
DefaultOPCUAProtocol = _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL
DefaultGRPCProtocol = _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL
DefaultLoRaProtocol = _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL
DefaultBluetoothProtocol = _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL
DefaultCentralMemory = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY
DefaultPool = _LAILA_IDENTIFIABLE_POOL
DefaultPoolRouter = _LAILA_IDENTIFIABLE_POOL_ROUTER


AUTO_INITIALIZE_POLICY = True

# ============================================================
# DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING
LAILA_UNIVERSAL_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "laila")
# ============================================================

_DEFAULT_ROOT = os.path.expanduser("~/.laila")

LAILA_DEFAULT_DIRECTORIES = {
    "root": _DEFAULT_ROOT,
    "pools": os.path.join(_DEFAULT_ROOT, "pools"),
    "logs": os.path.join(_DEFAULT_ROOT, "logs"),
    "secrets": os.path.join(_DEFAULT_ROOT, "secrets"),
    "indices": os.path.join(_DEFAULT_ROOT, "indices"),
}

for _dir in LAILA_DEFAULT_DIRECTORIES.values():
    os.makedirs(_dir, exist_ok=True)
