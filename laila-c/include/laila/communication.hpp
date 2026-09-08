// central.communication protocols (policy/central/communication/*). laila ships
// a TCP/IP protocol; laila-C generalizes to a broad, pluggable set of link
// types. Every protocol shares one CommProtocol base (identity + connect/send/
// recv/close over a HAL Connection). A type with no HAL backend on the target
// raises Status::Unsupported on connect. Loopback is always available
// (in-process) so the layer is testable everywhere, incl. baremetal.
#ifndef LAILA_COMMUNICATION_HPP
#define LAILA_COMMUNICATION_HPP

#include <memory>
#include <string>
#include <vector>

#include "laila/cli_capable.hpp"
#include "laila/hal/hal.hpp"
#include "laila/identity.hpp"

namespace laila_c {

namespace ws { class WsSession; }
class StreamFramer;

using hal::ConnectionConfig;
using hal::ConnectionType;

class _LAILA_IDENTIFIABLE_COMM_PROTOCOL : public _LAILA_CLI_CAPABLE_CLASS,
                                         public _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
public:
  _LAILA_IDENTIFIABLE_COMM_PROTOCOL(ConnectionType type, ConnectionConfig cfg)
      : type_(type), config_(std::move(cfg)) {
    scopes_ = {scope::COMM_PROTOCOL};
  }
  ~_LAILA_IDENTIFIABLE_COMM_PROTOCOL() override = default;

  ConnectionType type() const { return type_; }
  const char* type_name() const { return hal::connection_type_name(type_); }
  virtual const char* name() const = 0;  // laila-style protocol name
  const ConnectionConfig& config() const { return config_; }
  ConnectionConfig& config() { return config_; }

  // peer_secret_key mirrors laila's protocol field: a remote peer must present a
  // matching secret during the peer.connect handshake (empty => accept any).
  const std::string& peer_secret_key() const { return config_.secret; }
  // OS-assigned listener port after add_connection() opens an inbound listener
  // (mirrors laila's bound_port; falls back to the configured port).
  uint16_t bound_port() const { return bound_port_ != 0 ? bound_port_ : config_.port; }
  void set_bound_port(uint16_t p) { bound_port_ = p; }

  // Open the link via the HAL transport. Raises Status::Unsupported if the
  // active platform has no backend for this connection type.
  virtual void connect();
  virtual bool send(const std::vector<uint8_t>& data);
  bool send_text(const std::string& s) { return send({s.begin(), s.end()}); }
  virtual bool recv(std::vector<uint8_t>& out, int timeout_ms = 1000);
  virtual void close();
  virtual bool is_open() const { return conn_ != nullptr && conn_->is_open(); }
  // A persistent transport keeps one connection open across many RPC frames
  // (WebSocket). One-shot transports (raw TCP) reconnect per request, matching
  // the inbound one-frame-per-connection server path.
  virtual bool persistent() const { return false; }

protected:
  ConnectionType type_;
  ConnectionConfig config_;
  std::unique_ptr<hal::Connection> conn_;
  uint16_t bound_port_ = 0;
};
// Compatibility spelling; the faithful (Python) name above is primary.
using CommProtocol = _LAILA_IDENTIFIABLE_COMM_PROTOCOL;
using CommProtocolPtr = std::shared_ptr<_LAILA_IDENTIFIABLE_COMM_PROTOCOL>;

// Generic named protocol with a config constructor (covers every type).
#define LAILA_DECLARE_PROTOCOL(ClassName, Kind, NameStr)                    \
  class ClassName : public CommProtocol {                                   \
   public:                                                                  \
    explicit ClassName(ConnectionConfig cfg = {})                          \
        : CommProtocol(ConnectionType::Kind, std::move(cfg)) {}            \
    const char* name() const override { return NameStr; }                  \
  }

// --- Application/IP protocols with friendly constructors ---
// Stream carrier (TCP): length-prefixed JSON-RPC frames + laila's peer.connect
// handshake, byte-compatible with Python laila's tcp:// transport. send()/recv()
// carry one framed message each; the connection is persistent.
class _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL : public _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
 public:
  explicit _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(ConnectionConfig cfg = {});
  _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(std::string host, uint16_t port, std::string peer_secret_key = "");
  ~_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL() override;
  const char* name() const override { return "tcpip"; }
  void connect() override;
  bool send(const std::vector<uint8_t>& data) override;       // one length-prefixed frame
  bool recv(std::vector<uint8_t>& out, int timeout_ms = 1000) override;
  bool persistent() const override { return true; }
  // peer.connect handshake; returns the remote policy's global_id.
  std::string peer_connect(const std::string& from_id, const std::string& secret);

 private:
  std::unique_ptr<StreamFramer> framer_;
};
// Compatibility spelling + laila Default aliases (macros/defaults.py:
// DefaultTCPIPProtocol == the tcpip stream transport).
using TCPIPProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL;
using DefaultTCPIPProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL;

// Raw-TCP transport (laila's lighter tcp:// transport, distinct class from the
// tcpip stream). Reuses the stream carrier; name() reports "tcp".
class _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL : public _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL {
 public:
  using _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL;
  const char* name() const override { return "tcp"; }
};
using DefaultTCPProtocol = _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL;
class UDPProtocol : public CommProtocol {
 public:
  explicit UDPProtocol(ConnectionConfig cfg = {}) : CommProtocol(ConnectionType::UDP, std::move(cfg)) {}
  UDPProtocol(std::string host, uint16_t port) : CommProtocol(ConnectionType::UDP, {}) {
    config_.host = std::move(host); config_.port = port;
  }
  const char* name() const override { return "udp"; }
};
class MQTTProtocol : public CommProtocol {
 public:
  explicit MQTTProtocol(ConnectionConfig cfg = {}) : CommProtocol(ConnectionType::MQTT, std::move(cfg)) {}
  MQTTProtocol(std::string host, uint16_t port, std::string topic)
      : CommProtocol(ConnectionType::MQTT, {}) {
    config_.host = std::move(host); config_.port = port; config_.topic = std::move(topic);
  }
  const char* name() const override { return "mqtt"; }
};
class LoRaProtocol : public CommProtocol {
 public:
  explicit LoRaProtocol(ConnectionConfig cfg = {}) : CommProtocol(ConnectionType::LoRa, std::move(cfg)) {}
  LoRaProtocol(uint32_t frequency_hz, uint8_t spreading_factor)
      : CommProtocol(ConnectionType::LoRa, {}) {
    config_.frequency_hz = frequency_hz; config_.spreading_factor = spreading_factor;
  }
  const char* name() const override { return "lora"; }
};
class SerialProtocol : public CommProtocol {
 public:
  explicit SerialProtocol(ConnectionConfig cfg = {}) : CommProtocol(ConnectionType::Serial, std::move(cfg)) {}
  SerialProtocol(std::string device, uint32_t baud) : CommProtocol(ConnectionType::Serial, {}) {
    config_.device = std::move(device); config_.baud = baud;
  }
  const char* name() const override { return "serial"; }
};
class I2CProtocol : public CommProtocol {
 public:
  explicit I2CProtocol(ConnectionConfig cfg = {}) : CommProtocol(ConnectionType::I2C, std::move(cfg)) {}
  I2CProtocol(std::string device, uint8_t address) : CommProtocol(ConnectionType::I2C, {}) {
    config_.device = std::move(device); config_.address = address;
  }
  const char* name() const override { return "i2c"; }
};

// Real RFC6455 WebSocket client: opens TCP, performs the WS handshake, then
// speaks laila's peer.connect/rpc.call JSON-RPC over a persistent frame stream.
// This is the transport that interoperates with unmodified Python `laila`
// (ws://). send()/recv() carry one WebSocket text frame each.
class WebSocketProtocol : public CommProtocol {
 public:
  explicit WebSocketProtocol(ConnectionConfig cfg = {});
  WebSocketProtocol(std::string host, uint16_t port, std::string secret = "");
  ~WebSocketProtocol() override;
  const char* name() const override { return "websocket"; }
  void connect() override;                                   // TCP + WS handshake
  bool send(const std::vector<uint8_t>& data) override;      // one WS text frame
  bool recv(std::vector<uint8_t>& out, int timeout_ms = 1000) override;
  void close() override;
  bool is_open() const override;
  bool persistent() const override { return true; }  // long-lived WS session
  // peer.connect handshake (sends from_id + secret); returns the remote policy's
  // global_id. Raises on rejection/timeout.
  std::string peer_connect(const std::string& from_id, const std::string& secret);

 private:
  std::unique_ptr<ws::WsSession> session_;
};

// --- The rest, declared via the macro (config constructor) ---
LAILA_DECLARE_PROTOCOL(TLSProtocol, TLS, "tls");
LAILA_DECLARE_PROTOCOL(HTTPProtocol, HTTP, "http");
LAILA_DECLARE_PROTOCOL(CoAPProtocol, CoAP, "coap");
LAILA_DECLARE_PROTOCOL(AMQPProtocol, AMQP, "amqp");
LAILA_DECLARE_PROTOCOL(GRPCProtocol, GRPC, "grpc");
LAILA_DECLARE_PROTOCOL(LoRaWANProtocol, LoRaWAN, "lorawan");
LAILA_DECLARE_PROTOCOL(BLEProtocol, BLE, "ble");
LAILA_DECLARE_PROTOCOL(BluetoothClassicProtocol, BluetoothClassic, "bluetooth");
LAILA_DECLARE_PROTOCOL(ZigbeeProtocol, Zigbee, "zigbee");
LAILA_DECLARE_PROTOCOL(ThreadProtocol, Thread, "thread");
LAILA_DECLARE_PROTOCOL(NFCProtocol, NFC, "nfc");
LAILA_DECLARE_PROTOCOL(WiFiDirectProtocol, WiFiDirect, "wifi-direct");
LAILA_DECLARE_PROTOCOL(CellularProtocol, Cellular, "cellular");
LAILA_DECLARE_PROTOCOL(SPIProtocol, SPI, "spi");
LAILA_DECLARE_PROTOCOL(CANProtocol, CAN, "can");
LAILA_DECLARE_PROTOCOL(RS485Protocol, RS485, "rs485");
LAILA_DECLARE_PROTOCOL(EthernetProtocol, Ethernet, "ethernet");

// In-process protocol: always available (no HAL needed). config.uri is the
// local endpoint name, config.host is the peer endpoint to send to.
class LoopbackProtocol : public CommProtocol {
 public:
  explicit LoopbackProtocol(ConnectionConfig cfg = {})
      : CommProtocol(ConnectionType::Loopback, std::move(cfg)) {}
  LoopbackProtocol(std::string local, std::string peer)
      : CommProtocol(ConnectionType::Loopback, {}) {
    config_.uri = std::move(local); config_.host = std::move(peer);
  }
  const char* name() const override { return "loopback"; }
  void connect() override;
  bool send(const std::vector<uint8_t>& data) override;
  bool recv(std::vector<uint8_t>& out, int timeout_ms = 1000) override;
  void close() override;
  bool is_open() const override { return open_; }

 private:
  bool open_ = false;
};

// Factory: build a protocol by type or by URI scheme (tcp://, udp://, mqtt://,
// tls://, ws://, lora://, serial://, loopback://, ...).
CommProtocolPtr make_protocol(ConnectionType type, ConnectionConfig cfg = {});
CommProtocolPtr make_protocol_from_uri(const std::string& uri, const std::string& secret = "");

}  // namespace laila_c

#endif  // LAILA_COMMUNICATION_HPP
