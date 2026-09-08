#include "laila/communication.hpp"

#include <deque>
#include <map>
#include <memory>

#include "laila/framing.hpp"
#include "laila/identity.hpp"
#include "laila/json.hpp"
#include "laila/status.hpp"
#include "laila/websocket.hpp"

namespace laila_c {
namespace hal {

const char* connection_type_name(ConnectionType t) {
  switch (t) {
    case ConnectionType::Loopback: return "loopback";
    case ConnectionType::TCP: return "tcp";
    case ConnectionType::UDP: return "udp";
    case ConnectionType::TLS: return "tls";
    case ConnectionType::WebSocket: return "websocket";
    case ConnectionType::HTTP: return "http";
    case ConnectionType::MQTT: return "mqtt";
    case ConnectionType::CoAP: return "coap";
    case ConnectionType::AMQP: return "amqp";
    case ConnectionType::GRPC: return "grpc";
    case ConnectionType::LoRa: return "lora";
    case ConnectionType::LoRaWAN: return "lorawan";
    case ConnectionType::BLE: return "ble";
    case ConnectionType::BluetoothClassic: return "bluetooth";
    case ConnectionType::Zigbee: return "zigbee";
    case ConnectionType::Thread: return "thread";
    case ConnectionType::NFC: return "nfc";
    case ConnectionType::WiFiDirect: return "wifi-direct";
    case ConnectionType::Cellular: return "cellular";
    case ConnectionType::Serial: return "serial";
    case ConnectionType::I2C: return "i2c";
    case ConnectionType::SPI: return "spi";
    case ConnectionType::CAN: return "can";
    case ConnectionType::RS485: return "rs485";
    case ConnectionType::Ethernet: return "ethernet";
  }
  return "unknown";
}

}  // namespace hal

// ---------------- CommProtocol (HAL-backed) ----------------
void CommProtocol::connect() {
  if (is_open()) return;
  hal::Transport& tr = hal::get().transport();
  if (!tr.supports(type_))
    raise(Status::Unsupported, std::string(name()) + " (" + type_name() +
                                   ") connection is not supported on this target");
  hal::Connection* c = tr.open(type_, config_);
  if (c == nullptr)
    raise(Status::Error, std::string("failed to open ") + name() + " connection to " +
                             (config_.uri.empty() ? config_.host : config_.uri));
  conn_.reset(c);
}

bool CommProtocol::send(const std::vector<uint8_t>& data) {
  if (!is_open()) connect();
  return conn_->send(data);
}

bool CommProtocol::recv(std::vector<uint8_t>& out, int timeout_ms) {
  if (!is_open()) connect();
  return conn_->recv(out, timeout_ms);
}

void CommProtocol::close() {
  if (conn_) conn_->close();
  conn_.reset();
}

// ---------------- TCPIPProtocol (stream carrier: length-prefixed JSON-RPC) ----------------
_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(ConnectionConfig cfg)
    : CommProtocol(ConnectionType::TCP, std::move(cfg)) {}

_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(std::string host, uint16_t port, std::string secret)
    : CommProtocol(ConnectionType::TCP, {}) {
  config_.host = std::move(host);
  config_.port = port;
  config_.secret = std::move(secret);
}

_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL::~_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL() = default;

void TCPIPProtocol::connect() {
  if (is_open()) return;
  hal::Transport& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP))
    raise(Status::Unsupported, "tcp/ip is not supported on this target");
  hal::Connection* c = tr.open(ConnectionType::TCP, config_);
  if (c == nullptr)
    raise(Status::Error, "failed to open TCP connection to " + config_.host);
  conn_.reset(c);
  framer_ = std::make_unique<StreamFramer>(conn_.get());
}

bool TCPIPProtocol::send(const std::vector<uint8_t>& data) {
  if (!is_open()) connect();
  return framer_ && framer_->send(std::string(data.begin(), data.end()));
}

bool TCPIPProtocol::recv(std::vector<uint8_t>& out, int timeout_ms) {
  if (!framer_) return false;
  std::string msg;
  if (!framer_->recv(msg, timeout_ms)) return false;
  out.assign(msg.begin(), msg.end());
  return true;
}

std::string TCPIPProtocol::peer_connect(const std::string& from_id, const std::string& secret) {
  if (!is_open()) connect();
  Json req = Json::object();
  req["jsonrpc"] = std::string("2.0");
  req["id"] = uuid4();
  req["method"] = std::string("peer.connect");
  Json params = Json::object();
  params["from_id"] = from_id;
  params["secret"] = secret;
  req["params"] = params;
  if (!framer_->send(req.dump())) raise(Status::Error, "peer.connect send failed");
  std::string msg;
  if (!framer_->recv(msg, 10000)) raise(Status::Timeout, "peer.connect timed out");
  Json r = Json::parse(msg);
  if (r.contains("error") && !r.at("error").is_null())
    raise(Status::Error, "peer rejected connection: " + r.at("error").dump());
  return r.at("result").at("peer_id").as_string();
}

// ---------------- LoopbackProtocol (in-process mailboxes) ----------------
namespace {
using Mailbox = std::deque<std::vector<uint8_t>>;
std::map<std::string, Mailbox>& mailboxes() {
  static std::map<std::string, Mailbox> m;
  return m;
}
}  // namespace

void LoopbackProtocol::connect() {
  hal::Guard g;
  mailboxes()[config_.uri];   // ensure local endpoint exists
  mailboxes()[config_.host];  // ensure peer endpoint exists
  open_ = true;
}

bool LoopbackProtocol::send(const std::vector<uint8_t>& data) {
  if (!open_) connect();
  hal::Guard g;
  mailboxes()[config_.host].push_back(data);  // deliver to peer's inbox
  return true;
}

bool LoopbackProtocol::recv(std::vector<uint8_t>& out, int timeout_ms) {
  if (!open_) connect();
  uint64_t deadline = hal::get().clock().now_ms() + (timeout_ms < 0 ? 0 : (uint64_t)timeout_ms);
  for (;;) {
    {
      hal::Guard g;
      Mailbox& mb = mailboxes()[config_.uri];
      if (!mb.empty()) {
        out = mb.front();
        mb.pop_front();
        return true;
      }
    }
    if (timeout_ms == 0 || hal::get().clock().now_ms() >= deadline) return false;
    hal::get().clock().sleep_ms(1);
  }
}

void LoopbackProtocol::close() { open_ = false; }

// ---------------- WebSocketProtocol (RFC6455 client over HAL TCP) ----------------
WebSocketProtocol::WebSocketProtocol(ConnectionConfig cfg)
    : CommProtocol(ConnectionType::WebSocket, std::move(cfg)) {}

WebSocketProtocol::WebSocketProtocol(std::string host, uint16_t port, std::string secret)
    : CommProtocol(ConnectionType::WebSocket, {}) {
  config_.host = std::move(host);
  config_.port = port;
  config_.secret = std::move(secret);
}

WebSocketProtocol::~WebSocketProtocol() = default;

void WebSocketProtocol::connect() {
  if (is_open()) return;
  hal::Transport& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP))
    raise(Status::Unsupported, "websocket requires a TCP transport on this target");
  hal::Connection* c = tr.open(ConnectionType::TCP, config_);
  if (c == nullptr)
    raise(Status::Error, "failed to open TCP socket for websocket to " + config_.host);
  conn_.reset(c);
  session_ = std::make_unique<ws::WsSession>(conn_.get(), /*is_client=*/true);
  std::string path = config_.topic.empty() ? "/" : ("/" + config_.topic);
  if (!session_->client_handshake(config_.host, config_.port, path)) {
    conn_.reset();
    session_.reset();
    raise(Status::Error, "websocket handshake failed to " + config_.host);
  }
}

bool WebSocketProtocol::send(const std::vector<uint8_t>& data) {
  if (!is_open()) connect();
  return session_ && session_->send_text(std::string(data.begin(), data.end()));
}

bool WebSocketProtocol::recv(std::vector<uint8_t>& out, int timeout_ms) {
  if (!session_) return false;
  std::string msg;
  if (!session_->recv_text(msg, timeout_ms)) return false;
  out.assign(msg.begin(), msg.end());
  return true;
}

void WebSocketProtocol::close() {
  if (session_) session_->close();
  session_.reset();
  if (conn_) conn_->close();
  conn_.reset();
}

bool WebSocketProtocol::is_open() const { return conn_ != nullptr && conn_->is_open(); }

std::string WebSocketProtocol::peer_connect(const std::string& from_id,
                                            const std::string& secret) {
  if (!is_open()) connect();
  Json req = Json::object();
  req["jsonrpc"] = std::string("2.0");
  req["id"] = uuid4();
  req["method"] = std::string("peer.connect");
  Json params = Json::object();
  params["from_id"] = from_id;
  params["secret"] = secret;
  req["params"] = params;
  if (!session_->send_text(req.dump())) raise(Status::Error, "peer.connect send failed");
  std::string msg;
  if (!session_->recv_text(msg, 10000)) raise(Status::Timeout, "peer.connect timed out");
  Json r = Json::parse(msg);
  if (r.contains("error") && !r.at("error").is_null())
    raise(Status::Error, "peer rejected connection: " + r.at("error").dump());
  return r.at("result").at("peer_id").as_string();
}

// ---------------- Factory ----------------
CommProtocolPtr make_protocol(ConnectionType type, ConnectionConfig cfg) {
  switch (type) {
    case ConnectionType::Loopback: return std::make_shared<LoopbackProtocol>(std::move(cfg));
    case ConnectionType::TCP: return std::make_shared<TCPIPProtocol>(std::move(cfg));
    case ConnectionType::UDP: return std::make_shared<UDPProtocol>(std::move(cfg));
    case ConnectionType::TLS: return std::make_shared<TLSProtocol>(std::move(cfg));
    case ConnectionType::WebSocket: return std::make_shared<WebSocketProtocol>(std::move(cfg));
    case ConnectionType::HTTP: return std::make_shared<HTTPProtocol>(std::move(cfg));
    case ConnectionType::MQTT: return std::make_shared<MQTTProtocol>(std::move(cfg));
    case ConnectionType::CoAP: return std::make_shared<CoAPProtocol>(std::move(cfg));
    case ConnectionType::AMQP: return std::make_shared<AMQPProtocol>(std::move(cfg));
    case ConnectionType::GRPC: return std::make_shared<GRPCProtocol>(std::move(cfg));
    case ConnectionType::LoRa: return std::make_shared<LoRaProtocol>(std::move(cfg));
    case ConnectionType::LoRaWAN: return std::make_shared<LoRaWANProtocol>(std::move(cfg));
    case ConnectionType::BLE: return std::make_shared<BLEProtocol>(std::move(cfg));
    case ConnectionType::BluetoothClassic: return std::make_shared<BluetoothClassicProtocol>(std::move(cfg));
    case ConnectionType::Zigbee: return std::make_shared<ZigbeeProtocol>(std::move(cfg));
    case ConnectionType::Thread: return std::make_shared<ThreadProtocol>(std::move(cfg));
    case ConnectionType::NFC: return std::make_shared<NFCProtocol>(std::move(cfg));
    case ConnectionType::WiFiDirect: return std::make_shared<WiFiDirectProtocol>(std::move(cfg));
    case ConnectionType::Cellular: return std::make_shared<CellularProtocol>(std::move(cfg));
    case ConnectionType::Serial: return std::make_shared<SerialProtocol>(std::move(cfg));
    case ConnectionType::I2C: return std::make_shared<I2CProtocol>(std::move(cfg));
    case ConnectionType::SPI: return std::make_shared<SPIProtocol>(std::move(cfg));
    case ConnectionType::CAN: return std::make_shared<CANProtocol>(std::move(cfg));
    case ConnectionType::RS485: return std::make_shared<RS485Protocol>(std::move(cfg));
    case ConnectionType::Ethernet: return std::make_shared<EthernetProtocol>(std::move(cfg));
  }
  raise(Status::Error, "unknown connection type");
}

CommProtocolPtr make_protocol_from_uri(const std::string& uri, const std::string& secret) {
  auto pos = uri.find("://");
  if (pos == std::string::npos) raise(Status::Error, "invalid connection uri: " + uri);
  std::string scheme = uri.substr(0, pos);
  std::string rest = uri.substr(pos + 3);

  struct SchemeMap { const char* scheme; ConnectionType type; };
  static const SchemeMap schemes[] = {
      {"loopback", ConnectionType::Loopback}, {"tcp", ConnectionType::TCP},
      {"udp", ConnectionType::UDP},           {"tls", ConnectionType::TLS},
      {"ws", ConnectionType::WebSocket},      {"wss", ConnectionType::WebSocket},
      {"http", ConnectionType::HTTP},         {"https", ConnectionType::HTTP},
      {"mqtt", ConnectionType::MQTT},         {"coap", ConnectionType::CoAP},
      {"amqp", ConnectionType::AMQP},         {"grpc", ConnectionType::GRPC},
      {"lora", ConnectionType::LoRa},         {"lorawan", ConnectionType::LoRaWAN},
      {"ble", ConnectionType::BLE},           {"bluetooth", ConnectionType::BluetoothClassic},
      {"zigbee", ConnectionType::Zigbee},     {"thread", ConnectionType::Thread},
      {"nfc", ConnectionType::NFC},           {"wifi-direct", ConnectionType::WiFiDirect},
      {"cellular", ConnectionType::Cellular}, {"serial", ConnectionType::Serial},
      {"i2c", ConnectionType::I2C},           {"spi", ConnectionType::SPI},
      {"can", ConnectionType::CAN},           {"rs485", ConnectionType::RS485},
      {"ethernet", ConnectionType::Ethernet},
  };
  ConnectionType type = ConnectionType::TCP;
  bool found = false;
  for (auto& s : schemes) {
    if (scheme == s.scheme) { type = s.type; found = true; break; }
  }
  if (!found) raise(Status::Error, "unknown connection scheme: " + scheme);

  ConnectionConfig cfg;
  cfg.uri = uri;
  cfg.secret = secret;
  // host[:port][/topic]
  std::string hostport = rest;
  std::string path;
  auto slash = rest.find('/');
  if (slash != std::string::npos) { hostport = rest.substr(0, slash); path = rest.substr(slash + 1); }
  auto colon = hostport.find(':');
  if (colon != std::string::npos) {
    cfg.host = hostport.substr(0, colon);
    cfg.port = (uint16_t)std::atoi(hostport.substr(colon + 1).c_str());
  } else {
    cfg.host = hostport;
  }
  if (!path.empty()) cfg.topic = path;
  return make_protocol(type, cfg);
}

}  // namespace laila_c
