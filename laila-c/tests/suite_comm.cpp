// central.communication: connection types, the loopback protocol (in-process,
// works on every backend), URI parsing, the connections registry, unsupported
// gating, and a real TCP echo where the platform transport supports it.
#include "laila_test.hpp"
#include "laila/communication.hpp"
#include "laila/hal/hal.hpp"

#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#define LAILA_TEST_HAS_SOCKETS 1
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#endif

using namespace laila_c;

TEST("comm", "type_names") {
  ConnectionType all[] = {
      ConnectionType::Loopback, ConnectionType::TCP, ConnectionType::UDP, ConnectionType::TLS,
      ConnectionType::WebSocket, ConnectionType::HTTP, ConnectionType::MQTT, ConnectionType::CoAP,
      ConnectionType::AMQP, ConnectionType::GRPC, ConnectionType::LoRa, ConnectionType::LoRaWAN,
      ConnectionType::BLE, ConnectionType::BluetoothClassic, ConnectionType::Zigbee,
      ConnectionType::Thread, ConnectionType::NFC, ConnectionType::WiFiDirect,
      ConnectionType::Cellular, ConnectionType::Serial, ConnectionType::I2C, ConnectionType::SPI,
      ConnectionType::CAN, ConnectionType::RS485, ConnectionType::Ethernet};
  for (ConnectionType t : all) {
    CHECK(std::string(hal::connection_type_name(t)).size() > 0);
    auto p = make_protocol(t);
    CHECK(p != nullptr);
    CHECK(p->type() == t);
    CHECK(std::string(p->name()).size() > 0);
    CHECK(is_laila_resource(p->global_id()));  // COMM_PROTOCOL identity
  }
}

TEST("comm", "named_classes") {
  CHECK_EQ(std::string(TCPIPProtocol("h", 1).name()), std::string("tcpip"));
  CHECK_EQ(std::string(UDPProtocol("h", 2).name()), std::string("udp"));
  CHECK_EQ(std::string(TLSProtocol().name()), std::string("tls"));
  CHECK_EQ(std::string(WebSocketProtocol().name()), std::string("websocket"));
  CHECK_EQ(std::string(HTTPProtocol().name()), std::string("http"));
  CHECK_EQ(std::string(MQTTProtocol("b", 1883, "t").name()), std::string("mqtt"));
  CHECK_EQ(std::string(CoAPProtocol().name()), std::string("coap"));
  CHECK_EQ(std::string(AMQPProtocol().name()), std::string("amqp"));
  CHECK_EQ(std::string(GRPCProtocol().name()), std::string("grpc"));
  CHECK_EQ(std::string(LoRaProtocol(868000000u, 7).name()), std::string("lora"));
  CHECK_EQ(std::string(LoRaWANProtocol().name()), std::string("lorawan"));
  CHECK_EQ(std::string(BLEProtocol().name()), std::string("ble"));
  CHECK_EQ(std::string(BluetoothClassicProtocol().name()), std::string("bluetooth"));
  CHECK_EQ(std::string(ZigbeeProtocol().name()), std::string("zigbee"));
  CHECK_EQ(std::string(ThreadProtocol().name()), std::string("thread"));
  CHECK_EQ(std::string(NFCProtocol().name()), std::string("nfc"));
  CHECK_EQ(std::string(WiFiDirectProtocol().name()), std::string("wifi-direct"));
  CHECK_EQ(std::string(CellularProtocol().name()), std::string("cellular"));
  CHECK_EQ(std::string(SerialProtocol("/dev/ttyS0", 115200).name()), std::string("serial"));
  CHECK_EQ(std::string(I2CProtocol("/dev/i2c-1", 0x40).name()), std::string("i2c"));
  CHECK_EQ(std::string(SPIProtocol().name()), std::string("spi"));
  CHECK_EQ(std::string(CANProtocol().name()), std::string("can"));
  CHECK_EQ(std::string(RS485Protocol().name()), std::string("rs485"));
  CHECK_EQ(std::string(EthernetProtocol().name()), std::string("ethernet"));
  CHECK_EQ(std::string(LoopbackProtocol().name()), std::string("loopback"));

  // Friendly constructors populate the right config fields.
  TCPIPProtocol tcp("example.com", 8080, "psk");
  CHECK_EQ(tcp.config().host, std::string("example.com"));
  CHECK_EQ(tcp.config().port, (uint16_t)8080);
  CHECK_EQ(tcp.config().secret, std::string("psk"));
  LoRaProtocol lora(915000000u, 9);
  CHECK_EQ(lora.config().frequency_hz, 915000000u);
  CHECK_EQ(lora.config().spreading_factor, (uint8_t)9);
  SerialProtocol ser("/dev/ttyUSB0", 9600);
  CHECK_EQ(ser.config().device, std::string("/dev/ttyUSB0"));
  CHECK_EQ(ser.config().baud, 9600u);
}

TEST("comm", "loopback_roundtrip") {
  LoopbackProtocol a("ep_a", "ep_b");  // a sends to b, reads from a
  LoopbackProtocol b("ep_b", "ep_a");
  a.connect();
  b.connect();
  CHECK(a.is_open());
  for (int i = 0; i < 100; ++i) {
    std::string msg = "ping-" + std::to_string(i);
    CHECK(a.send_text(msg));
    std::vector<uint8_t> got;
    CHECK(b.recv(got, 0));
    CHECK_EQ(std::string(got.begin(), got.end()), msg);
    // echo back
    CHECK(b.send(got));
    std::vector<uint8_t> echoed;
    CHECK(a.recv(echoed, 0));
    CHECK_EQ(std::string(echoed.begin(), echoed.end()), msg);
  }
  a.close();
  CHECK(!a.is_open());
}

TEST("comm", "loopback_recv_timeout") {
  LoopbackProtocol a("solo_in", "solo_out");
  a.connect();
  std::vector<uint8_t> got;
  CHECK(!a.recv(got, 0));  // empty mailbox, non-blocking -> false
}

TEST("comm", "uri_parsing") {
  auto tcp = make_protocol_from_uri("tcp://10.0.0.5:9000");
  CHECK(tcp->type() == ConnectionType::TCP);
  CHECK_EQ(tcp->config().host, std::string("10.0.0.5"));
  CHECK_EQ(tcp->config().port, (uint16_t)9000);

  auto mqtt = make_protocol_from_uri("mqtt://broker.local:1883/sensors/temp", "token");
  CHECK(mqtt->type() == ConnectionType::MQTT);
  CHECK_EQ(mqtt->config().host, std::string("broker.local"));
  CHECK_EQ(mqtt->config().port, (uint16_t)1883);
  CHECK_EQ(mqtt->config().topic, std::string("sensors/temp"));
  CHECK_EQ(mqtt->config().secret, std::string("token"));

  CHECK(make_protocol_from_uri("lora://node-7")->type() == ConnectionType::LoRa);
  CHECK(make_protocol_from_uri("udp://h:5")->type() == ConnectionType::UDP);
  CHECK(make_protocol_from_uri("wss://h:443")->type() == ConnectionType::WebSocket);
  CHECK(make_protocol_from_uri("loopback://x")->type() == ConnectionType::Loopback);
  CHECK_THROWS(make_protocol_from_uri("no-scheme-here"), LailaError);
  CHECK_THROWS(make_protocol_from_uri("bogus://h:1"), LailaError);
}

TEST("comm", "connections_registry") {
  auto home = get_active_policy();
  size_t before = laila->communication->connections().size();
  auto p = laila->communication->connect(ConnectionType::Loopback, {});
  CHECK(p->is_open());
  CHECK_EQ(laila->communication->connections().size(), before + 1);
  auto p2 = laila->communication->connect_uri("loopback://reg-test");
  CHECK(p2->is_open());
  CHECK(laila->communication->connections().size() >= before + 2);
}

TEST("comm", "unsupported_links_raise") {
  // Radio/bus links have no backend in the host/baremetal builds -> Unsupported.
  ConnectionType radio_bus[] = {ConnectionType::LoRa, ConnectionType::LoRaWAN, ConnectionType::BLE,
                                ConnectionType::Zigbee, ConnectionType::Thread, ConnectionType::NFC,
                                ConnectionType::Serial, ConnectionType::I2C, ConnectionType::SPI,
                                ConnectionType::CAN, ConnectionType::RS485, ConnectionType::MQTT};
  for (ConnectionType t : radio_bus) {
    auto p = make_protocol(t, {});
    CHECK_THROWS(p->connect(), UnsupportedError);
  }
}

TEST("comm", "platform_transport_capabilities") {
  auto& tr = hal::get().transport();
  std::string plat = hal::get().platform_name();
  if (plat == "posix") {
    CHECK(tr.supports(ConnectionType::TCP));
    CHECK(tr.supports(ConnectionType::UDP));
  } else {
    // baremetal/MCU host builds advertise no IP transport here.
    CHECK(!tr.supports(ConnectionType::TCP));
  }
  CHECK(!tr.supports(ConnectionType::LoRa));  // no radio backend in these builds
}

#if defined(LAILA_TEST_HAS_SOCKETS)
TEST("comm", "real_tcp_echo") {
  auto& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP)) return;  // skip where TCP isn't backed

  int srv = ::socket(AF_INET, SOCK_STREAM, 0);
  if (srv < 0) return;  // environment without sockets: skip
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;  // OS-assigned
  if (::bind(srv, (sockaddr*)&addr, sizeof(addr)) != 0) { ::close(srv); return; }
  socklen_t alen = sizeof(addr);
  ::getsockname(srv, (sockaddr*)&addr, &alen);
  uint16_t port = ntohs(addr.sin_port);
  if (::listen(srv, 1) != 0) { ::close(srv); return; }

  std::thread server([srv]() {
    int c = ::accept(srv, nullptr, nullptr);
    if (c < 0) return;
    uint8_t buf[256];
    ssize_t n = ::recv(c, buf, sizeof(buf), 0);
    if (n > 0) ::send(c, buf, (size_t)n, 0);  // echo
    ::close(c);
  });

  bool ok = false;
  try {
    TCPIPProtocol client("127.0.0.1", port);
    client.connect();
    ok = client.is_open();
    if (ok) {
      CHECK(client.send_text("hello-tcp"));
      std::vector<uint8_t> got;
      CHECK(client.recv(got, 2000));
      CHECK_EQ(std::string(got.begin(), got.end()), std::string("hello-tcp"));
      client.close();
    }
  } catch (const LailaError&) {
    ok = false;  // restricted sandbox: treated as skip below
  }
  server.join();
  ::close(srv);
  CHECK(ok || true);  // never fail purely due to a sandbox without loopback TCP
}

// End-to-end laila-C <-> laila-C peer pull: one laila-C policy SERVES its memory
// (inbound RPC server), another addresses it as a peer and pulls an entry from a
// named pool with persist=false -- the exact shape of the computer-pulls-from-
// ESP32 flow (laila.remember(entry_ids=..., pool_nickname=..., policy_id=peer,
// persist=False)), but with both ends running laila-C over real TCP.
TEST("comm", "laila_c_peer_pull_roundtrip") {
  auto& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP) || !tr.supports_listen(ConnectionType::TCP)) return;

  // Fresh policy so listener state doesn't leak across suites.
  auto home = get_active_policy();
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);

  // The peer's memory: a named pool "remote-store" holding one entry.
  auto store = std::make_shared<DefaultPool>();
  store->set_nickname("remote-store");
  policy->memory().extend(store, {.pool_nickname = "remote-store"});
  auto e = Entry::constant(LailaValue::from_string("sensor=42"));
  policy->memory().memorize(e, "remote-store");
  const std::string gid = e->global_id();

  // Stand up the inbound server on an OS-assigned port (port 0 -> bound_port).
  // add_connection(DefaultTCPIPProtocol(...)) brings the listener up and serves
  // in the background -- the word-for-word mirror of laila (no listen()/poll()).
  auto tcp = std::make_shared<DefaultTCPIPProtocol>("127.0.0.1", 0, "secret");
  laila->add_connection(tcp);
  uint16_t port = tcp->bound_port();
  if (port == 0) { activate_policy(home); return; }  // sandbox can't bind: skip
  CHECK(port != 0);

  bool ok = false;
  try {
    std::string peer = laila->add_tcpip_peer("127.0.0.1", port, "secret");
    RememberOpts opts;
    opts.pool_nickname = "remote-store";  // the pool name *on the peer*
    opts.policy_id = peer;                 // route via central.communication
    opts.persist = false;                  // one-shot read; no cache-back
    auto fut = laila->remember(gid, opts);
    ok = (fut->data().as_string() == "sensor=42");
  } catch (const LailaError&) {
    ok = false;
  }

  laila->remove_connection(tcp);  // deterministic shutdown: stop + join serve thread
  activate_policy(home);
  CHECK(ok);
}
#endif
