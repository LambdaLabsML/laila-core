// Wire-level regressions from the cross-language interop soak:
//   - a WebSocket peer Close ends the session and closes TCP (RFC 6455 close
//     handshake; Python `websockets` waits for the EOF),
//   - the inbound listener serves accepted peers concurrently (one session
//     thread each) instead of one-at-a-time,
//   - the outbound client demultiplexes replies by JSON-RPC id and answers an
//     inbound `__comm_ping__` that arrives while its own call is in flight.
#include "laila_test.hpp"
#include "laila/communication.hpp"
#include "laila/framing.hpp"
#include "laila/hal/hal.hpp"
#include "laila/websocket.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#define LAILA_TEST_HAS_SOCKETS 1
#endif

using namespace laila_c;

namespace {

// In-memory hal::Connection: `inbox` is what the peer "sent" us, `outbox`
// collects what we sent. recv() hands out the inbox in one piece.
class FakeConn : public hal::Connection {
public:
  std::vector<uint8_t> inbox;
  std::vector<uint8_t> outbox;
  bool closed = false;
  bool send(const std::vector<uint8_t>& data) override {
    if (closed) return false;
    outbox.insert(outbox.end(), data.begin(), data.end());
    return true;
  }
  bool recv(std::vector<uint8_t>& out, int /*timeout_ms*/) override {
    if (closed || inbox.empty()) return false;
    out = inbox;
    inbox.clear();
    return true;
  }
  void close() override { closed = true; }
  bool is_open() const override { return !closed; }
};

// Masked (client -> server) WebSocket frame with the given opcode and payload.
std::vector<uint8_t> masked_frame(uint8_t opcode, const std::string& payload) {
  std::vector<uint8_t> f;
  f.push_back((uint8_t)(0x80 | opcode));
  f.push_back((uint8_t)(0x80 | payload.size()));  // payloads < 126 only
  const uint8_t mk[4] = {0x11, 0x22, 0x33, 0x44};
  f.insert(f.end(), mk, mk + 4);
  for (size_t i = 0; i < payload.size(); ++i) f.push_back((uint8_t)(payload[i] ^ mk[i & 3]));
  return f;
}

size_t count_close_frames(const std::vector<uint8_t>& bytes) {
  // Server frames are unmasked; an empty Close is exactly {0x88, 0x00}.
  size_t n = 0;
  for (size_t i = 0; i + 1 < bytes.size(); ++i)
    if (bytes[i] == 0x88 && bytes[i + 1] == 0x00) ++n;
  return n;
}

// A fresh policy with one entry in a "remote-store" pool, served on an
// OS-assigned port. Restores the previously active policy on destruction.
struct ServedPolicy {
  PolicyPtr previous;
  PolicyPtr policy;
  std::shared_ptr<DefaultTCPIPProtocol> tcp;
  std::string gid;
  uint16_t port = 0;

  explicit ServedPolicy(int max_sessions = 8) {
    previous = get_active_policy();
    policy = std::make_shared<Policy>();
    activate_policy(policy);
    auto store = std::make_shared<DefaultPool>();
    store->set_nickname("remote-store");
    policy->memory().extend(store, {.pool_nickname = "remote-store"});
    auto e = Entry::constant(LailaValue::from_string("sensor=42"));
    policy->memory().memorize(e, "remote-store");
    gid = e->global_id();
    tcp = std::make_shared<DefaultTCPIPProtocol>("127.0.0.1", 0, "secret");
    tcp->config().max_inbound_sessions = max_sessions;
    laila->add_connection(tcp);
    port = tcp->bound_port();
  }
  ~ServedPolicy() {
    laila->remove_connection(tcp);
    activate_policy(previous);
  }
};

bool have_tcp_serving() {
  auto& tr = hal::get().transport();
  return tr.supports(ConnectionType::TCP) && tr.supports_listen(ConnectionType::TCP);
}

}  // namespace

TEST("wire", "ws_close_ends_session") {
  FakeConn conn;
  ws::WsSession s(&conn, /*is_client=*/false);
  CHECK(s.ok());
  conn.inbox = masked_frame(0x8, "");  // peer's Close
  std::string msg;
  CHECK(!s.recv_text(msg, 100));
  CHECK(!s.ok());
  CHECK(s.closed());
  CHECK(conn.closed);  // server closes TCP after the handshake
  CHECK_EQ(count_close_frames(conn.outbox), (size_t)1);
  // Idempotent: a later close() neither re-sends Close nor reopens anything.
  s.close();
  CHECK_EQ(count_close_frames(conn.outbox), (size_t)1);
  CHECK(!s.ok());
}

TEST("wire", "ws_close_from_our_side_is_once") {
  FakeConn conn;
  ws::WsSession s(&conn, /*is_client=*/false);
  s.close();
  s.close();
  CHECK_EQ(count_close_frames(conn.outbox), (size_t)1);
  CHECK(conn.closed);
  CHECK(!s.ok());
  // Nothing can be read from a closed session, even if bytes are queued.
  conn.closed = false;
  conn.inbox = masked_frame(0x1, "late");
  std::string msg;
  CHECK(!s.recv_text(msg, 10));
}

#if defined(LAILA_TEST_HAS_SOCKETS)
TEST("wire", "ws_server_closes_tcp_after_close") {
  if (!have_tcp_serving()) return;
  ServedPolicy served(/*max_sessions=*/1);
  if (served.port == 0) return;  // sandbox can't bind: skip

  // Hand-rolled WebSocket client so we can send a bare Close frame and watch
  // what the server does with the TCP connection afterwards.
  ConnectionConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = served.port;
  std::unique_ptr<hal::Connection> raw(hal::get().transport().open(ConnectionType::TCP, cfg));
  CHECK(raw != nullptr);
  if (!raw) return;
  ws::WsSession client(raw.get(), /*is_client=*/true);
  CHECK(client.client_handshake("127.0.0.1", served.port));
  Json req = Json::object();
  req["jsonrpc"] = std::string("2.0");
  req["id"] = std::string("c1");
  req["method"] = std::string("peer.connect");
  Json params = Json::object();
  params["from_id"] = std::string("LAILA:POLICY:test");
  params["secret"] = std::string("secret");
  req["params"] = params;
  CHECK(client.send_text(req.dump()));
  std::string msg;
  CHECK(client.recv_text(msg, 5000));
  CHECK(Json::parse(msg).at("result").at("peer_id").as_string() == served.policy->global_id());

  // Send Close (masked, as a client must) directly on the socket.
  uint64_t t0 = hal::get().clock().now_ms();
  CHECK(raw->send(masked_frame(0x8, "")));
  // The server must echo Close and then close TCP: our reads end in EOF.
  bool got_close_echo = false, eof = false;
  while (hal::get().clock().now_ms() - t0 < 2000) {
    std::vector<uint8_t> chunk;
    if (raw->recv(chunk, 100)) {
      if (count_close_frames(chunk) > 0) got_close_echo = true;
      continue;
    }
    if (!raw->is_open()) { eof = true; break; }
  }
  uint64_t took = hal::get().clock().now_ms() - t0;
  CHECK(got_close_echo);
  CHECK(eof);
  CHECK(took < 500);

  // And the session slot is free again: with the cap at 1, a second peer can
  // only be admitted once the first session really ended.
  uint64_t t1 = hal::get().clock().now_ms();
  TCPIPProtocol second("127.0.0.1", served.port, "secret");
  second.connect();
  CHECK(second.peer_connect("LAILA:POLICY:test2", "secret") == served.policy->global_id());
  CHECK(hal::get().clock().now_ms() - t1 < 1000);
  second.close();
}

TEST("wire", "tcp_close_after_close_frees_slot") {
  if (!have_tcp_serving()) return;
  ServedPolicy served(/*max_sessions=*/1);
  if (served.port == 0) return;
  for (int round = 0; round < 3; ++round) {
    uint64_t t0 = hal::get().clock().now_ms();
    TCPIPProtocol p("127.0.0.1", served.port, "secret");
    p.connect();
    CHECK(p.peer_connect("LAILA:POLICY:t", "secret") == served.policy->global_id());
    p.close();
    CHECK(hal::get().clock().now_ms() - t0 < 1000);
  }
}

TEST("wire", "concurrent_inbound_peers") {
  if (!have_tcp_serving()) return;
  ServedPolicy served;
  if (served.port == 0) return;

  // Two peers hold their sessions open at the same time and interleave calls.
  // A serial accept loop admits the second only after the first hangs up, so
  // its peer.connect times out.
  const int kPeers = 2, kCalls = 5;
  std::mutex mu;
  std::condition_variable cv;
  int handshaken = 0;
  std::atomic<int> ok_calls{0};
  std::vector<std::string> errors(kPeers);

  auto worker = [&](int idx) {
    try {
      auto proto = std::make_shared<TCPIPProtocol>("127.0.0.1", served.port, "secret");
      proto->connect();
      std::string rid = proto->peer_connect("LAILA:POLICY:w" + std::to_string(idx), "secret");
      {
        std::unique_lock<std::mutex> lk(mu);
        ++handshaken;
        cv.notify_all();
        // Both sessions must be live before anyone starts calling.
        cv.wait_for(lk, std::chrono::seconds(15), [&] { return handshaken == kPeers; });
      }
      RemotePolicyProxy proxy(rid, proto);
      for (int i = 0; i < kCalls; ++i) {
        auto fut = proxy.remember(served.gid, "remote-store", false);
        if (fut->data().as_string() == "sensor=42") ++ok_calls;
      }
      proto->close();
    } catch (const LailaError& e) {
      errors[idx] = e.what();
    }
  };
  std::vector<std::thread> threads;
  for (int i = 0; i < kPeers; ++i) threads.emplace_back(worker, i);
  for (auto& t : threads) t.join();
  for (int i = 0; i < kPeers; ++i) {
    if (!errors[i].empty()) std::printf("  peer %d: %s\n", i, errors[i].c_str());
    CHECK(errors[i].empty());
  }
  CHECK_EQ(ok_calls.load(), kPeers * kCalls);
}

TEST("wire", "ping_demux") {
  auto& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP) || !tr.supports_listen(ConnectionType::TCP)) return;

  // The entry the fake server will hand back, in laila's wire form.
  auto e = Entry::constant(LailaValue::from_string("pinged-but-fine"));
  TransformationSequence b64 = TransformationSequence::base64();
  Json blob = e->serialize(&b64);

  ConnectionConfig lcfg;
  lcfg.host = "127.0.0.1";
  lcfg.port = 0;
  std::unique_ptr<hal::Listener> listener(tr.listen(ConnectionType::TCP, lcfg));
  if (!listener || listener->local_port() == 0) return;  // sandbox: skip
  uint16_t port = listener->local_port();

  // What the fake server observed about the pong.
  std::string pong_id, pong_result, failure;
  std::thread server([&]() {
    std::unique_ptr<hal::Connection> c;
    for (int i = 0; i < 100 && !c; ++i) c.reset(listener->accept(100));
    if (!c) { failure = "no client connected"; return; }
    StreamFramer fr(c.get());
    std::string msg;
    // 1) peer.connect -> ok
    if (!fr.recv(msg, 5000)) { failure = "no peer.connect"; return; }
    Json req = Json::parse(msg);
    Json ok = Json::object();
    ok["jsonrpc"] = std::string("2.0");
    ok["id"] = req.at("id");
    Json res = Json::object();
    res["peer_id"] = std::string("LAILA:POLICY:fake");
    ok["result"] = res;
    fr.send(ok.dump());
    // 2) the real request arrives; before answering, probe liveness like a
    //    laila server does (__comm_ping__ with its own id)...
    if (!fr.recv(msg, 5000)) { failure = "no rpc.call"; return; }
    Json call = Json::parse(msg);
    Json ping = Json::object();
    ping["jsonrpc"] = std::string("2.0");
    ping["id"] = std::string("ping-7");
    ping["method"] = std::string("rpc.call");
    Json pp = Json::object();
    Json path = Json::array();
    path.push_back(Json(std::string("__comm_ping__")));
    pp["path"] = path;
    pp["args"] = Json::array();
    pp["kwargs"] = Json::object();
    ping["params"] = pp;
    fr.send(ping.dump());
    // ...then send the real reply.
    Json reply = Json::object();
    reply["jsonrpc"] = std::string("2.0");
    reply["id"] = call.at("id");
    Json list = Json::array();
    list.push_back(blob);
    reply["result"] = list;
    fr.send(reply.dump());
    // 3) the client must have answered the ping with "pong" under the ping's id.
    if (!fr.recv(msg, 5000)) { failure = "no pong"; return; }
    Json pong = Json::parse(msg);
    pong_id = pong.contains("id") ? pong.at("id").dump() : "";
    pong_result = pong.contains("result") ? pong.at("result").dump() : pong.dump();
  });

  std::string got;
  std::string err;
  try {
    auto proto = std::make_shared<TCPIPProtocol>("127.0.0.1", port, "secret");
    proto->connect();
    std::string rid = proto->peer_connect(get_active_policy()->global_id(), "secret");
    RemotePolicyProxy proxy(rid, proto);
    got = proxy.remember(e->global_id(), "remote-store", false)->data().as_string();
    proto->close();
  } catch (const LailaError& ex) {
    err = ex.what();
  }
  server.join();
  listener->close();
  if (!err.empty()) std::printf("  client: %s\n", err.c_str());
  if (!failure.empty()) std::printf("  server: %s\n", failure.c_str());
  CHECK(err.empty());
  CHECK(failure.empty());
  CHECK_EQ(got, std::string("pinged-but-fine"));
  CHECK_EQ(pong_id, std::string("\"ping-7\""));
  CHECK_EQ(pong_result, std::string("\"pong\""));
}
#endif
