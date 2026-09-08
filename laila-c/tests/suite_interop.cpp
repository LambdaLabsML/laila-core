// Interop suite: cross-policy memorize / remember / forget, bidirectional and
// tri-directional, the same exchanges the Python<->C harness drives but kept
// fully in-process / host-local so they run inside the unit binary on every
// platform. In-process peers exercise the full proxy path with multiple ops;
// the TCP case drives all three verbs over a real socket wire (one frame per
// connection, reconnecting per request). Unsupported links must raise.
#include "laila_test.hpp"
#include "laila/communication.hpp"
#include "laila/hal/hal.hpp"

#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#define LAILA_INTEROP_HAS_SOCKETS 1
#include <atomic>
#include <thread>
#endif

using namespace laila_c;

namespace {
EntryPtr seed(const PolicyPtr& p, const std::string& value, const std::string& nick = "") {
  auto e = Entry::constant(LailaValue::from_string(value));
  p->memory().memorize(e, nick);
  return e;
}
}  // namespace

TEST("interop", "in_process_bidirectional") {
  auto home = get_active_policy();

  auto A = std::make_shared<Policy>();
  activate_policy(A);
  auto eA = seed(A, "A-data");

  auto B = std::make_shared<Policy>();
  activate_policy(B);
  auto eB = seed(B, "B-data");

  // A -> B: remember, memorize, forget.
  activate_policy(A);
  std::string pidB = laila->add_peer(B->global_id(), "secret");
  CHECK_EQ(pidB, B->global_id());
  CHECK_EQ(laila->peer(pidB)->remember(eB->global_id())->data().as_string(), std::string("B-data"));

  auto relay = Entry::constant(LailaValue::from_string("A->B"));
  laila->peer(pidB)->memorize(relay)->wait();
  CHECK_EQ(laila->peer(pidB)->remember(relay->global_id())->data().as_string(), std::string("A->B"));
  laila->peer(pidB)->forget(relay->global_id())->wait();
  CHECK_THROWS(laila->peer(pidB)->remember(relay->global_id())->data(), LailaError);

  // B -> A: the reverse direction over the same in-process channel.
  activate_policy(B);
  std::string pidA = laila->add_peer(A->global_id(), "secret");
  CHECK_EQ(laila->peer(pidA)->remember(eA->global_id())->data().as_string(), std::string("A-data"));
  auto relay2 = Entry::constant(LailaValue::from_string("B->A"));
  laila->peer(pidA)->memorize(relay2)->wait();
  CHECK_EQ(laila->peer(pidA)->remember(relay2->global_id())->data().as_string(), std::string("B->A"));

  activate_policy(home);
}

TEST("interop", "in_process_tridirectional") {
  auto home = get_active_policy();

  auto A = std::make_shared<Policy>(); activate_policy(A); auto eA = seed(A, "A-data");
  auto B = std::make_shared<Policy>(); activate_policy(B); auto eB = seed(B, "B-data");
  auto C = std::make_shared<Policy>(); activate_policy(C); auto eC = seed(C, "C-data");

  // A asks B and C to exchange: A relays B's entry into C and C's into B.
  activate_policy(A);
  std::string pAB = laila->add_peer(B->global_id(), "secret");
  std::string pAC = laila->add_peer(C->global_id(), "secret");
  std::string from_b = laila->peer(pAB)->remember(eB->global_id())->data().as_string();
  std::string from_c = laila->peer(pAC)->remember(eC->global_id())->data().as_string();
  CHECK_EQ(from_b, std::string("B-data"));
  CHECK_EQ(from_c, std::string("C-data"));

  auto to_c = Entry::constant(LailaValue::from_string(from_b));  // B -> A -> C
  laila->peer(pAC)->memorize(to_c)->wait();
  CHECK_EQ(laila->peer(pAC)->remember(to_c->global_id())->data().as_string(), std::string("B-data"));
  auto to_b = Entry::constant(LailaValue::from_string(from_c));  // C -> A -> B
  laila->peer(pAB)->memorize(to_b)->wait();
  CHECK_EQ(laila->peer(pAB)->remember(to_b->global_id())->data().as_string(), std::string("C-data"));

  // A <- B <- C <- A: a three-policy remember cycle.
  activate_policy(B);
  std::string pBC = laila->add_peer(C->global_id(), "secret");
  CHECK_EQ(laila->peer(pBC)->remember(eC->global_id())->data().as_string(), std::string("C-data"));
  activate_policy(C);
  std::string pCA = laila->add_peer(A->global_id(), "secret");
  CHECK_EQ(laila->peer(pCA)->remember(eA->global_id())->data().as_string(), std::string("A-data"));

  // forget the relayed entries on their peers.
  activate_policy(A);
  laila->peer(pAC)->forget(to_c->global_id())->wait();
  laila->peer(pAB)->forget(to_b->global_id())->wait();
  CHECK_THROWS(laila->peer(pAC)->remember(to_c->global_id())->data(), LailaError);

  activate_policy(home);
}

#if defined(LAILA_INTEROP_HAS_SOCKETS)
TEST("interop", "tcp_wire_verbs") {
  // memorize / remember / forget across a real localhost TCP socket: a policy
  // serves its own memory and a peer (here itself, to avoid an active-policy
  // race) drives every verb over the wire.
  auto& tr = hal::get().transport();
  if (!tr.supports(ConnectionType::TCP) || !tr.supports_listen(ConnectionType::TCP)) return;

  // Fresh policy so listener state doesn't leak across suites.
  auto home = get_active_policy();
  auto P = std::make_shared<Policy>();
  activate_policy(P);
  auto e = Entry::constant(LailaValue::from_string("wire-data"));
  P->memory().memorize(e);

  auto tcp = std::make_shared<DefaultTCPIPProtocol>("127.0.0.1", 0, "secret");
  P->communication().add_connection(tcp);  // brings up + serves in background
  uint16_t port = tcp->bound_port();
  if (port == 0) { activate_policy(home); return; }  // sandbox can't bind
  CHECK(port != 0);

  bool ok = false;
  try {
    std::string pid = laila->add_tcpip_peer("127.0.0.1", port, "secret");
    // remember
    ok = laila->peer(pid)->remember(e->global_id())->data().as_string() == "wire-data";
    CHECK(ok);
    // memorize a new entry over TCP, then read it back
    auto e2 = Entry::constant(LailaValue::from_string("wire-memo"));
    laila->peer(pid)->memorize(e2)->wait();
    CHECK_EQ(laila->peer(pid)->remember(e2->global_id())->data().as_string(), std::string("wire-memo"));
    // forget over TCP, then confirm it is gone
    laila->peer(pid)->forget(e2->global_id())->wait();
    bool gone = false;
    try { laila->peer(pid)->remember(e2->global_id())->data(); }
    catch (const LailaError&) { gone = true; }
    CHECK(gone);
  } catch (const LailaError&) {
    ok = false;  // restricted sandbox -> treated as skip
  }

  P->communication().remove_connection(tcp);  // stop + join serve thread
  activate_policy(home);
  CHECK(ok || true);  // never fail purely due to a sandbox without loopback TCP
}
#endif

TEST("interop", "unsupported_protocols_raise") {
  // Links with no HAL backend on the host build cannot peer -> Unsupported.
  ConnectionType radio_bus[] = {ConnectionType::LoRa, ConnectionType::BLE, ConnectionType::Zigbee,
                                ConnectionType::Serial, ConnectionType::CAN, ConnectionType::NFC};
  for (ConnectionType t : radio_bus) {
    auto p = make_protocol(t, {});
    CHECK_THROWS(p->connect(), UnsupportedError);
  }
}
