// In-process peers + RemoteFuture: register another local policy as a peer and
// fetch entries that live on it. Restores the active policy afterward.
#include "laila_test.hpp"

using namespace laila_c;

TEST("peers", "remote_remember_roundtrip") {
  auto home = get_active_policy();
  for (int i = 0; i < 30; ++i) {
    auto peer = std::make_shared<Policy>();
    activate_policy(peer);
    auto e = Entry::constant(LailaValue::from_string("peer-" + std::to_string(i)));
    std::string gid = e->global_id();
    laila->memorize(e)->wait();

    activate_policy(home);
    std::string pid = laila->communication->add_peer(peer->global_id(), "secret");
    CHECK_EQ(pid, peer->global_id());
    auto rf = laila->communication->remote_remember(pid, gid);
    CHECK(rf->data().as_string() == "peer-" + std::to_string(i));
    // home does not have it locally.
    CHECK_THROWS(laila->remember(gid)->data(), LailaError);
  }
  activate_policy(home);
}

TEST("peers", "request_in_process") {
  auto home = get_active_policy();
  auto peer = std::make_shared<Policy>();
  activate_policy(peer);
  auto e = Entry::constant(LailaValue::from_int(12345));
  std::string gid = e->global_id();
  laila->memorize(e)->wait();

  activate_policy(home);
  std::string pid = laila->add_peer(peer->global_id(), "secret");
  // laila.request(peer, "central.memory.remember", [gid])
  Json args = Json::array();
  args.push_back(Json(gid));
  auto f = laila->request(pid, "central.memory.remember", args);
  CHECK_EQ(f->data().as_int(), (int64_t)12345);
  // convenience verb on the proxy
  CHECK_EQ(laila->peer(pid)->remember(gid)->data().as_int(), (int64_t)12345);
  activate_policy(home);
}

TEST("peers", "unknown_peer_errors") {
  auto home = get_active_policy();
  CHECK_THROWS(laila->communication->remote_remember(
                   "LAILA:POLICY:GLOBAL_ID:00000000-0000-0000-0000-000000000000", "x"),
               LailaError);
  // add_peer to a non-existent local policy with no network transport -> Unsupported.
  CHECK_THROWS(laila->communication->add_peer("not-a-real-uri", "s"), LailaError);
  activate_policy(home);
}
