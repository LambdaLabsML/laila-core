// laila-C interop harness: a small CLI that brings up a laila-C policy and
// exchanges entries with a peer over a real connection. Driven by run_interop.py
// to peer a laila-C policy with an (unmodified) Python `laila` policy and verify
// memorize / remember / forget in both directions and tri-directionally.
//
// Modes:
//   serve   <port> <secret> <value>          run a TCP/WebSocket inbound server,
//                                            seed <value> into the alpha pool and
//                                            a named "remote-store" pool, print
//                                            identifiers + READY, then poll.
//   remember <uri> <secret> <gid> [nick]     peer with <uri>, pull <gid> (from the
//                                            optional named pool), print VALUE.
//   memorize <uri> <secret> <value>          peer with <uri>, push a new entry,
//                                            print MEMO <gid>.
//   forget   <uri> <secret> <gid>            peer with <uri>, forget <gid>, print
//                                            FORGOT.
//
// All cross-process replies use laila's native (untagged) entry wire dict, so the
// same harness interoperates with Python laila and with another laila-C process.
#include <csignal>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "laila/laila.hpp"
#include "laila/pools.hpp"
#include "laila/status.hpp"

using namespace laila_c;

static volatile std::sig_atomic_t g_stop = 0;
static void on_signal(int) { g_stop = 1; }

static int usage() {
  std::fprintf(stderr,
               "usage: c_harness serve <port> <secret> <value>\n"
               "       c_harness remember <uri> <secret> <gid> [pool_nickname]\n"
               "       c_harness memorize <uri> <secret> <value>\n"
               "       c_harness memorize_typed <uri> <secret> <kind>\n"
               "       c_harness forget <uri> <secret> <gid>\n");
  return 2;
}

// Build a fixed entry of a given kind via the implicit LailaValue constructors,
// so the Python interop driver can confirm a C-built (auto-inferred) payload of
// each type loads back through an unmodified Python `laila`. The values here
// must match TYPED in run_interop.py.
static EntryPtr build_typed(const std::string& kind) {
  if (kind == "int") return laila->constant(1234567);
  if (kind == "double") return laila->constant(3.5);
  if (kind == "bool") return laila->constant(true);
  if (kind == "bytes") return laila->constant(std::vector<uint8_t>{0, 1, 254, 255});
  if (kind == "dict") {
    Json o = Json::object();
    o["a"] = (int64_t)1;
    Json b = Json::array();
    b.push_back(Json((int64_t)1));
    b.push_back(Json((int64_t)2));
    b.push_back(Json((int64_t)3));
    o["b"] = b;
    return laila->constant(o);
  }
  if (kind == "numpy") return laila->constant(ndarray<float>({1.0f, 2.0f, 3.0f, 4.0f}, {2, 2}));
  raise(Status::Error, "unknown typed kind: " + kind);
  return nullptr;
}

static int do_serve(uint16_t port, const std::string& secret, const std::string& value) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);

  // One entry in the alpha pool, one in a named pool a peer can address.
  auto alpha_entry = laila->constant(LailaValue::from_string(value));
  laila->memorize(alpha_entry)->wait();

  auto store = std::make_shared<DefaultPool>();
  store->set_nickname("remote-store");
  policy->memory().extend(store, {.pool_nickname = "remote-store"});
  auto store_entry = laila->constant(LailaValue::from_string(value));
  policy->memory().memorize(store_entry, "remote-store");

  // add_connection brings the listener up and serves in the background; the
  // secret travels with the protocol (peer_secret_key).
  auto tcp = std::make_shared<DefaultTCPIPProtocol>("127.0.0.1", port, secret);
  laila->add_connection(tcp);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
  std::printf("POLICY %s\n", policy->global_id().c_str());
  std::printf("ALPHA_GID %s\n", alpha_entry->global_id().c_str());
  std::printf("STORE_GID %s\n", store_entry->global_id().c_str());
  std::printf("READY %u\n", (unsigned)tcp->bound_port());
  std::fflush(stdout);

  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return 0;
}

static int do_remember(const std::string& uri, const std::string& secret, const std::string& gid,
                       const std::string& nick) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string pid = laila->add_peer(uri, secret);
  RememberOpts opts;
  opts.policy_id = pid;
  opts.persist = false;
  if (!nick.empty()) opts.pool_nickname = nick;
  auto fut = laila->remember(gid, opts);
  std::printf("VALUE %s\n", fut->data().as_string().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_memorize(const std::string& uri, const std::string& secret, const std::string& value) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string pid = laila->add_peer(uri, secret);
  auto e = laila->constant(LailaValue::from_string(value));
  laila->peer(pid)->memorize(e)->wait();
  std::printf("MEMO %s\n", e->global_id().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_memorize_typed(const std::string& uri, const std::string& secret,
                             const std::string& kind) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string pid = laila->add_peer(uri, secret);
  auto e = build_typed(kind);
  laila->peer(pid)->memorize(e)->wait();
  std::printf("MEMO %s\n", e->global_id().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_forget(const std::string& uri, const std::string& secret, const std::string& gid) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string pid = laila->add_peer(uri, secret);
  laila->peer(pid)->forget(gid)->wait();
  std::printf("FORGOT %s\n", gid.c_str());
  std::fflush(stdout);
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) return usage();
  std::string mode = argv[1];
#if !defined(LAILA_NO_EXCEPTIONS)
  try {
#endif
    if (mode == "serve" && argc >= 5)
      return do_serve((uint16_t)std::stoi(argv[2]), argv[3], argv[4]);
    if (mode == "remember" && argc >= 5)
      return do_remember(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "memorize" && argc >= 5)
      return do_memorize(argv[2], argv[3], argv[4]);
    if (mode == "memorize_typed" && argc >= 5)
      return do_memorize_typed(argv[2], argv[3], argv[4]);
    if (mode == "forget" && argc >= 5)
      return do_forget(argv[2], argv[3], argv[4]);
    return usage();
#if !defined(LAILA_NO_EXCEPTIONS)
  } catch (const std::exception& e) {
    std::printf("ERROR %s\n", e.what());
    std::fflush(stdout);
    return 1;
  }
#endif
}
