// laila-C interop harness: a small CLI that brings up a laila-C policy and
// exchanges entries with a peer over a real connection. Driven by run_interop.py
// to peer a laila-C policy with an (unmodified) Python `laila` policy and verify
// memorize / remember / forget in both directions and tri-directionally.
//
// Modes (the shared harness contract; see tests/interop/README.md at the repo
// root -- py_harness.py and js_harness.js implement the same CLI):
//   serve   [scheme] <port> <secret> <value> [peer_uri ...]
//                                            run a TCP/WebSocket inbound server
//                                            (one listener serves both tcp:// and
//                                            ws://, so <scheme> is accepted and
//                                            ignored), seed <value> into the alpha
//                                            pool and a named "remote-store" pool,
//                                            peer with each <peer_uri>, print
//                                            identifiers + READY, then poll.
//   remember <uri> <secret> <gid> [nick]     peer with <uri>, pull <gid> (from the
//                                            optional named pool), print VALUE.
//   memorize <uri> <secret> <value> [nick]   peer with <uri>, push a new entry
//                                            (into the optional named pool),
//                                            print MEMO <gid>.
//   memorize_typed <uri> <secret> <kind> [nick]
//                                            push a fixed entry of <kind>, MEMO.
//   remember_typed <uri> <secret> <gid> <kind>
//                                            pull <gid>, compare with the fixed
//                                            <kind> value: TYPED_OK / TYPED_MISMATCH.
//   memorize_json <uri> <secret> <json> [nick]
//                                            parse <json>, push it (dict/list as
//                                            msgpack, scalars natively), MEMO.
//   remember_json <uri> <secret> <gid> [nick]
//                                            pull <gid>, print JSON <json>.
//   memorize_bytes <uri> <secret> <n> <seed> [nick]
//                                            push <n> deterministic bytes
//                                            (xorshift32(<seed>)), print
//                                            MEMO <gid> <fnv1a64>.
//   remember_hash <uri> <secret> <gid> [nick]
//                                            pull <gid>, print HASH <fnv1a64> <len>.
//   relay   <via_uri> <secret> <from_gid> <gid> [pool_nickname]
//                                            3-party: ask the policy at <via_uri>
//                                            to pull <gid> from policy <from_gid>
//                                            into its own memory, read it back,
//                                            print VALUE.
//   forget   <uri> <secret> <gid> [nick]     peer with <uri>, forget <gid>, print
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
#include <vector>

#include "laila/laila.hpp"
#include "laila/pools.hpp"
#include "laila/status.hpp"

using namespace laila_c;

static volatile std::sig_atomic_t g_stop = 0;
static void on_signal(int) { g_stop = 1; }

static int usage() {
  std::fprintf(stderr,
               "usage: c_harness serve [scheme] <port> <secret> <value> [peer_uri ...]\n"
               "       c_harness remember <uri> <secret> <gid> [pool_nickname]\n"
               "       c_harness memorize <uri> <secret> <value> [pool_nickname]\n"
               "       c_harness memorize_typed <uri> <secret> <kind> [pool_nickname]\n"
               "       c_harness remember_typed <uri> <secret> <gid> <kind>\n"
               "       c_harness memorize_json <uri> <secret> <json> [pool_nickname]\n"
               "       c_harness remember_json <uri> <secret> <gid> [pool_nickname]\n"
               "       c_harness memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]\n"
               "       c_harness remember_hash <uri> <secret> <gid> [pool_nickname]\n"
               "       c_harness relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]\n"
               "       c_harness forget <uri> <secret> <gid> [pool_nickname]\n");
  return 2;
}

// Deterministic byte stream shared by every harness (xorshift32, low byte per
// step); keep in sync with xorshift_bytes in py_harness.py / js_harness.js.
static std::vector<uint8_t> xorshift_bytes(size_t n, uint32_t seed) {
  uint32_t x = seed ? seed : 1u;
  std::vector<uint8_t> out(n);
  for (size_t i = 0; i < n; ++i) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    out[i] = (uint8_t)(x & 0xFFu);
  }
  return out;
}

// FNV-1a 64 as 16 lowercase hex digits.
static std::string fnv1a64(const std::vector<uint8_t>& data) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (uint8_t b : data) {
    h ^= b;
    h *= 0x100000001b3ULL;
  }
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)h);
  return buf;
}

// Round-trippable JSON text (the library's Json::dump() prints doubles with the
// default 6-digit ostream precision, which is not enough to compare values; the
// orchestrator parses this line, so emit %.17g and standard escapes).
static void dump_json_text(const Json& j, std::string& out) {
  switch (j.type()) {
    case Json::Type::Null: out += "null"; return;
    case Json::Type::Bool: out += j.as_bool() ? "true" : "false"; return;
    case Json::Type::Int: out += std::to_string(j.as_int()); return;
    case Json::Type::Double: {
      char buf[40];
      std::snprintf(buf, sizeof(buf), "%.17g", j.as_double());
      out += buf;
      return;
    }
    case Json::Type::String:
    case Json::Type::Bytes: {
      std::string s = j.type() == Json::Type::String
                          ? j.as_string()
                          : std::string(j.as_bytes().begin(), j.as_bytes().end());
      out.push_back('"');
      for (unsigned char c : s) {
        if (c == '"') out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else out.push_back((char)c);
      }
      out.push_back('"');
      return;
    }
    case Json::Type::Array: {
      out.push_back('[');
      bool first = true;
      for (const auto& e : j.elements()) {
        if (!first) out.push_back(',');
        first = false;
        dump_json_text(e, out);
      }
      out.push_back(']');
      return;
    }
    case Json::Type::Object: {
      out.push_back('{');
      bool first = true;
      for (const auto& kv : j.items()) {
        if (!first) out.push_back(',');
        first = false;
        dump_json_text(Json(kv.first), out);
        out.push_back(':');
        dump_json_text(kv.second, out);
      }
      out.push_back('}');
      return;
    }
  }
}

// A parsed JSON document as a laila payload: containers travel as msgpack
// (Json), scalars as native primitives (mirrors what Python's json.loads +
// laila.constant would produce).
static LailaValue json_to_value(const Json& j) {
  switch (j.type()) {
    case Json::Type::Null: return LailaValue::none();
    case Json::Type::Bool: return LailaValue::from_bool(j.as_bool());
    case Json::Type::Int: return LailaValue::from_int(j.as_int());
    case Json::Type::Double: return LailaValue::from_double(j.as_double());
    case Json::Type::String: return LailaValue::from_string(j.as_string());
    default: return LailaValue::from_json(j);
  }
}

// A pulled payload as JSON (nullopt-style: returns false for bytes/tensors).
static bool value_to_json(const LailaValue& v, Json& out) {
  using K = LailaValue::Kind;
  switch (v.kind()) {
    case K::None: out = Json(nullptr); return true;
    case K::Bool: out = Json(v.as_bool()); return true;
    case K::Int: out = Json(v.as_int()); return true;
    case K::Double: out = Json(v.as_double()); return true;
    case K::String: out = Json(v.as_string()); return true;
    case K::Json: out = v.as_json(); return true;
    case K::Bytes:
    case K::Tensor: return false;
  }
  return false;
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

// Does *got* (a value pulled from a peer) equal the fixed value of <kind>?
static bool typed_matches(const std::string& kind, const LailaValue& got) {
  using K = LailaValue::Kind;
  if (kind == "int") return got.kind() == K::Int && got.as_int() == 1234567;
  if (kind == "double") return got.kind() == K::Double && got.as_double() == 3.5;
  if (kind == "bool") return got.kind() == K::Bool && got.as_bool() == true;
  if (kind == "bytes")
    return got.kind() == K::Bytes && got.as_bytes() == std::vector<uint8_t>{0, 1, 254, 255};
  if (kind == "dict") {
    if (got.kind() != K::Json || !got.as_json().is_object()) return false;
    const Json& o = got.as_json();
    if (o.items().size() != 2 || !o.contains("a") || !o.contains("b")) return false;
    if (o.at("a").as_int() != 1 || !o.at("b").is_array()) return false;
    const auto& b = o.at("b").elements();
    return b.size() == 3 && b[0].as_int() == 1 && b[1].as_int() == 2 && b[2].as_int() == 3;
  }
  if (kind == "numpy") {
    if (got.kind() != K::Tensor || got.dtype() != "<f4") return false;
    if (got.shape() != std::vector<int64_t>{2, 2}) return false;
    const auto& raw = got.as_bytes();
    if (raw.size() != 4 * sizeof(float)) return false;
    float f[4];
    std::memcpy(f, raw.data(), sizeof(f));
    return f[0] == 1.0f && f[1] == 2.0f && f[2] == 3.0f && f[3] == 4.0f;
  }
  raise(Status::Error, "unknown typed kind: " + kind);
  return false;
}

static std::string describe(const LailaValue& v) {
  using K = LailaValue::Kind;
  switch (v.kind()) {
    case K::None: return "none";
    case K::Bool: return std::string("bool:") + (v.as_bool() ? "true" : "false");
    case K::Int: return "int:" + std::to_string(v.as_int());
    case K::Double: return "double:" + std::to_string(v.as_double());
    case K::String: return "str:" + v.as_string();
    case K::Bytes: return "bytes[" + std::to_string(v.as_bytes().size()) + "]";
    case K::Json: return "json:" + v.as_json().dump();
    case K::Tensor: return "tensor:" + v.dtype() + "[" + std::to_string(v.as_bytes().size()) + "B]";
  }
  return "?";
}

static int do_serve(uint16_t port, const std::string& secret, const std::string& value,
                    const std::vector<std::string>& peers) {
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
  for (const auto& uri : peers) {
    std::string pid = laila->add_peer(uri, secret);
    std::printf("PEER %s %s\n", uri.c_str(), pid.c_str());
  }
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

// Activate a fresh policy and peer it with <uri>; returns the peer id.
static std::string connect(const std::string& uri, const std::string& secret) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  return laila->add_peer(uri, secret);
}

// Push entry *e* to peer <pid> (into the pool <nick> when non-empty).
static void push(const std::string& pid, const EntryPtr& e, const std::string& nick) {
  laila->peer(pid)->memorize(e, nick)->wait();  // throws on peer error (Python parity)
}

// Peer with <uri> and pull <gid> (from the pool <nick> when non-empty).
static FuturePtr pull(const std::string& uri, const std::string& secret, const std::string& gid,
                     const std::string& nick) {
  std::string pid = connect(uri, secret);
  RememberOpts opts;
  opts.policy_id = pid;
  opts.persist = false;
  if (!nick.empty()) opts.pool_nickname = nick;
  return laila->remember(gid, opts);
}

static int do_memorize(const std::string& uri, const std::string& secret, const std::string& value,
                       const std::string& nick) {
  std::string pid = connect(uri, secret);
  auto e = laila->constant(LailaValue::from_string(value));
  push(pid, e, nick);
  std::printf("MEMO %s\n", e->global_id().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_memorize_typed(const std::string& uri, const std::string& secret,
                             const std::string& kind, const std::string& nick) {
  std::string pid = connect(uri, secret);
  auto e = build_typed(kind);
  push(pid, e, nick);
  std::printf("MEMO %s\n", e->global_id().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_memorize_json(const std::string& uri, const std::string& secret,
                            const std::string& text, const std::string& nick) {
  std::string pid = connect(uri, secret);
  auto e = laila->constant(json_to_value(Json::parse(text)));
  push(pid, e, nick);
  std::printf("MEMO %s\n", e->global_id().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_remember_json(const std::string& uri, const std::string& secret,
                            const std::string& gid, const std::string& nick) {
  const LailaValue& got = pull(uri, secret, gid, nick)->data();
  Json j;
  if (!value_to_json(got, j)) {
    std::printf("JSON_TYPE %s\n", describe(got).c_str());
  } else {
    std::string text;
    dump_json_text(j, text);
    std::printf("JSON %s\n", text.c_str());
  }
  std::fflush(stdout);
  return 0;
}

static int do_memorize_bytes(const std::string& uri, const std::string& secret, size_t n,
                             uint32_t seed, const std::string& nick) {
  std::string pid = connect(uri, secret);
  auto data = xorshift_bytes(n, seed);
  std::string sum = fnv1a64(data);
  auto e = laila->constant(LailaValue::from_bytes(std::move(data)));
  push(pid, e, nick);
  std::printf("MEMO %s %s\n", e->global_id().c_str(), sum.c_str());
  std::fflush(stdout);
  return 0;
}

static int do_remember_hash(const std::string& uri, const std::string& secret,
                            const std::string& gid, const std::string& nick) {
  const LailaValue& got = pull(uri, secret, gid, nick)->data();
  if (got.kind() != LailaValue::Kind::Bytes)
    std::printf("HASH_TYPE %s\n", describe(got).c_str());
  else
    std::printf("HASH %s %zu\n", fnv1a64(got.as_bytes()).c_str(), got.as_bytes().size());
  std::fflush(stdout);
  return 0;
}

static int do_remember_typed(const std::string& uri, const std::string& secret,
                             const std::string& gid, const std::string& kind) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string pid = laila->add_peer(uri, secret);
  RememberOpts opts;
  opts.policy_id = pid;
  opts.persist = false;
  const LailaValue& got = laila->remember(gid, opts)->data();
  if (typed_matches(kind, got))
    std::printf("TYPED_OK %s\n", kind.c_str());
  else
    std::printf("TYPED_MISMATCH %s %s\n", kind.c_str(), describe(got).c_str());
  std::fflush(stdout);
  return 0;
}

// 3-party: this policy (A) asks the policy at via_uri (B) to pull <gid> from the
// policy <from_gid> (C, which B is already peered to) into B's own memory, then
// A reads it back from B.
static int do_relay(const std::string& via_uri, const std::string& secret,
                    const std::string& from_gid, const std::string& gid, const std::string& nick) {
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  std::string via = laila->add_peer(via_uri, secret);
  RememberOpts relay;
  relay.src_policy = via;
  relay.dst_policy = from_gid;
  if (!nick.empty()) relay.dst_pool = nick;
  laila->remember(gid, relay)->wait();
  RememberOpts back;
  back.policy_id = via;
  back.persist = false;
  std::printf("VALUE %s\n", laila->remember(gid, back)->data().as_string().c_str());
  std::fflush(stdout);
  return 0;
}

static int do_forget(const std::string& uri, const std::string& secret, const std::string& gid,
                     const std::string& nick) {
  std::string pid = connect(uri, secret);
  laila->peer(pid)->forget(gid, nick)->wait();
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
    if (mode == "serve" && argc >= 5) {
      // Optional leading scheme ("tcp" / "ws"): one listener serves both, so it
      // is accepted for contract parity and otherwise ignored.
      int i = 2;
      std::string first = argv[i];
      if (first == "tcp" || first == "ws") ++i;
      if (argc < i + 3) return usage();
      std::vector<std::string> peers;
      for (int k = i + 3; k < argc; ++k) peers.emplace_back(argv[k]);
      return do_serve((uint16_t)std::stoi(argv[i]), argv[i + 1], argv[i + 2], peers);
    }
    if (mode == "remember" && argc >= 5)
      return do_remember(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "memorize" && argc >= 5)
      return do_memorize(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "memorize_typed" && argc >= 5)
      return do_memorize_typed(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "remember_typed" && argc >= 6)
      return do_remember_typed(argv[2], argv[3], argv[4], argv[5]);
    if (mode == "memorize_json" && argc >= 5)
      return do_memorize_json(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "remember_json" && argc >= 5)
      return do_remember_json(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "memorize_bytes" && argc >= 6)
      return do_memorize_bytes(argv[2], argv[3], (size_t)std::stoull(argv[4]),
                               (uint32_t)std::stoul(argv[5]), argc >= 7 ? argv[6] : "");
    if (mode == "remember_hash" && argc >= 5)
      return do_remember_hash(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    if (mode == "relay" && argc >= 6)
      return do_relay(argv[2], argv[3], argv[4], argv[5], argc >= 7 ? argv[6] : "");
    if (mode == "forget" && argc >= 5)
      return do_forget(argv[2], argv[3], argv[4], argc >= 6 ? argv[5] : "");
    return usage();
#if !defined(LAILA_NO_EXCEPTIONS)
  } catch (const std::exception& e) {
    std::printf("ERROR %s\n", e.what());
    std::fflush(stdout);
    return 1;
  }
#endif
}
