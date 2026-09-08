// The `laila` facade. Mirrors laila/__init__.py's top-level verbs and module
// properties so user code reads identically modulo `.`->`->`:
//
//     auto e = laila_c::Entry::constant(laila_c::LailaValue::from_json(...));
//     laila->memorize(e)->wait();
//     auto got = laila->remember(e->global_id())->data();
//
// `laila` is a global facade pointer; `Entry::constant` mirrors the classmethod.
#ifndef LAILA_LAILA_HPP
#define LAILA_LAILA_HPP

#include <memory>
#include <string>
#include <vector>

#include "laila/args.hpp"
#include "laila/entry.hpp"
#include "laila/future.hpp"
#include "laila/logger.hpp"
#include "laila/manifest.hpp"
#include "laila/opts.hpp"
#include "laila/pointer.hpp"
#include "laila/policy.hpp"
#include "laila/pool.hpp"
#include "laila/runtime.hpp"

namespace laila_c {

// Proxy members for the active policy's central components. Spelled with
// operator-> so user code reads laila->memory->extend(...),
// laila->command->..., laila->communication->... -- the mirror of
// laila.memory.extend(...) modulo `.`->`->`. operator-> re-resolves the active
// policy on every access (the active policy can change at runtime).
struct _CentralMemoryProxy {
  CentralMemory* operator->() const { return &get_active_policy()->memory(); }
  CentralMemory& operator*() const { return get_active_policy()->memory(); }
};
struct _CentralCommandProxy {
  CentralCommand* operator->() const { return &get_active_policy()->command(); }
  CentralCommand& operator*() const { return get_active_policy()->command(); }
};
struct _CentralCommunicationProxy {
  CentralCommunication* operator->() const { return &get_active_policy()->communication(); }
  CentralCommunication& operator*() const { return get_active_policy()->communication(); }
};

// Facade over the active policy. Methods forward to central.command/memory and
// always return a Future (policy.md: all work returns a Future).
class Laila {
public:
  // Construction verbs (laila.constant / laila.variable / laila.contingent).
  EntryPtr constant(const LailaValue& data, const ConstantOpts& opts = {}) {
    return Entry::constant(data, opts);
  }
  EntryPtr variable(const LailaValue& data = LailaValue::none(), const VariableOpts& opts = {}) {
    return Entry::variable(data, opts);
  }
  // laila.manifest(...) (== the Manifest class): build a blueprint manifest.
  ManifestPtr manifest(const ManifestOpts& opts = {}) { return Manifest::create(opts); }

  // Core verbs (local + 2-party peer + 3-party relay via the opts structs).
  FuturePtr memorize(const EntryPtr& entry);
  FuturePtr memorize(const EntryPtr& entry, const MemorizeOpts& opts);
  FuturePtr memorize(const std::vector<EntryPtr>& entries);  // -> GroupFuture
  FuturePtr remember(const std::string& entry_id, const RememberOpts& opts = {});
  FuturePtr forget(const std::string& entry_id);
  FuturePtr forget(const std::string& entry_id, const ForgetOpts& opts);
  FuturePtr build(const EntryPtr& entry);

  std::vector<std::string> terminate();

  // central.{memory,command,communication}: proxy members accessed with `->`,
  // so user code reads laila->memory->extend(...), laila->command->...,
  // laila->communication->... (mirrors laila.memory.extend(...) etc.).
  _CentralMemoryProxy memory;
  _CentralCommandProxy command;
  _CentralCommunicationProxy communication;
  PoolPtr alpha_pool() { return get_active_policy()->memory().alpha_pool(); }
  Logger& logger() { return Logger::get(); }

  PolicyPtr active_policy() { return get_active_policy(); }
  std::string add_peer(const std::string& uri, const std::string& secret) {
    return get_active_policy()->communication().add_peer(uri, secret);
  }
  // laila.communication.add_tcpip_peer(host, port, secret).
  std::string add_tcpip_peer(const std::string& host, uint16_t port, const std::string& secret) {
    return get_active_policy()->communication().add_tcpip_peer(host, port, secret);
  }
  // laila.communication.add_connection(protocol): register the protocol and, when
  // the platform can serve the link, bring its inbound listener up automatically
  // (no listen()/poll() in the public API -- the mirror of laila's add_connection).
  void add_connection(const CommProtocolPtr& protocol) {
    get_active_policy()->communication().add_connection(protocol);
  }
  // laila.communication.remove_connection(protocol): stop + drop a protocol.
  void remove_connection(const CommProtocolPtr& protocol) {
    get_active_policy()->communication().remove_connection(protocol);
  }
  RemotePolicyProxyPtr peer(const std::string& peer_id) {
    return get_active_policy()->communication().peer(peer_id);
  }
  // laila.request: invoke a method on a peer policy, returning a Future.
  FuturePtr request(const std::string& peer_id, const std::string& method, const Json& args) {
    return get_active_policy()->communication().request(peer_id, method, args);
  }
  // laila.guarantee (utils/guarantee.py): used as `{ auto g = laila->guarantee(); ... }`
  // to block until every Future created in the scope completes. Returned by value;
  // C++17 guaranteed copy elision constructs it directly into the caller's guard.
  _Guarantee guarantee() { return _Guarantee(); }

  // laila.read_args / laila.args (utils/args): load a config file into the
  // process-wide args store, then read values via laila->args().get("KEY").
  void read_args(const std::string& source) { laila_c::read_args(source); }
  _LailaArgs& args() { return laila_c::args(); }

  void set_active_namespace(const std::string& key) { laila_c::set_active_namespace(key); }
};

}  // namespace laila_c

// The global facade lives in the GLOBAL namespace so user code reads exactly
// like laila Python modulo `.`->`->`:  laila->memorize(...), laila->constant(...)
extern laila_c::Laila* laila;

#endif  // LAILA_LAILA_HPP
