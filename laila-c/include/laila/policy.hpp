// Policy mirror (policy/*). The "vehicle" with four central components
// (policy.md): command (engine; returns Futures), memory (pools), communication
// (peers/telemetry), control (logic; unimplemented). Golden rules enforced:
// only central.memory touches pools; laila is "started" iff >=1 local policy
// exists and the active policy is consistent with that.
#ifndef LAILA_POLICY_HPP
#define LAILA_POLICY_HPP

#include <atomic>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "laila/cli_capable.hpp"
#include "laila/communication.hpp"
#include "laila/entry.hpp"
#include "laila/future.hpp"
#include "laila/identity.hpp"
#include "laila/pool.hpp"

namespace laila_c {

class _LAILA_IDENTIFIABLE_POLICY;
// Compatibility spelling; the faithful (Python) name is primary.
using Policy = _LAILA_IDENTIFIABLE_POLICY;

// central.command: all work flows through here and returns a Future.
class _LAILA_IDENTIFIABLE_CENTRAL_COMMAND : public _LAILA_CLI_CAPABLE_CLASS,
                                           public _LAILA_IDENTIFIABLE_OBJECT {
public:
  explicit _LAILA_IDENTIFIABLE_CENTRAL_COMMAND(Policy* policy) : policy_(policy) {}
  // Submit a unit of work; returns a Future registered in the policy bank.
  FuturePtr submit(std::function<EntryPtr()> work);

  // Guarantee-scope stack (mirrors laila's central.command _guarantee_* helpers).
  // While a scope is active, every Future created via submit() registers with the
  // top frame; the `with laila.guarantee:` RAII guard (_Guarantee) waits for them
  // on scope exit. Accessed from the submitting/owning thread only.
  void _guarantee_enter() { guarantee_stack_.emplace_back(); }
  std::vector<FuturePtr> _guarantee_exit() {
    std::vector<FuturePtr> created;
    if (!guarantee_stack_.empty()) {
      created = std::move(guarantee_stack_.back());
      guarantee_stack_.pop_back();
    }
    return created;
  }
  void _guarantee_register(const FuturePtr& f) {
    if (!guarantee_stack_.empty()) guarantee_stack_.back().push_back(f);
  }

private:
  Policy* policy_;
  std::vector<std::vector<FuturePtr>> guarantee_stack_;
};
using CentralCommand = _LAILA_IDENTIFIABLE_CENTRAL_COMMAND;

// central.memory: the ONLY component allowed to touch pools directly.
class _LAILA_IDENTIFIABLE_CENTRAL_MEMORY : public _LAILA_CLI_CAPABLE_CLASS,
                                          public _LAILA_IDENTIFIABLE_OBJECT {
public:
  explicit _LAILA_IDENTIFIABLE_CENTRAL_MEMORY(Policy* policy);
  // extend(pool, *, affinity=None, pool_nickname=None): register a pool with
  // central memory; first registered is alpha.
  void extend(const PoolPtr& pool, const ExtendOpts& opts = {});
  PoolPtr alpha_pool() const { return pool_router_.route({}); }  // default-nickname pool
  PoolPtr pool_by_nickname(const std::string& nickname) const;
  // central.memory.pool_router (policy/central/memory/schema/base.py): the router
  // that registers pools and resolves routing.
  _LAILA_IDENTIFIABLE_POOL_ROUTER& pool_router() { return pool_router_; }
  const _LAILA_IDENTIFIABLE_POOL_ROUTER& pool_router() const { return pool_router_; }

  // Pool-routed operations (central.memory.memorize/remember/forget). The empty
  // pool_nickname routes to the default (_memory) pool. For remember, `persist`
  // caches the fetched entry back into the default pool when the source pool is
  // not the default (mirrors laila's persist semantics; a write on flash targets).
  EntryPtr memorize(const EntryPtr& e, const std::string& pool_nickname = "");
  EntryPtr remember(const std::string& global_id, const std::string& pool_nickname = "",
                    bool persist = true);
  EntryPtr forget(const std::string& global_id, const std::string& pool_nickname = "");

private:
  Policy* policy_;
  _LAILA_IDENTIFIABLE_POOL_ROUTER pool_router_;
};
using CentralMemory = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY;

// Client-side stand-in for a remote (or in-process) peer policy. Mirrors
// laila's RemotePolicyProxy: method calls are routed to the peer, returning a
// RemoteFuture. laila uses dynamic attribute chains (proxy.central.memory.
// remember(...)); C++ uses the explicit request(path, args) form, with
// remember/memorize/forget convenience verbs on top.
class RemotePolicyProxy {
public:
  RemotePolicyProxy(std::string peer_id, std::shared_ptr<Policy> in_process)
      : peer_id_(std::move(peer_id)), local_(std::move(in_process)) {}
  RemotePolicyProxy(std::string peer_id, CommProtocolPtr transport)
      : peer_id_(std::move(peer_id)), transport_(std::move(transport)) {}

  const std::string& peer_id() const { return peer_id_; }
  bool is_remote() const { return transport_ != nullptr; }

  // method is a dotted path on the remote policy (e.g. "remember" or
  // "central.memory.remember"); args is a JSON array of positional arguments and
  // kwargs an (optional) object mirroring laila's RPC params {path,args,kwargs}.
  FuturePtr request(const std::string& method, const Json& args, const Json& kwargs = {});
  FuturePtr remember(const std::string& entry_id, const std::string& pool_nickname = "",
                     bool persist = true);
  FuturePtr forget(const std::string& entry_id, const std::string& pool_nickname = "");
  FuturePtr memorize(const EntryPtr& entry, const std::string& pool_nickname = "");

private:
  std::string peer_id_;
  std::shared_ptr<Policy> local_;   // in-process peer
  CommProtocolPtr transport_;       // networked peer (JSON-RPC over a link)
};
using RemotePolicyProxyPtr = std::shared_ptr<RemotePolicyProxy>;

// central.communication. A "peer" is another policy reachable in-process (by
// global_id) or over a network link (uri scheme, e.g. tcp://host:port). The
// peer registry mirrors laila's communication.peers.
class _LAILA_IDENTIFIABLE_COMMUNICATION : public _LAILA_CLI_CAPABLE_CLASS,
                                         public _LAILA_IDENTIFIABLE_OBJECT {
public:
  explicit _LAILA_IDENTIFIABLE_COMMUNICATION(Policy* policy) : policy_(policy) {}
  ~_LAILA_IDENTIFIABLE_COMMUNICATION();
  // uri: a local policy global_id (in-process) OR a network uri (tcp://host:port,
  // ws://..., etc.; requires a supported Transport, else Status::Unsupported).
  // Returns the peer id and registers a RemotePolicyProxy.
  std::string add_peer(const std::string& uri, const std::string& secret);
  // laila.communication.add_tcpip_peer(host, port, secret): register a TCP peer
  // by host/port (sugar over add_peer("tcp://host:port", secret)).
  std::string add_tcpip_peer(const std::string& host, uint16_t port, const std::string& secret);
  RemotePolicyProxyPtr peer(const std::string& peer_id) const;
  const std::map<std::string, RemotePolicyProxyPtr>& peers() const { return peers_; }

  // Generic RPC to a peer (the laila.request entry point).
  FuturePtr request(const std::string& peer_id, const std::string& method, const Json& args);
  // Convenience: fetch an entry from a peer's memory (optionally a named pool on
  // the peer; persist controls cache-back into the peer's alpha pool).
  FuturePtr remote_remember(const std::string& peer_id, const std::string& entry_id,
                            const std::string& pool_nickname = "", bool persist = true);

  // Open/register a raw protocol connection (any ConnectionType). Mirrors
  // laila's central.communication.connections runtime registry.
  CommProtocolPtr connect(ConnectionType type, ConnectionConfig cfg = {});
  CommProtocolPtr connect_uri(const std::string& uri, const std::string& secret = "");
  // laila.communication.add_connection(protocol): register a protocol and, when
  // the active platform can serve that link type, bring up the protocol's inbound
  // listener (bound to its host/port) and begin accepting peer RPCs in the
  // background -- exactly like laila's add_connection -> protocol.start(). No
  // user-driven listen()/poll(); the listener is live when this returns.
  void add_connection(const CommProtocolPtr& protocol);
  // laila.communication.remove_connection(protocol): stop the protocol (close its
  // inbound listener + join its serve thread) and drop it from the registry.
  void remove_connection(const CommProtocolPtr& protocol);
  const std::map<std::string, CommProtocolPtr>& connections() const { return connections_; }

private:
  // One inbound-serving record per add_connection that opened a listener: the
  // listener, its background accept/dispatch thread, and a per-record stop flag
  // so a single connection can be torn down (remove_connection) or all of them
  // (destructor) deterministically -- without a user-driven listen()/poll().
  struct ServeRecord {
    std::string protocol_id;
    hal::Listener* listener = nullptr;
    std::shared_ptr<std::atomic<bool>> stop;
    std::thread thread;
  };

  // Serve one accepted connection: a WebSocket session (Python laila peer) or the
  // raw one-shot JSON-RPC frame used by a laila-C TCP peer. `expected_secret` is
  // the receiving protocol's peer_secret_key (empty => accept any).
  void serve_connection(hal::Connection* conn, const std::vector<uint8_t>& first,
                        const std::string& expected_secret,
                        const std::shared_ptr<std::atomic<bool>>& stop);
  // Background accept/dispatch loop for one listener; exits when *stop is set.
  void serve_loop(hal::Listener* l, std::string expected_secret,
                  std::shared_ptr<std::atomic<bool>> stop);
  // Stop + join one record (by index) and reclaim its listener.
  void stop_record(ServeRecord& rec);
  // Stop every serve thread and reclaim listeners. Called by the destructor.
  void stop_all_serving();

  Policy* policy_;
  std::map<std::string, RemotePolicyProxyPtr> peers_;
  std::map<std::string, CommProtocolPtr> connections_;
  std::vector<ServeRecord> serving_;
};
using CentralCommunication = _LAILA_IDENTIFIABLE_COMMUNICATION;

// Dispatch one inbound JSON-RPC frame (laila's {method:"rpc.call", params:{path,
// args,kwargs}} shape) against a policy's central.memory and return the response
// frame. Serves central.memory.{remember,memorize,forget} on the LOCAL policy
// (rule-compliant: a policy only ever serves its own memory).
Json dispatch_rpc(Policy* policy, const Json& request);

class _LAILA_IDENTIFIABLE_POLICY : public _LAILA_CLI_CAPABLE_CLASS,
                                  public _LAILA_IDENTIFIABLE_OBJECT {
public:
  _LAILA_IDENTIFIABLE_POLICY();
  CentralCommand& command() { return command_; }
  CentralMemory& memory() { return memory_; }
  CentralCommunication& communication() { return communication_; }

  std::map<std::string, FuturePtr>& future_bank() { return future_bank_; }

private:
  CentralCommand command_;
  CentralMemory memory_;
  CentralCommunication communication_;
  std::map<std::string, FuturePtr> future_bank_;
};
// laila `DefaultPolicy` alias (macros/defaults.py) + shared-ptr handle.
using DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY;
using PolicyPtr = std::shared_ptr<_LAILA_IDENTIFIABLE_POLICY>;

// ---- Policy registry / lifecycle (policy.md golden rules) ----
PolicyPtr get_active_policy();         // lazily activates a DefaultPolicy
void activate_policy(const PolicyPtr& p);
const std::map<std::string, PolicyPtr>& local_policies();

// RAII mirror of laila's `with laila.guarantee:` context manager (utils/guarantee.py).
// Construction enters a guarantee scope on the active policy's central.command;
// destruction pops the scope and synchronously waits for every Future created
// inside it. A C++ `{ ... }` block plays the role of the Python `with` body:
//
//     {
//       auto g = laila->guarantee();         // with laila.guarantee:
//       laila->memorize(entry, {...});       // registers with this scope
//     }                                      // ~g: waits for all in-scope futures
//
// The first in-scope error is re-raised on exit, but only when no exception is
// already propagating (std::uncaught_exceptions() == 0) -- mirroring laila's
// "don't mask the body's exception" rule (__exit__ returning False).
class _Guarantee {
public:
  _Guarantee() { get_active_policy()->command()._guarantee_enter(); }
  ~_Guarantee() noexcept(false) {
    auto created = get_active_policy()->command()._guarantee_exit();
    std::exception_ptr first;
    for (auto& f : created) {
      if (!f) continue;
#if defined(LAILA_NO_EXCEPTIONS)
      f->wait();
#else
      try { f->wait(); } catch (...) { if (!first) first = std::current_exception(); }
#endif
    }
#if !defined(LAILA_NO_EXCEPTIONS)
    if (first && std::uncaught_exceptions() == 0) std::rethrow_exception(first);
#endif
  }
  _Guarantee(const _Guarantee&) = delete;
  _Guarantee& operator=(const _Guarantee&) = delete;
};

}  // namespace laila_c

#endif  // LAILA_POLICY_HPP
