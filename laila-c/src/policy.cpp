#include "laila/policy.hpp"

#include <algorithm>

#include "laila/framing.hpp"
#include "laila/hal/hal.hpp"
#include "laila/status.hpp"
#include "laila/websocket.hpp"

namespace laila_c {

// ---------------- CentralCommand ----------------
FuturePtr CentralCommand::submit(std::function<EntryPtr()> work) {
  auto fut = std::make_shared<ConcurrentPackageFuture>();
  { hal::Guard g; policy_->future_bank()[fut->global_id()] = fut; }
  _guarantee_register(fut);  // register with the active `with laila.guarantee:` scope
  fut->set_status(FutureStatus::RUNNING);
  hal::Executor& ex = hal::get().executor();
  // The completion logic runs inside the executor task. Cooperative executors
  // run this inline; threaded executors run it on a worker.
  ex.submit([fut, work]() {
#if defined(LAILA_NO_EXCEPTIONS)
    EntryPtr r = work();
    fut->set_result_entry(r);
    fut->set_status(FutureStatus::FINISHED);
#else
    try {
      EntryPtr r = work();
      fut->set_result_entry(r);
      fut->set_status(FutureStatus::FINISHED);
    } catch (const LailaError& e) {
      fut->set_exception(e.code(), e.what());
      fut->set_status(FutureStatus::ERROR);
    } catch (const std::exception& e) {
      fut->set_exception(Status::Error, e.what());
      fut->set_status(FutureStatus::ERROR);
    }
#endif
  });
  return fut;
}

// ---------------- CentralMemory ----------------
_LAILA_IDENTIFIABLE_CENTRAL_MEMORY::_LAILA_IDENTIFIABLE_CENTRAL_MEMORY(Policy* policy) : policy_(policy) {
  auto mem = std::make_shared<DefaultPool>();
  mem->set_nickname("_memory");
  extend(mem, {.pool_nickname = "_memory"});
}

void CentralMemory::extend(const PoolPtr& pool, const ExtendOpts& opts) {
  pool_router_.extend(pool, opts);
}

PoolPtr CentralMemory::pool_by_nickname(const std::string& nickname) const {
  RouteOpts ro;
  if (!nickname.empty()) ro.pool_nickname = nickname;
  return pool_router_.route(ro);
}

EntryPtr CentralMemory::memorize(const EntryPtr& e, const std::string& pool_nickname) {
  RouteOpts ro;
  if (!pool_nickname.empty()) ro.pool_nickname = pool_nickname;
  PoolPtr target = pool_router_.route(ro);
  // A variable whose payload was re-assigned since its last memorize becomes
  // a new evolution (in place); an untouched one is re-written under the same key.
  e->bump_evolution_if_locally_modified();
  const TransformationSequence& t = target->transformations();
  Json blob = e->serialize(t.empty() ? nullptr : &t);
  target->put(e->global_id(), blob);
  e->mark_memorized();
  return e;
}

namespace {
// Evolution encoded in a storage key; -1 for a constant (no '@').
int64_t key_evolution(const std::string& key) {
  size_t at = key.find('@');
  if (at == std::string::npos) return -1;
  for (const auto& kv : parse_global_id_attributes(key.substr(at + 1))) {
    if (kv.first != EVOLUTION_ATTRIBUTE) continue;
    for (char c : kv.second)
      if (c < '0' || c > '9') return -1;
    return kv.second.empty() ? -1 : std::stoll(kv.second);
  }
  return -1;
}

bool is_int_literal(const std::string& s) {
  if (s.empty()) return false;
  size_t i = s[0] == '-' ? 1 : 0;
  if (i >= s.size()) return false;
  for (; i < s.size(); ++i)
    if (s[i] < '0' || s[i] > '9') return false;
  return true;
}
}  // namespace

std::pair<std::string, std::optional<Json>> CentralMemory::_resolve_entry_key(
    const PoolPtr& pool, const std::string& eid) {
  auto split = split_global_id_attributes(eid);
  const std::string& base = split.first;
  std::optional<int64_t> evolution;
  std::optional<std::string> creation_timestamp;
  for (const auto& kv : split.second) {
    if (kv.first == EVOLUTION_ATTRIBUTE) {
      if (!is_int_literal(kv.second)) raise(Status::Error, "Invalid evolution in GID: " + eid);
      evolution = std::stoll(kv.second);
    } else if (kv.first == CREATION_TIMESTAMP_ATTRIBUTE) {
      creation_timestamp = kv.second;
    } else {
      raise(Status::Error, "Unsupported search attribute '" + kv.first + "' in " + eid +
                               "; supported: evolution, creation_timestamp");
    }
  }

  // Fast paths: a non-negative explicit evolution is an exact key; a bare id
  // that exists as-is (a constant) needs no lookup.
  if (!creation_timestamp.has_value()) {
    if (evolution.has_value() && *evolution >= 0) return {eid, std::nullopt};
    if (!evolution.has_value() && pool->exists(base)) return {base, std::nullopt};
  }

  // 1. Index-first, tier by tier, validating every hit. A hit whose key is
  //    gone is removed from the shard (repairing it) and the tier asked again.
  for (_LAILA_IDENTIFIABLE_POOL* tier = pool.get(); tier != nullptr; tier = tier->proxy_to()) {
    for (int attempt = 0; attempt < 64; ++attempt) {
      auto key = tier->_resolve_indexed(base, split.second);
      if (!key.has_value()) break;
      if (tier->exists(*key)) return {*key, std::nullopt};
      tier->index().remove(*key);
    }
  }

  // 2. Scan fallback: candidate (tier, key) pairs across the proxy chain.
  std::vector<std::pair<_LAILA_IDENTIFIABLE_POOL*, std::string>> candidates;
  const std::string prefix = base + "@";
  for (_LAILA_IDENTIFIABLE_POOL* tier = pool.get(); tier != nullptr; tier = tier->proxy_to()) {
    auto indexed = tier->_search_keys(base, split.second);
    std::vector<std::string> keys = indexed.has_value() ? *indexed : tier->_candidate_keys(base);
    for (const auto& key : keys) {
      if (key != base && key.compare(0, prefix.size(), prefix) != 0) continue;
      if (evolution.has_value() && *evolution >= 0 && key_evolution(key) != *evolution) continue;
      candidates.emplace_back(tier, key);
    }
  }

  if (evolution.has_value() && *evolution < 0 && !candidates.empty()) {
    // k-th from the end over the sorted, de-duplicated evolutions (constants rank lowest).
    std::vector<int64_t> ranked;
    for (const auto& c : candidates) {
      int64_t r = key_evolution(c.second);
      if (std::find(ranked.begin(), ranked.end(), r) == ranked.end()) ranked.push_back(r);
    }
    std::sort(ranked.begin(), ranked.end());
    int64_t idx = static_cast<int64_t>(ranked.size()) + *evolution;
    if (idx < 0) {
      candidates.clear();
    } else {
      int64_t wanted = ranked[static_cast<size_t>(idx)];
      std::vector<std::pair<_LAILA_IDENTIFIABLE_POOL*, std::string>> kept;
      for (const auto& c : candidates)
        if (key_evolution(c.second) == wanted) kept.push_back(c);
      candidates = std::move(kept);
    }
  }

  if (!creation_timestamp.has_value()) {
    if (candidates.empty())
      raise(Status::NotFound, "Entry " + eid + " not found in pool " + pool->global_id());
    if (!evolution.has_value()) {
      // Prefer an exact (constant) key; otherwise the highest evolution.
      for (const auto& c : candidates)
        if (c.second == base) return {c.second, std::nullopt};
    }
    const auto* best = &candidates.front();
    for (const auto& c : candidates)
      if (key_evolution(c.second) > key_evolution(best->second)) best = &c;
    return {best->second, std::nullopt};
  }

  // Time-based search: inspect each candidate record's creation_timestamp on the tier
  // that holds it (no write-back into the front tier).
  std::optional<std::pair<std::string, Json>> best;
  for (const auto& c : candidates) {
    auto raw = c.first->read_local(c.second);
    if (!raw.has_value()) continue;
    auto hb = record_creation_timestamp(*raw);
    if (!hb.has_value() || *hb != *creation_timestamp) continue;
    if (!best.has_value() || key_evolution(c.second) > key_evolution(best->first))
      best = std::make_pair(c.second, *raw);
  }
  if (!best.has_value())
    raise(Status::NotFound, "No evolution of " + base + " with creation_timestamp='" + *creation_timestamp +
                                "' in pool " + pool->global_id());
  return {best->first, best->second};
}

EntryPtr CentralMemory::remember(const std::string& global_id, const std::string& pool_nickname,
                                 bool persist) {
  PoolPtr def = pool_router_.route({});  // default-nickname (_memory) pool
  RouteOpts ro;
  if (!pool_nickname.empty()) ro.pool_nickname = pool_nickname;
  PoolPtr source = pool_router_.route(ro);
  auto resolved = _resolve_entry_key(source, global_id);
  std::optional<Json> blob = resolved.second;
  if (!blob.has_value()) blob = source->get(resolved.first);
  if (!blob.has_value()) raise(Status::NotFound, "Entry not found: " + global_id);
  EntryPtr e = Entry::build_from_dict(*blob);
  // Cache-back: when reading from a non-default pool and persist is requested,
  // also write into the default pool (a flash write on flash-backed targets).
  if (persist && source != def) {
    const TransformationSequence& t = def->transformations();
    def->put(e->global_id(), e->serialize(t.empty() ? nullptr : &t));
  }
  return e;
}

EntryPtr CentralMemory::forget(const std::string& global_id, const std::string& pool_nickname) {
  RouteOpts ro;
  if (!pool_nickname.empty()) ro.pool_nickname = pool_nickname;
  PoolPtr target = pool_router_.route(ro);
  // A negative evolution ("@evolution=-1" = latest) is resolved to the
  // concrete key; an evolution-less reference stays exact-key.
  std::string key = global_id;
  for (const auto& kv : split_global_id_attributes(global_id).second) {
    if (kv.first == EVOLUTION_ATTRIBUTE && !kv.second.empty() && kv.second[0] == '-') {
      key = _resolve_entry_key(target, global_id).first;
      break;
    }
  }
  target->erase(key);
  return Entry::constant(LailaValue::none());
}

// ---------------- RemotePolicyProxy ----------------
namespace {
// Send one JSON-RPC frame over a peer transport and return the parsed "result".
// For the persistent WebSocket transport this rides the open session; for the
// raw-TCP one-shot transport connect() opens a fresh frame exchange.
Json peer_send_recv(const CommProtocolPtr& tr, const Json& req) {
  tr->connect();
  tr->send_text(req.dump());
  std::vector<uint8_t> resp;
  bool got = tr->recv(resp, 5000);
  // One-shot transports (raw TCP) get a fresh connection per request, matching
  // the inbound one-frame-per-connection server; the persistent WS session stays
  // open across requests.
  if (!tr->persistent()) tr->close();
  if (!got) raise(Status::Timeout, "peer RPC timed out");
  Json r = Json::parse(std::string(resp.begin(), resp.end()));
  if (r.contains("error") && !r.at("error").is_null())
    raise(Status::Error, "peer RPC error: " + r.at("error").dump());
  return r.at("result");
}

// JSON-RPC request frame, byte-compatible with laila's protocol.make_request
// ("rpc.call" with params {path, args, kwargs}).
Json make_rpc_call(const std::string& method, const Json& args, const Json& kwargs) {
  Json path = Json::array();
  std::string cur;
  for (char c : method) {
    if (c == '.') { path.push_back(Json(cur)); cur.clear(); }
    else cur.push_back(c);
  }
  path.push_back(Json(cur));
  Json params = Json::object();
  params["path"] = path;
  params["args"] = args.is_array() ? args : Json::array();
  params["kwargs"] = kwargs.is_object() ? kwargs : Json::object();
  Json req = Json::object();
  req["jsonrpc"] = std::string("2.0");
  req["id"] = uuid4();
  req["method"] = std::string("rpc.call");
  req["params"] = params;
  return req;
}

// Read a string/bool kwarg with a default (the in-process and server paths share
// these so pool_nickname/persist behave identically locally and over the wire).
std::string kw_str(const Json& kwargs, const std::string& key) {
  return kwargs.is_object() && kwargs.contains(key) && kwargs.at(key).is_string()
             ? kwargs.at(key).as_string() : std::string();
}
bool kw_bool(const Json& kwargs, const std::string& key, bool dflt) {
  if (kwargs.is_object() && kwargs.contains(key) &&
      kwargs.at(key).type() == Json::Type::Bool)
    return kwargs.at(key).as_bool();
  return dflt;
}

// JSON-RPC 2.0 error frame echoing the request id (when present).
Json make_rpc_error(const Json& req, int code, const std::string& message) {
  Json resp = Json::object();
  resp["jsonrpc"] = std::string("2.0");
  resp["id"] = req.is_object() && req.contains("id") ? req.at("id") : Json();
  Json err = Json::object();
  err["code"] = (int64_t)code;
  err["message"] = message;
  resp["error"] = err;
  return resp;
}

// laila's cross-the-wire entry form: serialize(transformation_base64) ->
// {payload: base64(pickle/msgpack/npy bytes), constitution: {simple, codes:[...]}}.
Json serialize_wire(const EntryPtr& e) {
  TransformationSequence t = TransformationSequence::base64();
  return e->serialize(&t);
}

// Run one RPC against a peer: in-process peers dispatch directly against the
// peer policy (mirroring the wire), networked peers send a framed JSON-RPC frame.
// Returns the parsed "result".
Json do_proxy_call(const std::shared_ptr<Policy>& local, const CommProtocolPtr& transport,
                   const Json& req) {
  if (local) {
    Json resp = dispatch_rpc(local.get(), req);
    if (resp.contains("error") && !resp.at("error").is_null())
      raise(Status::Error, "peer error: " + resp.at("error").dump());
    return resp.at("result");
  }
  return peer_send_recv(transport, req);
}

// Build a single-element list arg (Python passes [gid]/[blob] to _remote_*).
Json one_list(const Json& item) { Json a = Json::array(); a.push_back(item); return a; }
}  // namespace

FuturePtr RemotePolicyProxy::request(const std::string& method, const Json& args,
                                     const Json& kwargs) {
  PolicyPtr home = get_active_policy();
  std::shared_ptr<Policy> local = local_;
  CommProtocolPtr tr = transport_;
  Json req = make_rpc_call(method, args, kwargs);
  auto fut = std::make_shared<RemoteFuture>([local, tr, req]() -> EntryPtr {
    Json result = do_proxy_call(local, tr, req);
    // A laila NATIVE policy returns a future envelope for the bare verbs; resolve
    // it the way laila's RemoteFuture does (block via _wait_future -> result gid).
    if (result.is_object() && result.contains("__laila_future__")) {
      Json wargs = one_list(result.contains("global_id") ? result.at("global_id") : Json(std::string()));
      Json gid = do_proxy_call(local, tr, make_rpc_call("_wait_future", wargs, Json::object()));
      return Entry::constant(LailaValue::from_json_payload(gid));
    }
    if (result.is_object() && result.contains("_uuid")) return Entry::build_from_dict(result);
    return Entry::constant(LailaValue::from_json_payload(result));
  });
  { hal::Guard g; home->future_bank()[fut->global_id()] = fut; }
  return fut;
}

// The peer memory verbs use laila's wire protocol: central.memory._remote_* with
// list args, entries shipped as serialize(transformation_base64) blobs, results
// as blob/gid lists.
FuturePtr RemotePolicyProxy::remember(const std::string& entry_id,
                                      const std::string& pool_nickname, bool /*persist*/) {
  PolicyPtr home = get_active_policy();
  std::shared_ptr<Policy> local = local_;
  CommProtocolPtr tr = transport_;
  Json kwargs = Json::object();
  if (!pool_nickname.empty()) kwargs["pool"] = pool_nickname;
  Json req = make_rpc_call("central.memory._remote_remember", one_list(one_list(Json(entry_id))), kwargs);
  auto fut = std::make_shared<RemoteFuture>([local, tr, req]() -> EntryPtr {
    Json result = do_proxy_call(local, tr, req);  // list of serialized blobs
    if (!result.is_array() || result.elements().empty())
      raise(Status::NotFound, "peer returned no entry");
    return Entry::build_from_dict(result.elements().at(0));
  });
  { hal::Guard g; home->future_bank()[fut->global_id()] = fut; }
  return fut;
}

FuturePtr RemotePolicyProxy::forget(const std::string& entry_id, const std::string& pool_nickname) {
  PolicyPtr home = get_active_policy();
  std::shared_ptr<Policy> local = local_;
  CommProtocolPtr tr = transport_;
  Json kwargs = Json::object();
  if (!pool_nickname.empty()) kwargs["pool"] = pool_nickname;
  Json req = make_rpc_call("central.memory._remote_forget", one_list(one_list(Json(entry_id))), kwargs);
  auto fut = std::make_shared<RemoteFuture>([local, tr, req]() -> EntryPtr {
    do_proxy_call(local, tr, req);
    return Entry::constant(LailaValue::none());
  });
  { hal::Guard g; home->future_bank()[fut->global_id()] = fut; }
  return fut;
}

FuturePtr RemotePolicyProxy::memorize(const EntryPtr& entry, const std::string& pool_nickname) {
  PolicyPtr home = get_active_policy();
  std::shared_ptr<Policy> local = local_;
  CommProtocolPtr tr = transport_;
  Json blob = serialize_wire(entry);
  Json kwargs = Json::object();
  if (!pool_nickname.empty()) kwargs["pool"] = pool_nickname;
  Json req = make_rpc_call("central.memory._remote_memorize", one_list(one_list(blob)), kwargs);
  EntryPtr e = entry;
  auto fut = std::make_shared<RemoteFuture>([local, tr, req, e]() -> EntryPtr {
    do_proxy_call(local, tr, req);  // list of stored gids
    return e;
  });
  { hal::Guard g; home->future_bank()[fut->global_id()] = fut; }
  return fut;
}

// ---------------- CentralCommunication ----------------
std::string CentralCommunication::add_peer(const std::string& uri, const std::string& secret) {
  // In-process peer: uri is another local policy's global_id.
  const auto& locals = local_policies();
  auto it = locals.find(uri);
  if (it != locals.end()) {
    peers_[uri] = std::make_shared<RemotePolicyProxy>(uri, it->second);
    return uri;
  }
  // Networked peer: uri carries a scheme (tcp://host:port, ws://..., etc.).
  // The stream (tcp/tls/unix) and WebSocket carriers do laila's peer.connect
  // handshake and register the peer under the remote policy's global_id.
  if (uri.find("://") != std::string::npos) {
    CommProtocolPtr proto = make_protocol_from_uri(uri, secret);
    proto->connect();  // raises Unsupported if the target lacks this link type
    std::string remote_id;
    if (proto->type() == ConnectionType::WebSocket) {
      remote_id = std::static_pointer_cast<WebSocketProtocol>(proto)->peer_connect(policy_->global_id(), secret);
    } else if (proto->type() == ConnectionType::TCP) {
      remote_id = std::static_pointer_cast<TCPIPProtocol>(proto)->peer_connect(policy_->global_id(), secret);
    } else {
      // Other carriers not yet handshake-capable on this target.
      raise(Status::Unsupported, "add_peer: peering not supported over " +
                                     std::string(proto->name()) + " on this target");
    }
    peers_[remote_id] = std::make_shared<RemotePolicyProxy>(remote_id, proto);
    return remote_id;
  }
  raise(Status::Unsupported, "add_peer: '" + uri +
                                 "' is neither a local policy id nor a network uri");
}

std::string CentralCommunication::add_tcpip_peer(const std::string& host, uint16_t port,
                                                 const std::string& secret) {
  return add_peer("tcp://" + host + ":" + std::to_string(port), secret);
}

RemotePolicyProxyPtr CentralCommunication::peer(const std::string& peer_id) const {
  auto it = peers_.find(peer_id);
  return it == peers_.end() ? nullptr : it->second;
}

FuturePtr CentralCommunication::request(const std::string& peer_id, const std::string& method,
                                        const Json& args) {
  auto p = peer(peer_id);
  if (!p) raise(Status::NotFound, "peer not registered: " + peer_id);
  return p->request(method, args);
}

FuturePtr CentralCommunication::remote_remember(const std::string& peer_id,
                                                const std::string& entry_id,
                                                const std::string& pool_nickname, bool persist) {
  auto p = peer(peer_id);
  if (!p) raise(Status::NotFound, "peer not registered: " + peer_id);
  return p->remember(entry_id, pool_nickname, persist);
}

CommProtocolPtr CentralCommunication::connect(ConnectionType type, ConnectionConfig cfg) {
  CommProtocolPtr proto = make_protocol(type, std::move(cfg));
  proto->connect();  // raises Status::Unsupported if the target lacks this link
  connections_[proto->global_id()] = proto;
  return proto;
}

CommProtocolPtr CentralCommunication::connect_uri(const std::string& uri, const std::string& secret) {
  CommProtocolPtr proto = make_protocol_from_uri(uri, secret);
  proto->connect();
  connections_[proto->global_id()] = proto;
  return proto;
}

void CentralCommunication::add_connection(const CommProtocolPtr& protocol) {
  connections_[protocol->global_id()] = protocol;
  // If the platform can serve this link type, stand up the protocol's inbound
  // listener and begin accepting peers in the background (mirrors laila's
  // add_connection -> protocol.start()). The listener is live when this returns.
  if (hal::get().transport().supports_listen(protocol->type())) {
    hal::Listener* l = hal::get().transport().listen(protocol->type(), protocol->config());
    if (l) {
      protocol->set_bound_port(l->local_port());  // resolves OS-assigned port 0
      auto stop = std::make_shared<std::atomic<bool>>(false);
      std::string expected_secret = protocol->peer_secret_key();
      std::thread t([this, l, expected_secret, stop]() {
        serve_loop(l, expected_secret, stop);
      });
      serving_.push_back(ServeRecord{protocol->global_id(), l, stop, std::move(t)});
    }
  }
}

void CentralCommunication::remove_connection(const CommProtocolPtr& protocol) {
  const std::string id = protocol->global_id();
  for (auto it = serving_.begin(); it != serving_.end(); ++it) {
    if (it->protocol_id == id) {
      stop_record(*it);
      serving_.erase(it);
      break;
    }
  }
  connections_.erase(id);
}

// ---- Inbound RPC server (internal; driven by add_connection) ----
void CentralCommunication::serve_loop(hal::Listener* l, std::string expected_secret,
                                      std::shared_ptr<std::atomic<bool>> stop) {
  while (!stop->load()) {
    hal::Connection* c = l->accept(50);  // bounded so the stop flag is seen
    if (!c) continue;
    std::unique_ptr<hal::Connection> conn(c);
    std::vector<uint8_t> first;
    if (conn->recv(first, 5000)) serve_connection(conn.get(), first, expected_secret, stop);
    conn->close();
  }
}

void CentralCommunication::serve_connection(hal::Connection* conn,
                                            const std::vector<uint8_t>& first,
                                            const std::string& expected_secret,
                                            const std::shared_ptr<std::atomic<bool>>& stop) {
  if (ws::looks_like_ws_upgrade(first)) {
    // WebSocket peer (e.g. unmodified Python laila): handshake, then a
    // peer.connect handshake, then a persistent stream of rpc.call frames.
    ws::WsSession s(conn, /*is_client=*/false);
    if (!s.server_handshake(first)) return;

    std::string msg;
    if (!s.recv_text(msg, 10000)) return;
    Json req;
#if defined(LAILA_NO_EXCEPTIONS)
    req = Json::parse(msg);
#else
    try { req = Json::parse(msg); } catch (...) { return; }
#endif
    std::string method = req.is_object() && req.contains("method") && req.at("method").is_string()
                             ? req.at("method").as_string() : std::string();
    if (method != "peer.connect") {
      s.send_text(make_rpc_error(req, -32600, "First message must be a peer.connect request.").dump());
      return;
    }
    const Json& p = req.at("params");
    std::string secret = p.is_object() && p.contains("secret") && p.at("secret").is_string()
                             ? p.at("secret").as_string() : std::string();
    if (!expected_secret.empty() && secret != expected_secret) {
      s.send_text(make_rpc_error(req, -32001, "Invalid peer secret key.").dump());
      return;
    }
    Json ok = Json::object();
    ok["jsonrpc"] = std::string("2.0");
    ok["id"] = req.contains("id") ? req.at("id") : Json();
    Json res = Json::object();
    res["peer_id"] = policy_->global_id();
    ok["result"] = res;
    if (!s.send_text(ok.dump())) return;

    // Persistent session: serve rpc.call frames until the peer closes or
    // stop_serving() is requested (bounded recv so the flag is seen promptly).
    while (!stop->load()) {
      if (!s.recv_text(msg, 200)) {
        if (!s.ok()) break;  // peer closed
        continue;            // idle timeout; re-check the stop flag
      }
      Json call;
#if defined(LAILA_NO_EXCEPTIONS)
      call = Json::parse(msg);
      s.send_text(dispatch_rpc(policy_, call).dump());
#else
      try {
        call = Json::parse(msg);
        s.send_text(dispatch_rpc(policy_, call).dump());
      } catch (const LailaError& e) {
        s.send_text(make_rpc_error(call, -32000, e.what()).dump());
      } catch (...) {
        break;
      }
#endif
    }
    s.close();
    return;
  }

  // Stream carrier (length-prefixed JSON-RPC): peer.connect, then a persistent
  // stream of rpc.call frames. This is laila's tcp:// transport.
  StreamFramer fr(conn);
  fr.seed(first);
  std::string msg;
  if (!fr.recv(msg, 10000)) return;
  Json req;
#if defined(LAILA_NO_EXCEPTIONS)
  req = Json::parse(msg);
#else
  try { req = Json::parse(msg); } catch (...) { return; }
#endif
  std::string method = req.is_object() && req.contains("method") && req.at("method").is_string()
                           ? req.at("method").as_string() : std::string();
  if (method != "peer.connect") {
    fr.send(make_rpc_error(req, -32600, "First message must be a peer.connect request.").dump());
    return;
  }
  const Json& p = req.at("params");
  std::string secret = p.is_object() && p.contains("secret") && p.at("secret").is_string()
                           ? p.at("secret").as_string() : std::string();
  if (!expected_secret.empty() && secret != expected_secret) {
    fr.send(make_rpc_error(req, -32001, "Invalid peer secret key.").dump());
    return;
  }
  Json ok = Json::object();
  ok["jsonrpc"] = std::string("2.0");
  ok["id"] = req.contains("id") ? req.at("id") : Json();
  Json res = Json::object();
  res["peer_id"] = policy_->global_id();
  ok["result"] = res;
  if (!fr.send(ok.dump())) return;

  while (!stop->load()) {
    if (!fr.recv(msg, 200)) {
      if (!conn->is_open()) break;  // peer closed
      continue;                     // idle timeout; re-check the stop flag
    }
    Json call;
#if defined(LAILA_NO_EXCEPTIONS)
    call = Json::parse(msg);
    fr.send(dispatch_rpc(policy_, call).dump());
#else
    try {
      call = Json::parse(msg);
      fr.send(dispatch_rpc(policy_, call).dump());
    } catch (const LailaError& e) {
      fr.send(make_rpc_error(call, -32000, e.what()).dump());
    } catch (...) {
      break;
    }
#endif
  }
}

void CentralCommunication::stop_record(ServeRecord& rec) {
  if (rec.stop) rec.stop->store(true);
  if (rec.listener) rec.listener->close();
  if (rec.thread.joinable()) rec.thread.join();
  delete rec.listener;
  rec.listener = nullptr;
}

void CentralCommunication::stop_all_serving() {
  for (auto& rec : serving_) stop_record(rec);
  serving_.clear();
}

_LAILA_IDENTIFIABLE_COMMUNICATION::~_LAILA_IDENTIFIABLE_COMMUNICATION() {
  stop_all_serving();
}

// ---- Inbound dispatcher ----
// Serves THIS policy's own central.memory over laila's wire protocol: the
// list-based _remote_*/_relay_* methods (entries shipped as
// serialize(transformation_base64) blobs, results as blob/gid lists), the bare
// verbs (single entry, for laila.request), the future-resolution verbs, and the
// __comm_ping__ liveness frame. Golden rule: a policy only serves its own memory.
Json dispatch_rpc(Policy* policy, const Json& request) {
  Json resp = Json::object();
  resp["jsonrpc"] = std::string("2.0");
  resp["id"] = request.contains("id") ? request.at("id") : Json();
  const Json& params = request.at("params");
  const Json& path = params.at("path");
  std::string verb = path.is_array() && !path.elements().empty()
                         ? path.elements().back().as_string() : std::string();
  const Json& args = params.at("args");
  const Json& kwargs = params.at("kwargs");
  // laila's newer kwarg is `pool`; accept the legacy `pool_nickname` too.
  std::string pool = kw_str(kwargs, "pool");
  if (pool.empty()) pool = kw_str(kwargs, "pool_nickname");
  bool persist = kw_bool(kwargs, "persist", true);

  auto id_list = [&]() -> const std::vector<Json>& { return args.elements().at(0).elements(); };

  Json result;
  if (verb == "__comm_ping__") {
    result = std::string("pong");  // liveness
  } else if (verb == "_remote_remember") {
    Json out = Json::array();
    for (const auto& id : id_list())
      out.push_back(serialize_wire(policy->memory().remember(id.as_string(), pool, false)));
    result = out;
  } else if (verb == "_remote_memorize") {
    Json out = Json::array();
    for (const auto& blob : id_list()) {
      EntryPtr e = Entry::build_from_dict(blob);
      policy->memory().memorize(e, pool);
      out.push_back(Json(e->global_id()));
    }
    result = out;
  } else if (verb == "_remote_forget") {
    Json out = Json::array();
    for (const auto& id : id_list()) {
      policy->memory().forget(id.as_string(), pool);
      out.push_back(id);
    }
    result = out;
  } else if (verb == "_relay_remember") {
    // This policy (B) pulls each id from dst_policy (C) over its own B<->C link,
    // optionally caching into its src_pool, and returns the gids.
    std::string dst_policy = kw_str(kwargs, "dst_policy");
    std::string dst_pool = kw_str(kwargs, "dst_pool");
    std::string src_pool = kw_str(kwargs, "src_pool");
    Json out = Json::array();
    for (const auto& id : id_list()) {
      EntryPtr e = policy->communication().remote_remember(dst_policy, id.as_string(), dst_pool, false)->result();
      if (persist) policy->memory().memorize(e, src_pool);
      out.push_back(Json(e->global_id()));
    }
    result = out;
  } else if (verb == "_relay_memorize") {
    // This policy (B) reads each id from its src_pool and pushes to dst_policy (C).
    std::string dst_policy = kw_str(kwargs, "dst_policy");
    std::string dst_pool = kw_str(kwargs, "dst_pool");
    std::string src_pool = kw_str(kwargs, "src_pool");
    auto peerC = policy->communication().peer(dst_policy);
    if (!peerC) raise(Status::NotFound, "relay: source not peered to destination " + dst_policy);
    Json out = Json::array();
    for (const auto& id : id_list()) {
      EntryPtr e = policy->memory().remember(id.as_string(), src_pool, false);
      peerC->memorize(e, dst_pool)->result();
      out.push_back(Json(e->global_id()));
    }
    result = out;
  } else if (verb == "remember") {
    result = serialize_wire(policy->memory().remember(args.elements().at(0).as_string(), pool, persist));
  } else if (verb == "memorize") {
    EntryPtr e = policy->memory().memorize(Entry::build_from_dict(args.elements().at(0)), pool);
    result = serialize_wire(e);
  } else if (verb == "forget") {
    policy->memory().forget(args.elements().at(0).as_string(), pool);
    result = Json(nullptr);
  } else if (verb == "_get_future_status") {
    auto it = policy->future_bank().find(args.elements().at(0).as_string());
    result = std::string(it != policy->future_bank().end()
                             ? future_status_name(it->second->status())
                             : "UNKNOWN");
  } else if (verb == "_wait_future" || verb == "_get_future_result_id") {
    auto it = policy->future_bank().find(args.elements().at(0).as_string());
    if (it != policy->future_bank().end()) result = std::string(it->second->result_id());
    else result = Json(nullptr);
  } else if (verb == "_get_future_exception") {
    result = Json(nullptr);
  } else {
    raise(Status::Unsupported, "inbound method not supported: " + verb);
  }
  resp["result"] = result;
  return resp;
}

// ---------------- Policy ----------------
_LAILA_IDENTIFIABLE_POLICY::_LAILA_IDENTIFIABLE_POLICY() : command_(this), memory_(this), communication_(this) {
  scopes_ = {scope::POLICY};
}

// ---------------- Registry / lifecycle ----------------
namespace {
PolicyPtr g_active;
std::map<std::string, PolicyPtr> g_local;
}  // namespace

PolicyPtr get_active_policy() {
  if (!g_active) {
    auto p = std::make_shared<Policy>();
    activate_policy(p);
  }
  return g_active;
}

void activate_policy(const PolicyPtr& p) {
  g_active = p;
  g_local[p->global_id()] = p;
}

const std::map<std::string, PolicyPtr>& local_policies() { return g_local; }

// Internal access for runtime/terminate.
std::map<std::string, PolicyPtr>& mutable_local_policies() { return g_local; }
void clear_active_policy() { g_active.reset(); }

}  // namespace laila_c
