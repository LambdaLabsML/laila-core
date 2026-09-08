#include "laila/laila.hpp"

#include "laila/status.hpp"

namespace laila_c {

// Defined in policy.cpp.
std::map<std::string, PolicyPtr>& mutable_local_policies();
void clear_active_policy();

namespace {
// First present optional, mirroring laila's back-compat alias folding.
std::string first_of(const std::optional<std::string>& a, const std::optional<std::string>& b = {},
                     const std::optional<std::string>& c = {}) {
  if (a) return *a;
  if (b) return *b;
  if (c) return *c;
  return "";
}
// args shaped as Python's relay call: one positional that is the id list.
Json ids_arg(const std::string& gid) {
  Json ids = Json::array();
  ids.push_back(Json(gid));
  Json args = Json::array();
  args.push_back(ids);
  return args;
}
}  // namespace

FuturePtr Laila::memorize(const EntryPtr& entry) {
  PolicyPtr policy = get_active_policy();
  return policy->command().submit([policy, entry]() { return policy->memory().memorize(entry); });
}

FuturePtr Laila::memorize(const EntryPtr& entry, const MemorizeOpts& opts) {
  PolicyPtr policy = get_active_policy();
  std::string active = policy->global_id();
  std::string src = first_of(opts.src_policy);
  std::string dst = first_of(opts.dst_policy, opts.policy_id);
  std::string dst_pool = first_of(opts.dst_pool, opts.pool_id, opts.pool_nickname);
  std::string src_pool = first_of(opts.src_pool);

  // src is another policy -> 3-party relay (B pushes its src_pool -> C's dst_pool).
  if (!src.empty() && src != active) {
    auto p = policy->communication().peer(src);
    if (!p) raise(Status::NotFound, "relay: active policy is not peered to source " + src);
    Json kwargs = Json::object();
    kwargs["dst_policy"] = dst; kwargs["dst_pool"] = dst_pool; kwargs["src_pool"] = src_pool;
    return p->request("central.memory._relay_memorize", ids_arg(entry->global_id()), kwargs);
  }
  // active -> peer push (2-party).
  if (!dst.empty() && dst != active) {
    auto p = policy->communication().peer(dst);
    if (!p) raise(Status::NotFound, "not peered to " + dst);
    return p->memorize(entry, dst_pool);
  }
  // local write.
  return policy->command().submit(
      [policy, entry, dst_pool]() { return policy->memory().memorize(entry, dst_pool); });
}

FuturePtr Laila::memorize(const std::vector<EntryPtr>& entries) {
  PolicyPtr policy = get_active_policy();
  auto group = std::make_shared<GroupFuture>();
  policy->future_bank()[group->global_id()] = group;
  for (const auto& e : entries) group->add_child(memorize(e));
  return group;
}

FuturePtr Laila::remember(const std::string& entry_id, const RememberOpts& opts) {
  PolicyPtr policy = get_active_policy();
  std::string gid = entry_id;
  if (opts.nickname) {
    std::string uuid = generate_uuid_from_nickname(*opts.nickname);
    gid = to_global_id(uuid, {scope::ENTRY}, opts.evolution);
  }
  std::string active = policy->global_id();
  std::string src = first_of(opts.src_policy);
  std::string dst = first_of(opts.dst_policy, opts.policy_id);
  std::string dst_pool = first_of(opts.dst_pool, opts.pool_id, opts.pool_nickname);
  std::string src_pool = first_of(opts.src_pool);
  bool persist = opts.persist;

  // src is another policy -> 3-party relay (B pulls from C into its src_pool).
  if (!src.empty() && src != active) {
    auto p = policy->communication().peer(src);
    if (!p) raise(Status::NotFound, "relay: active policy is not peered to source " + src);
    Json kwargs = Json::object();
    kwargs["dst_policy"] = dst; kwargs["dst_pool"] = dst_pool; kwargs["src_pool"] = src_pool;
    kwargs["persist"] = persist;
    return p->request("central.memory._relay_remember", ids_arg(gid), kwargs);
  }
  // active pulls from a peer (2-party).
  if (!dst.empty() && dst != active) {
    return policy->communication().remote_remember(dst, gid, dst_pool, persist);
  }
  // local read.
  return policy->command().submit(
      [policy, gid, dst_pool, persist]() { return policy->memory().remember(gid, dst_pool, persist); });
}

FuturePtr Laila::forget(const std::string& entry_id) {
  PolicyPtr policy = get_active_policy();
  return policy->command().submit([policy, entry_id]() { return policy->memory().forget(entry_id); });
}

FuturePtr Laila::forget(const std::string& entry_id, const ForgetOpts& opts) {
  PolicyPtr policy = get_active_policy();
  std::string gid = entry_id;
  if (opts.nickname) {
    std::string uuid = generate_uuid_from_nickname(*opts.nickname);
    gid = to_global_id(uuid, {scope::ENTRY}, opts.evolution);
  }
  std::string active = policy->global_id();
  std::string target = first_of(opts.policy, opts.policy_id);
  std::string pool = first_of(opts.pool, opts.pool_id, opts.pool_nickname);

  if (!target.empty() && target != active) {
    auto p = policy->communication().peer(target);
    if (!p) raise(Status::NotFound, "not peered to " + target);
    return p->forget(gid, pool);
  }
  return policy->command().submit(
      [policy, gid, pool]() { return policy->memory().forget(gid, pool); });
}

FuturePtr Laila::build(const EntryPtr& entry) {
  PolicyPtr policy = get_active_policy();
  return policy->command().submit([entry]() {
    if (entry->constitution()) entry->build_inplace();
    return entry;
  });
}

std::vector<std::string> Laila::terminate() {
  std::vector<std::string> ids;
  for (const auto& kv : mutable_local_policies()) ids.push_back(kv.first);
  mutable_local_policies().clear();
  clear_active_policy();
  return ids;
}

}  // namespace laila_c

static laila_c::Laila g_laila;
laila_c::Laila* laila = &g_laila;
