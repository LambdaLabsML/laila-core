#include "laila/manifest.hpp"

#include "laila/laila.hpp"
#include "laila/status.hpp"

namespace laila_c {

// ---------------- RealizedNode ----------------
RealizedNode& RealizedNode::operator[](const std::string& key) {
  for (auto& kv : obj_)
    if (kv.first == key) return kv.second;
  raise(Status::NotFound, "RealizedNode: no key '" + key + "'");
  return obj_.front().second;  // unreachable (raise throws / aborts)
}

const RealizedNode& RealizedNode::operator[](const std::string& key) const {
  for (const auto& kv : obj_)
    if (kv.first == key) return kv.second;
  raise(Status::NotFound, "RealizedNode: no key '" + key + "'");
  return obj_.front().second;  // unreachable
}

// ---------------- Manifest ----------------
namespace {

// Depth-first, insertion-order walk collecting every leaf global_id string
// (mirrors laila's Manifest._iter_global_ids).
void collect_leaves(const Json& node, std::vector<std::string>& out) {
  if (node.is_string()) {
    out.push_back(node.as_string());
  } else if (node.is_array()) {
    for (const auto& e : node.elements()) collect_leaves(e, out);
  } else if (node.is_object()) {
    for (const auto& kv : node.items()) collect_leaves(kv.second, out);
  }
}

// Rebuild the realized node tree from the blueprint shape, substituting resolved
// entries at the gid-string leaves.
RealizedNode rebuild(const Json& node, const std::map<std::string, EntryPtr>& resolved) {
  if (node.is_string()) {
    auto it = resolved.find(node.as_string());
    return RealizedNode::leaf(it == resolved.end() ? nullptr : it->second);
  }
  if (node.is_array()) {
    RealizedNode list = RealizedNode::list();
    for (const auto& e : node.elements()) list.append(rebuild(e, resolved));
    return list;
  }
  if (node.is_object()) {
    RealizedNode obj = RealizedNode::object();
    for (const auto& kv : node.items()) obj.set(kv.first, rebuild(kv.second, resolved));
    return obj;
  }
  return RealizedNode::leaf(nullptr);
}

}  // namespace

ManifestPtr Manifest::create(const ManifestOpts& opts) {
  if (opts.global_id && opts.uuid)
    raise(Status::Error, "Cannot set both global_id and uuid at the same time.");

  auto m = std::make_shared<Manifest>();

  std::string uuid;
  if (opts.uuid) uuid = *opts.uuid;
  else if (opts.global_id) uuid = process_global_id(*opts.global_id).uuid;
  if (opts.nickname) uuid = generate_uuid_from_nickname(*opts.nickname);
  if (uuid.empty()) uuid = uuid4();
  m->set_uuid(uuid);
  m->set_evolution(std::nullopt);  // a manifest is a constant (no evolution)

  if (opts.data && !opts.data->is_none()) {
    m->set_data(*opts.data);
    m->set_state(EntryState::READY);
  }
  m->rebuild_index();
  return m;
}

void Manifest::rebuild_index() {
  keys_.clear();
  leaves_.clear();
  const LailaValue& d = data();
  if (d.kind() != LailaValue::Kind::Json) return;
  const Json& bp = d.as_json();
  if (!bp.is_object()) return;
  for (const auto& kv : bp.items()) keys_.push_back(kv.first);
  collect_leaves(bp, leaves_);
}

std::vector<std::string> Manifest::keys() const { return keys_; }

FuturePtr Manifest::remember(const RememberOpts& opts) {
  auto group = std::make_shared<GroupFuture>();
  for (const auto& gid : leaves_) {
    group->add_child(laila->remember(gid, opts));
  }
  return group;
}

RealizedNode Manifest::realized() const {
  const LailaValue& d = data();
  if (d.kind() != LailaValue::Kind::Json)
    raise(Status::Error, "No blueprint to resolve -- manifest is empty.");
  const Json& bp = d.as_json();

  // Resolve every leaf gid via the active policy's central memory (alpha pool).
  std::map<std::string, EntryPtr> resolved;
  for (const auto& gid : leaves_) {
    if (resolved.count(gid)) continue;
    resolved[gid] = laila->remember(gid, {})->result();
  }
  return rebuild(bp, resolved);
}

}  // namespace laila_c
