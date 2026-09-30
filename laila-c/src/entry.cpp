#include "laila/entry.hpp"

#include "laila/computational_data.hpp"
#include "laila/manifest.hpp"
#include "laila/status.hpp"

namespace laila_c {

static const LailaValue kNoneValue;

EntryPtr Entry::constant(const LailaValue& data, const ConstantOpts& opts) {
  if (opts.global_id && opts.uuid)
    raise(Status::Error, "Cannot set both global_id and uuid at the same time.");
  std::string uuid;
  if (opts.uuid) uuid = *opts.uuid;
  else if (opts.global_id) {
    ParsedGid p = process_global_id(*opts.global_id);
    if (p.evolution.has_value()) raise(Status::Error, "Cannot have a constant with an evolution.");
    uuid = p.uuid;
  }
  if (opts.nickname) uuid = generate_uuid_from_nickname(*opts.nickname);
  if (uuid.empty()) uuid = uuid4();

  auto e = std::make_shared<Entry>();
  e->set_uuid(uuid);
  e->set_evolution(std::nullopt);
  e->set_data(data);
  e->set_state(EntryState::READY);
  e->mark_memorized();  // the initial payload is the baseline, not a change
  return e;
}

EntryPtr Entry::variable(const LailaValue& data, const VariableOpts& opts) {
  if (opts.global_id && (opts.uuid || opts.evolution))
    raise(Status::Error, "Cannot set both global_id and <uuid, evolution> at the same time.");
  if (opts.constitution && !data.is_none())
    raise(Status::Error, "Cannot set both constitution and data.");
  if (opts.constitution.has_value() != static_cast<bool>(opts.manifest))
    raise(Status::Error, "`constitution` and `manifest` must both be provided together.");

  std::string uuid;
  std::optional<int64_t> evolution = opts.evolution;
  if (opts.global_id) {
    ParsedGid p = process_global_id(*opts.global_id);
    uuid = p.uuid;
    evolution = p.evolution;
  } else if (opts.uuid) {
    uuid = *opts.uuid;
  }
  if (!evolution.has_value()) evolution = 0;
  if (opts.nickname) uuid = generate_uuid_from_nickname(*opts.nickname);
  if (uuid.empty()) uuid = uuid4();

  auto e = std::make_shared<Entry>();
  e->set_uuid(uuid);
  e->set_evolution(evolution);
  if (opts.constitution) {
    e->set_constitution(std::make_shared<ComplexConstitution>(*opts.constitution));
    e->set_manifest(opts.manifest);
    e->set_state(EntryState::STAGED);
  } else {
    e->set_data(data);
    e->set_state(opts.state.value_or(EntryState::READY));
  }
  e->mark_memorized();
  return e;
}

EntryPtr Entry::contingent(const LailaValue& data, std::optional<std::string> uuid,
                           std::vector<std::string> scopes, std::optional<int64_t> evolution,
                           EntryState state) {
  auto e = std::make_shared<Entry>();
  e->set_uuid(uuid.value_or(uuid4()));
  if (!scopes.empty()) e->set_scopes(scopes);
  e->set_evolution(evolution);
  if (!data.is_none()) e->set_data(data);
  e->set_state(state);
  e->mark_memorized();
  return e;
}

const LailaValue& Entry::data() const {
  if (payload_.has_value()) return *payload_;
  if (constitution_)
    raise(Status::NotBuilt,
          "Entry " + global_id() + " is not built. Use laila->build(entry)->wait() first.");
  return kNoneValue;
}

void Entry::set_data(const LailaValue& v) {
  if (v.is_none()) payload_.reset();
  else payload_ = v;
  locally_modified_ = true;
}

bool Entry::bump_evolution_if_locally_modified() {
  if (!evolution_.has_value() || !locally_modified_) return false;
  *evolution_ += 1;
  creation_timestamp_ = now_creation_timestamp();
  return true;
}

EntryPtr Entry::evolve(const LailaValue& data) {
  if (!evolution_.has_value()) raise(Status::Error, "Can't evolve a constant.");
  if (constitution_) raise(Status::Error, "Entry has not been built yet; cannot evolve.");
  int64_t next = *evolution_ + 1;
  return Entry::contingent(data, uuid_, scopes_, next, EntryState::READY);
}

void Entry::build_inplace() {
  if (!constitution_) raise(Status::Error, "Entry has no constitution attached.");
  LailaValue input = payload_.has_value() ? *payload_ : LailaValue::none();
  LailaValue result = constitution_->build_with(input, manifest_.get());
  set_data(result);
  constitution_.reset();
  manifest_.reset();
  state_ = EntryState::READY;
  locally_modified_ = false;  // materializing stored bytes is not a user change
}

// Identity / state header shared by as_dict, to_wire_dict and serialize
// (Python: _uuid, _evolution, _scopes, _state, _creation_timestamp).
Json Entry::_identity_dict() const {
  Json o = Json::object();
  o["_uuid"] = uuid_;
  if (evolution_.has_value()) o["_evolution"] = Json((int64_t)*evolution_);
  else o["_evolution"] = Json(nullptr);
  Json scopes = Json::array();
  for (const auto& s : scopes_) scopes.push_back(Json(s));
  o["_scopes"] = scopes;
  o["_state"] = std::string(entry_state_name(state_));
  o["_creation_timestamp"] = creation_timestamp_;
  return o;
}

Json Entry::as_dict() const {
  Json o = _identity_dict();
  o["payload"] = payload_.has_value() ? payload_->to_json_payload() : Json(nullptr);
  o["constitution"] = constitution_ ? constitution_->as_dict() : Json(nullptr);
  return o;
}

Json Entry::to_wire_dict() const {
  // Same shape as as_dict() but with a laila-native (untagged) payload so an
  // unmodified Python `laila` peer can read it directly.
  Json o = _identity_dict();
  o["payload"] = payload_.has_value() ? payload_->to_wire_payload() : Json(nullptr);
  o["constitution"] = constitution_ ? constitution_->as_dict() : Json(nullptr);
  return o;
}

Json Entry::serialize(const TransformationSequence* transformations) const {
  if (state_ != EntryState::READY)
    raise(Status::Error, std::string("Cannot serialize entry in state ") + entry_state_name(state_));
  // The in-memory path stores the dict itself: since Json is a value type the
  // stored record is already a snapshot, so a later in-place evolution bump
  // (bump_evolution_if_locally_modified) cannot alias it -- no extra copy needed here.
  if (transformations == nullptr || transformations->empty()) return as_dict();

  Json o = _identity_dict();

  std::vector<std::string> codes;
  if (payload_.has_value()) {
    // Python: serialized_payload, code = self._payload.serialize() where
    // _payload is a ComputationalData. Wrap the payload in the matching taxonomy
    // subclass and serialize through it.
    auto serialized = ComputationalData::wrap(*payload_)->serialize();  // (bytes, code)
    LailaValue bytes_val = LailaValue::from_bytes(serialized.first);
    auto fwd = transformations->forward(bytes_val);      // (transformed, inverse_codes)
    // The transformed payload is laila-native (a raw base64 string for the
    // base64 pipeline), so an unmodified Python peer reads it directly.
    o["payload"] = fwd.first.to_wire_payload();
    codes = fwd.second;
    codes.push_back(serialized.second);
  } else {
    o["payload"] = Json(nullptr);
  }
  SimpleConstitution sc(codes);
  o["constitution"] = sc.as_dict();
  return o;
}

EntryPtr Entry::from_dict(const Json& node) {
  auto e = std::make_shared<Entry>();
  e->set_uuid(node.at("_uuid").as_string());
  const Json& evo = node.at("_evolution");
  if (evo.is_null()) e->set_evolution(std::nullopt);
  else e->set_evolution(evo.as_int());
  std::vector<std::string> scopes;
  for (const auto& s : node.at("_scopes").elements()) scopes.push_back(s.as_string());
  if (!scopes.empty()) e->set_scopes(scopes);
  const Json& payload = node.at("payload");
  if (!payload.is_null()) e->set_data(LailaValue::from_json_payload(payload));
  e->set_state(entry_state_from_name(node.contains("_state") ? node.at("_state").as_string() : "STAGED"));
  e->set_constitution(Constitution::from_dict(node.at("constitution")));
  // Restore the persisted creation stamp; dicts that predate the field (or
  // come from other producers) are stamped "now", like Python's from_dict.
  if (node.contains("_creation_timestamp") && node.at("_creation_timestamp").is_string() &&
      !node.at("_creation_timestamp").as_string().empty())
    e->set_creation_timestamp(node.at("_creation_timestamp").as_string());
  e->mark_memorized();  // a pool round-trip restores the memorized baseline
  return e;
}

EntryPtr Entry::build_from_dict(const Json& node) {
  EntryPtr e = from_dict(node);
  // Only SimpleConstitution entries are materialized inline here (no RTTI:
  // dispatch on the virtual kind() tag). Complex constitutions need the build
  // pipeline (a manifest), mirroring laila's _build_from_dict_sync.
  auto c = e->constitution();
  if (c && std::string(c->kind()) == "simple") e->build_inplace();
  return e;
}

}  // namespace laila_c
