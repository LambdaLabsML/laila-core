#include "laila/pool.hpp"

#include <algorithm>

#include "laila/entry.hpp"
#include "laila/status.hpp"

namespace laila_c {

// ---------------- index helpers ----------------
static const std::string kIndexKeyPrefix = std::string(scope::TOPMOST) + ":" + scope::POOL_INDEX + ":";

bool is_index_key(const std::string& key) {
  return key.compare(0, kIndexKeyPrefix.size(), kIndexKeyPrefix) == 0;
}

std::optional<std::string> record_creation_timestamp(const Json& raw) {
  const Json* entry = &raw;
  if (raw.is_object() && raw.contains("entry") && raw.at("entry").is_object()) entry = &raw.at("entry");
  if (!entry->is_object() || !entry->contains("_creation_timestamp")) return std::nullopt;
  const Json& ts = entry->at("_creation_timestamp");
  if (!ts.is_string()) return std::nullopt;
  return ts.as_string();
}

// Evolution encoded in a storage key; nullopt for a constant (no '@').
static std::optional<int64_t> key_evolution_opt(const std::string& key) {
  size_t at = key.find('@');
  if (at == std::string::npos) return std::nullopt;
  for (const auto& kv : parse_global_id_attributes(key.substr(at + 1))) {
    if (kv.first != EVOLUTION_ATTRIBUTE) continue;
    if (kv.second.empty()) return std::nullopt;
    for (char c : kv.second)
      if (c < '0' || c > '9') return std::nullopt;
    return std::stoll(kv.second);
  }
  return std::nullopt;
}

static std::string evolution_key(const std::string& base, std::optional<int64_t> evolution) {
  if (!evolution.has_value()) return base;
  return base + "@" + EVOLUTION_ATTRIBUTE + "=" + std::to_string(*evolution);
}

// Sort rank: constants (no evolution) rank below every evolution.
static int64_t rank(std::optional<int64_t> evolution) { return evolution.has_value() ? *evolution : -1; }

// ---------------- PoolIndex ----------------
_LAILA_IDENTIFIABLE_POOL* PoolIndex::index_pool() const {
  return owner_->index_pool() != nullptr ? owner_->index_pool() : owner_;
}

std::string PoolIndex::nickname(const std::string& base) const {
  return "pool_index:" + owner_->uuid() + ":" + base;
}

std::string PoolIndex::shard_id(const std::string& base) const {
  return to_global_id(generate_uuid_from_nickname(nickname(base)), {scope::POOL_INDEX}, std::nullopt);
}

std::optional<std::vector<std::string>> PoolIndex::candidates(const std::string& base) {
  hal::Guard g;
  Shard* s = shard(base, false);
  if (s == nullptr) return std::nullopt;
  std::vector<std::string> out;
  if (s->constant) out.push_back(base);
  for (int64_t e : s->evolutions) out.push_back(evolution_key(base, e));
  return out;
}

std::optional<std::string> PoolIndex::nth(const std::string& base, int64_t n) {
  hal::Guard g;
  Shard* s = shard(base, false);
  if (s == nullptr) return std::nullopt;
  if (n >= 0) {
    if (std::find(s->evolutions.begin(), s->evolutions.end(), n) == s->evolutions.end()) return std::nullopt;
    return evolution_key(base, n);
  }
  std::vector<std::optional<int64_t>> ranked;
  if (s->constant) ranked.push_back(std::nullopt);
  for (int64_t e : s->evolutions) ranked.push_back(e);
  int64_t idx = static_cast<int64_t>(ranked.size()) + n;
  if (idx < 0 || idx >= static_cast<int64_t>(ranked.size())) return std::nullopt;
  return evolution_key(base, ranked[static_cast<size_t>(idx)]);
}

std::optional<std::string> PoolIndex::by_creation_timestamp(const std::string& base,
                                                            const std::string& timestamp,
                                                            std::optional<int64_t> evolution) {
  hal::Guard g;
  Shard* s = shard(base, false);
  if (s == nullptr) return std::nullopt;
  for (const auto& kv : s->creation_timestamps) {
    if (kv.first != timestamp) continue;
    std::string key = evolution_key(base, kv.second);
    if (evolution.has_value() && nth(base, *evolution) != key) return std::nullopt;
    return key;
  }
  return std::nullopt;
}

void PoolIndex::record(const std::string& key, const Json& value) {
  if (is_index_key(key)) return;
  hal::Guard g;
  const std::string base = strip_global_id_attributes(key);
  std::optional<int64_t> evolution = key_evolution_opt(key);
  std::optional<std::string> stamp = record_creation_timestamp(value);
  Shard* s = shard(base, true);
  if (!evolution.has_value()) {
    s->constant = true;
  } else if (std::find(s->evolutions.begin(), s->evolutions.end(), *evolution) == s->evolutions.end()) {
    s->evolutions.push_back(*evolution);
    std::sort(s->evolutions.begin(), s->evolutions.end());
  }
  if (stamp.has_value()) {
    // An evolution re-written with a different stamp: drop the stale one.
    auto& ts = s->creation_timestamps;
    ts.erase(std::remove_if(ts.begin(), ts.end(),
                            [&](const auto& kv) { return kv.second == evolution && kv.first != *stamp; }),
             ts.end());
    bool found = false;
    for (auto& kv : ts) {
      if (kv.first != *stamp) continue;
      found = true;
      if (rank(evolution) > rank(kv.second)) kv.second = evolution;  // same-ms collision: highest wins
    }
    if (!found) ts.emplace_back(*stamp, evolution);
  }
  flush(base);
}

void PoolIndex::remove(const std::string& key) {
  if (is_index_key(key)) return;
  hal::Guard g;
  const std::string base = strip_global_id_attributes(key);
  std::optional<int64_t> evolution = key_evolution_opt(key);
  Shard* s = shard(base, false);
  if (s == nullptr) return;
  if (!evolution.has_value()) {
    s->constant = false;
  } else {
    s->evolutions.erase(std::remove(s->evolutions.begin(), s->evolutions.end(), *evolution),
                        s->evolutions.end());
  }
  auto& ts = s->creation_timestamps;
  ts.erase(std::remove_if(ts.begin(), ts.end(), [&](const auto& kv) { return kv.second == evolution; }),
           ts.end());
  if (!s->constant && s->evolutions.empty()) drop_shard(base);
  else flush(base);
}

void PoolIndex::invalidate() {
  hal::Guard g;
  shards_.clear();
  entries_.clear();
  missing_.clear();
}

void PoolIndex::invalidate(const std::string& base) {
  hal::Guard g;
  shards_.erase(base);
  entries_.erase(base);
  missing_.erase(std::remove(missing_.begin(), missing_.end(), base), missing_.end());
}

void PoolIndex::clear() {
  hal::Guard g;
  std::vector<std::string> bases;
  for (const auto& k : owner_->_keys()) {
    if (is_index_key(k)) continue;
    std::string b = strip_global_id_attributes(k);
    if (std::find(bases.begin(), bases.end(), b) == bases.end()) bases.push_back(b);
  }
  for (const auto& b : bases) drop_shard(b);
  invalidate();
}

void PoolIndex::rebuild() {
  hal::Guard g;
  std::map<std::string, std::vector<std::string>> by_base;
  for (const auto& k : owner_->_keys()) {
    if (is_index_key(k)) continue;
    by_base[strip_global_id_attributes(k)].push_back(k);
  }
  for (auto& kv : by_base) {
    Shard* s = shard(kv.first, true);  // load first so the shard entry's counter continues
    s->evolutions.clear();
    s->constant = false;
    s->creation_timestamps.clear();
    for (const auto& key : kv.second) {
      std::optional<int64_t> evolution = key_evolution_opt(key);
      if (!evolution.has_value()) s->constant = true;
      else s->evolutions.push_back(*evolution);
      auto raw = owner_->_read(key);
      if (!raw.has_value()) continue;
      auto stamp = record_creation_timestamp(*raw);
      if (!stamp.has_value()) continue;
      bool found = false;
      for (auto& ts : s->creation_timestamps) {
        if (ts.first != *stamp) continue;
        found = true;
        if (rank(evolution) > rank(ts.second)) ts.second = evolution;
      }
      if (!found) s->creation_timestamps.emplace_back(*stamp, evolution);
    }
    std::sort(s->evolutions.begin(), s->evolutions.end());
    flush(kv.first);
  }
}

PoolIndex::Shard* PoolIndex::shard(const std::string& base, bool create) {
  auto it = shards_.find(base);
  if (it != shards_.end()) return &it->second;
  bool known_missing = std::find(missing_.begin(), missing_.end(), base) != missing_.end();
  if (!known_missing) {
    Shard* loaded = load(base);
    if (loaded != nullptr) return loaded;
    missing_.push_back(base);
  }
  if (!create) return nullptr;
  missing_.erase(std::remove(missing_.begin(), missing_.end(), base), missing_.end());
  return &shards_[base];
}

PoolIndex::Shard* PoolIndex::load(const std::string& base) {
  _LAILA_IDENTIFIABLE_POOL* pool = index_pool();
  std::vector<std::string> keys = pool->_candidate_keys(shard_id(base));
  if (keys.empty()) return nullptr;
  std::sort(keys.begin(), keys.end(),
            [](const std::string& a, const std::string& b) { return rank(key_evolution_opt(a)) < rank(key_evolution_opt(b)); });
  auto raw = pool->_read(keys.back());
  if (!raw.has_value()) return nullptr;
  // Shards written by Python are Record envelopes; laila-C writes bare entry dicts.
  const Json& node = (raw->is_object() && raw->contains("entry") && raw->at("entry").is_object()) ? raw->at("entry") : *raw;
  EntryPtr entry = Entry::build_from_dict(node);
  const LailaValue& data = entry->data();
  if (data.kind() != LailaValue::Kind::Json || !data.as_json().is_object()) return nullptr;
  const Json& d = data.as_json();
  Shard s;
  for (const auto& e : d.at("evolutions").elements()) s.evolutions.push_back(e.as_int());
  std::sort(s.evolutions.begin(), s.evolutions.end());
  s.constant = d.contains("constant") && d.at("constant").as_bool();
  for (const auto& kv : d.at("creation_timestamps").items()) {
    if (kv.second.is_null()) s.creation_timestamps.emplace_back(kv.first, std::nullopt);
    else s.creation_timestamps.emplace_back(kv.first, kv.second.as_int());
  }
  shards_[base] = std::move(s);
  entries_[base] = entry;
  for (size_t k = 0; k + 1 < keys.size(); ++k) pool->_delete(keys[k]);  // stragglers
  return &shards_[base];
}

void PoolIndex::flush(const std::string& base) {
  auto it = shards_.find(base);
  if (it == shards_.end()) return;
  const Shard& s = it->second;
  _LAILA_IDENTIFIABLE_POOL* pool = index_pool();
  Json payload = Json::object();
  payload["base"] = base;
  Json evos = Json::array();
  for (int64_t e : s.evolutions) evos.push_back(Json(e));
  payload["evolutions"] = evos;
  payload["constant"] = Json(s.constant);
  Json ts = Json::object();
  for (const auto& kv : s.creation_timestamps) ts[kv.first] = kv.second.has_value() ? Json(*kv.second) : Json(nullptr);
  payload["creation_timestamps"] = ts;

  auto eit = entries_.find(base);
  EntryPtr entry;
  std::optional<std::string> previous_key;
  if (eit == entries_.end()) {
    // First shard evolution: constructed with its payload, lands as @evolution=0.
    entry = Entry::contingent(LailaValue::from_json(payload), generate_uuid_from_nickname(nickname(base)),
                              {scope::POOL_INDEX}, 0, EntryState::READY);
    entries_[base] = entry;
  } else {
    entry = eit->second;
    previous_key = entry->global_id();
    entry->set_data(LailaValue::from_json(payload));
  }
  entry->bump_evolution_if_locally_modified();
  const TransformationSequence& t = pool->transformations();
  pool->_write(entry->global_id(), entry->serialize(t.empty() ? nullptr : &t));
  entry->mark_memorized();
  if (previous_key.has_value() && *previous_key != entry->global_id()) pool->_delete(*previous_key);
}

void PoolIndex::drop_shard(const std::string& base) {
  _LAILA_IDENTIFIABLE_POOL* pool = index_pool();
  for (const auto& k : pool->_candidate_keys(shard_id(base))) pool->_delete(k);
  shards_.erase(base);
  entries_.erase(base);
  if (std::find(missing_.begin(), missing_.end(), base) == missing_.end()) missing_.push_back(base);
}

// ---- Base (in-memory default hooks; guarded for the threaded executor) ----
std::optional<Json> Pool::_read(const std::string& key) {
  hal::Guard g;
  auto it = resource_.find(key);
  if (it == resource_.end()) return std::nullopt;
  return it->second;
}
void Pool::_write(const std::string& key, const Json& value) { hal::Guard g; resource_[key] = value; }
void Pool::_delete(const std::string& key) { hal::Guard g; resource_.erase(key); }
bool Pool::_exists(const std::string& key) { hal::Guard g; return resource_.find(key) != resource_.end(); }
std::vector<std::string> Pool::_keys() {
  hal::Guard g;
  std::vector<std::string> out;
  for (const auto& kv : resource_) out.push_back(kv.first);
  return out;
}
void Pool::_empty() { hal::Guard g; resource_.clear(); }

std::vector<std::string> Pool::_candidate_keys(const std::string& base_gid) {
  const std::string prefix = base_gid + "@";
  std::vector<std::string> out;
  for (const auto& k : _keys()) {
    if (k == base_gid || k.compare(0, prefix.size(), prefix) == 0) out.push_back(k);
  }
  return out;
}

void Pool::sync() {
  raise(Status::Error,
        "Sync is not implemented for this pool, the pool is cacheless, i.e. "
        "operations are immediately executed on the underlying storage.");
}

std::optional<Json> Pool::get(const std::string& key) {
  auto v = _read(key);
  if (v.has_value()) return v;
  if (proxy_to_ != nullptr) {
    auto up = proxy_to_->get(key);
    if (up.has_value()) {
      put(key, *up);  // write-back cache (indexed like any other write)
      return up;
    }
  }
  return std::nullopt;
}

void Pool::put(const std::string& key, const Json& value) {
  _write(key, value);
  if (index_enabled_ && !is_index_key(key)) index_.record(key, value);
}

void Pool::erase(const std::string& key) {
  _delete(key);
  if (index_enabled_ && !is_index_key(key)) index_.remove(key);
}

std::vector<std::string> Pool::keys() {
  std::vector<std::string> out;
  for (auto& k : _keys())
    if (!is_index_key(k)) out.push_back(std::move(k));
  return out;
}

void Pool::empty() {
  if (index_enabled_) index_.clear();
  _empty();
}

std::optional<std::vector<std::string>> Pool::_search_keys(const std::string& base_gid,
                                                           const GidAttributes& attributes) {
  (void)attributes;
  if (!index_enabled_) return std::nullopt;
  return index_.candidates(base_gid);
}

std::optional<std::string> Pool::_resolve_indexed(const std::string& base_gid,
                                                  const GidAttributes& attributes) {
  if (!index_enabled_) return std::nullopt;
  std::optional<int64_t> evolution;
  std::optional<std::string> timestamp;
  for (const auto& kv : attributes) {
    if (kv.first == EVOLUTION_ATTRIBUTE) evolution = std::stoll(kv.second);
    else if (kv.first == CREATION_TIMESTAMP_ATTRIBUTE) timestamp = kv.second;
  }
  if (timestamp.has_value()) return index_.by_creation_timestamp(base_gid, *timestamp, evolution);
  return index_.nth(base_gid, evolution.value_or(-1));
}

// ---- FilesystemPool ----
FilesystemPool::FilesystemPool(const FilesystemPoolOpts& opts) {
  if (opts.nickname) set_nickname(*opts.nickname);
  // Python derives the storage dir from the pool UUID; mirror that by defaulting
  // the HAL storage name to the nickname (if any) else this pool's uuid.
  storage_name_ = opts.storage_name.value_or(opts.nickname.value_or(uuid()));
  storage_ = hal::get().open_storage(storage_name_);
  set_transformations(TransformationSequence::base64());
}

std::optional<Json> FilesystemPool::_read(const std::string& key) {
  if (!storage_) raise(Status::Unsupported, "FilesystemPool: no storage backend on this target");
  std::vector<uint8_t> bytes;
  if (!storage_->read(key, bytes)) return std::nullopt;
  return Json::parse(std::string(bytes.begin(), bytes.end()));
}
void FilesystemPool::_write(const std::string& key, const Json& value) {
  if (!storage_) raise(Status::Unsupported, "FilesystemPool: no storage backend on this target");
  std::string text = value.dump();
  storage_->write(key, std::vector<uint8_t>(text.begin(), text.end()));
}
void FilesystemPool::_delete(const std::string& key) {
  if (storage_) storage_->remove(key);
}
bool FilesystemPool::_exists(const std::string& key) { return storage_ && storage_->exists(key); }
std::vector<std::string> FilesystemPool::_keys() {
  return storage_ ? storage_->keys() : std::vector<std::string>{};
}
void FilesystemPool::_empty() { if (storage_) storage_->clear(); }

// ---- PoolRouter (policy/central/memory/router/pool_router.py) ----
void _LAILA_IDENTIFIABLE_POOL_ROUTER::extend(const PoolPtr& pool, const ExtendOpts& opts) {
  if (!pool) return;
  double affinity = opts.affinity.value_or(0.0);  // farthest away
  pools_pq_.push({-affinity, pool->pool_id()});
  pools_[pool->pool_id()] = pool;
  if (opts.pool_nickname.has_value()) {
    pool->set_nickname(*opts.pool_nickname);
    pools_nicknames_[*opts.pool_nickname] = pool->pool_id();
  }
}

PoolPtr _LAILA_IDENTIFIABLE_POOL_ROUTER::route(const RouteOpts& opts) const {
  if (opts.pool_id.has_value()) {
    auto it = pools_.find(*opts.pool_id);
    if (it == pools_.end()) raise(Status::NotFound, "pool not registered: " + *opts.pool_id);
    return it == pools_.end() ? nullptr : it->second;
  }
  return _route_by_nickname(opts.pool_nickname);
}

PoolPtr _LAILA_IDENTIFIABLE_POOL_ROUTER::_route_by_nickname(
    const std::optional<std::string>& pool_nickname) const {
  // Default to the _memory pool (_DEFAULT_POOL_NICKNAME), always registered.
  std::string nick = pool_nickname.value_or(std::string("_memory"));
  auto n = pools_nicknames_.find(nick);
  if (n == pools_nicknames_.end()) raise(Status::NotFound, "pool not registered: " + nick);
  if (n == pools_nicknames_.end()) return nullptr;
  auto it = pools_.find(n->second);
  if (it == pools_.end()) raise(Status::NotFound, "pool not registered: " + nick);
  return it == pools_.end() ? nullptr : it->second;
}

}  // namespace laila_c
