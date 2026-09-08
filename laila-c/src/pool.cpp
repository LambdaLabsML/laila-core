#include "laila/pool.hpp"

#include "laila/status.hpp"

namespace laila_c {

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
      _write(key, *up);  // write-back cache
      return up;
    }
  }
  return std::nullopt;
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
