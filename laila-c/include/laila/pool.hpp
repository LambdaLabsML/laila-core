// Pool mirror (pool/schema/base.py): a key/value store of serialized Entry
// blobs, with proxy-chaining (cache << origin) for multi-tier read-through
// caches. Concrete backends override the storage hooks. All ~15 laila pool
// class names are preserved; this slice ships the in-memory DefaultPool +
// FilesystemPool, others construct but report Status::Unsupported on first I/O
// (added incrementally).
#ifndef LAILA_POOL_HPP
#define LAILA_POOL_HPP

#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "laila/cli_capable.hpp"
#include "laila/hal/hal.hpp"
#include "laila/identity.hpp"
#include "laila/json.hpp"
#include "laila/transformation.hpp"

namespace laila_c {

class _LAILA_IDENTIFIABLE_POOL : public _LAILA_CLI_CAPABLE_CLASS,
                                public _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
public:
  _LAILA_IDENTIFIABLE_POOL() { scopes_ = {scope::POOL}; }
  virtual ~_LAILA_IDENTIFIABLE_POOL() = default;

  std::string pool_id() const { return global_id(); }
  const std::string& nickname() const { return nickname_; }
  void set_nickname(const std::string& n) { nickname_ = n; }

  const TransformationSequence& transformations() const { return transformations_; }
  void set_transformations(TransformationSequence t) { transformations_ = std::move(t); }

  // Proxy chaining (mirrors pool/schema/base.py __lshift__/__rshift__).
  // `cache << origin`: cache fronts origin (cache._proxy_to = origin); returns
  // origin so `mem << hdf5 << s3` chains right-to-left.
  _LAILA_IDENTIFIABLE_POOL& operator<<(_LAILA_IDENTIFIABLE_POOL& other) { this->proxy_to_ = &other; return other; }
  // `origin >> cache`: cache fronts origin (cache._proxy_to = origin); returns
  // cache so `s3 >> hdf5 >> mem` chains left-to-right.
  _LAILA_IDENTIFIABLE_POOL& operator>>(_LAILA_IDENTIFIABLE_POOL& other) { other.proxy_to_ = this; return other; }
  _LAILA_IDENTIFIABLE_POOL* proxy_to() const { return proxy_to_; }
  void set_proxy_to(_LAILA_IDENTIFIABLE_POOL* p) { proxy_to_ = p; }

  // Proxy-aware public API (mirrors base.py __getitem__/__setitem__/__delitem__
  // and exists/keys/empty). get() falls through the proxy chain on a local miss
  // and caches the upstream hit (== Python __getitem__).
  std::optional<Json> get(const std::string& key);
  void put(const std::string& key, const Json& value) { _write(key, value); }
  void erase(const std::string& key) { _delete(key); }
  bool exists(const std::string& key) { return _exists(key); }
  std::vector<std::string> keys() { return _keys(); }
  void empty() { _empty(); }
  // sync(): flush an in-memory write cache. Base pools are cacheless, so this
  // raises (mirrors base.py sync()'s NotImplementedError).
  virtual void sync();

  // Whether the backend handles batched writes more efficiently (consulted by
  // central memory). Mirrors base.py's batch_accelerated field.
  bool batch_accelerated() const { return batch_accelerated_; }
  void set_batch_accelerated(bool b) { batch_accelerated_ = b; }

protected:
  // Internal storage hooks (override in subclasses; defaults use the in-memory
  // `resource` map). Names mirror base.py: _read/_write/_delete/_exists/_keys/_empty.
  virtual std::optional<Json> _read(const std::string& key);
  virtual void _write(const std::string& key, const Json& value);
  virtual void _delete(const std::string& key);
  virtual bool _exists(const std::string& key);
  virtual std::vector<std::string> _keys();
  virtual void _empty();

  std::string nickname_;
  _LAILA_IDENTIFIABLE_POOL* proxy_to_ = nullptr;
  bool batch_accelerated_ = false;
  TransformationSequence transformations_;
  std::map<std::string, Json> resource_;  // default in-memory backing store
};
// Compatibility spelling + the laila `DefaultPool` alias (macros/defaults.py).
using Pool = _LAILA_IDENTIFIABLE_POOL;
using DefaultPool = _LAILA_IDENTIFIABLE_POOL;
using PoolPtr = std::shared_ptr<_LAILA_IDENTIFIABLE_POOL>;

// Mirrors FilesystemPool's kwargs (filesystem.py): nickname (base) + transformations
// (default base64). Python derives the dir from the pool UUID; the C HAL needs a
// named storage, so storage_name is optional and defaults to nickname-or-uuid.
struct FilesystemPoolOpts {
  std::optional<std::string> nickname = std::nullopt;
  std::optional<std::string> storage_name = std::nullopt;  // override; tests use this
};

// Filesystem/flash-backed pool over a hal::Storage. Uses a base64 transform so
// arbitrary payload bytes survive text-oriented backends.
class FilesystemPool : public Pool {
public:
  using Opts = FilesystemPoolOpts;  // enables laila_pointer<FilesystemPool>({...})
  explicit FilesystemPool(const FilesystemPoolOpts& opts = {});

protected:
  std::optional<Json> _read(const std::string& key) override;
  void _write(const std::string& key, const Json& value) override;
  void _delete(const std::string& key) override;
  bool _exists(const std::string& key) override;
  std::vector<std::string> _keys() override;
  void _empty() override;

private:
  hal::Storage* storage_ = nullptr;
  std::string storage_name_;
};

// extend(pool, *, affinity=None, pool_nickname=None) kwargs mirror
// (policy/central/memory/router/pool_router.py and central/memory/schema/base.py).
struct ExtendOpts {
  std::optional<double> affinity = std::nullopt;
  std::optional<std::string> pool_nickname = std::nullopt;
};

// route(entries, *, pool_id=None, pool_nickname=None, affinity=None) kwargs
// mirror. `entries` is omitted -- Python's routing does not depend on it
// (pool_router.py: "routing decision does not actually depend on entries").
struct RouteOpts {
  std::optional<std::string> pool_id = std::nullopt;
  std::optional<std::string> pool_nickname = std::nullopt;
  std::optional<double> affinity = std::nullopt;
};

// Pool router (policy/central/memory/router/pool_router.py): registers pools and
// resolves which pool a memory call targets. Routing precedence mirrors laila:
// explicit pool_id > pool_nickname > the default-nickname pool (_memory).
class _LAILA_IDENTIFIABLE_POOL_ROUTER {
public:
  void extend(const PoolPtr& pool, const ExtendOpts& opts = {});
  // Resolve the destination pool. Raises Status::NotFound when a named pool is
  // not registered (mirrors PoolRouter.route / _route_by_nickname's KeyError).
  PoolPtr route(const RouteOpts& opts = {}) const;

private:
  PoolPtr _route_by_nickname(const std::optional<std::string>& pool_nickname) const;
  std::map<std::string, PoolPtr> pools_;                // pools: gid -> Pool
  std::map<std::string, std::string> pools_nicknames_;  // pools_nicknames: nickname -> gid
  std::priority_queue<std::pair<double, std::string>> pools_pq_;  // pools_pq: (-affinity, gid)
};
using DefaultPoolRouter = _LAILA_IDENTIFIABLE_POOL_ROUTER;

}  // namespace laila_c

#endif  // LAILA_POOL_HPP
