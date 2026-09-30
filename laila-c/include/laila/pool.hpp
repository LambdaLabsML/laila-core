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

class _LAILA_IDENTIFIABLE_POOL;

// Name of the search attribute keyed by an entry's creation stamp.
inline constexpr const char* CREATION_TIMESTAMP_ATTRIBUTE = "creation_timestamp";

// True for storage keys that belong to an index shard (LAILA:POOL_INDEX:...).
bool is_index_key(const std::string& key);
// Entry creation_timestamp out of a stored record (entry dict, or a Python
// Record envelope with an "entry" member) without rebuilding it.
std::optional<std::string> record_creation_timestamp(const Json& raw);

// Per-pool evolution / creation-timestamp index (data/schema/pool_index.py).
//
// One shard per base gid (LAILA:ENTRY:<uuid>): sorted evolutions, whether a
// constant key exists, and creation_timestamp -> evolution. A shard is an
// evolvable Entry (scope POOL_INDEX, id uuid5("pool_index:<owner>:<base>"))
// stored in the owner's index_pool (default: the owner itself), rewritten
// write-through on every record()/remove(), latest shard evolution only.
//
// The index is a validated cache: central memory confirms every hit and
// calls remove() on a stale key (which repairs the persisted shard); a
// failed shard write invalidates the base and never fails the caller.
class PoolIndex {
public:
  struct Shard {
    std::vector<int64_t> evolutions;                          // sorted ascending
    bool constant = false;                                    // exact base key stored
    std::vector<std::pair<std::string, std::optional<int64_t>>> creation_timestamps;
  };

  explicit PoolIndex(_LAILA_IDENTIFIABLE_POOL* owner) : owner_(owner) {}

  _LAILA_IDENTIFIABLE_POOL* index_pool() const;
  std::string shard_id(const std::string& base) const;

  // Queries (nullopt == unindexed / out of range).
  std::optional<std::vector<std::string>> candidates(const std::string& base);
  std::optional<std::string> latest(const std::string& base) { return nth(base, -1); }
  // n >= 0: exact evolution; n < 0: from the end (-1 = latest; a constant
  // key ranks lowest and is returned when it is all there is).
  std::optional<std::string> nth(const std::string& base, int64_t n);
  std::optional<std::string> by_creation_timestamp(const std::string& base,
                                                   const std::string& timestamp,
                                                   std::optional<int64_t> evolution = std::nullopt);

  // Maintenance (write-through; taken under the HAL guard).
  void record(const std::string& key, const Json& value);
  void remove(const std::string& key);
  void invalidate();                        // forget all in-memory shards
  void invalidate(const std::string& base);
  void clear();                             // drop every shard of the owner's keys
  void rebuild();                           // rebuild every shard from the owner's keys

private:
  Shard* shard(const std::string& base, bool create);
  Shard* load(const std::string& base);
  void flush(const std::string& base);
  void drop_shard(const std::string& base);
  std::string nickname(const std::string& base) const;

  _LAILA_IDENTIFIABLE_POOL* owner_;
  std::map<std::string, Shard> shards_;
  std::map<std::string, std::shared_ptr<class Entry>> entries_;  // live shard entries
  std::vector<std::string> missing_;                               // bases with no shard
};

class _LAILA_IDENTIFIABLE_POOL : public _LAILA_CLI_CAPABLE_CLASS,
                                public _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
public:
  _LAILA_IDENTIFIABLE_POOL() : index_(this) { scopes_ = {scope::POOL}; }
  virtual ~_LAILA_IDENTIFIABLE_POOL() = default;
  _LAILA_IDENTIFIABLE_POOL(const _LAILA_IDENTIFIABLE_POOL&) = delete;
  _LAILA_IDENTIFIABLE_POOL& operator=(const _LAILA_IDENTIFIABLE_POOL&) = delete;

  // ---- Index (base.py index / index_enabled / index_pool) ----
  PoolIndex& index() { return index_; }
  bool index_enabled() const { return index_enabled_; }
  void set_index_enabled(bool b) { index_enabled_ = b; }
  // Where the shards live; nullptr (default) == this pool.
  _LAILA_IDENTIFIABLE_POOL* index_pool() const { return index_pool_; }
  void set_index_pool(_LAILA_IDENTIFIABLE_POOL* p) { index_pool_ = p; }
  // Answer an attribute lookup from the index alone (base.py _resolve_indexed):
  // evolution may be negative (-1 = latest); creation_timestamp exact.
  std::optional<std::string> _resolve_indexed(const std::string& base_gid,
                                              const GidAttributes& attributes);

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
  // Local-only read: no proxy fall-through, no write-back (Python callers use
  // `_read_async` directly for this; central memory's time-based search does).
  std::optional<Json> read_local(const std::string& key) { return _read(key); }
  // Index-maintaining write / delete (base.py write / delete wrappers): every
  // writer goes through these; backends override only _write/_delete.
  void put(const std::string& key, const Json& value);
  void erase(const std::string& key);
  bool exists(const std::string& key) { return _exists(key); }
  // Entry keys only -- index shards (LAILA:POOL_INDEX:...) are hidden
  // (base.py keys(include_index=False)); raw_keys() shows everything.
  std::vector<std::string> keys();
  std::vector<std::string> raw_keys() { return _keys(); }
  void empty();

  // Candidate keys for an attribute lookup (base.py _search_keys). Default
  // answers from this pool's index when it has a shard for base_gid, else
  // nullopt ("no index, scan"). A returned list (even empty) is authoritative.
  virtual std::optional<std::vector<std::string>> _search_keys(const std::string& base_gid,
                                                               const GidAttributes& attributes);
  // Local-only keys that are evolutions of base_gid: the exact key (constant)
  // plus every "base_gid@..." key (base.py _candidate_keys).
  std::vector<std::string> _candidate_keys(const std::string& base_gid);
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
  PoolIndex index_;
  bool index_enabled_ = true;
  _LAILA_IDENTIFIABLE_POOL* index_pool_ = nullptr;

  friend class PoolIndex;  // shard I/O uses the raw _read/_write/_delete hooks
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
