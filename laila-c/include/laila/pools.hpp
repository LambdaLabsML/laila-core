// laila pool class names (pool/*). The lineage mirrors Python 1:1:
// _LAILA_IDENTIFIABLE_POOL -> BotoPool -> {S3Pool, CloudflarePool, BackblazePool}
// and every other pool is a direct subclass of the base. A pool whose native
// client/SDK is not wired into laila-C on the active target raises
// Status::Unsupported on first I/O (the translation hard-error rule). The host
// build ships DefaultPool + FilesystemPool live, and -- when built with the [s3]
// extra (LAILA_WITH_S3) -- a real S3Pool backed by aws-sdk-cpp.
#ifndef LAILA_POOLS_HPP
#define LAILA_POOLS_HPP

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "laila/pool.hpp"

namespace laila_c {

// Raise Status::Unsupported for a pool whose backend SDK is not wired in here.
void raise_pool_unsupported(const std::string& backend);

// Inject the six storage hooks as "backend not available -> LAILA_UNSUPPORTED"
// for pools whose native client (redis, psycopg, gcs, ...) isn't compiled into
// laila-C on this target. Mirrors Python pools raising when their optional
// client dependency is absent, without inventing an intermediate class.
#define LAILA_POOL_UNSUPPORTED_HOOKS(BACKEND)                                              \
 protected:                                                                                \
  std::optional<Json> _read(const std::string&) override {                                 \
    raise_pool_unsupported(BACKEND);                                                        \
    return std::nullopt;                                                                    \
  }                                                                                         \
  void _write(const std::string&, const Json&) override { raise_pool_unsupported(BACKEND); } \
  void _delete(const std::string&) override { raise_pool_unsupported(BACKEND); }            \
  bool _exists(const std::string&) override {                                               \
    raise_pool_unsupported(BACKEND);                                                        \
    return false;                                                                           \
  }                                                                                         \
  std::vector<std::string> _keys() override {                                               \
    raise_pool_unsupported(BACKEND);                                                        \
    return {};                                                                              \
  }                                                                                         \
  void _empty() override { raise_pool_unsupported(BACKEND); }

// ---- BotoPool family (pool/boto/boto.py + pool/s3, cloudflare, backblaze) ----

// The subset of the boto3 S3 client surface BotoPool drives (boto.py). A real
// implementation is supplied per platform (the posix aws-sdk-cpp backend); when
// none is compiled in, BotoPool::_get_client() raises Status::Unsupported.
class BotoClient {
public:
  virtual ~BotoClient() = default;
  // get_object: object body, or nullopt for a NoSuchKey miss.
  virtual std::optional<std::string> get_object(const std::string& bucket, const std::string& key) = 0;
  virtual void put_object(const std::string& bucket, const std::string& key, const std::string& body,
                          const std::string& content_type) = 0;
  virtual void delete_object(const std::string& bucket, const std::string& key) = 0;
  virtual bool head_object(const std::string& bucket, const std::string& key) = 0;
  virtual std::vector<std::string> list_objects_v2(const std::string& bucket) = 0;
};

// Factory for the aws-sdk-cpp-backed client. Defined by the posix S3 backend
// when built with LAILA_WITH_S3 (the [s3] extra); otherwise a stub returns
// nullptr (so S3Pool reports Unsupported = "no S3 backend compiled in").
std::unique_ptr<BotoClient> make_aws_s3_client(const std::string& access_key_id,
                                               const std::string& secret_access_key,
                                               const std::string& region_name,
                                               int max_pool_connections);

// BotoPool (pool/boto/boto.py): abstract base for S3-API-compatible pools. The
// vendor-specific client lives in _get_client(); key encoding, throttling, and
// the read/write/delete/exists/keys/empty hooks are implemented here.
class BotoPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit BotoPool(std::string bucket_name) {
    bucket_name_ = std::move(bucket_name);
    set_transformations(TransformationSequence::base64());  // boto.py default transformation_base64
  }
  const std::string& bucket_name() const { return bucket_name_; }

protected:
  BotoPool() { set_transformations(TransformationSequence::base64()); }

  // Subclasses return a configured client; base raises (mirrors boto.py's
  // NotImplementedError "Subclasses must implement _get_client").
  virtual BotoClient* _get_client();

  std::string _object_key(const std::string& key) const;          // quote(key, safe="")
  std::string _logical_key(const std::string& object_key) const;  // unquote(object_key)
  void _throttle();

  std::optional<Json> _read(const std::string& key) override;
  void _write(const std::string& key, const Json& value) override;
  void _delete(const std::string& key) override;
  bool _exists(const std::string& key) override;
  std::vector<std::string> _keys() override;
  void _empty() override;

  std::string bucket_name_;
  std::optional<double> max_req_per_second_;
  bool async_default_ = true;
  int max_pool_connections_ = 128;
};

// Mirrors S3Pool's constructor kwargs (pydantic fields): bucket_name (BotoPool),
// access_key_id / secret_access_key / region_name (S3Pool), nickname (base
// pool). Python kwargs -> matching *Opts struct.
struct S3PoolOpts {
  std::string bucket_name;
  std::optional<std::string> access_key_id = std::nullopt;
  std::optional<std::string> secret_access_key = std::nullopt;
  std::optional<std::string> region_name = std::nullopt;
  std::optional<std::string> nickname = std::nullopt;
};

// S3Pool (pool/s3/s3.py): AWS S3-backed pool. _get_client() builds a real
// aws-sdk-cpp client when LAILA_WITH_S3 is compiled in, else raises Unsupported.
class S3Pool : public BotoPool {
public:
  using Opts = S3PoolOpts;
  explicit S3Pool(const S3PoolOpts& opts) {
    bucket_name_ = opts.bucket_name;
    access_key_id_ = opts.access_key_id.value_or("");
    secret_access_key_ = opts.secret_access_key.value_or("");
    region_name_ = opts.region_name.value_or("");
    if (opts.nickname) set_nickname(*opts.nickname);
  }

protected:
  BotoClient* _get_client() override;

private:
  std::string access_key_id_, secret_access_key_, region_name_;
  std::unique_ptr<BotoClient> client_;  // cached, like boto.py's self._client
};

// CloudflarePool / BackblazePool (BotoPool subclasses). Their vendor clients are
// not wired into laila-C, so _get_client() falls through to BotoPool's
// Unsupported until a backend is added.
class CloudflarePool : public BotoPool {
public:
  explicit CloudflarePool(std::string bucket) : BotoPool(std::move(bucket)) {}
};
class BackblazePool : public BotoPool {
public:
  explicit BackblazePool(std::string bucket) : BotoPool(std::move(bucket)) {}
};

// ---- Other object stores (direct base subclasses in Python) ----
class GCSPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit GCSPool(std::string bucket) : bucket_(std::move(bucket)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("GCSPool")
 private:
  std::string bucket_;
};
class AzurePool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit AzurePool(std::string container) : container_(std::move(container)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("AzurePool")
 private:
  std::string container_;
};
class HuggingFacePool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit HuggingFacePool(std::string repo) : repo_(std::move(repo)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("HuggingFacePool")
 private:
  std::string repo_;
};

// ---- Key/value ----
// Mirrors RedisPool's kwargs (redis.py, all defaulted) + nickname (base pool).
// The redis backend is not wired into laila-C, so I/O raises Unsupported; the
// config fields are kept for surface parity.
struct RedisPoolOpts {
  std::optional<std::string> nickname = std::nullopt;
  std::optional<std::string> key_prefix = std::nullopt;      // Field("pool")
  std::optional<std::string> lock_prefix = std::nullopt;     // Field("pool_lock")
  std::optional<std::string> redis_password = std::nullopt;  // Field(None)
};
class RedisPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  using Opts = RedisPoolOpts;
  explicit RedisPool(const RedisPoolOpts& opts = {}) {
    if (opts.nickname) set_nickname(*opts.nickname);
  }
  LAILA_POOL_UNSUPPORTED_HOOKS("RedisPool")
};

// ---- SQL / document ----
class SQLitePool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit SQLitePool(std::string path) : path_(std::move(path)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("SQLitePool")
 private:
  std::string path_;
};
class DuckDBPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit DuckDBPool(std::string path) : path_(std::move(path)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("DuckDBPool")
 private:
  std::string path_;
};
class PostgresPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit PostgresPool(std::string dsn) : dsn_(std::move(dsn)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("PostgresPool")
 private:
  std::string dsn_;
};
class MongoPool : public _LAILA_IDENTIFIABLE_POOL {
public:
  explicit MongoPool(std::string uri) : uri_(std::move(uri)) {}
  LAILA_POOL_UNSUPPORTED_HOOKS("MongoPool")
 private:
  std::string uri_;
};

// ---- Blob ----
// Mirrors HDF5Pool's kwargs (hdf5.py): nickname (base) + optional file_path. No
// HDF5 backend on this target, so I/O raises Unsupported.
struct HDF5PoolOpts {
  std::optional<std::string> nickname = std::nullopt;
  std::optional<std::string> file_path = std::nullopt;  // hdf5.py Field(None)
};
class HDF5Pool : public _LAILA_IDENTIFIABLE_POOL {
public:
  using Opts = HDF5PoolOpts;
  explicit HDF5Pool(const HDF5PoolOpts& opts = {}) {
    if (opts.nickname) set_nickname(*opts.nickname);
  }
  LAILA_POOL_UNSUPPORTED_HOOKS("HDF5Pool")
};

}  // namespace laila_c

#endif  // LAILA_POOLS_HPP
