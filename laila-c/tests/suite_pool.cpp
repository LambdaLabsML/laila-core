// Pools: DefaultPool, FilesystemPool (HAL storage), proxy chaining/write-back,
// and the LAILA_UNSUPPORTED gating for every named remote-backend pool class.
#include "laila_test.hpp"

using namespace laila_c;

static Json blob_for(int i) {
  Json o = Json::object();
  o["i"] = (int64_t)i;
  o["s"] = std::string("val-") + std::to_string(i);
  return o;
}

TEST("pool", "ram_crud") {
  DefaultPool p;
  for (int i = 0; i < 200; ++i) p.put("k" + std::to_string(i), blob_for(i));
  CHECK_EQ(p.keys().size(), (size_t)200);
  for (int i = 0; i < 200; ++i) {
    auto v = p.get("k" + std::to_string(i));
    CHECK(v.has_value());
    CHECK_EQ(v->at("i").as_int(), (int64_t)i);
    CHECK(p.exists("k" + std::to_string(i)));
  }
  for (int i = 0; i < 100; ++i) p.erase("k" + std::to_string(i));
  CHECK_EQ(p.keys().size(), (size_t)100);
  CHECK(!p.exists("k0"));
  CHECK(!p.get("missing").has_value());
  p.empty();
  CHECK_EQ(p.keys().size(), (size_t)0);
}

TEST("pool", "filesystem_crud") {
  FilesystemPool p({.storage_name = "suite_pool_fs"});
  p.empty();
  for (int i = 0; i < 100; ++i) {
    auto e = Entry::constant(LailaValue::from_bytes({(uint8_t)i, (uint8_t)(i + 1), 0xAB}));
    const TransformationSequence& t = p.transformations();
    p.put(e->global_id(), e->serialize(t.empty() ? nullptr : &t));
    auto got = p.get(e->global_id());
    CHECK(got.has_value());
    auto rebuilt = Entry::build_from_dict(*got);
    CHECK(value_eq(rebuilt->data(), e->data()));
  }
  p.empty();
}

TEST("pool", "proxy_two_tier") {
  for (int trial = 0; trial < 20; ++trial) {
    DefaultPool cache;
    DefaultPool origin;
    cache << origin;  // cache fronts origin
    std::string key = "k" + std::to_string(trial);
    origin.put(key, blob_for(trial));
    CHECK(!cache.exists(key));        // not cached yet
    auto v = cache.get(key);          // miss -> fall through -> cache
    CHECK(v.has_value());
    CHECK_EQ(v->at("i").as_int(), (int64_t)trial);
    CHECK(cache.exists(key));         // write-back cached
  }
}

TEST("pool", "proxy_three_tier") {
  DefaultPool mem, mid, backing;
  mem << mid << backing;  // mem -> mid -> backing
  for (int i = 0; i < 30; ++i) backing.put("k" + std::to_string(i), blob_for(i));
  for (int i = 0; i < 30; ++i) {
    auto v = mem.get("k" + std::to_string(i));  // pulls through both tiers
    CHECK(v.has_value());
    CHECK_EQ(v->at("i").as_int(), (int64_t)i);
  }
}

TEST("pool", "operator_rshift") {
  DefaultPool origin, cache;
  origin >> cache;  // cache fronts origin
  origin.put("k", blob_for(7));
  auto v = cache.get("k");
  CHECK(v.has_value());
  CHECK_EQ(v->at("i").as_int(), (int64_t)7);
}

TEST("pool", "named_object_stores_unsupported") {
  std::vector<PoolPtr> pools = {
      std::make_shared<GCSPool>("b"),         std::make_shared<AzurePool>("c"),
      std::make_shared<BackblazePool>("b"),   std::make_shared<CloudflarePool>("b"),
      std::make_shared<BotoPool>("b"),        std::make_shared<HuggingFacePool>("r")};
#ifndef LAILA_WITH_S3
  // Without the [s3] backend compiled in, S3Pool also reports Unsupported.
  // (With LAILA_WITH_S3 it is functional and talks to real S3, so it's excluded.)
  pools.push_back(std::make_shared<S3Pool>(S3PoolOpts{.bucket_name = "b"}));
#endif
  Json b = blob_for(1);
  for (auto& p : pools) {
    CHECK_THROWS(p->put("k", b), UnsupportedError);
    CHECK_THROWS(p->get("k"), UnsupportedError);
    CHECK_THROWS(p->exists("k"), UnsupportedError);
    CHECK_THROWS(p->keys(), UnsupportedError);
    CHECK_THROWS(p->erase("k"), UnsupportedError);
  }
}

TEST("pool", "named_kv_sql_blob_unsupported") {
  std::vector<PoolPtr> pools = {
      std::make_shared<RedisPool>(),           std::make_shared<SQLitePool>("/x.db"),
      std::make_shared<DuckDBPool>("/x.duck"),  std::make_shared<PostgresPool>("dsn"),
      std::make_shared<MongoPool>("uri"),       std::make_shared<HDF5Pool>(HDF5PoolOpts{.file_path = "/x.h5"})};
  Json b = blob_for(2);
  for (auto& p : pools) {
    CHECK_THROWS(p->put("k", b), UnsupportedError);
    CHECK_THROWS(p->get("k"), UnsupportedError);
  }
}

// nickname Opts ctors + laila_pointer<T>({...}) (the mirror of Python's
// FilesystemPool(nickname=...), RedisPool(nickname=...), HDF5Pool(nickname=...)).
TEST("pool", "nickname_ctors") {
  // FilesystemPool via laila_pointer: nickname set, real round-trip on host.
  PoolPtr fs = laila_pointer<FilesystemPool>({.nickname = "tutorial_fs"});
  CHECK_EQ(fs->nickname(), std::string("tutorial_fs"));
  auto e = Entry::constant(ndarray<double>({1.0, 2.0, 3.0, 4.0}, {2, 2}));
  const TransformationSequence& t = fs->transformations();
  fs->put(e->global_id(), e->serialize(t.empty() ? nullptr : &t));
  auto got = fs->get(e->global_id());
  CHECK(got.has_value());
  CHECK(value_eq(Entry::build_from_dict(*got)->data(), e->data()));
  fs->empty();

  // Redis/HDF5 accept nickname too; I/O still raises Unsupported on host.
  PoolPtr rd = laila_pointer<RedisPool>({.nickname = "tutorial_redis"});
  CHECK_EQ(rd->nickname(), std::string("tutorial_redis"));
  CHECK_THROWS(rd->put("k", blob_for(1)), UnsupportedError);

  PoolPtr h5 = laila_pointer<HDF5Pool>({.nickname = "tutorial_hdf5"});
  CHECK_EQ(h5->nickname(), std::string("tutorial_hdf5"));
  CHECK_THROWS(h5->get("k"), UnsupportedError);
}
