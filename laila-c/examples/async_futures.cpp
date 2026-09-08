// Mirrors tutorials/01_basics/07_async_futures.ipynb using C++20 coroutines.
// The blocking laila Future API (->wait()/->result(), RAII guarantee()) is
// replaced by `co_await` / `guarantee_async` from laila/async.hpp, so this
// reads line-for-line like the Python `await` tutorial.
//
// Prereqs (like the notebook): build with -DLAILA_WITH_S3=ON and provide a
// ./secrets.toml alongside the binary; otherwise S3 I/O raises
// LAILA_UNSUPPORTED at runtime.
#include <cstdio>
#include <string>
#include <vector>

#include "laila/async.hpp"
#include "laila/laila.hpp"
#include "laila/pools.hpp"

using namespace laila_c;

// Step 2: async worker -- remember entries, double each value, write back.
// (The C++ facade's remember is single-id, so we co_await one id at a time.)
Task<std::vector<EntryPtr>> double_entries(std::vector<std::string> ids, std::string pool) {
  RememberOpts ro;
  ro.pool_nickname = pool;
  std::vector<EntryPtr> remembered;
  for (auto& id : ids) remembered.push_back(co_await laila->remember(id, ro));

  std::vector<EntryPtr> doubled;
  int i = 1;
  for (auto& e : remembered) {
    ConstantOpts co;
    co.nickname = "doubled_number_" + std::to_string(i++);
    doubled.push_back(laila->constant(e->data().as_int() * 2, co));
  }

  co_await laila->memorize(doubled);
  co_return doubled;
}

Task<void> app() {
  // Load credentials and register the S3 pool.
  laila->read_args("./secrets.toml");

  S3Pool::Opts o;
  o.bucket_name = laila->args().get("AWS_BUCKET_NAME");
  o.access_key_id = laila->args().get("AWS_ACCESS_KEY_ID");
  o.secret_access_key = laila->args().get("AWS_SECRET_ACCESS_KEY");
  o.region_name = laila->args().get("AWS_REGION");
  o.nickname = "async_pool";
  ExtendOpts eo;
  eo.pool_nickname = "async_pool";
  laila->memory->extend(std::make_shared<S3Pool>(o), eo);

  // Step 1: create + upload 1..10; await the GroupFuture directly.
  std::vector<EntryPtr> entries;
  for (int i = 1; i <= 10; ++i) {
    ConstantOpts co;
    co.nickname = "number_" + std::to_string(i);
    entries.push_back(laila->constant((int64_t)i, co));
  }
  auto up = laila->memorize(entries);
  std::printf("Upload status before await: %s\n", future_status_name(runtime::status(up)));
  co_await up;
  std::printf("Upload status after await:  %s\n", future_status_name(runtime::status(up)));
  std::printf("Uploaded %zu entries to S3\n", entries.size());

  // Step 3: double them.
  std::vector<std::string> ids;
  for (auto& e : entries) ids.push_back(e->global_id());
  auto doubled = co_await double_entries(ids, "async_pool");

  std::printf("%10s  %10s\n", "Original", "Doubled");
  std::printf("-----------------------\n");
  for (size_t k = 0; k < entries.size(); ++k)
    std::printf("%10lld  %10lld\n", (long long)entries[k]->data().as_int(),
                (long long)doubled[k]->data().as_int());

  // Step 4: verify round-trip purely by global_id.
  std::vector<std::string> doubled_ids;
  for (auto& e : doubled) doubled_ids.push_back(e->global_id());
  doubled.clear();

  RememberOpts vro;
  vro.pool_nickname = "async_pool";
  std::printf("Verified doubled values from S3:\n");
  int vi = 1;
  for (auto& gid : doubled_ids) {
    auto entry = co_await laila->remember(gid, vro);
    long long expected = (long long)vi * 2;
    long long got = (long long)entry->data().as_int();
    std::printf("  number_%d: expected %lld, got %lld%s\n", vi, expected, got,
                got == expected ? "" : "  <-- MISMATCH");
    ++vi;
  }

  // Step 5: reactive processing with guarantee_async.
  std::vector<EntryPtr> quadrupled;
  co_await guarantee_async([&]() -> Task<void> {
    int i = 1;
    for (auto& gid : doubled_ids) {
      RememberOpts ro;
      ro.pool_nickname = "async_pool";
      auto recalled = co_await laila->remember(gid, ro);

      ConstantOpts co;
      co.nickname = "quadrupled_number_" + std::to_string(i++);
      auto quad = laila->constant(recalled->data().as_int() * 2, co);

      MemorizeOpts mo;
      mo.pool_nickname = "async_pool";
      laila->memorize(quad, mo);  // tracked by the scope; awaited on exit
      quadrupled.push_back(quad);

      std::printf("  %4lld -> %4lld\n", (long long)recalled->data().as_int(),
                  (long long)quad->data().as_int());
    }
    co_return;
  });
  std::printf("Processed %zu entries inside guarantee_async\n", quadrupled.size());

  // Clean up.
  std::vector<std::string> all_ids = ids;
  all_ids.insert(all_ids.end(), doubled_ids.begin(), doubled_ids.end());
  for (auto& e : quadrupled) all_ids.push_back(e->global_id());
  co_await guarantee_async([&]() -> Task<void> {
    for (auto& gid : all_ids) {
      ForgetOpts fo;
      fo.pool_nickname = "async_pool";
      laila->forget(gid, fo);
    }
    co_return;
  });
  std::printf("Cleaned up %zu entries from S3\n", all_ids.size());

  co_return;
}

int main() {
  run(app());
  laila->terminate();
  return 0;
}
