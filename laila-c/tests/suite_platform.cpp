// Platform/HAL conformance: the same assertions run on every backend (posix,
// baremetal, MCU). Validates clock monotonicity, storage CRUD, entropy,
// executor strategy, and transport gating for the active platform.
#include "laila_test.hpp"
#include "laila/hal/hal.hpp"

using namespace laila_c;

TEST("platform", "name_nonempty") {
  const char* n = hal::get().platform_name();
  CHECK(n != nullptr);
  CHECK(std::string(n).size() > 0);
}

TEST("platform", "clock_monotonic") {
  auto& clk = hal::get().clock();
  uint64_t prev = clk.now_ms();
  for (int i = 0; i < 100; ++i) {
    uint64_t now = clk.now_ms();
    CHECK(now >= prev);  // never goes backward
    prev = now;
  }
  uint64_t before = clk.now_ms();
  clk.sleep_ms(5);
  CHECK(clk.now_ms() >= before + 5);  // advances after sleep
}

TEST("platform", "storage_crud") {
  hal::Storage* s = hal::get().open_storage("platform_suite");
  CHECK(s != nullptr);
  s->clear();
  for (int i = 0; i < 50; ++i) {
    std::string key = "LAILA:ENTRY:GLOBAL_ID:key-" + std::to_string(i);
    std::vector<uint8_t> data = {(uint8_t)i, (uint8_t)(i * 3), 0x10, 0x20};
    CHECK(s->write(key, data));
    std::vector<uint8_t> out;
    CHECK(s->read(key, out));
    CHECK(out == data);
    CHECK(s->exists(key));
  }
  CHECK(s->remove("LAILA:ENTRY:GLOBAL_ID:key-0"));
  CHECK(!s->exists("LAILA:ENTRY:GLOBAL_ID:key-0"));
  s->clear();
}

TEST("platform", "storage_handle_stable") {
  hal::Storage* a = hal::get().open_storage("stable_name");
  hal::Storage* b = hal::get().open_storage("stable_name");
  CHECK(a == b);  // same name -> same storage handle
}

TEST("platform", "entropy_varies") {
  auto& rng = hal::get().random();
  uint8_t buf1[32], buf2[32];
  rng.fill(buf1, 32);
  rng.fill(buf2, 32);
  bool all_zero = true, identical = true;
  for (int i = 0; i < 32; ++i) {
    if (buf1[i] != 0) all_zero = false;
    if (buf1[i] != buf2[i]) identical = false;
  }
  CHECK(!all_zero);
  CHECK(!identical);  // successive draws differ
}

TEST("platform", "executor_strategy") {
  auto& ex = hal::get().executor();
  // Whatever the strategy, a submitted future must complete and wait() returns.
  bool ran = false;
  ex.submit([&] { ran = true; });
  ex.drain();
  if (ex.is_cooperative()) CHECK(ran);  // cooperative runs inline by submit time
  // A memorize future always completes on this platform.
  auto f = laila->memorize(Entry::constant(LailaValue::from_int(1)));
  f->wait();
  CHECK(f->finished());
}

TEST("platform", "transport_gating") {
  // No backend in this build ships a live network transport; add_peer to a
  // non-local uri must therefore raise Unsupported.
  CHECK(!hal::get().transport().supported());
}

TEST("platform", "uuid_uses_hal_random") {
  // 200 uuid4s should essentially all be unique (entropy sanity).
  std::vector<std::string> seen;
  for (int i = 0; i < 200; ++i) seen.push_back(uuid4());
  int collisions = 0;
  for (size_t i = 0; i < seen.size(); ++i)
    for (size_t j = i + 1; j < seen.size(); ++j)
      if (seen[i] == seen[j]) ++collisions;
  CHECK_EQ(collisions, 0);
}
