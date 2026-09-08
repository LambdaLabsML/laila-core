// Stress / soak: high-volume memorize/remember/forget across mixed payload
// kinds with interleaved operations, plus deep evolve chains and many
// serialize round-trips. Catches state/lifetime regressions.
#include "laila_test.hpp"

using namespace laila_c;

static LailaValue gen(lt::Rng& rng) {
  switch (rng.range(0, 6)) {
    case 0: return LailaValue::from_int((int64_t)rng.u32() - 2000000000LL);
    case 1: return LailaValue::from_double((double)rng.range(-100000, 100000) / 7.0);
    case 2: {
      std::string s;
      int n = rng.range(0, 80);
      for (int i = 0; i < n; ++i) s.push_back((char)(' ' + (rng.u32() % 94)));
      return LailaValue::from_string(s);
    }
    case 3: {
      std::vector<uint8_t> b((size_t)rng.range(0, 128));
      for (auto& x : b) x = (uint8_t)(rng.u32() & 0xFF);
      return LailaValue::from_bytes(b);
    }
    case 4: {
      Json o = Json::object();
      int f = rng.range(0, 4);
      for (int i = 0; i < f; ++i) o["f" + std::to_string(i)] = (int64_t)rng.range(-9, 9);
      return LailaValue::from_json(o);
    }
    case 5: {
      std::vector<int64_t> shape = {rng.range(1, 4), rng.range(1, 4)};
      std::vector<uint8_t> raw((size_t)(shape[0] * shape[1]));
      for (auto& x : raw) x = (uint8_t)(rng.u32() & 0xFF);
      return LailaValue::tensor(raw, "uint8", shape);
    }
    default: return LailaValue::from_bool((rng.u32() % 2) == 0);
  }
}

TEST("stress", "mixed_memorize_remember") {
  lt::Rng rng(0x5172E55ull);
  std::vector<std::pair<std::string, LailaValue>> live;
  for (int i = 0; i < 400; ++i) {
    LailaValue v = gen(rng);
    auto e = Entry::constant(v);
    laila->memorize(e)->wait();
    live.emplace_back(e->global_id(), v);
    // Occasionally read back an earlier entry immediately.
    if (!live.empty() && (rng.u32() % 3) == 0) {
      auto& pick = live[rng.u32() % live.size()];
      CHECK(value_eq(laila->remember(pick.first)->data(), pick.second));
    }
  }
  // Full verification pass.
  for (auto& kv : live) CHECK(value_eq(laila->remember(kv.first)->data(), kv.second));
}

TEST("stress", "interleaved_forget") {
  lt::Rng rng(0xF06E7ull);
  std::vector<std::string> ids;
  for (int i = 0; i < 200; ++i) {
    auto e = Entry::constant(LailaValue::from_int(i));
    laila->memorize(e)->wait();
    ids.push_back(e->global_id());
  }
  for (size_t i = 0; i < ids.size(); i += 2) laila->forget(ids[i])->wait();
  for (size_t i = 0; i < ids.size(); ++i) {
    if (i % 2 == 0) {
      CHECK_THROWS(laila->remember(ids[i])->data(), LailaError);
    } else {
      CHECK_EQ(laila->remember(ids[i])->data().as_int(), (int64_t)i);
    }
  }
}

TEST("stress", "deep_evolve_chain") {
  auto cur = Entry::variable(LailaValue::from_int(0));
  std::string uuid = cur->uuid();
  for (int i = 1; i <= 300; ++i) {
    cur = cur->evolve(LailaValue::from_int(i));
    CHECK_EQ(cur->uuid(), uuid);
    CHECK_EQ(*cur->evolution(), (int64_t)i);
  }
  CHECK_EQ(cur->data().as_int(), (int64_t)300);
}

TEST("stress", "serialize_roundtrip_volume") {
  lt::Rng rng(0x5E812EFFull);
  TransformationSequence t = TransformationSequence::base64();
  for (int i = 0; i < 300; ++i) {
    LailaValue v = gen(rng);
    auto e = Entry::constant(v);
    auto rebuilt = Entry::build_from_dict(e->serialize(&t));
    CHECK(value_eq(rebuilt->data(), v));
  }
}
