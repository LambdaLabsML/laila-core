// Entry: factories, identity, evolve chains, as_dict/from_dict, and the
// base64-transform serialize round-trip, across many payloads.
#include "laila_test.hpp"

using namespace laila_c;

static LailaValue make_value(lt::Rng& rng, int which) {
  switch (which % 6) {
    case 0: return LailaValue::from_int(rng.range(-100000, 100000));
    case 1: return LailaValue::from_double((double)rng.range(-1000, 1000) / 8.0);
    case 2: {
      std::string s;
      int n = rng.range(0, 40);
      for (int i = 0; i < n; ++i) s.push_back((char)('a' + (rng.u32() % 26)));
      return LailaValue::from_string(s);
    }
    case 3: {
      std::vector<uint8_t> b((size_t)rng.range(0, 48));
      for (auto& x : b) x = (uint8_t)(rng.u32() & 0xFF);
      return LailaValue::from_bytes(b);
    }
    case 4: {
      Json o = Json::object();
      o["n"] = (int64_t)rng.range(0, 9999);
      o["s"] = std::string("tag");
      return LailaValue::from_json(o);
    }
    default: return LailaValue::from_bool((rng.u32() % 2) == 0);
  }
}

TEST("entry", "constant_basic") {
  lt::Rng rng(0xC04501ull);
  for (int i = 0; i < 120; ++i) {
    LailaValue v = make_value(rng, i);
    auto e = Entry::constant(v);
    CHECK(e->state() == EntryState::READY);
    CHECK(!e->evolution().has_value());
    CHECK(value_eq(e->data(), v));
    CHECK(is_laila_resource(e->global_id()));
  }
}

TEST("entry", "constant_with_uuid_and_nickname") {
  ConstantOpts o1;
  o1.uuid = "33333333-3333-3333-3333-333333333333";
  auto e1 = Entry::constant(LailaValue::from_int(1), o1);
  CHECK_EQ(e1->uuid(), std::string("33333333-3333-3333-3333-333333333333"));

  ConstantOpts o2;
  o2.nickname = "checkpoint";
  auto a = Entry::constant(LailaValue::from_int(1), o2);
  auto b = Entry::constant(LailaValue::from_int(2), o2);
  CHECK_EQ(a->uuid(), b->uuid());  // nickname -> deterministic uuid

  ConstantOpts bad;
  bad.uuid = "x";
  bad.global_id = "LAILA:ENTRY:GLOBAL_ID:33333333-3333-3333-3333-333333333333";
  CHECK_THROWS(Entry::constant(LailaValue::from_int(1), bad), LailaError);
}

TEST("entry", "variable_and_evolve") {
  for (int start = 0; start < 20; ++start) {
    VariableOpts o;
    o.evolution = start;
    auto v = Entry::variable(LailaValue::from_int(start), o);
    CHECK(v->evolution().has_value());
    CHECK_EQ(*v->evolution(), (int64_t)start);
    std::string uuid = v->uuid();
    auto cur = v;
    for (int step = 1; step <= 10; ++step) {
      cur = cur->evolve(LailaValue::from_int(start + step));
      CHECK_EQ(cur->uuid(), uuid);  // identity stable
      CHECK_EQ(*cur->evolution(), (int64_t)(start + step));
      CHECK_EQ(cur->data().as_int(), (int64_t)(start + step));
    }
  }
}

TEST("entry", "constant_cannot_evolve") {
  auto c = Entry::constant(LailaValue::from_int(5));
  CHECK_THROWS(c->evolve(LailaValue::from_int(6)), LailaError);
}

TEST("entry", "as_dict_roundtrip") {
  lt::Rng rng(0xA5D1Cull);
  for (int i = 0; i < 150; ++i) {
    LailaValue v = make_value(rng, i);
    auto e = Entry::constant(v);
    Json d = e->as_dict();
    auto e2 = Entry::build_from_dict(d);
    CHECK(value_eq(e2->data(), v));
    CHECK_EQ(e2->uuid(), e->uuid());
  }
}

TEST("entry", "serialize_base64_roundtrip") {
  lt::Rng rng(0x5E812Eull);
  TransformationSequence t = TransformationSequence::base64();
  for (int i = 0; i < 150; ++i) {
    LailaValue v = make_value(rng, i);
    auto e = Entry::constant(v);
    Json blob = e->serialize(&t);
    // Wire blob is pure text and carries a simple constitution.
    CHECK(blob.at("constitution").contains("_kind"));
    auto rebuilt = Entry::build_from_dict(blob);
    CHECK(value_eq(rebuilt->data(), v));
  }
}

TEST("entry", "serialize_requires_ready") {
  VariableOpts o;
  o.constitution = "noop_builder";
  o.manifest = std::make_shared<Manifest>();
  auto staged = Entry::variable(LailaValue::none(), o);
  CHECK(staged->state() == EntryState::STAGED);
  CHECK_THROWS(staged->serialize(), LailaError);     // not READY
  CHECK_THROWS(staged->data(), EntryNotBuiltError);  // unbuilt
}
