// Policy/facade: memorize/remember/forget across many diverse payloads,
// nickname addressing, and policy lifecycle (active policy, started state).
#include "laila_test.hpp"

using namespace laila_c;

static LailaValue gen(lt::Rng& rng, int i) {
  switch (i % 5) {
    case 0: return LailaValue::from_int(rng.range(-1000000, 1000000));
    case 1: return LailaValue::from_string(std::string("p") + std::to_string(rng.u32()));
    case 2: {
      std::vector<uint8_t> b((size_t)rng.range(0, 64));
      for (auto& x : b) x = (uint8_t)(rng.u32() & 0xFF);
      return LailaValue::from_bytes(b);
    }
    case 3: {
      Json o = Json::object();
      o["k"] = (int64_t)rng.range(0, 1000);
      return LailaValue::from_json(o);
    }
    default: return LailaValue::from_double((double)rng.range(-500, 500) / 4.0);
  }
}

TEST("policy", "memorize_remember_many") {
  lt::Rng rng(0xB011C1ull);
  std::vector<std::pair<std::string, LailaValue>> saved;
  for (int i = 0; i < 250; ++i) {
    LailaValue v = gen(rng, i);
    auto e = Entry::constant(v);
    laila->memorize(e)->wait();
    saved.emplace_back(e->global_id(), v);
  }
  for (auto& kv : saved) {
    auto got = laila->remember(kv.first)->data();
    CHECK(value_eq(got, kv.second));
  }
}

TEST("policy", "forget_removes") {
  for (int i = 0; i < 50; ++i) {
    auto e = Entry::constant(LailaValue::from_int(i));
    std::string gid = e->global_id();
    laila->memorize(e)->wait();
    CHECK(value_eq(laila->remember(gid)->data(), LailaValue::from_int(i)));
    laila->forget(gid)->wait();
    CHECK_THROWS(laila->remember(gid)->data(), LailaError);  // gone
  }
}

TEST("policy", "nickname_addressing") {
  for (int i = 0; i < 50; ++i) {
    std::string nick = "asset-" + std::to_string(i);
    ConstantOpts opts;
    opts.nickname = nick;
    auto e = Entry::constant(LailaValue::from_string("payload-" + std::to_string(i)), opts);
    laila->memorize(e)->wait();
    RememberOpts r;
    r.nickname = nick;
    auto got = laila->remember("", r)->data();
    CHECK_EQ(got.as_string(), std::string("payload-" + std::to_string(i)));
  }
}

TEST("policy", "facade_constant_variable") {
  // laila->constant / laila->variable mirror laila.constant / laila.variable.
  auto c = laila->constant(LailaValue::from_int(11));
  CHECK(!c->evolution().has_value());
  auto v = laila->variable(LailaValue::from_int(22));
  CHECK(v->evolution().has_value());
}

TEST("policy", "lifecycle") {
  // laila is "started" iff there is an active local policy.
  auto p = get_active_policy();
  CHECK(p != nullptr);
  CHECK(!local_policies().empty());
  CHECK(is_laila_resource(p->global_id()));
  // alpha pool exists and is the default in-memory pool.
  CHECK(laila->alpha_pool() != nullptr);
}

TEST("policy", "build_via_facade") {
  register_builder("pol_double", [](const Manifest& m) {
    return LailaValue::from_int(m.get("n")->data().as_int() * 2);
  });
  for (int i = 0; i < 30; ++i) {
    auto man = std::make_shared<Manifest>();
    man->put("n", Entry::constant(LailaValue::from_int(i)));
    VariableOpts o;
    o.constitution = "pol_double";
    o.manifest = man;
    auto e = laila->variable(LailaValue::none(), o);
    laila->build(e)->wait();
    CHECK_EQ(e->data().as_int(), (int64_t)(i * 2));
  }
}
