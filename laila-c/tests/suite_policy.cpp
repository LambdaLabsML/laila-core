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

TEST("policy", "memorize_locally_modified_bumps_evolution") {
  // Fresh variables start clean and keep their evolution on the first memorize.
  VariableOpts vo;
  vo.evolution = 5;
  auto v = Entry::variable(LailaValue::from_int(1), vo);
  CHECK(!v->locally_modified());
  laila->memorize(v)->wait();
  CHECK_EQ(*v->evolution(), (int64_t)5);
  // Untouched -> idempotent re-write under the same key.
  laila->memorize(v)->wait();
  CHECK_EQ(*v->evolution(), (int64_t)5);
  std::string hb5 = v->creation_timestamp();
  std::string gid5 = v->global_id();
  // Re-assigned payload -> next evolution in place, fresh creation_timestamp, new key.
  hal::get().clock().sleep_ms(2);
  v->set_data(LailaValue::from_int(2));
  CHECK(v->locally_modified());
  laila->memorize(v)->wait();
  CHECK(!v->locally_modified());
  CHECK_EQ(*v->evolution(), (int64_t)6);
  CHECK(v->creation_timestamp() != hb5);
  CHECK(v->global_id() != gid5);
  // The previous evolution kept its own payload (no aliasing).
  CHECK_EQ(laila->remember(gid5)->data().as_int(), (int64_t)1);
  CHECK_EQ(laila->remember(v->global_id())->data().as_int(), (int64_t)2);
  // Constants are never bumped.
  auto c = Entry::constant(LailaValue::from_int(1));
  laila->memorize(c)->wait();
  c->set_data(LailaValue::from_int(9));
  laila->memorize(c)->wait();
  CHECK(!c->evolution().has_value());
  CHECK_EQ(laila->remember(c->global_id())->data().as_int(), (int64_t)9);
  // A remembered entry is the memorized baseline: not locally modified.
  auto r = laila->remember(v->global_id())->result();
  CHECK(!r->locally_modified());
  laila->memorize(r)->wait();
  CHECK_EQ(*r->evolution(), (int64_t)6);
}

TEST("policy", "remember_search_attributes") {
  VariableOpts vo;
  vo.nickname = "evo-nick";
  auto v = Entry::variable(LailaValue::from_int(0), vo);
  std::vector<std::string> stamps;
  laila->memorize(v)->wait();
  stamps.push_back(v->creation_timestamp());
  for (int i = 1; i <= 2; ++i) {
    hal::get().clock().sleep_ms(2);
    v->set_data(LailaValue::from_int(i));
    laila->memorize(v)->wait();
    stamps.push_back(v->creation_timestamp());
  }
  const std::string base = strip_global_id_attributes(v->global_id());
  // No evolution -> highest stored evolution.
  auto hi = laila->remember(base)->result();
  CHECK_EQ(*hi->evolution(), (int64_t)2);
  CHECK_EQ(hi->data().as_int(), (int64_t)2);
  // Explicit evolution -> exact.
  auto e1 = laila->remember(base + "@evolution=1")->result();
  CHECK_EQ(*e1->evolution(), (int64_t)1);
  CHECK_EQ(e1->data().as_int(), (int64_t)1);
  // creation_timestamp -> the evolution stamped at that instant.
  auto t1 = laila->remember(base + "@creation_timestamp=" + stamps[1])->result();
  CHECK_EQ(*t1->evolution(), (int64_t)1);
  auto t2 = laila->remember(base + "@evolution=2,creation_timestamp=" + stamps[2])->result();
  CHECK_EQ(*t2->evolution(), (int64_t)2);
  // Mismatch / unknown attribute / missing.
  CHECK_THROWS(laila->remember(base + "@evolution=1,creation_timestamp=" + stamps[2])->result(),
               LailaError);
  CHECK_THROWS(laila->remember(base + "@creation_timestamp=1970-01-01T00:00:00.000+00:00")->result(),
               LailaError);
  CHECK_THROWS(laila->remember(base + "@foo=bar")->result(), LailaError);
  CHECK_THROWS(laila->remember("LAILA:ENTRY:00000000-0000-0000-0000-00000000dead")->result(),
               LailaError);
  // Exact (constant) key is preferred over an evolution scan.
  ConstantOpts co;
  co.nickname = "mixed-nick";
  auto c = Entry::constant(LailaValue::from_string("const"), co);
  laila->memorize(c)->wait();
  auto got = laila->remember(c->global_id())->result();
  CHECK(!got->evolution().has_value());
  CHECK_EQ(got->data().as_string(), std::string("const"));
}

TEST("policy", "remember_highest_evolution_through_proxy_chain") {
  auto origin = std::make_shared<DefaultPool>();
  origin->set_nickname("chain-origin");
  auto front = std::make_shared<DefaultPool>();
  front->set_nickname("chain-front");
  *front << *origin;  // front caches origin
  auto& mem = get_active_policy()->memory();
  mem.extend(origin, {.pool_nickname = "chain-origin"});
  mem.extend(front, {.pool_nickname = "chain-front"});

  auto v = Entry::variable(LailaValue::from_int(0));
  mem.memorize(v, "chain-origin");
  std::string hb0 = v->creation_timestamp();
  hal::get().clock().sleep_ms(2);
  v->set_data(LailaValue::from_int(1));
  mem.memorize(v, "chain-origin");
  const std::string base = strip_global_id_attributes(v->global_id());

  auto hi = mem.remember(base, "chain-front", false);
  CHECK_EQ(*hi->evolution(), (int64_t)1);
  // Only the winner is cached into the front tier.
  CHECK_EQ(front->keys().size(), (size_t)1);
  CHECK_EQ(front->keys()[0], v->global_id());
  auto t0 = mem.remember(base + "@creation_timestamp=" + hb0, "chain-front", false);
  CHECK_EQ(*t0->evolution(), (int64_t)0);
  // Resolved through the origin's index, then read through the chain like any
  // other key: the front tier caches it and indexes it.
  CHECK_EQ(front->keys().size(), (size_t)2);
  CHECK_EQ(*front->index().latest(base), v->global_id());
  // Entry keys only in keys(); the shard is visible through raw_keys().
  CHECK_EQ(front->raw_keys().size(), (size_t)3);
}

namespace {
// Pool with a trivial evolution index attached through _search_keys; counts
// full _keys() scans so the test can assert the index short-circuits them.
class IndexedPool : public DefaultPool {
public:
  int scans = 0;
  std::optional<std::vector<std::string>> _search_keys(const std::string& base_gid,
                                                       const GidAttributes&) override {
    return _candidate_keys_indexed(base_gid);
  }
protected:
  std::vector<std::string> _keys() override {
    ++scans;
    return DefaultPool::_keys();
  }
private:
  std::vector<std::string> _candidate_keys_indexed(const std::string& base_gid) {
    std::vector<std::string> out;
    const std::string prefix = base_gid + "@";
    for (const auto& kv : resource_)
      if (kv.first == base_gid || kv.first.compare(0, prefix.size(), prefix) == 0) out.push_back(kv.first);
    return out;
  }
};
}  // namespace

TEST("policy", "search_keys_index_hook_skips_scan") {
  auto indexed = std::make_shared<IndexedPool>();
  indexed->set_nickname("indexed");
  indexed->set_index_enabled(false);  // the custom hook is the only index here
  auto& mem = get_active_policy()->memory();
  mem.extend(indexed, {.pool_nickname = "indexed"});
  auto v = Entry::variable(LailaValue::from_int(0));
  mem.memorize(v, "indexed");
  v->set_data(LailaValue::from_int(1));
  mem.memorize(v, "indexed");
  int before = indexed->scans;
  auto hi = mem.remember(strip_global_id_attributes(v->global_id()), "indexed", false);
  CHECK_EQ(*hi->evolution(), (int64_t)1);
  CHECK_EQ(indexed->scans, before);
}

namespace {
// Plain pool that counts full _keys() scans (built-in index left on).
class CountingPool : public DefaultPool {
public:
  int scans = 0;
protected:
  std::vector<std::string> _keys() override {
    ++scans;
    return DefaultPool::_keys();
  }
};

struct ThreeEvolutions {
  EntryPtr v;
  std::vector<std::string> stamps;
  std::string base;
};

ThreeEvolutions store_three(CentralMemory& mem, const std::string& pool_nickname, const char* nick) {
  VariableOpts vo;
  vo.nickname = nick;
  ThreeEvolutions out;
  out.v = Entry::variable(LailaValue::from_int(0), vo);
  mem.memorize(out.v, pool_nickname);
  out.stamps.push_back(out.v->creation_timestamp());
  for (int i = 1; i <= 2; ++i) {
    hal::get().clock().sleep_ms(2);
    out.v->set_data(LailaValue::from_int(i));
    mem.memorize(out.v, pool_nickname);
    out.stamps.push_back(out.v->creation_timestamp());
  }
  out.base = strip_global_id_attributes(out.v->global_id());
  return out;
}
}  // namespace

TEST("policy", "builtin_index_resolves_without_scan") {
  auto pool = std::make_shared<CountingPool>();
  auto& mem = get_active_policy()->memory();
  mem.extend(pool, {.pool_nickname = "cnt"});
  auto t = store_three(mem, "cnt", "cnt-nick");
  // Shards are hidden from keys(), visible through raw_keys().
  CHECK_EQ(pool->keys().size(), (size_t)3);
  CHECK_EQ(pool->raw_keys().size(), (size_t)4);
  for (const auto& k : pool->keys()) CHECK(!is_index_key(k));
  int before = pool->scans;
  CHECK_EQ(*mem.remember(t.base, "cnt", false)->evolution(), (int64_t)2);
  CHECK_EQ(*mem.remember(t.base + "@evolution=-1", "cnt", false)->evolution(), (int64_t)2);
  CHECK_EQ(*mem.remember(t.base + "@evolution=-2", "cnt", false)->evolution(), (int64_t)1);
  CHECK_EQ(*mem.remember(t.base + "@evolution=-3", "cnt", false)->evolution(), (int64_t)0);
  CHECK_EQ(*mem.remember(t.base + "@creation_timestamp=" + t.stamps[1], "cnt", false)->evolution(), (int64_t)1);
  CHECK_EQ(*mem.remember(t.base + "@evolution=-1,creation_timestamp=" + t.stamps[2], "cnt", false)->evolution(),
           (int64_t)2);
  CHECK_EQ(pool->scans, before);
  CHECK_THROWS(mem.remember(t.base + "@evolution=-4", "cnt", false), LailaError);
  CHECK_THROWS(mem.remember(t.base + "@evolution=-1,creation_timestamp=" + t.stamps[0], "cnt", false),
               LailaError);
  // Index queries directly.
  auto cands = pool->index().candidates(t.base);
  CHECK(cands.has_value());
  CHECK_EQ(cands->size(), (size_t)3);
  CHECK_EQ(*pool->index().latest(t.base), t.v->global_id());
  CHECK(!pool->index().nth(t.base, 7).has_value());
  CHECK(!pool->index().candidates("LAILA:ENTRY:00000000-0000-0000-0000-000000000000").has_value());
}

TEST("policy", "index_disabled_falls_back_to_scan") {
  auto pool = std::make_shared<CountingPool>();
  pool->set_index_enabled(false);
  auto& mem = get_active_policy()->memory();
  mem.extend(pool, {.pool_nickname = "noidx"});
  auto t = store_three(mem, "noidx", "noidx-nick");
  CHECK_EQ(pool->raw_keys().size(), (size_t)3);  // no shards
  int before = pool->scans;
  CHECK_EQ(*mem.remember(t.base + "@evolution=-1", "noidx", false)->evolution(), (int64_t)2);
  CHECK_EQ(*mem.remember(t.base + "@evolution=-2", "noidx", false)->evolution(), (int64_t)1);
  CHECK(pool->scans > before);
}

TEST("policy", "index_stale_hit_self_heals") {
  auto pool = std::make_shared<DefaultPool>();
  auto& mem = get_active_policy()->memory();
  mem.extend(pool, {.pool_nickname = "heal"});
  auto t = store_three(mem, "heal", "heal-nick");
  // Delete the latest evolution behind the index's back (raw hook).
  struct Peek : DefaultPool { using DefaultPool::_delete; };
  static_cast<Peek*>(pool.get())->_delete(t.v->global_id());
  CHECK_EQ(*mem.remember(t.base + "@evolution=-1", "heal", false)->evolution(), (int64_t)1);
  auto cands = pool->index().candidates(t.base);
  CHECK(cands.has_value());
  CHECK_EQ(cands->size(), (size_t)2);
}

TEST("policy", "forget_negative_evolution_and_remove") {
  auto pool = std::make_shared<DefaultPool>();
  auto& mem = get_active_policy()->memory();
  mem.extend(pool, {.pool_nickname = "fgt"});
  auto t = store_three(mem, "fgt", "fgt-nick");
  mem.forget(t.base + "@evolution=-1", "fgt");
  CHECK(!pool->exists(t.v->global_id()));
  CHECK_EQ(*mem.remember(t.base, "fgt", false)->evolution(), (int64_t)1);
  // Evolution-less forget stays exact-key (nothing named exactly `base`).
  mem.forget(t.base, "fgt");
  CHECK_EQ(*mem.remember(t.base, "fgt", false)->evolution(), (int64_t)1);
  // Removing every evolution drops the shard entirely.
  mem.forget(t.base + "@evolution=1", "fgt");
  mem.forget(t.base + "@evolution=0", "fgt");
  CHECK_EQ(pool->raw_keys().size(), (size_t)0);
  CHECK(!pool->index().candidates(t.base).has_value());
}

TEST("policy", "index_in_foreign_pool_and_reload") {
  auto ram = std::make_shared<DefaultPool>();
  auto data = std::make_shared<DefaultPool>();
  data->set_index_pool(ram.get());
  auto& mem = get_active_policy()->memory();
  mem.extend(data, {.pool_nickname = "data"});
  auto t = store_three(mem, "data", "foreign-nick");
  CHECK_EQ(data->raw_keys().size(), (size_t)3);   // data pool holds no shards
  CHECK_EQ(ram->raw_keys().size(), (size_t)1);    // exactly one shard, latest evolution only
  CHECK(is_index_key(ram->raw_keys()[0]));
  CHECK_EQ(*mem.remember(t.base + "@evolution=-1", "data", false)->evolution(), (int64_t)2);
  // A fresh index over the same storage reloads the persisted shard.
  data->index().invalidate();
  CHECK_EQ(*data->index().latest(t.base), t.v->global_id());
  CHECK_EQ(*data->index().by_creation_timestamp(t.base, t.stamps[0]), t.base + "@evolution=0");
  // empty() drops the shards held by the foreign pool too.
  data->empty();
  CHECK_EQ(ram->raw_keys().size(), (size_t)0);
  CHECK_EQ(data->raw_keys().size(), (size_t)0);
}

TEST("policy", "index_rebuild_over_unindexed_keys") {
  auto pool = std::make_shared<DefaultPool>();
  pool->set_index_enabled(false);
  auto& mem = get_active_policy()->memory();
  mem.extend(pool, {.pool_nickname = "rb"});
  auto t = store_three(mem, "rb", "rb-nick");
  pool->set_index_enabled(true);
  CHECK(!pool->index().candidates(t.base).has_value());
  pool->index().rebuild();
  CHECK_EQ(*pool->index().latest(t.base), t.v->global_id());
  CHECK_EQ(*pool->index().by_creation_timestamp(t.base, t.stamps[1]), t.base + "@evolution=1");
  CHECK_EQ(pool->raw_keys().size(), (size_t)4);
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
