// Identity: uuid4 format, uuid5 determinism, global_id encode/parse across many
// scope/evolution combinations, and resource validation.
#include "laila_test.hpp"

using namespace laila_c;

TEST("identity", "uuid4_format") {
  for (int i = 0; i < 200; ++i) {
    std::string u = uuid4();
    CHECK_EQ(u.size(), (size_t)36);
    CHECK_EQ(u[8], '-');
    CHECK_EQ(u[13], '-');
    CHECK_EQ(u[14], '4');   // version
    CHECK_EQ(u[18], '-');
    CHECK_EQ(u[23], '-');
    char var = u[19];
    CHECK(var == '8' || var == '9' || var == 'a' || var == 'b');  // variant
  }
}

TEST("identity", "uuid5_determinism") {
  const std::string ns = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
  std::string prev;
  for (int i = 0; i < 150; ++i) {
    std::string name = "name-" + std::to_string(i);
    std::string a = uuid5(ns, name);
    std::string b = uuid5(ns, name);
    CHECK_EQ(a, b);             // deterministic
    CHECK_EQ(a[14], '5');       // version 5
    if (!prev.empty()) CHECK(a != prev);  // distinct names differ
    prev = a;
  }
}

TEST("identity", "nickname_namespace") {
  set_active_namespace("tenant-A");
  std::string a1 = generate_uuid_from_nickname("model");
  std::string a2 = generate_uuid_from_nickname("model");
  CHECK_EQ(a1, a2);
  set_active_namespace("tenant-B");
  std::string b1 = generate_uuid_from_nickname("model");
  CHECK(a1 != b1);  // different namespace -> different uuid
  set_active_namespace("laila");  // restore default-ish
}

TEST("identity", "global_id_roundtrip") {
  std::vector<std::vector<std::string>> scope_sets = {
      {scope::ENTRY}, {scope::POOL}, {scope::FUTURE}, {scope::POLICY}, {scope::MANIFEST}};
  std::vector<int64_t> evos = {0, 1, 2, 7, 42, 100, 999999};
  for (auto& scopes : scope_sets) {
    // constant (no evolution)
    {
      std::string uuid = uuid4();
      std::string gid = to_global_id(uuid, scopes, std::nullopt);
      CHECK(is_laila_resource(gid));
      ParsedGid p = process_global_id(gid);
      CHECK_EQ(p.uuid, uuid);
      CHECK(!p.evolution.has_value());
    }
    for (int64_t evo : evos) {
      std::string uuid = uuid4();
      std::string gid = to_global_id(uuid, scopes, evo);
      CHECK(is_laila_resource(gid));
      ParsedGid p = process_global_id(gid);
      CHECK_EQ(p.uuid, uuid);
      CHECK(p.evolution.has_value());
      CHECK_EQ(*p.evolution, evo);
    }
  }
}

TEST("identity", "global_id_format_literal") {
  std::string gid = to_global_id("11111111-1111-1111-1111-111111111111", {scope::ENTRY}, 3);
  CHECK_EQ(gid, std::string("LAILA:ENTRY:GLOBAL_ID:11111111-1111-1111-1111-111111111111-3"));
  std::string gid2 = to_global_id("22222222-2222-2222-2222-222222222222", {scope::POOL}, std::nullopt);
  CHECK_EQ(gid2, std::string("LAILA:POOL:GLOBAL_ID:22222222-2222-2222-2222-222222222222"));
}

TEST("identity", "invalid_resources") {
  const char* bad[] = {"", "not-a-gid", "LAILA:ENTRY:foo", "random:thing:123",
                       "LAILA:ENTRY:GLOBAL_ID:short"};
  for (const char* s : bad) {
    CHECK(!is_laila_resource(s));
    CHECK_THROWS(process_global_id(s), LailaError);
  }
}

TEST("identity", "identifiable_object") {
  for (int i = 0; i < 50; ++i) {
    Entry e;  // default identity (ENTRY scope, random uuid)
    std::string gid = e.global_id();
    CHECK(is_laila_resource(gid));
    // process_global_id mirrors laila exactly (parsed scopes retain GLOBAL_ID),
    // so we validate uuid/evolution extraction rather than a full gid identity.
    ParsedGid p = process_global_id(gid);
    CHECK_EQ(p.uuid, e.uuid());
    CHECK_EQ(p.evolution.has_value(), e.evolution().has_value());
  }
}
