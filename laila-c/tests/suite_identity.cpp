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
  CHECK_EQ(gid, std::string("LAILA:ENTRY:11111111-1111-1111-1111-111111111111@evolution=3"));
  std::string gid2 = to_global_id("22222222-2222-2222-2222-222222222222", {scope::POOL}, std::nullopt);
  CHECK_EQ(gid2, std::string("LAILA:POOL:22222222-2222-2222-2222-222222222222"));
}

TEST("identity", "invalid_resources") {
  const char* bad[] = {"", "not-a-gid", "LAILA:ENTRY:foo", "random:thing:123",
                       "LAILA:ENTRY:short",
                       // legacy `-<evolution>` suffix and `GLOBAL_ID:` framing
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111-3",
                       // malformed attribute lists
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@",
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@evolution",
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@=3",
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@evolution=3,",
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@evolution=1,evolution=2",
                       "LAILA:ENTRY:11111111-1111-1111-1111-111111111111@1abc=2"};
  for (const char* s : bad) {
    CHECK(!is_laila_resource(s));
    CHECK_THROWS(process_global_id(s), LailaError);
  }
  // Well-formed attributes but a non-numeric evolution: resource-shaped, but
  // not a valid identity.
  CHECK_THROWS(process_global_id("LAILA:ENTRY:11111111-1111-1111-1111-111111111111@evolution=x"),
               LailaError);
}

TEST("identity", "attributes_search_arguments") {
  const std::string uid = "11111111-1111-1111-1111-111111111111";
  const std::string hb = "2026-09-15T17:50:00.123+00:00";
  const std::string gid = "LAILA:ENTRY:" + uid + "@evolution=2,creation_timestamp=" + hb;
  CHECK(is_laila_resource(gid));
  // Only `evolution` is identity; other keys are ignored by process_global_id.
  ParsedGid p = process_global_id(gid);
  CHECK_EQ(p.uuid, uid);
  CHECK_EQ(*p.evolution, (int64_t)2);
  CHECK_EQ(to_global_id(p.uuid, p.scopes, p.evolution), std::string("LAILA:ENTRY:" + uid + "@evolution=2"));
  // ... but every attribute is exposed, in order, as raw strings.
  GidAttributes attrs = get_attributes_from_global_id(gid);
  CHECK_EQ(attrs.size(), (size_t)2);
  CHECK_EQ(attrs[0].first, std::string("evolution"));
  CHECK_EQ(attrs[0].second, std::string("2"));
  CHECK_EQ(attrs[1].first, std::string("creation_timestamp"));
  CHECK_EQ(attrs[1].second, hb);
  CHECK_EQ(format_global_id_attributes(attrs), std::string("evolution=2,creation_timestamp=" + hb));
  CHECK_EQ(strip_global_id_attributes(gid), std::string("LAILA:ENTRY:" + uid));
  // A creation_timestamp-only suffix is a constant identity.
  CHECK(!process_global_id("LAILA:ENTRY:" + uid + "@creation_timestamp=" + hb).evolution.has_value());
  auto split = split_global_id_attributes("ENTRY:nick@evolution=3");
  CHECK_EQ(split.first, std::string("ENTRY:nick"));
  CHECK_EQ(split.second.size(), (size_t)1);
  CHECK(split_global_id_attributes("ENTRY:nick").second.empty());
  CHECK_THROWS(parse_global_id_attributes("a=1,a=2"), LailaError);
  CHECK_THROWS(parse_global_id_attributes("=1"), LailaError);
}

TEST("identity", "creation_timestamp_shape") {
  // ISO-8601 UTC with millisecond precision: 2026-09-15T17:50:00.123+00:00
  std::string ts = now_creation_timestamp();
  CHECK_EQ(ts.size(), (size_t)29);
  CHECK_EQ(ts[4], '-');
  CHECK_EQ(ts[7], '-');
  CHECK_EQ(ts[10], 'T');
  CHECK_EQ(ts[13], ':');
  CHECK_EQ(ts[16], ':');
  CHECK_EQ(ts[19], '.');
  CHECK_EQ(ts.substr(23), std::string("+00:00"));
  Entry e;
  CHECK_EQ(e.creation_timestamp().size(), (size_t)29);
  CHECK(e.creation_timestamp() >= ts);  // stamped at construction, monotone
}

TEST("identity", "identifiable_object") {
  for (int i = 0; i < 50; ++i) {
    Entry e;  // default identity (ENTRY scope, random uuid)
    std::string gid = e.global_id();
    CHECK(is_laila_resource(gid));
    ParsedGid p = process_global_id(gid);
    CHECK_EQ(p.uuid, e.uuid());
    CHECK_EQ(p.evolution.has_value(), e.evolution().has_value());
    // Parsed scopes are the middle segments only, so the gid round-trips.
    CHECK_EQ(p.scopes, e.scopes());
    CHECK_EQ(to_global_id(p.uuid, p.scopes, p.evolution), gid);
  }
}

TEST("identity", "process_global_id_round_trip_scopes") {
  std::string gid = "LAILA:A:B:11111111-1111-1111-1111-111111111111@evolution=3";
  ParsedGid p = process_global_id(gid);
  CHECK_EQ(p.scopes, (std::vector<std::string>{"A", "B"}));
  CHECK_EQ(to_global_id(p.uuid, p.scopes, p.evolution), gid);
}
