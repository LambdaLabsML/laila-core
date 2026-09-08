// Cross-language conformance: verify laila-C against REAL vectors produced by
// the Python `laila` library (tests/gen_vectors.py). Confirms byte-level UUID5
// parity and that laila-C reads laila's Entry.as_dict wire format. Host-only
// (reads fixture files); regenerate vectors with:
//   PYTHONPATH=/path python3 tests/gen_vectors.py
#include "laila_test.hpp"

#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#define LAILA_TEST_HAS_FS 1
#include <fstream>
#include <sstream>
#endif

using namespace laila_c;

#if defined(LAILA_TEST_HAS_FS)
#ifndef LAILA_VECTORS_DIR
#define LAILA_VECTORS_DIR "vectors"
#endif

static bool read_file(const std::string& path, std::string& out) {
  std::ifstream f(path, std::ios::binary);
  if (!f) return false;
  std::ostringstream ss;
  ss << f.rdbuf();
  out = ss.str();
  return true;
}

static std::string vec_path(const char* name) {
  return std::string(LAILA_VECTORS_DIR) + "/" + name;
}

TEST("xlang", "uuid5_parity") {
  std::string text;
  if (!read_file(vec_path("uuid5.json"), text)) return;  // vectors absent: skip
  Json doc = Json::parse(text);
  // laila-C's default namespace must match laila's universal namespace.
  CHECK_EQ(get_active_namespace_uuid(), doc.at("namespace").as_string());
  for (const auto& c : doc.at("cases").elements()) {
    std::string nick = c.at("nickname").as_string();
    // Byte-identical nickname -> UUID5 with laila.
    CHECK_EQ(generate_uuid_from_nickname(nick), c.at("uuid").as_string());
    // And the composed global_id matches laila.constant(nickname=...).global_id.
    std::string gid = to_global_id(generate_uuid_from_nickname(nick), {scope::ENTRY}, std::nullopt);
    CHECK_EQ(gid, c.at("global_id").as_string());
  }
}

TEST("xlang", "as_dict_wire_format") {
  std::string text;
  if (!read_file(vec_path("entries.json"), text)) return;  // skip if absent
  Json arr = Json::parse(text);
  for (const auto& v : arr.elements()) {
    std::string name = v.at("name").as_string();
    // Rebuild a laila-produced Entry dict in laila-C and validate identity + value.
    EntryPtr e = Entry::build_from_dict(v.at("as_dict"));
    CHECK_EQ(e->uuid(), v.at("uuid").as_string());
    CHECK_EQ(e->global_id(), v.at("global_id").as_string());

    const Json& payload = v.at("as_dict").at("payload");
    LailaValue got = e->data();
    switch (payload.type()) {
      case Json::Type::String:
        CHECK(got.kind() == LailaValue::Kind::String);
        CHECK_EQ(got.as_string(), payload.as_string());
        break;
      case Json::Type::Int:
        CHECK(got.kind() == LailaValue::Kind::Int);
        CHECK_EQ(got.as_int(), payload.as_int());
        break;
      case Json::Type::Double:
        CHECK(got.kind() == LailaValue::Kind::Double);
        CHECK_NEAR(got.as_double(), payload.as_double(), 1e-9);
        break;
      case Json::Type::Bool:
        CHECK(got.kind() == LailaValue::Kind::Bool);
        CHECK_EQ(got.as_bool(), payload.as_bool());
        break;
      case Json::Type::Object:
      case Json::Type::Array:
        // dict/list payloads come back as structured JSON, value-identical.
        CHECK(got.kind() == LailaValue::Kind::Json);
        CHECK_EQ(got.as_json().dump(), payload.dump());
        break;
      case Json::Type::Null:
        CHECK(got.is_none());
        break;
    }
  }
}

TEST("xlang", "roundtrip_back_to_laila_shape") {
  // laila-C's own as_dict for primitive kinds must stay parseable and produce
  // the same logical value (kinds laila-C tags, but values must round-trip).
  std::string text;
  if (!read_file(vec_path("entries.json"), text)) return;
  Json arr = Json::parse(text);
  for (const auto& v : arr.elements()) {
    EntryPtr e = Entry::build_from_dict(v.at("as_dict"));
    EntryPtr again = Entry::build_from_dict(e->as_dict());  // laila-C tagged round-trip
    CHECK(value_eq(e->data(), again->data()));
  }
}
#endif  // LAILA_TEST_HAS_FS
