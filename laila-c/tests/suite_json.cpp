// JSON encoder/parser: idempotent dump(parse(dump(x))) round-trips across
// primitives, nesting, and escaping; plus targeted malformed-input checks.
#include "laila_test.hpp"

using namespace laila_c;

static void idem(const Json& j) {
  std::string a = j.dump();
  std::string b = Json::parse(a).dump();
  CHECK_EQ(a, b);
}

TEST("json", "primitives") {
  idem(Json(nullptr));
  idem(Json(true));
  idem(Json(false));
  for (int i = -300; i <= 300; ++i) idem(Json((int64_t)i));
  const char* strs[] = {"", "a", "hello world", "with \"quotes\"", "tab\tnl\n", "a:b:c"};
  for (const char* s : strs) idem(Json(std::string(s)));
}

TEST("json", "nested_random") {
  lt::Rng rng(0x550Eull);
  for (int t = 0; t < 150; ++t) {
    Json root = (rng.u32() % 2) ? Json::object() : Json::array();
    int n = rng.range(0, 6);
    for (int i = 0; i < n; ++i) {
      Json v;
      switch (rng.range(0, 4)) {
        case 0: v = (int64_t)rng.range(-99999, 99999); break;
        case 1: v = std::string("s") + std::to_string(rng.u32() % 10000); break;
        case 2: v = (rng.u32() % 2) == 0; break;
        case 3: v = Json(nullptr); break;
        default: {
          v = Json::array();
          int m = rng.range(0, 3);
          for (int k = 0; k < m; ++k) v.push_back(Json((int64_t)k));
        }
      }
      if (root.is_object()) root["f" + std::to_string(i)] = v;
      else root.push_back(v);
    }
    idem(root);
  }
}

TEST("json", "deep_nesting") {
  for (int depth = 1; depth <= 40; ++depth) {
    Json j = Json::object();
    j["leaf"] = (int64_t)depth;
    for (int d = 0; d < depth; ++d) {
      Json wrap = Json::object();
      wrap["child"] = j;
      j = wrap;
    }
    idem(j);
  }
}

TEST("json", "escaping_roundtrip") {
  const char* hard[] = {"\"", "\\", "\n", "\r", "\t", "\b", "\f", "\x01\x02\x1f",
                        "mix\"\\\n\t end", "control\x07here"};
  for (const char* s : hard) {
    Json j = Json(std::string(s));
    CHECK_EQ(Json::parse(j.dump()).as_string(), std::string(s));
  }
}

TEST("json", "object_access") {
  Json o = Json::object();
  o["a"] = (int64_t)1;
  o["b"] = std::string("two");
  CHECK(o.contains("a"));
  CHECK(!o.contains("zzz"));
  CHECK_EQ(o.at("a").as_int(), 1);
  CHECK_EQ(o.at("b").as_string(), std::string("two"));
  CHECK(o.at("missing").is_null());  // sentinel
}

TEST("json", "malformed_inputs") {
  const char* bad[] = {"{", "[", "{\"a\":}", "[1,]", "\"unterminated", "tru", "{\"a\" 1}"};
  for (const char* s : bad) CHECK_THROWS(Json::parse(s), LailaError);
}
