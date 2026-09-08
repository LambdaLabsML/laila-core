// Constitution: SimpleConstitution chains (recognized snippets), the
// ComplexConstitution builder registry, manifest-driven builds, from_dict
// dispatch, and unsupported tokens.
#include "laila_test.hpp"

using namespace laila_c;

TEST("constitution", "simple_chain_identity") {
  // codes run in order; a value backward-code recovers the payload.
  for (int i = -50; i <= 50; ++i) {
    auto v = LailaValue::from_int(i);
    auto pr = v.serialize();  // (bytes, code)
    SimpleConstitution sc({pr.second});
    LailaValue out = sc.build(LailaValue::from_bytes(pr.first));
    CHECK(value_eq(out, v));
  }
}

TEST("constitution", "simple_chain_base64_then_value") {
  TransformationSequence t = TransformationSequence::base64();
  for (int n = 0; n <= 40; ++n) {
    std::vector<uint8_t> b((size_t)n);
    for (int i = 0; i < n; ++i) b[i] = (uint8_t)(i * 9 + 1);
    auto v = LailaValue::from_bytes(b);
    auto serialized = v.serialize();
    auto fwd = t.forward(LailaValue::from_bytes(serialized.first));
    std::vector<std::string> codes = fwd.second;
    codes.push_back(serialized.second);
    SimpleConstitution sc(codes);
    LailaValue out = sc.build(fwd.first);
    CHECK(value_eq(out, v));
  }
}

TEST("constitution", "simple_as_dict_from_dict") {
  SimpleConstitution sc({codes::PY_BASE64, codes::PY_PICKLE});
  Json d = sc.as_dict();
  CHECK_EQ(d.at("_kind").as_string(), std::string("simple"));
  auto back = Constitution::from_dict(d);
  CHECK(back != nullptr);
  CHECK_EQ(std::string(back->kind()), std::string("simple"));
}

TEST("constitution", "complex_builder_registry") {
  register_builder("c_sum", [](const Manifest& m) {
    return LailaValue::from_int(m.get("a")->data().as_int() + m.get("b")->data().as_int());
  });
  register_builder("c_concat", [](const Manifest& m) {
    return LailaValue::from_string(m.get("a")->data().as_string() + m.get("b")->data().as_string());
  });
  CHECK(has_builder("c_sum"));
  CHECK(has_builder("c_concat"));
  CHECK(!has_builder("c_missing"));

  for (int i = 0; i < 50; ++i) {
    Manifest m;
    m.put("a", Entry::constant(LailaValue::from_int(i)));
    m.put("b", Entry::constant(LailaValue::from_int(i * 2)));
    ComplexConstitution c("c_sum");
    CHECK_EQ(c.build_with(m).as_int(), (int64_t)(i + i * 2));
  }

  Manifest ms;
  ms.put("a", Entry::constant(LailaValue::from_string("foo")));
  ms.put("b", Entry::constant(LailaValue::from_string("bar")));
  ComplexConstitution cc("c_concat");
  CHECK_EQ(cc.build_with(ms).as_string(), std::string("foobar"));
}

TEST("constitution", "complex_requires_manifest") {
  ComplexConstitution c("c_sum");
  CHECK_THROWS(c.build(LailaValue::none()), LailaError);
  CHECK_THROWS(c.build_with(LailaValue::none(), nullptr), LailaError);
}

TEST("constitution", "complex_unknown_token") {
  ComplexConstitution c("definitely_not_registered");
  Manifest m;
  CHECK_THROWS(c.build_with(m), UnsupportedError);
}

TEST("constitution", "complex_as_dict_from_dict") {
  ComplexConstitution c("c_sum");
  Json d = c.as_dict();
  CHECK_EQ(d.at("_kind").as_string(), std::string("complex"));
  CHECK_EQ(d.at("class_token").as_string(), std::string("c_sum"));
  auto back = Constitution::from_dict(d);
  CHECK_EQ(std::string(back->kind()), std::string("complex"));
}

TEST("constitution", "from_dict_null_and_bad") {
  CHECK(Constitution::from_dict(Json(nullptr)) == nullptr);
  Json missing = Json::object();
  CHECK_THROWS(Constitution::from_dict(missing), LailaError);
  Json badkind = Json::object();
  badkind["_kind"] = std::string("nonsense");
  CHECK_THROWS(Constitution::from_dict(badkind), LailaError);
}
