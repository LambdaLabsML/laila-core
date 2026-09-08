// base64 + the recognized-snippet registry. Round-trips over many sizes, RFC
// 4648 known vectors, the base64 TransformationSequence, and the
// LAILA_UNSUPPORTED path for unknown snippets.
#include "laila_test.hpp"

using namespace laila_c;

TEST("transform", "base64_known_vectors") {
  struct V { const char* in; const char* out; };
  V vs[] = {{"", ""},   {"f", "Zg=="},     {"fo", "Zm8="},      {"foo", "Zm9v"},
            {"foob", "Zm9vYg=="}, {"fooba", "Zm9vYmE="}, {"foobar", "Zm9vYmFy"}};
  for (auto& v : vs) {
    std::string in(v.in);
    std::vector<uint8_t> bytes(in.begin(), in.end());
    CHECK_EQ(base64_encode(bytes), std::string(v.out));
    auto dec = base64_decode(v.out);
    CHECK(dec == bytes);
  }
}

TEST("transform", "base64_roundtrip_sizes") {
  for (int n = 0; n <= 300; ++n) {
    std::vector<uint8_t> b((size_t)n);
    for (int i = 0; i < n; ++i) b[i] = (uint8_t)((i * 37 + n * 11 + 5) & 0xFF);
    std::string enc = base64_encode(b);
    CHECK(base64_decode(enc) == b);
  }
}

TEST("transform", "sequence_forward_inverse") {
  TransformationSequence seq = TransformationSequence::base64();
  for (int n = 0; n <= 64; ++n) {
    std::vector<uint8_t> b((size_t)n);
    for (int i = 0; i < n; ++i) b[i] = (uint8_t)(255 - (i & 0xFF));
    LailaValue in = LailaValue::from_bytes(b);
    auto fwd = seq.forward(in);  // (base64 string, inverse codes)
    CHECK(fwd.first.kind() == LailaValue::Kind::String);
    CHECK_EQ(fwd.second.size(), (size_t)1);
    LailaValue back = apply_backward(fwd.second[0], fwd.first);
    CHECK(back.as_bytes() == b);
  }
}

TEST("transform", "registry_known") {
  // The Python recovery snippets laila-C emits are recognized by signature.
  CHECK(has_backward(codes::PY_BASE64));
  CHECK(has_backward(codes::PY_PICKLE));
  CHECK(has_backward(codes::PY_MSGPACK));
  CHECK(has_backward(codes::PY_NUMPY));
  // Recognition is by semantic signature (library+op), tolerant to wrapping.
  CHECK(has_backward("python wrapper around pickle.loads(...)"));
  CHECK(!has_backward("os.system('x')"));
}

TEST("transform", "custom_registered_snippet") {
  register_backward("test:negate_int", [](const LailaValue& v) {
    return LailaValue::from_int(-v.as_int());
  });
  CHECK(has_backward("test:negate_int"));
  CHECK_EQ(apply_backward("test:negate_int", LailaValue::from_int(7)).as_int(), -7);
}

TEST("transform", "unsupported_snippets") {
  // Snippets with no recognized library+op signature are LAILA_UNSUPPORTED.
  const char* bad[] = {"python:exec", "os.system('x')", "eval(...)", "unknown:op"};
  for (const char* code : bad)
    CHECK_THROWS(apply_backward(code, LailaValue::from_string("x")), UnsupportedError);
}
