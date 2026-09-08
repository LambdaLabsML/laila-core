// LailaValue: exhaustive round-trips through both the raw JSON payload encoding
// (as_dict path) and the binary serialize()/value_from_bytes_code path
// (transform/pool path). Covers every Kind across many sizes/values.
#include <cstring>

#include "laila_test.hpp"

using namespace laila_c;

static void roundtrip_both(const LailaValue& v) {
  // JSON payload path (Entry::as_dict / from_dict).
  CHECK(value_eq(v, LailaValue::from_json_payload(v.to_json_payload())));
  // compdata path: serialize() -> (bytes, python recovery code) -> apply_backward.
  auto pr = v.serialize();
  CHECK(value_eq(v, apply_backward(pr.second, LailaValue::from_bytes(pr.first))));
}

TEST("value", "none_and_bool") {
  roundtrip_both(LailaValue::none());
  roundtrip_both(LailaValue::from_bool(true));
  roundtrip_both(LailaValue::from_bool(false));
}

TEST("value", "ints_dense") {
  for (int i = -200; i <= 200; ++i) roundtrip_both(LailaValue::from_int(i));
}

TEST("value", "ints_wide") {
  int64_t vals[] = {0, 1, -1, 127, 128, 255, 256, 65535, 65536,
                    2147483647LL, -2147483648LL, 9223372036854775807LL,
                    -9223372036854775807LL - 1, 1000000007LL, -1000000007LL};
  for (int64_t v : vals) roundtrip_both(LailaValue::from_int(v));
}

TEST("value", "doubles") {
  double exact[] = {0.0, 0.5, -0.5, 1.25, -2.5, 100.0, 3.140625, 1e10, -1e-10};
  for (double d : exact) roundtrip_both(LailaValue::from_double(d));
  lt::Rng rng(0xD00B1Eull);
  for (int i = 0; i < 120; ++i) {
    uint64_t bits = ((uint64_t)rng.u32() << 32) | rng.u32();
    double d;
    std::memcpy(&d, &bits, 8);
    if (std::isnan(d)) continue;  // NaN != NaN by design
    auto pr = LailaValue::from_double(d).serialize();
    CHECK(value_eq(LailaValue::from_double(d), apply_backward(pr.second, LailaValue::from_bytes(pr.first))));
  }
}

TEST("value", "strings_lengths") {
  lt::Rng rng(0x57A1ull);
  for (int n = 0; n <= 130; ++n) {
    std::string s;
    for (int i = 0; i < n; ++i) s.push_back((char)('!' + (rng.u32() % 90)));
    roundtrip_both(LailaValue::from_string(s));
  }
}

TEST("value", "strings_special") {
  const char* specials[] = {"", " ", "\t", "line1\nline2", "quote\"inside",
                            "back\\slash", "ünïcödé", "emoji-ish:\xF0\x9F\x98\x80",
                            "{\"json\":true}", "a:b:c-3"};
  for (const char* s : specials) roundtrip_both(LailaValue::from_string(s));
}

TEST("value", "bytes_sizes") {
  for (int n = 0; n <= 200; ++n) {
    std::vector<uint8_t> b((size_t)n);
    for (int i = 0; i < n; ++i) b[i] = (uint8_t)((i * 31 + n * 7) & 0xFF);
    roundtrip_both(LailaValue::from_bytes(b));
  }
}

TEST("value", "bytes_edge_patterns") {
  std::vector<std::vector<uint8_t>> patterns = {
      {}, {0}, {255}, {0, 0, 0}, {255, 255, 255, 255},
      {0, 1, 2, 3, 4, 5, 6, 7}, {0xDE, 0xAD, 0xBE, 0xEF}};
  for (auto& p : patterns) roundtrip_both(LailaValue::from_bytes(p));
}

TEST("value", "json_structures") {
  lt::Rng rng(0x1234);
  for (int t = 0; t < 80; ++t) {
    Json o = Json::object();
    int fields = rng.range(0, 5);
    for (int f = 0; f < fields; ++f) {
      std::string key = "k" + std::to_string(f);
      switch (rng.range(0, 3)) {
        case 0: o[key] = (int64_t)rng.range(-1000, 1000); break;
        case 1: o[key] = std::string("v") + std::to_string(rng.u32() % 1000); break;
        case 2: {
          Json arr = Json::array();
          int n = rng.range(0, 4);
          for (int i = 0; i < n; ++i) arr.push_back(Json((int64_t)i));
          o[key] = arr;
          break;
        }
        default: o[key] = (rng.u32() % 2) == 0; break;
      }
    }
    roundtrip_both(LailaValue::from_json(o));
  }
}

TEST("value", "tensors") {
  const char* dtypes[] = {"uint8", "int32", "float32", "float64", "int8"};
  lt::Rng rng(0x7E4501ull);
  for (int t = 0; t < 60; ++t) {
    int rank = rng.range(1, 3);
    std::vector<int64_t> shape;
    size_t total = 1;
    for (int r = 0; r < rank; ++r) {
      int d = rng.range(1, 5);
      shape.push_back(d);
      total *= (size_t)d;
    }
    std::vector<uint8_t> raw(total);
    for (size_t i = 0; i < total; ++i) raw[i] = (uint8_t)(rng.u32() & 0xFF);
    roundtrip_both(LailaValue::tensor(raw, dtypes[t % 5], shape));
  }
}

// Each implicit constructor must match its from_* factory in kind, value, AND
// serialized (bytes, recovery-code) form -- so the Python-loadable wire is
// byte-identical to the already-verified explicit path.
TEST("value", "implicit_conversions") {
  auto same = [](const LailaValue& a, const LailaValue& b) {
    CHECK(value_eq(a, b));
    auto pa = a.serialize();
    auto pb = b.serialize();
    CHECK(pa.first == pb.first);
    CHECK_EQ(pa.second, pb.second);
  };
  same(LailaValue(true), LailaValue::from_bool(true));
  same(LailaValue(false), LailaValue::from_bool(false));
  same(LailaValue(42), LailaValue::from_int(42));
  same(LailaValue((int64_t)9223372036854775807LL), LailaValue::from_int(9223372036854775807LL));
  same(LailaValue(3.14), LailaValue::from_double(3.14));
  same(LailaValue("hello"), LailaValue::from_string("hello"));
  same(LailaValue(std::string("world")), LailaValue::from_string("world"));
  same(LailaValue(std::vector<uint8_t>{0, 1, 2, 254, 255}),
       LailaValue::from_bytes({0, 1, 2, 254, 255}));
  Json o = Json::object();
  o["message"] = "hi";
  same(LailaValue(o), LailaValue::from_json(o));

  // Supported types are constructible; unsupported ones fail to compile via the
  // SFINAE static_assert (e.g. laila->constant(std::vector<int>{1,2,3})).
  static_assert(std::is_constructible<LailaValue, Json>::value, "");
  static_assert(std::is_constructible<LailaValue, int>::value, "");
  static_assert(std::is_constructible<LailaValue, const char*>::value, "");
  static_assert(std::is_constructible<LailaValue, NdArray>::value, "");
}

// laila->constant(payload) with an inferred Json payload, and the numpy NdArray
// thin layer: dtype inferred from the C++ element type, byte-identical to the
// explicit LailaValue::numpy path (so it cross-loads to Python as np.ndarray).
TEST("value", "constant_payload_and_ndarray") {
  Json o = Json::object();
  o["message"] = "hello from laila";
  auto e = Entry::constant(o);
  CHECK(e->state() == EntryState::READY);
  CHECK_EQ(e->data().as_json().dump(), o.dump());

  std::vector<float> buf = {1.0f, 2.0f, 3.0f, 4.0f};
  LailaValue nv = ndarray<float>(buf, {2, 2});
  CHECK(nv.kind() == LailaValue::Kind::Tensor);
  CHECK(nv.tensor_framework() == LailaValue::TensorFramework::Numpy);
  CHECK_EQ(nv.dtype(), std::string("<f4"));
  CHECK(nv.shape() == std::vector<int64_t>({2, 2}));

  std::vector<uint8_t> raw(buf.size() * sizeof(float));
  std::memcpy(raw.data(), buf.data(), raw.size());
  LailaValue explicit_np = LailaValue::numpy(raw, "<f4", {2, 2});
  CHECK(value_eq(nv, explicit_np));
  auto p1 = nv.serialize();
  auto p2 = explicit_np.serialize();
  CHECK(p1.first == p2.first);
  CHECK_EQ(p1.second, p2.second);

  CHECK_EQ(ndarray<int32_t>({1, 2, 3}, {3}).dtype, std::string("<i4"));
  CHECK_EQ(ndarray<uint8_t>({9, 8, 7, 6}, {4}).dtype, std::string("|u1"));
  CHECK_EQ(ndarray<double>({1.0, 2.0}, {2}).dtype, std::string("<f8"));

  // Round-trips through the compdata (npy) recovery path.
  auto pr = nv.serialize();
  CHECK(value_eq(nv, apply_backward(pr.second, LailaValue::from_bytes(pr.first))));
}
