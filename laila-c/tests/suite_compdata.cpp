// compdata cross-language parity: build entries from REAL Python
// `entry.serialize(transformation_base64)` output (tests/vectors/compdata.json)
// and verify value parity. For the deterministic codecs (msgpack/npy) also
// verify byte-identity by re-encoding in laila-C and comparing to Python's
// payload bytes. Plus a pure-C round-trip over every type. Host-only (reads
// fixtures); regenerate with:
//   PYTHONPATH=/path python3 tests/gen_vectors.py
#include "laila_test.hpp"
#include "laila/compdata.hpp"
#include "laila/computational_data.hpp"
#include "laila/serialization.hpp"
#include "laila/status.hpp"
#include "laila/transformation.hpp"

#if defined(__linux__) || defined(__APPLE__) || defined(__unix__)
#define LAILA_COMPDATA_HAS_FS 1
#include <fstream>
#include <sstream>
#endif

using namespace laila_c;

#if defined(LAILA_COMPDATA_HAS_FS)
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

// Python's payload field is base64(serializer_bytes); decode it for byte-identity.
static std::vector<uint8_t> payload_bytes(const Json& ser) {
  return base64_decode(ser.at("payload").as_string());
}

TEST("compdata", "python_vector_parity") {
  std::string text;
  if (!read_file(std::string(LAILA_VECTORS_DIR) + "/compdata.json", text)) return;  // absent: skip
  Json arr = Json::parse(text);
  for (const auto& v : arr.elements()) {
    std::string name = v.at("name").as_string();
    std::string check = v.at("check").as_string();
    const Json& ser = v.at("serialized");

    if (check == "bignum_unsupported") {
      CHECK_THROWS(Entry::build_from_dict(ser), LailaError);  // >int64 pickle LONG
      continue;
    }

    EntryPtr e = Entry::build_from_dict(ser);
    LailaValue got = e->data();

    if (check == "none") {
      CHECK(got.is_none());
    } else if (check == "value") {
      LailaValue exp = LailaValue::from_json_payload(v.at("value"));
      CHECK(value_eq(got, exp));
      if (v.contains("mp_identity") && v.at("mp_identity").as_bool()) {
        // byte-identical msgpack: re-encode laila-C's value, compare to Python.
        CHECK(cd::msgpack_encode(got.as_json()) == payload_bytes(ser));
      }
    } else if (check == "bytes") {
      CHECK(got.kind() == LailaValue::Kind::Bytes);
      CHECK(got.as_bytes() == base64_decode(v.at("b64").as_string()));
    } else if (check == "nested_bytes") {
      CHECK(got.kind() == LailaValue::Kind::Json);
      const Json& node = got.as_json().at(v.at("key").as_string());
      CHECK(node.is_bytes());
      CHECK(node.as_bytes() == base64_decode(v.at("b64").as_string()));
    } else if (check == "numpy") {
      CHECK(got.kind() == LailaValue::Kind::Tensor);
      CHECK_EQ(got.dtype(), v.at("dtype").as_string());
      CHECK(got.as_bytes() == base64_decode(v.at("raw_b64").as_string()));
      std::vector<int64_t> shape;
      for (const auto& d : v.at("shape").elements()) shape.push_back(d.as_int());
      CHECK(got.shape() == shape);
      // byte-identical .npy re-encode.
      cd::NpyArray a;
      a.raw = got.as_bytes(); a.dtype = got.dtype(); a.shape = got.shape();
      CHECK(cd::npy_encode(a) == payload_bytes(ser));
    }
  }
}
#endif  // LAILA_COMPDATA_HAS_FS

// Pure-C round-trip for every supported kind (no Python needed): serialize() ->
// apply the emitted recovery code -> value-equal.
TEST("compdata", "c_roundtrip_all_kinds") {
  auto rt = [](const LailaValue& v) {
    auto pr = v.serialize();
    LailaValue back = apply_backward(pr.second, LailaValue::from_bytes(pr.first));
    CHECK(value_eq(v, back));
  };
  rt(LailaValue::from_bool(true));
  rt(LailaValue::from_int(-42));
  rt(LailaValue::from_double(2.5));
  rt(LailaValue::from_string("round trip"));
  rt(LailaValue::from_bytes({0, 1, 2, 250, 255}));
  Json o = Json::object();
  o["z"] = (int64_t)1; o["a"] = std::string("x"); o["m"] = Json::bytes({9, 8, 7});
  rt(LailaValue::from_json(o));
  // dict key order is preserved through msgpack.
  auto pr = LailaValue::from_json(o).serialize();
  LailaValue back = apply_backward(pr.second, LailaValue::from_bytes(pr.first));
  const auto& items = back.as_json().items();
  CHECK_EQ(items.size(), (size_t)3);
  CHECK_EQ(items[0].first, std::string("z"));
  CHECK_EQ(items[1].first, std::string("a"));
  CHECK_EQ(items[2].first, std::string("m"));
}

// ComputationalData.wrap dispatches to the taxonomy subclass matching the
// payload (register_cdtype mirror), and each subclass's serialize() yields the
// same bytes + recovery code as the byte codecs / serializer classes.
TEST("compdata", "taxonomy_dispatch") {
  // numpy ndarray -> CD_numpyarray -> NumpySerializer (.npy + PY_NUMPY)
  std::vector<uint8_t> raw = {1, 2, 3, 4, 5, 6, 7, 8};
  LailaValue np = LailaValue::numpy(raw, "<i4", {2});
  auto ser_np = ComputationalData::wrap(np)->serialize();
  cd::NpyArray a; a.raw = raw; a.dtype = "<i4"; a.shape = {2}; a.fortran_order = false;
  CHECK(ser_np.first == cd::npy_encode(a));
  CHECK_EQ(ser_np.second, std::string(codes::PY_NUMPY));

  // dict -> CD_dict -> MsgpackSerializer (PY_MSGPACK)
  Json o = Json::object(); o["k"] = (int64_t)5;
  auto ser_d = ComputationalData::wrap(LailaValue::from_json(o))->serialize();
  CHECK(ser_d.first == cd::msgpack_encode(o));
  CHECK_EQ(ser_d.second, std::string(codes::PY_MSGPACK));

  // scalar -> CD_generic -> PickleSerializer (PY_PICKLE), value round-trips
  LailaValue s = LailaValue::from_int(-7);
  auto ser_s = ComputationalData::wrap(s)->serialize();
  CHECK_EQ(ser_s.second, std::string(codes::PY_PICKLE));
  CHECK(value_eq(s, apply_backward(ser_s.second, LailaValue::from_bytes(ser_s.first))));
}

// torch.Tensor -> CD_torchtensor -> TorchSerializer, which has no native
// torch.save on this target and raises LAILA_UNSUPPORTED (the hard-error rule).
TEST("compdata", "torch_unsupported") {
  LailaValue t = LailaValue::torch_tensor({0, 0, 0, 0}, "<f4", {1});
  CHECK_THROWS(ComputationalData::wrap(t)->serialize(), UnsupportedError);
  CHECK_THROWS(t.serialize(), UnsupportedError);
  TorchSerializer ts;
  CHECK_THROWS(ts.forward(t), UnsupportedError);
  CHECK_THROWS(ts.backward(LailaValue::from_bytes({1, 2})), UnsupportedError);
}

// The serializer classes (mirrors of transformation/serialization/*.py) are
// reversible: forward() -> bytes, backward() -> value-equal payload.
TEST("compdata", "serializer_classes_roundtrip") {
  NumpySerializer ns;
  LailaValue np = LailaValue::numpy({9, 8, 7, 6}, "|u1", {4});
  LailaValue np_bytes = ns.forward(np);
  CHECK(np_bytes.kind() == LailaValue::Kind::Bytes);
  LailaValue np_back = ns.backward(np_bytes);
  CHECK(value_eq(np, np_back));
  CHECK(np_back.tensor_framework() == LailaValue::TensorFramework::Numpy);

  MsgpackSerializer ms;
  Json arr = Json::array(); arr.push_back(Json((int64_t)1)); arr.push_back(Json((int64_t)2));
  LailaValue lst = LailaValue::from_json(arr);
  CHECK(value_eq(lst, ms.backward(ms.forward(lst))));

  PickleSerializer ps;
  LailaValue sv = LailaValue::from_string("pickle me");
  CHECK(value_eq(sv, ps.backward(ps.forward(sv))));
}
