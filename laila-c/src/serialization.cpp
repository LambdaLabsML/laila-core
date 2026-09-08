#include "laila/serialization.hpp"

#include "laila/compdata.hpp"
#include "laila/status.hpp"

namespace laila_c {

// ---------------- PickleSerializer ----------------
const std::string& PickleSerializer::name() const { static std::string n = "pickle"; return n; }

LailaValue PickleSerializer::forward(const LailaValue& inp) const {
  // Mirror ComputationalData's scalar dispatch: None/bool/int/float/str/bytes
  // pickle to the bare CPython value.
  Json node;
  switch (inp.kind()) {
    case LailaValue::Kind::Bool: node = Json(inp.as_bool()); break;
    case LailaValue::Kind::Int: node = Json(inp.as_int()); break;
    case LailaValue::Kind::Double: node = Json(inp.as_double()); break;
    case LailaValue::Kind::String: node = Json(inp.as_string()); break;
    case LailaValue::Kind::Bytes: node = Json::bytes(inp.as_bytes()); break;
    case LailaValue::Kind::Json: node = inp.as_json(); break;
    default: node = Json(nullptr); break;  // None
  }
  return LailaValue::from_bytes(cd::pickle_encode(node));
}

LailaValue PickleSerializer::backward(const LailaValue& inp) const {
  return LailaValue::from_json_payload(cd::pickle_decode(inp.as_bytes()));
}

std::string PickleSerializer::backward_code() const { return codes::PY_PICKLE; }

// ---------------- MsgpackSerializer ----------------
const std::string& MsgpackSerializer::name() const { static std::string n = "msgpack"; return n; }

LailaValue MsgpackSerializer::forward(const LailaValue& inp) const {
  return LailaValue::from_bytes(cd::msgpack_encode(inp.as_json()));
}

LailaValue MsgpackSerializer::backward(const LailaValue& inp) const {
  return LailaValue::from_json_payload(cd::msgpack_decode(inp.as_bytes()));
}

std::string MsgpackSerializer::backward_code() const { return codes::PY_MSGPACK; }

// ---------------- NumpySerializer ----------------
const std::string& NumpySerializer::name() const { static std::string n = "numpy"; return n; }

LailaValue NumpySerializer::forward(const LailaValue& inp) const {
  cd::NpyArray a;
  a.raw = inp.as_bytes(); a.dtype = inp.dtype(); a.shape = inp.shape(); a.fortran_order = false;
  return LailaValue::from_bytes(cd::npy_encode(a));
}

LailaValue NumpySerializer::backward(const LailaValue& inp) const {
  cd::NpyArray a = cd::npy_decode(inp.as_bytes());
  return LailaValue::numpy(a.raw, a.dtype, a.shape);
}

std::string NumpySerializer::backward_code() const { return codes::PY_NUMPY; }

// ---------------- TorchSerializer ----------------
const std::string& TorchSerializer::name() const { static std::string n = "torch"; return n; }

LailaValue TorchSerializer::forward(const LailaValue&) const {
  raise(Status::Unsupported, "torch.save serialization is not supported on this target");
  return LailaValue::none();  // unreachable when exceptions are enabled
}

LailaValue TorchSerializer::backward(const LailaValue&) const {
  raise(Status::Unsupported, "torch.load deserialization is not supported on this target");
  return LailaValue::none();
}

std::string TorchSerializer::backward_code() const { return codes::PY_TORCH; }

}  // namespace laila_c
