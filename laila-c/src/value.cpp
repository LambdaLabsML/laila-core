#include "laila/value.hpp"

#include <cstring>

#include "laila/compdata.hpp"
#include "laila/computational_data.hpp"
#include "laila/status.hpp"
#include "laila/transformation.hpp"

namespace laila_c {

// Implicit converters (mirror the from_* factories; same Kind + fields, so the
// serialized/wire form is byte-identical to the explicit path).
LailaValue::LailaValue(bool b) : kind_(Kind::Bool), b_(b) {}
LailaValue::LailaValue(int i) : kind_(Kind::Int), i_(i) {}
LailaValue::LailaValue(int64_t i) : kind_(Kind::Int), i_(i) {}
LailaValue::LailaValue(double d) : kind_(Kind::Double), d_(d) {}
LailaValue::LailaValue(const char* s) : kind_(Kind::String), s_(s) {}
LailaValue::LailaValue(const std::string& s) : kind_(Kind::String), s_(s) {}
LailaValue::LailaValue(std::vector<uint8_t> b) : kind_(Kind::Bytes), bytes_(std::move(b)) {}
LailaValue::LailaValue(Json j) : kind_(Kind::Json), json_(std::move(j)) {}
LailaValue::LailaValue(const NdArray& a) { *this = numpy(a.raw, a.dtype, a.shape); }

LailaValue LailaValue::from_bool(bool b) { LailaValue v; v.kind_ = Kind::Bool; v.b_ = b; return v; }
LailaValue LailaValue::from_int(int64_t i) { LailaValue v; v.kind_ = Kind::Int; v.i_ = i; return v; }
LailaValue LailaValue::from_double(double d) { LailaValue v; v.kind_ = Kind::Double; v.d_ = d; return v; }
LailaValue LailaValue::from_string(const std::string& s) { LailaValue v; v.kind_ = Kind::String; v.s_ = s; return v; }
LailaValue LailaValue::from_bytes(std::vector<uint8_t> b) { LailaValue v; v.kind_ = Kind::Bytes; v.bytes_ = std::move(b); return v; }
LailaValue LailaValue::from_json(Json j) { LailaValue v; v.kind_ = Kind::Json; v.json_ = std::move(j); return v; }
LailaValue LailaValue::numpy(std::vector<uint8_t> raw, const std::string& dtype, std::vector<int64_t> shape) {
  LailaValue v; v.kind_ = Kind::Tensor; v.tensor_framework_ = TensorFramework::Numpy;
  v.bytes_ = std::move(raw); v.dtype_ = dtype; v.shape_ = std::move(shape); return v;
}
LailaValue LailaValue::torch_tensor(std::vector<uint8_t> raw, const std::string& dtype, std::vector<int64_t> shape) {
  LailaValue v; v.kind_ = Kind::Tensor; v.tensor_framework_ = TensorFramework::Torch;
  v.bytes_ = std::move(raw); v.dtype_ = dtype; v.shape_ = std::move(shape); return v;
}
LailaValue LailaValue::tensor(std::vector<uint8_t> raw, const std::string& dtype, std::vector<int64_t> shape) {
  return numpy(std::move(raw), dtype, std::move(shape));
}

std::pair<std::vector<uint8_t>, std::string> LailaValue::serialize() const {
  // Mirror of Python: an Entry payload is wrapped in a ComputationalData (the
  // matching taxonomy subclass) whose serialize() picks the right serializer.
  return ComputationalData::wrap(*this)->serialize();
}

// laila-C's own tagged payload uses a collision-proof sentinel key so it never
// clashes with a user dict that happens to contain "k"/"v". laila's native
// as_dict embeds RAW (untagged) values, handled in the fallback below.
static const char* kTagKey = "__lv__";

Json LailaValue::to_json_payload() const {
  Json o = Json::object();
  switch (kind_) {
    case Kind::None: o[kTagKey] = std::string("none"); break;
    case Kind::Bool: o[kTagKey] = std::string("bool"); o["v"] = b_; break;
    case Kind::Int: o[kTagKey] = std::string("int"); o["v"] = i_; break;
    case Kind::Double: o[kTagKey] = std::string("double"); o["v"] = d_; break;
    case Kind::String: o[kTagKey] = std::string("string"); o["v"] = s_; break;
    case Kind::Bytes: o[kTagKey] = std::string("bytes"); o["v"] = base64_encode(bytes_); break;
    case Kind::Json: o[kTagKey] = std::string("json"); o["v"] = json_; break;
    case Kind::Tensor: {
      o[kTagKey] = std::string("tensor");
      o["dtype"] = dtype_;
      Json shape = Json::array();
      for (int64_t d : shape_) shape.push_back(Json((int64_t)d));
      o["shape"] = shape;
      o["v"] = base64_encode(bytes_);
      break;
    }
  }
  return o;
}

Json LailaValue::to_wire_payload() const {
  // Mirrors laila's Entry.as_dict payload: the bare value, untagged.
  switch (kind_) {
    case Kind::None: return Json(nullptr);
    case Kind::Bool: return Json(b_);
    case Kind::Int: return Json(i_);
    case Kind::Double: return Json(d_);
    case Kind::String: return Json(s_);
    case Kind::Json: return json_;
    case Kind::Bytes:
    case Kind::Tensor:
      // Not representable as a bare JSON value cross-language; keep tagged so a
      // laila-C peer can still round-trip it.
      return to_json_payload();
  }
  return Json(nullptr);
}

LailaValue LailaValue::from_json_payload(const Json& node) {
  if (node.is_null()) return LailaValue::none();
  if (!node.is_object() || !node.contains(kTagKey)) {
    // Untagged payload (this is how laila's Entry.as_dict embeds values).
    // Map by JSON type to the matching LailaValue kind for cross-language reads.
    switch (node.type()) {
      case Json::Type::Bool: return LailaValue::from_bool(node.as_bool());
      case Json::Type::Int: return LailaValue::from_int(node.as_int());
      case Json::Type::Double: return LailaValue::from_double(node.as_double());
      case Json::Type::String: return LailaValue::from_string(node.as_string());
      case Json::Type::Bytes: return LailaValue::from_bytes(node.as_bytes());
      case Json::Type::Array:
      case Json::Type::Object: return LailaValue::from_json(node);
      case Json::Type::Null: return LailaValue::none();
    }
    return LailaValue::from_json(node);
  }
  const std::string& k = node.at(kTagKey).as_string();
  const Json& v = node.at("v");
  if (k == "none") return LailaValue::none();
  if (k == "bool") return LailaValue::from_bool(v.as_bool());
  if (k == "int") return LailaValue::from_int(v.as_int());
  if (k == "double") return LailaValue::from_double(v.as_double());
  if (k == "string") return LailaValue::from_string(v.as_string());
  if (k == "bytes") return LailaValue::from_bytes(base64_decode(v.as_string()));
  if (k == "json") return LailaValue::from_json(v);
  if (k == "tensor") {
    std::vector<int64_t> shape;
    for (const auto& e : node.at("shape").elements()) shape.push_back(e.as_int());
    return LailaValue::tensor(base64_decode(v.as_string()), node.at("dtype").as_string(), shape);
  }
  raise(Status::Error, "unknown payload kind: " + k);
}

}  // namespace laila_c
