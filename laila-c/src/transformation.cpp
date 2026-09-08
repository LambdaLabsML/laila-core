#include "laila/transformation.hpp"

#include <algorithm>
#include <map>
#include <memory>

#include "laila/compdata.hpp"
#include "laila/status.hpp"

namespace laila_c {

// ---------------- base64 ----------------
static const char kB64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64_encode(const std::vector<uint8_t>& data) {
  std::string out;
  size_t i = 0;
  while (i + 2 < data.size()) {
    uint32_t n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
    out.push_back(kB64[(n >> 18) & 63]);
    out.push_back(kB64[(n >> 12) & 63]);
    out.push_back(kB64[(n >> 6) & 63]);
    out.push_back(kB64[n & 63]);
    i += 3;
  }
  size_t rem = data.size() - i;
  if (rem == 1) {
    uint32_t n = data[i] << 16;
    out.push_back(kB64[(n >> 18) & 63]);
    out.push_back(kB64[(n >> 12) & 63]);
    out += "==";
  } else if (rem == 2) {
    uint32_t n = (data[i] << 16) | (data[i + 1] << 8);
    out.push_back(kB64[(n >> 18) & 63]);
    out.push_back(kB64[(n >> 12) & 63]);
    out.push_back(kB64[(n >> 6) & 63]);
    out.push_back('=');
  }
  return out;
}

std::vector<uint8_t> base64_decode(const std::string& text) {
  auto val = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
  };
  std::vector<uint8_t> out;
  int buf = 0, bits = 0;
  for (char c : text) {
    if (c == '=' || c == '\n' || c == '\r') continue;
    int v = val(c);
    if (v < 0) continue;
    buf = (buf << 6) | v;
    bits += 6;
    if (bits >= 8) { bits -= 8; out.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF)); }
  }
  return out;
}

// ---------------- Base64 transformation ----------------
namespace {
class Base64Transform : public Transformation {
public:
  const std::string& name() const override { static std::string n = "base64"; return n; }
  LailaValue forward(const LailaValue& in) const override {
    std::vector<uint8_t> bytes;
    if (in.kind() == LailaValue::Kind::Bytes) bytes = in.as_bytes();
    else if (in.kind() == LailaValue::Kind::String) bytes.assign(in.as_string().begin(), in.as_string().end());
    else { auto p = in.serialize(); bytes = p.first; }
    return LailaValue::from_string(base64_encode(bytes));
  }
  LailaValue backward(const LailaValue& in) const override {
    return LailaValue::from_bytes(base64_decode(in.as_string()));
  }
  std::string backward_code() const override { return codes::PY_BASE64; }
};
}  // namespace

std::pair<LailaValue, std::vector<std::string>> TransformationSequence::forward(const LailaValue& data) const {
  LailaValue current = data;
  std::vector<std::string> inverse;
  for (const auto& t : items_) {
    current = t->forward(current);
    inverse.push_back(t->backward_code());
  }
  std::reverse(inverse.begin(), inverse.end());
  return {current, inverse};
}

TransformationSequence TransformationSequence::base64() {
  TransformationSequence seq;
  seq.append(std::make_shared<Base64Transform>());
  return seq;
}

// ---------------- recognized-snippet registry ----------------
static std::map<std::string, BackwardFn>& backward_registry() {
  static std::map<std::string, BackwardFn> r;
  return r;
}

void register_backward(const std::string& code, BackwardFn fn) { backward_registry()[code] = std::move(fn); }

// Semantic signature match: which library + op a Python `backward` snippet runs,
// tolerant to kwargs-repr / whitespace differences.
static bool sig(const std::string& code, const char* needle) {
  return code.find(needle) != std::string::npos;
}

bool has_backward(const std::string& code) {
  if (sig(code, "b64decode") || sig(code, "msgpack.unpackb") || sig(code, "pickle.loads") ||
      sig(code, "np.load") || sig(code, "json.loads"))
    return true;
  return backward_registry().count(code) > 0;
}

static std::vector<uint8_t> as_bytes(const LailaValue& in) {
  if (in.kind() == LailaValue::Kind::Bytes) return in.as_bytes();
  if (in.kind() == LailaValue::Kind::String) return std::vector<uint8_t>(in.as_string().begin(), in.as_string().end());
  return in.serialize().first;
}

LailaValue apply_backward(const std::string& code, const LailaValue& in) {
  // base64.b64decode: base64 str -> raw bytes.
  if (sig(code, "b64decode")) return LailaValue::from_bytes(base64_decode(in.as_string()));
  // msgpack.unpackb: bytes -> dict/list/scalar value.
  if (sig(code, "msgpack.unpackb")) return LailaValue::from_json_payload(cd::msgpack_decode(as_bytes(in)));
  // pickle.loads: bytes -> scalar/str/bytes value (the Python default serializer).
  if (sig(code, "pickle.loads")) return LailaValue::from_json_payload(cd::pickle_decode(as_bytes(in)));
  // numpy np.load: bytes -> ndarray (Tensor).
  if (sig(code, "np.load")) {
    cd::NpyArray a = cd::npy_decode(as_bytes(in));
    return LailaValue::tensor(a.raw, a.dtype, a.shape);
  }
  // json.loads: json str -> value.
  if (sig(code, "json.loads")) return LailaValue::from_json_payload(Json::parse(in.as_string()));
  // Not portable to a non-Python target.
  if (sig(code, "zlib.decompress"))
    raise(Status::Unsupported, "zlib transform not yet supported on this target");
  if (sig(code, "Fernet"))
    raise(Status::Unsupported, "Fernet-encrypted payloads are not supported on this target");
  auto it = backward_registry().find(code);
  if (it != backward_registry().end()) return it->second(in);
  raise(Status::Unsupported, "unrecognized constitution snippet on this target");
}

}  // namespace laila_c
