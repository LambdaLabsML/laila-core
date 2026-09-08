// msgpack encode/decode over the laila-C `Json` value model, byte-compatible with
// Python `msgpack.packb(use_bin_type=True)` / `unpackb(raw=False)`: str -> str
// family, bytes -> bin family, dict -> map (insertion order preserved), int uses
// the most compact representation (matching msgpack-python).
#include "laila/compdata.hpp"

#include <cstring>

#include "laila/status.hpp"

namespace laila_c {
namespace cd {
namespace {

void put_be(std::vector<uint8_t>& o, uint64_t v, int n) {
  for (int i = n - 1; i >= 0; --i) o.push_back((uint8_t)((v >> (i * 8)) & 0xFF));
}

void enc_uint(std::vector<uint8_t>& o, uint64_t u) {
  if (u < 0x80) { o.push_back((uint8_t)u); }
  else if (u <= 0xFF) { o.push_back(0xCC); o.push_back((uint8_t)u); }
  else if (u <= 0xFFFF) { o.push_back(0xCD); put_be(o, u, 2); }
  else if (u <= 0xFFFFFFFFull) { o.push_back(0xCE); put_be(o, u, 4); }
  else { o.push_back(0xCF); put_be(o, u, 8); }
}

void enc_int(std::vector<uint8_t>& o, int64_t i) {
  if (i >= 0) { enc_uint(o, (uint64_t)i); return; }
  if (i >= -0x20) { o.push_back((uint8_t)(0xE0 | (i & 0x1F))); }
  else if (i >= -0x80) { o.push_back(0xD0); o.push_back((uint8_t)(int8_t)i); }
  else if (i >= -0x8000) { o.push_back(0xD1); put_be(o, (uint64_t)(uint16_t)(int16_t)i, 2); }
  else if (i >= -0x80000000LL) { o.push_back(0xD2); put_be(o, (uint64_t)(uint32_t)(int32_t)i, 4); }
  else { o.push_back(0xD3); put_be(o, (uint64_t)i, 8); }
}

void enc_str(std::vector<uint8_t>& o, const std::string& s) {
  size_t n = s.size();
  if (n < 32) o.push_back((uint8_t)(0xA0 | n));
  else if (n <= 0xFF) { o.push_back(0xD9); o.push_back((uint8_t)n); }
  else if (n <= 0xFFFF) { o.push_back(0xDA); put_be(o, n, 2); }
  else { o.push_back(0xDB); put_be(o, n, 4); }
  o.insert(o.end(), s.begin(), s.end());
}

void enc_bin(std::vector<uint8_t>& o, const std::vector<uint8_t>& b) {
  size_t n = b.size();
  if (n <= 0xFF) { o.push_back(0xC4); o.push_back((uint8_t)n); }
  else if (n <= 0xFFFF) { o.push_back(0xC5); put_be(o, n, 2); }
  else { o.push_back(0xC6); put_be(o, n, 4); }
  o.insert(o.end(), b.begin(), b.end());
}

void encode(std::vector<uint8_t>& o, const Json& v) {
  switch (v.type()) {
    case Json::Type::Null: o.push_back(0xC0); break;
    case Json::Type::Bool: o.push_back(v.as_bool() ? 0xC3 : 0xC2); break;
    case Json::Type::Int: enc_int(o, v.as_int()); break;
    case Json::Type::Double: {
      o.push_back(0xCB);
      uint64_t bits;
      double d = v.as_double();
      std::memcpy(&bits, &d, 8);
      put_be(o, bits, 8);
      break;
    }
    case Json::Type::String: enc_str(o, v.as_string()); break;
    case Json::Type::Bytes: enc_bin(o, v.as_bytes()); break;
    case Json::Type::Array: {
      size_t n = v.elements().size();
      if (n < 16) o.push_back((uint8_t)(0x90 | n));
      else if (n <= 0xFFFF) { o.push_back(0xDC); put_be(o, n, 2); }
      else { o.push_back(0xDD); put_be(o, n, 4); }
      for (const auto& e : v.elements()) encode(o, e);
      break;
    }
    case Json::Type::Object: {
      size_t n = v.items().size();
      if (n < 16) o.push_back((uint8_t)(0x80 | n));
      else if (n <= 0xFFFF) { o.push_back(0xDE); put_be(o, n, 2); }
      else { o.push_back(0xDF); put_be(o, n, 4); }
      for (const auto& kv : v.items()) { enc_str(o, kv.first); encode(o, kv.second); }
      break;
    }
  }
}

struct Reader {
  const uint8_t* p;
  size_t n;
  size_t i = 0;
  uint8_t u8() { if (i >= n) raise(Status::Error, "msgpack: truncated"); return p[i++]; }
  uint64_t be(int k) { uint64_t v = 0; for (int j = 0; j < k; ++j) v = (v << 8) | u8(); return v; }
  std::string str(size_t len) {
    if (i + len > n) raise(Status::Error, "msgpack: truncated str");
    std::string s((const char*)p + i, len); i += len; return s;
  }
  std::vector<uint8_t> bin(size_t len) {
    if (i + len > n) raise(Status::Error, "msgpack: truncated bin");
    std::vector<uint8_t> b(p + i, p + i + len); i += len; return b;
  }

  Json value() {
    uint8_t c = u8();
    if (c <= 0x7F) return Json((int64_t)c);                       // positive fixint
    if (c >= 0xE0) return Json((int64_t)(int8_t)c);               // negative fixint
    if ((c & 0xF0) == 0x80) return map(c & 0x0F);                 // fixmap
    if ((c & 0xF0) == 0x90) return array(c & 0x0F);               // fixarray
    if ((c & 0xE0) == 0xA0) return Json(str(c & 0x1F));           // fixstr
    switch (c) {
      case 0xC0: return Json(nullptr);
      case 0xC2: return Json(false);
      case 0xC3: return Json(true);
      case 0xCC: return Json((int64_t)be(1));
      case 0xCD: return Json((int64_t)be(2));
      case 0xCE: return Json((int64_t)be(4));
      case 0xCF: return Json((int64_t)be(8));
      case 0xD0: return Json((int64_t)(int8_t)be(1));
      case 0xD1: return Json((int64_t)(int16_t)be(2));
      case 0xD2: return Json((int64_t)(int32_t)be(4));
      case 0xD3: return Json((int64_t)be(8));
      case 0xCA: { uint32_t b = (uint32_t)be(4); float f; std::memcpy(&f, &b, 4); return Json((double)f); }
      case 0xCB: { uint64_t b = be(8); double d; std::memcpy(&d, &b, 8); return Json(d); }
      case 0xD9: return Json(str(be(1)));
      case 0xDA: return Json(str(be(2)));
      case 0xDB: return Json(str(be(4)));
      case 0xC4: return Json::bytes(bin(be(1)));
      case 0xC5: return Json::bytes(bin(be(2)));
      case 0xC6: return Json::bytes(bin(be(4)));
      case 0xDC: return array(be(2));
      case 0xDD: return array(be(4));
      case 0xDE: return map(be(2));
      case 0xDF: return map(be(4));
      default: raise(Status::Unsupported, "msgpack: unsupported tag");
    }
  }
  Json array(size_t len) {
    Json a = Json::array();
    for (size_t k = 0; k < len; ++k) a.push_back(value());
    return a;
  }
  Json map(size_t len) {
    Json o = Json::object();
    for (size_t k = 0; k < len; ++k) {
      Json key = value();
      if (!key.is_string()) raise(Status::Unsupported, "msgpack: non-string map key");
      o[key.as_string()] = value();
    }
    return o;
  }
};

}  // namespace

std::vector<uint8_t> msgpack_encode(const Json& value) {
  std::vector<uint8_t> o;
  encode(o, value);
  return o;
}

Json msgpack_decode(const std::vector<uint8_t>& data) {
  Reader r{data.data(), data.size()};
  return r.value();
}

}  // namespace cd
}  // namespace laila_c
