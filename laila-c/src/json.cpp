#include "laila/json.hpp"

#include <cstdio>
#include <sstream>

#include "laila/status.hpp"

namespace laila_c {

static const Json kNullSentinel;

Json& Json::operator[](const std::string& key) {
  if (type_ != Type::Object) { type_ = Type::Object; }
  for (auto& kv : obj_) {
    if (kv.first == key) return kv.second;
  }
  obj_.emplace_back(key, Json());
  return obj_.back().second;
}

bool Json::contains(const std::string& key) const {
  if (type_ != Type::Object) return false;
  for (const auto& kv : obj_) {
    if (kv.first == key) return true;
  }
  return false;
}

const Json& Json::at(const std::string& key) const {
  if (type_ != Type::Object) return kNullSentinel;
  for (const auto& kv : obj_) {
    if (kv.first == key) return kv.second;
  }
  return kNullSentinel;
}

static void dump_string(const std::string& s, std::string& out) {
  out.push_back('"');
  for (char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out.push_back(c);
        }
    }
  }
  out.push_back('"');
}

void Json::dump_to(std::string& out) const {
  switch (type_) {
    case Type::Null: out += "null"; break;
    case Type::Bool: out += bool_ ? "true" : "false"; break;
    case Type::Int: out += std::to_string(int_); break;
    case Type::Double: {
      std::ostringstream ss;
      ss << dbl_;
      out += ss.str();
      break;
    }
    case Type::String: dump_string(str_, out); break;
    case Type::Bytes: {
      // JSON has no byte type; emit base64 (best-effort -- the lossless path for
      // nested bytes is msgpack, not JSON text).
      static const char kB64[] =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
      std::string b64;
      size_t k = 0;
      while (k + 2 < bytes_.size()) {
        uint32_t n = (bytes_[k] << 16) | (bytes_[k + 1] << 8) | bytes_[k + 2];
        b64.push_back(kB64[(n >> 18) & 63]); b64.push_back(kB64[(n >> 12) & 63]);
        b64.push_back(kB64[(n >> 6) & 63]); b64.push_back(kB64[n & 63]);
        k += 3;
      }
      size_t rem = bytes_.size() - k;
      if (rem == 1) {
        uint32_t n = bytes_[k] << 16;
        b64.push_back(kB64[(n >> 18) & 63]); b64.push_back(kB64[(n >> 12) & 63]); b64 += "==";
      } else if (rem == 2) {
        uint32_t n = (bytes_[k] << 16) | (bytes_[k + 1] << 8);
        b64.push_back(kB64[(n >> 18) & 63]); b64.push_back(kB64[(n >> 12) & 63]);
        b64.push_back(kB64[(n >> 6) & 63]); b64.push_back('=');
      }
      dump_string(b64, out);
      break;
    }
    case Type::Array: {
      out.push_back('[');
      for (size_t i = 0; i < arr_.size(); ++i) {
        if (i) out.push_back(',');
        arr_[i].dump_to(out);
      }
      out.push_back(']');
      break;
    }
    case Type::Object: {
      out.push_back('{');
      bool first = true;
      for (const auto& kv : obj_) {
        if (!first) out.push_back(',');
        first = false;
        dump_string(kv.first, out);
        out.push_back(':');
        kv.second.dump_to(out);
      }
      out.push_back('}');
      break;
    }
  }
}

std::string Json::dump() const {
  std::string out;
  dump_to(out);
  return out;
}

// ---- Parser ----
namespace {
struct Parser {
  const std::string& s;
  size_t i = 0;
  explicit Parser(const std::string& str) : s(str) {}

  void skip_ws() {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) ++i;
  }
  [[noreturn]] void fail(const char* what) { raise(Status::Error, std::string("JSON parse error: ") + what); }

  Json parse_value() {
    skip_ws();
    if (i >= s.size()) fail("unexpected end");
    char c = s[i];
    if (c == '{') return parse_object();
    if (c == '[') return parse_array();
    if (c == '"') return Json(parse_string());
    if (c == 't' || c == 'f') return parse_bool();
    if (c == 'n') { expect("null"); return Json(nullptr); }
    if (c == '-' || c == '+' || (c >= '0' && c <= '9')) return parse_number();
    fail("unexpected token");
  }
  void expect(const char* lit) {
    for (const char* p = lit; *p; ++p) {
      if (i >= s.size() || s[i] != *p) fail("literal");
      ++i;
    }
  }
  Json parse_bool() {
    if (s[i] == 't') { expect("true"); return Json(true); }
    expect("false");
    return Json(false);
  }
  Json parse_number() {
    size_t start = i;
    bool is_double = false;
    if (i < s.size() && (s[i] == '-' || s[i] == '+')) ++i;
    while (i < s.size() && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.' || s[i] == 'e' ||
                            s[i] == 'E' || s[i] == '+' || s[i] == '-')) {
      if (s[i] == '.' || s[i] == 'e' || s[i] == 'E') is_double = true;
      ++i;
    }
    std::string num = s.substr(start, i - start);
    if (num.empty() || num == "-" || num == "+") fail("invalid number");
    try {
      if (is_double) return Json(std::stod(num));
      return Json(static_cast<int64_t>(std::stoll(num)));
    } catch (const std::exception&) {
      fail("invalid number");
    }
  }
  std::string parse_string() {
    ++i;  // opening quote
    std::string out;
    while (i < s.size() && s[i] != '"') {
      char c = s[i++];
      if (c == '\\') {
        if (i >= s.size()) fail("bad escape");
        char e = s[i++];
        switch (e) {
          case '"': out.push_back('"'); break;
          case '\\': out.push_back('\\'); break;
          case '/': out.push_back('/'); break;
          case 'n': out.push_back('\n'); break;
          case 'r': out.push_back('\r'); break;
          case 't': out.push_back('\t'); break;
          case 'b': out.push_back('\b'); break;
          case 'f': out.push_back('\f'); break;
          case 'u': {
            if (i + 4 > s.size()) fail("bad \\u");
            unsigned cp = (unsigned)std::stoi(s.substr(i, 4), nullptr, 16);
            i += 4;
            // Combine a UTF-16 surrogate pair (e.g. emoji from ensure_ascii JSON).
            if (cp >= 0xD800 && cp <= 0xDBFF) {
              if (i + 6 <= s.size() && s[i] == '\\' && s[i + 1] == 'u') {
                unsigned lo = (unsigned)std::stoi(s.substr(i + 2, 4), nullptr, 16);
                if (lo >= 0xDC00 && lo <= 0xDFFF) {
                  cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                  i += 6;
                }
              }
            }
            if (cp < 0x80) {
              out.push_back(static_cast<char>(cp));
            } else if (cp < 0x800) {
              out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp < 0x10000) {
              out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
              out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            break;
          }
          default: fail("bad escape char");
        }
      } else {
        out.push_back(c);
      }
    }
    if (i >= s.size()) fail("unterminated string");
    ++i;  // closing quote
    return out;
  }
  Json parse_array() {
    Json arr = Json::array();
    ++i;  // [
    skip_ws();
    if (i < s.size() && s[i] == ']') { ++i; return arr; }
    while (true) {
      arr.push_back(parse_value());
      skip_ws();
      if (i >= s.size()) fail("unterminated array");
      if (s[i] == ',') { ++i; continue; }
      if (s[i] == ']') { ++i; break; }
      fail("expected , or ]");
    }
    return arr;
  }
  Json parse_object() {
    Json obj = Json::object();
    ++i;  // {
    skip_ws();
    if (i < s.size() && s[i] == '}') { ++i; return obj; }
    while (true) {
      skip_ws();
      if (i >= s.size() || s[i] != '"') fail("expected key");
      std::string key = parse_string();
      skip_ws();
      if (i >= s.size() || s[i] != ':') fail("expected :");
      ++i;
      obj[key] = parse_value();
      skip_ws();
      if (i >= s.size()) fail("unterminated object");
      if (s[i] == ',') { ++i; continue; }
      if (s[i] == '}') { ++i; break; }
      fail("expected , or }");
    }
    return obj;
  }
};
}  // namespace

Json Json::parse(const std::string& text) {
  Parser p(text);
  Json v = p.parse_value();
  return v;
}

}  // namespace laila_c
