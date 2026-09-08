// CPython pickle mirror over the `Json` value model. The WRITER emits protocol 4
// (no framing, no memo) for None/bool/int/float/str/bytes and the stdlib
// containers -- Python's pickle.loads accepts any protocol. The READER accepts
// the opcodes CPython's default pickler (protocol 5) emits for those types,
// including FRAME and the memo ops. Opcodes that would execute code
// (REDUCE/GLOBAL/STACK_GLOBAL/BUILD/INST/OBJ) raise Status::Unsupported -- that
// is the clean boundary for torch tensors and arbitrary custom objects.
#include "laila/compdata.hpp"

#include <cstring>
#include <map>

#include "laila/status.hpp"

namespace laila_c {
namespace cd {
namespace {

// ---- opcodes ----
enum : uint8_t {
  PROTO = 0x80, FRAME = 0x95, STOP = 0x2E, MARK = 0x28,
  NONE = 0x4E, NEWTRUE = 0x88, NEWFALSE = 0x89,
  BININT = 0x4A, BININT1 = 0x4B, BININT2 = 0x4D, LONG1 = 0x8A, LONG4 = 0x8B,
  BINFLOAT = 0x47,
  SHORT_BINUNICODE = 0x8C, BINUNICODE = 0x58, BINUNICODE8 = 0x8D,
  SHORT_BINBYTES = 0x43, BINBYTES = 0x42, BINBYTES8 = 0x8E,
  EMPTY_LIST = 0x5D, APPEND = 0x61, APPENDS = 0x65,
  EMPTY_DICT = 0x7D, SETITEM = 0x73, SETITEMS = 0x75,
  EMPTY_TUPLE = 0x29, TUPLE = 0x74, TUPLE1 = 0x85, TUPLE2 = 0x86, TUPLE3 = 0x87,
  MEMOIZE = 0x94, BINPUT = 0x71, LONG_BINPUT = 0x72, BINGET = 0x68, LONG_BINGET = 0x6A,
};

// ----------------------------- writer -----------------------------
void put_le(std::vector<uint8_t>& o, uint64_t v, int n) {
  for (int i = 0; i < n; ++i) o.push_back((uint8_t)((v >> (i * 8)) & 0xFF));
}

void wr_int(std::vector<uint8_t>& o, int64_t i) {
  if (i >= 0 && i <= 0xFF) { o.push_back(BININT1); o.push_back((uint8_t)i); }
  else if (i >= 0 && i <= 0xFFFF) { o.push_back(BININT2); put_le(o, (uint64_t)i, 2); }
  else if (i >= -0x80000000LL && i <= 0x7FFFFFFFLL) { o.push_back(BININT); put_le(o, (uint64_t)(uint32_t)(int32_t)i, 4); }
  else {
    // LONG1: minimal little-endian two's complement.
    uint8_t buf[9];
    int len = 0;
    uint64_t u = (uint64_t)i;
    for (int k = 0; k < 8; ++k) buf[k] = (uint8_t)((u >> (k * 8)) & 0xFF);
    len = 8;
    // trim redundant sign bytes
    bool neg = i < 0;
    while (len > 1) {
      uint8_t top = buf[len - 1];
      uint8_t prev_msb = buf[len - 2] & 0x80;
      if (neg && top == 0xFF && prev_msb) { --len; continue; }
      if (!neg && top == 0x00 && !prev_msb) { --len; continue; }
      break;
    }
    o.push_back(LONG1);
    o.push_back((uint8_t)len);
    for (int k = 0; k < len; ++k) o.push_back(buf[k]);
  }
}

void wr_value(std::vector<uint8_t>& o, const Json& v) {
  switch (v.type()) {
    case Json::Type::Null: o.push_back(NONE); break;
    case Json::Type::Bool: o.push_back(v.as_bool() ? NEWTRUE : NEWFALSE); break;
    case Json::Type::Int: wr_int(o, v.as_int()); break;
    case Json::Type::Double: {
      o.push_back(BINFLOAT);
      uint64_t bits; double d = v.as_double(); std::memcpy(&bits, &d, 8);
      for (int i = 7; i >= 0; --i) o.push_back((uint8_t)((bits >> (i * 8)) & 0xFF));  // big-endian
      break;
    }
    case Json::Type::String: {
      const std::string& s = v.as_string();
      if (s.size() < 256) { o.push_back(SHORT_BINUNICODE); o.push_back((uint8_t)s.size()); }
      else { o.push_back(BINUNICODE); put_le(o, s.size(), 4); }
      o.insert(o.end(), s.begin(), s.end());
      break;
    }
    case Json::Type::Bytes: {
      const auto& b = v.as_bytes();
      if (b.size() < 256) { o.push_back(SHORT_BINBYTES); o.push_back((uint8_t)b.size()); }
      else { o.push_back(BINBYTES); put_le(o, b.size(), 4); }
      o.insert(o.end(), b.begin(), b.end());
      break;
    }
    case Json::Type::Array: {
      o.push_back(EMPTY_LIST);
      if (!v.elements().empty()) {
        o.push_back(MARK);
        for (const auto& e : v.elements()) wr_value(o, e);
        o.push_back(APPENDS);
      }
      break;
    }
    case Json::Type::Object: {
      o.push_back(EMPTY_DICT);
      if (!v.items().empty()) {
        o.push_back(MARK);
        for (const auto& kv : v.items()) {
          o.push_back(SHORT_BINUNICODE);
          o.push_back((uint8_t)kv.first.size());
          o.insert(o.end(), kv.first.begin(), kv.first.end());
          wr_value(o, kv.second);
        }
        o.push_back(SETITEMS);
      }
      break;
    }
  }
}

// ----------------------------- reader -----------------------------
struct Reader {
  const uint8_t* p;
  size_t n;
  size_t i = 0;
  std::vector<Json> stack;
  std::vector<size_t> marks;
  std::map<int64_t, Json> memo;
  int64_t memo_next = 0;

  uint8_t u8() { if (i >= n) raise(Status::Error, "pickle: truncated"); return p[i++]; }
  uint64_t le(int k) { uint64_t v = 0; for (int j = 0; j < k; ++j) v |= (uint64_t)u8() << (j * 8); return v; }
  void need(size_t k) { if (i + k > n) raise(Status::Error, "pickle: truncated payload"); }
  Json& top() { if (stack.empty()) raise(Status::Error, "pickle: empty stack"); return stack.back(); }

  int64_t read_long(size_t len) {
    if (len == 0) return 0;
    need(len);
    bool neg = (p[i + len - 1] & 0x80) != 0;
    // bytes beyond 8 must be pure sign-extension or it overflows int64
    for (size_t k = 8; k < len; ++k) {
      uint8_t expect = neg ? 0xFF : 0x00;
      if (p[i + k] != expect) raise(Status::Unsupported, "pickle: integer exceeds 64-bit");
    }
    uint64_t v = neg ? ~0ull : 0ull;
    size_t take = len < 8 ? len : 8;
    v = neg ? ~0ull : 0ull;
    for (size_t k = 0; k < take; ++k) {
      v &= ~((uint64_t)0xFF << (k * 8));
      v |= (uint64_t)p[i + k] << (k * 8);
    }
    i += len;
    return (int64_t)v;
  }

  Json run() {
    while (i < n) {
      uint8_t op = u8();
      switch (op) {
        case PROTO: u8(); break;                 // proto version
        case FRAME: le(8); break;                // frame length (ignored)
        case STOP: { Json r = top(); stack.pop_back(); return r; }
        case MARK: marks.push_back(stack.size()); break;
        case NONE: stack.push_back(Json(nullptr)); break;
        case NEWTRUE: stack.push_back(Json(true)); break;
        case NEWFALSE: stack.push_back(Json(false)); break;
        case BININT1: stack.push_back(Json((int64_t)u8())); break;
        case BININT2: stack.push_back(Json((int64_t)le(2))); break;
        case BININT: stack.push_back(Json((int64_t)(int32_t)(uint32_t)le(4))); break;
        case LONG1: { size_t len = u8(); stack.push_back(Json(read_long(len))); break; }
        case LONG4: { size_t len = (size_t)le(4); stack.push_back(Json(read_long(len))); break; }
        case BINFLOAT: {
          need(8);
          uint64_t bits = 0;
          for (int k = 0; k < 8; ++k) bits = (bits << 8) | p[i + k];  // big-endian
          i += 8;
          double d; std::memcpy(&d, &bits, 8);
          stack.push_back(Json(d));
          break;
        }
        case SHORT_BINUNICODE: { size_t len = u8(); need(len); stack.push_back(Json(std::string((const char*)p + i, len))); i += len; break; }
        case BINUNICODE: { size_t len = (size_t)le(4); need(len); stack.push_back(Json(std::string((const char*)p + i, len))); i += len; break; }
        case BINUNICODE8: { size_t len = (size_t)le(8); need(len); stack.push_back(Json(std::string((const char*)p + i, len))); i += len; break; }
        case SHORT_BINBYTES: { size_t len = u8(); need(len); stack.push_back(Json::bytes(std::vector<uint8_t>(p + i, p + i + len))); i += len; break; }
        case BINBYTES: { size_t len = (size_t)le(4); need(len); stack.push_back(Json::bytes(std::vector<uint8_t>(p + i, p + i + len))); i += len; break; }
        case BINBYTES8: { size_t len = (size_t)le(8); need(len); stack.push_back(Json::bytes(std::vector<uint8_t>(p + i, p + i + len))); i += len; break; }
        case EMPTY_LIST: stack.push_back(Json::array()); break;
        case EMPTY_DICT: stack.push_back(Json::object()); break;
        case EMPTY_TUPLE: stack.push_back(Json::array()); break;
        case APPEND: { Json v = top(); stack.pop_back(); top().push_back(v); break; }
        case APPENDS: {
          if (marks.empty()) raise(Status::Error, "pickle: APPENDS without MARK");
          size_t m = marks.back(); marks.pop_back();
          std::vector<Json> items(stack.begin() + m, stack.end());
          stack.resize(m);
          for (auto& it : items) top().push_back(it);
          break;
        }
        case SETITEM: {
          Json v = top(); stack.pop_back();
          Json k = top(); stack.pop_back();
          if (!k.is_string()) raise(Status::Unsupported, "pickle: non-string dict key");
          top()[k.as_string()] = v;
          break;
        }
        case SETITEMS: {
          if (marks.empty()) raise(Status::Error, "pickle: SETITEMS without MARK");
          size_t m = marks.back(); marks.pop_back();
          std::vector<Json> kvs(stack.begin() + m, stack.end());
          stack.resize(m);
          for (size_t k = 0; k + 1 < kvs.size(); k += 2) {
            if (!kvs[k].is_string()) raise(Status::Unsupported, "pickle: non-string dict key");
            top()[kvs[k].as_string()] = kvs[k + 1];
          }
          break;
        }
        case TUPLE1: { Json a = Json::array(); a.push_back(top()); stack.pop_back(); stack.push_back(a); break; }
        case TUPLE2: {
          Json b = top(); stack.pop_back();
          Json a0 = top(); stack.pop_back();
          Json a = Json::array(); a.push_back(a0); a.push_back(b); stack.push_back(a); break;
        }
        case TUPLE3: {
          Json c = top(); stack.pop_back();
          Json b = top(); stack.pop_back();
          Json a0 = top(); stack.pop_back();
          Json a = Json::array(); a.push_back(a0); a.push_back(b); a.push_back(c); stack.push_back(a); break;
        }
        case TUPLE: {
          if (marks.empty()) raise(Status::Error, "pickle: TUPLE without MARK");
          size_t m = marks.back(); marks.pop_back();
          Json a = Json::array();
          for (size_t k = m; k < stack.size(); ++k) a.push_back(stack[k]);
          stack.resize(m);
          stack.push_back(a);
          break;
        }
        case MEMOIZE: memo[memo_next++] = top(); break;
        case BINPUT: { int64_t idx = u8(); memo[idx] = top(); if (idx >= memo_next) memo_next = idx + 1; break; }
        case LONG_BINPUT: { int64_t idx = (int64_t)le(4); memo[idx] = top(); if (idx >= memo_next) memo_next = idx + 1; break; }
        case BINGET: { int64_t idx = u8(); stack.push_back(memo.at(idx)); break; }
        case LONG_BINGET: { int64_t idx = (int64_t)le(4); stack.push_back(memo.at(idx)); break; }
        default:
          raise(Status::Unsupported, "pickle: opcode 0x" + std::string(1, "0123456789abcdef"[op >> 4]) +
                                          std::string(1, "0123456789abcdef"[op & 0xF]) +
                                          " not supported (arbitrary objects/torch)");
      }
    }
    raise(Status::Error, "pickle: missing STOP");
  }
};

}  // namespace

std::vector<uint8_t> pickle_encode(const Json& value) {
  std::vector<uint8_t> o;
  o.push_back(PROTO);
  o.push_back(4);
  wr_value(o, value);
  o.push_back(STOP);
  return o;
}

Json pickle_decode(const std::vector<uint8_t>& data) {
  Reader r{data.data(), data.size()};
  return r.run();
}

}  // namespace cd
}  // namespace laila_c
