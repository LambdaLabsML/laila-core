// numpy .npy v1.0 container mirror (numpy.save/numpy.load, allow_pickle=False):
//   \x93NUMPY \x01\x00 <u16 LE header_len> <ascii dict header padded to 64, '\n'> <raw data>
// The header dict is byte-identical to numpy's writer so vectors round-trip.
#include "laila/compdata.hpp"

#include <string>

#include "laila/status.hpp"

namespace laila_c {
namespace cd {
namespace {
const char kMagic[] = {(char)0x93, 'N', 'U', 'M', 'P', 'Y'};

std::string shape_repr(const std::vector<int64_t>& shape) {
  std::string s = "(";
  for (size_t k = 0; k < shape.size(); ++k) {
    if (k) s += ", ";
    s += std::to_string(shape[k]);
  }
  if (shape.size() == 1) s += ",";
  s += ")";
  return s;
}
}  // namespace

std::vector<uint8_t> npy_encode(const NpyArray& a) {
  std::string hdr = "{'descr': '" + a.dtype + "', 'fortran_order': " +
                    (a.fortran_order ? "True" : "False") + ", 'shape': " + shape_repr(a.shape) +
                    ", }";
  // Pad with spaces + trailing newline so 10 + len(hdr) is a multiple of 64.
  size_t base = 10 + hdr.size() + 1;
  size_t pad = (64 - (base % 64)) % 64;
  hdr.append(pad, ' ');
  hdr.push_back('\n');

  std::vector<uint8_t> out;
  out.insert(out.end(), kMagic, kMagic + 6);
  out.push_back(0x01);
  out.push_back(0x00);
  out.push_back((uint8_t)(hdr.size() & 0xFF));
  out.push_back((uint8_t)((hdr.size() >> 8) & 0xFF));
  out.insert(out.end(), hdr.begin(), hdr.end());
  out.insert(out.end(), a.raw.begin(), a.raw.end());
  return out;
}

NpyArray npy_decode(const std::vector<uint8_t>& data) {
  if (data.size() < 10 || std::string((const char*)data.data(), 6) != std::string(kMagic, 6))
    raise(Status::Error, "npy: bad magic");
  uint8_t major = data[6];
  size_t hdr_off, hdr_len;
  if (major == 1) {
    hdr_len = data[8] | (data[9] << 8);
    hdr_off = 10;
  } else {
    hdr_len = (size_t)data[8] | ((size_t)data[9] << 8) | ((size_t)data[10] << 16) |
              ((size_t)data[11] << 24);
    hdr_off = 12;
  }
  if (hdr_off + hdr_len > data.size()) raise(Status::Error, "npy: truncated header");
  std::string hdr((const char*)data.data() + hdr_off, hdr_len);

  NpyArray a;
  auto field = [&](const std::string& key) -> std::string {
    size_t p = hdr.find(key);
    if (p == std::string::npos) return "";
    return hdr.substr(p + key.size());
  };
  // descr: '<f8'
  {
    std::string rest = field("'descr':");
    size_t q1 = rest.find('\'');
    size_t q2 = rest.find('\'', q1 + 1);
    if (q1 != std::string::npos && q2 != std::string::npos) a.dtype = rest.substr(q1 + 1, q2 - q1 - 1);
  }
  a.fortran_order = field("'fortran_order':").find("True") < field("'fortran_order':").find("False");
  // shape: (d0, d1, ...)
  {
    std::string rest = field("'shape':");
    size_t o = rest.find('(');
    size_t c = rest.find(')', o);
    std::string dims = rest.substr(o + 1, c - o - 1);
    std::string cur;
    for (char ch : dims) {
      if (ch >= '0' && ch <= '9') cur.push_back(ch);
      else if (!cur.empty()) { a.shape.push_back(std::stoll(cur)); cur.clear(); }
    }
    if (!cur.empty()) a.shape.push_back(std::stoll(cur));
  }
  a.raw.assign(data.begin() + hdr_off + hdr_len, data.end());
  return a;
}

}  // namespace cd
}  // namespace laila_c
