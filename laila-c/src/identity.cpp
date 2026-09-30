#include "laila/identity.hpp"

#include <array>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include "laila/detail/sha1.hpp"
#include "laila/hal/hal.hpp"
#include "laila/status.hpp"

namespace laila_c {

// ---------------- SHA-1 (for UUID5) ----------------
namespace {
struct Sha1 {
  uint32_t h[5] = {0x67452301u, 0xEFCDAB89u, 0x98BADCFEu, 0x10325476u, 0xC3D2E1F0u};
  uint64_t length = 0;
  uint8_t block[64];
  size_t block_len = 0;

  static uint32_t rol(uint32_t v, int b) { return (v << b) | (v >> (32 - b)); }

  void process(const uint8_t* p) {
    uint32_t w[80];
    for (int i = 0; i < 16; ++i)
      w[i] = (p[i * 4] << 24) | (p[i * 4 + 1] << 16) | (p[i * 4 + 2] << 8) | p[i * 4 + 3];
    for (int i = 16; i < 80; ++i) w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
    for (int i = 0; i < 80; ++i) {
      uint32_t f, k;
      if (i < 20) { f = (b & c) | ((~b) & d); k = 0x5A827999u; }
      else if (i < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1u; }
      else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDCu; }
      else { f = b ^ c ^ d; k = 0xCA62C1D6u; }
      uint32_t tmp = rol(a, 5) + f + e + k + w[i];
      e = d; d = c; c = rol(b, 30); b = a; a = tmp;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e;
  }

  void update(const uint8_t* data, size_t n) {
    length += n * 8;
    while (n > 0) {
      size_t take = 64 - block_len;
      if (take > n) take = n;
      std::memcpy(block + block_len, data, take);
      block_len += take;
      data += take;
      n -= take;
      if (block_len == 64) { process(block); block_len = 0; }
    }
  }

  void finish(uint8_t out[20]) {
    uint8_t pad = 0x80;
    uint64_t len = length;
    update(&pad, 1);
    uint8_t zero = 0;
    while (block_len != 56) update(&zero, 1);
    uint8_t lb[8];
    for (int i = 0; i < 8; ++i) lb[i] = static_cast<uint8_t>((len >> (56 - i * 8)) & 0xFF);
    // update() would re-add to length; write tail directly.
    std::memcpy(block + block_len, lb, 8);
    process(block);
    for (int i = 0; i < 5; ++i) {
      out[i * 4] = static_cast<uint8_t>(h[i] >> 24);
      out[i * 4 + 1] = static_cast<uint8_t>(h[i] >> 16);
      out[i * 4 + 2] = static_cast<uint8_t>(h[i] >> 8);
      out[i * 4 + 3] = static_cast<uint8_t>(h[i]);
    }
  }
};

std::string format_uuid(const uint8_t b[16]) {
  char buf[37];
  std::snprintf(buf, sizeof(buf),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
                b[12], b[13], b[14], b[15]);
  return std::string(buf);
}

bool parse_uuid_bytes(const std::string& uuid, uint8_t out[16]) {
  int n = 0;
  uint8_t cur = 0;
  bool high = true;
  for (char c : uuid) {
    if (c == '-') continue;
    uint8_t v;
    if (c >= '0' && c <= '9') v = c - '0';
    else if (c >= 'a' && c <= 'f') v = c - 'a' + 10;
    else if (c >= 'A' && c <= 'F') v = c - 'A' + 10;
    else return false;
    if (high) { cur = v << 4; high = false; }
    else { cur |= v; if (n < 16) out[n] = cur; ++n; high = true; }
  }
  return n == 16;
}

std::string g_active_namespace_uuid;  // set lazily
}  // namespace

namespace detail {
// Reuse the SHA-1 core above for the WebSocket handshake accept key.
void sha1(const uint8_t* data, size_t n, uint8_t out[20]) {
  Sha1 sha;
  sha.update(data, n);
  sha.finish(out);
}
}  // namespace detail

// ---------------- UUID ----------------
std::string uuid4() {
  uint8_t b[16];
  hal::get().random().fill(b, 16);
  b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x40);  // version 4
  b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);  // variant
  return format_uuid(b);
}

std::string uuid5(const std::string& namespace_uuid, const std::string& name) {
  uint8_t ns[16];
  if (!parse_uuid_bytes(namespace_uuid, ns)) raise(Status::Error, "bad namespace uuid");
  Sha1 sha;
  sha.update(ns, 16);
  sha.update(reinterpret_cast<const uint8_t*>(name.data()), name.size());
  uint8_t digest[20];
  sha.finish(digest);
  uint8_t b[16];
  std::memcpy(b, digest, 16);
  b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x50);  // version 5
  b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);  // variant
  return format_uuid(b);
}

// RFC 4122 DNS namespace, used to derive the default/active namespace.
static const char* kDnsNamespace = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

// laila's LAILA_UNIVERSAL_NAMESPACE (macros/defaults.py). Hardcoded so
// nickname-derived ids match laila byte-for-byte under the default namespace.
static const char* kUniversalNamespace = "6c8dcd4c-d490-58bc-81a7-6afbe8d17594";

void set_active_namespace(const std::string& namespace_key) {
  // Matches laila: uuid5(NAMESPACE_DNS, namespace_key).
  g_active_namespace_uuid = uuid5(kDnsNamespace, namespace_key);
}
std::string get_active_namespace_uuid() {
  if (g_active_namespace_uuid.empty()) g_active_namespace_uuid = kUniversalNamespace;
  return g_active_namespace_uuid;
}
std::string generate_uuid_from_nickname(const std::string& nickname) {
  return uuid5(get_active_namespace_uuid(), nickname);
}

// ---------------- global_id attributes ----------------
static bool is_attr_key_start(char c) {
  return c == '_' || std::isalpha(static_cast<unsigned char>(c));
}
static bool is_attr_key_char(char c) {
  return c == '_' || std::isalnum(static_cast<unsigned char>(c));
}

GidAttributes parse_global_id_attributes(const std::string& attributes) {
  GidAttributes out;
  if (attributes.empty()) return out;
  size_t start = 0;
  while (true) {
    size_t comma = attributes.find(',', start);
    std::string pair = attributes.substr(start, comma == std::string::npos ? std::string::npos
                                                                            : comma - start);
    size_t eq = pair.find('=');
    if (eq == std::string::npos || eq == 0)
      raise(Status::Error, "Invalid global id attribute: '" + pair + "'");
    std::string key = pair.substr(0, eq);
    std::string value = pair.substr(eq + 1);
    if (!is_attr_key_start(key[0]))
      raise(Status::Error, "Invalid global id attribute: '" + pair + "'");
    for (char c : key)
      if (!is_attr_key_char(c)) raise(Status::Error, "Invalid global id attribute: '" + pair + "'");
    for (char c : value)
      if (c == '@') raise(Status::Error, "Invalid global id attribute: '" + pair + "'");
    for (const auto& kv : out)
      if (kv.first == key) raise(Status::Error, "Duplicate global id attribute: '" + key + "'");
    out.emplace_back(key, value);
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return out;
}

std::string format_global_id_attributes(const GidAttributes& attributes) {
  std::string out;
  for (const auto& kv : attributes) {
    if (!out.empty()) out += ",";
    out += kv.first;
    out += "=";
    out += kv.second;
  }
  return out;
}

std::pair<std::string, GidAttributes> split_global_id_attributes(const std::string& ref) {
  size_t at = ref.find('@');
  if (at == std::string::npos) return {ref, {}};
  std::string tail = ref.substr(at + 1);
  if (tail.empty()) raise(Status::Error, "Invalid GID format: " + ref);
  return {ref.substr(0, at), parse_global_id_attributes(tail)};
}

std::string strip_global_id_attributes(const std::string& global_id) {
  size_t at = global_id.find('@');
  return at == std::string::npos ? global_id : global_id.substr(0, at);
}

GidAttributes get_attributes_from_global_id(const std::string& global_id) {
  if (!is_laila_resource(global_id)) raise(Status::Error, "Invalid GID format: " + global_id);
  return split_global_id_attributes(global_id).second;
}

// ---------------- global_id ----------------
std::string to_global_id(const std::string& uuid, const std::vector<std::string>& scopes_in,
                         std::optional<int64_t> evolution) {
  std::vector<std::string> scopes = scopes_in;
  if (scopes.empty()) scopes = {scope::OBJECT};
  std::string out = scope::TOPMOST;
  for (const auto& s : scopes) { out += ":"; out += s; }
  out += ":";
  out += uuid;
  if (evolution.has_value()) {
    out += "@";
    out += EVOLUTION_ATTRIBUTE;
    out += "=";
    out += std::to_string(*evolution);
  }
  return out;
}

static bool is_uuid_segment(const std::string& seg) {
  if (seg.size() != 36) return false;
  int hex = 0, dash = 0;
  for (size_t k = 0; k < 36; ++k) {
    char c = seg[k];
    if (c == '-') { ++dash; continue; }
    if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
    ++hex;
  }
  return hex == 32 && dash == 4;
}

static std::vector<std::string> split_colon(const std::string& s) {
  std::vector<std::string> parts;
  std::string cur;
  for (char c : s) {
    if (c == ':') { parts.push_back(cur); cur.clear(); }
    else cur.push_back(c);
  }
  parts.push_back(cur);
  return parts;
}

static bool all_digits(const std::string& s) {
  if (s.empty()) return false;
  for (char c : s)
    if (!std::isdigit(static_cast<unsigned char>(c))) return false;
  return true;
}

// Non-raising structural check of a "k=v,k=v" attribute list (no duplicates,
// well-formed keys, no '@' in values). parse_global_id_attributes() raises on
// the same conditions; this lets is_laila_resource stay a pure predicate.
static bool valid_attributes(const std::string& tail) {
  if (tail.empty()) return false;
  std::vector<std::string> keys;
  size_t start = 0;
  while (true) {
    size_t comma = tail.find(',', start);
    std::string pair =
        tail.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
    size_t eq = pair.find('=');
    if (eq == std::string::npos || eq == 0 || !is_attr_key_start(pair[0])) return false;
    for (size_t k = 0; k < eq; ++k)
      if (!is_attr_key_char(pair[k])) return false;
    for (size_t k = eq + 1; k < pair.size(); ++k)
      if (pair[k] == '@') return false;
    std::string key = pair.substr(0, eq);
    for (const auto& seen : keys)
      if (seen == key) return false;
    keys.push_back(key);
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return true;
}

bool is_laila_resource(const std::string& global_id) {
  // <scopes>:<uuid>[@k=v,...]; the scope chain must be LAILA:<at least one>.
  size_t at = global_id.find('@');
  std::string head = at == std::string::npos ? global_id : global_id.substr(0, at);
  if (at != std::string::npos && !valid_attributes(global_id.substr(at + 1))) return false;
  std::vector<std::string> parts = split_colon(head);
  if (parts.size() < 3) return false;
  if (parts.front() != scope::TOPMOST) return false;
  for (size_t k = 1; k + 1 < parts.size(); ++k) {
    if (parts[k].empty()) return false;
    for (char c : parts[k])
      if (!is_attr_key_char(c)) return false;
  }
  return is_uuid_segment(parts.back());
}

ParsedGid process_global_id(const std::string& global_id) {
  if (!is_laila_resource(global_id)) raise(Status::Error, "Invalid GID format: " + global_id);
  auto split = split_global_id_attributes(global_id);
  std::vector<std::string> parts = split_colon(split.first);
  ParsedGid out;
  out.uuid = parts.back();
  out.evolution = std::nullopt;
  for (const auto& kv : split.second) {
    if (kv.first == EVOLUTION_ATTRIBUTE) {
      if (!all_digits(kv.second)) raise(Status::Error, "Invalid evolution in GID: " + global_id);
      out.evolution = std::stoll(kv.second);
    }
    // Other attributes are search arguments, not identity.
  }
  // scopes = the middle segments only: drop the leading TOPMOST and the uuid
  // so that to_global_id(out.uuid, out.scopes, out.evolution) round-trips.
  // Mirrors laila's split(":")[1:-1].
  for (size_t k = 1; k + 1 < parts.size(); ++k) out.scopes.push_back(parts[k]);
  return out;
}

// ---------------- creation timestamp ----------------
// Civil date from days since 1970-01-01 (H. Hinnant's algorithm); avoids a
// libc gmtime dependency so MCU targets format identically to the host.
static void civil_from_days(int64_t z, int& y, unsigned& m, unsigned& d) {
  z += 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const unsigned doe = static_cast<unsigned>(z - era * 146097);
  const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t yy = static_cast<int64_t>(yoe) + era * 400;
  const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const unsigned mp = (5 * doy + 2) / 153;
  d = doy - (153 * mp + 2) / 5 + 1;
  m = mp < 10 ? mp + 3 : mp - 9;
  y = static_cast<int>(yy + (m <= 2));
}

std::string now_creation_timestamp() {
  const uint64_t ms = hal::get().clock().epoch_ms();
  const int64_t secs = static_cast<int64_t>(ms / 1000);
  const unsigned milli = static_cast<unsigned>(ms % 1000);
  const int64_t days = secs / 86400;
  const int64_t sod = secs - days * 86400;
  int y; unsigned mo, d;
  civil_from_days(days, y, mo, d);
  char buf[40];
  std::snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02lld:%02lld:%02lld.%03u+00:00", y, mo, d,
                static_cast<long long>(sod / 3600), static_cast<long long>((sod % 3600) / 60),
                static_cast<long long>(sod % 60), milli);
  return std::string(buf);
}

void _LAILA_IDENTIFIABLE_OBJECT::set_global_id(const std::string& value) {
  ParsedGid p = process_global_id(value);
  uuid_ = p.uuid;
  scopes_ = p.scopes;
  evolution_ = p.evolution;
}

}  // namespace laila_c
