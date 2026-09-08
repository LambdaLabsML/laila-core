#include "laila/pools.hpp"

#include <cstdio>

#include "laila/hal/hal.hpp"
#include "laila/status.hpp"

namespace laila_c {

void raise_pool_unsupported(const std::string& backend) {
  raise(Status::Unsupported,
        backend + ": backend not available on this target (LAILA_UNSUPPORTED). "
                  "Wire its HAL transport/SDK or route to a supported pool.");
}

// ---------------- BotoPool (pool/boto/boto.py) ----------------

// quote(key, safe="") / unquote(object_key): percent-encode everything outside
// the RFC 3986 unreserved set so a logical key survives as one S3 object key.
static bool is_unreserved(unsigned char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
         c == '-' || c == '_' || c == '.' || c == '~';
}

std::string BotoPool::_object_key(const std::string& key) const {
  static const char* hex = "0123456789ABCDEF";
  std::string out;
  out.reserve(key.size());
  for (unsigned char c : key) {
    if (is_unreserved(c)) {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0x0F]);
    }
  }
  return out;
}

std::string BotoPool::_logical_key(const std::string& object_key) const {
  auto unhex = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
  };
  std::string out;
  out.reserve(object_key.size());
  for (size_t i = 0; i < object_key.size(); ++i) {
    if (object_key[i] == '%' && i + 2 < object_key.size()) {
      int hi = unhex(object_key[i + 1]);
      int lo = unhex(object_key[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out.push_back(static_cast<char>((hi << 4) | lo));
        i += 2;
        continue;
      }
    }
    out.push_back(object_key[i]);
  }
  return out;
}

void BotoPool::_throttle() {
  if (max_req_per_second_.has_value() && *max_req_per_second_ > 0.0) {
    hal::get().clock().sleep_ms(static_cast<uint32_t>(1000.0 / *max_req_per_second_));
  }
}

BotoClient* BotoPool::_get_client() {
  raise(Status::Unsupported,
        "BotoPool: subclasses must implement _get_client (no S3-compatible client "
        "wired on this target)");
  return nullptr;
}

std::optional<Json> BotoPool::_read(const std::string& key) {
  _throttle();
  auto body = _get_client()->get_object(bucket_name_, _object_key(key));
  if (!body.has_value()) return std::nullopt;
  return Json::parse(*body);
}

void BotoPool::_write(const std::string& key, const Json& value) {
  std::string text = value.dump();  // blob is already serialized (mirror json.dumps)
  _throttle();
  _get_client()->put_object(bucket_name_, _object_key(key), text, "application/json");
}

void BotoPool::_delete(const std::string& key) {
  _throttle();
  _get_client()->delete_object(bucket_name_, _object_key(key));
}

bool BotoPool::_exists(const std::string& key) {
  _throttle();
  return _get_client()->head_object(bucket_name_, _object_key(key));
}

std::vector<std::string> BotoPool::_keys() {
  std::vector<std::string> out;
  for (const auto& object_key : _get_client()->list_objects_v2(bucket_name_))
    out.push_back(_logical_key(object_key));
  return out;
}

void BotoPool::_empty() {
  BotoClient* c = _get_client();
  for (const auto& object_key : c->list_objects_v2(bucket_name_)) {
    _throttle();
    c->delete_object(bucket_name_, object_key);
  }
}

// ---------------- S3Pool (pool/s3/s3.py) ----------------
BotoClient* S3Pool::_get_client() {
  if (client_) return client_.get();  // cached, like boto.py self._client
  client_ = make_aws_s3_client(access_key_id_, secret_access_key_, region_name_, max_pool_connections_);
  if (!client_)
    raise(Status::Unsupported,
          "S3Pool: aws-sdk-cpp backend not compiled into laila-C on this target "
          "(enable the [s3] extra: build with -DLAILA_WITH_S3=ON).");
  return client_.get();
}

#ifndef LAILA_WITH_S3
// No S3 backend compiled in: the factory yields nothing, so S3Pool reports
// Unsupported. The posix aws-sdk-cpp backend overrides this when LAILA_WITH_S3.
std::unique_ptr<BotoClient> make_aws_s3_client(const std::string&, const std::string&,
                                               const std::string&, int) {
  return nullptr;
}
#endif

}  // namespace laila_c
