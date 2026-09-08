#include "laila/framing.hpp"

namespace laila_c {

// 256 MiB matches laila's MAX_FRAME_BYTES; lower it per-target if memory-bound.
static const uint32_t kMaxFrame = 256u * 1024u * 1024u;

bool StreamFramer::ensure(size_t n, int timeout_ms) {
  if (conn_ == nullptr) return false;
  uint64_t deadline =
      hal::get().clock().now_ms() + (timeout_ms < 0 ? 0 : (uint64_t)timeout_ms);
  while (buf_.size() < n) {
    std::vector<uint8_t> chunk;
    int slice = timeout_ms < 0 ? 1000 : timeout_ms;
    if (conn_->recv(chunk, slice) && !chunk.empty()) {
      buf_.insert(buf_.end(), chunk.begin(), chunk.end());
      continue;
    }
    if (timeout_ms >= 0 && hal::get().clock().now_ms() >= deadline) return false;
    if (!conn_->is_open()) return false;
  }
  return true;
}

bool StreamFramer::send(const std::string& payload) {
  uint32_t n = (uint32_t)payload.size();
  std::vector<uint8_t> f;
  f.reserve(4 + payload.size());
  f.push_back((uint8_t)((n >> 24) & 0xFF));
  f.push_back((uint8_t)((n >> 16) & 0xFF));
  f.push_back((uint8_t)((n >> 8) & 0xFF));
  f.push_back((uint8_t)(n & 0xFF));
  f.insert(f.end(), payload.begin(), payload.end());
  return conn_ && conn_->send(f);
}

bool StreamFramer::recv(std::string& out, int timeout_ms) {
  if (!ensure(4, timeout_ms)) return false;
  uint32_t n = ((uint32_t)buf_[0] << 24) | ((uint32_t)buf_[1] << 16) |
               ((uint32_t)buf_[2] << 8) | (uint32_t)buf_[3];
  if (n > kMaxFrame) return false;
  if (!ensure((size_t)4 + n, timeout_ms)) return false;
  out.assign((const char*)buf_.data() + 4, n);
  buf_.erase(buf_.begin(), buf_.begin() + 4 + n);
  return true;
}

}  // namespace laila_c
