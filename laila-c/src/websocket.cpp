#include "laila/websocket.hpp"

#include <cstring>

#include "laila/detail/sha1.hpp"
#include "laila/transformation.hpp"  // base64_encode

namespace laila_c {
namespace ws {

namespace {
const char* kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

std::string to_lower(std::string s) {
  for (char& c : s) c = (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
  return s;
}

// Case-insensitive header lookup over a raw HTTP header block.
std::string header_value(const std::string& headers, const std::string& name) {
  std::string lc = to_lower(headers);
  std::string key = to_lower(name) + ":";
  size_t pos = lc.find(key);
  if (pos == std::string::npos) return std::string();
  pos += key.size();
  size_t end = headers.find("\r\n", pos);
  if (end == std::string::npos) end = headers.size();
  std::string v = headers.substr(pos, end - pos);
  size_t b = v.find_first_not_of(" \t");
  size_t e = v.find_last_not_of(" \t\r\n");
  if (b == std::string::npos) return std::string();
  return v.substr(b, e - b + 1);
}
}  // namespace

std::string compute_accept(const std::string& key) {
  std::string concat = key + kGuid;
  uint8_t digest[20];
  detail::sha1(reinterpret_cast<const uint8_t*>(concat.data()), concat.size(), digest);
  return base64_encode(std::vector<uint8_t>(digest, digest + 20));
}

bool looks_like_ws_upgrade(const std::vector<uint8_t>& first) {
  // A WebSocket client opens with an HTTP request line ("GET ... HTTP/1.1").
  if (first.size() < 4) return false;
  return first[0] == 'G' && first[1] == 'E' && first[2] == 'T' && first[3] == ' ';
}

bool WsSession::ensure(size_t n, int timeout_ms) {
  if (conn_ == nullptr) return false;
  uint64_t deadline =
      hal::get().clock().now_ms() + (timeout_ms < 0 ? 0 : (uint64_t)timeout_ms);
  while (buf_.size() < n) {
    std::vector<uint8_t> chunk;
    int slice = timeout_ms < 0 ? 1000 : (int)timeout_ms;
    if (conn_->recv(chunk, slice) && !chunk.empty()) {
      buf_.insert(buf_.end(), chunk.begin(), chunk.end());
      continue;
    }
    if (timeout_ms >= 0 && hal::get().clock().now_ms() >= deadline) return false;
    if (!conn_->is_open()) return false;
  }
  return true;
}

bool WsSession::read_http_headers(std::string& headers, int timeout_ms) {
  // Accumulate until the CRLFCRLF header terminator.
  const std::string term = "\r\n\r\n";
  uint64_t deadline =
      hal::get().clock().now_ms() + (timeout_ms < 0 ? 30000 : (uint64_t)timeout_ms);
  for (;;) {
    std::string cur(buf_.begin(), buf_.end());
    size_t pos = cur.find(term);
    if (pos != std::string::npos) {
      headers = cur.substr(0, pos);
      buf_.erase(buf_.begin(), buf_.begin() + (pos + term.size()));
      return true;
    }
    std::vector<uint8_t> chunk;
    if (conn_->recv(chunk, 1000) && !chunk.empty()) {
      buf_.insert(buf_.end(), chunk.begin(), chunk.end());
      continue;
    }
    if (hal::get().clock().now_ms() >= deadline || !conn_->is_open()) return false;
  }
}

bool WsSession::client_handshake(const std::string& host, uint16_t port,
                                 const std::string& path) {
  uint8_t rnd[16];
  hal::get().random().fill(rnd, 16);
  std::string key = base64_encode(std::vector<uint8_t>(rnd, rnd + 16));

  std::string req = "GET " + path + " HTTP/1.1\r\n";
  req += "Host: " + host + ":" + std::to_string(port) + "\r\n";
  req += "Upgrade: websocket\r\n";
  req += "Connection: Upgrade\r\n";
  req += "Sec-WebSocket-Key: " + key + "\r\n";
  req += "Sec-WebSocket-Version: 13\r\n\r\n";
  if (!conn_->send(std::vector<uint8_t>(req.begin(), req.end()))) return false;

  std::string headers;
  if (!read_http_headers(headers, 30000)) return false;
  if (headers.find(" 101") == std::string::npos) return false;
  std::string accept = header_value(headers, "Sec-WebSocket-Accept");
  return accept == compute_accept(key);
}

bool WsSession::server_handshake(const std::vector<uint8_t>& already_read) {
  if (!already_read.empty()) buf_.insert(buf_.begin(), already_read.begin(), already_read.end());
  std::string headers;
  if (!read_http_headers(headers, 10000)) return false;
  std::string key = header_value(headers, "Sec-WebSocket-Key");
  if (key.empty()) return false;
  std::string resp = "HTTP/1.1 101 Switching Protocols\r\n";
  resp += "Upgrade: websocket\r\n";
  resp += "Connection: Upgrade\r\n";
  resp += "Sec-WebSocket-Accept: " + compute_accept(key) + "\r\n\r\n";
  return conn_->send(std::vector<uint8_t>(resp.begin(), resp.end()));
}

bool WsSession::send_frame(uint8_t opcode, const uint8_t* data, size_t n) {
  std::vector<uint8_t> f;
  f.push_back((uint8_t)(0x80 | opcode));  // FIN + opcode
  uint8_t mask_bit = client_ ? 0x80 : 0x00;
  if (n < 126) {
    f.push_back((uint8_t)(mask_bit | n));
  } else if (n <= 0xFFFF) {
    f.push_back((uint8_t)(mask_bit | 126));
    f.push_back((uint8_t)((n >> 8) & 0xFF));
    f.push_back((uint8_t)(n & 0xFF));
  } else {
    f.push_back((uint8_t)(mask_bit | 127));
    for (int i = 7; i >= 0; --i) f.push_back((uint8_t)((n >> (i * 8)) & 0xFF));
  }
  if (client_) {
    uint8_t mk[4];
    hal::get().random().fill(mk, 4);
    f.insert(f.end(), mk, mk + 4);
    for (size_t i = 0; i < n; ++i) f.push_back((uint8_t)(data[i] ^ mk[i & 3]));
  } else {
    f.insert(f.end(), data, data + n);
  }
  return conn_->send(f);
}

bool WsSession::send_text(const std::string& payload) {
  return send_frame(0x1, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
}

bool WsSession::recv_text(std::string& out, int timeout_ms) {
  for (;;) {
    if (!ensure(2, timeout_ms)) return false;
    uint8_t b0 = buf_[0];
    uint8_t b1 = buf_[1];
    uint8_t opcode = b0 & 0x0F;
    bool masked = (b1 & 0x80) != 0;
    uint64_t len = b1 & 0x7F;
    size_t header = 2;
    if (len == 126) {
      if (!ensure(4, timeout_ms)) return false;
      len = ((uint64_t)buf_[2] << 8) | buf_[3];
      header = 4;
    } else if (len == 127) {
      if (!ensure(10, timeout_ms)) return false;
      len = 0;
      for (int i = 0; i < 8; ++i) len = (len << 8) | buf_[2 + i];
      header = 10;
    }
    uint8_t mask_key[4] = {0, 0, 0, 0};
    if (masked) {
      if (!ensure(header + 4, timeout_ms)) return false;
      for (int i = 0; i < 4; ++i) mask_key[i] = buf_[header + i];
      header += 4;
    }
    if (!ensure(header + len, timeout_ms)) return false;
    std::string payload;
    payload.resize(len);
    for (uint64_t i = 0; i < len; ++i) {
      uint8_t c = buf_[header + i];
      if (masked) c ^= mask_key[i & 3];
      payload[i] = (char)c;
    }
    buf_.erase(buf_.begin(), buf_.begin() + (header + len));

    if (opcode == 0x8) {  // close
      send_frame(0x8, nullptr, 0);
      return false;
    }
    if (opcode == 0x9) {  // ping -> pong with same payload
      send_frame(0xA, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
      continue;
    }
    if (opcode == 0xA) continue;  // pong: ignore
    // text (0x1) or binary (0x2): hand the payload back.
    out = std::move(payload);
    return true;
  }
}

void WsSession::close() {
  if (conn_ && conn_->is_open()) send_frame(0x8, nullptr, 0);
}

}  // namespace ws
}  // namespace laila_c
