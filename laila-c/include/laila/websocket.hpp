// Minimal RFC6455 WebSocket layer for laila-C, used so that the (unmodifiable)
// Python `laila` peer transport -- native WebSocket + JSON-RPC `peer.connect` /
// `rpc.call` -- can interoperate with laila-C policies. It rides on top of any
// HAL `Connection` (a raw TCP socket on POSIX, an lwIP socket on ESP32), so the
// exact same code path serves the host and the emulated embedded target.
//
// Scope: client + server opening handshakes, and unfragmented text/binary data
// frames with control-frame (ping/pong/close) handling. Client frames are
// masked per the spec; server frames are not. permessage-deflate is never
// negotiated (we omit the extension header), so payloads are plain JSON.
#ifndef LAILA_WEBSOCKET_HPP
#define LAILA_WEBSOCKET_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "laila/hal/hal.hpp"

namespace laila_c {
namespace ws {

// Sec-WebSocket-Accept = base64(SHA1(key + RFC6455 GUID)).
std::string compute_accept(const std::string& sec_websocket_key);

// True if the freshly-accepted bytes look like an HTTP WebSocket upgrade
// request (vs. a raw one-shot JSON-RPC frame from a laila-C TCP peer).
bool looks_like_ws_upgrade(const std::vector<uint8_t>& first_bytes);

// A persistent WebSocket session layered over a (non-owning) HAL Connection.
// One side acts as client (masks outbound frames), the other as server.
class WsSession {
public:
  WsSession(hal::Connection* conn, bool is_client) : conn_(conn), client_(is_client) {}

  // Client side: send the HTTP upgrade and validate the 101 response.
  bool client_handshake(const std::string& host, uint16_t port, const std::string& path = "/");
  // Server side: parse the inbound upgrade request (continuing to read until the
  // header terminator if needed) and reply 101. `already_read` are bytes the
  // caller peeked from the socket to classify the connection.
  bool server_handshake(const std::vector<uint8_t>& already_read);

  // Send one text data frame.
  bool send_text(const std::string& payload);
  // Receive the next text/binary message; transparently answers ping with pong
  // and returns false on close/timeout/error. `timeout_ms` < 0 blocks.
  bool recv_text(std::string& out, int timeout_ms);

  // Send a close frame (best-effort).
  void close();
  bool ok() const { return conn_ != nullptr && conn_->is_open(); }

private:
  // Ensure the internal buffer holds at least `n` bytes (reading more from the
  // connection until the deadline). Returns false on timeout/closed.
  bool ensure(size_t n, int timeout_ms);
  bool read_http_headers(std::string& headers, int timeout_ms);
  bool send_frame(uint8_t opcode, const uint8_t* data, size_t n);

  hal::Connection* conn_;
  bool client_;
  std::vector<uint8_t> buf_;  // unconsumed bytes already read from the socket
};

}  // namespace ws
}  // namespace laila_c

#endif  // LAILA_WEBSOCKET_HPP
