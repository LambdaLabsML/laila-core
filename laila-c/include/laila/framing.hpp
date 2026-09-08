// Length-prefixed stream framing for the stream carrier (TCP/TLS/Unix), matching
// laila's `_carriers/codec.py`: each message is `[uint32 big-endian length][payload]`.
// Buffers across HAL recv() calls so a frame can span reads (or several frames
// arrive in one read). Used by both the outbound TCP/IP protocol and the inbound
// server session.
#ifndef LAILA_FRAMING_HPP
#define LAILA_FRAMING_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "laila/hal/hal.hpp"

namespace laila_c {

class StreamFramer {
public:
  explicit StreamFramer(hal::Connection* conn) : conn_(conn) {}

  // Seed already-read bytes (the bytes the server peeked to classify the link).
  void seed(const std::vector<uint8_t>& bytes) { buf_.insert(buf_.end(), bytes.begin(), bytes.end()); }

  // Send one length-prefixed frame.
  bool send(const std::string& payload);
  // Receive the next full frame's payload; false on timeout/closed/oversize.
  bool recv(std::string& out, int timeout_ms);

private:
  bool ensure(size_t n, int timeout_ms);
  hal::Connection* conn_;
  std::vector<uint8_t> buf_;
};

}  // namespace laila_c

#endif  // LAILA_FRAMING_HPP
