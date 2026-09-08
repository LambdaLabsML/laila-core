// Internal SHA-1 helper shared by identity (UUID5) and the WebSocket handshake
// (Sec-WebSocket-Accept). Dependency-free; one implementation in identity.cpp.
#ifndef LAILA_DETAIL_SHA1_HPP
#define LAILA_DETAIL_SHA1_HPP

#include <cstddef>
#include <cstdint>

namespace laila_c {
namespace detail {

// Compute the SHA-1 digest of `n` bytes at `data` into `out` (20 bytes).
void sha1(const uint8_t* data, size_t n, uint8_t out[20]);

}  // namespace detail
}  // namespace laila_c

#endif  // LAILA_DETAIL_SHA1_HPP
