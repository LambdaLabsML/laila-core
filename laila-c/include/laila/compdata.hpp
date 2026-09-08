// compdata codecs: byte-level mirrors of the serializers laila's ComputationalData
// uses, so laila-C round-trips with (unmodified) Python laila. All operate on the
// order-preserving `Json` value model (null/bool/int/double/str/bytes/array/object):
//
//   - msgpack   (dict/list payloads)      packb(use_bin_type=True)/unpackb(raw=False)
//   - pickle    (scalars/str/bytes/...)   CPython proto 2-5 reader + a canonical writer
//   - npy       (numpy arrays)            .npy v1.0 container
//
// Dependency-free, pure C++17, so the same code compiles for host and MCU targets.
// Unsupported inputs (e.g. pickled arbitrary objects, REDUCE/GLOBAL) raise
// Status::Unsupported (LAILA_UNSUPPORTED), matching the translation contract.
#ifndef LAILA_COMPDATA_HPP
#define LAILA_COMPDATA_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "laila/json.hpp"

namespace laila_c {
namespace cd {

// ---- msgpack (mirrors Python `msgpack`) ----
std::vector<uint8_t> msgpack_encode(const Json& value);
Json msgpack_decode(const std::vector<uint8_t>& data);

// ---- pickle (mirrors CPython `pickle`) ----
// Writer emits protocol 4 (no framing) for None/bool/int/float/str/bytes (+ the
// stdlib containers). Reader accepts protocols 2-5 for those opcodes; anything
// requiring code execution (REDUCE/GLOBAL/BUILD) raises Status::Unsupported.
std::vector<uint8_t> pickle_encode(const Json& value);
Json pickle_decode(const std::vector<uint8_t>& data);

// ---- numpy .npy (mirrors `numpy.save`/`numpy.load`, allow_pickle=False) ----
struct NpyArray {
  std::vector<uint8_t> raw;       // raw element buffer
  std::string dtype;              // numpy descr, e.g. "<f8", "<i4", "|b1"
  std::vector<int64_t> shape;     // dimensions
  bool fortran_order = false;
};
std::vector<uint8_t> npy_encode(const NpyArray& a);
NpyArray npy_decode(const std::vector<uint8_t>& data);

}  // namespace cd
}  // namespace laila_c

#endif  // LAILA_COMPDATA_HPP
