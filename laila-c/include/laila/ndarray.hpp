// Thin, dependency-free numpy array helper. A "numpy value" in laila-C is just
// (raw bytes + dtype + shape) -- exactly what LailaValue::numpy stores and what
// cd_npy.cpp writes byte-identically to numpy's .npy container. This header lets
// users build one from a typed std::vector with the dtype INFERRED from the C++
// element type, so `laila->constant(ndarray<float>(buf, {3, 224, 224}))` works
// and cross-loads into Python as a real np.ndarray.
//
// No real numpy dependency: the descr tokens match numpy.dtype.str (e.g. "<f4").
// A torch equivalent is intentionally deferred to a later round.
#ifndef LAILA_NDARRAY_HPP
#define LAILA_NDARRAY_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace laila_c {

// numpy dtype descr token for a C++ element type (numpy.dtype.str form). The
// byteorder char is '<' (little-endian) for multibyte types and '|' (n/a) for
// single-byte types -- correct on every current target (x86/arm posix, esp32,
// rp2040, stm32). A big-endian target would need '>' tokens (follow-up).
//
// The primary template is left undefined on purpose: an unsupported element
// type fails to compile here, which is the array-element guard.
template <class T>
struct numpy_descr;
template <> struct numpy_descr<float>    { static constexpr const char* v = "<f4"; };
template <> struct numpy_descr<double>   { static constexpr const char* v = "<f8"; };
template <> struct numpy_descr<int8_t>   { static constexpr const char* v = "|i1"; };
template <> struct numpy_descr<uint8_t>  { static constexpr const char* v = "|u1"; };
template <> struct numpy_descr<int16_t>  { static constexpr const char* v = "<i2"; };
template <> struct numpy_descr<uint16_t> { static constexpr const char* v = "<u2"; };
template <> struct numpy_descr<int32_t>  { static constexpr const char* v = "<i4"; };
template <> struct numpy_descr<uint32_t> { static constexpr const char* v = "<u4"; };
template <> struct numpy_descr<int64_t>  { static constexpr const char* v = "<i8"; };
template <> struct numpy_descr<uint64_t> { static constexpr const char* v = "<u8"; };

// A numpy array payload: C-order (row-major) bytes + numpy descr + shape.
struct NdArray {
  std::vector<uint8_t> raw;
  std::string dtype;
  std::vector<int64_t> shape;
};

// Build an NdArray from a typed buffer, inferring the dtype from T. The data is
// copied verbatim (host little-endian byte layout, which is what .npy expects).
// Note: do NOT use std::vector<bool> here (it is bit-packed); use uint8_t.
template <class T>
NdArray ndarray(const std::vector<T>& data, std::vector<int64_t> shape) {
  NdArray a;
  a.dtype = numpy_descr<T>::v;
  a.shape = std::move(shape);
  a.raw.resize(data.size() * sizeof(T));
  if (!data.empty()) std::memcpy(a.raw.data(), data.data(), a.raw.size());
  return a;
}

}  // namespace laila_c

#endif  // LAILA_NDARRAY_HPP
