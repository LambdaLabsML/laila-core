// LailaValue: the type-free payload. In Python an Entry holds arbitrary objects;
// in C/C++ "type-free" becomes an opaque byte buffer + a type descriptor. The
// translation step resolves concrete C types; the descriptor preserves enough
// to round-trip through pools and across languages (dtype/shape for tensors).
#ifndef LAILA_VALUE_HPP
#define LAILA_VALUE_HPP

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

#include "laila/json.hpp"
#include "laila/ndarray.hpp"

namespace laila_c {

class LailaValue {
public:
  enum class Kind {
    None,
    Bool,
    Int,
    Double,
    String,
    Bytes,
    Json,    // structured dict/list (JSON-representable: matches Python dict/list)
    Tensor,  // opaque numeric buffer + dtype + shape
  };

  // Which numeric framework a Tensor came from. Python dispatches the wrapper by
  // type(data) (np.ndarray -> CD_numpyarray, torch.Tensor -> CD_torchtensor);
  // the C value model is type-free, so a tensor carries this tag to drive the
  // same ComputationalData dispatch.
  enum class TensorFramework { Numpy, Torch };

  LailaValue() : kind_(Kind::None) {}

  // Implicit converters: mirror Python's auto type-inference at the call site so
  // laila->constant(x) works for native C++ payloads (the from_* factories
  // remain for explicit use). Single-step only; numeric arrays use ndarray<T>().
  LailaValue(bool b);
  LailaValue(int i);
  LailaValue(int64_t i);
  LailaValue(double d);
  LailaValue(const char* s);
  LailaValue(const std::string& s);
  LailaValue(std::vector<uint8_t> b);
  LailaValue(Json j);
  LailaValue(const NdArray& a);  // numpy: -> numpy(a.raw, a.dtype, a.shape)

  // Any other payload type is rejected at compile time with a clear message.
  // The constraint excludes every supported type AND LailaValue itself, so this
  // catch-all never hijacks copy/move or a supported lvalue overload; it is only
  // instantiated for genuinely unsupported types, where the static_assert fires.
  template <class T, class D = std::decay_t<T>,
            std::enable_if_t<
                !std::is_same_v<D, LailaValue> && !std::is_same_v<D, bool> &&
                    !std::is_integral_v<D> && !std::is_floating_point_v<D> &&
                    !std::is_same_v<D, std::string> && !std::is_same_v<D, const char*> &&
                    !std::is_same_v<D, char*> && !std::is_same_v<D, std::vector<uint8_t>> &&
                    !std::is_same_v<D, Json> && !std::is_same_v<D, NdArray>,
                int> = 0>
  LailaValue(T&&) {
    static_assert(sizeof(T) == 0,
                  "laila->constant(x): unsupported payload type. Supported: bool, integral "
                  "and floating-point scalars, const char*/std::string, std::vector<uint8_t> "
                  "(bytes), Json (dict/list), and NdArray. For numeric arrays use ndarray<T>(...).");
  }

  static LailaValue none() { return LailaValue(); }
  static LailaValue from_bool(bool b);
  static LailaValue from_int(int64_t i);
  static LailaValue from_double(double d);
  static LailaValue from_string(const std::string& s);
  static LailaValue from_bytes(std::vector<uint8_t> b);
  static LailaValue from_json(Json j);
  // numpy(...) is the canonical numeric-array constructor (np.ndarray payload);
  // torch_tensor(...) marks a torch.Tensor payload (serialized via torch.save,
  // which is LAILA_UNSUPPORTED on this target). tensor(...) is a back-compat
  // alias for numpy(...).
  static LailaValue numpy(std::vector<uint8_t> raw, const std::string& dtype,
                          std::vector<int64_t> shape);
  static LailaValue torch_tensor(std::vector<uint8_t> raw, const std::string& dtype,
                                 std::vector<int64_t> shape);
  static LailaValue tensor(std::vector<uint8_t> raw, const std::string& dtype,
                           std::vector<int64_t> shape);

  Kind kind() const { return kind_; }
  TensorFramework tensor_framework() const { return tensor_framework_; }
  bool is_none() const { return kind_ == Kind::None; }

  bool as_bool() const { return b_; }
  int64_t as_int() const { return i_; }
  double as_double() const { return d_; }
  const std::string& as_string() const { return s_; }
  const std::vector<uint8_t>& as_bytes() const { return bytes_; }
  const Json& as_json() const { return json_; }
  const std::string& dtype() const { return dtype_; }
  const std::vector<int64_t>& shape() const { return shape_; }

  // Type-dispatched serialization mirroring ComputationalData.serialize():
  // returns (serialized_bytes, python_backward_code). dict/list -> msgpack,
  // ndarray -> numpy .npy, scalars/str/bytes -> pickle.
  std::pair<std::vector<uint8_t>, std::string> serialize() const;

  // Embed the raw (untransformed) value into a JSON node for as_dict().
  Json to_json_payload() const;
  static LailaValue from_json_payload(const Json& node);

  // laila-native (untagged) payload for the cross-language RPC wire: a primitive
  // is embedded as the bare JSON value (exactly as Python's Entry.as_dict does),
  // so the (unmodifiable) Python `laila` can read it via Entry.from_dict. Bytes
  // and tensors fall back to the tagged form (not JSON-native cross-language).
  Json to_wire_payload() const;

private:
  Kind kind_;
  bool b_ = false;
  int64_t i_ = 0;
  double d_ = 0.0;
  std::string s_;
  std::vector<uint8_t> bytes_;
  Json json_;
  std::string dtype_;
  std::vector<int64_t> shape_;
  TensorFramework tensor_framework_ = TensorFramework::Numpy;
};

}  // namespace laila_c

#endif  // LAILA_VALUE_HPP
