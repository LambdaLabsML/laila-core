// Transformations (base64/zlib/encryption/serializers) + the recognized-snippet
// registry. laila stores inverse "backward_code" strings inside a
// SimpleConstitution and exec()s them on read. C cannot exec Python, so we keep
// the strings verbatim (wire-identical) and dispatch known ones to native ops.
// An unrecognized snippet on a target -> Status::Unsupported.
#ifndef LAILA_TRANSFORMATION_HPP
#define LAILA_TRANSFORMATION_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "laila/value.hpp"

namespace laila_c {

// The recovery codes laila stores in a SimpleConstitution are executable Python
// `def backward(inp): ...` source strings. laila-C emits these EXACT strings
// (byte-identical to laila's, default kwargs) so an unmodified Python peer can
// `exec` them, and RECOGNIZES them on read by semantic signature (library + op)
// in apply_backward, dispatching to the native C++ inverse.
namespace codes {
inline constexpr const char* PY_BASE64 =
    "def backward(inp):\n"
    "    import base64\n"
    "    kwargs = {}\n"
    "    if isinstance(inp, memoryview):\n"
    "        inp = inp.tobytes()\n"
    "    return base64.b64decode(inp, **kwargs)\n";
inline constexpr const char* PY_MSGPACK =
    "\ndef backward(inp):\n"
    "    import msgpack\n"
    "    kwargs = {\"raw\": False, **{}}\n"
    "    return msgpack.unpackb(inp, **kwargs)\n";
inline constexpr const char* PY_PICKLE =
    "\ndef backward(inp):\n"
    "    import pickle\n"
    "    kwargs = {}\n"
    "    return pickle.loads(inp, **kwargs)\n";
inline constexpr const char* PY_NUMPY =
    "\ndef backward(inp):\n"
    "    import io\n"
    "    import numpy as np\n"
    "    buf = io.BytesIO(inp)\n"
    "    kwargs = {'allow_pickle': False, **{}}\n"
    "    return np.load(buf, **kwargs)\n";
inline constexpr const char* PY_TORCH =
    "\ndef backward(inp):\n"
    "    import io\n"
    "    import torch\n"
    "    buf = io.BytesIO(inp)\n"
    "    kwargs = {}\n"
    "    return torch.load(buf, **kwargs)\n";
}  // namespace codes

// A single reversible transformation over opaque data (mirrors
// _data_transformation in compdata/transformation/base.py): forward() applies on
// the way out, backward() is its inverse, and backward_code() is the Python
// `def backward(inp): ...` snippet stored in the constitution for the read side.
class Transformation {
public:
  virtual ~Transformation() = default;
  virtual const std::string& name() const = 0;
  virtual LailaValue forward(const LailaValue& in) const = 0;
  virtual LailaValue backward(const LailaValue& in) const = 0;
  virtual std::string backward_code() const = 0;  // snippet stored on serialize
};

// Ordered pipeline. forward() applies left-to-right and returns the transformed
// value plus the inverse-code list already reversed for replay (mirrors
// TransformationSequence.forward in compdata/transformation/base.py).
class TransformationSequence {
public:
  void append(std::shared_ptr<Transformation> t) { items_.push_back(std::move(t)); }
  bool empty() const { return items_.empty(); }
  std::pair<LailaValue, std::vector<std::string>> forward(const LailaValue& data) const;

  static TransformationSequence base64();  // common pool default

private:
  std::vector<std::shared_ptr<Transformation>> items_;
};

// Base64 helpers (used by serializers and the FilesystemPool transform).
std::string base64_encode(const std::vector<uint8_t>& data);
std::vector<uint8_t> base64_decode(const std::string& text);

// The recognized-snippet registry: code string -> native inverse op.
using BackwardFn = std::function<LailaValue(const LailaValue&)>;
void register_backward(const std::string& code, BackwardFn fn);
bool has_backward(const std::string& code);
// Applies the inverse op for `code` to `in`; raises Status::Unsupported if the
// snippet is not recognized on this target.
LailaValue apply_backward(const std::string& code, const LailaValue& in);

}  // namespace laila_c

#endif  // LAILA_TRANSFORMATION_HPP
