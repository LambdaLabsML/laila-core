// Serializer transformations (compdata/transformation/serialization/*.py).
//
// Each serializer is a Transformation whose forward() turns a payload into bytes
// and whose backward() does the inverse; backward_code() is the byte-identical
// Python `def backward(inp): ...` snippet laila stores in the constitution. They
// are the first step of a pool's transformation pipeline (encoding / compression
// / encryption stack on top of the resulting bytes).
//
//   PickleSerializer   catch-all (None/bool/int/float/str/bytes)   -> pickle
//   MsgpackSerializer  dict / list payloads                        -> msgpack
//   NumpySerializer    numpy.ndarray (preserves dtype/shape/order)  -> .npy
//   TorchSerializer    torch.Tensor (torch.save) -- LAILA_UNSUPPORTED here
#ifndef LAILA_SERIALIZATION_HPP
#define LAILA_SERIALIZATION_HPP

#include <string>

#include "laila/transformation.hpp"
#include "laila/value.hpp"

namespace laila_c {

// PickleSerializer (serialization/pickle.py): reversible pickle for arbitrary
// objects. forward() pickles the payload; backward() unpickles.
class PickleSerializer : public Transformation {
public:
  const std::string& name() const override;
  LailaValue forward(const LailaValue& inp) const override;
  LailaValue backward(const LailaValue& inp) const override;
  std::string backward_code() const override;
};

// MsgpackSerializer (serialization/msgpack.py): compact, language-agnostic
// encoding for dict / list payloads.
class MsgpackSerializer : public Transformation {
public:
  const std::string& name() const override;
  LailaValue forward(const LailaValue& inp) const override;
  LailaValue backward(const LailaValue& inp) const override;
  std::string backward_code() const override;
};

// NumpySerializer (serialization/numpy.py): np.save / np.load, preserving dtype,
// shape, and byte order.
class NumpySerializer : public Transformation {
public:
  const std::string& name() const override;
  LailaValue forward(const LailaValue& inp) const override;
  LailaValue backward(const LailaValue& inp) const override;
  std::string backward_code() const override;
};

// TorchSerializer (serialization/torch.py): torch.save / torch.load. There is no
// native torch.save on a generic C target, so forward()/backward() raise
// Status::Unsupported (LAILA_UNSUPPORTED) -- the translation hard-error rule.
class TorchSerializer : public Transformation {
public:
  const std::string& name() const override;
  LailaValue forward(const LailaValue& inp) const override;
  LailaValue backward(const LailaValue& inp) const override;
  std::string backward_code() const override;
};

}  // namespace laila_c

#endif  // LAILA_SERIALIZATION_HPP
