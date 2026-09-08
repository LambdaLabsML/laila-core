// ComputationalData taxonomy (compdata/taxonomy/*.py): the type-dispatched
// payload wrapper. Every Entry payload is wrapped in a ComputationalData (or one
// of its registered subclasses) so the system has a uniform serialize() handle
// regardless of the underlying value type. The wrapper owns a default serializer
// and serialize() returns (serialized_bytes, backward_code) -- the read side
// rebuilds via the constitution without knowing which serializer was used.
//
// Type dispatch
// -------------
// Python decorates subclasses with @register_cdtype(*types) and selects the
// wrapper by type(data) (walking the MRO, CD_generic as the final fallback).
// The C value model is type-free, so ComputationalData::wrap dispatches on
// LailaValue::Kind (+ the tensor framework tag): dict/list -> CD_dict/CD_list,
// numpy tensor -> CD_numpyarray, torch tensor -> CD_torchtensor, everything else
// -> CD_generic.
#ifndef LAILA_COMPUTATIONAL_DATA_HPP
#define LAILA_COMPUTATIONAL_DATA_HPP

#include <memory>
#include <utility>
#include <vector>

#include "laila/serialization.hpp"
#include "laila/value.hpp"

namespace laila_c {

// Base ComputationalData (taxonomy/compdata.py).
class ComputationalData {
public:
  virtual ~ComputationalData() = default;
  explicit ComputationalData(LailaValue data) : data_(std::move(data)) {}

  // The unwrapped payload.
  const LailaValue& data() const { return data_; }

  // The serializer used by serialize(). Subclasses override with their bespoke
  // serializer (CDNumpy -> NumpySerializer, CDTorch -> TorchSerializer, ...).
  virtual const Transformation& serializer() const = 0;

  // serialize() -> (serialized_bytes, backward_code), exactly like Python's
  // ComputationalData.serialize() == (serializer.forward(data), backward_code).
  std::pair<std::vector<uint8_t>, std::string> serialize() const {
    const Transformation& s = serializer();
    return {s.forward(data_).as_bytes(), s.backward_code()};
  }

  // Dispatch ComputationalData(data) to the registered subclass for the payload
  // (the C mirror of register_cdtype + ComputationalData.__new__).
  static std::shared_ptr<ComputationalData> wrap(const LailaValue& data);

protected:
  LailaValue data_;
};
using ComputationalDataPtr = std::shared_ptr<ComputationalData>;

// CD_numpyarray (taxonomy/cd_numpy.py): numpy.ndarray -> NumpySerializer (.npy).
class CD_numpyarray : public ComputationalData {
public:
  using ComputationalData::ComputationalData;
  const Transformation& serializer() const override { static NumpySerializer s; return s; }
};

// CD_torchtensor (taxonomy/cd_torch.py): torch.Tensor -> TorchSerializer
// (torch.save), which is LAILA_UNSUPPORTED on this target.
class CD_torchtensor : public ComputationalData {
public:
  using ComputationalData::ComputationalData;
  const Transformation& serializer() const override { static TorchSerializer s; return s; }
};

// CD_dict (taxonomy/cd_dict.py): dict -> MsgpackSerializer.
class CD_dict : public ComputationalData {
public:
  using ComputationalData::ComputationalData;
  const Transformation& serializer() const override { static MsgpackSerializer s; return s; }
};

// CD_list (taxonomy/cd_list.py): list / tuple -> MsgpackSerializer.
class CD_list : public ComputationalData {
public:
  using ComputationalData::ComputationalData;
  const Transformation& serializer() const override { static MsgpackSerializer s; return s; }
};

// CD_generic (taxonomy/cd_object.py): catch-all -> PickleSerializer.
class CD_generic : public ComputationalData {
public:
  using ComputationalData::ComputationalData;
  const Transformation& serializer() const override { static PickleSerializer s; return s; }
};

}  // namespace laila_c

#endif  // LAILA_COMPUTATIONAL_DATA_HPP
