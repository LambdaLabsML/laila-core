#include "laila/computational_data.hpp"

namespace laila_c {

// Mirror of register_cdtype + ComputationalData.__new__: pick the wrapper for
// the payload. Python keys on type(data); the type-free C value keys on Kind
// (and, for tensors, the numpy-vs-torch framework tag). CD_generic is the final
// fallback, exactly like the @register_cdtype(object) catch-all.
ComputationalDataPtr ComputationalData::wrap(const LailaValue& data) {
  switch (data.kind()) {
    case LailaValue::Kind::Json:
      if (data.as_json().is_array()) return std::make_shared<CD_list>(data);
      return std::make_shared<CD_dict>(data);
    case LailaValue::Kind::Tensor:
      if (data.tensor_framework() == LailaValue::TensorFramework::Torch)
        return std::make_shared<CD_torchtensor>(data);
      return std::make_shared<CD_numpyarray>(data);
    default:
      return std::make_shared<CD_generic>(data);
  }
}

}  // namespace laila_c
