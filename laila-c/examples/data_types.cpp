// Mirrors laila's README example. Compare to the Python:
//
//     import laila
//     dict_entry = laila.constant(data={"key": [1, 2, 3]})
//     laila.memorize(dict_entry)
//     laila.remember(dict_entry.global_id).data    # type-free!
//
// Same shape in laila-C (only `.`->`->` and kwargs->opts differ):
#include <cstdio>

#include "laila/laila.hpp"

using namespace laila_c;

int main() {
  Json arr = Json::array();
  arr.push_back(Json((int64_t)1));
  arr.push_back(Json((int64_t)2));
  arr.push_back(Json((int64_t)3));
  Json d = Json::object();
  d["key"] = arr;

  auto dict_entry = laila->constant(d);  // laila.constant(...) -- Json infers automatically
  laila->memorize(dict_entry)->wait();

  LailaValue got = laila->remember(dict_entry->global_id())->data();
  std::printf("remembered: %s\n", got.as_json().dump().c_str());

  // A string entry round-trips through the same three verbs.
  auto str_entry = laila->constant("hello laila-C");  // const char* infers to a string payload
  laila->memorize(str_entry)->wait();
  std::printf("remembered: %s\n", laila->remember(str_entry->global_id())->data().as_string().c_str());

  // A numpy array: dtype is inferred from the C++ element type and cross-loads
  // into Python as a real np.ndarray.
  auto np_entry = laila->constant(ndarray<float>({1.0f, 2.0f, 3.0f, 4.0f}, {2, 2}));
  laila->memorize(np_entry)->wait();
  LailaValue np = laila->remember(np_entry->global_id())->data();
  std::printf("remembered ndarray: dtype=%s rank=%zu\n", np.dtype().c_str(), np.shape().size());

  laila->terminate();
  return 0;
}
