// runtime mirror (runtime/__init__.py): policy-agnostic future introspection.
// Accepts a Future or a raw global_id string; resolves across local policies.
#ifndef LAILA_RUNTIME_HPP
#define LAILA_RUNTIME_HPP

#include <string>

#include "laila/future.hpp"

namespace laila_c {
namespace runtime {

FuturePtr resolve(const FuturePtr& f);
FuturePtr resolve(const std::string& global_id);

FutureStatus status(const FuturePtr& f);
FutureStatus status(const std::string& global_id);
EntryPtr result(const FuturePtr& f);
EntryPtr result(const std::string& global_id);
EntryPtr wait(const FuturePtr& f, int64_t timeout_ms = -1);
EntryPtr wait(const std::string& global_id, int64_t timeout_ms = -1);

}  // namespace runtime
}  // namespace laila_c

#endif  // LAILA_RUNTIME_HPP
