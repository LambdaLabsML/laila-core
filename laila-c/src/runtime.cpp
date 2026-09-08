#include "laila/runtime.hpp"

#include "laila/policy.hpp"
#include "laila/status.hpp"

namespace laila_c {
namespace runtime {

FuturePtr resolve(const FuturePtr& f) { return f; }

FuturePtr resolve(const std::string& global_id) {
  for (const auto& kv : local_policies()) {
    auto& bank = kv.second->future_bank();
    auto it = bank.find(global_id);
    if (it != bank.end()) return it->second;
  }
  raise(Status::NotFound, "Future " + global_id + " not found in any local policy bank");
}

FutureStatus status(const FuturePtr& f) { return f->status(); }
FutureStatus status(const std::string& global_id) { return resolve(global_id)->status(); }
EntryPtr result(const FuturePtr& f) { return f->result(); }
EntryPtr result(const std::string& global_id) { return resolve(global_id)->result(); }
EntryPtr wait(const FuturePtr& f, int64_t timeout_ms) { f->wait(timeout_ms); return f->result(); }
EntryPtr wait(const std::string& global_id, int64_t timeout_ms) {
  auto f = resolve(global_id);
  f->wait(timeout_ms);
  return f->result();
}

}  // namespace runtime
}  // namespace laila_c
