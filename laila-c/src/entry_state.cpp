#include "laila/entry_state.hpp"

#include "laila/status.hpp"

namespace laila_c {

const char* entry_state_name(EntryState s) {
  switch (s) {
    case EntryState::READY: return "READY";
    case EntryState::STAGED: return "STAGED";
    case EntryState::POOLING: return "POOLING";
    case EntryState::POOLED: return "POOLED";
    case EntryState::STALE: return "STALE";
    case EntryState::NA: return "NA";
  }
  return "STAGED";
}

EntryState entry_state_from_name(const std::string& name) {
  if (name == "READY") return EntryState::READY;
  if (name == "STAGED") return EntryState::STAGED;
  if (name == "POOLING") return EntryState::POOLING;
  if (name == "POOLED") return EntryState::POOLED;
  if (name == "STALE") return EntryState::STALE;
  if (name == "NA") return EntryState::NA;
  return EntryState::STAGED;
}

}  // namespace laila_c
