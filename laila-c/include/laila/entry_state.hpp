// EntryState mirror (entry/entry_state.py): lifecycle of an Entry's payload.
#ifndef LAILA_ENTRY_STATE_HPP
#define LAILA_ENTRY_STATE_HPP

#include <string>

namespace laila_c {

enum class EntryState {
  READY,    // payload materialized
  STAGED,   // has a constitution; must be built before .data
  POOLING,  // in flight to a pool
  POOLED,   // persisted
  STALE,
  NA,       // payload-less subclasses (e.g. Manifest)
};

const char* entry_state_name(EntryState s);
EntryState entry_state_from_name(const std::string& name);

}  // namespace laila_c

#endif  // LAILA_ENTRY_STATE_HPP
