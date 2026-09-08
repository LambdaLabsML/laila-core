// Error model. laila-C mirrors Python exceptions, but on embedded targets
// exceptions may be disabled (LAILA_NO_EXCEPTIONS): every throwing API has a
// status-code path, and LAILA_UNSUPPORTED is the canonical "this target cannot
// honor this capability" signal demanded by the translation contract.
#ifndef LAILA_STATUS_HPP
#define LAILA_STATUS_HPP

#include <stdexcept>
#include <string>

namespace laila_c {

enum class Status {
  Ok = 0,
  Error,           // generic failure (maps to RuntimeError/ValueError)
  NotFound,        // missing key/entry/future (KeyError)
  NotBuilt,        // EntryNotBuiltError
  Unsupported,     // LAILA_UNSUPPORTED: capability absent on this target
  Timeout,
  Cancelled,
};

const char* status_str(Status s);

// Base laila exception (only thrown when exceptions are enabled).
class LailaError : public std::runtime_error {
public:
  LailaError(Status code, const std::string& msg)
      : std::runtime_error(msg), code_(code) {}
  Status code() const { return code_; }
private:
  Status code_;
};

class UnsupportedError : public LailaError {
public:
  explicit UnsupportedError(const std::string& msg)
      : LailaError(Status::Unsupported, msg) {}
};

class EntryNotBuiltError : public LailaError {
public:
  explicit EntryNotBuiltError(const std::string& msg)
      : LailaError(Status::NotBuilt, msg) {}
};

// Raise (or, under LAILA_NO_EXCEPTIONS, abort with a recorded last-error).
[[noreturn]] void raise(Status code, const std::string& msg);
[[noreturn]] inline void raise_unsupported(const std::string& msg) {
  raise(Status::Unsupported, msg);
}

}  // namespace laila_c

#endif  // LAILA_STATUS_HPP
