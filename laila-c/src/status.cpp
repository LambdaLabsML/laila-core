#include "laila/status.hpp"

#include <cstdio>
#include <cstdlib>

namespace laila_c {

const char* status_str(Status s) {
  switch (s) {
    case Status::Ok: return "Ok";
    case Status::Error: return "Error";
    case Status::NotFound: return "NotFound";
    case Status::NotBuilt: return "NotBuilt";
    case Status::Unsupported: return "Unsupported";
    case Status::Timeout: return "Timeout";
    case Status::Cancelled: return "Cancelled";
  }
  return "Unknown";
}

void raise(Status code, const std::string& msg) {
#if defined(LAILA_NO_EXCEPTIONS)
  std::fprintf(stderr, "[laila:%s] %s\n", status_str(code), msg.c_str());
  std::abort();
#else
  switch (code) {
    case Status::Unsupported: throw UnsupportedError(msg);
    case Status::NotBuilt: throw EntryNotBuiltError(msg);
    default: throw LailaError(code, msg);
  }
#endif
}

}  // namespace laila_c
