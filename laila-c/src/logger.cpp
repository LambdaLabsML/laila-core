#include "laila/logger.hpp"

#include <cstdio>

#include "laila/hal/hal.hpp"
#include "laila/json.hpp"

namespace laila_c {

Logger& Logger::get() {
  static Logger instance;  // process-wide singleton (policy.md)
  return instance;
}

void Logger::emit(const char* level, const std::string& event, const std::string& detail) {
  if (!enabled_) return;
  Json o = Json::object();
  o["ts_ms"] = (int64_t)hal::get().clock().now_ms();
  o["level"] = std::string(level);
  o["event"] = event;
  if (!detail.empty()) o["detail"] = detail;
  std::fprintf(stderr, "%s\n", o.dump().c_str());
}

void Logger::info(const std::string& event, const std::string& detail) { emit("INFO", event, detail); }
void Logger::error(const std::string& event, const std::string& detail) { emit("ERROR", event, detail); }

}  // namespace laila_c
