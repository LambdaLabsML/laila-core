// Logger mirror (policy.md): a process-wide singleton that lives OUTSIDE
// policies and explicitly NOT under central.communication. Emits structured
// JSON records. Persist-as-Entry is OFF by default on embedded targets.
#ifndef LAILA_LOGGER_HPP
#define LAILA_LOGGER_HPP

#include <string>

namespace laila_c {

class Logger {
public:
  static Logger& get();  // singleton

  void info(const std::string& event, const std::string& detail = "");
  void error(const std::string& event, const std::string& detail = "");

  void set_enabled(bool e) { enabled_ = e; }
  // Persist records as Entries via laila.memorize (default off on embedded).
  void set_persist_pool_nickname(const std::string& nickname) { persist_pool_ = nickname; }

private:
  Logger() = default;
  void emit(const char* level, const std::string& event, const std::string& detail);
  bool enabled_ = true;
  std::string persist_pool_;
};

}  // namespace laila_c

#endif  // LAILA_LOGGER_HPP
