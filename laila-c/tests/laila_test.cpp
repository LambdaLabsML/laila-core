#include "laila_test.hpp"

namespace lt {

std::vector<Case>& registry() {
  static std::vector<Case> r;
  return r;
}

int register_case(const std::string& suite, const std::string& name, std::function<void()> fn) {
  registry().push_back({suite, name, std::move(fn)});
  return 0;
}

long g_total_checks = 0;
long g_total_fail = 0;
long g_case_fail = 0;

void check(bool cond, const char* expr, const char* file, int line) {
  ++g_total_checks;
  if (!cond) {
    ++g_total_fail;
    ++g_case_fail;
    std::printf("    FAIL [%s:%d]: %s\n", file, line, expr);
  }
}

}  // namespace lt
