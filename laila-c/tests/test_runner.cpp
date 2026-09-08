// Test runner: discovers all registered cases and runs them (optionally
// filtered by --suite). Prints a per-suite summary and the platform name, and
// enforces a minimum assertion count for the full run (the suite is designed to
// hold well over 1000 diverse assertions).
#include <cstring>
#include <map>
#include <string>

#include "laila/hal/hal.hpp"
#include "laila_test.hpp"

int main(int argc, char** argv) {
  const char* platform = laila_c::hal::get().platform_name();
  std::string suite_filter;
  bool list_only = false;
  long min_checks = 1000;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--list") list_only = true;
    else if (a == "--suite" && i + 1 < argc) suite_filter = argv[++i];
    else if (a == "--min" && i + 1 < argc) min_checks = std::atol(argv[++i]);
  }

  auto& cases = lt::registry();
  if (list_only) {
    for (auto& c : cases) std::printf("%s/%s\n", c.suite.c_str(), c.name.c_str());
    return 0;
  }

  std::printf("== laila-C test suite (platform: %s) ==\n", platform);
  if (!suite_filter.empty()) std::printf("   suite filter: %s\n", suite_filter.c_str());

  std::map<std::string, int> suite_cases, suite_fail;
  int ran = 0, failed_cases = 0;
  for (auto& c : cases) {
    if (!suite_filter.empty() && c.suite != suite_filter) continue;
    long before_fail = lt::g_total_fail;
    lt::g_case_fail = 0;
    c.fn();
    ++ran;
    ++suite_cases[c.suite];
    if (lt::g_total_fail > before_fail) {
      ++failed_cases;
      ++suite_fail[c.suite];
      std::printf("  [FAIL] %s/%s (%ld failed checks)\n", c.suite.c_str(), c.name.c_str(),
                  lt::g_total_fail - before_fail);
    }
  }

  std::printf("\n--- per-suite ---\n");
  for (auto& kv : suite_cases) {
    std::printf("  %-14s cases=%-4d failed=%d\n", kv.first.c_str(), kv.second,
                suite_fail[kv.first]);
  }

  bool enforce_min = suite_filter.empty();
  std::printf("\nplatform=%s cases=%d (failed=%d) assertions=%ld (failed=%ld)\n", platform, ran,
              failed_cases, lt::g_total_checks, lt::g_total_fail);

  if (enforce_min && lt::g_total_checks < min_checks) {
    std::printf("ERROR: only %ld assertions ran; require >= %ld\n", lt::g_total_checks, min_checks);
    return 2;
  }
  return lt::g_total_fail == 0 ? 0 : 1;
}
