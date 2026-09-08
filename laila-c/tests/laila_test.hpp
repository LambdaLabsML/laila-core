// Tiny self-registering test framework for laila-C. Dependency-free so the same
// suites build on every platform backend (host, baremetal, MCU). Tests live in
// suite_*.cpp via TEST(suite, name){...}; assertions use CHECK*.
#ifndef LAILA_TEST_HPP
#define LAILA_TEST_HPP

#include <cmath>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "laila/laila.hpp"
#include "laila/manifest.hpp"
#include "laila/pools.hpp"

namespace lt {

struct Case {
  std::string suite;
  std::string name;
  std::function<void()> fn;
};

std::vector<Case>& registry();
int register_case(const std::string& suite, const std::string& name, std::function<void()> fn);

// Counters (defined in laila_test.cpp).
extern long g_total_checks;
extern long g_total_fail;
extern long g_case_fail;

void check(bool cond, const char* expr, const char* file, int line);

// A small deterministic PRNG so suites generate diverse-but-reproducible data
// without depending on the platform RNG.
struct Rng {
  uint64_t s;
  explicit Rng(uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
  uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
  uint32_t u32() { return (uint32_t)(next() & 0xFFFFFFFFu); }
  int range(int lo, int hi) { return lo + (int)(u32() % (uint32_t)(hi - lo + 1)); }
};

}  // namespace lt

// Value equality helper shared across suites.
inline bool value_eq(const laila_c::LailaValue& a, const laila_c::LailaValue& b) {
  using K = laila_c::LailaValue::Kind;
  if (a.kind() != b.kind()) return false;
  switch (a.kind()) {
    case K::None: return true;
    case K::Bool: return a.as_bool() == b.as_bool();
    case K::Int: return a.as_int() == b.as_int();
    case K::Double: return a.as_double() == b.as_double();
    case K::String: return a.as_string() == b.as_string();
    case K::Bytes: return a.as_bytes() == b.as_bytes();
    case K::Json: return a.as_json().dump() == b.as_json().dump();
    case K::Tensor:
      return a.as_bytes() == b.as_bytes() && a.dtype() == b.dtype() && a.shape() == b.shape();
  }
  return false;
}

#define LT_CAT2(a, b) a##b
#define LT_CAT(a, b) LT_CAT2(a, b)
#define TEST(suite_str, name_str) LT_TEST_IMPL(suite_str, name_str, __COUNTER__)
#define LT_TEST_IMPL(suite_str, name_str, ctr)                                          \
  static void LT_CAT(lt_fn_, ctr)();                                                    \
  static int LT_CAT(lt_reg_, ctr) =                                                     \
      ::lt::register_case(suite_str, name_str, &LT_CAT(lt_fn_, ctr));                   \
  static void LT_CAT(lt_fn_, ctr)()

#define CHECK(cond) ::lt::check((cond), #cond, __FILE__, __LINE__)
#define CHECK_EQ(a, b) ::lt::check((a) == (b), #a " == " #b, __FILE__, __LINE__)
#define CHECK_NEAR(a, b, eps) \
  ::lt::check(std::fabs((double)(a) - (double)(b)) <= (eps), #a " ~= " #b, __FILE__, __LINE__)
#define CHECK_THROWS(stmt, Exc)                                              \
  do {                                                                       \
    bool lt_threw = false;                                                   \
    try {                                                                    \
      stmt;                                                                  \
    } catch (const Exc&) {                                                   \
      lt_threw = true;                                                       \
    }                                                                        \
    ::lt::check(lt_threw, "throws " #Exc ": " #stmt, __FILE__, __LINE__);    \
  } while (0)

#endif  // LAILA_TEST_HPP
