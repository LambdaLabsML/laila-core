// Verb option structs (the kwargs->*Opts mirror of laila's memorize/remember/
// forget signatures). Extracted into their own header so both the facade
// (laila.hpp) and types that take them (e.g. Manifest) can share them without an
// include cycle.
#ifndef LAILA_OPTS_HPP
#define LAILA_OPTS_HPP

#include <cstdint>
#include <optional>
#include <string>

namespace laila_c {

// Mirrors laila's src/dst/relay verb signatures. src_* names the policy the data
// comes FROM (default: active); dst_* names where it goes / is read. Back-compat
// aliases fold in: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
struct MemorizeOpts {
  std::optional<std::string> src_policy;
  std::optional<std::string> src_pool;
  std::optional<std::string> dst_policy;
  std::optional<std::string> dst_pool;
  std::optional<std::string> comm;
  std::optional<std::string> policy_id;      // alias -> dst_policy
  std::optional<std::string> pool_nickname;  // alias -> dst_pool
  std::optional<std::string> pool_id;        // alias -> dst_pool
};

struct RememberOpts {
  bool persist = true;
  std::optional<std::string> nickname;       // entry nickname -> derived global_id
  std::optional<int64_t> evolution;
  std::optional<std::string> src_policy;
  std::optional<std::string> src_pool;
  std::optional<std::string> dst_policy;
  std::optional<std::string> dst_pool;
  std::optional<std::string> comm;
  std::optional<std::string> policy_id;      // alias -> dst_policy
  std::optional<std::string> pool_nickname;  // alias -> dst_pool
  std::optional<std::string> pool_id;        // alias -> dst_pool
};

struct ForgetOpts {
  std::optional<std::string> policy;         // policy to delete from (default: active)
  std::optional<std::string> pool;
  std::optional<std::string> comm;
  std::optional<std::string> nickname;
  std::optional<int64_t> evolution;
  std::optional<std::string> policy_id;      // alias -> policy
  std::optional<std::string> pool_nickname;  // alias -> pool
  std::optional<std::string> pool_id;        // alias -> pool
};

}  // namespace laila_c

#endif  // LAILA_OPTS_HPP
