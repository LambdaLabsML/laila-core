// _LAILA_IDENTIFIABLE_OBJECT mirror: (uuid, scopes, evolution) -> global_id.
// Encoding matches laila exactly: LAILA:scope1:...:scopeN:GLOBAL_ID:<uuid>[-<evo>]
// (see basics/definitions/identifiable_object.py and macros/strings.py).
#ifndef LAILA_IDENTITY_HPP
#define LAILA_IDENTITY_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace laila_c {

// Scope-name constants (macros/strings.py). Renaming is a wire-format break.
namespace scope {
inline constexpr const char* OBJECT = "OBJECT";
inline constexpr const char* ENTRY = "ENTRY";
inline constexpr const char* FUTURE = "FUTURE";
inline constexpr const char* GROUP_FUTURE = "GROUP_FUTURE";
inline constexpr const char* COMPLEX_FUTURE = "COMPLEX_FUTURE";
inline constexpr const char* POOL = "POOL";
inline constexpr const char* POLICY = "POLICY";
inline constexpr const char* MANIFEST = "MANIFEST";
inline constexpr const char* COMM_PROTOCOL = "COMM_PROTOCOL";
inline constexpr const char* TOPMOST = "LAILA";
inline constexpr const char* GLOBAL_ID = "GLOBAL_ID";
}  // namespace scope

struct ParsedGid {
  std::string uuid;
  std::vector<std::string> scopes;
  std::optional<int64_t> evolution;
};

// Namespace management for nickname -> deterministic UUID5.
void set_active_namespace(const std::string& namespace_key);
std::string get_active_namespace_uuid();

// UUID helpers (RFC 4122). uuid4 uses hal::Random; uuid5 uses SHA-1.
std::string uuid4();
std::string uuid5(const std::string& namespace_uuid, const std::string& name);
std::string generate_uuid_from_nickname(const std::string& nickname);

// global_id encode/parse (byte-identical to laila).
std::string to_global_id(const std::string& uuid,
                         const std::vector<std::string>& scopes,
                         std::optional<int64_t> evolution);
ParsedGid process_global_id(const std::string& global_id);
bool is_laila_resource(const std::string& global_id);

// Base identity carried by Entry, Future, Pool, Policy, ... laila's name for this
// base is _LAILA_IDENTIFIABLE_OBJECT (basics/definitions/identifiable_object.py);
// the Mirror Law keeps that exact name as the primary class.
class _LAILA_IDENTIFIABLE_OBJECT {
public:
  _LAILA_IDENTIFIABLE_OBJECT() : scopes_{scope::OBJECT} {}
  virtual ~_LAILA_IDENTIFIABLE_OBJECT() = default;

  const std::string& uuid() const { return uuid_; }
  void set_uuid(const std::string& u) { uuid_ = u; }
  const std::vector<std::string>& scopes() const { return scopes_; }
  void set_scopes(const std::vector<std::string>& s) { scopes_ = s; }
  std::optional<int64_t> evolution() const { return evolution_; }
  void set_evolution(std::optional<int64_t> e) { evolution_ = e; }
  bool has_evolution() const { return evolution_.has_value(); }

  std::string global_id() const { return to_global_id(uuid_, scopes_, evolution_); }
  void set_global_id(const std::string& gid);

protected:
  std::string uuid_ = uuid4();
  std::vector<std::string> scopes_;
  std::optional<int64_t> evolution_;
};

// Compatibility spelling for internal code; the faithful (Python) name above is
// the primary one.
using Identifiable = _LAILA_IDENTIFIABLE_OBJECT;

// Locally-atomic identifiable object (atomic/definitions/locally_atomic_object.py):
// an identifiable object with process-local atomic-update semantics, used as the
// identity base of Pool and CommProtocol. The atomic machinery itself is a tracked
// follow-up; this faithfully-named class anchors that level of the hierarchy.
class _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT : public _LAILA_IDENTIFIABLE_OBJECT {
public:
  using _LAILA_IDENTIFIABLE_OBJECT::_LAILA_IDENTIFIABLE_OBJECT;
};

}  // namespace laila_c

#endif  // LAILA_IDENTITY_HPP
