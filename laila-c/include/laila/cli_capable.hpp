// CLI-capable base (basics/definitions/cli_capable.py). In laila this is the
// configurability mixin that every first-class object (policy, taskforce, pool,
// comm protocol, logger) inherits, auto-filling missing constructor arguments
// from laila.args via a 4-tier resolution order.
//
// This is the faithful first-version surface: the class exists under its exact
// laila name so the hierarchy mirrors Python. Full laila.args injection / the
// environment mirror is a tracked follow-up; the contract is documented here.
#ifndef LAILA_CLI_CAPABLE_HPP
#define LAILA_CLI_CAPABLE_HPP

#include <string>

#include "laila/args.hpp"

namespace laila_c {

class _LAILA_CLI_CAPABLE_CLASS {
public:
  virtual ~_LAILA_CLI_CAPABLE_CLASS() = default;

protected:
  // laila's 4-tier parameter resolution (basics/definitions/cli_capable.py):
  //   1. explicit ctor arg always wins,
  //   2. else a value from laila.args at `args_key`,
  //   3. else the field default.
  // (The 4th tier -- raising on a still-empty required field -- is left to the
  // caller via require_arg.) Subclasses call this from their constructors to opt
  // a field into the CLI surface. Full auto-injection-during-construction (the
  // Pydantic model_validator hook) is a tracked follow-up.
  static std::string resolve_arg(const std::string& explicit_value,
                                 const std::string& args_key,
                                 const std::string& default_value = "") {
    if (!explicit_value.empty()) return explicit_value;
    if (args().has(args_key)) return args().get(args_key);
    return default_value;
  }
};

}  // namespace laila_c

#endif  // LAILA_CLI_CAPABLE_HPP
