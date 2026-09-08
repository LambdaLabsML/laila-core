// laila.args / laila.read_args (utils/args + __init__.read_args). In Python,
// read_args loads a TOML/JSON/.env/.xml file (or terminal flags) into the live
// laila.args DotMap, after which values are read as laila.args.<KEY>.
//
// C++ has no dynamic attributes, so the faithful mapping is string-keyed access:
// laila.args.AWS_BUCKET_NAME  ->  laila->args().get("AWS_BUCKET_NAME").
//
// This first version supports the common flat key/value sources (.env and simple
// TOML/JSON `KEY = value` lines) into a single process-wide store; nested tables
// and XML are a tracked follow-up.
#ifndef LAILA_ARGS_HPP
#define LAILA_ARGS_HPP

#include <map>
#include <string>

namespace laila_c {

class _LailaArgs {
public:
  // Value for `key` (empty string if absent). Mirrors laila.args.<key> access.
  std::string get(const std::string& key) const {
    auto it = values_.find(key);
    return it == values_.end() ? std::string() : it->second;
  }
  bool has(const std::string& key) const { return values_.count(key) != 0; }
  void set(const std::string& key, const std::string& value) { values_[key] = value; }
  const std::map<std::string, std::string>& values() const { return values_; }
  void clear() { values_.clear(); }

private:
  std::map<std::string, std::string> values_;
};

// Process-wide args store backing laila.args (a singleton, like laila's).
_LailaArgs& args();

// laila.read_args(source): merge a TOML/JSON/.env file's flat key/value pairs
// into laila.args. Quotes and surrounding whitespace are stripped; lines that
// are blank, comments (`#`), or TOML section headers (`[table]`) are skipped.
void read_args(const std::string& source);

}  // namespace laila_c

#endif  // LAILA_ARGS_HPP
