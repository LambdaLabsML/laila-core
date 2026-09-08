#include "laila/args.hpp"

#include <fstream>
#include <sstream>

#include "laila/json.hpp"

namespace laila_c {

_LailaArgs& args() {
  static _LailaArgs g_args;
  return g_args;
}

namespace {

std::string strip(const std::string& s) {
  size_t a = s.find_first_not_of(" \t\r\n");
  if (a == std::string::npos) return std::string();
  size_t b = s.find_last_not_of(" \t\r\n");
  return s.substr(a, b - a + 1);
}

std::string unquote(const std::string& s) {
  if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') ||
                        (s.front() == '\'' && s.back() == '\''))) {
    return s.substr(1, s.size() - 2);
  }
  return s;
}

bool ends_with(const std::string& s, const char* suffix) {
  std::string suf(suffix);
  return s.size() >= suf.size() && s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}

// Flatten a JSON object's string/number/bool leaves into KEY=value pairs. Nested
// objects are joined with '.' (mirrors laila's nested-table -> dotted-path).
void flatten_json(const Json& node, const std::string& prefix, _LailaArgs& out) {
  if (!node.is_object()) return;
  for (const auto& kv : node.items()) {
    const std::string key = prefix.empty() ? kv.first : prefix + "." + kv.first;
    const Json& v = kv.second;
    if (v.is_object()) {
      flatten_json(v, key, out);
    } else if (v.is_string()) {
      out.set(key, v.as_string());
    } else {
      out.set(key, v.dump());
    }
  }
}

}  // namespace

void read_args(const std::string& source) {
  std::ifstream f(source);
  if (!f.is_open()) return;  // mirrors laila's read_args returning None on miss
  std::stringstream buf;
  buf << f.rdbuf();
  const std::string text = buf.str();

  if (ends_with(source, ".json")) {
#if defined(LAILA_NO_EXCEPTIONS)
    flatten_json(Json::parse(text), "", args());
#else
    try {
      flatten_json(Json::parse(text), "", args());
    } catch (...) {
      // fall through to line parsing on malformed JSON
    }
#endif
    return;
  }

  // .env / flat TOML: parse `KEY = value` lines.
  std::istringstream lines(text);
  std::string line;
  while (std::getline(lines, line)) {
    std::string t = strip(line);
    if (t.empty() || t[0] == '#' || t[0] == '[') continue;  // blank/comment/table
    size_t eq = t.find('=');
    if (eq == std::string::npos) continue;
    std::string key = strip(t.substr(0, eq));
    std::string value = unquote(strip(t.substr(eq + 1)));
    if (!key.empty()) args().set(key, value);
  }
}

}  // namespace laila_c
