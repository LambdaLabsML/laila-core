// Constitution mirror (entry/constitution/*). A recipe to materialize a
// payload. SimpleConstitution = ordered inverse-code chain (replayed through
// the recognized-snippet registry). ComplexConstitution = a builder referenced
// by class_token (the LLM transpiles Python f(manifest)->payload into a
// registered C builder). from_dict dispatches on the "_kind" tag.
#ifndef LAILA_CONSTITUTION_HPP
#define LAILA_CONSTITUTION_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "laila/json.hpp"
#include "laila/value.hpp"

namespace laila_c {

class Manifest;  // fwd

class Constitution {
public:
  virtual ~Constitution() = default;
  virtual const char* kind() const = 0;
  virtual LailaValue build(const LailaValue& payload_input) const = 0;
  // Manifest-aware build used by the build pipeline. Simple constitutions
  // ignore the manifest; complex ones require it.
  virtual LailaValue build_with(const LailaValue& payload_input, const Manifest* /*m*/) const {
    return build(payload_input);
  }
  virtual Json as_dict() const = 0;
  static std::shared_ptr<Constitution> from_dict(const Json& node);  // null-safe
};

// Ordered list of inverse-code strings. build() threads payload_input through
// each recognized snippet in order: code[0](input) -> code[1](...) -> ...
class SimpleConstitution : public Constitution {
public:
  explicit SimpleConstitution(std::vector<std::string> codes) : codes_(std::move(codes)) {}
  const char* kind() const override { return "simple"; }
  LailaValue build(const LailaValue& payload_input) const override;
  Json as_dict() const override;
  const std::vector<std::string>& codes() const { return codes_; }

private:
  std::vector<std::string> codes_;
};

// Builder registry for ComplexConstitution: token -> native builder.
using BuilderFn = std::function<LailaValue(const Manifest&)>;
void register_builder(const std::string& class_token, BuilderFn fn);
bool has_builder(const std::string& class_token);

class ComplexConstitution : public Constitution {
public:
  explicit ComplexConstitution(std::string class_token, Json manifest_ref = Json())
      : class_token_(std::move(class_token)), manifest_ref_(std::move(manifest_ref)) {}
  const char* kind() const override { return "complex"; }
  LailaValue build(const LailaValue& payload_input) const override;  // requires manifest
  LailaValue build_with(const LailaValue& payload_input, const Manifest* m) const override;
  LailaValue build_with(const Manifest& m) const;  // convenience
  Json as_dict() const override;
  const std::string& class_token() const { return class_token_; }

private:
  std::string class_token_;
  Json manifest_ref_;
};

}  // namespace laila_c

#endif  // LAILA_CONSTITUTION_HPP
