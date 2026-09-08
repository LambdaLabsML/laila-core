#include "laila/constitution.hpp"

#include <map>

#include "laila/manifest.hpp"
#include "laila/status.hpp"
#include "laila/transformation.hpp"

namespace laila_c {

// ---- SimpleConstitution ----
LailaValue SimpleConstitution::build(const LailaValue& payload_input) const {
  LailaValue current = payload_input;
  for (const auto& code : codes_) {
    current = apply_backward(code, current);
  }
  return current;
}

Json SimpleConstitution::as_dict() const {
  Json o = Json::object();
  o["_kind"] = std::string("simple");
  Json arr = Json::array();
  for (const auto& c : codes_) arr.push_back(Json(c));
  o["codes"] = arr;
  return o;
}

// ---- ComplexConstitution + builder registry ----
static std::map<std::string, BuilderFn>& builder_registry() {
  static std::map<std::string, BuilderFn> r;
  return r;
}
void register_builder(const std::string& token, BuilderFn fn) { builder_registry()[token] = std::move(fn); }
bool has_builder(const std::string& token) { return builder_registry().count(token) > 0; }

LailaValue ComplexConstitution::build_with(const Manifest& m) const {
  auto it = builder_registry().find(class_token_);
  if (it == builder_registry().end())
    raise(Status::Unsupported, "no builder registered for class_token: " + class_token_);
  return it->second(m);
}

LailaValue ComplexConstitution::build_with(const LailaValue&, const Manifest* m) const {
  if (m == nullptr)
    raise(Status::Error, "ComplexConstitution requires a manifest to build");
  return build_with(*m);
}

LailaValue ComplexConstitution::build(const LailaValue&) const {
  // Without a live manifest the complex build cannot run; the build pipeline
  // calls build_with(payload, manifest). Mirrors laila requiring a resolved
  // manifest before executing the constitution body.
  raise(Status::Error, "ComplexConstitution requires a manifest; use the build pipeline");
}

Json ComplexConstitution::as_dict() const {
  Json o = Json::object();
  o["_kind"] = std::string("complex");
  o["class_token"] = class_token_;
  o["manifest"] = manifest_ref_;
  return o;
}

// ---- dispatcher ----
std::shared_ptr<Constitution> Constitution::from_dict(const Json& node) {
  if (node.is_null()) return nullptr;
  if (!node.contains("_kind")) raise(Status::Error, "serialized constitution missing '_kind'");
  const std::string& kind = node.at("_kind").as_string();
  if (kind == "simple") {
    std::vector<std::string> codes;
    for (const auto& c : node.at("codes").elements()) codes.push_back(c.as_string());
    return std::make_shared<SimpleConstitution>(std::move(codes));
  }
  if (kind == "complex") {
    return std::make_shared<ComplexConstitution>(node.at("class_token").as_string(), node.at("manifest"));
  }
  raise(Status::Error, "no Constitution subclass for kind '" + kind + "'");
}

}  // namespace laila_c
