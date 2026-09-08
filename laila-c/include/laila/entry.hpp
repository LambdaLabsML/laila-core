// Entry mirror (entry/entry.py): the fundamental data unit. Identity + state +
// (payload XOR constitution). Public factories constant/variable/contingent and
// evolve/serialize/as_dict/from_dict match laila's signatures; Python kwargs
// are reproduced via *Opts structs with matching field names.
#ifndef LAILA_ENTRY_HPP
#define LAILA_ENTRY_HPP

#include <memory>
#include <optional>
#include <string>

#include "laila/constitution.hpp"
#include "laila/entry_state.hpp"
#include "laila/identity.hpp"
#include "laila/json.hpp"
#include "laila/transformation.hpp"
#include "laila/value.hpp"

namespace laila_c {

class Entry;
using EntryPtr = std::shared_ptr<Entry>;

// Keyword-argument mirrors (the one permitted "small" syntactic difference).
struct ConstantOpts {
  std::optional<std::string> global_id;
  std::optional<std::string> uuid;
  std::optional<std::string> nickname;
};
struct VariableOpts {
  std::optional<std::string> uuid;
  std::optional<int64_t> evolution;
  std::optional<EntryState> state;
  std::optional<std::string> constitution;  // builder class_token
  std::shared_ptr<class Manifest> manifest;
  std::optional<std::string> global_id;
  std::optional<std::string> nickname;
};

class Entry : public Identifiable, public std::enable_shared_from_this<Entry> {
public:
  Entry() { scopes_ = {scope::ENTRY}; state_ = EntryState::STAGED; }

  // ---- Factories (Entry.constant / Entry.variable / Entry.contingent) ----
  static EntryPtr constant(const LailaValue& data, const ConstantOpts& opts = {});
  static EntryPtr variable(const LailaValue& data = LailaValue::none(),
                           const VariableOpts& opts = {});
  static EntryPtr contingent(const LailaValue& data, std::optional<std::string> uuid,
                             std::vector<std::string> scopes,
                             std::optional<int64_t> evolution, EntryState state);

  // ---- Properties ----
  const LailaValue& data() const;        // raises EntryNotBuiltError if unbuilt
  void set_data(const LailaValue& v);
  EntryState state() const { return state_; }
  void set_state(EntryState s) { state_ = s; }
  std::shared_ptr<Constitution> constitution() const { return constitution_; }
  void set_constitution(std::shared_ptr<Constitution> c) { constitution_ = c; }
  std::shared_ptr<class Manifest> manifest() const { return manifest_; }
  void set_manifest(std::shared_ptr<class Manifest> m) { manifest_ = m; }

  // ---- Operations ----
  EntryPtr evolve(const LailaValue& data = LailaValue::none());
  void build_inplace();  // run attached constitution synchronously

  // ---- Serialization ----
  Json as_dict() const;
  // laila-native dict (untagged/raw payload) for the cross-language peer RPC
  // wire, so the (unmodifiable) Python `laila` can hydrate it via Entry.from_dict.
  Json to_wire_dict() const;
  // transformations==nullptr => identity (in-memory path returns this entry's
  // raw dict). Otherwise applies the pipeline and bundles a SimpleConstitution.
  Json serialize(const TransformationSequence* transformations = nullptr) const;
  static EntryPtr from_dict(const Json& node);
  static EntryPtr build_from_dict(const Json& node);  // from_dict + simple build

  std::string str() const { return global_id(); }

private:
  EntryState state_ = EntryState::STAGED;
  std::optional<LailaValue> payload_;
  std::shared_ptr<Constitution> constitution_;
  std::shared_ptr<class Manifest> manifest_;  // bound for complex builds
};

}  // namespace laila_c

#endif  // LAILA_ENTRY_HPP
