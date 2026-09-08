// Manifest (policy/central/memory/schema/manifest.py): an Entry subclass whose
// payload IS a blueprint -- a nested dict whose leaves are global_id strings (or
// lists of them). laila exposes the class as `laila.manifest` (== Manifest), so
// laila-C mirrors it via laila->manifest(...). The blueprint is the Entry data:
// `blueprint` == `data`. `realized()` resolves every referenced entry.
#ifndef LAILA_MANIFEST_HPP
#define LAILA_MANIFEST_HPP

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "laila/entry.hpp"
#include "laila/future.hpp"
#include "laila/identity.hpp"
#include "laila/json.hpp"
#include "laila/opts.hpp"

namespace laila_c {

class Manifest;
using ManifestPtr = std::shared_ptr<Manifest>;

// kwargs mirror for laila.manifest(...) / Manifest(...). `blueprint` is an alias
// of `data` in laila; both map to this single `data` field here.
struct ManifestOpts {
  std::optional<LailaValue> data;       // blueprint: nested gid-string structure
  std::optional<std::string> nickname;  // -> deterministic uuid
  std::optional<std::string> global_id;
  std::optional<std::string> uuid;
};

// realized() result node: mirrors the blueprint shape with resolved entries at
// the leaves. An object (keyed children), a list (ordered children), or a single
// resolved Entry.
class RealizedNode {
public:
  enum class Kind { Object, List, Entry };

  static RealizedNode object() { RealizedNode n; n.kind_ = Kind::Object; return n; }
  static RealizedNode list() { RealizedNode n; n.kind_ = Kind::List; return n; }
  static RealizedNode leaf(EntryPtr e) { RealizedNode n; n.kind_ = Kind::Entry; n.entry_ = std::move(e); return n; }

  Kind kind() const { return kind_; }
  bool is_object() const { return kind_ == Kind::Object; }
  bool is_list() const { return kind_ == Kind::List; }
  bool is_entry() const { return kind_ == Kind::Entry; }

  void set(const std::string& key, RealizedNode node) { obj_.emplace_back(key, std::move(node)); }
  void append(RealizedNode node) { list_.push_back(std::move(node)); }

  // object element count, list element count, or 1 for a leaf entry.
  size_t size() const {
    if (kind_ == Kind::Object) return obj_.size();
    if (kind_ == Kind::List) return list_.size();
    return 1;
  }

  // Object access by key (raises if missing / not an object).
  RealizedNode& operator[](const std::string& key);
  const RealizedNode& operator[](const std::string& key) const;
  // List access by index.
  RealizedNode& operator[](size_t i) { return list_.at(i); }
  const RealizedNode& operator[](size_t i) const { return list_.at(i); }

  EntryPtr entry() const { return entry_; }

private:
  Kind kind_ = Kind::Object;
  std::vector<std::pair<std::string, RealizedNode>> obj_;  // ordered, like a dict
  std::vector<RealizedNode> list_;
  EntryPtr entry_;
};

class Manifest : public Entry {
public:
  Manifest() { scopes_ = {scope::MANIFEST}; }

  // laila.manifest(...) / Manifest(...): build from a blueprint (gid-string
  // structure) and/or an identity (nickname/global_id/uuid).
  static ManifestPtr create(const ManifestOpts& opts = {});

  // blueprint == data (the nested gid structure).
  const LailaValue& blueprint() const { return data(); }

  // Mapping API over the TOP-LEVEL blueprint keys (laila's keys()/__len__).
  std::vector<std::string> keys() const;
  size_t size() const { return keys().size(); }

  // Iteration yields every leaf global_id depth-first (laila's __iter__);
  // `sum(1 for _ in manifest)` == std::distance(begin(), end()).
  std::vector<std::string> global_ids() const { return leaves_; }
  std::vector<std::string>::const_iterator begin() const { return leaves_.begin(); }
  std::vector<std::string>::const_iterator end() const { return leaves_.end(); }

  // Recall every referenced entry from the pool; returns a GroupFuture.
  FuturePtr remember(const RememberOpts& opts = {});
  // Synchronously fetch every referenced entry, returning a nested structure of
  // resolved entries mirroring the blueprint (laila's `realized`).
  RealizedNode realized() const;

  // --- Build-pipeline bundle API (named Entry references for ComplexConstitution
  // builders). Preserved from the original Manifest; orthogonal to the blueprint.
  void put(const std::string& name, const EntryPtr& e) { entries_[name] = e; }
  EntryPtr get(const std::string& name) const {
    auto it = entries_.find(name);
    return it == entries_.end() ? nullptr : it->second;
  }
  const std::map<std::string, EntryPtr>& entries() const { return entries_; }

private:
  void rebuild_index();  // recompute keys_/leaves_ from the blueprint

  std::map<std::string, EntryPtr> entries_;  // build-pipeline bundle
  std::vector<std::string> keys_;            // top-level blueprint keys (ordered)
  std::vector<std::string> leaves_;          // all leaf gids (DFS order)
};

}  // namespace laila_c

#endif  // LAILA_MANIFEST_HPP
