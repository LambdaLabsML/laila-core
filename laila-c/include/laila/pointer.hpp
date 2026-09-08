// laila_pointer<T>({...}): a type-safe std::make_shared wrapper that hides the
// *Opts type name at the call site. Because T is fixed explicitly, the parameter
// type T::Opts is concrete, so a braced-init-list copy-list-initializes it -- the
// reason plain std::make_shared<T>({...}) does NOT compile (a braced-init-list is
// a non-deduced context for the make_shared forwarding template).
//
//   auto fs = laila_pointer<FilesystemPool>({.nickname = "tutorial_fs"});
//   // == std::make_shared<FilesystemPool>(FilesystemPoolOpts{.nickname=...})
//
// This is C-only ergonomics over make_shared (like make_shared itself); it renames
// or shadows no Python symbol, so it does not affect Mirror-Law surface parity.
// Requires the target type to expose a nested `using Opts = <its>Opts;` alias.
#ifndef LAILA_POINTER_HPP
#define LAILA_POINTER_HPP

#include <memory>

namespace laila_c {

template <class T>
std::shared_ptr<T> laila_pointer(const typename T::Opts& opts = {}) {
  return std::make_shared<T>(opts);
}

}  // namespace laila_c

#endif  // LAILA_POINTER_HPP
