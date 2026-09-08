// Future hierarchy mirror (policy/central/command/schema/future/future/*).
// Identity + status + result-as-Entry + status callbacks + wait(). Every laila
// verb returns a Future. Concrete types keep laila's names. Single-core is an
// internal completion strategy (cooperative pump), not a new public type.
#ifndef LAILA_FUTURE_HPP
#define LAILA_FUTURE_HPP

#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "laila/entry.hpp"
#include "laila/identity.hpp"
#include "laila/status.hpp"

namespace laila_c {

enum class FutureStatus {
  NOT_STARTED,
  RUNNING,
  FINISHED,
  ERROR,
  CANCELLED,
  POLL_TIMEOUT,
  UNKNOWN,
};
const char* future_status_name(FutureStatus s);

class Future : public Identifiable {
public:
  Future() { scopes_ = {scope::FUTURE}; }
  virtual ~Future() = default;

  FutureStatus status() const { return status_; }
  void set_status(FutureStatus s);

  // result blocks until completion; re-raises on ERROR/CANCELLED.
  EntryPtr result();
  LailaValue data();  // result()->data()
  // global_id of the result entry (blocks until completion). Used by the
  // peer-served future-resolution verbs (_wait_future / _get_future_result_id).
  std::string result_id() { result(); return result_global_id_; }
  void set_result_entry(const EntryPtr& e);
  void set_result_value(const LailaValue& v);  // auto-wrap via Entry::constant
  Status exception() const { return exception_; }
  void set_exception(Status code, const std::string& msg);

  void add_status_callback(FutureStatus s, std::function<void(Future&)> fn);

  virtual void wait(int64_t timeout_ms = -1) = 0;

  bool finished() const { return status_ == FutureStatus::FINISHED; }
  bool running() const { return status_ == FutureStatus::RUNNING; }
  bool cancelled() const { return status_ == FutureStatus::CANCELLED; }
  bool error() const { return status_ == FutureStatus::ERROR; }
  bool not_started() const { return status_ == FutureStatus::NOT_STARTED; }

protected:
  FutureStatus status_ = FutureStatus::NOT_STARTED;
  EntryPtr return_value_;
  std::string result_global_id_;
  Status exception_ = Status::Ok;
  std::string exception_msg_;
  std::map<FutureStatus, std::vector<std::function<void(Future&)>>> status_callbacks_;
};
using FuturePtr = std::shared_ptr<Future>;

// Backed by the Executor HAL (threaded worker or cooperative inline).
class ConcurrentPackageFuture : public Future {
public:
  void wait(int64_t timeout_ms = -1) override;
};

// Proxies a future/result that lives on a peer policy. For the in-process peer
// path the fetch runs locally against the peer's memory; the same class is the
// seam where a real socket-RPC backend plugs in.
class RemoteFuture : public Future {
public:
  explicit RemoteFuture(std::function<EntryPtr()> fetch) : fetch_(std::move(fetch)) {}
  void wait(int64_t timeout_ms = -1) override;

private:
  std::function<EntryPtr()> fetch_;
};

// Aggregates child futures; returned when memorizing many entries.
class GroupFuture : public Future {
public:
  GroupFuture() { scopes_ = {scope::GROUP_FUTURE}; }
  void add_child(const FuturePtr& f) { children_.push_back(f); }
  const std::vector<FuturePtr>& children() const { return children_; }
  void wait(int64_t timeout_ms = -1) override;

private:
  std::vector<FuturePtr> children_;
};

}  // namespace laila_c

#endif  // LAILA_FUTURE_HPP
