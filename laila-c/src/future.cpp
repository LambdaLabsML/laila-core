#include "laila/future.hpp"

#include "laila/hal/hal.hpp"

namespace laila_c {

const char* future_status_name(FutureStatus s) {
  switch (s) {
    case FutureStatus::NOT_STARTED: return "NOT_STARTED";
    case FutureStatus::RUNNING: return "RUNNING";
    case FutureStatus::FINISHED: return "FINISHED";
    case FutureStatus::ERROR: return "ERROR";
    case FutureStatus::CANCELLED: return "CANCELLED";
    case FutureStatus::POLL_TIMEOUT: return "POLL_TIMEOUT";
    case FutureStatus::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

void Future::set_status(FutureStatus s) {
  std::vector<std::function<void(Future&)>> to_fire;
  {
    hal::Guard g;
    FutureStatus prev = status_;
    status_ = s;
    if (prev != s) {
      auto it = status_callbacks_.find(s);
      if (it != status_callbacks_.end()) to_fire = it->second;
    }
  }
  // Fire callbacks outside the lock to avoid holding it during user code.
  for (auto& cb : to_fire) cb(*this);
}

void Future::add_status_callback(FutureStatus s, std::function<void(Future&)> fn) {
  bool fire_now = false;
  {
    hal::Guard g;
    status_callbacks_[s].push_back(fn);
    fire_now = (status_ == s);
  }
  if (fire_now) fn(*this);  // close the registration/completion race
}

EntryPtr Future::result() {
  if (status_ == FutureStatus::ERROR || status_ == FutureStatus::CANCELLED)
    raise(exception_, exception_msg_.empty() ? "future failed" : exception_msg_);
  if (status_ == FutureStatus::FINISHED) return return_value_;
  wait(-1);
  if (status_ == FutureStatus::ERROR || status_ == FutureStatus::CANCELLED)
    raise(exception_, exception_msg_.empty() ? "future failed" : exception_msg_);
  return return_value_;
}

LailaValue Future::data() {
  EntryPtr e = result();
  if (!e) raise(Status::Error, "Future result is not an Entry; cannot access .data");
  return e->data();
}

void Future::set_result_entry(const EntryPtr& e) {
  hal::Guard g;
  return_value_ = e;
  result_global_id_ = e ? e->global_id() : "";
}

void Future::set_result_value(const LailaValue& v) {
  EntryPtr wrapped = Entry::constant(v);
  hal::Guard g;
  return_value_ = wrapped;
  result_global_id_ = wrapped->global_id();
}

void Future::set_exception(Status code, const std::string& msg) {
  hal::Guard g;
  exception_ = code;
  exception_msg_ = msg;
}

void ConcurrentPackageFuture::wait(int64_t timeout_ms) {
  // Cooperative backends run submitted work inline: a pump completes it.
  // Threaded backends run it on a worker: poll the monotonic clock until the
  // future reaches a terminal status (or the optional timeout elapses).
  hal::Executor& ex = hal::get().executor();
  uint64_t deadline = timeout_ms < 0 ? 0 : hal::get().clock().now_ms() + (uint64_t)timeout_ms;
  while (status_ == FutureStatus::NOT_STARTED || status_ == FutureStatus::RUNNING) {
    if (ex.is_cooperative()) {
      ex.drain();
      if (status_ == FutureStatus::NOT_STARTED || status_ == FutureStatus::RUNNING) break;
    } else {
      if (timeout_ms >= 0 && hal::get().clock().now_ms() >= deadline) {
        set_status(FutureStatus::POLL_TIMEOUT);
        break;
      }
      hal::get().clock().sleep_ms(1);
    }
  }
}

void RemoteFuture::wait(int64_t /*timeout_ms*/) {
  if (status_ == FutureStatus::FINISHED || status_ == FutureStatus::ERROR) return;
  set_status(FutureStatus::RUNNING);
#if defined(LAILA_NO_EXCEPTIONS)
  set_result_entry(fetch_());
  set_status(FutureStatus::FINISHED);
#else
  try {
    set_result_entry(fetch_());
    set_status(FutureStatus::FINISHED);
  } catch (const LailaError& e) {
    set_exception(e.code(), e.what());
    set_status(FutureStatus::ERROR);
  }
#endif
}

void GroupFuture::wait(int64_t timeout_ms) {
  bool all_ok = true;
  for (auto& c : children_) {
    c->wait(timeout_ms);
    if (!c->finished()) all_ok = false;
  }
  set_status(all_ok ? FutureStatus::FINISHED : FutureStatus::ERROR);
}

}  // namespace laila_c
