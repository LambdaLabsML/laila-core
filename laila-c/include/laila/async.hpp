// C++20 coroutine layer for laila-C. Makes every laila Future awaitable so
// async code reads line-for-line like laila Python's `await` (Jupyter's asyncio
// loop -> EventLoop here). Header-only; only C++20 translation units include it.
#ifndef LAILA_ASYNC_HPP
#define LAILA_ASYNC_HPP

#include <atomic>
#include <coroutine>
#include <deque>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

#include "laila/future.hpp"
#include "laila/hal/hal.hpp"
#include "laila/policy.hpp"

namespace laila_c {

// Single-threaded scheduler. Coroutines suspend on a Future and are re-queued
// here by the Future's completion callback; the loop pumps the Executor HAL
// (cooperative drain, or a brief sleep while a worker thread runs the I/O).
class EventLoop {
public:
  static EventLoop& get() {
    static EventLoop loop;
    return loop;
  }

  void enqueue(std::coroutine_handle<> h) {
    hal::Guard g;
    ready_.push_back(h);
  }

  void pump_until(std::coroutine_handle<> root) {
    hal::Executor& ex = hal::get().executor();
    while (!root.done()) {
      if (auto h = pop_ready()) {
        h.resume();
        continue;
      }
      if (ex.is_cooperative())
        ex.drain();
      else
        hal::get().clock().sleep_ms(1);
    }
  }

private:
  std::coroutine_handle<> pop_ready() {
    hal::Guard g;
    if (ready_.empty()) return {};
    auto h = ready_.front();
    ready_.pop_front();
    return h;
  }
  std::deque<std::coroutine_handle<>> ready_;
};

namespace detail {
// On completion, transfer control to whoever awaited us (or park at noop).
struct final_awaiter {
  bool await_ready() noexcept { return false; }
  template <class P>
  std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
    auto c = h.promise().continuation_;
    return c ? c : std::noop_coroutine();
  }
  void await_resume() noexcept {}
};
}  // namespace detail

// A suspendable async function. Lazy-start: the EventLoop (or an awaiting
// coroutine) resumes it. Mirrors an `async def`.
template <class T>
class Task {
public:
  struct promise_type {
    T value_{};
    std::exception_ptr err_;
    std::coroutine_handle<> continuation_;

    Task get_return_object() {
      return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    detail::final_awaiter final_suspend() noexcept { return {}; }
    void return_value(T v) { value_ = std::move(v); }
    void unhandled_exception() { err_ = std::current_exception(); }
  };
  using handle_t = std::coroutine_handle<promise_type>;

  explicit Task(handle_t h) : h_(h) {}
  Task(Task&& o) noexcept : h_(std::exchange(o.h_, {})) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() {
    if (h_) h_.destroy();
  }

  handle_t handle() const { return h_; }
  T result() {
    if (h_.promise().err_) std::rethrow_exception(h_.promise().err_);
    return std::move(h_.promise().value_);
  }

  bool await_ready() { return false; }
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) {
    h_.promise().continuation_ = caller;
    return h_;  // symmetric transfer: start the nested task
  }
  T await_resume() { return result(); }

private:
  handle_t h_;
};

template <>
class Task<void> {
public:
  struct promise_type {
    std::exception_ptr err_;
    std::coroutine_handle<> continuation_;

    Task get_return_object() {
      return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    detail::final_awaiter final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { err_ = std::current_exception(); }
  };
  using handle_t = std::coroutine_handle<promise_type>;

  explicit Task(handle_t h) : h_(h) {}
  Task(Task&& o) noexcept : h_(std::exchange(o.h_, {})) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() {
    if (h_) h_.destroy();
  }

  handle_t handle() const { return h_; }
  void result() {
    if (h_.promise().err_) std::rethrow_exception(h_.promise().err_);
  }

  bool await_ready() { return false; }
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) {
    h_.promise().continuation_ = caller;
    return h_;
  }
  void await_resume() { result(); }

private:
  handle_t h_;
};

// Makes `co_await some_future` work. Suspends until the Future (or, for a
// GroupFuture, all children) reaches a terminal status, then resolves to the
// result Entry (re-raising on error) -- exactly like Python's `await ref`.
struct FutureAwaiter {
  FuturePtr f_;
  bool await_ready() { return f_->finished() || f_->error() || f_->cancelled(); }

  void await_suspend(std::coroutine_handle<> h) {
    if (auto grp = std::dynamic_pointer_cast<GroupFuture>(f_)) {
      if (grp->children().empty()) {
        EventLoop::get().enqueue(h);
        return;
      }
      auto left = std::make_shared<std::atomic<int>>((int)grp->children().size());
      auto cb = [left, grp, h](Future&) {
        if (--*left == 0) {
          bool ok = true;
          for (auto& c : grp->children())
            if (!c->finished()) ok = false;
          grp->set_status(ok ? FutureStatus::FINISHED : FutureStatus::ERROR);
          EventLoop::get().enqueue(h);
        }
      };
      for (auto& c : grp->children()) {
        c->add_status_callback(FutureStatus::FINISHED, cb);
        c->add_status_callback(FutureStatus::ERROR, cb);
      }
    } else {
      auto cb = [h](Future&) { EventLoop::get().enqueue(h); };
      f_->add_status_callback(FutureStatus::FINISHED, cb);
      f_->add_status_callback(FutureStatus::ERROR, cb);
    }
  }
  EntryPtr await_resume() { return f_->result(); }
};
inline FutureAwaiter operator co_await(FuturePtr f) { return FutureAwaiter{std::move(f)}; }

// Drive a top-level Task to completion from a normal (non-coroutine) context.
// This is the `asyncio.run(...)` / Jupyter-loop analog.
template <class T>
T run(Task<T> task) {
  auto h = task.handle();
  h.resume();
  EventLoop::get().pump_until(h);
  return task.result();
}

// Async analog of `with laila.guarantee:` -> `async with laila.guarantee_async:`.
// C++ has no async destructor, so the scope body is passed as a coroutine
// lambda; every Future created inside is tracked and awaited before returning.
template <class BodyFn>  // BodyFn() -> Task<void>
Task<void> guarantee_async(BodyFn body) {
  auto& cmd = get_active_policy()->command();
  cmd._guarantee_enter();
  std::exception_ptr err;
  try {
    co_await body();
  } catch (...) {
    err = std::current_exception();
  }
  for (auto& f : cmd._guarantee_exit()) {
    if (f) co_await f;
  }
  if (err) std::rethrow_exception(err);
}

}  // namespace laila_c

#endif  // LAILA_ASYNC_HPP
