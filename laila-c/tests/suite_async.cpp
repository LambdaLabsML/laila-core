// Async / coroutine layer (laila/async.hpp): the C++20 `co_await` mirror of
// tutorial 07. Exercises operator co_await on a Future, GroupFuture aggregation,
// nested Tasks, error propagation, and guarantee_async -- all over the default
// in-process pool, so no S3/credentials are required.
#include "laila/async.hpp"
#include "laila_test.hpp"

using namespace laila_c;

// A remember-double-write worker (the notebook's double_entries, minus S3).
static Task<int64_t> roundtrip(int64_t v) {
  auto e = Entry::constant(LailaValue::from_int(v));
  co_await laila->memorize(e);                        // await a single Future
  auto r = co_await laila->remember(e->global_id());  // await ref -> Entry
  co_return r->data().as_int();
}

static Task<int64_t> read_id(std::string gid) {
  auto r = co_await laila->remember(gid);
  co_return r->data().as_int();
}

TEST("async", "co_await_roundtrip") {
  for (int i = 0; i < 50; ++i) CHECK_EQ(run(roundtrip(i)), (int64_t)i);
}

// co_await a GroupFuture: resolves only when every child completes, and leaves
// the group status FINISHED (aggregated by the awaiter, like the notebook's
// status-after-await print).
static Task<int> group_upload(int n) {
  std::vector<EntryPtr> es;
  for (int i = 0; i < n; ++i) es.push_back(Entry::constant(LailaValue::from_int(i)));
  auto g = laila->memorize(es);
  co_await g;
  if (runtime::status(g) != FutureStatus::FINISHED) co_return -1;
  for (int i = 0; i < n; ++i) {
    auto r = co_await laila->remember(es[i]->global_id());
    if (r->data().as_int() != (int64_t)i) co_return -1;
  }
  co_return n;
}

TEST("async", "group_await") {
  for (int n = 1; n <= 20; ++n) CHECK_EQ(run(group_upload(n)), n);
}

// Nested coroutine: co_await another Task and use its value.
static Task<int64_t> doubler(int64_t v) {
  auto base = co_await roundtrip(v);
  co_return base * 2;
}

TEST("async", "nested_task") {
  CHECK_EQ(run(doubler(21)), (int64_t)42);
  CHECK_EQ(run(doubler(0)), (int64_t)0);
}

// An error on an awaited Future surfaces as a thrown exception at the co_await
// point and propagates out through run().
static Task<void> await_failing() {
  auto f = std::make_shared<ConcurrentPackageFuture>();
  f->set_exception(Status::Error, "boom");
  f->set_status(FutureStatus::ERROR);
  co_await FuturePtr(f);
}

TEST("async", "error_propagation") {
  CHECK_THROWS(run(await_failing()), LailaError);
}

// guarantee_async: futures created (but not awaited) inside the scope are
// tracked and awaited on exit, so their entries are retrievable afterward.
TEST("async", "guarantee_async_awaits_scope") {
  std::vector<std::string> ids;
  auto driver = [&]() -> Task<void> {
    auto body = [&]() -> Task<void> {
      for (int i = 0; i < 10; ++i) {
        auto e = Entry::constant(LailaValue::from_int(1000 + i));
        ids.push_back(e->global_id());
        laila->memorize(e);  // not awaited here -- guarantee_async awaits on exit
      }
      co_return;
    };
    co_await guarantee_async(body);
    co_return;
  };
  run(driver());

  CHECK_EQ((int)ids.size(), 10);
  for (size_t i = 0; i < ids.size(); ++i)
    CHECK_EQ(run(read_id(ids[i])), (int64_t)(1000 + i));
}
