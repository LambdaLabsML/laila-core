// Futures: status lifecycle, result-as-Entry auto-wrap, callbacks (including
// late registration), GroupFuture aggregation, and runtime introspection.
#include "laila_test.hpp"

using namespace laila_c;

TEST("future", "memorize_status") {
  for (int i = 0; i < 100; ++i) {
    auto e = Entry::constant(LailaValue::from_int(i));
    auto f = laila->memorize(e);
    f->wait();
    CHECK(f->finished());
    CHECK(!f->error());
    CHECK(!f->cancelled());
    CHECK_EQ(f->status(), FutureStatus::FINISHED);
    CHECK_EQ(f->result()->data().as_int(), (int64_t)i);
  }
}

TEST("future", "result_auto_wrap") {
  auto f = std::make_shared<ConcurrentPackageFuture>();
  f->set_result_value(LailaValue::from_string("wrapped"));
  f->set_status(FutureStatus::FINISHED);
  CHECK_EQ(f->result()->data().as_string(), std::string("wrapped"));
  CHECK(f->data().kind() == LailaValue::Kind::String);
}

TEST("future", "status_callbacks") {
  auto f = std::make_shared<ConcurrentPackageFuture>();
  int fired = 0;
  f->add_status_callback(FutureStatus::FINISHED, [&](Future&) { ++fired; });
  CHECK_EQ(fired, 0);
  f->set_result_value(LailaValue::from_int(1));
  f->set_status(FutureStatus::FINISHED);
  CHECK_EQ(fired, 1);
  // Late registration on an already-fired status fires immediately.
  int late = 0;
  f->add_status_callback(FutureStatus::FINISHED, [&](Future&) { ++late; });
  CHECK_EQ(late, 1);
}

TEST("future", "error_propagation") {
  auto f = std::make_shared<ConcurrentPackageFuture>();
  f->set_exception(Status::Error, "boom");
  f->set_status(FutureStatus::ERROR);
  CHECK(f->error());
  CHECK_THROWS(f->result(), LailaError);
}

TEST("future", "group_aggregation") {
  for (int sz = 1; sz <= 30; ++sz) {
    std::vector<EntryPtr> entries;
    for (int i = 0; i < sz; ++i) entries.push_back(Entry::constant(LailaValue::from_int(i)));
    auto g = laila->memorize(entries);
    g->wait();
    CHECK(g->finished());
  }
}

TEST("future", "runtime_introspection") {
  auto e = Entry::constant(LailaValue::from_string("rt"));
  laila->memorize(e)->wait();
  auto f = laila->remember(e->global_id());
  std::string gid = f->global_id();
  // resolve by gid string and by handle.
  CHECK_EQ(runtime::status(f), runtime::status(gid));
  CHECK_EQ(runtime::wait(f)->data().as_string(), std::string("rt"));
  CHECK(runtime::resolve(gid) != nullptr);
  CHECK_THROWS(runtime::resolve(std::string("LAILA:FUTURE:GLOBAL_ID:00000000-0000-0000-0000-000000000000")), LailaError);
}

TEST("future", "wait_idempotent") {
  auto e = Entry::constant(LailaValue::from_int(99));
  auto f = laila->memorize(e);
  for (int i = 0; i < 10; ++i) {
    f->wait();
    CHECK(f->finished());
  }
  // result is stable across repeated reads.
  for (int i = 0; i < 10; ++i) CHECK_EQ(f->result()->data().as_int(), (int64_t)99);
}
