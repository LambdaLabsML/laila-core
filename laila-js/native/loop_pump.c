/*
 * laila-js native loop pump.
 *
 * Exposes `run_once(suspend_immediates)` which spins the *current thread's*
 * libuv loop for one iteration (`uv_run(loop, UV_RUN_ONCE)`), blocking until
 * at least one event has been processed. This is what lets the JS port offer
 * Python-identical blocking `Future.wait()` / `.result` / `.data` from
 * synchronous code: the caller keeps its stack while the event loop keeps
 * progressing underneath it (the JS analogue of a Python thread blocking on
 * `threading.Event.wait()` while other threads make progress).
 *
 * `uv_run` is re-entered from inside a JS callback here. libuv's phases and
 * Node's timer / I/O / MessagePort processing tolerate that, but Node's
 * `processImmediate` does not: re-entered from the 2nd+ immediate of a batch
 * it dereferences a null `prevImmediate` and the process aborts
 * (`v8::ToLocalChecked Empty MaybeLocal` in `CheckImmediate`). When the JS
 * side detects that the pump was started from inside a Node `Immediate`
 * callback it passes `suspend_immediates = true`: every active `uv_check_t` /
 * `uv_idle_t` handle (Node's immediate check + idle handles) is stopped for
 * the duration of the iteration and restarted afterwards, so Node's
 * immediates are deferred until the pump unwinds. laila's own macrotask hops
 * are driven by a `MessageChannel` (poll phase), not by immediates, so they
 * keep running (see `src/_compat/pump.js`).
 *
 * Microtasks are NOT drained here (N-API has no stable entry point for that);
 * the JS side interleaves `process._tickCallback()` between iterations,
 * exactly like `deasync` does.
 *
 * Returns the `uv_run` result: non-zero while the loop still has live
 * (referenced) handles or requests, zero when it is idle.
 *
 * Pure C, raw N-API (no node-addon-api), so the same binary works on every
 * Node release that implements NAPI_VERSION 8 (Node >= 12.22 / 14.17 / 16+).
 */
#define NAPI_VERSION 8
#include <node_api.h>
#include <uv.h>

#define MAX_SUSPENDED 64

typedef struct {
  uv_handle_t* handles[MAX_SUSPENDED];
  int n;
} suspended_t;

static void suspend_walk_cb(uv_handle_t* h, void* arg) {
  suspended_t* s = (suspended_t*)arg;
  if (s->n >= MAX_SUSPENDED) return;
  if (!uv_is_active(h) || uv_is_closing(h)) return;
  if (h->type == UV_CHECK) {
    uv_check_stop((uv_check_t*)h);
    s->handles[s->n++] = h;
  } else if (h->type == UV_IDLE) {
    uv_idle_stop((uv_idle_t*)h);
    s->handles[s->n++] = h;
  }
}

static void resume_suspended(suspended_t* s) {
  int i;
  for (i = 0; i < s->n; i++) {
    uv_handle_t* h = s->handles[i];
    if (uv_is_closing(h)) continue;
    if (h->type == UV_CHECK) {
      uv_check_t* c = (uv_check_t*)h;
      uv_check_start(c, c->check_cb);
    } else if (h->type == UV_IDLE) {
      uv_idle_t* d = (uv_idle_t*)h;
      uv_idle_start(d, d->idle_cb);
    }
  }
}

static napi_value run_mode(napi_env env, napi_callback_info info, uv_run_mode mode) {
  size_t argc = 1;
  napi_value argv[1];
  bool suspend = false;
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (argc >= 1) {
    napi_valuetype t;
    if (napi_typeof(env, argv[0], &t) == napi_ok && t == napi_boolean) napi_get_value_bool(env, argv[0], &suspend);
  }

  uv_loop_t* loop = NULL;
  napi_status st = napi_get_uv_event_loop(env, &loop);
  if (st != napi_ok || loop == NULL) {
    napi_throw_error(env, "ERR_LAILA_PUMP", "napi_get_uv_event_loop failed");
    return NULL;
  }

  suspended_t s;
  s.n = 0;
  if (suspend) uv_walk(loop, suspend_walk_cb, &s);
  int alive = uv_run(loop, mode);
  if (suspend) resume_suspended(&s);

  napi_value out;
  napi_create_int32(env, alive, &out);
  return out;
}

static napi_value RunOnce(napi_env env, napi_callback_info info) {
  return run_mode(env, info, UV_RUN_ONCE);
}

static napi_value RunNoWait(napi_env env, napi_callback_info info) {
  return run_mode(env, info, UV_RUN_NOWAIT);
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_value fn;
  napi_create_function(env, "run_once", NAPI_AUTO_LENGTH, RunOnce, NULL, &fn);
  napi_set_named_property(env, exports, "run_once", fn);
  napi_create_function(env, "run_nowait", NAPI_AUTO_LENGTH, RunNoWait, NULL, &fn);
  napi_set_named_property(env, exports, "run_nowait", fn);
  /* Lets the JS side tell a stale prebuilt binary apart (it lacks `suspend`). */
  napi_value version;
  napi_create_int32(env, 2, &version);
  napi_set_named_property(env, exports, "abi", version);
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
