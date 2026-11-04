//! Tier 1 (typed Rust) coverage of the command subsystem internals the
//! transpiled Python suites cannot reach: the typed `ContextVar`s of
//! `schema::parking` (`_CURRENT_SLOT`, `_RESOLVE_CHAIN`; the three
//! `test_parking` cases skipped in the py2rs override) and the sync
//! executor recycle of `PythonAsyncThreadPoolTaskForce` (the
//! timing-dependent thread-count assertion of
//! `test_idle_sync_threads_are_reaped`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use laila::entry::Entry;
use laila::policy::central::command::schema::parking::{_CURRENT_SLOT, _RESOLVE_CHAIN};
use laila::policy::central::command::taskforce::async_thread_pool_executor::taskforce::SyncExecutor;
use laila::policy::central::command::{Command, Future, PythonAsyncThreadPoolTaskForce};
use laila::prelude::*;
use laila::python::stdlib::asyncio;

const T: f64 = 30.0;

fn command() -> Command {
    let _ = laila::laila.get_active_policy();
    laila::laila.command()
}

/// A started 1x1 taskforce registered with the active command.
fn new_tf() -> PythonAsyncThreadPoolTaskForce {
    let mut a = Args::new();
    a.push_keyword(
        "policy_id",
        laila::laila.active_policy().unwrap().global_id(),
    );
    a.push_keyword("num_workers", 1);
    a.push_keyword("max_async_per_thread", 1);
    let tf = PythonAsyncThreadPoolTaskForce::new(a).unwrap();
    tf.start(Args::new()).unwrap();
    command().add_taskforce(tf.clone()).unwrap();
    tf
}

fn drop_tf(tf: &PythonAsyncThreadPoolTaskForce) {
    let _ = tf.shutdown(Args::new());
    command().remove_taskforce(&tf.global_id());
}

fn submit(cmd: &Command, task: Arg, tf: Option<&PythonAsyncThreadPoolTaskForce>) -> Future {
    let mut a = Args::new();
    a.push_positional(vec![task].into_arg());
    if let Some(tf) = tf {
        a.push_keyword("taskforce_id", tf.global_id());
    }
    match cmd.submit(a).unwrap() {
        Arg::Future(f) => f,
        other => panic!("submit returned {}", other.type_name()),
    }
}

fn unwrap(v: Arg) -> Arg {
    let mut v = v;
    while let Ok(d) = v.__getattr__("data") {
        v = d;
    }
    v
}

fn wait(f: &Future) -> Arg {
    let mut a = Args::new();
    a.push_keyword("timeout", T);
    let out = f.wait(a).unwrap();
    let _ = f.release(Args::new());
    out
}

// test_013_slot_context_visible_inside_sync_body
#[test]
fn slot_context_visible_inside_sync_body() {
    let cmd = command();
    let tf = new_tf();
    let tf_id = tf.global_id();
    let body = callable(move || {
        _CURRENT_SLOT
            .get()
            .is_some_and(|ctx| ctx.tf.global_id() == tf_id)
    });
    let fut = submit(&cmd, body, Some(&tf));
    assert_eq!(unwrap(wait(&fut)), Arg::Bool(true));
    assert!(_CURRENT_SLOT.get().is_none());
    drop_tf(&tf);
}

// test_023_remember_on_active_chain_raises
#[test]
fn remember_on_active_chain_raises() {
    let _cmd = command();
    let mut a = Args::new();
    a.push_keyword("data", 1);
    let e = Entry::constant(a).unwrap();
    let gid = e.global_id();
    wait(
        &laila::laila
            .memorize(Args::from(e.clone()))
            .unwrap()
            .into_local()
            .unwrap(),
    );
    let token = _RESOLVE_CHAIN.set(Arc::new(vec![gid.clone()]));
    let err = laila::laila.remember(Args::from(gid.clone())).unwrap_err();
    _RESOLVE_CHAIN.reset(token);
    assert_eq!(err.kind, ExcKind::CyclicDependencyError);
    let again = laila::laila
        .remember(Args::from(gid))
        .unwrap()
        .into_local()
        .unwrap();
    assert_eq!(unwrap(wait(&again)), Arg::Int(1));
}

// test_024_chain_propagates_into_child_root_tasks
#[test]
fn chain_propagates_into_child_root_tasks() {
    let cmd = command();
    let child = asyncio::coroutine_function("child", |_a: Args| async {
        Ok((*_RESOLVE_CHAIN.get())
            .iter()
            .map(|s| Arg::from(s.as_str()))
            .collect::<Vec<_>>()
            .into_arg())
    });
    let cmd2 = cmd.clone();
    let parent = asyncio::coroutine_function("parent", move |_a: Args| {
        let (cmd, child) = (cmd2.clone(), child.clone());
        async move {
            let mut chain = (*_RESOLVE_CHAIN.get()).clone();
            chain.push("LAILA:ENTRY:p".into());
            let token = _RESOLVE_CHAIN.set(Arc::new(chain));
            let out = match submit(&cmd, child, None).await {
                Ok(v) => Ok(unwrap(v)),
                Err(e) => Err(e),
            };
            _RESOLVE_CHAIN.reset(token);
            out
        }
    });
    let chain = unwrap(wait(&submit(&cmd, parent, None)));
    assert_eq!(chain, vec![Arg::from("LAILA:ENTRY:p")].into_arg());
}

// F-W24-02: the sync executor recycles its idle workers.
#[test]
fn sync_executor_spawns_per_concurrent_body_and_reaps_on_shutdown() {
    let ex = SyncExecutor::new("test-sync".into());
    let gate = Arc::new(laila::python::stdlib::Event::new(Args::new()));
    let n = 6;
    for _ in 0..n {
        let g = gate.clone();
        ex.submit(Box::new(move || {
            let mut a = Args::new();
            a.push_positional(T);
            let _ = g.wait(a);
        }))
        .unwrap();
    }
    // Every body blocks: one worker per body.
    let deadline = Instant::now() + Duration::from_secs(10);
    while ex.workers() < n && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(ex.workers(), n);
    gate.set(Args::new());
    // Idle workers stay alive until the executor is shut down (recycled).
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(ex.workers(), n);
    // `shutdown(wait=True)` returns once every worker has exited.
    ex.shutdown(true);
    assert_eq!(ex.workers(), 0);
    assert!(ex.submit(Box::new(|| {})).is_err());
}

// The taskforce keeps working across an executor swap (idle recycle).
#[test]
fn taskforce_recycles_idle_executor_and_keeps_working() {
    let cmd = command();
    let tf = new_tf();
    let first = submit(&cmd, callable(|| 41), Some(&tf));
    assert_eq!(unwrap(wait(&first)), Arg::Int(41));
    let gen_before = tf.executor_generation();
    // Completely idle for longer than `_EXECUTOR_IDLE_RECYCLE_S`.
    let deadline = Instant::now() + Duration::from_secs(10);
    while tf.executor_generation() == gen_before && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(tf.executor_generation() > gen_before);
    let again = submit(&cmd, callable(|| 42), Some(&tf));
    assert_eq!(unwrap(wait(&again)), Arg::Int(42));
    assert_eq!(tf.inflight(), 0);
    drop_tf(&tf);
}
