//! Tier 1 (typed Rust) coverage of `laila::atomic` -- the lock lifecycle the
//! Python suites only exercise indirectly: `RLock` re-entrancy and timeouts,
//! the mixin's `lock` / `unlock` / `locked` / `atomic(scope=..)` error
//! texts, `synchronized`, copy / pickle hooks, and the dynamic (`Arg`)
//! surface of the `Atomic*` wrappers.

use laila::atomic::definitions::{
    synchronized, synchronized_on, AtomicGuard, LockKind, RLock,
    _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
    _LAILA_LOCALLY_ATOMIC_OBJECT,
};
use laila::atomic::{AtomicDict, AtomicDotMap, AtomicFlag, AtomicInt, AtomicList, AtomicStr};
use laila::prelude::*;
use laila::python::HasAttrs;

py_class!(Plain);
impl _LAILA_LOCALLY_ATOMIC_OBJECT for Plain {}

// A `py_class!` deriving from an identifiable mixin keeps its identity in
// its `__dict__` (`laila::python!` emits the `dict_identity!` line itself).
py_class!(Ident);
laila::dict_identity!(Ident);
impl _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT for Ident {}

py_class!(Global);
laila::dict_identity!(Global);
impl _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT for Global {}

#[test]
fn rlock_is_reentrant_and_times_out() {
    let l = RLock::new();
    assert!(l.acquire(None));
    assert!(l.acquire(Some(0.0)));
    assert_eq!(l.recursion_count(), 2);
    assert!(l.locked() && l.is_owned());
    l.release().unwrap();
    l.release().unwrap();
    assert!(!l.locked());
    let e = l.release().unwrap_err();
    assert_eq!(
        e.to_string(),
        "RuntimeError: cannot release un-acquired lock"
    );

    // Another thread cannot take a held lock; a timeout reports `false`.
    let held = RLock::new();
    held.acquire(None);
    let other = held.clone();
    let got = std::thread::spawn(move || other.acquire(Some(0.05)))
        .join()
        .unwrap();
    assert!(!got);
    held.release().unwrap();
    let other = held.clone();
    assert!(std::thread::spawn(move || other.acquire(Some(0.05)))
        .join()
        .unwrap());
}

#[test]
fn local_mixin_lock_unlock_locked_and_scope_errors() {
    let p = Plain::default();
    assert!(!p.locked(Args::new()));
    assert!(p.lock(Args::new()).unwrap());
    assert!(p.locked(Args::new()));
    // the lock lives in `__dict__` under Python's name
    assert!(p.__attrs__().__hasattr__("_local_lock"));
    p.unlock().unwrap();
    assert!(!p.locked(Args::new()));
    let e = p.unlock().unwrap_err();
    assert_eq!(
        e.to_string(),
        "RuntimeError: cannot release un-acquired lock"
    );

    let cm = p.atomic(args!(scope = "global")).unwrap();
    let e = cm.__enter__().unwrap_err();
    assert_eq!(
        e.to_string(),
        "ValueError: Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call."
    );

    // `with p.atomic() as locked:` -- re-entrant, released on drop
    {
        let cm = p.atomic(Args::new()).unwrap();
        let locked: AtomicGuard<Plain> = cm.__enter__().unwrap();
        assert!(locked.locked(Args::new()));
        assert!(p.lock(args!(timeout_s = 0.0)).unwrap());
        p.unlock().unwrap();
    }
    assert!(!p.locked(Args::new()));

    // timeout from another thread
    p.lock(Args::new()).unwrap();
    let q = p.clone();
    let e = std::thread::spawn(move || {
        let cm = q.atomic(args!(timeout_s = 0.02)).unwrap();
        cm.__enter__().map(|_| ()).unwrap_err()
    })
    .join()
    .unwrap();
    assert_eq!(
        e.to_string(),
        "TimeoutError: Timed out acquiring local lock."
    );
    p.unlock().unwrap();
}

#[test]
fn global_mixin_scope_and_policy_errors() {
    let g = Global::default();
    let e = g
        .atomic(args!(scope = "weird"))
        .unwrap()
        .__enter__()
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "ValueError: Invalid scope for _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic() call."
    );
    let e = g
        .atomic(args!(scope = "global"))
        .unwrap()
        .__enter__()
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "ValueError: policy_id is required for global atomic scope."
    );
    let secret = g.lock_global(args!("LAILA:POLICY:a")).unwrap();
    assert!(secret.as_str().is_some_and(|s| s.len() == 16));
    assert!(g.global_locked(Args::new()).unwrap());
    let e = g
        .atomic(args!(scope = "global", policy_id = "LAILA:POLICY:b"))
        .unwrap()
        .__enter__()
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "PermissionError: Global lock already held by another policy."
    );
    // the gate: a wrong secret raises, the holder's passes
    let e = g.__gate__("bump", args!(secret = "nope")).unwrap_err();
    assert_eq!(
        e.to_string(),
        "PermissionError: Global.bump() is globally locked by policy 'LAILA:POLICY:a'; pass secret=<token> or call it inside the holder's atomic(scope='global') block."
    );
    let passed = g.__gate__("bump", args!(secret = secret.clone())).unwrap();
    assert!(passed.keywords.is_empty(), "secret is stripped");
    assert!(g.__gate__("_private", Args::new()).is_ok());
    assert!(g.__gate__("lock_global", Args::new()).is_ok());
    g.unlock_global(secret).unwrap();
    assert!(!g.global_locked(Args::new()).unwrap());
    assert!(!g._global_lock());
    assert!(g.global_id().starts_with("LAILA:"));
}

#[test]
fn synchronized_locks_identifiable_arguments_in_id_order() {
    let a = Ident::default();
    let b = Ident::default();
    let d = AtomicDict::new(Args::new()).unwrap();
    // typed form
    synchronized_on(&[&a, &b], || {
        assert!(a.locked(Args::new()) && b.locked(Args::new()));
        Ok(())
    })
    .unwrap();
    assert!(!a.locked(Args::new()) && !b.locked(Args::new()));
    // decorator form over dynamic values: non-identifiable atomics (an
    // `AtomicDict` argument) are *not* locked -- only `__atomic_lock__`
    // implementors are (`Entry` / pools once they carry the local lock).
    let seen = AtomicList::new(Args::new()).unwrap();
    let probe = {
        let seen = seen.clone();
        let d2 = d.clone();
        callable(move || -> Result<Arg> {
            seen.append(d2.locked(Args::new()))?;
            Ok(Arg::None)
        })
    };
    let wrapped = synchronized(args!(arg(&probe))).unwrap();
    wrapped.__call__(args!(arg(&d))).unwrap();
    assert_eq!(seen.to_list().unwrap(), py!([false]));
    let _ = &a;
    let e = synchronized(args!(arg(&probe), scope = "global"))
        .unwrap()
        .__call__(args!(arg(&a)))
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "NotImplementedError: Global synchronization is not implemented."
    );
}

#[test]
fn copy_and_pickle_hooks_replace_locks() {
    let p = Plain::default();
    p.__setattr__("x", 1).unwrap();
    p.lock(Args::new()).unwrap();
    let state = p.__getstate__();
    assert_eq!(
        state.__getitem__("__laila_dict_locks__").unwrap(),
        py!({"_local_lock": "rlock"})
    );
    assert_eq!(state.__getitem__("__dict__").unwrap(), py!({"x": 1}));
    let copy = _LAILA_LOCALLY_ATOMIC_OBJECT::__deepcopy__(&p).unwrap();
    assert_eq!(copy.__getattr__("x").unwrap(), 1);
    assert!(!copy.locked(Args::new()), "the copy has a fresh lock");
    let restored = Plain::default();
    restored.__setstate__(state).unwrap();
    assert_eq!(restored.__getattr__("x").unwrap(), 1);
    assert!(!restored.locked(Args::new()));
    assert_eq!(
        LockKind::of(&RLock::new().into_arg()),
        Some(LockKind::RLock)
    );
    p.unlock().unwrap();

    let f = AtomicFlag::new(args!(value = true)).unwrap();
    let f2 = f.deepcopy();
    f.clear(Args::new()).unwrap();
    assert!(f2.is_set(Args::new()).unwrap());
}

#[test]
fn atomic_types_dynamic_surface() {
    let f: Arg = AtomicFlag::new(Args::new()).unwrap().into_arg();
    f.__call_method__("set", Args::new()).unwrap();
    assert!(f.__bool__());
    assert_eq!(repr(&f), "AtomicFlag(True)");
    f.__setattr__("value", false).unwrap();
    assert_eq!(f.__getattr__("value").unwrap(), false);
    assert_eq!(
        f.__hash__().unwrap_err().to_string(),
        "TypeError: unhashable type: 'AtomicFlag'"
    );

    let i: Arg = AtomicInt::new(args!(value = 5)).unwrap().into_arg();
    assert_eq!(i.add(2).unwrap(), 7);
    assert_eq!(i.increment(Args::new()).unwrap(), 8);
    assert_eq!(i.decrement().unwrap(), 7);
    assert_eq!(repr(&i), "AtomicInt(7)");

    let s: Arg = AtomicStr::new(args!(value = "ab")).unwrap().into_arg();
    assert_eq!(s.__call_method__("append", args!("c")).unwrap(), "abc");
    assert_eq!(str(&s), "abc");
    assert_eq!(repr(&s), "AtomicStr('abc')");
    assert_eq!(s.length().unwrap(), 3);

    let l: Arg = AtomicList::new(args!(value = py!([1, 2])))
        .unwrap()
        .into_arg();
    l.append(3).unwrap();
    assert_eq!(len(&l).unwrap(), 3);
    assert_eq!(l.__getitem__(-1).unwrap(), 3);
    assert_eq!(l.__getitem__(slice(0, 2)).unwrap(), py!([1, 2]));
    assert!(l.__contains__(2).unwrap());
    assert_eq!(l.pop(Args::new()).unwrap(), 3);
    assert_eq!(repr(&l), "AtomicList([1, 2])");
    assert_eq!(
        l.__getitem__(9).unwrap_err().to_string(),
        "IndexError: list index out of range"
    );

    let d: Arg = AtomicDict::new(args!(py!({"a": 1}))).unwrap().into_arg();
    d.__setitem__("b", 2).unwrap();
    assert_eq!(d.__getitem__("b").unwrap(), 2);
    assert_eq!(len(&d).unwrap(), 2);
    assert_eq!(d.__iter__().unwrap(), vec![py!("a"), py!("b")]);
    assert_eq!(d.keys(Args::new()).unwrap(), py!(["a", "b"]));
    assert_eq!(
        d.__getitem__("zz").unwrap_err().to_string(),
        "KeyError: 'zz'"
    );
    assert_eq!(d.popitem().unwrap(), py!(("b", 2)));
    assert_eq!(repr(&d), "AtomicDict({'a': 1})");
    assert_eq!(d.pretty(Args::new()).unwrap(), "AtomicDict {\n  'a': 1,\n}");
    let e = d.pop_next().unwrap();
    assert_eq!(e, py!(("a", 1)));
    assert_eq!(
        d.pop_next().unwrap_err().to_string(),
        "KeyError: 'AtomicDict is empty'"
    );

    let m: Arg = AtomicDotMap::new(Args::new()).unwrap().into_arg();
    m.__setattr__("k", 1).unwrap();
    assert_eq!(m.__getattr__("k").unwrap(), 1);
    assert_eq!(m.__getattr__("missing").unwrap(), Arg::None);
    assert_eq!(repr(&m), "AtomicDotMap({'k': 1})");
    m.__delattr__("k").unwrap();
    assert_eq!(m.__delattr__("k").unwrap_err().to_string(), "KeyError: 'k'");
    let e = AtomicDotMap::new(args!(x = 1)).unwrap_err();
    assert_eq!(
        e.to_string(),
        "TypeError: AtomicDotMap.__init__() got an unexpected keyword argument 'x'"
    );
}
