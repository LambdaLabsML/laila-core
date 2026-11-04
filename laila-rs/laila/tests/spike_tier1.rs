//! Phase 0 spike: Tier 1 object model and calling convention.

use laila::policy::central::command::FutureStatus;
use laila::prelude::*;
use laila::python::{ExcKind, Param, ParamKind, Signature};

/// Tests that touch the process-wide active policy (`laila.memorize` on
/// the alpha pool, `laila.terminate()`) run one at a time: the test
/// harness runs tests on parallel threads.
static ACTIVE_POLICY: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn active_policy_guard() -> std::sync::MutexGuard<'static, ()> {
    ACTIVE_POLICY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn python_numeric_equality_and_hash() {
    assert_eq!(py!(1), py!(1.0));
    assert_eq!(py!(True), py!(1));
    assert_eq!(py!(1).__hash__().unwrap(), py!(1.0).__hash__().unwrap());
    assert_eq!(py!(True).__hash__().unwrap(), py!(1).__hash__().unwrap());
    assert_ne!(py!(1), py!("1"));
    assert_eq!(py!(None), Arg::None);
    // dict keys follow Python: d[1.0] finds d[1]
    let d = py!({1: "a"});
    assert_eq!(d.__getitem__(1.0).unwrap(), "a");
    assert_eq!(d.__getitem__(true).unwrap(), "a");
}

#[test]
fn python_truthiness() {
    assert!(!truthy(&py!([])));
    assert!(truthy(&py!([0])));
    assert!(!truthy(&py!({})));
    assert!(!truthy(&py!("")));
    assert!(!truthy(&py!(0.0)));
    assert!(!truthy(&py!(None)));
    assert!(truthy(&py!("x")));
}

#[test]
fn repr_and_str_match_python() {
    assert_eq!(
        repr(py!({"a": [1, 2.5, None, True], "b": "it's"})),
        r#"{'a': [1, 2.5, None, True], 'b': "it's"}"#
    );
    assert_eq!(repr(py!((1,))), "(1,)");
    assert_eq!(repr(py!(1e100)), "1e+100");
    assert_eq!(str(py!("plain")), "plain");
    assert_eq!(str(ValueError::new(args!("bad"))), "bad");
}

#[test]
fn containers_alias_like_python_references() {
    let a = py!([1, 2]);
    let b = a.clone(); // Python: b = a
    b.__setitem__(0, 99).unwrap();
    assert_eq!(a.__getitem__(0).unwrap(), 99);
    let d = py!({"k": [1]});
    let inner = d.__getitem__("k").unwrap();
    inner.__setitem__(0, 7).unwrap();
    assert_eq!(repr(&d), "{'k': [7]}");
}

#[test]
fn getitem_errors_match_python() {
    let e = py!([1, 2]).__getitem__(5).unwrap_err();
    assert_eq!(e.kind, ExcKind::IndexError);
    assert_eq!(e.message, "list index out of range");
    let e = py!({"a": 1}).__getitem__("b").unwrap_err();
    assert_eq!(e.kind, ExcKind::KeyError);
    assert_eq!(e.__str__(), "'b'");
    let e = py!({}).__setitem__(py!([1]), 1).unwrap_err();
    assert_eq!(e.message, "unhashable type: 'list'");
    assert_eq!(
        py!([1, 2, 3, 4]).__getitem__(slice(1, -1)).unwrap(),
        py!([2, 3])
    );
    assert_eq!(py!("hello").__getitem__(slice(None, 2)).unwrap(), "he");
}

#[test]
fn args_macro_shapes() -> Result<()> {
    // `*`/`**` splats can fail (non-iterable / non-mapping), so they use `?`.
    let a = args!(1, "x", key = None, *py!([2, 3]), **py!({"z": 4}));
    assert_eq!(a.positional.len(), 4);
    assert_eq!(a.keywords.len(), 2);
    assert_eq!(a.get("z"), 4);
    let r#type = 1;
    let b = args!(type = r#type, data = {"m": [1]});
    assert_eq!(b.keywords[0].0, "type");
    assert_eq!(repr(b.get("data")), "{'m': [1]}");
    Ok(())
}

static SIG: Signature = Signature {
    name: "f",
    params: &[
        Param {
            name: "a",
            kind: ParamKind::PositionalOrKeyword,
            has_default: false,
            default: None,
        },
        Param {
            name: "b",
            kind: ParamKind::PositionalOrKeyword,
            has_default: true,
            default: None,
        },
        Param {
            name: "k",
            kind: ParamKind::KeywordOnly,
            has_default: true,
            default: None,
        },
    ],
};

#[test]
fn binding_errors_use_cpython_texts() {
    let e = SIG.bind(args!()).unwrap_err();
    assert_eq!(
        e.to_string(),
        "TypeError: f() missing 1 required positional argument: 'a'"
    );
    let e = SIG.bind(args!(1, 2, 3)).unwrap_err();
    assert_eq!(
        e.to_string(),
        "TypeError: f() takes from 1 to 2 positional arguments but 3 were given"
    );
    let e = SIG.bind(args!(1, a = 2)).unwrap_err();
    assert_eq!(
        e.to_string(),
        "TypeError: f() got multiple values for argument 'a'"
    );
    let e = SIG.bind(args!(1, zz = 2)).unwrap_err();
    assert_eq!(
        e.to_string(),
        "TypeError: f() got an unexpected keyword argument 'zz'"
    );
    let b = SIG.bind(args!(1, k = 5)).unwrap();
    assert_eq!(b.values[0], Some(Arg::Int(1)));
    assert_eq!(b.values[1], None);
    assert_eq!(b.values[2], Some(Arg::Int(5)));
}

#[pysig("g(data, *, nickname=None, flags=[], count=3)")]
fn g(args: impl Into<Args>) -> Result<Arg> {
    Ok(py!([data, nickname, flags, count]))
}

#[test]
fn pysig_binds_and_applies_defaults() {
    assert_eq!(repr(g(args!("d")).unwrap()), "['d', None, [], 3]");
    assert_eq!(
        repr(g(args!(nickname = "n", data = 1)).unwrap()),
        "[1, 'n', [], 3]"
    );
    let e = g(args!("d", "extra")).unwrap_err();
    assert_eq!(
        e.message,
        "g() takes 1 positional argument but 2 were given"
    );
    // single value is one positional argument
    assert_eq!(repr(g("solo").unwrap()), "['solo', None, [], 3]");
}

#[test]
fn py_try_passes_results_and_wraps_plain_values() -> Result<()> {
    fn plain() -> i32 {
        2
    }
    fn fallible(ok: bool) -> Result<i32> {
        if ok {
            Ok(1)
        } else {
            Err(ValueError::new(args!("nope")))
        }
    }
    let a = py_try!(plain())?;
    let b = py_try!(fallible(true))?;
    assert_eq!(a + b, 3);
    let r: Result<()> = (|| {
        py_try!(fallible(false))?;
        Ok(())
    })();
    assert!(r.unwrap_err().is::<ValueError>());
    Ok(())
}

#[test]
fn exception_hierarchy() {
    let e = KeyError::new(args!("k"));
    assert!(e.is::<LookupError>());
    assert!(e.is::<Exception>());
    assert!(!e.is::<ValueError>());
    assert!(isinstance::<(&ValueError, &KeyError)>(&arg(&e)));
}

#[test]
fn isinstance_markers() {
    assert!(isinstance::<str>(&py!("s")));
    assert!(isinstance::<int>(&py!(True)));
    assert!(!isinstance::<float>(&py!(1)));
    assert!(isinstance::<(&str, &int)>(&py!(3)));
    assert!(isinstance::<dict>(&py!({})));
}

#[test]
fn callables_and_lambdas() {
    let f = callable(|x: Arg| x.__getitem__(0));
    assert_eq!(f.__call__(args!(py!([9]))).unwrap(), 9);
    let g = callable(|| 5);
    assert_eq!(g.__call__(args!()).unwrap(), 5);
    let e = f.__call__(args!()).unwrap_err();
    assert_eq!(
        e.message,
        "<lambda>() takes 1 positional arguments but 0 were given"
    );
}

#[test]
fn facade_memorize_remember_roundtrip() -> Result<()> {
    let _serial = active_policy_guard();
    let entry = laila.constant(args!(data = {"msg": "hi"}, nickname = "spike"))?;
    let future = laila.memorize(args!(&entry))?;
    laila.wait(args!(&future))?;
    assert!(laila.alpha_pool().exists(entry.global_id())?);
    let back = laila.remember(args!(entry.global_id()))?.wait(args!())?;
    assert_eq!(back.as_entry().unwrap().data().unwrap(), py!({"msg": "hi"}));
    laila.forget(args!(entry.global_id()))?.wait(args!())?;
    assert!(!laila.alpha_pool().exists(entry.global_id())?);
    Ok(())
}

#[test]
fn pool_chain_shl_per_type() -> Result<()> {
    let _serial = active_policy_guard();
    use laila::data::{HDF5Pool, S3Pool};
    // `S3Pool` is a real client: the origin needs the credentials the
    // Python S3 suite reads (`~/.laila/secrets/s3_test.toml`).
    let secrets_path = format!(
        "{}/.laila/secrets/s3_test.toml",
        std::env::var("HOME").unwrap_or_default()
    );
    if !std::path::Path::new(&secrets_path).exists() {
        eprintln!("skipped: {secrets_path} required for the S3 origin");
        return Ok(());
    }
    laila.read_args(args!(secrets_path))?;
    let secrets = laila.args();
    let tmp = laila::python::stdlib::tempfile.mkdtemp(args!(prefix = "laila_spike_"))?;
    let hdf5_pool = HDF5Pool::new(args!(
        file_path = format!("{tmp}/cache.h5py"),
        nickname = "cache_hdf5"
    ))?;
    let s3_pool = S3Pool::new(args!(
        bucket_name = "laila-test-1",
        access_key_id = secrets.__getattr__("AWS_ACCESS_KEY")?,
        secret_access_key = secrets.__getattr__("AWS_SECRET_ACCESS_KEY")?,
        region_name = secrets.__getattr__("AWS_REGION")?,
        nickname = "origin_s3"
    ))?;
    let _ = laila.alpha_pool() << &hdf5_pool << &s3_pool;
    // Persistent tiers take serialized JSON (the cache tier writes back
    // what the origin served).
    s3_pool.write(args!("k", laila::python::stdlib::json.dumps(args!(1))?))?;
    assert!(!hdf5_pool.exists("k")?);
    // The in-memory tiers hand the stored text through; the HDF5 tier
    // decodes what it cached.
    assert_eq!(laila.alpha_pool().__getitem__("k")?, "1");
    assert!(hdf5_pool.exists("k")?);
    assert_eq!(hdf5_pool.__getitem__("k")?, 1);
    assert!(laila.alpha_pool().exists("k")?);
    laila.terminate(args!())?;
    Ok(())
}

/// Minimal executor so the spike needs no async runtime dependency.
fn block_on<F: std::future::IntoFuture>(fut: F) -> F::Output {
    use std::future::Future as _;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    struct Flag(std::sync::Mutex<bool>, std::sync::Condvar);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            *self.0.lock().unwrap() = true;
            self.1.notify_one();
        }
    }
    let flag = Arc::new(Flag(
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    ));
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    let fut = fut.into_future();
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        let mut g = flag.0.lock().unwrap();
        while !*g {
            g = flag.1.wait(g).unwrap();
        }
        *g = false;
    }
}

#[test]
fn futures_are_awaitable_and_waitable() {
    let f = laila::policy::central::command::Future::pending();
    let f2 = f.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        // `fut.result = 42` only records the value (Python setter
        // semantics); the terminal transition is a separate status flip.
        f2.set_result(py!(42));
        f2.set_status(FutureStatus::FINISHED);
    });
    // Awaiting / waiting hands back the result *entry* (raw values are
    // wrapped by `Entry.constant` on first read).
    let v = block_on(f.clone()).unwrap();
    assert_eq!(v.data().unwrap(), 42);
    assert_eq!(f.wait(args!()).unwrap().data().unwrap(), 42);
    let pending = laila::policy::central::command::Future::pending();
    let e = pending.wait(args!(timeout = 0.01)).unwrap_err();
    assert!(e.is::<TimeoutError>());
}
