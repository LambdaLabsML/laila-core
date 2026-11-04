#![allow(non_snake_case)]
//! Phase 0 spike: the README pool-chaining example, body verbatim from
//! Python, inside `laila::python! { .. }`.

use laila::data::{HDF5Pool, S3Pool};
use laila::policy::central::memory::record::Record;
use laila::prelude::*;

laila::python! {
    // `S3Pool` is a real client: the README's placeholder credentials are
    // the ones in `~/.laila/secrets/s3_test.toml` (what the Python S3 suite
    // reads); without that file the example is skipped.
    static SECRETS_PATH: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const_SECRETS_PATH().expect("module constant SECRETS_PATH"));
    fn __laila_const_SECRETS_PATH() -> Arg {
        return Path.home() / ".laila" / "secrets" / "s3_test.toml";
    }

    fn s3_secrets() -> Arg {
        if not SECRETS_PATH.exists() {
            return None;
        }
        laila.read_args(SECRETS_PATH);
        return laila.args;
    }

    fn readme_pool_chain(_secrets: Arg) {
        // Create an HDF5 pool (local disk) and an S3 pool (remote)
        let hdf5_pool = HDF5Pool(nickname="cache_hdf5");
        let s3_pool = S3Pool(
            bucket_name="laila-test-1",
            access_key_id=_secrets.AWS_ACCESS_KEY,
            secret_access_key=_secrets.AWS_SECRET_ACCESS_KEY,
            region_name=_secrets.AWS_REGION,
            nickname="origin_s3",
        );

        // Register both pools with LAILA's memory system
        laila.memory.extend(hdf5_pool, pool_nickname="cache_hdf5");
        laila.memory.extend(s3_pool, pool_nickname="origin_s3");

        // Wire a three-tier proxy chain: memory <- HDF5 <- S3
        laila.alpha_pool << hdf5_pool << s3_pool;

        // Store an entry directly in S3
        let entry = laila.constant(data={"msg": "hello from S3"}, nickname="proxy_demo");
        let future = laila.memorize(entry, pool_nickname="origin_s3");
        laila.wait(future);

        print(laila.alpha_pool.exists(entry.global_id));  // False -- not cached yet
        assert(not laila.alpha_pool.exists(entry.global_id));

        // Read through the alpha pool -- cascades to S3, caches on the way back
        let blob = laila.alpha_pool[entry.global_id];

        print(laila.alpha_pool.exists(entry.global_id));  // True  -- cached in memory
        print(hdf5_pool.exists(entry.global_id));  // True  -- cached on disk
        print(s3_pool.exists(entry.global_id));  // True  -- the origin
        assert(laila.alpha_pool.exists(entry.global_id));
        assert(hdf5_pool.exists(entry.global_id));
        assert(s3_pool.exists(entry.global_id));
        // A pool holds serialized ``Record`` envelopes; the entry comes back
        // through the record's builder (what ``laila.remember`` does).
        let entry_back = Record._build_sync(blob)["entry"];
        assert(entry_back.data == {"msg": "hello from S3"});
        assert(entry_back.data["msg"] == "hello from S3");
        assert("msg" in entry_back.data and "nope" not in entry_back.data);
        laila.terminate();
    }

    fn python_shapes(items: impl IntoArg) -> Arg {
        let items = items.into_arg();
        let doubled = [x * 2 for x in items if x > 1];
        let d = {str(x): x for x in items};
        let total = 0;
        for x in items {
            total += x;
        }
        if not items {
            return None;
        }
        let label = f "n={len(items)} total={total} first={items[0]!r}";
        return {"doubled": doubled, "d": d, "label": label, "neg": items[-1], "slice": items[1:]};
    }

    fn raises_and_catches() {
        with pytest.raises(ValueError) as cm {
            raise ValueError("boom");
        }
        assert(str(cm.value) == "boom");
        let f = lambda x: x + 1;
        assert(f(1) == 2);
        let r = floordiv(7, 2);
        assert(r == 3 and floordiv(-7, 2) == -4);
        let p = 2 ** 10;
        assert(p == 1024);
        let s = "ab" * 2 + "c";
        assert(s == "ababc");
        let n = -3 % 5;
        assert(n == 2);
        with pytest.raises(TypeError) as cm {
            let _ = 1 + "x";
        }
        assert(str(cm.value) == "unsupported operand type(s) for +: 'int' and 'str'");
    }
}

#[test]
fn readme_example_runs() {
    let secrets = s3_secrets().unwrap();
    if secrets.is_none() {
        eprintln!("skipped: ~/.laila/secrets/s3_test.toml required for the S3 origin");
        return;
    }
    readme_pool_chain(secrets).unwrap();
}

#[test]
fn python_expression_shapes() {
    let out = python_shapes(py!([1, 2, 3])).unwrap();
    assert_eq!(repr(out.__getitem__("doubled").unwrap()), "[4, 6]");
    assert_eq!(
        repr(out.__getitem__("d").unwrap()),
        "{'1': 1, '2': 2, '3': 3}"
    );
    assert_eq!(out.__getitem__("label").unwrap(), "n=3 total=6 first=1");
    assert_eq!(out.__getitem__("neg").unwrap(), 3);
    assert_eq!(repr(out.__getitem__("slice").unwrap()), "[2, 3]");
    assert_eq!(python_shapes(py!([])).unwrap(), Arg::None);
}

#[test]
fn raises_lambda_floordiv_pow() {
    raises_and_catches().unwrap();
}
