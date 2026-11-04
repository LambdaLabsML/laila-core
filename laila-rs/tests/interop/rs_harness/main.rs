//! Rust `laila` peer harness for the N-language interop matrix.
//!
//! Port of `tests/interop/harness/py_harness.py`: implements the harness CLI
//! contract documented in `tests/interop/README.md` (the same contract
//! `laila-c/tests/interop/c_harness.cpp`, `laila-js/tests/interop/js_harness.js`
//! and `laila-go/tests/interop/go_harness/main.go` implement), so the
//! orchestrator in `tests/interop/` can drive an unmodified Rust `laila`
//! policy exactly like any other language.
//!
//! Modes
//! -----
//! ```text
//! serve <scheme> <port> <secret> <value> [peer_uri ...]
//!     Host a policy listening on <scheme> (``tcp`` or ``ws``); seed <value> into
//!     the alpha pool and into a named ``remote-store`` pool; peer with every
//!     <peer_uri> using <secret>. Prints ``POLICY``, ``ALPHA_GID``, ``STORE_GID``,
//!     one ``PEER <uri> <gid>`` per peer, then ``READY <port>`` and idles until
//!     SIGTERM / SIGINT.
//! remember <uri> <secret> <gid> [pool_nickname]      -> VALUE <string>
//! memorize <uri> <secret> <value> [pool_nickname]    -> MEMO <gid>
//! memorize_typed <uri> <secret> <kind> [pool_nickname]
//!                                                    -> MEMO <gid>
//! remember_typed <uri> <secret> <gid> <kind>         -> TYPED_OK <kind> | TYPED_MISMATCH <kind> <repr>
//! memorize_json <uri> <secret> <json> [pool_nickname]
//!                                                    -> MEMO <gid>
//! remember_json <uri> <secret> <gid> [pool_nickname] -> JSON <json>
//! memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]
//!                                                    -> MEMO <gid> <fnv1a64>
//! remember_hash <uri> <secret> <gid> [pool_nickname] -> HASH <fnv1a64> <len>
//! relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]
//!                                                    -> VALUE <string>
//! forget <uri> <secret> <gid> [pool_nickname]        -> FORGOT <gid>
//! ```
//!
//! Any failure prints `ERROR <message>` and exits 1.
//!
//! Build: `cd laila-rs && cargo build -p laila-interop-harness`
//! (`tests/interop/langs/rust.py` does this on demand with
//! `LAILA_INTEROP_BUILD_RUST=1`).
//!
//! The modes are the Python harness's bodies in `laila::python!`; the three
//! helpers that have no Python-shaped twin (the 64-bit FNV arithmetic, the
//! shared xorshift32 stream and `signal.signal` for `serve`) are plain Rust
//! above the block.
#![allow(
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unused_assignments,
    clippy::all
)]

use std::sync::atomic::{AtomicBool, Ordering};

use laila::macros::defaults::{
    DefaultPolicy, DefaultPool, DefaultTCPIPProtocol, DefaultTCPProtocol,
};
use laila::prelude::*;

const USAGE: &str = "usage: rs_harness <mode> [args...]
  serve <scheme> <port> <secret> <value> [peer_uri ...]
  remember <uri> <secret> <gid> [pool_nickname]
  memorize <uri> <secret> <value> [pool_nickname]
  memorize_typed <uri> <secret> <kind> [pool_nickname]
  remember_typed <uri> <secret> <gid> <kind>
  memorize_json <uri> <secret> <json> [pool_nickname]
  remember_json <uri> <secret> <gid> [pool_nickname]
  memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]
  remember_hash <uri> <secret> <gid> [pool_nickname]
  relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]
  forget <uri> <secret> <gid> [pool_nickname]
";

fn positional(args: &Args, i: usize) -> Arg {
    args.positional.get(i).cloned().unwrap_or(Arg::None)
}

/// `xorshift_bytes(n, seed)`: the deterministic byte stream shared by every
/// harness -- xorshift32 seeded with *seed* (forced non-zero), one low byte
/// per step. Keep in sync with `xorshift_bytes` in py_harness.py /
/// js_harness.js / c_harness.cpp / go_harness.
pub fn xorshift_bytes(args: Args) -> Result<Arg> {
    let n = positional(&args, 0)
        .as_i64()
        .ok_or_else(|| LailaError::type_error("xorshift_bytes: n must be an int"))?;
    let seed = positional(&args, 1)
        .as_i64()
        .ok_or_else(|| LailaError::type_error("xorshift_bytes: seed must be an int"))?;
    let n = usize::try_from(n)
        .map_err(|_| LailaError::value_error("xorshift_bytes: n must be >= 0"))?;
    let mut x = (seed as u64 & 0xFFFF_FFFF) as u32;
    if x == 0 {
        x = 1;
    }
    let mut out = vec![0u8; n];
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = (x & 0xff) as u8;
    }
    Ok(Arg::Bytes(out.into()))
}

/// `fnv1a64(data)`: FNV-1a 64 as 16 lowercase hex digits (laila-C has no
/// SHA-256, so this is the common checksum across harnesses).
pub fn fnv1a64(args: Args) -> Result<Arg> {
    let data = positional(&args, 0)
        .as_bytes_like()
        .ok_or_else(|| LailaError::type_error("fnv1a64: a bytes-like object is required"))?;
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for b in data {
        h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3);
    }
    Ok(Arg::Str(format!("{h:016x}").into()))
}

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

/// `wait_for_stop()`: `signal.signal(SIGTERM / SIGINT, stop.set)` plus the
/// `while not stop.is_set(): time.sleep(0.05)` idle loop of `serve`.
pub fn wait_for_stop(_args: Args) -> Result<Arg> {
    let handler = on_signal as extern "C" fn(libc::c_int) as *const () as libc::sighandler_t;
    // SAFETY: installing a handler that only stores to an atomic.
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
    while !STOP.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    Ok(Arg::None)
}

laila::python! {
    // Fixed typed payloads. Every harness must build byte-compatible values
    // for these kinds; keep in sync with ``TYPED`` in py_harness.py /
    // js_harness.js / go_harness and ``build_typed`` in c_harness.cpp.
    fn _typed() -> Arg {
        return {
            "int": 1234567,
            "double": 3.5,
            "bool": True,
            "bytes": b"\x00\x01\xfe\xff",
            "dict": {"a": 1, "b": [1, 2, 3]},
            "numpy": np.array([[1.0, 2.0], [3.0, 4.0]], dtype="<f4"),
        };
    }

    fn _say(line: Arg) {
        print(line, flush=True);
    }

    #[pysig("_wait(fut, timeout=60.0)")]
    fn _wait(args: Args) -> Arg {
        if fut is not None and hasattr(fut, "wait") {
            return fut.wait(timeout=timeout);
        }
        return fut;
    }

    fn _activate() -> Arg {
        let policy = DefaultPolicy();
        laila.activate_policy(policy);
        return policy;
    }

    #[pysig("_add_transports(secret, *, serve_scheme=None, port=0)")]
    fn _add_transports(args: Args) -> Arg {
        // Register a ``tcp://`` and a ``ws://`` protocol on the active policy
        // so it can dial either scheme; the one matching *serve_scheme* binds
        // *port*.
        let comm = laila.get_active_policy().central.communication;
        let tcp = DefaultTCPProtocol(
            host="127.0.0.1",
            port=port if serve_scheme == "tcp" else 0,
            peer_secret_key=secret,
        );
        comm.add_connection(tcp);
        let ws = DefaultTCPIPProtocol(
            host="127.0.0.1",
            port=port if serve_scheme == "ws" else 0,
            peer_secret_key=secret,
        );
        comm.add_connection(ws);
        return {"tcp": tcp, "ws": ws};
    }

    fn _typed_matches(kind: Arg, got: Arg) -> bool {
        let expected = _typed()[kind];
        if kind == "numpy" {
            return isinstance(got, np.ndarray)
                and got.dtype == expected.dtype
                and got.shape == expected.shape
                and np.array_equal(got, expected);
        }
        return r#type(got) is r#type(expected) and got == expected;
    }

    // -----------------------------------------------------------------------
    // modes
    // -----------------------------------------------------------------------

    fn do_serve(scheme: Arg, port: Arg, secret: Arg, value: Arg, peers: Arg) -> i64 {
        let policy = _activate();

        let alpha = laila.constant(data=value);
        _wait(laila.memorize(alpha));

        laila.memory.extend(pool=DefaultPool(), pool_nickname="remote-store");
        let store = laila.constant(data=value);
        _wait(laila.memorize(store, pool_nickname="remote-store"));

        let protos = _add_transports(secret, serve_scheme=scheme, port=port);

        _say(f "POLICY {policy.global_id}");
        _say(f "ALPHA_GID {alpha.global_id}");
        _say(f "STORE_GID {store.global_id}");
        for uri in peers {
            let pid = laila.add_peer(uri, secret);
            _say(f "PEER {uri} {pid}");
        }
        _say(f "READY {protos[scheme].bound_port}");

        wait_for_stop();
        return 0;
    }

    fn do_remember(uri: Arg, secret: Arg, gid: Arg, nick: Arg) -> i64 {
        _activate();
        _add_transports(secret);
        let pid = laila.add_peer(uri, secret);
        let fut = laila.remember(gid, dst_policy=pid, dst_pool=nick, persist=False);
        let entry = _wait(fut);
        _say(f "VALUE {entry.data}");
        return 0;
    }

    fn _push(uri: Arg, secret: Arg, data: Arg, nick: Arg) -> Arg {
        // Peer with *uri* and memorize a new constant holding *data* there
        // (into the pool *nick* when given). Returns the entry.
        _activate();
        _add_transports(secret);
        let pid = laila.add_peer(uri, secret);
        let e = laila.constant(data=data);
        _wait(laila.memorize(e, dst_policy=pid, dst_pool=nick));
        return e;
    }

    fn _pull(uri: Arg, secret: Arg, gid: Arg, nick: Arg) -> Arg {
        // Peer with *uri* and read *gid* (from the pool *nick* when given).
        _activate();
        _add_transports(secret);
        let pid = laila.add_peer(uri, secret);
        return _wait(laila.remember(gid, dst_policy=pid, dst_pool=nick, persist=False));
    }

    fn do_memorize(uri: Arg, secret: Arg, value: Arg, nick: Arg) -> i64 {
        let e = _push(uri, secret, value, nick);
        _say(f "MEMO {e.global_id}");
        return 0;
    }

    fn do_memorize_typed(uri: Arg, secret: Arg, kind: Arg, nick: Arg) -> i64 {
        if kind not in _typed() {
            raise ValueError(f "unknown typed kind: {kind}");
        }
        let e = _push(uri, secret, _typed()[kind], nick);
        _say(f "MEMO {e.global_id}");
        return 0;
    }

    fn do_memorize_json(uri: Arg, secret: Arg, text: Arg, nick: Arg) -> i64 {
        let e = _push(uri, secret, json.loads(text), nick);
        _say(f "MEMO {e.global_id}");
        return 0;
    }

    fn do_remember_json(uri: Arg, secret: Arg, gid: Arg, nick: Arg) -> i64 {
        let mut got = _pull(uri, secret, gid, nick).data;
        if isinstance(got, np.ndarray) {
            got = got.tolist();
        }
        // ensure_ascii keeps the reply a single ASCII line whatever the stdout codec.
        _say("JSON " + json.dumps(got, ensure_ascii=True, allow_nan=False));
        return 0;
    }

    fn do_memorize_bytes(uri: Arg, secret: Arg, n: Arg, seed: Arg, nick: Arg) -> i64 {
        let data = xorshift_bytes(n, seed);
        let e = _push(uri, secret, data, nick);
        _say(f "MEMO {e.global_id} {fnv1a64(data)}");
        return 0;
    }

    fn do_remember_hash(uri: Arg, secret: Arg, gid: Arg, nick: Arg) -> i64 {
        let got = _pull(uri, secret, gid, nick).data;
        if not (isinstance(got, bytes) or isinstance(got, bytearray)) {
            _say(f "HASH_TYPE {r#type(got).__name__}");
            return 0;
        }
        _say(f "HASH {fnv1a64(bytes(got))} {len(got)}");
        return 0;
    }

    fn do_remember_typed(uri: Arg, secret: Arg, gid: Arg, kind: Arg) -> i64 {
        if kind not in _typed() {
            raise ValueError(f "unknown typed kind: {kind}");
        }
        _activate();
        _add_transports(secret);
        let pid = laila.add_peer(uri, secret);
        let entry = _wait(laila.remember(gid, dst_policy=pid, persist=False));
        let got = entry.data;
        if _typed_matches(kind, got) {
            _say(f "TYPED_OK {kind}");
            return 0;
        }
        _say(f "TYPED_MISMATCH {kind} {r#type(got).__name__}:{got!r}");
        return 0;
    }

    fn do_relay(via_uri: Arg, secret: Arg, from_gid: Arg, gid: Arg, nick: Arg) -> i64 {
        // 3-party: this policy (A) asks the policy at *via_uri* (B) to pull
        // *gid* from the policy *from_gid* (C, which B is already peered to;
        // optionally from C's pool *nick*) into B's own memory; then A reads
        // it back from B.
        _activate();
        _add_transports(secret);
        let via = laila.add_peer(via_uri, secret);
        _wait(laila.remember(gid, src_policy=via, dst_policy=from_gid, dst_pool=nick));
        let entry = _wait(laila.remember(gid, dst_policy=via, persist=False));
        _say(f "VALUE {entry.data}");
        return 0;
    }

    fn do_forget(uri: Arg, secret: Arg, gid: Arg, nick: Arg) -> i64 {
        _activate();
        _add_transports(secret);
        let pid = laila.add_peer(uri, secret);
        _wait(laila.forget(gid, policy=pid, pool=nick));
        _say(f "FORGOT {gid}");
        return 0;
    }

    fn usage() -> i64 {
        sys.stderr.write(USAGE);
        return 2;
    }

    fn _nick(a: Arg, i: Arg) -> Arg {
        return a[i] if len(a) > i else None;
    }

    fn _main(argv: Arg) -> i64 {
        if len(argv) < 2 {
            return usage();
        }
        let (mode, a) = (argv[1], argv[2:]);
        try {
            if mode == "serve" and len(a) >= 4 {
                return do_serve(a[0], int(a[1]), a[2], a[3], a[4:]);
            }
            if mode == "remember" and len(a) >= 3 {
                return do_remember(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "memorize" and len(a) >= 3 {
                return do_memorize(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "memorize_typed" and len(a) >= 3 {
                return do_memorize_typed(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "remember_typed" and len(a) >= 4 {
                return do_remember_typed(a[0], a[1], a[2], a[3]);
            }
            if mode == "memorize_json" and len(a) >= 3 {
                return do_memorize_json(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "remember_json" and len(a) >= 3 {
                return do_remember_json(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "memorize_bytes" and len(a) >= 4 {
                return do_memorize_bytes(a[0], a[1], int(a[2]), int(a[3]), _nick(a, 4));
            }
            if mode == "remember_hash" and len(a) >= 3 {
                return do_remember_hash(a[0], a[1], a[2], _nick(a, 3));
            }
            if mode == "relay" and len(a) >= 4 {
                return do_relay(a[0], a[1], a[2], a[3], _nick(a, 4));
            }
            if mode == "forget" and len(a) >= 3 {
                return do_forget(a[0], a[1], a[2], _nick(a, 3));
            }
            return usage();
        } except Exception as exc {
            _say(f "ERROR {r#type(exc).__name__}: {exc}");
            return 1;
        } finally {
            try {
                laila.get_active_policy().central.communication.stop();
            } except Exception {
                pass;
            }
        }
    }

    fn main() {
        let code = _main(sys.argv);
        sys.stdout.flush();
        // Policies own background threads; don't let a straggler keep the
        // harness alive.
        os._exit(code);
    }
}
