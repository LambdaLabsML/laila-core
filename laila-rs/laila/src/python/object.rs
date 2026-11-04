//! `Arg`: laila's Python object.
//!
//! One enum stands in for Python's `object`. Everything laila passes around
//! (payload data, entries, pools, futures, callables) is an `Arg`, and the
//! enum implements Python's protocols with Python's semantics: `__eq__`
//! (`1 == 1.0 == True`), `__hash__` (unhashable containers raise
//! `TypeError`), truthiness, `__len__`, `__getitem__` with negative indices
//! and slices, `__contains__`, `__iter__`, `__str__`, `__repr__`.
//!
//! Containers are shared and mutable (`Arc<RwLock<..>>`) so a clone aliases
//! the same list/dict exactly like a Python reference; scalars, strings and
//! bytes are immutable.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;
use core::hash::{Hash, Hasher};
use indexmap::{IndexMap, IndexSet};
use num_bigint::BigInt;

use super::args::Args;
use super::errors::{LailaError, Result};
use super::sync::RwLock;

/// Shared, mutable Python list.
pub type PyList = Arc<RwLock<Vec<Arg>>>;
/// Shared, mutable, insertion-ordered Python dict.
/// `IndexMap` with the crate-wide dict hasher (see `_compat::hash`).
pub type PyMap<K, V> = IndexMap<K, V, crate::_compat::hash::DictHasher>;
pub type PyDict = Arc<RwLock<PyMap<Arg, Arg>>>;
/// Insertion-ordered set storage (Python `set` / `frozenset`).
pub type PySetInner = IndexSet<Arg, crate::_compat::hash::DictHasher>;
/// Shared, mutable Python set.
pub type PySet = Arc<RwLock<PySetInner>>;
/// Shared, mutable Python bytearray.
pub type PyByteArray = Arc<RwLock<Vec<u8>>>;
/// A Python callable (`lambda`, `def`, bound method).
pub type Callable = Arc<dyn Fn(Args) -> Result<Arg> + Send + Sync>;

/// A Python object.
#[derive(Clone)]
pub enum Arg {
    None,
    Bool(bool),
    Int(i64),
    /// A Python `int` outside the `i64` range (always normalised: an `Arg::Long`
    /// never holds a value that fits in `Arg::Int`).
    Long(Arc<BigInt>),
    Float(f64),
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    ByteArray(PyByteArray),
    List(PyList),
    Tuple(Arc<[Arg]>),
    Dict(PyDict),
    Set(PySet),
    FrozenSet(Arc<PySetInner>),
    /// `numpy.ndarray`.
    NDArray(Arc<crate::_compat::ndarray::NDArray>),
    Entry(crate::entry::Entry),
    Pool(crate::data::Pool),
    Future(crate::policy::central::command::Future),
    Callable(Callable),
    Exception(Arc<LailaError>),
    /// Any other object (enums, user classes, stdlib shims) through the
    /// `PyObject` vtable, so `==`, `repr` and `hash` follow the type.
    Object(Arc<dyn PyObject>),
}

/// The protocol a Rust type implements to live inside `Arg::Object`.
/// `laila::py_object!(Type)` implements it for any `PartialEq + Debug + Hash`
/// type; enums (`EntryState`, `FutureStatus`) use it.
pub trait PyObject: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn type_name(&self) -> &'static str;
    fn __repr__(&self) -> String;
    /// `str(obj)`; defaults to `repr(obj)` like Python.
    fn __str__(&self) -> String {
        self.__repr__()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool;
    /// `self == other` for any Python value (numpy's `dtype == "<f8"`,
    /// `DotMap == dict`); defaults to `__eq__` for objects, `False` otherwise.
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64>;
    /// `obj.name` for attribute-style access on dynamic objects.
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute '{name}'",
            self.type_name()
        )))
    }
    /// `obj.name = value` on dynamic objects.
    fn __setattr__(&self, name: &str, _value: Arg) -> Result<()> {
        Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute '{name}'",
            self.type_name()
        )))
    }
    /// `obj.name(*args, **kwargs)` on dynamic objects: method dispatch by
    /// name for values that live in containers (`threads[0].start()`).
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute '{name}'",
            self.type_name()
        )))
    }
    /// `del obj.name` on dynamic objects.
    fn __delattr__(&self, name: &str) -> Result<()> {
        Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute '{name}'",
            self.type_name()
        )))
    }
    /// `len(obj)`; the default is Python's `TypeError` for types without `__len__`.
    fn __len__(&self) -> Result<usize> {
        Err(LailaError::type_error(alloc::format!(
            "object of type '{}' has no len()",
            self.type_name()
        )))
    }
    /// `bool(obj)`; defaults to `len(obj) != 0` when the type has `__len__`, else `True`.
    fn __bool__(&self) -> bool {
        match self.__len__() {
            Ok(n) => n != 0,
            Err(_) => true,
        }
    }
    /// `item in obj`; the default is Python's `TypeError` for non-iterables.
    fn __contains__(&self, _item: &Arg) -> Result<bool> {
        Err(LailaError::type_error(alloc::format!(
            "argument of type '{}' is not iterable",
            self.type_name()
        )))
    }
    /// `iter(obj)` as a snapshot; the default is Python's `TypeError`.
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object is not iterable",
            self.type_name()
        )))
    }
    /// Is this an *iterator* (`__next__`), pulled lazily by `for` rather
    /// than snapshotted by `__iter__`? (A `Relay`, a generator.)
    fn __is_iterator__(&self) -> bool {
        false
    }
    /// `next(obj)`: the next item, or `StopIteration` when exhausted.
    fn __next__(&self) -> Result<Arg> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object is not an iterator",
            self.type_name()
        )))
    }
    /// `obj[key]`; the default is Python's `TypeError`.
    fn __getitem__(&self, _key: &Arg) -> Result<Arg> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object is not subscriptable",
            self.type_name()
        )))
    }
    /// `obj[key] = value`; the default is Python's `TypeError`.
    fn __setitem__(&self, _key: &Arg, _value: Arg) -> Result<()> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object does not support item assignment",
            self.type_name()
        )))
    }
    /// `del obj[key]`; the default is Python's `TypeError`.
    fn __delitem__(&self, _key: &Arg) -> Result<()> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object does not support item deletion",
            self.type_name()
        )))
    }
    /// `dir(obj)`: the attribute names a dynamic object exposes (module
    /// objects, model instances). Defaults to none.
    fn __dir__(&self) -> Vec<String> {
        Vec::new()
    }
    /// The builtin types the object's class derives from, by name
    /// (`OrderedDict` -> `["dict"]`): the MRO walk of `isinstance`-style
    /// dispatch on builtin ancestors (`ComputationalData`'s taxonomy).
    fn __bases__(&self) -> &'static [&'static str] {
        &[]
    }
    /// `copy.deepcopy(obj)` for objects with interior state; `None` means
    /// the handle itself is the copy (immutable / identity objects).
    fn __deepcopy__(&self) -> Option<Arg> {
        None
    }
    /// `copy.copy(obj)` for objects with a shallow-copy protocol; `None`
    /// means the handle itself is the copy.
    fn __copy__(&self) -> Option<Arg> {
        None
    }
    /// `obj(*args, **kwargs)` for callable objects (class objects construct);
    /// defaults to Python's `TypeError: '<type>' object is not callable`.
    fn __call__(&self, _args: Args) -> Result<Arg> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object is not callable",
            self.type_name()
        )))
    }
    /// `type(obj)` for objects that know their Python class (model
    /// instances, module-level class aliases). Defaults to `None`, meaning
    /// "a builtin whose class is named by `type_name`".
    fn __class__(&self) -> Option<super::pyclass::PyClass> {
        None
    }
    /// The CLI-eligible `(field, value)` pairs of a typed laila object
    /// (what `_eligible_model_fields` + the property setters yield for a
    /// pydantic model): the `laila.args.environment` mirror of a policy,
    /// command, taskforce, memory or router. `None` for everything else.
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        None
    }
    /// `isinstance(obj, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT)`: the
    /// per-instance re-entrant lock of an identifiable atomic object, used by
    /// `@synchronized` to lock every such argument in `id()` order. `None`
    /// for everything else (including the non-identifiable `Atomic*` types).
    fn __atomic_lock__(&self) -> Option<super::sync::RLock> {
        None
    }
    /// `self is other` for two *distinct* allocations that stand for the
    /// same Python object (class objects built on demand, the
    /// `NotImplemented` singleton). Pointer equality is checked first by
    /// [`Arg::is`]; this hook only extends it. Defaults to `False`.
    fn __is__(&self, _other: &dyn PyObject) -> bool {
        false
    }
}

/// An object with a Python `__dict__`: `py_class!` structs and every typed
/// laila object that stores dynamic / private attributes in a [`PyAttrs`].
/// The `laila.atomic` mixin traits are implemented on top of it (the local
/// lock lives under `_local_lock`, exactly where Python keeps it).
pub trait HasAttrs {
    fn __attrs__(&self) -> &PyAttrs;
    /// `type(self).__name__`
    fn __class_name__(&self) -> &'static str;
}

/// `return expr` in a `laila::python!` function declared `-> T`: the value
/// is converted when it is a dynamic `Arg` holding a `T` (`def f() -> int:
/// return self.n`), and passed through unchanged otherwise.
pub trait RetInto<T> {
    fn ret_into(self) -> Result<T>;
}
impl<T> RetInto<T> for T {
    fn ret_into(self) -> Result<T> {
        Ok(self)
    }
}
macro_rules! ret_into_from_arg {
    ($($t:ty => $m:ident $name:literal),* $(,)?) => {$(
        impl RetInto<$t> for Arg {
            fn ret_into(self) -> Result<$t> {
                self.$m().map(Into::into).ok_or_else(|| LailaError::type_error(
                    alloc::format!("expected {}, got '{}'", $name, self.type_name())))
            }
        }
    )*};
}
ret_into_from_arg! { i64 => as_i64 "int", f64 => as_f64 "float", bool => as_bool "bool", String => as_str "str" }
impl RetInto<f64> for i64 {
    fn ret_into(self) -> Result<f64> {
        Ok(self as f64)
    }
}
impl RetInto<i64> for usize {
    fn ret_into(self) -> Result<i64> {
        Ok(self as i64)
    }
}
impl RetInto<i64> for i32 {
    fn ret_into(self) -> Result<i64> {
        Ok(self as i64)
    }
}
impl RetInto<f64> for usize {
    fn ret_into(self) -> Result<f64> {
        Ok(self as f64)
    }
}
impl RetInto<usize> for Arg {
    fn ret_into(self) -> Result<usize> {
        self.as_i64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "expected a non-negative int, got '{}'",
                    self.type_name()
                ))
            })
    }
}
/// `def f() -> bytes: return data[:n]`.
impl RetInto<Vec<u8>> for Arg {
    fn ret_into(self) -> Result<Vec<u8>> {
        self.as_bytes_like().ok_or_else(|| {
            LailaError::type_error(alloc::format!("expected bytes, got '{}'", self.type_name()))
        })
    }
}
/// `_blob(b"literal")` into a `data: Vec<u8>` parameter.
impl<const N: usize> RetInto<Vec<u8>> for &[u8; N] {
    fn ret_into(self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
}
impl RetInto<Vec<u8>> for &[u8] {
    fn ret_into(self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
}
/// `return self` from a `-> Self` method (`__enter__`), or any `-> T`
/// returning a borrowed `T`: a clone (a handle alias for `Arg` / classes).
impl<T: Clone> RetInto<T> for &T {
    fn ret_into(self) -> Result<T> {
        Ok(self.clone())
    }
}
/// A `-> None` method: whatever the dynamic twin returned is dropped.
impl RetInto<()> for Arg {
    fn ret_into(self) -> Result<()> {
        Ok(())
    }
}

/// Method-style access on a dynamically typed value: every laila / shim
/// method name gets a forwarding method on `Arg` that dispatches through
/// `__call_method__`. Each takes Python-style `Args` (the build script adds
/// these names to the rewriter's `Args` table).
macro_rules! arg_methods {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "(..)` on a dynamic value.")]
                pub fn $name(&self, args: impl Into<Args>) -> Result<Arg> {
                    self.__call_method__(stringify!($name), args.into())
                }
            )*
        }
    };
}
arg_methods! {
    // threading / tempfile / files
    start, is_alive, is_set, acquire, release, locked, cleanup, close, write, flush, read,
    // unittest.mock
    stop, reset_mock, assert_called, assert_called_once, assert_not_called,
    // numpy
    reshape, astype, item,
    // entry / pool / future verbs that may be called on container elements
    exists, memorize, remember, forget, evolve, build, wait, submit, terminate, duplicate, guarantee,
    serialize, _build, _build_from_dict, sub_manifest, floating_leaves, sql, build_index,
    clear_index, _snapshot, _build_sync, _build_inplace, _build_async,
    mark_memorized, bump_evolution_if_locally_modified, _build_from_dict_sync,
    _build_from_dict_async, from_dict, _candidate_pools,
    // contextvars (`var.set(v)` on a `ContextVar` module value)
    set,
    // datetime
    isoformat, strftime, astimezone,
    // io.BytesIO / msgpack.Unpacker / Fernet
    seek, truncate, feed, encrypt, encrypt_at_time, decrypt, decrypt_at_time, extract_timestamp,
    rotate,
    // DotMap
    has_key, empty,
    // pathlib.Path (`(tmp_path / name).write_text(body)` on a dynamic path)
    write_text, read_text, write_bytes, read_bytes, mkdir, touch, unlink, is_file, is_dir,
    // misc Python protocol names
    __exit__,
    // identifiable objects / pydantic models (basics)
    from_global_id, resolve_global_id, to_global_id, process_global_id, model_dump,
    // laila.atomic (names whose Python signature has defaults / keywords)
    lock, lock_global, global_locked, trim, increment, pretty, run_atomic, atomic,
    // pools / PoolIndex / router / central memory (keyword-taking methods)
    sync, _keys, by_creation_timestamp, invalidate, _shard, route, _route_by_nickname,
    _read_entries_direct, put,
    // DuckDBPool (`checkpoint(compact=..)`) / its raw connection (`duckdb`,
    // `sqlite3` cursors: `execute(sql, params)`, `fetchall()`)
    checkpoint, connect, execute, fetchall, _note_churn,
    // numpy.random.Generator
    random, integers,
    // command / taskforces / futures (keyword-taking methods)
    shutdown, imap, add_taskforce, _queue_submit, clear_callbacks, trigger_callback, set_result,
    set_exception, cancel, _wait_impl, _guarantee_enter, _guarantee_exit,
    _register_future_with_active_guarantees, _get_future_status, _get_future_exception,
    _get_future_result_id, _wait_future, _get_future_result_entry, _wait_future_entry,
    _read_entries_async, _parallel_individual_record,
    _parallel_individual_fetch, _parallel_individual_delete, _batch_accelerated_record,
    _batch_accelerated_fetch, _batch_accelerated_delete, _record, _fetch,
    _duplicate_pool, _resolve, spawn_group, spawn_error_group, sleep_and_return, raise_after,
    // laila.utils.args.ArgReader
    load, from_json, from_toml, from_env, from_xml, from_terminal, _apply,
    // laila.logger.Logger (keyword-taking emitters) / stdlib `logging`
    info, debug, warning, critical, emit, set_level, record_memorize, record_remember,
    record_forget, record_future_created, record_future_transition, record_group_future_created,
    _memorize_record, _remove_installed_handlers, log,
    // policy.central.communication: hub / transport / proxy verbs and the
    // remote methods the RPC tests install on a peer (`object.__setattr__`)
    add_connection, has_peer, matches_token, can_handle_uri, watch,
    _resolve_protocol_for_token, _handle_request_frame, _register_peer, _fail_pending,
    _register_pending, echo, echo_a, echo_b, fib, get_val, set_val, push, logic_and,
    generate_uuid_from_nickname, square, sum_all, process, half, inc, kw, kw_count,
    is_laila_resource, from_a, from_b, format_msg, divide, disconnect, count_users,
    // stream lanes (`channel.send(..)`, `proxy.channels()`) and the
    // stream-lane test fixtures (`peer.emit_threadsafe(..)`)
    send, channel_names, emit_threadsafe, send_raw_frame_threadsafe,
    send_obj_threadsafe, _write_raw, _peer_has_lanes,
}

/// One-argument method forwarders (`x.set_to(v)`, `d.key_at(i)`): Python
/// methods with exactly one positional parameter keep their typed spelling.
macro_rules! arg_methods1 {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "(x)` on a dynamic value.")]
                pub fn $name(&self, x: impl IntoArg) -> Result<Arg> {
                    self.__call_method__(stringify!($name), Args::from(x.into_arg()))
                }
            )*
        }
    };
}
arg_methods1! {
    // laila.atomic
    unlock_global, set_to, add, get_at, item_at, key_at, value_at,
    // pools / PoolIndex (one positional parameter)
    delete, _read, _delete, _exists, _candidate_keys, _index_remove, candidates, latest,
    shard_id, _shard_uuid, _flush, _drop_shard, __lshift__, __rshift__, __le__, _resolve_pool_ref,
    // stdlib `logging` (`root.addHandler(h)`, `h.setLevel(logging.DEBUG)`)
    addHandler, removeHandler, setLevel, setFormatter, handle, isEnabledFor,
    // compdata transformations (`op.forward(data)`, `op.backward(blob)`)
    forward, backward,
    // policy.central.communication (one positional parameter)
    via, remove_peer, has_entry, remove_connection, _resolve_protocol_for_uri, _holder_protocol,
    _unregister_peer,
}

/// Two-argument method forwarders (`lst.set_at(i, v)`, `d.compute(k, f)`).
macro_rules! arg_methods2 {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "(a, b)` on a dynamic value.")]
                pub fn $name(&self, a: impl IntoArg, b: impl IntoArg) -> Result<Arg> {
                    let mut args = Args::from(a.into_arg());
                    args.push_positional(b.into_arg());
                    self.__call_method__(stringify!($name), args)
                }
            )*
        }
    };
}
arg_methods2! {
    // laila.atomic
    set_at, compute,
    // pools / PoolIndex (two positional parameters)
    _write, _index_record, _resolve_indexed, _search_keys, nth, record,
    // futures (`fut.add_status_callback(status, fn)`)
    add_callback, add_status_callback, remove_callback,
    // policy.central.communication (two positional parameters) and the
    // remote methods of the RPC tests
    _select_protocol_for_peer, intersect, matmul, add_peer,
}

/// Three-argument method forwarders (`comm._execute_rpc(path, args, kwargs)`).
macro_rules! arg_methods3 {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "(a, b, c)` on a dynamic value.")]
                pub fn $name(&self, a: impl IntoArg, b: impl IntoArg, c: impl IntoArg) -> Result<Arg> {
                    let mut args = Args::from(a.into_arg());
                    args.push_positional(b.into_arg());
                    args.push_positional(c.into_arg());
                    self.__call_method__(stringify!($name), args)
                }
            )*
        }
    };
}
arg_methods3! {
    // policy.central.communication
    _execute_rpc, add_tcpip_peer, _maybe_wrap_remote_future,
}

/// Four-argument method forwarders (`proto.send_rpc(peer, path, args, kwargs)`).
macro_rules! arg_methods4 {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "(a, b, c, d)` on a dynamic value.")]
                pub fn $name(
                    &self,
                    a: impl IntoArg,
                    b: impl IntoArg,
                    c: impl IntoArg,
                    d: impl IntoArg,
                ) -> Result<Arg> {
                    let mut args = Args::from(a.into_arg());
                    args.push_positional(b.into_arg());
                    args.push_positional(c.into_arg());
                    args.push_positional(d.into_arg());
                    self.__call_method__(stringify!($name), args)
                }
            )*
        }
    };
}
arg_methods4! {
    // policy.central.communication
    send_rpc, _send_rpc, _send_once,
}

/// Zero-argument method forwarders (`x.tolist()`, `td.total_seconds()`):
/// Python methods that take no arguments keep their bare spelling, so the
/// `Arg` forwarder is zero-arg too.
macro_rules! arg_methods0 {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "()` on a dynamic value.")]
                pub fn $name(&self) -> Result<Arg> {
                    self.__call_method__(stringify!($name), Args::new())
                }
            )*
        }
    };
}
arg_methods0! {
    // numpy
    tolist, tobytes, flatten,
    // datetime / timedelta / timezone
    weekday, isoweekday, toordinal, utcoffset, tzname, total_seconds, isocalendar,
    // io.BytesIO / msgpack.Unpacker
    getvalue, getbuffer, tell, unpack,
    // DotMap
    to_dict, toDict, popitem,
    // laila.atomic
    toggle, decrement, reset, reindex, unlock, pop_next, to_list, length,
    // futures / taskforces (no-argument predicates and views)
    finished, cancelled, error, not_started, running, what, group_status,
    as_dict, __json__, clear_all_callbacks,
    future_identity, __await__, internal_slots, alpha_slots,
    // `with cm as x:` on a dynamic context manager (`__exit__` is the guard's drop)
    __enter__,
    // identifiable objects (basics)
    has_evolution, identity, identity_as_json,
    // manifests (`m._sql_state` is a private attribute read as a method)
    _sql_state, invalidate_index,
    // pools / PoolIndex / MultiBuffer
    rebuild, _upstream_tiers, _empty, _ensure_migrated, _migrate_legacy_shards, qsize,
    // db cursors (`duckdb`, `sqlite3`) / DuckDBPool maintenance
    fetchone, _should_compact, _compact, _run_pending_checkpoint, _root,
    // stdlib `logging` (`record.getMessage()`, `logger.getEffectiveLevel()`)
    getMessage, getEffectiveLevel, hasHandlers,
    // policy.central.communication (`proxy.ping()` is the installed remote
    // method, `proto.ping(peer)` the transport's: a call with arguments is
    // the dynamic twin) and the remote methods of the RPC tests
    ping, peer, stop_all, report, __subclasses__, _acquire_rpc_slot, _release_rpc_slot,
    _try_admit, _shutdown_executor, _command, _owner_gid, _policy, big, boom, config, constant,
    deep_dict, empty_dict, empty_list, fail_assert, fail_idx, fail_key, fail_rt, fail_type,
    fail_val, flaky, get_alpha_tf, get_counter, get_int, get_items, greet, is_closed, is_running,
    mixed, my_id, neg, nested, no, nonexistent_attribute_xyz, noop, ok, pi, slow, slow_a, slow_b,
    tf_count, uni, yes, zero, who, pool_size, _stop_liveness, _start_liveness, _liveness_running,
    _inflight_rpcs,
    // stream lanes (`proxy.channels()`, `proto._local_caps()`, `selector.get_map()`)
    channels, _local_caps, get_map,
    // serial / file-like handles (`proto._serial.fileno()`)
    fileno,
}

/// Attribute-style access on a dynamically typed value: Tier 2 lowers
/// `x.data` to `x.data()`, so every laila attribute name gets a method on
/// `Arg` that dispatches through `__getattr__` (raising `AttributeError`
/// exactly where Python would). Extend this list when a Tier 3 type gains
/// a public attribute.
macro_rules! arg_attrs {
    ($($name:ident),* $(,)?) => {
        impl Arg {
            $(
                #[doc = concat!("`obj.", stringify!($name), "` on a dynamic value.")]
                pub fn $name(&self) -> Result<Arg> {
                    self.__getattr__(stringify!($name))
                }
            )*
        }
    };
}
arg_attrs! {
    // Entry / Manifest
    data, nickname, evolution, state, uuid, global_id, metadata, creation_time, modification_time,
    constitution, locally_modified, blueprint, realized, async_realized, _pending_entries,
    _source_pool_gid, _locally_modified, _payload, _constitution, _state, _uuid, _scopes,
    _evolution, _creation_timestamp,
    // constitutions / compdata transformations
    code, manifest, manifest_global_id, codes, _kind, backward_code, forward_kwargs,
    backward_kwargs, serializer,
    // Future / command / taskforces
    status, result, command, future, entry, entries, taskforces, alpha_taskforce,
    internal_taskforce, taskforce_id, policy_id, future_group_id, precedence, purpose, exception,
    result_global_id, futures, future_ids, stage_future_ids, children, callbacks,
    _status_callbacks, _default_callbacks, _return_value, _result_pending_wrap, _exception,
    _result_global_id, _result_cache,
    _native_future, native_future, rank, num_workers, max_async_per_thread, sync_workers, backend,
    inflight, parked, future_bank, active_policy, _local_policies, _active_policy_gid, stages,
    stage_fns, is_group, group_future,
    // Pool / PoolIndex / PoolWrapper / router / central memory
    pool_nickname, nickname_to_pool, size, capacity, policy, pools, proxy, pool_id, proxy_to,
    _proxy_to, index_enabled, index_pool, resource, transformations, batch_accelerated,
    _persistent_storage, _persistent_index_identity, _index, _shards, _entries, _missing,
    _identity, _migrated, _owner, pool, pool_router, pools_nicknames, pools_pq, alpha_pool,
    central, memory, file_path, bucket_name, queue, pool_dir, mount_dir, image_path,
    // DuckDBPool / HDF5Pool privates
    memory_limit, _conn, _file, _churn_bytes, _churn_rows, _checkpoint_pending,
    // cloud / DB pools (S3 / Cloudflare / Backblaze / GCS / Azure / HuggingFace /
    // Redis / Postgres / Mongo) fields and privates
    container_name, connection_string, repo_id, repo_type, revision, token, path_prefix,
    project_id, service_account_info, endpoint_url, region_name, account_id, access_key_id,
    secret_access_key, application_key_id, application_key, host, port, user, password, dbname,
    dsn, uri, key_prefix, lock_prefix, lock_timeout, redis_password, redis_dir, redis_hash_key,
    redis_lock_key, max_pool_connections, max_req_per_second, server_start_timeout_s,
    _client, _bucket, _container, _api, _postgres_dir, _owns_local_server, _socket_dir,
    _postgres_proc, _mongo_proc, _mongo_dir, _local_user, _local_dbname, _db_dump_name,
    _redis_proc, _redis_socket_name,
    // unittest.mock
    side_effect, return_value, call_count, called, call_args, call_args_list, kwargs,
    // MultiBuffer / Record
    read_head, write_head, _read_head, _write_head, slots, mapped, recorder, borrower,
    record_timestamp, entry_id,
    // exceptions / misc
    message, args, value, name, kind, payload, timestamp, source, target, flags,
    // numpy / threading / complex
    shape, dtype, ndim, nbytes, itemsize, fortran_order, ident, daemon, real, imag, byteorder,
    // datetime / timedelta
    year, month, day, hour, minute, second, microsecond, tzinfo, fold, days, seconds, microseconds,
    // laila.atomic (private state read by tests: `c._global_lock`, `c._hold_count`)
    _global_lock, _holder_policy_id, _holder_secret, _hold_count, hint,
    // identifiable objects / class objects (basics)
    scopes, creation_timestamp, __name__, __class__, model_fields, json_schema_extra, default,
    // stdlib `logging` / laila.logger.Logger
    handlers, level, levelno, levelname, msg, created, propagate, enabled, display,
    capture_traceback, records, logger_id, _installed_handlers, _last_pool_sink_error,
    _stdlib_root, _in_sink, _initialized,
    // policy.central.communication: the hub, transports, proxies and the
    // teardown harness (`pr.pa`, `pr.ca`)
    communication, peers, connections, peer_secret_key, bound_port, bound_path, protocol_name,
    persistent, supports_ping, _started, _pending_rpcs, _unacked, _inbound_executor,
    _liveness_thread, _communication, calls, deeply,
    pa, pb, ca, cb, a, b, double, nonexistent, __self__, stdout,
    // stream lanes (`Channel` / `StreamEntry` / `StreamMeta`), the UART
    // transports and the stream-lane test fixtures (`_helpers.py`)
    stream, opened, closed, closed_reason, dropped, discarded, tx_dropped, lane_id, tx_lane_id,
    seq, arrived_at, dropped_before, lane, channel, peer_id, mtu, ping_timeout,
    channel_queue_size, stream_chunk_bytes, supports_channels, handshake_params, gid, aid, bid,
    proto, comm, errors, _serial, _event_loop, _loop_thread, _read_transport, _recv_task, _link,
    stream_frames_seen, stream_bytes_seen, rpc_paths, messages_with_drop, responses_seen,
    handshakes, pings_answered, chunks_dropped, _unknown_lane_drops, _reserved_marker_drops,
    _malformed_stream_drops,
    // the HEVC test-stream twin (`_hevc.py`: `ParsedAU` / `AccessUnit`)
    types, is_idr, nals, au_sizes, au_count, idr_count, nal_histogram, total_bytes,
    // the UART link (`proto._link.stream_q`)
    stream_q, rpc_q,
    // `Relay.timeout`, `Channel.rx_seq` / `tx_seq` / `max_message_bytes`
    timeout, rx_seq, tx_seq, max_message_bytes, _tx_chunk_seq, _queue_bytes,
}

/// Implements `PyObject` (+ `IntoArg`, `PyArg`, `isinstance`) for a
/// `Clone + PartialEq + Debug + Hash + Send + Sync + 'static` type.
#[macro_export]
macro_rules! py_object {
    ($t:ty) => {
        impl $crate::python::object::PyObject for $t {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                stringify!($t)
            }
            fn __repr__(&self) -> $crate::python::object::StringReexport {
                $crate::python::object::format_debug(self)
            }
            fn __eq__(&self, other: &dyn $crate::python::object::PyObject) -> bool {
                other.as_any().downcast_ref::<$t>().map(|o| o == self).unwrap_or(false)
            }
            fn __hash__(&self) -> $crate::python::Result<i64> {
                use core::hash::{Hash, Hasher};
                let mut h = $crate::python::object::FnvHasher::default();
                self.hash(&mut h);
                Ok((h.finish() >> 1) as i64)
            }
        }
        impl $crate::python::IntoArg for $t {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::Arg::Object($crate::python::object::ArcReexport::new(self))
            }
        }
        impl $crate::python::PyArg for $t {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::Arg::Object($crate::python::object::ArcReexport::new(self.clone()))
            }
        }
        impl $crate::python::builtins::IsInstance for $t {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                matches!(x, $crate::python::Arg::Object(o) if o.as_any().is::<$t>())
            }
        }
        impl PartialEq<$crate::python::Arg> for $t {
            fn eq(&self, other: &$crate::python::Arg) -> bool {
                matches!(other, $crate::python::Arg::Object(o) if o.as_any().downcast_ref::<$t>() == Some(self))
            }
        }
    };
}

#[doc(hidden)]
pub use alloc::string::String as StringReexport;
#[doc(hidden)]
pub use alloc::sync::Arc as ArcReexport;
#[doc(hidden)]
pub fn format_debug<T: core::fmt::Debug>(t: &T) -> String {
    alloc::format!("{t:?}")
}

/// Small FNV hasher used by `py_object!`.
#[derive(Default)]
#[doc(hidden)]
pub struct FnvHasher(u64);
impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = h;
    }
}

/// Python's `slice(start, stop, step)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Slice {
    pub start: Option<i64>,
    pub stop: Option<i64>,
    pub step: Option<i64>,
}

/// A slice bound as written in Python: an integer, or `None` (open), or
/// an `Arg` holding either (`payload[i * chunk:(i + 1) * chunk]` where
/// `chunk` is dynamic).
pub trait SliceBound {
    fn __laila_slice_bound(&self) -> Option<i64>;
}
macro_rules! slice_bound_int {
    ($($t:ty),*) => {$(
        impl SliceBound for $t {
            fn __laila_slice_bound(&self) -> Option<i64> {
                Some(*self as i64)
            }
        }
    )*};
}
slice_bound_int!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);
impl SliceBound for Arg {
    fn __laila_slice_bound(&self) -> Option<i64> {
        match self {
            Arg::None => None,
            other => other.as_i64(),
        }
    }
}
impl<T: SliceBound + ?Sized> SliceBound for &T {
    fn __laila_slice_bound(&self) -> Option<i64> {
        (**self).__laila_slice_bound()
    }
}
impl<T: SliceBound> SliceBound for Option<T> {
    fn __laila_slice_bound(&self) -> Option<i64> {
        self.as_ref().and_then(SliceBound::__laila_slice_bound)
    }
}

/// `slice(a, b)` / `slice(a, b, c)`; Tier 2 lowers `x[a:b]` to `x.__getitem__(slice(a, b))`.
pub fn slice(start: impl Into<Option<i64>>, stop: impl Into<Option<i64>>) -> Slice {
    Slice {
        start: start.into(),
        stop: stop.into(),
        step: None,
    }
}

impl Slice {
    /// Python's `slice.indices(len)` -> (start, stop, step).
    pub fn indices(&self, len: usize) -> (i64, i64, i64) {
        let len = len as i64;
        let step = self.step.unwrap_or(1);
        let (lo, hi) = if step < 0 { (-1, len - 1) } else { (0, len) };
        let norm = |v: Option<i64>, default: i64| -> i64 {
            match v {
                None => default,
                Some(v) if v < 0 => (v + len).max(lo),
                Some(v) => v.min(hi),
            }
        };
        let start = norm(self.start, if step < 0 { hi } else { lo });
        let stop = norm(self.stop, if step < 0 { lo } else { hi });
        (start, stop, step)
    }
}

/// Python's `type(x).__name__`.
impl Arg {
    pub fn type_name(&self) -> &'static str {
        match self {
            Arg::None => "NoneType",
            Arg::Bool(_) => "bool",
            Arg::Int(_) | Arg::Long(_) => "int",
            Arg::Float(_) => "float",
            Arg::Str(_) => "str",
            Arg::Bytes(_) => "bytes",
            Arg::ByteArray(_) => "bytearray",
            Arg::List(_) => "list",
            Arg::Tuple(_) => "tuple",
            Arg::Dict(_) => "dict",
            Arg::Set(_) => "set",
            Arg::FrozenSet(_) => "frozenset",
            Arg::NDArray(_) => "ndarray",
            Arg::Entry(e) => e.__class_name__(),
            Arg::Pool(p) => p.__class_name__(),
            Arg::Future(_) => "Future",
            Arg::Callable(_) => "function",
            Arg::Exception(e) => e.kind.name(),
            Arg::Object(o) => o.type_name(),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Arg::None)
    }

    /// Python `is`: identity for shared objects, value for immutable scalars.
    pub fn is(&self, other: &Arg) -> bool {
        match (self, other) {
            (Arg::None, Arg::None) => true,
            (Arg::Bool(a), Arg::Bool(b)) => a == b,
            (Arg::Int(a), Arg::Int(b)) => a == b,
            // Immutable scalars passed around as values: the same object.
            (Arg::Float(a), Arg::Float(b)) => a.to_bits() == b.to_bits(),
            (Arg::Str(a), Arg::Str(b)) => Arc::ptr_eq(a, b) || a == b,
            (Arg::List(a), Arg::List(b)) => Arc::ptr_eq(a, b),
            (Arg::Dict(a), Arg::Dict(b)) => Arc::ptr_eq(a, b),
            (Arg::Tuple(a), Arg::Tuple(b)) => Arc::ptr_eq(a, b),
            (Arg::Bytes(a), Arg::Bytes(b)) => Arc::ptr_eq(a, b),
            (Arg::Long(a), Arg::Long(b)) => Arc::ptr_eq(a, b),
            (Arg::ByteArray(a), Arg::ByteArray(b)) => Arc::ptr_eq(a, b),
            (Arg::Set(a), Arg::Set(b)) => Arc::ptr_eq(a, b),
            (Arg::FrozenSet(a), Arg::FrozenSet(b)) => Arc::ptr_eq(a, b),
            (Arg::NDArray(a), Arg::NDArray(b)) => Arc::ptr_eq(a, b),
            (Arg::Entry(a), Arg::Entry(b)) => a.is(b),
            (Arg::Pool(a), Arg::Pool(b)) => a.is(b),
            (Arg::Future(a), Arg::Future(b)) => a.is(b),
            (Arg::Callable(a), Arg::Callable(b)) => Arc::ptr_eq(a, b),
            (Arg::Exception(a), Arg::Exception(b)) => Arc::ptr_eq(a, b) || a.is_same(b),
            (Arg::Object(a), Arg::Object(b)) => Arc::ptr_eq(a, b) || a.__is__(&**b),
            _ => false,
        }
    }

    /// Python truthiness (`bool(x)`).
    pub fn __bool__(&self) -> bool {
        match self {
            Arg::None => false,
            Arg::Bool(b) => *b,
            Arg::Int(i) => *i != 0,
            Arg::Float(f) => *f != 0.0,
            Arg::Str(s) => !s.is_empty(),
            Arg::Long(_) => true,
            Arg::Bytes(b) => !b.is_empty(),
            Arg::ByteArray(b) => !b.read().is_empty(),
            Arg::List(l) => !l.read().is_empty(),
            Arg::Tuple(t) => !t.is_empty(),
            Arg::Dict(d) => !d.read().is_empty(),
            Arg::Set(s) => !s.read().is_empty(),
            Arg::FrozenSet(s) => !s.is_empty(),
            Arg::Entry(e) => e.__bool__(),
            Arg::Object(o) => o.__bool__(),
            _ => true,
        }
    }

    /// Python `len(x)`.
    pub fn __len__(&self) -> Result<usize> {
        Ok(match self {
            Arg::Str(s) => s.chars().count(),
            Arg::Bytes(b) => b.len(),
            Arg::ByteArray(b) => b.read().len(),
            Arg::List(l) => l.read().len(),
            Arg::Tuple(t) => t.len(),
            Arg::Dict(d) => d.read().len(),
            Arg::Set(s) => s.read().len(),
            Arg::FrozenSet(s) => s.len(),
            Arg::NDArray(a) if a.ndim() > 0 => a.shape[0],
            Arg::NDArray(_) => return Err(LailaError::type_error("len() of unsized object")),
            Arg::Entry(e) => return e.__len__(),
            Arg::Future(f) => return crate::python::builtins::PyLen::__len__(f),
            Arg::Pool(p) => return p.keys_vec(false).map(|k| k.len()),
            Arg::Object(o) => return o.__len__(),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "object of type '{}' has no len()",
                    other.type_name()
                )))
            }
        })
    }

    fn norm_index(i: i64, len: usize, what: &str) -> Result<usize> {
        let l = len as i64;
        let j = if i < 0 { i + l } else { i };
        if j < 0 || j >= l {
            return Err(LailaError::new(
                super::errors::ExcKind::IndexError,
                alloc::format!("{what} index out of range"),
            ));
        }
        Ok(j as usize)
    }

    /// Python `x[key]` (`key` may be an index, a dict key or a `slice(..)`).
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        match (self, &key) {
            (Arg::NDArray(a), Arg::Int(_) | Arg::Bool(_)) => a.__getitem__(key),
            (Arg::List(l), Arg::Int(i)) => {
                let g = l.read();
                Ok(g[Self::norm_index(*i, g.len(), "list")?].clone())
            }
            (Arg::List(l), Arg::Bool(b)) => {
                let g = l.read();
                Ok(g[Self::norm_index(*b as i64, g.len(), "list")?].clone())
            }
            (Arg::Tuple(t), Arg::Int(i)) => Ok(t[Self::norm_index(*i, t.len(), "tuple")?].clone()),
            (Arg::Str(s), Arg::Int(i)) => {
                let n = s.chars().count();
                let j = Self::norm_index(*i, n, "string")?;
                Ok(Arg::Str(s.chars().nth(j).unwrap().to_string().into()))
            }
            (Arg::Bytes(b), Arg::Int(i)) => {
                Ok(Arg::Int(b[Self::norm_index(*i, b.len(), "index")?] as i64))
            }
            (Arg::ByteArray(b), Arg::Int(i)) => {
                let g = b.read();
                Ok(Arg::Int(
                    g[Self::norm_index(*i, g.len(), "bytearray index")?] as i64,
                ))
            }
            (Arg::List(l), Arg::Object(a)) if a.as_any().is::<Slice>() => {
                let s = a.as_any().downcast_ref::<Slice>().unwrap();
                let g = l.read();
                Ok(Arg::List(Arc::new(RwLock::new(slice_vec(&g, s)))))
            }
            (Arg::Tuple(t), Arg::Object(a)) if a.as_any().is::<Slice>() => {
                let s = a.as_any().downcast_ref::<Slice>().unwrap();
                Ok(Arg::Tuple(slice_vec(t, s).into()))
            }
            (Arg::Str(st), Arg::Object(a)) if a.as_any().is::<Slice>() => {
                let s = a.as_any().downcast_ref::<Slice>().unwrap();
                let chars: Vec<char> = st.chars().collect();
                let out: String = slice_vec(&chars, s).into_iter().collect();
                Ok(Arg::Str(out.into()))
            }
            (Arg::Bytes(b), Arg::Object(a)) if a.as_any().is::<Slice>() => {
                let s = a.as_any().downcast_ref::<Slice>().unwrap();
                Ok(Arg::Bytes(slice_vec(b, s).into()))
            }
            (Arg::Dict(d), k) => {
                k.__hash__()?;
                d.read()
                    .get(k)
                    .cloned()
                    .ok_or_else(|| LailaError::key_error(k.__repr_inner__()))
            }
            (Arg::Entry(e), k) => e.__getitem__(k.clone()),
            (Arg::Pool(p), k) => p.__getitem__(k.clone()),
            (Arg::Object(o), k) => o.__getitem__(k),
            (Arg::List(_) | Arg::Tuple(_) | Arg::Str(_) | Arg::Bytes(_), k) => {
                Err(LailaError::type_error(alloc::format!(
                    "{} indices must be integers or slices, not {}",
                    self.type_name(),
                    k.type_name()
                )))
            }
            (other, _) => Err(LailaError::type_error(alloc::format!(
                "'{}' object is not subscriptable",
                other.type_name()
            ))),
        }
    }

    /// Python `x[key] = value`.
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        match (self, &key) {
            (Arg::List(l), Arg::Int(i)) => {
                let mut g = l.write();
                let j = Self::norm_index(*i, g.len(), "list assignment")?;
                g[j] = value;
                Ok(())
            }
            (Arg::Dict(d), k) => {
                k.__hash__()?;
                d.write().insert(k.clone(), value);
                Ok(())
            }
            // `ba[i] = n` / `ba[a:b] = bytes` (a contiguous slice replaced
            // in place, growing or shrinking the bytearray as Python does).
            (Arg::ByteArray(b), Arg::Int(i)) => {
                let mut g = b.write();
                let j = Self::norm_index(*i, g.len(), "bytearray")?;
                let n = value.as_i64().ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "'{}' object cannot be interpreted as an integer",
                        value.type_name()
                    ))
                })?;
                if !(0..=255).contains(&n) {
                    return Err(LailaError::value_error("byte must be in range(0, 256)"));
                }
                g[j] = n as u8;
                Ok(())
            }
            (Arg::ByteArray(b), Arg::Object(a)) if a.as_any().is::<Slice>() => {
                let s = a.as_any().downcast_ref::<Slice>().expect("slice");
                let new: Vec<u8> = match &value {
                    Arg::Bytes(x) => x.to_vec(),
                    Arg::ByteArray(x) => x.read().clone(),
                    other => other
                        .__iter__()?
                        .iter()
                        .map(|v| v.as_i64().unwrap_or(0) as u8)
                        .collect(),
                };
                let mut g = b.write();
                let (start, stop, step) = s.indices(g.len());
                if step != 1 {
                    return Err(LailaError::not_implemented(
                        "extended slice assignment on bytearray",
                    ));
                }
                let (start, stop) = (start.max(0) as usize, stop.max(start.max(0)) as usize);
                g.splice(start..stop, new);
                Ok(())
            }
            (Arg::Entry(e), k) => e.__setitem__(k.clone(), value),
            (Arg::Pool(p), k) => p.__setitem__(k.clone(), value),
            (Arg::Object(o), k) => o.__setitem__(k, value),
            (other, _) => Err(LailaError::type_error(alloc::format!(
                "'{}' object does not support item assignment",
                other.type_name()
            ))),
        }
    }

    /// Python `del x[key]`.
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        let key = key.into_arg();
        match (self, &key) {
            (Arg::List(l), Arg::Int(i)) => {
                let mut g = l.write();
                let j = Self::norm_index(*i, g.len(), "list assignment")?;
                g.remove(j);
                Ok(())
            }
            (Arg::Dict(d), k) => {
                k.__hash__()?;
                d.write()
                    .shift_remove(k)
                    .map(|_| ())
                    .ok_or_else(|| LailaError::key_error(k.__repr_inner__()))
            }
            (Arg::Entry(e), _) => Err(LailaError::type_error(alloc::format!(
                "'{}' object does not support item deletion",
                e.__class_name__()
            ))),
            (Arg::Pool(p), k) => p.__delitem__(k.clone()),
            (Arg::Object(o), k) => o.__delitem__(k),
            (other, _) => Err(LailaError::type_error(alloc::format!(
                "'{}' object does not support item deletion",
                other.type_name()
            ))),
        }
    }

    /// Python `key in x`.
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        let key = key.into_arg();
        Ok(match self {
            Arg::List(l) => l.read().iter().any(|v| v == &key),
            Arg::Tuple(t) => t.iter().any(|v| v == &key),
            Arg::Dict(d) => {
                key.__hash__()?;
                d.read().contains_key(&key)
            }
            Arg::Str(s) => match &key {
                Arg::Str(k) => s.contains(&**k),
                other => {
                    return Err(LailaError::type_error(alloc::format!(
                        "'in <string>' requires string as left operand, not {}",
                        other.type_name()
                    )))
                }
            },
            Arg::Bytes(b) => match &key {
                Arg::Int(i) => b.contains(&(*i as u8)),
                Arg::Bytes(k) => b.windows(k.len().max(1)).any(|w| w == &**k) || k.is_empty(),
                other => {
                    return Err(LailaError::type_error(alloc::format!(
                        "a bytes-like object is required, not '{}'",
                        other.type_name()
                    )))
                }
            },
            Arg::Set(s) => {
                key.__hash__()?;
                s.read().contains(&key)
            }
            Arg::FrozenSet(s) => {
                key.__hash__()?;
                s.contains(&key)
            }
            Arg::ByteArray(b) => match &key {
                Arg::Int(i) => b.read().contains(&(*i as u8)),
                Arg::Bytes(k) => {
                    let g = b.read();
                    k.is_empty() || g.windows(k.len()).any(|w| w == &**k)
                }
                other => {
                    return Err(LailaError::type_error(alloc::format!(
                        "a bytes-like object is required, not '{}'",
                        other.type_name()
                    )))
                }
            },
            Arg::Pool(p) => p.__contains__(key)?,
            Arg::Entry(e) => e.__contains__(key)?,
            Arg::Object(o) => o.__contains__(&key)?,
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "argument of type '{}' is not iterable",
                    other.type_name()
                )))
            }
        })
    }

    /// Python iteration: a snapshot of the elements (dict -> keys).
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(match self {
            Arg::List(l) => l.read().clone(),
            Arg::Tuple(t) => t.to_vec(),
            Arg::Dict(d) => d.read().keys().cloned().collect(),
            Arg::Str(s) => s.chars().map(|c| Arg::Str(c.to_string().into())).collect(),
            Arg::Bytes(b) => b.iter().map(|x| Arg::Int(*x as i64)).collect(),
            Arg::ByteArray(b) => b.read().iter().map(|x| Arg::Int(*x as i64)).collect(),
            Arg::Set(s) => s.read().iter().cloned().collect(),
            Arg::FrozenSet(s) => s.iter().cloned().collect(),
            Arg::NDArray(a) => {
                if a.ndim() == 0 {
                    return Err(LailaError::type_error("iteration over a 0-d array"));
                }
                match a.tolist() {
                    Arg::List(l) => l.read().clone(),
                    other => alloc::vec![other],
                }
            }
            Arg::Pool(p) => p.keys(Args::new())?.__iter__()?,
            Arg::Entry(e) => e.__iter__()?,
            Arg::Future(f) if f.kind() == crate::policy::central::command::FutureKind::Group => {
                f.__iter__()?
            }
            Arg::Object(o) => o.__iter__()?,
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "'{}' object is not iterable",
                    other.type_name()
                )))
            }
        })
    }

    /// Python `hash(x)`; `TypeError` for unhashable types.
    pub fn __hash__(&self) -> Result<i64> {
        const MOD: i64 = (1 << 61) - 1;
        Ok(match self {
            Arg::None => 0x5f3f_6c9a_1b2d_e4f7u64 as i64 >> 1,
            Arg::Bool(b) => *b as i64,
            Arg::Int(i) => {
                let h = i.rem_euclid(MOD) * i.signum().max(0) + (i % MOD).min(0);
                if h == -1 {
                    -2
                } else {
                    h
                }
            }
            Arg::Float(f) => {
                if crate::_compat::fmath::fract(*f) == 0.0 && f.abs() < 9.2e18 {
                    return Arg::Int(*f as i64).__hash__();
                }
                (f.to_bits() % (MOD as u64)) as i64
            }
            Arg::Long(l) => {
                use num_traits::ToPrimitive;
                // Python: hash(int) == int mod (2**61 - 1), sign preserved.
                let m = BigInt::from(MOD);
                let r = ((&**l) % &m).to_i64().unwrap_or(0);
                if r == -1 {
                    -2
                } else {
                    r
                }
            }
            Arg::Str(s) => fnv(s.as_bytes()),
            Arg::Bytes(b) => fnv(b),
            Arg::FrozenSet(s) => {
                // order-independent
                let mut acc: i64 = 1927868237;
                for v in s.iter() {
                    let h = v.__hash__()?;
                    acc ^= (h ^ (h << 16) ^ 89869747).wrapping_mul(3644798167);
                }
                acc
            }
            Arg::Tuple(t) => {
                let mut acc: i64 = 0x345678;
                for v in t.iter() {
                    acc = acc.wrapping_mul(1_000_003) ^ v.__hash__()?;
                }
                acc
            }
            Arg::Entry(e) => e.__hash__()?,
            Arg::Pool(p) => Arc::as_ptr(&p.0) as *const () as usize as i64,
            Arg::Future(f) => f.__hash__(),
            Arg::Callable(c) => Arc::as_ptr(c) as *const () as usize as i64,
            Arg::Exception(e) => Arc::as_ptr(e) as usize as i64,
            Arg::Object(o) => o.__hash__()?,
            Arg::List(_) | Arg::Dict(_) | Arg::Set(_) | Arg::ByteArray(_) | Arg::NDArray(_) => {
                return Err(LailaError::type_error(alloc::format!(
                    "unhashable type: '{}'",
                    self.type_name()
                )))
            }
        })
    }

    /// Python `str(x)`.
    pub fn __str__(&self) -> String {
        match self {
            Arg::Str(s) => s.to_string(),
            Arg::Exception(e) => e.__str__(),
            Arg::Object(o) => o.__str__(),
            Arg::Entry(e) => e.__str__(),
            other => other.__repr__(),
        }
    }

    /// Python `repr(x)`.
    pub fn __repr__(&self) -> String {
        self.__repr_inner__()
    }

    pub(crate) fn __repr_inner__(&self) -> String {
        match self {
            Arg::None => "None".into(),
            Arg::Bool(true) => "True".into(),
            Arg::Bool(false) => "False".into(),
            Arg::Int(i) => i.to_string(),
            Arg::Long(l) => l.to_string(),
            Arg::Float(f) => super::pyrepr::float_repr(*f),
            Arg::Str(s) => super::pyrepr::str_repr(s),
            Arg::Bytes(b) => super::pyrepr::bytes_repr(b),
            Arg::ByteArray(b) => {
                alloc::format!("bytearray({})", super::pyrepr::bytes_repr(&b.read()))
            }
            Arg::Set(s) => {
                let g = s.read();
                if g.is_empty() {
                    return "set()".into();
                }
                let items: Vec<String> = g.iter().map(|v| v.__repr_inner__()).collect();
                alloc::format!("{{{}}}", items.join(", "))
            }
            Arg::FrozenSet(s) => {
                if s.is_empty() {
                    return "frozenset()".into();
                }
                let items: Vec<String> = s.iter().map(|v| v.__repr_inner__()).collect();
                alloc::format!("frozenset({{{}}})", items.join(", "))
            }
            Arg::NDArray(a) => a.__repr__(),
            Arg::List(l) => {
                let g = l.read();
                let items: Vec<String> = g.iter().map(|v| v.__repr_inner__()).collect();
                alloc::format!("[{}]", items.join(", "))
            }
            Arg::Tuple(t) => {
                let items: Vec<String> = t.iter().map(|v| v.__repr_inner__()).collect();
                if items.len() == 1 {
                    alloc::format!("({},)", items[0])
                } else {
                    alloc::format!("({})", items.join(", "))
                }
            }
            Arg::Dict(d) => {
                let g = d.read();
                let items: Vec<String> = g
                    .iter()
                    .map(|(k, v)| alloc::format!("{}: {}", k.__repr_inner__(), v.__repr_inner__()))
                    .collect();
                alloc::format!("{{{}}}", items.join(", "))
            }
            Arg::Entry(e) => e.__repr__(),
            Arg::Pool(p) => p.__repr__(),
            Arg::Future(f) => f.__repr__(),
            Arg::Callable(_) => "<function>".into(),
            Arg::Exception(e) => alloc::format!("{}({:?})", e.kind.name(), e.message),
            Arg::Object(o) => o.__repr__(),
        }
    }

    /// Python's numeric view used by `__eq__`/ordering: `True == 1 == 1.0`.
    pub(crate) fn as_number(&self) -> Option<f64> {
        match self {
            Arg::Bool(b) => Some(*b as i64 as f64),
            Arg::Int(i) => Some(*i as f64),
            Arg::Long(l) => {
                use num_traits::ToPrimitive;
                l.to_f64()
            }
            Arg::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// A Python `int` of any size, normalised (`Arg::Int` when it fits).
    pub fn from_bigint(v: BigInt) -> Arg {
        use num_traits::ToPrimitive;
        match v.to_i64() {
            Some(i) => Arg::Int(i),
            None => Arg::Long(Arc::new(v)),
        }
    }

    /// The value of any Python `int` (`Int`, `Long`, `Bool`) as a `BigInt`.
    pub fn as_bigint(&self) -> Option<BigInt> {
        match self {
            Arg::Bool(b) => Some(BigInt::from(*b as i64)),
            Arg::Int(i) => Some(BigInt::from(*i)),
            Arg::Long(l) => Some((**l).clone()),
            _ => None,
        }
    }

    /// `bytes`-like view: `bytes` and `bytearray` contents.
    pub fn as_bytes_like(&self) -> Option<Vec<u8>> {
        match self {
            Arg::Bytes(b) => Some(b.to_vec()),
            Arg::ByteArray(b) => Some(b.read().clone()),
            _ => None,
        }
    }

    pub fn as_ndarray(&self) -> Option<&crate::_compat::ndarray::NDArray> {
        match self {
            Arg::NDArray(a) => Some(a),
            _ => None,
        }
    }

    // ---- extraction helpers (Tier 1 convenience) ----

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Arg::Str(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Arg::Int(i) => Some(*i),
            Arg::Bool(b) => Some(*b as i64),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        self.as_number()
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Arg::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn as_entry(&self) -> Option<&crate::entry::Entry> {
        match self {
            Arg::Entry(e) => Some(e),
            _ => None,
        }
    }
    pub fn as_pool(&self) -> Option<&crate::data::Pool> {
        match self {
            Arg::Pool(p) => Some(p),
            _ => None,
        }
    }
    pub fn as_future(&self) -> Option<&crate::policy::central::command::Future> {
        match self {
            Arg::Future(f) => Some(f),
            _ => None,
        }
    }
    /// Downcast an `Arg::Any`.
    pub fn downcast_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        match self {
            Arg::Object(o) => o.as_any().downcast_ref::<T>(),
            _ => None,
        }
    }
    /// `obj.name(*args, **kwargs)` on a dynamic object.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match self {
            Arg::Object(o) => o.__call_method__(name, args),
            Arg::NDArray(a) => crate::_compat::ndarray::call_method(a, name, args),
            Arg::Entry(e) => e.__call_method__(name, args),
            Arg::Future(f) => f.__call_method__(name, args),
            Arg::Pool(p) => p.__call_method__(name, args),
            other => match other.builtin_call_method(name, args) {
                Some(r) => r,
                None => Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{name}'",
                    other.type_name()
                ))),
            },
        }
    }
    /// `obj.name` on a dynamic object.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match self {
            Arg::Object(o) => o.__getattr__(name),
            Arg::NDArray(a) => crate::_compat::ndarray::getattr(a, name),
            Arg::Entry(e) => e.__getattr__(name),
            Arg::Future(f) => f.__getattr__(name),
            Arg::Pool(p) => p.__getattr__(name),
            // `d.update` / `s.add`: a bound method of a builtin container
            other if other.builtin_method_names().contains(&name) => {
                let this = other.clone();
                let name = String::from(name);
                Ok(callable(move |a: Args| this.__call_method__(&name, a)))
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{name}'",
                other.type_name()
            ))),
        }
    }
    /// `obj.name = value` on a dynamic object.
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match self {
            Arg::Entry(e) => e.__setattr__(name, value),
            Arg::Pool(p) => p.__setattr__(name, value),
            Arg::Future(f) => f.__setattr__(name, value),
            Arg::Object(o) => o.__setattr__(name, value.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{name}'",
                other.type_name()
            ))),
        }
    }
    /// `del obj.name` on a dynamic object.
    pub fn __delattr__(&self, name: &str) -> Result<()> {
        match self {
            Arg::Object(o) => o.__delattr__(name),
            Arg::Entry(e) => e.__delattr__(name),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{name}'",
                other.type_name()
            ))),
        }
    }
    /// Call a callable `Arg`.
    pub fn __call__(&self, args: impl Into<Args>) -> Result<Arg> {
        match self {
            Arg::Callable(c) => c(args.into()),
            Arg::Object(o) => o.__call__(args.into()),
            other => Err(LailaError::type_error(alloc::format!(
                "'{}' object is not callable",
                other.type_name()
            ))),
        }
    }
}

fn slice_vec<T: Clone>(v: &[T], s: &Slice) -> Vec<T> {
    let (start, stop, step) = s.indices(v.len());
    let mut out = Vec::new();
    if step > 0 {
        let mut i = start;
        while i < stop {
            out.push(v[i as usize].clone());
            i += step;
        }
    } else {
        let mut i = start;
        while i > stop {
            out.push(v[i as usize].clone());
            i += step;
        }
    }
    out
}

fn fnv(bytes: &[u8]) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h >> 2) as i64
}

impl PartialEq for Arg {
    fn eq(&self, other: &Arg) -> bool {
        // Big ints compare exactly (never equal to an i64, by normalisation).
        match (self, other) {
            (Arg::Long(a), Arg::Long(b)) => return a == b,
            (Arg::Long(l), Arg::Float(f)) | (Arg::Float(f), Arg::Long(l)) => {
                use num_traits::ToPrimitive;
                return crate::_compat::fmath::fract(*f) == 0.0 && l.to_f64() == Some(*f);
            }
            (Arg::Long(_), _) | (_, Arg::Long(_)) => return false,
            _ => {}
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            // Python compares int/float exactly; f64 is exact for |i| < 2^53 and
            // Int/Int compares as integers below.
            if let (Arg::Int(x), Arg::Int(y)) = (self, other) {
                return x == y;
            }
            return a == b;
        }
        match (self, other) {
            (Arg::None, Arg::None) => true,
            (Arg::Str(a), Arg::Str(b)) => a == b,
            (Arg::Bytes(a), Arg::Bytes(b)) => a == b,
            (Arg::ByteArray(a), Arg::ByteArray(b)) => Arc::ptr_eq(a, b) || *a.read() == *b.read(),
            (Arg::ByteArray(a), Arg::Bytes(b)) | (Arg::Bytes(b), Arg::ByteArray(a)) => {
                *a.read() == **b
            }
            (Arg::Set(a), Arg::Set(b)) => Arc::ptr_eq(a, b) || *a.read() == *b.read(),
            (Arg::FrozenSet(a), Arg::FrozenSet(b)) => a == b,
            (Arg::Set(a), Arg::FrozenSet(b)) | (Arg::FrozenSet(b), Arg::Set(a)) => *a.read() == **b,
            (Arg::NDArray(a), Arg::NDArray(b)) => Arc::ptr_eq(a, b) || a.array_equal(b, false),
            (Arg::List(a), Arg::List(b)) => Arc::ptr_eq(a, b) || *a.read() == *b.read(),
            (Arg::Tuple(a), Arg::Tuple(b)) => a == b,
            (Arg::Dict(a), Arg::Dict(b)) => {
                if Arc::ptr_eq(a, b) {
                    return true;
                }
                let (ga, gb) = (a.read(), b.read());
                ga.len() == gb.len() && ga.iter().all(|(k, v)| gb.get(k) == Some(v))
            }
            (Arg::Entry(a), Arg::Entry(b)) => a == b,
            (Arg::Pool(a), Arg::Pool(b)) => a.is(b),
            (Arg::Future(a), Arg::Future(b)) => a.is(b),
            (Arg::Callable(a), Arg::Callable(b)) => Arc::ptr_eq(a, b),
            (Arg::Exception(a), Arg::Exception(b)) => a == b,
            (Arg::Object(a), Arg::Object(b)) => Arc::ptr_eq(a, b) || a.__eq__(&**b),
            (Arg::Object(a), b) | (b, Arg::Object(a)) => a.__eq_arg__(b),
            _ => false,
        }
    }
}
impl Eq for Arg {}

impl Hash for Arg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.__hash__().unwrap_or(0));
    }
}

impl fmt::Debug for Arg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.__repr__())
    }
}
impl fmt::Display for Arg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.__str__())
    }
}

// ---- comparisons with plain Rust values, so `x == "INFO"` and `n == 3` read naturally ----

impl PartialEq<&str> for Arg {
    fn eq(&self, other: &&str) -> bool {
        matches!(self, Arg::Str(s) if &**s == *other)
    }
}
impl PartialEq<str> for Arg {
    fn eq(&self, other: &str) -> bool {
        matches!(self, Arg::Str(s) if &**s == other)
    }
}
impl PartialEq<String> for Arg {
    fn eq(&self, other: &String) -> bool {
        matches!(self, Arg::Str(s) if &**s == other.as_str())
    }
}
impl PartialEq<i64> for Arg {
    fn eq(&self, other: &i64) -> bool {
        *self == Arg::Int(*other)
    }
}
impl PartialEq<i32> for Arg {
    fn eq(&self, other: &i32) -> bool {
        *self == Arg::Int(*other as i64)
    }
}
impl PartialEq<usize> for Arg {
    fn eq(&self, other: &usize) -> bool {
        *self == Arg::Int(*other as i64)
    }
}
impl PartialEq<f64> for Arg {
    fn eq(&self, other: &f64) -> bool {
        *self == Arg::Float(*other)
    }
}
impl PartialEq<bool> for Arg {
    fn eq(&self, other: &bool) -> bool {
        *self == Arg::Bool(*other)
    }
}
/// `e.data == b"END"`: a bytes-like value against a bytes literal.
impl<const N: usize> PartialEq<&[u8; N]> for Arg {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_bytes_like()
            .is_some_and(|b| b.as_slice() == &other[..])
    }
}
impl<const N: usize> PartialEq<[u8; N]> for Arg {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_bytes_like()
            .is_some_and(|b| b.as_slice() == &other[..])
    }
}
impl PartialEq<&[u8]> for Arg {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_bytes_like().is_some_and(|b| b.as_slice() == *other)
    }
}
impl PartialEq<Vec<u8>> for Arg {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_bytes_like().is_some_and(|b| &b == other)
    }
}

// ---- conversions into Arg ----

/// By-value conversion into an `Arg` (what `args!` applies to each argument).
pub trait IntoArg {
    fn into_arg(self) -> Arg;
}

/// By-reference conversion into an `Arg` (an `Arc` clone for handles, a copy
/// for scalars). Tier 2 passes named arguments as `arg(&x)` so the Python
/// value stays usable after the call.
pub trait PyArg {
    fn to_arg(&self) -> Arg;
}

/// `arg(&x)`: share `x` as a Python object.
pub fn arg<T: PyArg + ?Sized>(x: &T) -> Arg {
    x.to_arg()
}

impl<T: PyArg + ?Sized> IntoArg for &T {
    fn into_arg(self) -> Arg {
        self.to_arg()
    }
}
impl<T: PyArg + ?Sized> IntoArg for &mut T {
    fn into_arg(self) -> Arg {
        self.to_arg()
    }
}
impl IntoArg for Arg {
    fn into_arg(self) -> Arg {
        self
    }
}
impl PyArg for Arg {
    fn to_arg(&self) -> Arg {
        self.clone()
    }
}

macro_rules! scalar_arg {
    ($($t:ty => |$v:ident| $e:expr),* $(,)?) => {
        $(
            impl IntoArg for $t { fn into_arg(self) -> Arg { let $v = self; $e } }
            impl PyArg for $t { fn to_arg(&self) -> Arg { let $v = self.clone(); $e } }
            impl From<$t> for Arg { fn from(v: $t) -> Arg { v.into_arg() } }
        )*
    };
}

scalar_arg! {
    bool => |v| Arg::Bool(v),
    i8 => |v| Arg::Int(v as i64), i16 => |v| Arg::Int(v as i64), i32 => |v| Arg::Int(v as i64),
    i64 => |v| Arg::Int(v), isize => |v| Arg::Int(v as i64),
    u8 => |v| Arg::Int(v as i64), u16 => |v| Arg::Int(v as i64), u32 => |v| Arg::Int(v as i64),
    u64 => |v| i64::try_from(v).map(Arg::Int).unwrap_or_else(|_| Arg::Long(Arc::new(BigInt::from(v)))),
    usize => |v| Arg::Int(v as i64),
    i128 => |v| Arg::from_bigint(BigInt::from(v)), u128 => |v| Arg::from_bigint(BigInt::from(v)),
    BigInt => |v| Arg::from_bigint(v),
    f32 => |v| Arg::Float(v as f64), f64 => |v| Arg::Float(v),
    String => |v| Arg::Str(v.into()),
    LailaError => |v| Arg::Exception(Arc::new(v)),
}

/// A module constant (`static NAME: LazyLock<Arg>`, rule P11) reads as its value.
#[cfg(feature = "std")]
impl PyArg for std::sync::LazyLock<Arg> {
    fn to_arg(&self) -> Arg {
        (**self).clone()
    }
}

impl IntoArg for crate::_compat::ndarray::Scalar {
    fn into_arg(self) -> Arg {
        crate::_compat::ndarray::Scalar::into_arg(self)
    }
}
impl IntoArg for crate::entry::Entry {
    fn into_arg(self) -> Arg {
        Arg::Entry(self)
    }
}
impl IntoArg for crate::policy::central::command::Future {
    fn into_arg(self) -> Arg {
        Arg::Future(self)
    }
}

/// `b"..."` literals (Rust `&[u8; N]`) are Python `bytes`.
impl<const N: usize> PyArg for [u8; N] {
    fn to_arg(&self) -> Arg {
        Arg::Bytes(Arc::from(&self[..]))
    }
}
impl<const N: usize> IntoArg for [u8; N] {
    fn into_arg(self) -> Arg {
        Arg::Bytes(Arc::from(&self[..]))
    }
}
/// `let data = b"..."` binds a `&[u8; N]`; comparisons and calls take it as
/// Python `bytes` like the owned array.
impl<const N: usize> PyArg for &[u8; N] {
    fn to_arg(&self) -> Arg {
        Arg::Bytes(Arc::from(&self[..]))
    }
}

impl PyArg for str {
    fn to_arg(&self) -> Arg {
        Arg::Str(self.into())
    }
}
impl PyArg for &str {
    fn to_arg(&self) -> Arg {
        Arg::Str((*self).into())
    }
}
impl<'a> From<&'a str> for Arg {
    fn from(s: &'a str) -> Arg {
        Arg::Str(s.into())
    }
}
crate::py_object!(Slice);
/// `b"..."` byte strings: `bytes(b"abc")` / `Bytes::from(..)`; `Vec<u8>` is a
/// Python *list of ints* like in Python, so bytes are explicit.
pub fn bytes(b: impl AsRef<[u8]>) -> Arg {
    Arg::Bytes(b.as_ref().into())
}
impl<T: IntoArg> IntoArg for Vec<T> {
    fn into_arg(self) -> Arg {
        Arg::List(Arc::new(RwLock::new(
            self.into_iter().map(IntoArg::into_arg).collect(),
        )))
    }
}
impl<T: PyArg> PyArg for Vec<T> {
    fn to_arg(&self) -> Arg {
        Arg::List(Arc::new(RwLock::new(
            self.iter().map(PyArg::to_arg).collect(),
        )))
    }
}
impl<T: PyArg> PyArg for [T] {
    fn to_arg(&self) -> Arg {
        Arg::List(Arc::new(RwLock::new(
            self.iter().map(PyArg::to_arg).collect(),
        )))
    }
}
impl<T: IntoArg> IntoArg for Option<T> {
    fn into_arg(self) -> Arg {
        match self {
            Some(v) => v.into_arg(),
            None => Arg::None,
        }
    }
}
impl<T: PyArg> PyArg for Option<T> {
    fn to_arg(&self) -> Arg {
        match self {
            Some(v) => v.to_arg(),
            None => Arg::None,
        }
    }
}
impl<T: IntoArg, E> IntoArg for core::result::Result<T, E>
where
    E: Into<LailaError>,
{
    fn into_arg(self) -> Arg {
        match self {
            Ok(v) => v.into_arg(),
            Err(e) => Arg::Exception(Arc::new(e.into())),
        }
    }
}
impl IntoArg for () {
    fn into_arg(self) -> Arg {
        Arg::None
    }
}
impl PyArg for () {
    fn to_arg(&self) -> Arg {
        Arg::None
    }
}

macro_rules! tuple_arg {
    ($($n:tt $t:ident),+) => {
        impl<$($t: IntoArg),+> IntoArg for ($($t,)+) {
            fn into_arg(self) -> Arg { Arg::Tuple(alloc::vec![$(self.$n.into_arg()),+].into()) }
        }
        impl<$($t: PyArg),+> PyArg for ($($t,)+) {
            fn to_arg(&self) -> Arg { Arg::Tuple(alloc::vec![$(self.$n.to_arg()),+].into()) }
        }
    };
}
tuple_arg!(0 A);
tuple_arg!(0 A, 1 B);
tuple_arg!(0 A, 1 B, 2 C);
tuple_arg!(0 A, 1 B, 2 C, 3 D);

/// Extraction of a typed parameter from an `Arg` (for closure parameters).
pub trait FromArg: Sized {
    fn from_arg(a: Arg) -> Result<Self>;
}
impl FromArg for Arg {
    fn from_arg(a: Arg) -> Result<Self> {
        Ok(a)
    }
}
macro_rules! from_arg {
    ($($t:ty => $m:ident $name:literal),* $(,)?) => {$(
        impl FromArg for $t {
            fn from_arg(a: Arg) -> Result<Self> {
                a.$m().map(Into::into).ok_or_else(|| LailaError::type_error(
                    alloc::format!("expected {}, got '{}'", $name, a.type_name())))
            }
        }
    )*};
}
from_arg! { i64 => as_i64 "int", f64 => as_f64 "float", bool => as_bool "bool", String => as_str "str" }

/// Closures become Python callables (`lambda`, `def`). Tier 2 lowers
/// `lambda x: e` to `callable(move |x| e)`. Bodies may return `T` or `Result<T>`.
pub trait PyCallable<A> {
    fn into_callable(self) -> Callable;
}
fn finish<R: IntoArg>(r: R) -> Result<Arg> {
    match r.into_arg() {
        Arg::Exception(e) => Err((*e).clone()),
        v => Ok(v),
    }
}
impl<F, R> PyCallable<()> for F
where
    F: Fn() -> R + Send + Sync + 'static,
    R: IntoArg,
{
    fn into_callable(self) -> Callable {
        Arc::new(move |_a| finish((self)()))
    }
}
macro_rules! py_callable {
    ($n:literal; $($i:tt $t:ident),+) => {
        impl<F, R, $($t: FromArg),+> PyCallable<($($t,)+)> for F
        where
            F: Fn($($t),+) -> R + Send + Sync + 'static,
            R: IntoArg,
        {
            fn into_callable(self) -> Callable {
                Arc::new(move |a: Args| {
                    if a.positional.len() != $n {
                        return Err(LailaError::type_error(alloc::format!(
                            "<lambda>() takes {} positional arguments but {} were given", $n, a.positional.len())));
                    }
                    let mut it = a.positional.into_iter();
                    finish((self)($({ let _ = $i; $t::from_arg(it.next().unwrap())? }),+))
                })
            }
        }
    };
}
py_callable!(1; 0 A);
py_callable!(2; 0 A, 1 B);
py_callable!(3; 0 A, 1 B, 2 C);
py_callable!(4; 0 A, 1 B, 2 C, 3 D);
py_callable!(5; 0 A, 1 B, 2 C, 3 D, 4 E);

/// Marker for the `PyCallable` impls over `async fn` items: calling the
/// value returns a coroutine object (what Python's `async def` does), so
/// a taskforce awaits it like any other coroutine.
pub struct AsyncCallable<A>(core::marker::PhantomData<A>);
use core::future::Future;

impl<F, Fut, R> PyCallable<AsyncCallable<()>> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoArg,
{
    fn into_callable(self) -> Callable {
        Arc::new(move |_a| {
            let fut = (self)();
            Ok(super::stdlib::asyncio::coroutine(async move {
                finish(fut.await)
            }))
        })
    }
}
macro_rules! py_async_callable {
    ($n:literal; $($i:tt $t:ident),+) => {
        impl<F, Fut, R, $($t: FromArg),+> PyCallable<AsyncCallable<($($t,)+)>> for F
        where
            F: Fn($($t),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoArg,
        {
            fn into_callable(self) -> Callable {
                Arc::new(move |a: Args| {
                    if a.positional.len() != $n {
                        return Err(LailaError::type_error(alloc::format!(
                            "<coroutine>() takes {} positional arguments but {} were given", $n, a.positional.len())));
                    }
                    let mut it = a.positional.into_iter();
                    let fut = (self)($({ let _ = $i; $t::from_arg(it.next().unwrap())? }),+);
                    Ok(super::stdlib::asyncio::coroutine(async move { finish(fut.await) }))
                })
            }
        }
    };
}
py_async_callable!(1; 0 A);
py_async_callable!(2; 0 A, 1 B);
py_async_callable!(3; 0 A, 1 B, 2 C);
/// A `#[pysig]` function (`fn f(args: Args)`) used as a value.
impl<F, R> PyCallable<Args> for F
where
    F: Fn(Args) -> R + Send + Sync + 'static,
    R: IntoArg,
{
    fn into_callable(self) -> Callable {
        Arc::new(move |a| finish((self)(a)))
    }
}

/// `callable(f)`: wrap a closure as a Python callable `Arg`.
pub fn callable<A, F: PyCallable<A>>(f: F) -> Arg {
    Arg::Callable(f.into_callable())
}

/// A Python *function object*: a callable that also knows its `__name__`
/// and `inspect.signature` (a nested `def` with a `#[pysig]` signature, the
/// thing a decorator such as `ensure_list` introspects). Plain closures
/// (`Arg::Callable`) carry neither.
pub struct Function {
    pub name: Arc<str>,
    pub sig: &'static super::args::Signature,
    pub call: Callable,
}

impl Function {
    /// A function value named *name* with signature *sig*, invoking `f`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<A, F: PyCallable<A>>(name: &str, sig: &'static super::args::Signature, f: F) -> Arg {
        Arg::Object(Arc::new(Function {
            name: Arc::from(name),
            sig,
            call: f.into_callable(),
        }))
    }
    /// The function object behind an `Arg`, if it is one.
    pub fn of(a: &Arg) -> Option<&Function> {
        a.downcast_ref::<Function>()
    }
    /// `inspect.signature(f)` for any callable value: the signature of a
    /// function object; `None` for closures.
    pub fn signature_of(a: &Arg) -> Option<&'static super::args::Signature> {
        Self::of(a).map(|f| f.sig)
    }
}

impl fmt::Debug for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<function {}>", self.name)
    }
}

impl PyObject for Function {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "function"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<function {} at {:#x}>",
            self.name,
            Arc::as_ptr(&self.call) as *const () as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Function>()
            .is_some_and(|o| Arc::ptr_eq(&o.call, &self.call))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.call) as *const () as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__name__" | "__qualname__" => Ok(Arg::Str(self.name.clone())),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'function' object has no attribute '{name}'"
            ))),
        }
    }
    fn __call__(&self, args: Args) -> Result<Arg> {
        (self.call)(args)
    }
}

/// Dynamic attribute storage for `#[laila::python(dynamic)]` structs
/// (unittest fixtures, user subclasses): Python's `__dict__`.
#[derive(Default)]
pub struct PyAttrs(RwLock<PyMap<String, Arg>>);

impl PyAttrs {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.0.read().get(name).cloned().ok_or_else(|| {
            LailaError::attribute_error(alloc::format!("object has no attribute '{name}'"))
        })
    }
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        self.0.write().insert(name.into(), value.into_arg());
        Ok(())
    }
    pub fn __hasattr__(&self, name: &str) -> bool {
        self.0.read().contains_key(name)
    }
    /// `del obj.name`
    pub fn __delattr__(&self, name: &str) -> Result<()> {
        match self.0.write().shift_remove(name) {
            Some(_) => Ok(()),
            None => Err(LailaError::attribute_error(alloc::format!(
                "object has no attribute '{name}'"
            ))),
        }
    }
    /// `getattr(obj, name, None)`
    pub fn get(&self, name: &str) -> Option<Arg> {
        self.0.read().get(name).cloned()
    }
    /// `obj.__dict__.setdefault(name, factory())`: one atomic
    /// check-and-insert, so a lazily created attribute (a lock) is created
    /// exactly once even under contention.
    pub fn __setdefault__(&self, name: &str, factory: impl FnOnce() -> Arg) -> Arg {
        let mut g = self.0.write();
        if let Some(v) = g.get(name) {
            return v.clone();
        }
        let v = factory();
        g.insert(name.into(), v.clone());
        v
    }
    /// `vars(obj)`: a snapshot of `(name, value)` pairs in insertion order.
    pub fn __dict__(&self) -> Vec<(String, Arg)> {
        self.0
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl fmt::Debug for PyAttrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", *self.0.read())
    }
}

/// The class `__dict__` of every `py_class!` type, by class name: where
/// `cls.x = v` (a classmethod) writes and where an instance read falls
/// back when its own `__dict__` has no `x` (Python's attribute lookup).
static CLASS_ATTRS: RwLock<Vec<(&'static str, Arc<PyAttrs>)>> = RwLock::new(Vec::new());

/// The class `__dict__` of the `py_class!` type named *class*.
pub fn class_attrs(class: &'static str) -> Arc<PyAttrs> {
    if let Some((_, a)) = CLASS_ATTRS.read().iter().find(|(c, _)| *c == class) {
        return a.clone();
    }
    let mut all = CLASS_ATTRS.write();
    if let Some((_, a)) = all.iter().find(|(c, _)| *c == class) {
        return a.clone();
    }
    let a = Arc::new(PyAttrs::new());
    all.push((class, a.clone()));
    a
}

/// A Python *closure cell*: an enclosing-scope local that a nested `def` /
/// `lambda` reads while the enclosing function keeps rebinding it
/// (`stop = False` ... `def worker(): while not stop: ..` ... `stop =
/// True`). Both scopes share the one cell (clones alias it), so the
/// worker sees the rebinding, exactly as Python's cell variables.
#[derive(Clone, Debug)]
pub struct Cell(Arc<super::sync::RwLock<Arg>>);

impl Cell {
    pub fn new(v: impl IntoArg) -> Cell {
        Cell(Arc::new(super::sync::RwLock::new(v.into_arg())))
    }
    /// The current value (a handle clone).
    pub fn get(&self) -> Arg {
        self.0.read().clone()
    }
    /// Rebind the cell.
    pub fn set(&self, v: impl IntoArg) {
        *self.0.write() = v.into_arg();
    }
}
impl IntoArg for Cell {
    fn into_arg(self) -> Arg {
        self.get()
    }
}
impl IntoArg for &Cell {
    fn into_arg(self) -> Arg {
        self.get()
    }
}
