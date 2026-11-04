//! Tier 1 (typed Rust) coverage of `laila::data` and the pool router --
//! what a Rust user without the `laila::python!` macro writes: the
//! [`PoolBackend`] contract (a custom backend plugged in through
//! `pool_class` / `Pool::construct`), the proxy chain, the per-pool index,
//! the `Record` envelope, the router and central memory routing, and the
//! `MultiBuffer` ring.

use std::sync::Arc;

use laila::_compat::pydantic::{LazyClass, ModelClass};
use laila::atomic::definitions::_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT;
use laila::data::schema::base::{exempt_default, field_default, pool_class, Held};
use laila::data::schema::pool_index::{
    _record_creation_timestamp, is_index_key, INDEX_IDENTITY_KEY, NAMESPACE_URL,
};
use laila::data::{
    DefaultPool, HDF5Pool, MultiBuffer, Pool, PoolBackend, PoolIndex, PoolObject, SQLitePool,
    _LAILA_IDENTIFIABLE_DATA_CONTAINER, _LAILA_IDENTIFIABLE_POOL,
};
use laila::entry::transformation_base64;
use laila::policy::central::memory::record::Record;
use laila::policy::central::memory::router::_LAILA_IDENTIFIABLE_POOL_ROUTER;
use laila::policy::schema::base::_LAILA_IDENTIFIABLE_POLICY;
use laila::prelude::*;
use laila::python::builtins::{issubclass, not_implemented};
use laila::python::stdlib::asyncio;
use laila::python::sync::RwLock;

fn err_text(e: &LailaError) -> (ExcKind, String) {
    (e.kind, e.__str__())
}

// ---------------------------------------------------------------------------
// A custom backend: the contract WP-H's Filesystem / SQLite / DuckDB / HDF5
// pools implement. It stores upper-cased copies in its own map, counts the
// hook calls and exposes one extra method (`stats`) and one property.

#[derive(Default)]
struct CountingBackend {
    store: RwLock<Vec<(String, Arg)>>,
    reads: RwLock<usize>,
}

impl PoolBackend for CountingBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        // The pydantic instance is complete here: fields are readable.
        assert_eq!(pool.field("tag").unwrap(), "counting");
        Ok(())
    }
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held: Held = pool.held();
        *self.reads.write() += 1;
        Ok(self
            .store
            .read()
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or(Arg::None))
    }
    fn _write(&self, pool: &PoolObject, key: &str, value: Arg) -> Result<()> {
        let _held = pool.held();
        let mut g = self.store.write();
        match g.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value,
            None => g.push((key.to_string(), value)),
        }
        Ok(())
    }
    fn _delete(&self, _pool: &PoolObject, key: &str) -> Result<()> {
        self.store.write().retain(|(k, _)| k != key);
        Ok(())
    }
    fn _exists(&self, _pool: &PoolObject, key: &str) -> Result<bool> {
        Ok(self.store.read().iter().any(|(k, _)| k == key))
    }
    fn _keys(&self, _pool: &PoolObject) -> Result<Vec<String>> {
        Ok(self.store.read().iter().map(|(k, _)| k.clone()).collect())
    }
    fn _empty(&self, _pool: &PoolObject) -> Result<()> {
        self.store.write().clear();
        Ok(())
    }
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        Some(Ok(()))
    }
    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        (name == "reads").then(|| Ok(Arg::Int(*self.reads.read() as i64)))
    }
    fn __call_method__(&self, _pool: &PoolObject, name: &str, _args: Args) -> Option<Result<Arg>> {
        (name == "stats").then(|| Ok(py!({"reads": *self.reads.read() as i64})))
    }
    fn method_names(&self) -> &'static [&'static str] {
        &["close", "stats"]
    }
}

static COUNTING_CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "CountingPool",
        "laila_tests.data_tier1",
        vec![],
        vec![
            ("tag".to_string(), field_default("str", "counting")),
            // A persistent backend keeps `transformations` non-None.
            (
                "transformations".to_string(),
                field_default("TransformationSequence | None", transformation_base64()),
            ),
            ("scratch".to_string(), exempt_default("Any", Arg::None)),
        ],
        vec![],
        vec![("_persistent_storage".to_string(), Arg::Bool(true))],
    )
});

laila::laila_pool_handle!(CountingPool, CountingBackend);

impl CountingPool {
    fn new(args: impl Into<Args>) -> Result<CountingPool> {
        Ok(CountingPool(Pool::construct(
            &COUNTING_CLASS,
            args.into(),
            |_| Ok(Box::new(CountingBackend::default())),
        )?))
    }
}

#[test]
fn custom_backend_plugs_into_the_base_class() -> Result<()> {
    let pool = CountingPool::new(args!(nickname = "counting-pool"))?;
    // Identity from the pydantic pipeline (nickname -> uuid5), POOL scope.
    assert!(pool.global_id().starts_with("LAILA:POOL:"));
    assert_eq!(pool.pool_id(), pool.global_id());
    assert_eq!(
        pool.uuid(),
        CountingPool::new(args!(nickname = "counting-pool"))?.uuid()
    );
    assert!(pool._persistent_storage());
    assert_eq!(pool.field("tag").unwrap(), "counting");
    assert!(!pool.transformations().is_none());
    // The base class drives the backend hooks.
    pool.write(args!("k", 1))?;
    assert_eq!(pool.__getitem__("k")?, 1);
    assert!(pool.exists("k")?);
    assert_eq!(pool.keys(args!())?, py!(["k"]));
    pool.delete("k")?;
    assert!(!pool.exists("k")?);
    // Backend extras through the dynamic protocol.
    assert_eq!(pool.__getattr__("reads")?, 1);
    assert_eq!(
        pool.__call_method__("stats", args!())?
            .__getitem__("reads")?,
        1
    );
    pool.close(args!())?;
    assert!(laila::python::builtins::hasattr(&pool, "close"));
    // `isinstance` through the handle type and the backend downcast.
    let a: Arg = pool.clone().into_arg();
    assert!(isinstance::<CountingPool>(&a));
    assert!(!isinstance::<HDF5Pool>(&a));
    assert!(isinstance::<_LAILA_IDENTIFIABLE_POOL>(&a));
    assert!(pool.downcast_ref::<CountingBackend>().is_some());
    Ok(())
}

#[test]
fn persistent_backend_rejects_transformations_none() {
    let e = CountingPool::new(args!(transformations = Arg::None)).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            "CountingPool is backed by persistent storage and requires a transformation sequence (got transformations=None); use e.g. laila.transformations.transformation_base64".into()
        )
    );
    // The in-memory base accepts it.
    let p = _LAILA_IDENTIFIABLE_POOL::new(args!(transformations = Arg::None)).unwrap();
    assert!(p.transformations().is_none());
}

#[test]
fn base_pool_hasattr_close_is_false() -> Result<()> {
    let pool = DefaultPool::new(args!())?;
    assert!(!laila::python::builtins::hasattr(&pool, "close"));
    let e = pool.close(args!()).unwrap_err();
    assert_eq!(e.kind, ExcKind::AttributeError);
    let e = pool.sync(args!()).unwrap_err();
    assert_eq!(
        err_text(&e),
        (ExcKind::NotImplementedError, "Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.".into())
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Proxy chain

#[test]
fn proxy_chain_operators_and_write_back() -> Result<()> {
    let mem = DefaultPool::new(args!(index_enabled = false))?;
    let tmp = laila::python::stdlib::tempfile.mkdtemp(args!(prefix = "laila_tier1_"))?;
    let hdf5 = HDF5Pool::new(args!(
        file_path = format!("{tmp}/pool.h5py"),
        index_enabled = false
    ))?;
    // The deepest tier is another local persistent pool (`S3Pool` is a real
    // client now: it needs credentials).
    let s3 = SQLitePool::new(args!(
        file_path = format!("{tmp}/pool.sqlite"),
        index_enabled = false
    ))?;
    // Typed operator form: `&mem << &hdf5 << &s3`.
    let _ = &mem << &hdf5 << &s3;
    assert!(mem.proxy_to_pool().unwrap().is(&hdf5));
    assert!(hdf5.proxy_to_pool().unwrap().is(&s3));
    assert!(s3.proxy_to().is_none());
    s3.write(args!("deep", "\"bottom\""))?;
    assert_eq!(mem.__getitem__("deep")?, "bottom");
    assert!(hdf5.exists("deep")?);
    assert!(mem.exists("deep")?);
    // Writes never propagate (a persistent backend takes serialized JSON).
    hdf5.write(args!("only", "1"))?;
    assert!(!mem.exists("only")? && !s3.exists("only")?);
    // `_upstream_tiers`, nearest first.
    let tiers: Vec<String> = mem._upstream_tiers().iter().map(Pool::global_id).collect();
    assert_eq!(tiers, [hdf5.global_id(), s3.global_id()]);
    // `>>` runs the other way.
    let a = DefaultPool::new(args!())?;
    let b = DefaultPool::new(args!())?;
    let back = a.__rshift__(&b)?;
    assert!(back.is(&b) && b.proxy_to_pool().unwrap().is(&a));
    Ok(())
}

#[test]
fn proxy_cycles_are_rejected() -> Result<()> {
    let a = DefaultPool::new(args!())?;
    let b = DefaultPool::new(args!())?;
    let c = DefaultPool::new(args!())?;
    let e = a.__lshift__(&a).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            format!("A pool cannot proxy itself ({})", a.global_id())
        )
    );
    a.__lshift__(&b)?;
    b.__lshift__(&c)?;
    let e = c.__setattr__("proxy_to", &a).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            format!(
                "Proxy cycle: {} already reaches {} through its proxy chain",
                a.global_id(),
                c.global_id()
            )
        )
    );
    // Rewiring through the dynamic surface.
    c.__setattr__("proxy_to", Arg::None)?;
    assert!(c.__getattr__("proxy_to")?.is_none());
    Ok(())
}

#[test]
fn async_read_through_caches_every_front_tier() -> Result<()> {
    let front = DefaultPool::new(args!())?;
    let mid = DefaultPool::new(args!())?;
    let origin = DefaultPool::new(args!())?;
    front.__lshift__(&mid)?.__lshift__(&origin)?;
    asyncio::run(origin.obj().write_async("k", py!({"v": 1})))?;
    let got = asyncio::run(front.obj()._read_through_async("k"))?;
    assert_eq!(got, py!({"v": 1}));
    assert!(mid.exists("k")? && front.exists("k")?);
    assert!(!asyncio::run(front.obj()._exists_async("nope"))?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Index

#[test]
fn index_identity_key_is_uuid5_of_the_url_namespace() {
    assert_eq!(
        INDEX_IDENTITY_KEY,
        format!(
            "LAILA:POOL_INDEX:{}",
            laila::_compat::uuid::uuid5(&NAMESPACE_URL, "laila:pool_index:identity")
        )
    );
    assert!(is_index_key(args!(INDEX_IDENTITY_KEY)).unwrap());
    assert!(!is_index_key(args!(42)).unwrap());
}

#[test]
fn index_shards_evolutions_and_timestamps() -> Result<()> {
    let pool = DefaultPool::new(args!())?;
    let base = "LAILA:ENTRY:11111111-1111-4111-8111-111111111111";
    for e in 0..3 {
        let stamp = format!("2026-01-0{}T00:00:00.000+00:00", e + 1);
        pool.write(args!(
            format!("{base}@evolution={e}"),
            py!({"entry": {"_creation_timestamp": stamp}})
        ))?;
    }
    let idx: PoolIndex = pool.index();
    assert_eq!(idx.identity()?, pool.uuid());
    assert_eq!(
        idx.candidates(base)?,
        py!([
            format!("{base}@evolution=0"),
            format!("{base}@evolution=1"),
            format!("{base}@evolution=2")
        ])
    );
    assert_eq!(idx.latest(base)?, format!("{base}@evolution=2"));
    assert_eq!(idx.nth(base, -3)?, format!("{base}@evolution=0"));
    assert!(idx.nth(base, -4)?.is_none());
    assert!(idx.nth(base, 7)?.is_none());
    assert_eq!(
        idx.by_creation_timestamp(args!(base, "2026-01-02T00:00:00.000+00:00"))?,
        format!("{base}@evolution=1")
    );
    assert!(idx
        .by_creation_timestamp(args!(base, "2026-01-02T00:00:00.000+00:00", 2))?
        .is_none());
    // One constant shard per base, hidden from `keys()`.
    let visible = pool.keys(args!())?.__iter__()?;
    let raw = pool.keys(args!(include_index = true))?.__iter__()?;
    assert_eq!((visible.len(), raw.len()), (3, 4));
    let shard = raw
        .iter()
        .find(|k| laila::data::schema::pool_index::index_key(&k.__str__()))
        .unwrap();
    assert_eq!(shard.__str__(), idx.shard_id(base)?);
    assert!(!shard.__str__().contains('@'));
    // The shard payload is a Record envelope around a POOL_INDEX entry.
    let record = pool.__getitem__(shard.clone())?;
    let entry = Record::_build_sync(record)?.__getitem__("entry")?;
    assert!(entry
        .global_id()?
        .__str__()
        .starts_with("LAILA:POOL_INDEX:"));
    assert_eq!(entry.data()?.__getitem__("evolutions")?, py!([0, 1, 2]));
    // The pool-side resolvers central memory calls.
    assert_eq!(
        pool._resolve_indexed(base, py!({"evolution": "-1"}))?,
        format!("{base}@evolution=2")
    );
    assert_eq!(
        pool._resolve_indexed(
            base,
            py!({"creation_timestamp": "2026-01-01T00:00:00.000+00:00"})
        )?,
        format!("{base}@evolution=0")
    );
    assert_eq!(pool._search_keys(base, py!({}))?.__len__()?, 3);
    assert!(pool
        ._search_keys("LAILA:ENTRY:00000000-0000-0000-0000-000000000000", py!({}))?
        .is_none());
    assert_eq!(pool._candidate_keys(base)?.__len__()?, 3);
    // Deleting the last evolution drops the shard.
    for e in 0..3 {
        pool.delete(format!("{base}@evolution={e}"))?;
    }
    assert!(idx.candidates(base)?.is_none());
    assert_eq!(pool.keys(args!(include_index = true))?.__len__()?, 0);
    Ok(())
}

#[test]
fn index_pool_and_disabled_index() -> Result<()> {
    let ram = DefaultPool::new(args!())?;
    let data = DefaultPool::new(args!(index_pool = &ram))?;
    let base = "LAILA:ENTRY:22222222-2222-4222-8222-222222222222";
    data.write(args!(base, py!({"x": 1})))?;
    assert_eq!(data.keys(args!(include_index = true))?.__len__()?, 1);
    assert_eq!(ram.keys(args!(include_index = true))?.__len__()?, 1);
    assert!(data.index().index_pool()?.is(&ram));
    data.empty(args!())?;
    assert_eq!(ram.keys(args!(include_index = true))?.__len__()?, 0);

    let off = DefaultPool::new(args!(index_enabled = false))?;
    off.write(args!(base, py!({"x": 1})))?;
    assert_eq!(off.keys(args!(include_index = true))?.__len__()?, 1);
    assert!(off._resolve_indexed(base, py!({}))?.is_none());
    assert!(off._search_keys(base, py!({}))?.is_none());
    // Plain keys are never indexed.
    let plain = DefaultPool::new(args!())?;
    plain.write(args!("plain-0", 1))?;
    assert_eq!(plain.keys(args!(include_index = true))?, py!(["plain-0"]));
    Ok(())
}

#[test]
fn record_creation_timestamp_extraction() {
    let stamp = "2026-01-01T00:00:00.000+00:00";
    assert_eq!(
        _record_creation_timestamp(&py!({"entry": {"_creation_timestamp": stamp}})),
        stamp
    );
    assert_eq!(
        _record_creation_timestamp(&Arg::from(format!(
            "{{\"entry\": {{\"_creation_timestamp\": \"{stamp}\"}}}}"
        ))),
        stamp
    );
    assert!(_record_creation_timestamp(&Arg::from("not json")).is_none());
    assert!(_record_creation_timestamp(&py!({"x": 1})).is_none());
    let e = laila.constant(args!(data = 1)).unwrap();
    assert_eq!(
        _record_creation_timestamp(&Arg::Entry(e.clone())),
        e.creation_timestamp()
    );
}

// ---------------------------------------------------------------------------
// Record

#[test]
fn record_envelope_round_trip() -> Result<()> {
    let e = laila.constant(args!(data = py!({"a": 1}), nickname = "rec"))?;
    let record = Record::new(args!(entry = &e, borrower = "LAILA:POLICY:x"))?;
    assert!(isinstance::<Record>(&record));
    assert_eq!(record.__getattr__("entry_id")?, e.global_id());
    assert_eq!(
        record.__getattr__("recorder")?,
        laila.get_active_policy()?.global_id()
    );
    assert_eq!(record.__getattr__("borrower")?, "LAILA:POLICY:x");
    let blob = Record::serialize_record(&record, transformation_base64().into_arg())?;
    assert!(matches!(blob.__getitem__("entry")?, Arg::Dict(_)));
    let built = Record::_build_sync(&blob)?;
    let back = built.__getitem__("entry")?;
    assert_eq!(back.global_id()?, e.global_id());
    assert_eq!(back.data()?, py!({"a": 1}));
    let again = Record::from_dict(&blob)?;
    assert_eq!(again.__getattr__("entry_id")?, e.global_id());
    // `extra="forbid"`
    let err = Record::new(args!(entry = &e, creator = "x")).unwrap_err();
    assert_eq!(err.kind, ExcKind::ValidationError);
    assert!(err.message.contains("Extra inputs are not permitted"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Router and central memory

#[test]
fn router_defaults_and_routing() -> Result<()> {
    let router = _LAILA_IDENTIFIABLE_POOL_ROUTER::new(args!())?;
    assert!(router.global_id().starts_with("LAILA:POOL_ROUTER:"));
    assert_eq!(router.pools().__len__()?, 1);
    assert!(router.pools_nicknames().__contains__("_memory")?);
    let default = router.route_pool(None, None)?;
    assert!(isinstance::<_LAILA_IDENTIFIABLE_POOL>(
        &default.clone().into_arg()
    ));
    let extra = DefaultPool::new(args!())?;
    router.extend(args!(&extra, affinity = 2, pool_nickname = "extra"))?;
    assert!(router.route_pool(None, Some("extra"))?.is(&extra));
    assert!(router
        .route_pool(Some(extra.global_id().as_str()), None)?
        .is(&extra));
    let e = router.route_pool(None, Some("nope")).unwrap_err();
    assert_eq!(err_text(&e), (ExcKind::KeyError, "'nope'".into()));
    let e = router.extend(args!(42)).unwrap_err();
    assert_eq!(e.kind, ExcKind::AttributeError);
    // `pools_pq` holds `(-affinity, gid)`.
    assert_eq!(router.pools_pq().__getattr__("queue")?.__len__()?, 2);
    Ok(())
}

#[test]
fn memory_routes_through_the_router() -> Result<()> {
    let policy = _LAILA_IDENTIFIABLE_POLICY::new(args!())?;
    let previous = laila.get_active_policy()?;
    laila.activate_policy(&policy)?;
    let memory = policy.central().memory();
    let pool = DefaultPool::new(args!(transformations = transformation_base64()))?;
    memory.extend(args!(&pool, pool_nickname = "p"))?;
    assert!(laila.memory().pool_router().is(&policy.pool_router()));
    assert!(memory._resolve_pool_ref(&Arg::from("p"))?.is(&pool));
    assert!(memory
        ._resolve_pool_ref(&Arg::from(pool.global_id()))?
        .is(&pool));
    let e = memory._resolve_pool_ref(&Arg::from("zz")).unwrap_err();
    assert_eq!(
        err_text(&e),
        (ExcKind::KeyError, "\"Pool 'zz' was not found.\"".into())
    );
    let e = memory._resolve_pool_ref(&Arg::Int(1)).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::TypeError,
            "pool_ref must be a pool object, pool id, or pool nickname.".into()
        )
    );
    // memorize / remember / forget through the facade and the nickname.
    let entry = laila.variable(args!(data = py!([1, 2]), nickname = "routed"))?;
    let gid = laila
        .memorize(args!(&entry, pool_nickname = "p"))?
        .wait(args!())?;
    // `wait()` hands back the result *entry* (a raw value is wrapped by
    // `Entry.constant` on first read); its payload is the written id.
    assert_eq!(gid.data()?, Arg::from(entry.global_id()));
    assert!(pool.exists(entry.global_id())?);
    assert!(!laila.alpha_pool().exists(entry.global_id())?);
    let back = laila
        .remember(args!(
            entry.global_id(),
            pool_nickname = "p",
            persist = false
        ))?
        .wait(args!())?;
    assert_eq!(back.data()?, py!([1, 2]));
    assert_eq!(back._source_pool_gid()?, pool.global_id());
    let e = laila
        .remember(args!("LAILA:ENTRY:nope", pool_nickname = "p"))?
        .wait(args!())
        .unwrap_err();
    assert_eq!(e.kind, ExcKind::KeyError);
    laila
        .forget(args!(entry.global_id(), pool_nickname = "p"))?
        .wait(args!())?;
    assert!(!pool.exists(entry.global_id())?);
    // A list of entries is a `GroupFuture`: `wait()` is the plain list of
    // the children's results (one result entry per id).
    let a = laila.constant(args!(data = 1))?;
    let b = laila.constant(args!(data = 2))?;
    let ids = laila
        .memorize(args!(py!([&a, &b]), pool_id = pool.global_id()))?
        .wait(args!())?;
    let ids: Vec<Arg> = ids
        .__iter__()?
        .iter()
        .map(|r| r.data())
        .collect::<Result<_>>()?;
    assert_eq!(ids.into_arg(), py!([a.global_id(), b.global_id()]));
    laila.activate_policy(&previous)?;
    Ok(())
}

#[test]
fn record_identity_mismatch_is_a_key_error() -> Result<()> {
    let pool = DefaultPool::new(args!())?;
    laila
        .memory()
        .extend(args!(&pool, pool_nickname = "mismatch"))?;
    let e = laila.constant(args!(data = 1))?;
    // A record stored under a foreign key must fail loudly.
    let record = Record::new(args!(entry = &e))?;
    pool.write(args!(
        "LAILA:ENTRY:99999999-9999-4999-8999-999999999999",
        Record::serialize_record(&record, Arg::None)?
    ))?;
    let err = laila
        .remember(args!(
            "LAILA:ENTRY:99999999-9999-4999-8999-999999999999",
            pool_id = pool.global_id()
        ))?
        .wait(args!())
        .unwrap_err();
    assert_eq!(err.kind, ExcKind::KeyError);
    assert!(err.message.contains("record identity mismatch"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Class objects, `isinstance`, `issubclass`, `NotImplemented`

#[test]
fn class_objects_and_hierarchy() -> Result<()> {
    assert!(issubclass::<
        _LAILA_IDENTIFIABLE_POOL,
        _LAILA_IDENTIFIABLE_DATA_CONTAINER,
    >());
    assert!(issubclass::<MultiBuffer, _LAILA_IDENTIFIABLE_DATA_CONTAINER>());
    assert!(!issubclass::<MultiBuffer, _LAILA_IDENTIFIABLE_POOL>());
    assert!(issubclass::<HDF5Pool, _LAILA_IDENTIFIABLE_POOL>());
    assert!(laila::DefaultPool
        .to_arg()
        .is(&_LAILA_IDENTIFIABLE_POOL.to_arg()));
    assert!(laila::DefaultMultiBuffer.to_arg().is(&MultiBuffer.to_arg()));
    let pool = DefaultPool::new(args!())?;
    assert!(pool.__le__(42)?.is(&not_implemented()));
    assert!(pool.__le__(Arg::None)?.is(&not_implemented()));
    // `pool <= pool` routes through central memory: an unregistered pool
    // is a `KeyError` (same as Python's `_duplicate_pool`).
    let e = pool.__le__(&pool).unwrap_err();
    assert_eq!(e.kind, ExcKind::KeyError);
    // `type(pool)` names the class; `_scopes` is `["POOL"]`.
    assert_eq!(pool.__class__().name, "_LAILA_IDENTIFIABLE_POOL");
    assert_eq!(pool._scopes(), py!(["POOL"]));
    assert_eq!(
        _LAILA_IDENTIFIABLE_DATA_CONTAINER::new(args!())?._scopes(),
        py!(["DATA_CONTAINER"])
    );
    let e = _LAILA_IDENTIFIABLE_DATA_CONTAINER::new(args!())?
        .__getitem__("k")
        .unwrap_err();
    assert_eq!(
        err_text(&e),
        (ExcKind::NotImplementedError, "_LAILA_IDENTIFIABLE_DATA_CONTAINER does not implement __getitem__; _LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.".into())
    );
    Ok(())
}

#[test]
fn pool_atomic_lock_is_reentrant() -> Result<()> {
    let pool = DefaultPool::new(args!())?;
    let cm = pool.atomic(args!())?;
    let guard = cm.__enter__()?;
    let inner = pool.atomic(args!())?;
    let g2 = inner.__enter__()?;
    pool.write(args!("k", 1))?;
    drop(g2);
    drop(guard);
    assert!(!pool.local_lock().locked());
    Ok(())
}

// ---------------------------------------------------------------------------
// MultiBuffer

#[test]
fn multibuffer_ring_heads() -> Result<()> {
    let mb = MultiBuffer::new(args!(capacity = 3))?;
    assert_eq!((mb.capacity(), mb.read_head(), mb.write_head()), (3, 0, 0));
    assert!(mb.global_id().contains("MULTI_BUFFER"));
    let a = laila.constant(args!(data = "a"))?;
    assert_eq!(mb.write(args!(&a))?, 0);
    assert!(isinstance::<Record>(&mb.slots().__getitem__(0)?));
    let out = mb.read(args!())?;
    assert!(matches!(&out, Arg::Entry(e) if e.is(&a)));
    assert_eq!((mb.read_head(), mb.write_head()), (1, 1));
    assert!(mb.read(args!())?.is_none());
    // Raw payloads are lifted to constant entries on both sides.
    mb.__setitem__(5, Arg::Bytes(Arc::from(&b"frame"[..])))?;
    assert_eq!(
        mb.__getitem__(2)?.data()?,
        Arg::Bytes(Arc::from(&b"frame"[..]))
    );
    let e = mb.__getitem__("0").unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::TypeError,
            "MultiBuffer indices must be integers, not str".into()
        )
    );
    // Mapped mode: external memory, `write()` only moves the head.
    let dma = py!([None, None]);
    let mapped = MultiBuffer::new(args!(slots = &dma, mapped = true))?;
    dma.__setitem__(0, Arg::Bytes(Arc::from(&b"raw"[..])))?;
    assert_eq!(mapped.write(args!())?, 0);
    assert_eq!(
        mapped.read(args!())?.data()?,
        Arg::Bytes(Arc::from(&b"raw"[..]))
    );
    let e = MultiBuffer::new(args!(slots = py!([]))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            "MultiBuffer.slots must contain at least one slot".into()
        )
    );
    let e = MultiBuffer::new(args!(capacity = 0)).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValidationError);
    mapped.empty(args!())?;
    assert_eq!(dma, py!([None, None]));
    Ok(())
}

#[test]
fn model_class_descriptor_of_the_pool() {
    let cls: &'static ModelClass = _LAILA_IDENTIFIABLE_POOL::__class__();
    let names: Vec<String> = cls.all_fields().into_iter().map(|(n, _)| n).collect();
    assert_eq!(
        names,
        [
            "resource",
            "batch_accelerated",
            "transformations",
            "index_enabled",
            "index_pool"
        ]
    );
    let eligible = laila::basics::definitions::cli_capable::_eligible_model_fields_of(cls);
    assert_eq!(eligible, ["batch_accelerated", "index_enabled"]);
    assert!(cls.is_subclass("_LAILA_CLI_CAPABLE_CLASS"));
    assert!(cls.is_subclass("_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT"));
    let hdf5 = HDF5Pool::__class__();
    assert_eq!(hdf5.mro()[1].name, "_LAILA_IDENTIFIABLE_POOL");
}
