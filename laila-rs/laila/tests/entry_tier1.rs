//! Tier 1 (typed Rust) coverage of `laila::entry` and the `Manifest`
//! subclass -- the behaviour the Python suites reach only through pools and
//! taskforces (Phase 3): the `EntryState` enum surface, the atomic mixin on
//! entries (`with entry.atomic()`, `@synchronized`), `laila.encryption_key`,
//! `laila.set_active_namespace`, the async build path, and the mapping /
//! iteration protocol of `Manifest`.

use std::sync::Arc;

use laila::atomic::definitions::{synchronized, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT};
use laila::entry::constitution::{register_builder_token, ComplexConstitution};
use laila::entry::{Entry, EntryKind, EntryState};
use laila::policy::central::memory::schema::Manifest;
use laila::prelude::*;
use laila::python::builtins::dict;
use laila::python::stdlib::asyncio;

fn kw(pairs: Vec<(&str, Arg)>) -> Arg {
    let out = dict(Args::new()).unwrap();
    for (k, v) in pairs {
        out.__setitem__(Arg::from(k), v).unwrap();
    }
    out
}

fn err_text(e: &LailaError) -> (ExcKind, String) {
    (e.kind, e.__str__())
}

#[test]
fn entry_state_enum_surface() {
    assert_eq!(EntryState::MEMBERS.len(), 6);
    let names: Vec<&str> = EntryState::MEMBERS.iter().map(|m| m.name()).collect();
    assert_eq!(
        names,
        ["READY", "POOLED", "POOLING", "STAGED", "STALE", "NA"]
    );
    let values: Vec<i64> = EntryState::MEMBERS.iter().map(|m| m.value()).collect();
    assert_eq!(values, [1, 2, 3, 4, 5, 6]);
    assert_eq!(EntryState::__getitem__("STALE").unwrap(), EntryState::STALE);
    let e = EntryState::__getitem__("NOPE").unwrap_err();
    assert_eq!(err_text(&e), (ExcKind::KeyError, "'NOPE'".into()));
    assert_eq!(EntryState::from_value(6).unwrap(), EntryState::NA);
    assert_eq!(
        EntryState::from_value(7).unwrap_err().__str__(),
        "7 is not a valid EntryState"
    );
    let a: Arg = EntryState::READY.into_arg();
    assert_eq!(repr(&a), "<EntryState.READY: 1>");
    assert_eq!(a.__str__(), "EntryState.READY");
    assert_eq!(a.__getattr__("name").unwrap(), "READY");
    assert_eq!(a.__getattr__("value").unwrap(), Arg::Int(1));
    assert!(isinstance::<EntryState>(&a));
    assert_eq!(a, EntryState::READY.into_arg());
    assert_ne!(a, EntryState::NA.into_arg());
}

#[test]
fn entry_lifecycle_texts() {
    let e = Entry::new(args!(data = Arg::Int(1))).unwrap();
    // The raw constructor defaults to STAGED; the factories to READY.
    assert_eq!(e.state(), EntryState::STAGED);
    assert_eq!(e.__class_name__(), "Entry");
    assert_eq!(e.kind(), EntryKind::Entry);
    assert_eq!(
        err_text(&e.set_state(Arg::from("READY")).unwrap_err()),
        (ExcKind::ValueError, "state must be an EntryState".into())
    );
    assert_eq!(
        err_text(&e.set_constitution(Arg::Int(1)).unwrap_err()),
        (
            ExcKind::TypeError,
            "constitution must be a Constitution or None".into()
        )
    );
    assert_eq!(
        err_text(&e.metadata().unwrap_err()),
        (
            ExcKind::NotImplementedError,
            "Metadata is not implemented for Entry".into()
        )
    );
    assert_eq!(
        err_text(&Entry::new(args!(constitution = Arg::Int(3))).unwrap_err()),
        (
            ExcKind::TypeError,
            "constitution must be a Constitution, list[str], or str".into()
        )
    );
    // a list of codes is a simple constitution; the entry starts STAGED
    let staged = Entry::new(args!(constitution = py!([]), state = EntryState::READY)).unwrap();
    assert_eq!(staged.state(), EntryState::STAGED);
    assert!(!staged.constitution().is_none());
    // serialize refuses non-READY entries with the exact text
    let e2 = Entry::constant(args!(data = Arg::Int(1))).unwrap();
    e2.__setattr__("_state", EntryState::POOLING).unwrap();
    assert_eq!(
        err_text(
            &e2.serialize(args!(
                transformations = laila::entry::transformation_base64()
            ))
            .unwrap_err()
        ),
        (
            ExcKind::RuntimeError,
            "Cannot serialize entry in state 'POOLING'; only READY entries can be serialized."
                .into()
        )
    );
    // pydantic: an unknown public attribute is a ValueError, a private one is stored
    assert_eq!(
        err_text(&e2.__setattr__("nope", Arg::Int(1)).unwrap_err()),
        (
            ExcKind::ValueError,
            "\"Entry\" object has no field \"nope\"".into()
        )
    );
    e2.__setattr__("_mine", Arg::Int(9)).unwrap();
    assert_eq!(e2.__getattr__("_mine").unwrap(), Arg::Int(9));
    assert_eq!(
        err_text(&e2.__getattr__("nickname").unwrap_err()),
        (
            ExcKind::AttributeError,
            "'Entry' object has no attribute 'nickname'".into()
        )
    );
    // locally-modified bookkeeping
    assert!(!e2.locally_modified());
    e2.set_data(Arg::Int(2)).unwrap();
    assert!(e2.locally_modified());
    assert!(!e2.bump_evolution_if_locally_modified()); // a constant never bumps
    e2.mark_memorized();
    assert!(!e2.locally_modified());
    let v = Entry::variable(args!(data = Arg::Int(1))).unwrap();
    assert!(!v.bump_evolution_if_locally_modified());
    v.set_data(Arg::Int(2)).unwrap();
    let before = v.creation_timestamp();
    assert!(v.bump_evolution_if_locally_modified());
    assert_eq!(v.evolution(), Arg::Int(1));
    assert!(v.global_id().ends_with("@evolution=1"));
    assert!(v.creation_timestamp() >= before);
    // evolve keeps the constitution rule
    let lazy = Entry::new(args!(constitution = py!([]), evolution = Arg::Int(0))).unwrap();
    assert_eq!(
        lazy.evolve(Args::new()).unwrap_err().kind,
        ExcKind::NotImplementedError
    );
    // ``type(entry)`` is the class
    let cls = r#type(&e2);
    assert_eq!(cls.__getattr__("__name__").unwrap(), "Entry");
    assert_eq!(repr(&cls), "<class 'laila.entry.entry.Entry'>");
}

#[test]
fn entry_is_a_locally_atomic_identifiable_object() {
    let e = Entry::constant(args!(data = py!([1, 2]))).unwrap();
    assert!(!e.locked(Args::new()));
    {
        let cm = e.atomic(args!(scope = "local")).unwrap();
        let locked = cm.__enter__().unwrap();
        assert!(e.locked(Args::new()));
        // re-entrant: reads under the lock still work
        assert_eq!(locked.data().unwrap(), py!([1, 2]));
        assert_eq!(locked.global_id(), e.global_id());
    }
    assert!(!e.locked(Args::new()));
    assert_eq!(
        e.atomic(args!(scope = "global"))
            .unwrap()
            .__enter__()
            .unwrap_err()
            .__str__(),
        "Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call."
    );
    // ``@synchronized`` locks every entry argument around the call
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let f = callable(move |x: Arg| {
        let locked = match &x {
            Arg::Entry(e) => e.locked(Args::new()),
            _ => false,
        };
        seen2.lock().unwrap().push(locked);
        Ok::<_, LailaError>(x)
    });
    let wrapped = synchronized(args!(f)).unwrap();
    wrapped.__call__(args!(e.clone())).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![true]);
    assert!(!e.locked(Args::new()));
    // the snapshot has its own lock
    let snap = e._snapshot().unwrap();
    assert!(!snap.is(&e));
    assert_eq!(snap, e);
    let cm = e.atomic(Args::new()).unwrap();
    let _g = cm.__enter__().unwrap();
    assert!(!snap.locked(Args::new()));
}

#[test]
fn facade_seams() {
    let _lock = laila::python::unittest::test_lock();
    // laila.encryption_key <-> laila.args.encryption.key
    let previous = laila.encryption_key();
    laila
        .__setattr__("encryption_key", "k".repeat(43) + "=")
        .unwrap();
    assert_eq!(laila.encryption_key(), "k".repeat(43) + "=");
    assert_eq!(
        laila
            .args()
            .__getattr__("encryption")
            .unwrap()
            .__getattr__("key")
            .unwrap(),
        "k".repeat(43) + "="
    );
    laila.set_encryption_key(Arg::None);
    assert!(laila.encryption_key().is_none());
    laila.set_encryption_key(previous);
    assert_eq!(
        laila.__setattr__("nope", Arg::None).unwrap_err().kind,
        ExcKind::AttributeError
    );

    // laila.set_active_namespace(key) == uuid5(NAMESPACE_DNS, key)
    let before = laila.get_active_namespace();
    laila
        .set_active_namespace("acme.research.experiments")
        .unwrap();
    assert_eq!(
        laila.get_active_namespace().to_string(),
        laila::_compat::uuid::uuid5(
            &laila::_compat::uuid::NAMESPACE_DNS,
            "acme.research.experiments"
        )
    );
    let a = Entry::constant(args!(data = Arg::Int(1), nickname = "n")).unwrap();
    laila::_compat::uuid_ns::set_active_namespace_uuid(&before.to_string()).unwrap();
    let b = Entry::constant(args!(data = Arg::Int(1), nickname = "n")).unwrap();
    assert_ne!(a.uuid(), b.uuid());
    assert_eq!(
        laila.set_active_namespace(Arg::Int(1)).unwrap_err().kind,
        ExcKind::TypeError
    );
}

#[test]
fn async_build_path() {
    let dir = std::env::temp_dir().join(format!("laila-rs-entry-tier1-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("LAILA_CONSTITUTION_KEY_FILE", dir.join("constitution.key"));

    let x = Entry::constant(args!(data = Arg::Int(2))).unwrap();
    let y = Entry::constant(args!(data = Arg::Int(3))).unwrap();
    let m = Manifest::new(args!(
        data = kw(vec![("x", x.into_arg()), ("y", y.into_arg())])
    ))
    .unwrap();
    assert_eq!(m.__class_name__(), "Manifest");
    assert_eq!(m.kind(), EntryKind::Manifest);
    assert!(m.global_id().starts_with("LAILA:MANIFEST:"));
    assert_eq!(m.state(), EntryState::READY);
    assert!(m.evolution().is_none());
    m.memorize(Args::new()).unwrap().wait(Args::new()).unwrap();
    assert!(m._pending_entries().is_none());

    register_builder_token(
        "tier1:product",
        Arc::new(|manifest: &Arg| {
            let d = manifest.__getattr__("realized")?;
            let x = d.__getitem__("x")?.__getattr__("data")?;
            let y = d.__getitem__("y")?.__getattr__("data")?;
            x.__mul__(&y)
        }),
    );
    let e = Entry::variable(args!(constitution = "tier1:product", manifest = m.clone())).unwrap();
    assert_eq!(e.state(), EntryState::STAGED);
    assert!(isinstance::<ComplexConstitution>(&e.constitution()));
    let err = e.data().unwrap_err();
    assert_eq!(err.kind, ExcKind::EntryNotBuiltError);
    assert!(err.kind.is_subclass_of(ExcKind::RuntimeError));

    // the awaitable twin of ``laila.build``
    let built = asyncio::run(e._build_async(Args::new())).unwrap();
    assert!(built.is(&e));
    assert_eq!(e.data().unwrap(), Arg::Int(6));
    assert_eq!(e.state(), EntryState::READY);
    assert!(e.constitution().is_none());
    // idempotent
    assert!(asyncio::run(e._build_async(Args::new())).unwrap().is(&e));
    // ``_build(asynchronous=True)`` returns the coroutine of the same path
    let e2 = Entry::variable(args!(constitution = "tier1:product", manifest = m.clone())).unwrap();
    let coro = e2._build(args!(asynchronous = true)).unwrap();
    assert!(asyncio::asyncio.iscoroutine(coro.clone()));
    assert_eq!(
        asyncio::run_value(coro).unwrap().data().unwrap(),
        Arg::Int(6)
    );
    // a lazily-bound manifest (deserialized constitution) resolves from memory
    let blob = e2.as_dict().unwrap();
    let e3 = Entry::variable(args!(constitution = "tier1:product", manifest = m.clone())).unwrap();
    let serialized = kw(vec![
        ("_uuid", Arg::from(e3.uuid().as_str())),
        ("_evolution", Arg::Int(0)),
        ("_scopes", py!(["ENTRY"])),
        ("_state", Arg::from("STAGED")),
        ("payload", Arg::None),
        (
            "constitution",
            e3.constitution_opt().unwrap().as_dict().unwrap(),
        ),
    ]);
    let _ = blob;
    let hydrated = Entry::_build_from_dict_sync(serialized).unwrap();
    assert_eq!(
        hydrated.state(),
        EntryState::STAGED,
        "complex entries stay staged"
    );
    assert_eq!(
        laila
            .build(args!(hydrated.clone()))
            .unwrap()
            .wait(Args::new())
            .unwrap(),
        Arg::Entry(hydrated.clone())
    );
    assert_eq!(hydrated.data().unwrap(), Arg::Int(6));
    // process pools are Phase 3
    let e4 = Entry::variable(args!(constitution = "tier1:product", manifest = m.clone())).unwrap();
    assert_eq!(
        asyncio::run(e4._build_async(args!(_process_taskforce_id = "LAILA:TASK_FORCE:x")))
            .unwrap_err()
            .kind,
        ExcKind::NotImplementedError
    );
    // ``command.taskforces[taskforce_id]``: an unknown taskforce is a KeyError.
    assert_eq!(
        laila
            .build(args!(e4.clone(), taskforce_id = "x"))
            .unwrap_err()
            .kind,
        ExcKind::KeyError
    );
}

#[test]
fn manifest_mapping_protocol_and_helpers() {
    let a = Entry::constant(args!(data = Arg::Int(1))).unwrap();
    let b = Entry::constant(args!(data = Arg::Int(2))).unwrap();
    let c = Entry::variable(args!(data = Arg::Int(3))).unwrap();
    let m = Manifest::new(args!(
        data = kw(vec![
            ("a", a.clone().into_arg()),
            ("more", kw(vec![("b", b.clone().into_arg())])),
            ("list", py!([]).clone()),
        ])
    ))
    .unwrap();
    m.__getitem__("list")
        .unwrap()
        .append(Arg::from(c.global_id().as_str()))
        .unwrap();
    assert_eq!(m.__len__().unwrap(), 3);
    assert_eq!(m.keys(Args::new()).unwrap(), py!(["a", "more", "list"]));
    assert_eq!(
        m.__iter__().unwrap(),
        vec![
            Arg::from(a.global_id().as_str()),
            Arg::from(b.global_id().as_str()),
            Arg::from(c.global_id().as_str())
        ]
    );
    assert!(m.__contains__(a.global_id()));
    assert!(!m.__contains__(Arg::Int(1)));
    assert!(!m.__contains__("LAILA:ENTRY:nope"));
    assert_eq!(m.__getitem__("a").unwrap(), a.global_id().as_str());
    assert_eq!(
        err_text(&m.__getitem__("zz").unwrap_err()),
        (ExcKind::KeyError, "'zz'".into())
    );
    assert_eq!(m.blueprint().unwrap(), m.data().unwrap());
    assert_eq!(
        m.__repr__(),
        format!("Manifest({}, entries=3)", m.global_id())
    );
    assert_eq!(m.__str__(), m.global_id());
    assert_eq!(m._pending_entries().__len__().unwrap(), 2);
    // the dynamic surface
    let dynamic: Arg = m.clone().into_arg();
    assert!(isinstance::<Manifest>(&dynamic));
    assert!(isinstance::<Entry>(&dynamic));
    assert_eq!(dynamic.__len__().unwrap(), 3);
    assert_eq!(dynamic.__iter__().unwrap().len(), 3);
    assert!(dynamic.__contains__(a.global_id()).unwrap());
    assert_eq!(
        dynamic.keys(Args::new()).unwrap(),
        m.keys(Args::new()).unwrap()
    );
    assert_eq!(repr(&dynamic), m.__repr__());
    assert_eq!(
        r#type(&dynamic).__getattr__("__name__").unwrap(),
        "Manifest"
    );
    assert_eq!(
        dynamic.__getattr__("_DEFAULT_SCOPES").unwrap(),
        py!(["MANIFEST"])
    );
    // mixing entries and strings is rejected; so are bad blueprints
    let e = Manifest::new(args!(
        data = kw(vec![
            ("a", a.clone().into_arg()),
            ("s", Arg::from("LAILA:ENTRY:x"))
        ])
    ))
    .unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert!(e
        .__str__()
        .starts_with("Manifest data must contain either all Entry objects"));
    for (bad, text) in [
        (
            kw(vec![("k", Arg::Int(1))]),
            "Blueprint values must be str, list[str], or dict \u{2014} got <class 'int'>",
        ),
        (
            kw(vec![("k", py!([1]))]),
            "List values in blueprint must be strings, got <class 'int'>",
        ),
    ] {
        let e = Manifest::new(args!(blueprint = bad)).unwrap_err();
        assert_eq!(err_text(&e), (ExcKind::ValueError, text.into()));
    }
    let e = Manifest::_validate_blueprint(&py!([1])).unwrap_err();
    assert_eq!(e.__str__(), "Blueprint must be a dict.");
    let non_str = dict(Args::new()).unwrap();
    non_str.__setitem__(Arg::Int(1), Arg::from("x")).unwrap();
    assert_eq!(
        Manifest::_validate_blueprint(&non_str)
            .unwrap_err()
            .__str__(),
        "Blueprint keys must be strings, got <class 'int'>"
    );
    // an empty manifest
    let empty = Manifest::new(Args::new()).unwrap();
    assert!(empty.data().unwrap().is_none());
    assert_eq!(empty.__len__().unwrap(), 0);
    assert_eq!(empty.keys(Args::new()).unwrap(), py!([]));
    assert_eq!(
        err_text(&empty.realized().unwrap_err()),
        (
            ExcKind::RuntimeError,
            "No blueprint to resolve \u{2014} manifest is empty.".into()
        )
    );
    assert_eq!(
        err_text(&empty.memorize(Args::new()).unwrap_err()),
        (
            ExcKind::RuntimeError,
            "Nothing to memorize \u{2014} manifest has no blueprint.".into()
        )
    );
    assert_eq!(
        err_text(&empty.sub_manifest(py!(["a"])).unwrap_err()),
        (
            ExcKind::RuntimeError,
            "Cannot create sub-manifest \u{2014} manifest is empty.".into()
        )
    );
    assert_eq!(empty.__getitem__("a").unwrap_err().__str__(), "'a'");
    assert!(!empty.__bool__());
    // sub_manifest / extend / + / +=
    let sub = m.sub_manifest(py!(["a"])).unwrap();
    assert_eq!(sub.keys(Args::new()).unwrap(), py!(["a"]));
    assert_eq!(
        err_text(&m.sub_manifest(py!(["a", "zz"])).unwrap_err()),
        (
            ExcKind::KeyError,
            "\"Keys not found in manifest: ['zz']\"".into()
        )
    );
    let other = Manifest::new(args!(
        blueprint = kw(vec![("z", Arg::from("LAILA:ENTRY:z"))])
    ))
    .unwrap();
    // `+` on typed manifests: `__add__`'s `NotImplemented` is the operator's `TypeError`.
    let sum = (&sub + other.clone()).unwrap();
    assert_eq!(sum.keys(Args::new()).unwrap(), py!(["a", "z"]));
    assert_eq!(
        err_text(&sum.extend(args!(other.clone())).unwrap_err()),
        (
            ExcKind::KeyError,
            "\"Duplicate top-level keys: ['z']\"".into()
        )
    );
    sum.extend(args!(other.clone(), overwrite = true)).unwrap();
    assert!(sum.locally_modified());
    assert_eq!(
        err_text(&sum.extend(args!(Arg::Int(1))).unwrap_err()),
        (
            ExcKind::TypeError,
            "extend() requires a Manifest, got int".into()
        )
    );
    let grown = sub
        .iadd_op(
            Manifest::new(args!(
                blueprint = kw(vec![("q", Arg::from("LAILA:ENTRY:q"))])
            ))
            .unwrap(),
        )
        .unwrap();
    assert!(grown.is(&sub));
    assert_eq!(sub.__len__().unwrap(), 2);
    // `__add__` / `__iadd__` with a non-manifest answer `NotImplemented`;
    // the operators raise `TypeError`.
    assert!(laila::python::builtins::is_not_implemented(
        &sub.__add__(Arg::Int(1)).unwrap()
    ));
    assert!(laila::python::builtins::is_not_implemented(
        &sub.__iadd__(Arg::Int(1)).unwrap()
    ));
    assert_eq!((&sub + Arg::Int(1)).unwrap_err().kind, ExcKind::TypeError);
    assert_eq!(
        sub.iadd_op(Arg::Int(1)).unwrap_err().kind,
        ExcKind::TypeError
    );
    assert_eq!(
        Manifest::new(Args::new())
            .unwrap()
            .__add__(empty.clone())
            .unwrap()
            .__len__()
            .unwrap(),
        0
    );
    // floating leaves
    let floating = Manifest::new(args!(blueprint = kw(vec![
        ("pinned", Arg::from("LAILA:ENTRY:00000000-0000-4000-8000-000000000000@evolution=2")),
        ("latest", Arg::from("LAILA:ENTRY:00000000-0000-4000-8000-000000000000@evolution=-1")),
        ("stamped", Arg::from("LAILA:ENTRY:00000000-0000-4000-8000-000000000000@creation_timestamp=2026-01-01T00:00:00.000+00:00")),
        ("broken", Arg::from("LAILA:ENTRY:x@")),
    ])))
    .unwrap();
    assert_eq!(
        floating.floating_leaves().unwrap(),
        py!([
            "LAILA:ENTRY:00000000-0000-4000-8000-000000000000@evolution=-1",
            "LAILA:ENTRY:00000000-0000-4000-8000-000000000000@creation_timestamp=2026-01-01T00:00:00.000+00:00"
        ])
    );
    // the SQL parser half
    let (items, from, where_) = Manifest::_parse_sql_typed(&Arg::from(
        "SELECT t.a, * FROM `tbl` WHERE owner == 'alice';",
    ))
    .unwrap();
    assert_eq!(items, vec!["a".to_string(), "*".to_string()]);
    assert_eq!(from, "tbl");
    assert_eq!(where_.as_deref(), Some("owner = 'alice'"));
    let (items, from, where_) = Manifest::_parse_sql_typed(&Arg::from("select x from t")).unwrap();
    assert_eq!(
        (items, from, where_),
        (vec!["x".to_string()], "t".to_string(), None)
    );
    assert_eq!(
        Manifest::_parse_sql(Arg::from("SELECT * FROM t ORDER BY x")).unwrap_err().__str__(),
        "Unsupported SQL clause: ORDER BY. Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported."
    );
    assert_eq!(
        Manifest::_parse_sql(Arg::from("DELETE FROM t"))
            .unwrap_err()
            .__str__(),
        "Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported."
    );
    assert_eq!(
        Manifest::_parse_sql(Arg::Int(1)).unwrap_err().__str__(),
        "sql() query must be a string."
    );
    assert_eq!(Manifest::_strip_table_prefix("`t`.\"col\""), "col");
    assert_eq!(
        Manifest::_validate_ident(Arg::from("1x"))
            .unwrap_err()
            .__str__(),
        "Invalid column identifier for indexing: '1x'"
    );
    assert_eq!(
        err_text(&m.sql("SELECT * FROM m").unwrap_err()),
        (
            ExcKind::TypeError,
            "Cannot index column 'value': row 'list' has value of type list. Index columns must be \
             primitive scalars. Flatten lists/dicts in your _sql_rows() override (e.g. \
             ','.join(tags) or json.dumps(meta))."
                .into()
        )
    );
    let (rows, columns) = m._sql_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(columns, vec![Arg::from("value"), Arg::from("b")]);
    // from_dict through the MANIFEST scope builder
    let d = m.as_dict().unwrap();
    assert_eq!(d.__getitem__("_scopes").unwrap(), py!(["MANIFEST"]));
    let back = laila::entry::constitution::build_maps::build_by_scope(args!(d)).unwrap();
    assert!(isinstance::<Manifest>(&back));
    let back = Manifest::from_arg(&back).unwrap();
    assert_eq!(back.global_id(), m.global_id());
    assert_eq!(back.data().unwrap(), m.data().unwrap());
    assert_eq!(Manifest::from_dict(m.as_dict().unwrap()).unwrap(), back);
}
