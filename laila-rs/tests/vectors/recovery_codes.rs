//! Recovery-code strings are hashed into constitutions, so the emitted Python
//! source must be byte-identical to `laila`'s `model_post_init` output
//! (`codecs.json["recovery_codes"]`), and `compile_backward` must execute the
//! recognised snippets natively.

mod fixture;

use fixture::*;
use laila::_codecs::recovery_codes as rc;
use laila::prelude::*;
use laila::python::builtins::dict;
use laila::python::errors::ExcKind;

fn kw(pairs: Vec<(&str, Arg)>) -> Arg {
    let out = dict(Args::new()).unwrap();
    for (k, v) in pairs {
        out.__setitem__(Arg::from(k), v).unwrap();
    }
    out
}

fn b(bytes_: &[u8]) -> Arg {
    laila::python::object::bytes(bytes_)
}

fn emitted() -> Vec<(&'static str, String)> {
    let fp = fixture()["fernet"]["fingerprint"].as_str().unwrap();
    vec![
        ("base64", rc::emit_base64(None)),
        (
            "base64_kw",
            rc::emit_base64(Some(&kw(vec![
                ("altchars", b(b"-_")),
                ("validate", Arg::Bool(true)),
            ]))),
        ),
        ("zlib", rc::emit_zlib(None)),
        (
            "zlib_kw",
            rc::emit_zlib(Some(&kw(vec![
                ("wbits", Arg::Int(15)),
                ("bufsize", Arg::Int(16384)),
            ]))),
        ),
        ("json_string", rc::emit_json_string()),
        ("msgpack", rc::emit_msgpack(None)),
        (
            "msgpack_kw",
            rc::emit_msgpack(Some(&kw(vec![("use_list", Arg::Bool(false))]))),
        ),
        ("numpy", rc::emit_numpy(None)),
        ("pickle", rc::emit_pickle(None)),
        (
            "pickle_kw",
            rc::emit_pickle(Some(&kw(vec![
                ("fix_imports", Arg::Bool(false)),
                ("encoding", Arg::from("ASCII")),
            ]))),
        ),
        ("fernet", rc::emit_fernet(fp, None)),
        (
            "fernet_kw",
            rc::emit_fernet(fp, Some(&kw(vec![("ttl", Arg::Int(60))]))),
        ),
    ]
}

#[test]
fn emit_is_string_identical() {
    let r = &fixture()["recovery_codes"];
    for (name, got) in emitted() {
        let want = r[name]
            .as_str()
            .unwrap_or_else(|| panic!("fixture lacks {name}"));
        assert_eq!(got, want, "emit {name}");
    }
}

#[test]
fn recognize_every_template() {
    let r = &fixture()["recovery_codes"];
    for (name, _) in emitted() {
        let info = rc::recognize(r[name].as_str().unwrap())
            .unwrap()
            .unwrap_or_else(|| panic!("recognize {name}"));
        assert_eq!(info.name, name.trim_end_matches("_kw"));
    }
    if let Some(torch) = r.get("torch").and_then(|v| v.as_str()) {
        assert_eq!(rc::recognize(torch).unwrap().unwrap().name, "torch");
    }
    assert_eq!(
        rc::recognize(&rc::emit_torch(None)).unwrap().unwrap().name,
        "torch"
    );
}

#[test]
fn recognized_kwargs_and_fingerprint() {
    let r = &fixture()["recovery_codes"];
    let fp = fixture()["fernet"]["fingerprint"].as_str().unwrap();
    let get = |n: &str| rc::recognize(r[n].as_str().unwrap()).unwrap().unwrap();
    assert_eq!(
        get("base64_kw").kwargs,
        kw(vec![("altchars", b(b"-_")), ("validate", Arg::Bool(true))])
    );
    assert_eq!(
        get("zlib_kw").kwargs,
        kw(vec![("wbits", Arg::Int(15)), ("bufsize", Arg::Int(16384))])
    );
    let fi = get("fernet_kw");
    assert_eq!(fi.fingerprint.as_deref(), Some(fp));
    assert_eq!(fi.kwargs, kw(vec![("ttl", Arg::Int(60))]));
    assert_eq!(get("base64").kwargs, kw(vec![]));
    assert!(rc::recognize("def backward(x):\n    return x\n")
        .unwrap()
        .is_none());
}

#[test]
fn compile_backward_executes_natively() {
    let fx = fixture();
    let r = &fx["recovery_codes"];
    let tf = &fx["transform_forward"];
    let code = |n: &str| r[n].as_str().unwrap();
    let zin = Arg::from(fx["zlib"]["input"].as_str().unwrap());

    let zb = rc::compile_backward(code("zlib")).unwrap();
    assert_eq!(
        zb(&Arg::from(tf["zlib_default"].as_str().unwrap())).unwrap(),
        zin
    );
    assert_eq!(
        zb(&Arg::from(tf["zlib_level9"].as_str().unwrap())).unwrap(),
        zin
    );

    assert_eq!(
        rc::compile_backward(code("base64")).unwrap()(&Arg::from("aGVsbG8=")).unwrap(),
        b(b"hello")
    );
    let all: Vec<u8> = (0..=255u8).collect();
    assert_eq!(
        rc::compile_backward(code("base64_kw")).unwrap()(&Arg::from(
            tf["base64_alt"].as_str().unwrap()
        ))
        .unwrap(),
        b(&all)
    );
    assert_eq!(
        rc::compile_backward(code("json_string")).unwrap()(&Arg::from(r#"{"a":[1,2.5,null]}"#))
            .unwrap(),
        kw(vec![(
            "a",
            vec![Arg::Int(1), Arg::Float(2.5), Arg::None].into_arg()
        )])
    );
    let dm = &fx["cases"]["dict_mixed"];
    assert_eq!(
        rc::compile_backward(code("msgpack")).unwrap()(&b(&decode(
            dm["msgpack"].as_str().unwrap()
        )))
        .unwrap(),
        case("dict_mixed")
    );
    assert_eq!(
        rc::compile_backward(code("pickle")).unwrap()(&b(&decode(dm["pickle"].as_str().unwrap())))
            .unwrap(),
        case("dict_mixed")
    );
    let arr = rc::compile_backward(code("numpy")).unwrap()(&b(&decode(
        fx["cases"]["npy_f64"]["npy"].as_str().unwrap(),
    )))
    .unwrap();
    assert!(arr
        .as_ndarray()
        .unwrap()
        .array_equal(&ndarray("npy_f64"), true));

    assert_eq!(
        rc::compile_backward("x = 1\n").err().map(|e| e.kind),
        Some(ExcKind::ValueError)
    );
    assert_eq!(zb(&Arg::Int(123)).unwrap_err().kind, ExcKind::TypeError);
    assert_eq!(
        rc::compile_backward(&rc::emit_torch(None)).unwrap()(&b(b""))
            .unwrap_err()
            .kind,
        ExcKind::NotImplementedError
    );
}

#[test]
fn kwargs_containing_template_text_roundtrip() {
    // A kwargs repr that quotes fragments of the template itself.
    let tricky = kw(vec![
        ("encoding", Arg::from("return pickle.loads(inp, **kwargs)")),
        ("fix_imports", Arg::from("kwargs = {}")),
    ]);
    let code = rc::emit_pickle(Some(&tricky));
    let info = rc::recognize(&code).unwrap().unwrap();
    assert_eq!(info.name, "pickle");
    assert_eq!(info.kwargs, tricky);
}
