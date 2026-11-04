//! Unit-level checks of `laila.entry.compdata` / `laila.entry.constitution`
//! semantics that the Python test-suite exercises through `Entry` (which is
//! being rewritten): dispatch, serializer fallback, deep copies, the trust
//! gate, HMAC signing, `from_dict` error texts, the builder-token design
//! for complex constitutions and the scope builder map.

use std::sync::{Arc, OnceLock};

use laila::_codecs::fernet::Fernet;
use laila::_compat::ndarray::NDArray;
use laila::entry::compdata::taxonomy::{CDKind, CD_dict, CD_generic, CD_list, CD_numpyarray};
use laila::entry::compdata::transformation::{
    self as tf, is_trusted_backward_code, set_encryption_key, Base64, FernetEncryption, JsonString,
    MsgpackSerializer, PickleSerializer, Transformation, TransformationSequence, Zlib,
};
use laila::entry::compdata::ComputationalData;
use laila::entry::constitution::{
    build_maps, complex_constitution, register_builder_token, trust, ComplexConstitution,
    Constitution, SimpleConstitution,
};
use laila::policy::central::memory::schema::Manifest;
use laila::prelude::*;
use laila::python::builtins::dict;
use laila::python::object::bytes;

fn setup() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("laila-rs-constitution-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("LAILA_CONSTITUTION_KEY_FILE", dir.join("constitution.key"));
    });
}

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

// --------------------------------------------------------------------------
// ComputationalData taxonomy
// --------------------------------------------------------------------------

#[test]
fn compdata_dispatch_by_payload_type() {
    let d = ComputationalData::new(kw(vec![("a", Arg::Int(1))])).unwrap();
    assert_eq!(d.kind(), CDKind::Dict);
    assert_eq!(d.serializer().unwrap().name(), "msgpack");
    assert!(isinstance::<CD_dict>(&d.clone().into_arg()));
    assert!(!isinstance::<CD_generic>(&d.clone().into_arg()));

    let l = ComputationalData::new(vec![Arg::Int(1)].into_arg()).unwrap();
    assert_eq!(l.kind(), CDKind::List);
    assert_eq!(l.serializer().unwrap().name(), "msgpack");
    assert!(isinstance::<CD_list>(&l.into_arg()));

    let t = ComputationalData::new(Arg::Tuple(vec![Arg::Int(1)].into())).unwrap();
    assert_eq!(t.kind(), CDKind::List);
    // tuples default to pickle so they come back as tuples
    assert_eq!(t.serializer().unwrap().name(), "pickle");

    let n = ComputationalData::new(NDArray::arange(3, "<i8").unwrap().into_arg()).unwrap();
    assert_eq!(n.kind(), CDKind::NumpyArray);
    assert_eq!(n.serializer().unwrap().name(), "numpy");
    assert!(isinstance::<CD_numpyarray>(&n.into_arg()));

    for v in [
        Arg::Int(7),
        Arg::from("s"),
        Arg::Bool(true),
        bytes(b"x"),
        Arg::Float(1.5),
    ] {
        let g = ComputationalData::new(v).unwrap();
        assert_eq!(g.kind(), CDKind::Generic);
        assert_eq!(g.serializer().unwrap().name(), "pickle");
    }
    // ``data=`` keyword form is equivalent
    let k = ComputationalData::new(args!(data = Arg::Int(1))).unwrap();
    assert_eq!(k.data(), Arg::Int(1));
}

#[test]
fn compdata_constructor_errors() {
    let e = ComputationalData::new(Args::new()).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::TypeError,
            "Missing required argument 'data'".into()
        )
    );
    let e = ComputationalData::new(Arg::None).unwrap_err();
    assert_eq!(e.__str__(), "Missing required argument 'data'");
    let e = ComputationalData::new(args!(Arg::Int(1), Arg::Int(2))).unwrap_err();
    assert_eq!(e.__str__(), "At most one positional argument (the payload)");
    let e = ComputationalData::new(args!(Arg::Int(1), data = Arg::Int(2))).unwrap_err();
    assert_eq!(
        e.__str__(),
        "Payload given both positionally and as 'data='"
    );
    // direct subclass construction bypasses dispatch and validates the type
    let e = CD_dict::new(vec![Arg::Int(1)].into_arg()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValidationError);
    assert_eq!(
        e.__str__(),
        "1 validation error for CD_dict\ndata\n  Input should be an instance of dict [type=is_instance_of, input_value=[1], input_type=list]\n    For further information visit https://errors.pydantic.dev/2.13/v/is_instance_of"
    );
    let e = CD_list::new(kw(vec![("a", Arg::Int(1))])).unwrap_err();
    assert_eq!(
        e.__str__(),
        "2 validation errors for CD_list\ndata.json-or-python[json=list[any],python=is-instance[list]]\n  Input should be an instance of list [type=is_instance_of, input_value={'a': 1}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.13/v/is_instance_of\ndata.`json-or-python[json=tuple[any, ...],python=is-instance[tuple]]`\n  Input should be an instance of tuple [type=is_instance_of, input_value={'a': 1}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.13/v/is_instance_of"
    );
    let e = CD_numpyarray::new(vec![Arg::Int(1)].into_arg()).unwrap_err();
    assert!(e.__str__().starts_with("1 validation error for CD_numpyarray\ndata\n  Input should be an instance of ndarray [type=is_instance_of, input_value=[1], input_type=list]"));
    let ok = CD_generic::new(kw(vec![])).unwrap();
    assert_eq!(ok.kind(), CDKind::Generic);
}

#[test]
fn compdata_len_shape_repr() {
    let d = ComputationalData::new(kw(vec![("a", Arg::Int(1)), ("b", Arg::Int(2))])).unwrap();
    assert_eq!(d.__len__().unwrap(), 2);
    assert_eq!(
        err_text(&d.shape().unwrap_err()),
        (
            ExcKind::AttributeError,
            "dict has no 'shape' attribute".into()
        )
    );
    assert_eq!(d.__repr__(), "CD_dict({'a': 1, 'b': 2})");

    let l = ComputationalData::new(vec![Arg::Int(1), Arg::Int(2), Arg::Int(3)].into_arg()).unwrap();
    assert_eq!(l.__len__().unwrap(), 3);
    assert_eq!(l.shape().unwrap(), Arg::Tuple(vec![Arg::Int(3)].into()));
    assert_eq!(l.__repr__(), "CD_list(type=list, len=3)");
    let t = ComputationalData::new(Arg::Tuple(vec![Arg::Int(1)].into())).unwrap();
    assert_eq!(t.__repr__(), "CD_list(type=tuple, len=1)");

    let g = ComputationalData::new(Arg::Int(7)).unwrap();
    assert_eq!(
        err_text(&g.__len__().unwrap_err()),
        (
            ExcKind::TypeError,
            "Length undefined for scalars / objects without __len__".into()
        )
    );
    assert_eq!(
        err_text(&g.shape().unwrap_err()),
        (
            ExcKind::AttributeError,
            "int has no 'shape' attribute".into()
        )
    );
    assert_eq!(g.__repr__(), "CD_generic(7)");
    let s = ComputationalData::new(Arg::from("abc")).unwrap();
    assert_eq!(s.__len__().unwrap(), 3);
    assert_eq!(s.__getitem__(Arg::Int(0)).unwrap(), "a");

    let n = ComputationalData::new(
        NDArray::arange(6, "<f8")
            .unwrap()
            .reshaped(vec![2, 3])
            .unwrap()
            .into_arg(),
    )
    .unwrap();
    assert_eq!(n.__len__().unwrap(), 2);
    assert_eq!(
        n.shape().unwrap(),
        Arg::Tuple(vec![Arg::Int(2), Arg::Int(3)].into())
    );
    assert_eq!(n.__repr__(), "CD_numpyarray(shape=(2, 3), dtype=float64)");
    let scalar = ComputationalData::new(
        NDArray::from_f64s("<f8", vec![], &[7.5])
            .unwrap()
            .into_arg(),
    )
    .unwrap();
    assert_eq!(
        scalar.__len__().unwrap_err().__str__(),
        "Length undefined for scalars / objects without __len__"
    );
}

#[test]
fn compdata_copy_semantics() {
    let inner = vec![Arg::Int(1)].into_arg();
    let d = ComputationalData::new(kw(vec![("k", inner.clone())])).unwrap();
    let shallow = d.__copy__().unwrap();
    assert!(!shallow.data().is(&d.data()));
    assert!(shallow.data().__getitem__("k").unwrap().is(&inner));
    let deep = d.__deepcopy__().unwrap();
    assert_eq!(deep.data(), d.data());
    assert!(!deep.data().__getitem__("k").unwrap().is(&inner));

    let t = ComputationalData::new(Arg::Tuple(vec![inner.clone()].into())).unwrap();
    // tuples are immutable: shallow copy keeps the same object
    assert!(t.__copy__().unwrap().data().is(&t.data()));
    let td = t.__deepcopy__().unwrap();
    assert!(matches!(td.data(), Arg::Tuple(_)));
    assert!(!td.data().__getitem__(Arg::Int(0)).unwrap().is(&inner));

    let arr = NDArray::arange(3, "<i8").unwrap().into_arg();
    let n = ComputationalData::new(arr.clone()).unwrap();
    let nc = n.__deepcopy__().unwrap();
    assert_eq!(nc.data(), arr);
    assert!(!nc.data().is(&arr));

    // non-copyable payloads are kept by reference
    let s = ComputationalData::new(Arg::from("x")).unwrap();
    assert_eq!(s.__deepcopy__().unwrap().data(), "x");
}

#[test]
fn compdata_serialize_and_fallback() {
    // dict -> msgpack
    let d = ComputationalData::new(kw(vec![("a", Arg::Int(1))])).unwrap();
    let (b, code) = d.serialize(Args::new()).unwrap();
    assert_eq!(b, bytes([0x81, 0xa1, b'a', 0x01]));
    assert_eq!(code, laila::_codecs::recovery_codes::emit_msgpack(None));

    // a tuple nested in a list cannot be expressed by strict msgpack:
    // fall back to pickle so the tuple survives the round trip
    let l =
        ComputationalData::new(vec![Arg::Int(1), Arg::Tuple(vec![Arg::Int(2)].into())].into_arg())
            .unwrap();
    assert_eq!(l.serializer().unwrap().name(), "msgpack");
    let (b, code) = l.serialize(Args::new()).unwrap();
    assert_eq!(code, laila::_codecs::recovery_codes::emit_pickle(None));
    let back = laila::_codecs::pickle::loads(&b.as_bytes_like().unwrap()).unwrap();
    assert_eq!(back, l.data());

    // an int beyond 64 bits in a dict: OverflowError -> pickle
    let big = Arg::from_bigint("18446744073709551616".parse().unwrap());
    let d = ComputationalData::new(kw(vec![("n", big)])).unwrap();
    let (_, code) = d.serialize(Args::new()).unwrap();
    assert_eq!(code, laila::_codecs::recovery_codes::emit_pickle(None));

    // an ndarray nested in a dict: TypeError -> pickle
    let d = ComputationalData::new(kw(vec![(
        "a",
        NDArray::arange(2, "<i8").unwrap().into_arg(),
    )]))
    .unwrap();
    let (_, code) = d.serialize(Args::new()).unwrap();
    assert_eq!(code, laila::_codecs::recovery_codes::emit_pickle(None));

    // the serializer setter type-checks per subclass
    let e = d
        .set_serializer(PickleSerializer::new(Args::new()).unwrap())
        .unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::TypeError,
            "serializer must be a MsgpackSerializer, got PickleSerializer".into()
        )
    );
    let e = d.set_serializer(Arg::Int(3)).unwrap_err();
    assert_eq!(
        e.__str__(),
        "serializer must be a MsgpackSerializer, got int"
    );
    d.set_serializer(MsgpackSerializer::new(Args::new()).unwrap())
        .unwrap();
    let l = ComputationalData::new(vec![Arg::Int(1)].into_arg()).unwrap();
    l.set_serializer(PickleSerializer::new(Args::new()).unwrap())
        .unwrap();
    assert_eq!(
        l.serialize(Args::new()).unwrap().1,
        laila::_codecs::recovery_codes::emit_pickle(None)
    );
    let e = l
        .set_serializer(Base64::new(Args::new()).unwrap())
        .unwrap_err();
    assert_eq!(
        e.__str__(),
        "serializer must be a MsgpackSerializer or PickleSerializer, got Base64"
    );
}

// --------------------------------------------------------------------------
// Transformations
// --------------------------------------------------------------------------

#[test]
fn transformation_fields_and_codes() {
    let b = Base64::new(Args::new()).unwrap();
    assert_eq!(b.name(), "base64");
    assert_eq!(
        b.backward_code(),
        laila::_codecs::recovery_codes::emit_base64(None)
    );
    assert_eq!(b.forward_kwargs(), kw(vec![]));
    let z = Zlib::new(args!(backward_kwargs = kw(vec![("wbits", Arg::Int(15))]))).unwrap();
    assert_eq!(
        z.backward_code(),
        laila::_codecs::recovery_codes::emit_zlib(Some(&kw(vec![("wbits", Arg::Int(15))])))
    );
    // pydantic: positional arguments are rejected, unknown keywords ignored
    let e = Base64::new(args!(Arg::Int(1))).unwrap_err();
    assert_eq!(
        e.__str__(),
        "BaseModel.__init__() takes 1 positional argument but 2 were given"
    );
    assert!(Base64::new(args!(unknown = Arg::Int(1))).is_ok());
    let e = Base64::new(args!(forward_kwargs = Arg::Int(1))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValidationError,
            "1 validation error for Base64\nforward_kwargs\n  Input should be a valid dictionary [type=dict_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/dict_type".into()
        )
    );
    let e = Base64::new(args!(forward_kwargs = Arg::None)).unwrap_err();
    assert!(e
        .__str__()
        .contains("input_value=None, input_type=NoneType"));
    let e = Base64::new(args!(name = Arg::Int(1))).unwrap_err();
    assert_eq!(
        e.__str__(),
        "1 validation error for Base64\nname\n  Input should be a valid string [type=string_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/string_type"
    );
    let bad_key = dict(Args::new()).unwrap();
    bad_key.__setitem__(Arg::Int(1), Arg::Int(2)).unwrap();
    let e = Base64::new(args!(forward_kwargs = bad_key)).unwrap_err();
    assert!(e.__str__().starts_with(
        "1 validation error for Base64\nforward_kwargs.1.[key]\n  Input should be a valid string"
    ));

    // dynamic surface: repr, attributes, equality, unhashable
    let a: Arg = Base64::new(Args::new()).unwrap().into_arg();
    assert!(repr(&a).starts_with("Base64(name='base64', forward_kwargs={}, backward_kwargs={}, backward_code='def backward(inp):\\n"));
    assert_eq!(a.__getattr__("name").unwrap(), "base64");
    assert_eq!(a, Base64::new(Args::new()).unwrap().into_arg());
    assert_eq!(
        a.__hash__().unwrap_err().__str__(),
        "unhashable type: 'Base64'"
    );
    assert!(isinstance::<Transformation>(&a));
    assert!(isinstance::<Base64>(&a));
    assert!(!isinstance::<Zlib>(&a));
}

#[test]
fn transformation_forward_backward_and_errors() {
    let b = Base64::new(Args::new()).unwrap();
    assert_eq!(b.forward(bytes(b"hello")).unwrap(), "aGVsbG8=");
    assert_eq!(b.backward(Arg::from("aGVsbG8=")).unwrap(), bytes(b"hello"));
    assert_eq!(
        err_text(&b.forward(Arg::from("str")).unwrap_err()),
        (
            ExcKind::TypeError,
            "Base64.forward expects bytes/bytearray/memoryview".into()
        )
    );
    assert_eq!(
        err_text(&b.backward(Arg::Int(1)).unwrap_err()),
        (ExcKind::ValueError, "Invalid Base64 payload".into())
    );
    let alt = Base64::new(args!(
        forward_kwargs = kw(vec![("altchars", bytes(b"-_"))]),
        backward_kwargs = kw(vec![("altchars", bytes(b"-_"))])
    ))
    .unwrap();
    let all: Vec<u8> = (0..=255u8).collect();
    let enc = alt.forward(bytes(&all)).unwrap();
    assert!(!enc.__str__().contains('+') && !enc.__str__().contains('/'));
    assert_eq!(alt.backward(&enc).unwrap(), bytes(&all));

    let z = Zlib::new(Args::new()).unwrap();
    let text = Arg::from("abc".repeat(50));
    let packed = z.forward(&text).unwrap();
    assert_eq!(z.backward(&packed).unwrap(), text);
    assert_eq!(
        z.forward(bytes(b"x")).unwrap_err().__str__(),
        "Zlib.forward expects a UTF-8 string (str)"
    );
    assert_eq!(
        z.backward(bytes(b"x")).unwrap_err().__str__(),
        "Zlib.backward expects a Base64 string (str)"
    );
    let z9 = Zlib::new(args!(forward_kwargs = kw(vec![("level", Arg::Int(9))]))).unwrap();
    assert_eq!(z.backward(z9.forward(&text).unwrap()).unwrap(), text);

    let j = JsonString::new(Args::new()).unwrap();
    let v = kw(vec![
        (
            "a",
            vec![Arg::Int(1), Arg::Float(2.5), Arg::None].into_arg(),
        ),
        ("u", Arg::from("ü")),
    ]);
    assert_eq!(j.forward(&v).unwrap(), "{\"a\":[1,2.5,null],\"u\":\"ü\"}");
    assert_eq!(j.backward(j.forward(&v).unwrap()).unwrap(), v);
    assert_eq!(
        j.backward(Arg::Int(1)).unwrap_err().__str__(),
        "JsonString.backward expects a JSON string (str)"
    );
    assert_eq!(
        err_text(&j.backward(Arg::from("{")).unwrap_err()),
        (
            ExcKind::TypeError,
            "JsonString.backward: invalid JSON string: Expecting property name enclosed in double quotes".into()
        )
    );
    let e = j.forward(bytes(b"x")).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::TypeError,
            "JsonString.forward: object not JSON serializable: Object of type bytes is not JSON serializable".into()
        )
    );

    // every backward is the native twin of its recovery code
    for (t, input) in [
        (
            Transformation::new(Base64::new(Args::new()).unwrap()),
            bytes(b"payload"),
        ),
        (
            Transformation::new(Zlib::new(Args::new()).unwrap()),
            Arg::from("payload"),
        ),
        (
            Transformation::new(JsonString::new(Args::new()).unwrap()),
            kw(vec![("k", Arg::Int(1))]),
        ),
        (
            Transformation::new(PickleSerializer::new(Args::new()).unwrap()),
            kw(vec![("k", Arg::Tuple(vec![Arg::Int(1)].into()))]),
        ),
        (
            Transformation::new(MsgpackSerializer::new(Args::new()).unwrap()),
            kw(vec![("k", Arg::Int(1))]),
        ),
    ] {
        let out = t.forward(input.clone()).unwrap();
        assert_eq!(t.backward(out.clone()).unwrap(), input, "{}", t.name());
        let native = laila::_codecs::recovery_codes::compile_backward(&t.backward_code()).unwrap();
        assert_eq!(native(&out).unwrap(), input, "{} native", t.name());
    }
}

#[test]
fn transformation_sequence_pipeline() {
    let seq = tf::transformation_base64_compression();
    assert_eq!(seq.__repr__(), "TransformationSequence(base64 -> zlib)");
    assert_eq!(
        TransformationSequence::new(Args::new()).unwrap().__repr__(),
        "TransformationSequence(identity)"
    );
    let (out, codes) = seq.forward(bytes(b"hello world")).unwrap();
    assert!(matches!(out, Arg::Str(_)));
    assert_eq!(
        codes,
        vec![
            laila::_codecs::recovery_codes::emit_zlib(None),
            laila::_codecs::recovery_codes::emit_base64(None)
        ]
    );
    // replay the inverse codes through a simple constitution
    let c = SimpleConstitution::from_codes(codes.into_arg()).unwrap();
    assert_eq!(c.build(out).unwrap(), bytes(b"hello world"));

    // identity pipeline
    let (same, none) = TransformationSequence::new(Args::new())
        .unwrap()
        .forward(Arg::Int(3))
        .unwrap();
    assert_eq!(same, Arg::Int(3));
    assert!(none.is_empty());

    // append: single, list, errors
    let s = TransformationSequence::new(Args::new()).unwrap();
    s.append(Base64::new(Args::new()).unwrap()).unwrap();
    s.append(vec![Zlib::new(Args::new()).unwrap().into_arg()].into_arg())
        .unwrap();
    assert_eq!(s.transformations().len(), 2);
    assert_eq!(
        s.append(vec![Arg::Int(1)].into_arg())
            .unwrap_err()
            .__str__(),
        "All elements must be _data_transformation instances."
    );
    assert_eq!(
        s.append(Arg::Int(1)).unwrap_err().__str__(),
        "append expects a _data_transformation or list of them."
    );
    // pydantic validation of the constructor list
    let e = TransformationSequence::new(args!(transformations = vec![Arg::Int(1)].into_arg()))
        .unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValidationError,
            "1 validation error for TransformationSequence\ntransformations.0\n  Input should be a valid dictionary or instance of _data_transformation [type=model_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/model_type".into()
        )
    );
    let e = TransformationSequence::new(args!(transformations = Arg::Int(1))).unwrap_err();
    assert_eq!(
        e.__str__(),
        "1 validation error for TransformationSequence\ntransformations\n  Input should be a valid list [type=list_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/list_type"
    );
    assert_eq!(
        repr(SimpleConstitution::new(Args::new()).unwrap()),
        "SimpleConstitution()"
    );
    assert_eq!(
        repr(ComplexConstitution::new(Args::new()).unwrap()),
        "ComplexConstitution()"
    );
}

#[test]
fn fernet_encryption_follows_the_process_key() {
    setup();
    // No key configured: the preset fails fast with the Python text.
    set_encryption_key(Arg::None);
    let e = FernetEncryption::new(Args::new()).unwrap_err();
    assert_eq!(e.kind, ExcKind::RuntimeError);
    assert!(e
        .__str__()
        .starts_with("no encryption key configured in this process;"));

    let key1 = Fernet::generate_key();
    let key2 = Fernet::generate_key();
    set_encryption_key(key1.clone());
    let f = FernetEncryption::new(Args::new()).unwrap();
    assert!(!f._explicit_key());
    let fp1 = tf::key_fingerprint(&key1).unwrap();
    assert_eq!(
        f.backward_code(),
        laila::_codecs::recovery_codes::emit_fernet(&fp1, None)
    );
    let token = f.forward(Arg::from("secret text")).unwrap();
    assert_eq!(f.backward(&token).unwrap(), "secret text");
    assert_eq!(
        f.forward(bytes(b"x")).unwrap_err().__str__(),
        "Encryption.forward expects a str (e.g. Base64 text); place Base64() before FernetEncryption() -- see the transformation_encryption preset"
    );
    assert_eq!(
        f.backward(bytes(b"x")).unwrap_err().__str__(),
        "Encryption.backward expects a Fernet token string (str)"
    );
    assert_eq!(
        err_text(&f.backward(Arg::from("not-a-token")).unwrap_err()),
        (
            ExcKind::ValueError,
            "Invalid Fernet token or TTL expired".into()
        )
    );

    // The recovery code is executable natively and checks the fingerprint.
    let native = laila::_codecs::recovery_codes::compile_backward(&f.backward_code()).unwrap();
    assert_eq!(native(&token).unwrap(), "secret text");
    set_encryption_key(key2.clone());
    let e = native(&token).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(
        e.__str__(),
        format!("configured encryption key (laila.encryption_key) does not match the key used at write time (fingerprint {fp1}); set the same key as the writer")
    );
    // ... while the live transformation follows the rotation.
    let token2 = f.forward(Arg::from("after rotation")).unwrap();
    assert_eq!(
        f.backward_code(),
        laila::_codecs::recovery_codes::emit_fernet(&tf::key_fingerprint(&key2).unwrap(), None)
    );
    assert_eq!(f.backward(&token2).unwrap(), "after rotation");

    // An explicit key is pinned and never serialised in the code.
    let pinned = FernetEncryption::new(args!(key = key1.clone())).unwrap();
    assert!(pinned._explicit_key());
    assert!(!pinned.backward_code().contains(key1.__str__().as_str()));
    assert_eq!(pinned.key(), key1);
    let e = FernetEncryption::new(args!(key = Arg::Int(1))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValidationError,
            "2 validation errors for FernetEncryption\nkey.str\n  Input should be a valid string [type=string_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/string_type\nkey.bytes\n  Input should be a valid bytes [type=bytes_type, input_value=1, input_type=int]\n    For further information visit https://errors.pydantic.dev/2.13/v/bytes_type".into()
        )
    );
    let e = FernetEncryption::new(args!(key = Arg::from("short"))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValidationError,
            "1 validation error for FernetEncryption\n  Value error, Fernet key must be 32 url-safe base64-encoded bytes. [type=value_error, input_value={'key': 'short'}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.13/v/value_error".into()
        )
    );
    assert_eq!(
        tf::encryption::encryption::_coerce_key_bytes(&Arg::Int(1))
            .unwrap_err()
            .__str__(),
        "Encryption.key must be bytes or str"
    );

    // The preset pipelines round-trip through a simple constitution.
    set_encryption_key(key1.clone());
    for seq in [
        tf::transformation_encryption(Args::new()).unwrap(),
        tf::transformation_base64_compression_encryption(Args::new()).unwrap(),
    ] {
        let (out, codes) = seq.forward(bytes(b"payload bytes")).unwrap();
        let d = SimpleConstitution::from_codes(codes.into_arg())
            .unwrap()
            .as_dict()
            .unwrap();
        let c = Constitution::from_dict(d).unwrap().unwrap();
        assert_eq!(c.build(out).unwrap(), bytes(b"payload bytes"));
    }
    set_encryption_key(Arg::None);
}

// --------------------------------------------------------------------------
// Trust gate
// --------------------------------------------------------------------------

#[test]
fn trust_gate_templates_and_known_codes() {
    setup();
    let names = tf::REGISTRY();
    for n in [
        "base64",
        "zlib",
        "fernet",
        "json_string",
        "msgpack",
        "numpy",
        "pickle",
        "torch",
    ] {
        assert!(names.iter().any(|x| x == n), "{n} registered");
    }
    // Codes written by other processes with other kwargs still match the template.
    let foreign = laila::_codecs::recovery_codes::emit_zlib(Some(&kw(vec![
        ("wbits", Arg::Int(31)),
        ("bufsize", Arg::Int(1)),
    ])));
    assert!(is_trusted_backward_code(&Arg::from(foreign.as_str())));
    let fernet_foreign = laila::_codecs::recovery_codes::emit_fernet(
        "0123456789ab",
        Some(&kw(vec![("ttl", Arg::Int(5))])),
    );
    assert!(is_trusted_backward_code(&Arg::from(
        fernet_foreign.as_str()
    )));
    // ... but a non-hex fingerprint or a non-literal kwargs slot does not.
    let bad_fp = laila::_codecs::recovery_codes::emit_fernet("0123456789ZZ", None);
    assert!(!is_trusted_backward_code(&Arg::from(bad_fp.as_str())));
    let injected = laila::_codecs::recovery_codes::emit_pickle(None)
        .replace("kwargs = {}", "kwargs = __import__('os').system('x')");
    assert!(!is_trusted_backward_code(&Arg::from(injected.as_str())));
    assert!(!is_trusted_backward_code(&Arg::from(
        "def backward(x):\n    return x\n"
    )));
    assert!(!is_trusted_backward_code(&Arg::from("")));
    assert!(!is_trusted_backward_code(&Arg::Int(1)));
    // Codes assigned in this process are trusted verbatim (third-party fallback).
    let t = Base64::new(Args::new()).unwrap();
    t.set_backward_code("def backward(x):\n    return x[::-1]\n");
    assert!(is_trusted_backward_code(&Arg::from(
        "def backward(x):\n    return x[::-1]\n"
    )));

    // check_simple_codes: template path, hmac path, and the ValueError text.
    trust::check_simple_codes(vec![Arg::from(foreign.as_str())].into_arg(), Arg::None).unwrap();
    let hand = vec![Arg::from("def f(x):\n    return x + 1\n")].into_arg();
    let e = trust::check_simple_codes(hand.clone(), Arg::None).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            "untrusted constitution: recovery code does not match any loaded transformation template and was not generated by this process (['def f(x):'])".into()
        )
    );
    let e = trust::check_simple_codes(vec![Arg::Int(3)].into_arg(), Arg::None).unwrap_err();
    assert!(e.__str__().ends_with("(['3'])"));
    let e = trust::check_simple_codes(vec![Arg::from("   ")].into_arg(), Arg::None).unwrap_err();
    assert!(e.__str__().ends_with("([])"));
    let sig = trust::sign_simple(hand.clone()).unwrap();
    trust::check_simple_codes(hand.clone(), Arg::from(sig.as_str())).unwrap();
    assert!(trust::check_simple_codes(hand.clone(), Arg::from("deadbeef")).is_err());
    assert!(trust::check_simple_codes(hand, Arg::Int(1)).is_err());
    // the signature is a 64-hex HMAC-SHA256 and stable across calls
    assert_eq!(sig.len(), 64);
    assert_eq!(
        sig,
        trust::sign_simple(vec![Arg::from("def f(x):\n    return x + 1\n")].into_arg()).unwrap()
    );
    assert_eq!(trust::constitution_secret().len(), 64);
}

#[test]
fn trust_secret_file_is_created_and_reused() {
    setup();
    let path = trust::_secret_path();
    let secret = trust::constitution_secret();
    assert!(path.exists(), "{path:?}");
    let on_disk = std::fs::read(&path).unwrap();
    assert_eq!(on_disk, secret);
    assert_eq!(secret.len(), 64);
    assert!(secret.iter().all(|b| b.is_ascii_hexdigit()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    // sign_complex is keyed by the file's secret: recompute by hand
    let code = Arg::from("token");
    let gid = Arg::from("LAILA:MANIFEST:x");
    let canonical = r#"{"code":"token","manifest_global_id":"LAILA:MANIFEST:x"}"#;
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&secret).unwrap();
    mac.update(canonical.as_bytes());
    let want: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(trust::sign_complex(code.clone(), gid.clone()), want);
    assert!(trust::verify_complex(
        code.clone(),
        gid.clone(),
        Arg::from(want.as_str())
    ));
    assert!(!trust::verify_complex(code.clone(), gid.clone(), Arg::None));
    assert!(!trust::verify_complex(
        code.clone(),
        gid.clone(),
        Arg::from("")
    ));
    assert!(!trust::verify_complex(
        code,
        Arg::None,
        Arg::from(want.as_str())
    ));
    // None code / gid canonicalise as JSON null
    let _ = trust::sign_complex(Arg::None, Arg::None);
}

// --------------------------------------------------------------------------
// Constitutions
// --------------------------------------------------------------------------

#[test]
fn simple_constitution_api() {
    setup();
    let codes = vec![
        laila::_codecs::recovery_codes::emit_base64(None),
        laila::_codecs::recovery_codes::emit_pickle(None),
    ];
    let c = SimpleConstitution::new(args!(codes = codes.clone().into_arg())).unwrap();
    assert_eq!(c.codes_vec(), codes);
    // defensive copy
    c.codes().append(Arg::from("x")).unwrap();
    assert_eq!(c.codes_vec().len(), 2);
    // empty chain by default
    assert!(SimpleConstitution::new(Args::new())
        .unwrap()
        .codes_vec()
        .is_empty());
    for bad in [
        Arg::from("def f"),
        vec![Arg::Int(1)].into_arg(),
        Arg::Tuple(vec![].into()),
    ] {
        let e = SimpleConstitution::new(args!(codes = bad)).unwrap_err();
        assert_eq!(
            err_text(&e),
            (
                ExcKind::TypeError,
                "codes must be a list of Python source strings".into()
            )
        );
    }
    let payload = Arg::from("gAWVDwAAAAAAAACMC2hlbGxvIGxhaWxhlC4=");
    assert_eq!(c.build(payload).unwrap(), "hello laila");
    let d = c.as_dict().unwrap();
    assert_eq!(d.__getitem__("_kind").unwrap(), "simple");
    assert_eq!(d.__getitem__("codes").unwrap(), codes.clone().into_arg());
    assert_eq!(
        d.__getitem__("_hmac").unwrap(),
        Arg::from(trust::sign_simple(codes.into_arg()).unwrap().as_str())
    );
    assert_eq!(d.__len__().unwrap(), 3);

    // Python source that is not a recognised template cannot run here.
    let src =
        SimpleConstitution::from_codes(vec![Arg::from("def f(x):\n    return x\n")].into_arg())
            .unwrap();
    let e = src.build(Arg::Int(1)).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::NotImplementedError,
            "laila-rs cannot execute Python constitution source; register a builder for this token"
                .into()
        )
    );
    // ... and a dict carrying it is rejected by the trust gate before execution,
    // unless it carries this user's hmac.
    let mut untrusted = kw(vec![("_kind", Arg::from("simple")), ("codes", src.codes())]);
    let e = Constitution::from_dict(untrusted.clone()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert!(e
        .__str__()
        .starts_with("untrusted constitution: recovery code does not match"));
    untrusted = src.as_dict().unwrap();
    assert_eq!(
        Constitution::from_dict(untrusted)
            .unwrap()
            .unwrap()
            .as_simple()
            .unwrap()
            .codes_vec(),
        src.codes_vec()
    );

    // the dynamic surface
    let a = c.clone().into_arg();
    assert!(isinstance::<SimpleConstitution>(&a));
    assert!(isinstance::<Constitution>(&a));
    assert_eq!(a.__getattr__("codes").unwrap(), c.codes());
    assert_eq!(
        a.__call_method__(
            "build",
            args!(Arg::from("gAWVDwAAAAAAAACMC2hlbGxvIGxhaWxhlC4="))
        )
        .unwrap(),
        "hello laila"
    );
}

#[test]
fn constitution_from_dict_dispatch_errors() {
    setup();
    assert!(Constitution::from_dict(Arg::None).unwrap().is_none());
    let e = Constitution::from_dict(kw(vec![("codes", Vec::<Arg>::new().into_arg())])).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::KeyError,
            "\"serialized constitution is missing the '_kind' tag\"".into()
        )
    );
    let e = Constitution::from_dict(kw(vec![("_kind", Arg::from("weird"))])).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            "no Constitution subclass registered for kind 'weird'. Registered: ['complex', 'simple']".into()
        )
    );
    // torch codes are recognised (trusted) but cannot run here
    let torch = vec![laila::_codecs::recovery_codes::emit_torch(None)].into_arg();
    let c = Constitution::from_dict(kw(vec![("_kind", Arg::from("simple")), ("codes", torch)]))
        .unwrap()
        .unwrap();
    let e = c.build(bytes(b"")).unwrap_err();
    assert_eq!(e.kind, ExcKind::NotImplementedError);
}

#[test]
fn complex_constitution_api_and_builder_tokens() {
    setup();
    let c = ComplexConstitution::new(Args::new()).unwrap();
    assert!(c.code().is_none());
    assert!(c.manifest().is_none());
    assert!(c.manifest_global_id().is_none());
    assert_eq!(
        err_text(&c.build(Arg::None).unwrap_err()),
        (
            ExcKind::RuntimeError,
            "no constitution code defined.".into()
        )
    );
    c.set_code("combine").unwrap();
    assert_eq!(c.code(), "combine");
    assert_eq!(
        err_text(&c.set_code("other").unwrap_err()),
        (
            ExcKind::AttributeError,
            "ComplexConstitution code is already set and cannot be reassigned.".into()
        )
    );
    assert_eq!(
        err_text(&ComplexConstitution::new(args!(code = Arg::Int(1))).unwrap_err()),
        (
            ExcKind::TypeError,
            "code must be a Python source string".into()
        )
    );
    assert_eq!(
        err_text(&c.build(Arg::None).unwrap_err()),
        (
            ExcKind::RuntimeError,
            "no manifest available to run constitution.".into()
        )
    );
    assert_eq!(
        err_text(&c.set_manifest(Arg::Int(1)).unwrap_err()),
        (
            ExcKind::TypeError,
            "manifest must be a Manifest instance".into()
        )
    );

    // Only a `Manifest` is accepted (an `Entry` with the `Manifest` class tag).
    let plain = laila::Entry::constant(args!(data = Arg::Int(1))).unwrap();
    assert_eq!(
        err_text(&c.set_manifest(plain).unwrap_err()),
        (
            ExcKind::TypeError,
            "manifest must be a Manifest instance".into()
        )
    );
    let a = laila::Entry::constant(args!(data = Arg::Int(10))).unwrap();
    let b = laila::Entry::constant(args!(data = Arg::Int(32))).unwrap();
    let manifest = Manifest::new(args!(
        data = kw(vec![
            ("a", a.clone().into_arg()),
            ("b", b.clone().into_arg())
        ])
    ))
    .unwrap();
    manifest
        .memorize(Args::new())
        .unwrap()
        .wait(Args::new())
        .unwrap();
    let manifest = manifest.entry().clone();
    let gid = manifest.global_id();
    assert!(gid.starts_with("LAILA:MANIFEST:"));
    c.set_manifest(manifest.clone()).unwrap();
    assert_eq!(c.manifest_global_id(), gid.as_str());
    assert!(c.manifest().is(&Arg::Entry(manifest.clone())));
    assert_eq!(
        err_text(&c.set_manifest(manifest.clone()).unwrap_err()),
        (
            ExcKind::AttributeError,
            "ComplexConstitution manifest is already set and cannot be reassigned.".into()
        )
    );

    // Unregistered token: NotImplementedError.
    let e = c.build(Arg::None).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::NotImplementedError,
            "laila-rs cannot execute Python constitution source; register a builder for this token"
                .into()
        )
    );
    // Registered token: builder(manifest) -> payload, through
    // ``manifest.realized`` (the referenced entries, fetched from memory).
    register_builder_token(
        "combine",
        Arc::new(|m: &Arg| {
            let d = m.__getattr__("realized")?;
            let a = d.__getitem__("a")?.__getattr__("data")?;
            let b = d.__getitem__("b")?.__getattr__("data")?;
            Ok(Arg::Int(a.as_i64().unwrap() + b.as_i64().unwrap()))
        }),
    );
    assert_eq!(c.build(Arg::None).unwrap(), Arg::Int(42));
    // Python source is detected and refused even when it would define one callable.
    let src = ComplexConstitution::new(args!(
        code = "def combine(manifest):\n    d = manifest.realized\n    return d['a'].data + d['b'].data\n",
        manifest = manifest.clone()
    ))
    .unwrap();
    assert_eq!(
        src.build(Arg::None).unwrap_err().kind,
        ExcKind::NotImplementedError
    );

    // as_dict / _from_dict with the HMAC.
    let d = c.as_dict().unwrap();
    assert_eq!(d.__getitem__("_kind").unwrap(), "complex");
    assert_eq!(d.__getitem__("code").unwrap(), "combine");
    assert_eq!(d.__getitem__("manifest_global_id").unwrap(), gid.as_str());
    assert_eq!(
        d.__getitem__("_hmac").unwrap(),
        Arg::from(trust::sign_complex("combine", gid.as_str()).as_str())
    );
    let back = Constitution::from_dict(d.clone()).unwrap().unwrap();
    let back = back.as_complex().unwrap();
    assert_eq!(back.code(), "combine");
    assert_eq!(back.manifest_global_id(), gid.as_str());
    assert!(back.manifest().is_none());
    // the lazily-bound manifest is resolved through laila.remember at build time
    laila
        .memorize(args!(manifest.clone()))
        .unwrap()
        .wait(Args::new())
        .unwrap();
    assert_eq!(back.build(Arg::None).unwrap(), Arg::Int(42));
    assert!(!back.manifest().is_none());

    // tampering / missing signature
    d.__setitem__("code", "evil").unwrap();
    let e = Constitution::from_dict(d.clone()).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::ValueError,
            "untrusted constitution: complex constitution signature missing or invalid (written by a process without this user's ~/.laila/constitution.key)".into()
        )
    );
    d.__delitem__("_hmac").unwrap();
    assert!(Constitution::from_dict(d).is_err());

    // unresolvable gid
    let lazy = ComplexConstitution::new(args!(
        code = "combine",
        manifest_global_id = "LAILA:MANIFEST:00000000-0000-4000-8000-000000000000"
    ))
    .unwrap();
    let e = lazy.build(Arg::None).unwrap_err();
    assert!(
        e.kind == ExcKind::KeyError || e.kind == ExcKind::RuntimeError,
        "{e}"
    );
    // candidate pools: alpha (None) first when no hint
    let pools = ComplexConstitution::_manifest_candidate_pools(None);
    assert_eq!(pools.first(), Some(&None));

    // the dynamic surface
    let a = c.clone().into_arg();
    assert!(isinstance::<ComplexConstitution>(&a));
    assert_eq!(a.__getattr__("code").unwrap(), "combine");
    assert_eq!(a.__getattr__("manifest_global_id").unwrap(), gid.as_str());
    let fresh = ComplexConstitution::new(Args::new()).unwrap().into_arg();
    fresh.__setattr__("code", "combine").unwrap();
    assert_eq!(fresh.__getattr__("code").unwrap(), "combine");
    let _ = complex_constitution::register_manifest_check;
}

// --------------------------------------------------------------------------
// build_maps
// --------------------------------------------------------------------------

#[test]
fn build_by_scope_dispatch() {
    setup();
    let d = kw(vec![
        ("_scopes", vec![Arg::from("WPD_TEST")].into_arg()),
        ("payload", Arg::Int(1)),
    ]);
    let e = build_maps::build_by_scope(args!(d.clone())).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert!(e
        .__str__()
        .starts_with("No builder registered for scope 'WPD_TEST'. Registered scopes: ["));

    let sync_fn: build_maps::SyncBuilder = Arc::new(|in_dict: Arg, kwargs: Args| {
        Ok(kw(vec![
            ("sync", in_dict.__getitem__("payload")?),
            ("extra", kwargs.get("extra")),
        ]))
    });
    let async_fn: build_maps::AsyncBuilder = Arc::new(|in_dict: Arg, _kwargs: Args| {
        let payload = in_dict.__getitem__("payload")?;
        Ok(laila::python::stdlib::asyncio::coroutine(async move {
            Ok(kw(vec![("async", payload)]))
        }))
    });
    build_maps::register_builder("WPD_TEST", sync_fn, async_fn);
    assert!(build_maps::builder_scopes().contains(&"WPD_TEST".to_string()));
    let out = build_maps::build_by_scope(args!(d.clone(), extra = Arg::Int(9))).unwrap();
    assert_eq!(out, kw(vec![("sync", Arg::Int(1)), ("extra", Arg::Int(9))]));
    // The async builder hands back a coroutine (nothing is registered in
    // the future bank); driving it yields the builder's value.
    let coro = build_maps::build_by_scope(args!(d.clone(), asynchronous = true)).unwrap();
    assert_eq!(
        laila::python::stdlib::asyncio::run(coro).unwrap(),
        kw(vec![("async", Arg::Int(1))])
    );
    // JSON text input
    let text = laila::_compat::pyjson::dumps(&d, &Default::default()).unwrap();
    assert_eq!(
        build_maps::build_by_scope(args!(Arg::from(text.as_str())))
            .unwrap()
            .__getitem__("sync")
            .unwrap(),
        Arg::Int(1)
    );
    let e = build_maps::build_by_scope(args!(Arg::from("{"))).unwrap_err();
    assert_eq!(e.kind, ExcKind::JSONDecodeError);
    let e = build_maps::build_by_scope(args!(Arg::from("[1]"))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::AttributeError,
            "'list' object has no attribute 'get'".into()
        )
    );
    // live entry short-circuits
    let entry = laila::Entry::constant(args!(data = Arg::Int(5))).unwrap();
    let same = build_maps::build_by_scope(args!(entry.clone())).unwrap();
    assert!(same.is(&Arg::Entry(entry.clone())));
    // the async path wraps the live entry in an already-resolved coroutine
    let coro = build_maps::build_by_scope(args!(entry.clone(), asynchronous = true)).unwrap();
    assert!(laila::python::stdlib::asyncio::run(coro)
        .unwrap()
        .is(&Arg::Entry(entry)));
    // invalid input
    let e = build_maps::build_by_scope(args!(Arg::Int(1))).unwrap_err();
    assert_eq!(
        err_text(&e),
        (
            ExcKind::RuntimeError,
            "Invalid input for entry build.".into()
        )
    );
    // default scope is ENTRY (registered by the Entry module): the dict
    // reaches ``Entry.from_dict``, which wants ``_uuid``.
    let e = build_maps::build_by_scope(args!(kw(vec![("payload", Arg::Int(1))]))).unwrap_err();
    assert_eq!(err_text(&e), (ExcKind::KeyError, "'_uuid'".into()));
    let e = build_maps::build_by_scope(args!(kw(vec![("_scopes", Vec::<Arg>::new().into_arg())])))
        .unwrap_err();
    assert_eq!(err_text(&e), (ExcKind::KeyError, "'_uuid'".into()));
    assert!(build_maps::builder_scopes().contains(&"ENTRY".to_string()));
    assert!(build_maps::builder_scopes().contains(&"POOL_INDEX".to_string()));
    assert!(build_maps::builder_scopes().contains(&"MANIFEST".to_string()));
    // a serialized entry round-trips through the ENTRY builder
    let entry = laila::Entry::constant(args!(data = kw(vec![("k", Arg::Int(2))]))).unwrap();
    let blob = entry
        .serialize(args!(transformations = tf::transformation_base64()))
        .unwrap();
    let back = build_maps::build_by_scope(args!(blob)).unwrap();
    assert_eq!(back.__getattr__("data").unwrap(), entry.data().unwrap());
    assert_eq!(
        back.__getattr__("global_id").unwrap(),
        entry.global_id().as_str()
    );
}
