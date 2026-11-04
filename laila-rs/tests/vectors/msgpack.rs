//! `msgpack` describe-block of `laila-js/tests/vectors/codecs.test.js`,
//! one for one, against `codecs.json` (msgpack-python 1.1.2 output).

mod fixture;

use fixture::{case, cases, decode, first_diff, fixture};
use laila::_codecs::msgpack::{self, default_hook, ExtType, PackOptions, UnpackOptions, Unpacker};
use laila::prelude::*;
use laila::python::errors::ExcKind;

/// The JS `norm`: tuples become lists (msgpack has no tuple type).
fn norm(x: &Arg) -> Arg {
    match x {
        Arg::Tuple(_) | Arg::List(_) => x
            .__iter__()
            .unwrap()
            .iter()
            .map(norm)
            .collect::<Vec<_>>()
            .into_arg(),
        Arg::Dict(d) => {
            let out = dict(Args::new()).unwrap();
            for (k, v) in d.read().iter() {
                out.__setitem__(norm(k), norm(v)).unwrap();
            }
            out
        }
        other => other.clone(),
    }
}

fn unpack_opts() -> UnpackOptions {
    UnpackOptions {
        raw: false,
        strict_map_key: false,
        use_list: true,
    }
}

#[test]
fn every_msgpack_case_is_byte_identical_and_round_trips() {
    let fx = fixture();
    let all = cases();
    let mut checked = 0;
    for (name, r) in fx["cases"].as_object().unwrap() {
        if r.get("msgpack_error").is_some() {
            continue;
        }
        let Some(payload) = r.get("msgpack").and_then(|v| v.as_str()) else {
            continue;
        };
        let (_, obj) = all
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("missing Rust construction for {name}"));
        let want = decode(payload);
        let got =
            msgpack::packb(obj, &PackOptions::default()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(got == want, "{name}: {}", first_diff(&got, &want));
        let back =
            msgpack::unpackb(&want, &unpack_opts()).unwrap_or_else(|e| panic!("{name}: {e}"));
        if name == "float_nan" {
            assert!(
                matches!(back, Arg::Float(f) if f.is_nan()),
                "{name}: {back:?}"
            );
        } else {
            let expect = norm(obj);
            assert!(
                back == expect,
                "{name}: roundtrip {}",
                &back.__repr__()[..back.__repr__().len().min(120)]
            );
        }
        checked += 1;
    }
    assert_eq!(
        checked, 78,
        "every msgpack payload in the fixture was checked"
    );
}

#[test]
fn every_msgpack_error_case_raises_that_class() {
    let fx = fixture();
    let mut checked = 0;
    for (name, r) in fx["cases"].as_object().unwrap() {
        let Some(class) = r.get("msgpack_error").and_then(|v| v.as_str()) else {
            continue;
        };
        let err = msgpack::packb(&case(name), &PackOptions::default()).expect_err(name);
        assert_eq!(err.kind.name(), class, "{name}: {err}");
        checked += 1;
    }
    assert_eq!(checked, 10);
    // the message texts msgpack-python produces
    let e = msgpack::packb(&case("set_123"), &PackOptions::default()).unwrap_err();
    assert_eq!(e.message, "can not serialize 'set' object");
    let e = msgpack::packb(&case("int_18446744073709551616"), &PackOptions::default()).unwrap_err();
    assert_eq!(e.message, "Integer value out of range");
}

#[test]
fn ext_type() {
    let v: Arg = vec![
        ExtType::new(5, vec![1u8, 2, 3]).into_arg(),
        ExtType::new(-1, vec![7u8; 16]).into_arg(),
    ]
    .into_arg();
    let p = msgpack::packb(&v, &PackOptions::default()).unwrap();
    // fixext 1? no: 3 bytes -> ext8; 16 bytes -> fixext16
    assert_eq!(
        &p[..],
        &[0x92, 0xc7, 3, 5, 1, 2, 3, 0xd8, 0xff, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7][..]
    );
    let back = msgpack::unpackb(&p, &UnpackOptions::default()).unwrap();
    let items = back.__iter__().unwrap();
    let e0 = items[0].downcast_ref::<ExtType>().expect("ExtType");
    assert_eq!(e0.code, 5);
    assert_eq!(e0.data, vec![1, 2, 3]);
    let e1 = items[1].downcast_ref::<ExtType>().expect("ExtType");
    assert_eq!(e1.code, -1);
    assert_eq!(e1.data.len(), 16);
    assert!(back == v);
    assert_eq!(
        items[0].__repr__(),
        "ExtType(code=5, data=b'\\x01\\x02\\x03')"
    );
    assert_eq!(items[0].type_name(), "ExtType");
    // every fixext / ext size
    for n in [0usize, 1, 2, 4, 8, 16, 17, 255, 256, 65535, 65536] {
        let e = ExtType::new(42, vec![0xabu8; n]).into_arg();
        let p = msgpack::packb(&e, &PackOptions::default()).unwrap();
        let head: &[u8] = match n {
            1 => &[0xd4],
            2 => &[0xd5],
            4 => &[0xd6],
            8 => &[0xd7],
            16 => &[0xd8],
            n if n <= 0xff => &[0xc7, n as u8],
            n if n <= 0xffff => &[0xc8, (n >> 8) as u8, n as u8],
            n => &[
                0xc9,
                (n >> 24) as u8,
                (n >> 16) as u8,
                (n >> 8) as u8,
                n as u8,
            ],
        };
        assert_eq!(&p[..head.len()], head, "ext header for n={n}");
        assert_eq!(p[head.len()], 42);
        assert_eq!(p.len(), head.len() + 1 + n);
        assert!(msgpack::unpackb(&p, &UnpackOptions::default()).unwrap() == e);
    }
}

#[test]
fn default_hook_extra_data_strict_map_key_streaming() {
    // default hook: set -> list
    let opts = PackOptions {
        default: Some(default_hook(|o| Ok(o.__iter__()?.into_arg()))),
        ..PackOptions::default()
    };
    let got = msgpack::packb(&set(args!(vec![1i64, 2].into_arg())).unwrap(), &opts).unwrap();
    assert_eq!(
        got,
        msgpack::packb(&vec![1i64, 2].into_arg(), &PackOptions::default()).unwrap()
    );
    // the hook is consulted once: an unsupported result is a TypeError
    let opts2 = PackOptions {
        default: Some(default_hook(Ok)),
        ..PackOptions::default()
    };
    let e = msgpack::packb(&set(args!(vec![1i64].into_arg())).unwrap(), &opts2).unwrap_err();
    assert_eq!(e.kind, ExcKind::TypeError);
    assert_eq!(e.message, "can not serialize 'set' object");
    // ... but applies again to children of the converted value
    let opts3 = PackOptions {
        default: Some(default_hook(|o| match o {
            Arg::Set(_) | Arg::FrozenSet(_) => Ok(o.__iter__()?.into_arg()),
            _ => Ok(Arg::Str("obj".into())),
        })),
        ..PackOptions::default()
    };
    let nested = set(args!(
        vec![frozenset(args!(vec![1i64].into_arg())).unwrap()].into_arg()
    ))
    .unwrap();
    assert_eq!(
        msgpack::packb(&nested, &opts3).unwrap(),
        vec![0x91, 0x91, 0x01]
    );

    // ExtraData
    let e = msgpack::unpackb(&[0x01, 0x02], &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ExtraData);
    assert_eq!(e.message, "unpack(b) received extra data.");

    // strict_map_key
    let d = dict(Args::new()).unwrap();
    d.__setitem__(1i64, 2i64).unwrap();
    let packed = msgpack::packb(&d, &PackOptions::default()).unwrap();
    assert_eq!(packed, vec![0x81, 0x01, 0x02]);
    let e = msgpack::unpackb(
        &packed,
        &UnpackOptions {
            strict_map_key: true,
            ..unpack_opts()
        },
    )
    .unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert!(e.message.contains("not allowed for map key"), "{e}");
    assert_eq!(
        e.message,
        "int is not allowed for map key when strict_map_key=True"
    );
    assert!(msgpack::unpackb(&packed, &unpack_opts()).unwrap() == d);
    // str and bytes keys are always allowed
    let sd = dict(Args::new()).unwrap();
    sd.__setitem__("k", 1i64).unwrap();
    sd.__setitem__(laila::python::object::bytes(b"b"), 2i64)
        .unwrap();
    let p = msgpack::packb(&sd, &PackOptions::default()).unwrap();
    assert!(msgpack::unpackb(&p, &UnpackOptions::default()).unwrap() == sd);

    // streaming Unpacker
    let mut u = Unpacker::new(UnpackOptions::default());
    u.feed(&msgpack::packb(&Arg::from("a"), &PackOptions::default()).unwrap());
    let mut chunk = msgpack::packb(&vec![1i64, 2].into_arg(), &PackOptions::default()).unwrap();
    chunk.extend_from_slice(&[0x92, 0x01]);
    u.feed(&chunk);
    let got: Vec<Arg> = u.by_ref().map(|r| r.unwrap()).collect();
    assert!(got == vec![Arg::from("a"), vec![1i64, 2].into_arg()]);
    assert_eq!(u.pending(), &[0x92, 0x01]);
    // completing the partial tail yields it
    u.feed(&[0x02]);
    let rest: Vec<Arg> = u.by_ref().map(|r| r.unwrap()).collect();
    assert!(rest == vec![vec![1i64, 2].into_arg()]);
    assert!(u.pending().is_empty());
    assert!(u.next().is_none());
}

#[test]
fn unpack_errors_and_options() {
    // truncated input
    let e = msgpack::unpackb(&[0x92, 0x01], &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(e.message, "Unpack failed: incomplete input");
    let e = msgpack::unpackb(&[], &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.message, "Unpack failed: incomplete input");
    // the never-used 0xc1 byte -> FormatError() (a ValueError, no message)
    let e = msgpack::unpackb(&[0xc1], &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::UnpackValueError);
    assert!(e.kind.is_subclass_of(ExcKind::ValueError));
    assert_eq!(e.message, "");
    // use_list=False -> tuples
    let back = msgpack::unpackb(
        &[0x92, 0x01, 0x92, 0x02, 0x03],
        &UnpackOptions {
            use_list: false,
            ..unpack_opts()
        },
    )
    .unwrap();
    assert!(matches!(back, Arg::Tuple(_)));
    assert!(
        back == Arg::Tuple(
            vec![
                Arg::Int(1),
                Arg::Tuple(vec![Arg::Int(2), Arg::Int(3)].into())
            ]
            .into()
        )
    );
    // raw=True -> bytes for str payloads
    let back = msgpack::unpackb(
        &[0xa2, b'h', b'i'],
        &UnpackOptions {
            raw: true,
            ..unpack_opts()
        },
    )
    .unwrap();
    assert!(back == laila::python::object::bytes(b"hi"));
    // invalid UTF-8 in a str payload
    let e = msgpack::unpackb(&[0xa1, 0xff], &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::UnicodeDecodeError);
    assert_eq!(
        e.message,
        "'utf-8' codec can't decode byte 0xff in position 0: invalid start byte"
    );
    // unhashable map key
    let e = msgpack::unpackb(&[0x81, 0x90, 0x01], &unpack_opts()).unwrap_err();
    assert_eq!(e.kind, ExcKind::TypeError);
    assert_eq!(e.message, "unhashable type: 'list'");
    // uint64 above i64 comes back as a Python int
    let back = msgpack::unpackb(
        &[0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        &UnpackOptions::default(),
    )
    .unwrap();
    assert!(back == case("int_18446744073709551615"));
    // float32
    let back =
        msgpack::unpackb(&[0xca, 0x3f, 0xc0, 0x00, 0x00], &UnpackOptions::default()).unwrap();
    assert!(back == Arg::Float(1.5));
    // use_single_float
    let got = msgpack::packb(
        &Arg::Float(1.5),
        &PackOptions {
            use_single_float: true,
            ..PackOptions::default()
        },
    )
    .unwrap();
    assert_eq!(got, vec![0xca, 0x3f, 0xc0, 0x00, 0x00]);
    // use_bin_type=False: bytes use the raw family, str8 is not available
    let legacy = PackOptions {
        use_bin_type: false,
        ..PackOptions::default()
    };
    assert_eq!(
        msgpack::packb(&laila::python::object::bytes(b"ab"), &legacy).unwrap(),
        vec![0xa2, b'a', b'b']
    );
    let s = Arg::from("z".repeat(40).as_str());
    assert_eq!(
        &msgpack::packb(&s, &legacy).unwrap()[..3],
        &[0xda, 0x00, 40]
    );
    // bytearray packs like bytes
    assert_eq!(
        msgpack::packb(&case("bytearray"), &PackOptions::default()).unwrap(),
        msgpack::packb(
            &laila::python::object::bytes(b"\x01\x02\x03"),
            &PackOptions::default()
        )
        .unwrap()
    );
    // recursion limit
    let mut deep = Vec::<Arg>::new().into_arg();
    for _ in 0..600 {
        deep = vec![deep].into_arg();
    }
    let e = msgpack::packb(&deep, &PackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(e.message, "recursion limit exceeded.");
    let mut bytes_ = vec![0x91u8; 600];
    bytes_.push(0xc0);
    let e = msgpack::unpackb(&bytes_, &UnpackOptions::default()).unwrap_err();
    assert_eq!(e.kind, ExcKind::UnpackValueError);
}
