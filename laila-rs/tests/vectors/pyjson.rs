//! `json.dumps` / `json.loads` against CPython 3.14 output (expected strings
//! and error texts produced with `python3` on this machine).

mod fixture;

use laila::_compat::pyjson::{dumps, loads, loads_bytes, DumpsOptions};
use laila::prelude::*;
use laila::python::errors::ExcKind;
use laila::python::object::bytes;
use num_bigint::BigInt;
use sha2::{Digest, Sha256};

enum Expect {
    Lit(&'static str),
    /// `(len, sha256 hex)` of CPython's output for the large cases.
    Sha(usize, &'static str),
}

impl Expect {
    fn check(&self, name: &str, got: &str) {
        match self {
            Expect::Lit(s) => assert_eq!(got, *s, "{name}"),
            Expect::Sha(len, hex) => {
                assert_eq!(got.len(), *len, "{name}: length");
                let h = Sha256::digest(got.as_bytes());
                let hs: String = h.iter().map(|b| format!("{b:02x}")).collect();
                assert_eq!(hs, *hex, "{name}: sha256");
            }
        }
    }
}

/// `(case, json.dumps(obj), json.dumps(obj, indent=2, sort_keys=True))`
const EXPECTED: &[(&str, Expect, Expect)] = &[
    ("none", Expect::Lit("null"), Expect::Lit("null")),
    ("true", Expect::Lit("true"), Expect::Lit("true")),
    ("false", Expect::Lit("false"), Expect::Lit("false")),
    ("int_0", Expect::Lit("0"), Expect::Lit("0")),
    ("int_1", Expect::Lit("1"), Expect::Lit("1")),
    ("int_255", Expect::Lit("255"), Expect::Lit("255")),
    ("int_256", Expect::Lit("256"), Expect::Lit("256")),
    ("int_65535", Expect::Lit("65535"), Expect::Lit("65535")),
    ("int_65536", Expect::Lit("65536"), Expect::Lit("65536")),
    ("int_2147483647", Expect::Lit("2147483647"), Expect::Lit("2147483647")),
    ("int_-1", Expect::Lit("-1"), Expect::Lit("-1")),
    ("int_-128", Expect::Lit("-128"), Expect::Lit("-128")),
    ("int_-129", Expect::Lit("-129"), Expect::Lit("-129")),
    ("int_-32768", Expect::Lit("-32768"), Expect::Lit("-32768")),
    ("int_-32769", Expect::Lit("-32769"), Expect::Lit("-32769")),
    ("int_-2147483648", Expect::Lit("-2147483648"), Expect::Lit("-2147483648")),
    ("int_-2147483649", Expect::Lit("-2147483649"), Expect::Lit("-2147483649")),
    ("int_2147483648", Expect::Lit("2147483648"), Expect::Lit("2147483648")),
    ("int_4294967295", Expect::Lit("4294967295"), Expect::Lit("4294967295")),
    ("int_4294967296", Expect::Lit("4294967296"), Expect::Lit("4294967296")),
    ("int_9007199254740992", Expect::Lit("9007199254740992"), Expect::Lit("9007199254740992")),
    ("int_9223372036854775807", Expect::Lit("9223372036854775807"), Expect::Lit("9223372036854775807")),
    ("int_-9223372036854775808", Expect::Lit("-9223372036854775808"), Expect::Lit("-9223372036854775808")),
    ("int_18446744073709551615", Expect::Lit("18446744073709551615"), Expect::Lit("18446744073709551615")),
    ("int_18446744073709551616", Expect::Lit("18446744073709551616"), Expect::Lit("18446744073709551616")),
    ("int_1000000000000000000000000000000", Expect::Lit("1000000000000000000000000000000"), Expect::Lit("1000000000000000000000000000000")),
    ("int_-1000000000000000000000000000000", Expect::Lit("-1000000000000000000000000000000"), Expect::Lit("-1000000000000000000000000000000")),
    ("float_3.5", Expect::Lit("3.5"), Expect::Lit("3.5")),
    ("float_neg", Expect::Lit("-2.5e-10"), Expect::Lit("-2.5e-10")),
    ("float_1.0", Expect::Lit("1.0"), Expect::Lit("1.0")),
    ("float_0.0", Expect::Lit("0.0"), Expect::Lit("0.0")),
    ("float_inf", Expect::Lit("Infinity"), Expect::Lit("Infinity")),
    ("float_nan", Expect::Lit("NaN"), Expect::Lit("NaN")),
    ("str_empty", Expect::Lit("\"\""), Expect::Lit("\"\"")),
    ("str_hello", Expect::Lit("\"hello laila\""), Expect::Lit("\"hello laila\"")),
    ("str_unicode", Expect::Lit("\"\\u00fcn\\u00efc\\u00f6d\\u00e9 \\ud83d\\ude00\""), Expect::Lit("\"\\u00fcn\\u00efc\\u00f6d\\u00e9 \\ud83d\\ude00\"")),
    ("str_255", Expect::Sha(257, "c739732b49d48f702fcb24e9805983321a67950c4e7497ebc88a832290269841"), Expect::Sha(257, "c739732b49d48f702fcb24e9805983321a67950c4e7497ebc88a832290269841")),
    ("str_256", Expect::Sha(258, "a3b004a8a9824a2c149bafc3df31af0547b4feb1077873d7839f9f713d9578de"), Expect::Sha(258, "a3b004a8a9824a2c149bafc3df31af0547b4feb1077873d7839f9f713d9578de")),
    ("str_300", Expect::Sha(302, "d24923841d98dcd5a081ba3b99596483edeeca895b41ded64c8aa77a3c438fa7"), Expect::Sha(302, "d24923841d98dcd5a081ba3b99596483edeeca895b41ded64c8aa77a3c438fa7")),
    ("str_70000", Expect::Sha(70002, "dca7a414549bd2b6a01921ee245a54331f331dde286db4d65f9a6a439bf2428c"), Expect::Sha(70002, "dca7a414549bd2b6a01921ee245a54331f331dde286db4d65f9a6a439bf2428c")),
    ("list_empty", Expect::Lit("[]"), Expect::Lit("[]")),
    ("list_one", Expect::Lit("[7]"), Expect::Lit("[\n  7\n]")),
    ("list_1234", Expect::Lit("[1, 2, 3, 4]"), Expect::Lit("[\n  1,\n  2,\n  3,\n  4\n]")),
    ("list_1000", Expect::Sha(4890, "3e726f1b6f58ece8e52f367572eb99447da3892ff52903b8790cc8472451385c"), Expect::Sha(6892, "41470c0969b0dcc7a9079c0a086b7797a774ad67b6e3850b931294cee01abd55")),
    ("list_1001", Expect::Sha(4896, "d65bd42ec2c845c4a774ada0a46df0e1a830502f8427f615c0e89a25b87c7d78"), Expect::Sha(6900, "670f465b1c62051903c37afd1282a109c91d40fd714a36b57ecadb7d81865bf4")),
    ("list_1500", Expect::Sha(7890, "db4d61b3884f4ea385554361100afffdadc8410c25cde105808924ea5f3f0a05"), Expect::Sha(10892, "d766450c83e193e2204b01d946c80fbddf8fe398e090e9cfa4b411b3fbaff7da")),
    ("list_2000", Expect::Sha(10890, "edd6c0024ff499a09e180e0cf8d1acdee66f285b5544a28346b6e74c5ffa0710"), Expect::Sha(14892, "2c7e45a086645b960f325a62272698dcce7782a60aa686a02f5974a5f917abdc")),
    ("list_2001", Expect::Sha(10896, "3252a1edb90fdfe38143adb2e20661678f8ff661e1dc0aad66a36d9488741a13"), Expect::Sha(14900, "c62f3cc627ecb52fd8822344f6a642c940edf71a64c1e0878c15abb56545a423")),
    ("list_nested", Expect::Lit("[[1, [2, [3]]], {\"k\": [null, true]}]"), Expect::Lit("[\n  [\n    1,\n    [\n      2,\n      [\n        3\n      ]\n    ]\n  ],\n  {\n    \"k\": [\n      null,\n      true\n    ]\n  }\n]")),
    ("list_floats", Expect::Lit("[1.5, 2.0, -0.0, 1e+300]"), Expect::Lit("[\n  1.5,\n  2.0,\n  -0.0,\n  1e+300\n]")),
    ("list_same_str", Expect::Lit("[\"shared\", \"shared\"]"), Expect::Lit("[\n  \"shared\",\n  \"shared\"\n]")),
    ("list_shared_list", Expect::Lit("[[1, 2], [1, 2]]"), Expect::Lit("[\n  [\n    1,\n    2\n  ],\n  [\n    1,\n    2\n  ]\n]")),
    ("bool_in_list", Expect::Lit("[true, false, null, 0, 1]"), Expect::Lit("[\n  true,\n  false,\n  null,\n  0,\n  1\n]")),
    ("neg_fixint", Expect::Lit("[-1, -32, -33, -127, -128]"), Expect::Lit("[\n  -1,\n  -32,\n  -33,\n  -127,\n  -128\n]")),
    ("uint_edges", Expect::Lit("[127, 128, 255, 256, 65535, 65536, 4294967295, 4294967296]"), Expect::Lit("[\n  127,\n  128,\n  255,\n  256,\n  65535,\n  65536,\n  4294967295,\n  4294967296\n]")),
    ("tuple_empty", Expect::Lit("[]"), Expect::Lit("[]")),
    ("tuple_1", Expect::Lit("[1]"), Expect::Lit("[\n  1\n]")),
    ("tuple_2", Expect::Lit("[1, 2]"), Expect::Lit("[\n  1,\n  2\n]")),
    ("tuple_3", Expect::Lit("[1, 2, 3]"), Expect::Lit("[\n  1,\n  2,\n  3\n]")),
    ("tuple_4", Expect::Lit("[1, 2, 3, 4]"), Expect::Lit("[\n  1,\n  2,\n  3,\n  4\n]")),
    ("tuple_nested", Expect::Lit("[[1, 2], [3, [4, 5]]]"), Expect::Lit("[\n  [\n    1,\n    2\n  ],\n  [\n    3,\n    [\n      4,\n      5\n    ]\n  ]\n]")),
    ("dict_empty", Expect::Lit("{}"), Expect::Lit("{}")),
    ("dict_one", Expect::Lit("{\"k\": \"v\"}"), Expect::Lit("{\n  \"k\": \"v\"\n}")),
    ("dict_ordered", Expect::Lit("{\"b\": 1, \"a\": 2, \"z\": 3}"), Expect::Lit("{\n  \"a\": 2,\n  \"b\": 1,\n  \"z\": 3\n}")),
    ("dict_intkeys", Expect::Lit("{\"1\": \"x\", \"2\": \"y\"}"), Expect::Lit("{\n  \"1\": \"x\",\n  \"2\": \"y\"\n}")),
    ("dict_1000", Expect::Sha(12780, "dca5cbf48cb92cb049b64766e13a9bbc4547d2ccb8394fce76aa67cfc31f8c69"), Expect::Sha(14782, "37715253755a1e662ea4e790cc8de351d999c99f3d22947218f88db7124f1be6")),
    ("dict_1001", Expect::Sha(12795, "838ececc2ab159f05b38232bad6296db80a93037e53d0ff0d3239eb7f7c1c7bd"), Expect::Sha(14799, "a479301e18529e851103fa275d7a903ce71a5eb9df6445c908c5c3a94dcd4ab5")),
    ("dict_1500", Expect::Sha(20280, "63fee7908bf20e0d2f9f1dd1ba998f0aec01bcef6d2fb2025fb888a0e75ddcd8"), Expect::Sha(23282, "e4114d3056cdecf12fdf3bcaea99fd4c5f9cb1beae83fd9aee94cb3eb86db296")),
    ("dict_2000", Expect::Sha(27780, "64837d1ebe9e89b546ed44109e177e1ef508078d66d6442a94d3513e9b6a4d22"), Expect::Sha(31782, "1ee7b2dd5467ac790725736f8c4112b0f51d5231dd15ca024224022ed209b3d5")),
    ("dict_nested", Expect::Lit("{\"a\": {\"b\": [1, 2, {\"c\": 3}]}}"), Expect::Lit("{\n  \"a\": {\n    \"b\": [\n      1,\n      2,\n      {\n        \"c\": 3\n      }\n    ]\n  }\n}")),
    ("dict_mixed", Expect::Lit("{\"i\": 1, \"f\": 2.5, \"s\": \"x\", \"b\": true, \"n\": null, \"l\": [1, \"two\"]}"), Expect::Lit("{\n  \"b\": true,\n  \"f\": 2.5,\n  \"i\": 1,\n  \"l\": [\n    1,\n    \"two\"\n  ],\n  \"n\": null,\n  \"s\": \"x\"\n}")),
    ("dict_same_str", Expect::Lit("{\"a\": \"a\"}"), Expect::Lit("{\n  \"a\": \"a\"\n}")),
    ("deep", Expect::Lit("{\"a\": [{\"b\": [{\"c\": [1, [2, 3]]}]}]}"), Expect::Lit("{\n  \"a\": [\n    {\n      \"b\": [\n        {\n          \"c\": [\n            1,\n            [\n              2,\n              3\n            ]\n          ]\n        }\n      ]\n    }\n  ]\n}")),
];

/// Fixture cases CPython's `json.dumps` rejects (`bytes`, `set`, tuple keys).
const NOT_SERIALIZABLE: &[(&str, &str)] = &[
    (
        "bytes_empty",
        "Object of type bytes is not JSON serializable",
    ),
    (
        "bytes_small",
        "Object of type bytes is not JSON serializable",
    ),
    ("bytes_255", "Object of type bytes is not JSON serializable"),
    ("bytes_256", "Object of type bytes is not JSON serializable"),
    ("bytes_300", "Object of type bytes is not JSON serializable"),
    (
        "bytes_65535",
        "Object of type bytes is not JSON serializable",
    ),
    (
        "bytes_65536",
        "Object of type bytes is not JSON serializable",
    ),
    (
        "bytes_70000",
        "Object of type bytes is not JSON serializable",
    ),
    (
        "bytearray",
        "Object of type bytearray is not JSON serializable",
    ),
    (
        "dict_bytes",
        "Object of type bytes is not JSON serializable",
    ),
    (
        "dict_tuplekey",
        "keys must be str, int, float, bool or None, not tuple",
    ),
    ("set_empty", "Object of type set is not JSON serializable"),
    ("set_123", "Object of type set is not JSON serializable"),
    ("set_1000", "Object of type set is not JSON serializable"),
    ("set_1001", "Object of type set is not JSON serializable"),
    ("set_1500", "Object of type set is not JSON serializable"),
    (
        "frozenset_empty",
        "Object of type frozenset is not JSON serializable",
    ),
    (
        "frozenset_123",
        "Object of type frozenset is not JSON serializable",
    ),
];

fn opts() -> DumpsOptions {
    DumpsOptions::default()
}
fn indent2_sorted() -> DumpsOptions {
    DumpsOptions {
        indent: Some(Arg::Int(2)),
        sort_keys: true,
        ..DumpsOptions::default()
    }
}
fn d(obj: &Arg) -> String {
    dumps(obj, &opts()).unwrap()
}
fn err_of<T>(r: laila::Result<T>) -> (ExcKind, String) {
    let e = r.err().expect("should fail");
    (e.kind, e.message)
}

#[test]
fn fixture_cases_match_cpython_dumps() {
    let cases = fixture::cases();
    let mut seen = 0;
    for (name, obj) in &cases {
        if let Some((_, plain, pretty)) = EXPECTED.iter().find(|(n, _, _)| n == name) {
            seen += 1;
            let got = d(obj);
            plain.check(name, &got);
            pretty.check(name, &dumps(obj, &indent2_sorted()).unwrap());
            // loads(dumps(x)) reproduces the text exactly (tuples become lists,
            // int keys become str keys, NaN stays NaN -- all invisible in JSON)
            let back = loads(&got).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(d(&back), got, "{name}: dumps(loads(dumps(x)))");
            // and the object itself when JSON can represent it faithfully
            let has_tuple = name.starts_with("tuple") || *name == "deep";
            let nan = matches!(obj, Arg::Float(f) if f.is_nan());
            if !has_tuple && !nan && *name != "dict_intkeys" {
                assert_eq!(&back, obj, "{name}: loads(dumps(x)) == x");
            }
        } else if let Some((_, msg)) = NOT_SERIALIZABLE.iter().find(|(n, _)| n == name) {
            seen += 1;
            assert_eq!(
                err_of(dumps(obj, &opts())),
                (ExcKind::TypeError, msg.to_string()),
                "{name}"
            );
        } else {
            panic!("fixture case {name} has no json expectation");
        }
    }
    assert_eq!(seen, cases.len());
}

#[test]
fn dumps_options() {
    let o = |indent: Option<Arg>,
             separators: Option<(&str, &str)>,
             sort_keys: bool,
             ensure_ascii: bool,
             allow_nan: bool| DumpsOptions {
        indent,
        separators: separators.map(|(a, b)| (a.to_string(), b.to_string())),
        sort_keys,
        ensure_ascii,
        allow_nan,
        default: None,
    };
    let list12 = vec![1i64, 2].into_arg();
    // allow_nan=False
    assert_eq!(
        err_of(dumps(
            &Arg::Float(f64::NAN),
            &o(None, None, false, true, false)
        )),
        (
            ExcKind::ValueError,
            "Out of range float values are not JSON compliant: nan".into()
        )
    );
    assert_eq!(
        err_of(dumps(
            &Arg::Float(f64::INFINITY),
            &o(None, None, false, true, false)
        )),
        (
            ExcKind::ValueError,
            "Out of range float values are not JSON compliant: inf".into()
        )
    );
    assert_eq!(
        err_of(dumps(
            &vec![Arg::Float(f64::NEG_INFINITY)].into_arg(),
            &o(None, None, false, true, false)
        )),
        (
            ExcKind::ValueError,
            "Out of range float values are not JSON compliant: -inf".into()
        )
    );
    assert_eq!(d(&Arg::Float(f64::NEG_INFINITY)), "-Infinity");
    // sort_keys sorts the original keys (ints numerically; mixed types raise)
    let dd = |pairs: Vec<(Arg, Arg)>| {
        let out = dict(Args::new()).unwrap();
        for (k, v) in pairs {
            out.__setitem__(k, v).unwrap();
        }
        out
    };
    assert_eq!(
        dumps(
            &dd(vec![
                (Arg::Int(10), Arg::Int(1)),
                (Arg::Int(9), Arg::Int(2))
            ]),
            &o(None, None, true, true, true)
        )
        .unwrap(),
        "{\"9\": 2, \"10\": 1}"
    );
    assert_eq!(
        err_of(dumps(
            &dd(vec![
                (Arg::from("b"), Arg::Int(1)),
                (Arg::Int(1), Arg::Int(2))
            ]),
            &o(None, None, true, true, true)
        )),
        (
            ExcKind::TypeError,
            "'<' not supported between instances of 'int' and 'str'".into()
        )
    );
    assert_eq!(
        err_of(dumps(
            &dd(vec![
                (Arg::Int(1), Arg::Int(2)),
                (Arg::from("b"), Arg::Int(1))
            ]),
            &o(None, None, true, true, true)
        )),
        (
            ExcKind::TypeError,
            "'<' not supported between instances of 'str' and 'int'".into()
        )
    );
    assert_eq!(
        dumps(
            &dd(vec![
                (Arg::from("b"), Arg::Int(1)),
                (Arg::from("a"), Arg::Int(2)),
                (Arg::from("B"), Arg::Int(3))
            ]),
            &o(None, None, true, true, true)
        )
        .unwrap(),
        "{\"B\": 3, \"a\": 2, \"b\": 1}"
    );
    assert_eq!(
        dumps(
            &dd(vec![
                (Arg::Float(2.5), Arg::Int(1)),
                (Arg::Int(1), Arg::Int(2)),
                (Arg::Bool(true), Arg::Int(3))
            ]),
            &o(None, None, true, true, true)
        )
        .unwrap(),
        "{\"1\": 3, \"2.5\": 1}"
    );
    // key coercion
    assert_eq!(
        d(&dd(vec![
            (Arg::Float(1.5), Arg::Int(1)),
            (Arg::Bool(true), Arg::Int(2)),
            (Arg::None, Arg::Int(3)),
            (Arg::Bool(false), Arg::Int(4)),
            (Arg::Int(7), Arg::Int(5)),
            (Arg::Float(f64::NAN), Arg::Int(6)),
        ])),
        "{\"1.5\": 1, \"true\": 2, \"null\": 3, \"false\": 4, \"7\": 5, \"NaN\": 6}"
    );
    assert_eq!(
        d(&dd(vec![
            (Arg::Int(1), Arg::Int(2)),
            (Arg::Float(1.0), Arg::Int(3))
        ])),
        "{\"1\": 3}"
    );
    assert_eq!(
        err_of(d_res(&dd(vec![(
            Arg::Tuple(vec![Arg::Int(1), Arg::Int(2)].into()),
            Arg::Int(1)
        )]))),
        (
            ExcKind::TypeError,
            "keys must be str, int, float, bool or None, not tuple".into()
        )
    );
    assert_eq!(
        err_of(d_res(&dd(vec![(bytes(b"k"), Arg::Int(1))]))),
        (
            ExcKind::TypeError,
            "keys must be str, int, float, bool or None, not bytes".into()
        )
    );
    // indent
    assert_eq!(
        dumps(&list12, &o(Some(Arg::Int(0)), None, false, true, true)).unwrap(),
        "[\n1,\n2\n]"
    );
    assert_eq!(
        dumps(&list12, &o(Some(Arg::Int(-1)), None, false, true, true)).unwrap(),
        "[\n1,\n2\n]"
    );
    assert_eq!(
        dumps(&list12, &o(Some(Arg::Bool(true)), None, false, true, true)).unwrap(),
        "[\n 1,\n 2\n]"
    );
    assert_eq!(
        dumps(&list12, &o(Some(Arg::None), None, false, true, true)).unwrap(),
        "[1, 2]"
    );
    assert_eq!(
        err_of(dumps(
            &list12,
            &o(Some(Arg::Float(1.5)), None, false, true, true)
        )),
        (
            ExcKind::TypeError,
            "can't multiply sequence by non-int of type 'float'".into()
        )
    );
    let nested = vec![
        Arg::Int(1),
        vec![Arg::Int(2), dd(vec![])].into_arg(),
        dd(vec![(Arg::from("a"), vec![Arg::None; 0].into_arg())]),
    ]
    .into_arg();
    assert_eq!(
        dumps(&nested, &o(Some(Arg::Int(2)), None, false, true, true)).unwrap(),
        "[\n  1,\n  [\n    2,\n    {}\n  ],\n  {\n    \"a\": []\n  }\n]"
    );
    let nd = dd(vec![(
        Arg::from("a"),
        vec![Arg::Int(1), dd(vec![(Arg::from("b"), Arg::Int(2))])].into_arg(),
    )]);
    assert_eq!(
        dumps(&nd, &o(Some(Arg::from("\t")), None, false, true, true)).unwrap(),
        "{\n\t\"a\": [\n\t\t1,\n\t\t{\n\t\t\t\"b\": 2\n\t\t}\n\t]\n}"
    );
    // separators
    assert_eq!(
        dumps(&list12, &o(None, Some((",", ":")), false, true, true)).unwrap(),
        "[1,2]"
    );
    assert_eq!(
        dumps(
            &dd(vec![(Arg::from("a"), Arg::Int(1))]),
            &o(Some(Arg::Int(1)), Some((" , ", " : ")), false, true, true)
        )
        .unwrap(),
        "{\n \"a\" : 1\n}"
    );
    assert_eq!(
        dumps(
            &dd(vec![
                (Arg::from("a"), Arg::Int(1)),
                (Arg::from("b"), list12.clone())
            ]),
            &o(None, Some((",", ":")), false, false, true)
        )
        .unwrap(),
        "{\"a\":1,\"b\":[1,2]}"
    );
    // ensure_ascii
    let s = Arg::from("\u{e9}\u{1f600}\u{7f}\u{1f}\"\\/\n");
    assert_eq!(d(&s), "\"\\u00e9\\ud83d\\ude00\\u007f\\u001f\\\"\\\\/\\n\"");
    assert_eq!(
        dumps(&s, &o(None, None, false, false, true)).unwrap(),
        "\"\u{e9}\u{1f600}\u{7f}\\u001f\\\"\\\\/\\n\""
    );
    assert_eq!(d(&Arg::from("\u{8}\u{c}\r\t\0")), "\"\\b\\f\\r\\t\\u0000\"");
    // floats via repr
    let floats = vec![
        1.0,
        1e16,
        1e-5,
        2.5e-10,
        1e300,
        -0.0,
        123_456_789.123_456_79,
        0.1,
        1e22,
        1e21,
    ]
    .into_arg();
    assert_eq!(
        d(&floats),
        "[1.0, 1e+16, 1e-05, 2.5e-10, 1e+300, -0.0, 123456789.12345679, 0.1, 1e+22, 1e+21]"
    );
    assert_eq!(
        d(&Arg::from_bigint(BigInt::from(1u8) << 70)),
        "1180591620717411303424"
    );
    // tuples are arrays
    assert_eq!(
        d(&Arg::Tuple(vec![Arg::Int(1), Arg::Int(2)].into())),
        "[1, 2]"
    );
    // circular references
    let l = vec![Arg::Int(1)].into_arg();
    l.append(l.clone()).unwrap();
    assert_eq!(
        err_of(d_res(&l)),
        (ExcKind::ValueError, "Circular reference detected".into())
    );
    let m = dd(vec![]);
    m.__setitem__("x", m.clone()).unwrap();
    assert_eq!(
        err_of(d_res(&m)),
        (ExcKind::ValueError, "Circular reference detected".into())
    );
    // the same (non-circular) list twice is fine
    let shared = vec![Arg::Int(1)].into_arg();
    assert_eq!(d(&vec![shared.clone(), shared].into_arg()), "[[1], [1]]");
    // default
    let with_default = |f: Arg| DumpsOptions {
        default: match f {
            Arg::Callable(c) => Some(c),
            _ => unreachable!(),
        },
        ..DumpsOptions::default()
    };
    let to_str = callable(|x: Arg| x.__str__());
    assert_eq!(
        dumps(&list12, &with_default(to_str.clone())).unwrap(),
        "[1, 2]"
    );
    assert_eq!(
        dumps(&bytes(b"xy"), &with_default(callable(|x: Arg| x.decode()))).unwrap(),
        "\"xy\""
    );
    assert_eq!(
        dumps(
            &set(args!(list12.clone())).unwrap(),
            &with_default(callable(|x: Arg| sorted(x)))
        )
        .unwrap(),
        "[1, 2]"
    );
    assert_eq!(
        err_of(dumps(&bytes(b"xy"), &with_default(callable(|x: Arg| x)))),
        (ExcKind::ValueError, "Circular reference detected".into())
    );
    let e = err_of(dumps(
        &bytes(b"xy"),
        &with_default(callable(|_x: Arg| -> laila::Result<Arg> {
            Err(LailaError::type_error("nope"))
        })),
    ));
    assert_eq!(e, (ExcKind::TypeError, "nope".into()));
    assert_eq!(
        err_of(d_res(&bytes(b"x"))),
        (
            ExcKind::TypeError,
            "Object of type bytes is not JSON serializable".into()
        )
    );
}

fn d_res(obj: &Arg) -> laila::Result<String> {
    dumps(obj, &opts())
}

#[test]
fn loads_values() {
    let dd = |pairs: Vec<(Arg, Arg)>| {
        let out = dict(Args::new()).unwrap();
        for (k, v) in pairs {
            out.__setitem__(k, v).unwrap();
        }
        out
    };
    assert!(matches!(loads("NaN").unwrap(), Arg::Float(f) if f.is_nan()));
    assert!(matches!(loads("Infinity").unwrap(), Arg::Float(f) if f == f64::INFINITY));
    assert!(matches!(loads("-Infinity").unwrap(), Arg::Float(f) if f == f64::NEG_INFINITY));
    assert!(matches!(loads("1e400").unwrap(), Arg::Float(f) if f == f64::INFINITY));
    assert!(matches!(loads("-1e400").unwrap(), Arg::Float(f) if f == f64::NEG_INFINITY));
    assert!(matches!(loads("-0").unwrap(), Arg::Int(0)));
    assert!(matches!(loads("-0.0").unwrap(), Arg::Float(f) if f == 0.0 && f.is_sign_negative()));
    assert!(matches!(loads("1.0").unwrap(), Arg::Float(f) if f == 1.0));
    assert!(matches!(loads("1E5").unwrap(), Arg::Float(f) if f == 100000.0));
    assert!(matches!(loads("1.5e+3").unwrap(), Arg::Float(f) if f == 1500.0));
    assert!(matches!(loads("1e-5").unwrap(), Arg::Float(f) if f == 1e-5));
    assert!(matches!(loads("0.1").unwrap(), Arg::Float(f) if f == 0.1));
    assert!(
        matches!(loads("123456789.123456789").unwrap(), Arg::Float(f) if f == 123_456_789.123_456_79)
    );
    assert!(matches!(loads("1").unwrap(), Arg::Int(1)));
    assert!(matches!(
        loads("9223372036854775807").unwrap(),
        Arg::Int(i64::MAX)
    ));
    assert!(matches!(
        loads("-9223372036854775808").unwrap(),
        Arg::Int(i64::MIN)
    ));
    assert!(matches!(
        loads("9223372036854775808").unwrap(),
        Arg::Long(_)
    ));
    assert_eq!(
        loads("123456789012345678901234567890").unwrap(),
        Arg::from_bigint("123456789012345678901234567890".parse::<BigInt>().unwrap())
    );
    assert_eq!(loads("[]").unwrap(), Vec::<Arg>::new().into_arg());
    assert!(matches!(loads("[]").unwrap(), Arg::List(_)));
    assert_eq!(loads("{}").unwrap(), dd(vec![]));
    assert!(matches!(loads("{}").unwrap(), Arg::Dict(_)));
    assert_eq!(loads("  []  ").unwrap(), Vec::<Arg>::new().into_arg());
    assert_eq!(
        loads(" \t\r\n[ 1 , 2 ] \n").unwrap(),
        vec![1i64, 2].into_arg()
    );
    assert_eq!(loads("true").unwrap(), Arg::Bool(true));
    assert_eq!(loads("false").unwrap(), Arg::Bool(false));
    assert_eq!(loads("null").unwrap(), Arg::None);
    // duplicate keys: last value wins, first position kept
    let dup = loads("{\"a\":1,\"a\":2,\"b\":3}").unwrap();
    assert_eq!(
        dup,
        dd(vec![
            (Arg::from("a"), Arg::Int(2)),
            (Arg::from("b"), Arg::Int(3))
        ])
    );
    assert_eq!(dup.__repr__(), "{'a': 2, 'b': 3}");
    // insertion order is kept
    assert_eq!(
        loads("{\"z\":1,\"a\":2}").unwrap().__repr__(),
        "{'z': 1, 'a': 2}"
    );
    // strings and escapes
    assert_eq!(loads("\"\\/\"").unwrap(), Arg::from("/"));
    assert_eq!(
        loads("\"\\\"\\\\\\b\\f\\n\\r\\t\"").unwrap(),
        Arg::from("\"\\\u{8}\u{c}\n\r\t")
    );
    assert_eq!(loads("\"\u{7f}\"").unwrap(), Arg::from("\u{7f}"));
    assert_eq!(loads("\"\u{2028}\"").unwrap(), Arg::from("\u{2028}"));
    assert_eq!(
        loads("\"\\u00e9\\u1234\"").unwrap(),
        Arg::from("\u{e9}\u{1234}")
    );
    assert_eq!(loads("\"\\u12345\"").unwrap(), Arg::from("\u{1234}5"));
    assert_eq!(loads("\"\\uD83D\\uDE00\"").unwrap(), Arg::from("\u{1f600}"));
    assert_eq!(loads("\"\\ud800\\udc00\"").unwrap(), Arg::from("\u{10000}"));
    assert_eq!(
        loads("\"\u{e9}\u{1f600}\"").unwrap(),
        Arg::from("\u{e9}\u{1f600}")
    );
    // a lone surrogate is not representable in a Rust str (documented deviation)
    assert_eq!(loads("\"\\ud800\"").unwrap(), Arg::from("\u{fffd}"));
    assert_eq!(loads("\"\\ud800x\"").unwrap(), Arg::from("\u{fffd}x"));
    assert_eq!(loads("\"\\ud800\\u0041\"").unwrap(), Arg::from("\u{fffd}A"));
    // nested
    let v = loads("{\"a\": [1, 2.5, null, true, {\"b\": \"c\"}], \"d\": {}}").unwrap();
    assert_eq!(
        v,
        dd(vec![
            (
                Arg::from("a"),
                vec![
                    Arg::Int(1),
                    Arg::Float(2.5),
                    Arg::None,
                    Arg::Bool(true),
                    dd(vec![(Arg::from("b"), Arg::from("c"))])
                ]
                .into_arg()
            ),
            (Arg::from("d"), dd(vec![])),
        ])
    );
    // bytes input
    assert_eq!(loads_bytes(b"{}").unwrap(), dd(vec![]));
    assert_eq!(loads_bytes(b"\xef\xbb\xbf{}").unwrap(), dd(vec![]));
    assert_eq!(
        loads_bytes("\"\u{e9}\"".as_bytes()).unwrap(),
        Arg::from("\u{e9}")
    );
    assert_eq!(
        loads_bytes(&bytes(b"[1]").as_bytes_like().unwrap()).unwrap(),
        vec![1i64].into_arg()
    );
}

#[test]
fn loads_errors() {
    let cases: &[(&str, &str)] = &[
        ("", "Expecting value: line 1 column 1 (char 0)"),
        (" ", "Expecting value: line 1 column 2 (char 1)"),
        (
            "{",
            "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
        ),
        (
            "{\"a\"",
            "Expecting ':' delimiter: line 1 column 5 (char 4)",
        ),
        ("{\"a\":", "Expecting value: line 1 column 6 (char 5)"),
        (
            "{\"a\":1",
            "Expecting ',' delimiter: line 1 column 7 (char 6)",
        ),
        (
            "{\"a\":1,",
            "Expecting property name enclosed in double quotes: line 1 column 8 (char 7)",
        ),
        (
            "{\"a\":1,}",
            "Illegal trailing comma before end of object: line 1 column 7 (char 6)",
        ),
        (
            "{\"a\" 1}",
            "Expecting ':' delimiter: line 1 column 6 (char 5)",
        ),
        (
            "{1:2}",
            "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
        ),
        (
            "{,}",
            "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
        ),
        (
            "{\"a\":1 \"b\":2}",
            "Expecting ',' delimiter: line 1 column 8 (char 7)",
        ),
        ("[", "Expecting value: line 1 column 2 (char 1)"),
        ("[1", "Expecting ',' delimiter: line 1 column 3 (char 2)"),
        ("[1,", "Expecting value: line 1 column 4 (char 3)"),
        (
            "[1,]",
            "Illegal trailing comma before end of array: line 1 column 3 (char 2)",
        ),
        ("[,]", "Expecting value: line 1 column 2 (char 1)"),
        ("[1 2]", "Expecting ',' delimiter: line 1 column 4 (char 3)"),
        ("1 2", "Extra data: line 1 column 3 (char 2)"),
        (
            "\"abc",
            "Unterminated string starting at: line 1 column 1 (char 0)",
        ),
        (
            "\"a\nb\"",
            "Invalid control character at: line 1 column 3 (char 2)",
        ),
        ("\"\\x\"", "Invalid \\escape: line 1 column 2 (char 1)"),
        (
            "\"\\u12\"",
            "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
        ),
        (
            "\"\\u1234",
            "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
        ),
        (
            "\"\\uzzzz\"",
            "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
        ),
        (
            "\"\\ud800\\u\"",
            "Invalid \\uXXXX escape: line 1 column 9 (char 8)",
        ),
        (
            "\"\\ud800\\uzzzz\"",
            "Invalid \\uXXXX escape: line 1 column 9 (char 8)",
        ),
        (
            "\"abc\\",
            "Unterminated string starting at: line 1 column 1 (char 0)",
        ),
        ("-", "Expecting value: line 1 column 1 (char 0)"),
        ("-Inf", "Expecting value: line 1 column 1 (char 0)"),
        ("01", "Extra data: line 1 column 2 (char 1)"),
        ("1.", "Extra data: line 1 column 2 (char 1)"),
        (".5", "Expecting value: line 1 column 1 (char 0)"),
        ("1e", "Extra data: line 1 column 2 (char 1)"),
        ("nul", "Expecting value: line 1 column 1 (char 0)"),
        ("tru", "Expecting value: line 1 column 1 (char 0)"),
        (
            "\u{feff}{}",
            "Unexpected UTF-8 BOM (decode using utf-8-sig): line 1 column 1 (char 0)",
        ),
        ("\n\n  x", "Expecting value: line 3 column 3 (char 4)"),
        (
            "a\n\"b\"\n\n  @",
            "Expecting value: line 1 column 1 (char 0)",
        ),
        (
            "\"\u{e9}\u{e9}\", @",
            "Extra data: line 1 column 5 (char 4)",
        ),
        (
            "[\"\u{1f600}\" 1]",
            "Expecting ',' delimiter: line 1 column 6 (char 5)",
        ),
        (
            "[1,\n2,\n]",
            "Illegal trailing comma before end of array: line 2 column 2 (char 5)",
        ),
        (
            "{\"a\":1,\n}",
            "Illegal trailing comma before end of object: line 1 column 7 (char 6)",
        ),
    ];
    for (src, msg) in cases {
        let e = loads(src)
            .err()
            .unwrap_or_else(|| panic!("{src:?} should fail"));
        assert_eq!(e.kind, ExcKind::JSONDecodeError, "{src:?}");
        assert_eq!(e.message, *msg, "{src:?}");
        assert_eq!(e.kind.base(), Some(ExcKind::ValueError));
    }
    // bytes: utf-8 decoding errors
    let bad: &[(&[u8], &str)] = &[
        (
            b"\xff",
            "'utf-8' codec can't decode byte 0xff in position 0: invalid start byte",
        ),
        (
            b"\"\xc3\"",
            "'utf-8' codec can't decode byte 0xc3 in position 1: invalid continuation byte",
        ),
        (
            b"\"\xe9\"",
            "'utf-8' codec can't decode byte 0xe9 in position 1: invalid continuation byte",
        ),
        (
            b"\xe2\x82",
            "'utf-8' codec can't decode bytes in position 0-1: unexpected end of data",
        ),
        (
            b"[\xe2",
            "'utf-8' codec can't decode byte 0xe2 in position 1: unexpected end of data",
        ),
    ];
    for (src, msg) in bad {
        let e = loads_bytes(src)
            .err()
            .unwrap_or_else(|| panic!("{src:?} should fail"));
        assert_eq!(e.kind, ExcKind::UnicodeDecodeError, "{src:?}");
        assert_eq!(e.message, *msg, "{src:?}");
    }
    // only one BOM is stripped for bytes, and the BOM check is str-only
    let e = loads_bytes(b"\xef\xbb\xbf\xef\xbb\xbf{}").unwrap_err();
    assert_eq!(e.message, "Expecting value: line 1 column 1 (char 0)");
}
