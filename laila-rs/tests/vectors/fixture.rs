//! Shared loader for `codecs.json` plus the Rust-side constructions of every
//! case (the twin of `laila-js/tests/vectors/_fixture.js`).
//!
//! The JSON is produced by `laila-js/tests/vectors/gen_vectors.py` with the
//! *real* CPython / msgpack / numpy / cryptography, so nothing here encodes an
//! expectation by hand.

#![allow(dead_code)]

use laila::_compat::ndarray::{Complex, NDArray, Scalar};
use laila::prelude::*;
use num_bigint::BigInt;
use std::sync::OnceLock;

pub const CODECS_JSON: &str = include_str!("../../../laila-js/tests/vectors/codecs.json");

pub fn fixture() -> &'static serde_json::Value {
    static F: OnceLock<serde_json::Value> = OnceLock::new();
    F.get_or_init(|| serde_json::from_str(CODECS_JSON).expect("codecs.json parses"))
}

/// Decode a fixture payload (`hex` or `z:<base64(zlib)>`).
pub fn decode(s: &str) -> Vec<u8> {
    if let Some(rest) = s.strip_prefix("z:") {
        let compressed = laila::_codecs::base64::b64decode(&Arg::from(rest), None, false)
            .expect("fixture base64");
        return laila::_codecs::zlib::decompress(&compressed, 15, 16384).expect("fixture zlib");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

pub fn first_diff(got: &[u8], want: &[u8]) -> String {
    let mut i = 0;
    while i < got.len() && i < want.len() && got[i] == want[i] {
        i += 1;
    }
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let lo = i.saturating_sub(8);
    format!(
        "len {} vs {}, first diff @{}: got {} want {}",
        got.len(),
        want.len(),
        i,
        hex(&got[lo..got.len().min(i + 16)]),
        hex(&want[lo..want.len().min(i + 16)])
    )
}

// ---- Python object constructions ------------------------------------------

fn l(items: Vec<Arg>) -> Arg {
    items.into_arg()
}
fn t(items: Vec<Arg>) -> Arg {
    Arg::Tuple(items.into())
}
fn d(pairs: Vec<(Arg, Arg)>) -> Arg {
    let out = dict(Args::new()).unwrap();
    for (k, v) in pairs {
        out.__setitem__(k, v).unwrap();
    }
    out
}
fn sd(pairs: Vec<(&str, Arg)>) -> Arg {
    d(pairs.into_iter().map(|(k, v)| (Arg::from(k), v)).collect())
}
fn s(items: Vec<Arg>) -> Arg {
    set(args!(l(items))).unwrap()
}
fn fs(items: Vec<Arg>) -> Arg {
    frozenset(args!(l(items))).unwrap()
}
fn b(bytes_: &[u8]) -> Arg {
    laila::python::object::bytes(bytes_)
}
fn i(v: i64) -> Arg {
    Arg::Int(v)
}
fn f(v: f64) -> Arg {
    Arg::Float(v)
}
fn st(v: &str) -> Arg {
    Arg::from(v)
}
fn big(v: &str) -> Arg {
    Arg::from_bigint(v.parse::<BigInt>().unwrap())
}
fn rng(n: i64) -> Vec<Arg> {
    (0..n).map(i).collect()
}
fn kdict(n: i64) -> Arg {
    sd((0..n)
        .map(|k| (Box::leak(format!("k{k}").into_boxed_str()) as &str, i(k)))
        .collect())
}

/// Every case name -> its Python object (same names as `gen_vectors.py`).
/// `str_surrogate` is absent: a lone surrogate cannot live in a Rust `str`.
pub fn cases() -> Vec<(String, Arg)> {
    let shared = l(vec![i(1), i(2)]);
    let mut out: Vec<(String, Arg)> = vec![
        ("none".into(), Arg::None),
        ("true".into(), Arg::Bool(true)),
        ("false".into(), Arg::Bool(false)),
        ("float_3.5".into(), f(3.5)),
        ("float_neg".into(), f(-2.5e-10)),
        ("float_1.0".into(), f(1.0)),
        ("float_0.0".into(), f(0.0)),
        ("float_inf".into(), f(f64::INFINITY)),
        ("float_nan".into(), f(f64::NAN)),
        ("str_empty".into(), st("")),
        ("str_hello".into(), st("hello laila")),
        (
            "str_unicode".into(),
            st("\u{fc}n\u{ef}c\u{f6}d\u{e9} \u{1f600}"),
        ),
        ("str_255".into(), st(&"z".repeat(255))),
        ("str_256".into(), st(&"z".repeat(256))),
        ("str_300".into(), st(&"x".repeat(300))),
        ("str_70000".into(), st(&"y".repeat(70000))),
        ("bytes_empty".into(), b(&[])),
        ("bytes_small".into(), b(&[0, 1, 2, 0xfe, 0xff])),
        ("bytes_255".into(), b(&[0x71; 255])),
        ("bytes_256".into(), b(&[0x71; 256])),
        (
            "bytes_300".into(),
            b(&(0..=255u8)
                .chain(std::iter::repeat_n(0, 44))
                .collect::<Vec<_>>()),
        ),
        ("bytes_65535".into(), b(&vec![2u8; 65535])),
        ("bytes_65536".into(), b(&vec![1u8; 65536])),
        ("bytes_70000".into(), b(&vec![0x7fu8; 70000])),
        ("bytearray".into(), bytearray(args!(b(&[1, 2, 3]))).unwrap()),
        ("list_empty".into(), l(vec![])),
        ("list_one".into(), l(vec![i(7)])),
        ("list_1234".into(), l(rng(5)[1..].to_vec())),
        ("list_1000".into(), l(rng(1000))),
        ("list_1001".into(), l(rng(1001))),
        ("list_1500".into(), l(rng(1500))),
        ("list_2000".into(), l(rng(2000))),
        ("list_2001".into(), l(rng(2001))),
        (
            "list_nested".into(),
            l(vec![
                l(vec![i(1), l(vec![i(2), l(vec![i(3)])])]),
                sd(vec![("k", l(vec![Arg::None, Arg::Bool(true)]))]),
            ]),
        ),
        (
            "list_floats".into(),
            l(vec![f(1.5), f(2.0), f(-0.0), f(1e300)]),
        ),
        ("list_same_str".into(), l(vec![st("shared"), st("shared")])),
        ("list_shared_list".into(), l(vec![shared.clone(), shared])),
        (
            "bool_in_list".into(),
            l(vec![
                Arg::Bool(true),
                Arg::Bool(false),
                Arg::None,
                i(0),
                i(1),
            ]),
        ),
        (
            "neg_fixint".into(),
            l(vec![i(-1), i(-32), i(-33), i(-127), i(-128)]),
        ),
        (
            "uint_edges".into(),
            l(vec![
                i(127),
                i(128),
                i(255),
                i(256),
                i(65535),
                i(65536),
                i(4294967295),
                i(4294967296),
            ]),
        ),
        ("tuple_empty".into(), t(vec![])),
        ("tuple_1".into(), t(vec![i(1)])),
        ("tuple_2".into(), t(vec![i(1), i(2)])),
        ("tuple_3".into(), t(vec![i(1), i(2), i(3)])),
        ("tuple_4".into(), t(vec![i(1), i(2), i(3), i(4)])),
        (
            "tuple_nested".into(),
            t(vec![
                t(vec![i(1), i(2)]),
                t(vec![i(3), t(vec![i(4), i(5)])]),
            ]),
        ),
        ("dict_empty".into(), sd(vec![])),
        ("dict_one".into(), sd(vec![("k", st("v"))])),
        (
            "dict_ordered".into(),
            sd(vec![("b", i(1)), ("a", i(2)), ("z", i(3))]),
        ),
        (
            "dict_intkeys".into(),
            d(vec![(i(1), st("x")), (i(2), st("y"))]),
        ),
        (
            "dict_tuplekey".into(),
            d(vec![(t(vec![i(1), i(2)]), st("y"))]),
        ),
        ("dict_1000".into(), kdict(1000)),
        ("dict_1001".into(), kdict(1001)),
        ("dict_1500".into(), kdict(1500)),
        ("dict_2000".into(), kdict(2000)),
        (
            "dict_nested".into(),
            sd(vec![(
                "a",
                sd(vec![("b", l(vec![i(1), i(2), sd(vec![("c", i(3))])]))]),
            )]),
        ),
        (
            "dict_mixed".into(),
            sd(vec![
                ("i", i(1)),
                ("f", f(2.5)),
                ("s", st("x")),
                ("b", Arg::Bool(true)),
                ("n", Arg::None),
                ("l", l(vec![i(1), st("two")])),
            ]),
        ),
        ("dict_same_str".into(), sd(vec![("a", st("a"))])),
        (
            "dict_bytes".into(),
            sd(vec![("k", b(&[0xde, 0xad, 0xbe, 0xef]))]),
        ),
        (
            "deep".into(),
            sd(vec![(
                "a",
                l(vec![sd(vec![(
                    "b",
                    t(vec![sd(vec![("c", l(vec![i(1), t(vec![i(2), i(3)])]))])]),
                )])]),
            )]),
        ),
        ("set_empty".into(), s(vec![])),
        ("set_123".into(), s(vec![i(1), i(2), i(3)])),
        ("set_1000".into(), s(rng(1000))),
        ("set_1001".into(), s(rng(1001))),
        ("set_1500".into(), s(rng(1500))),
        ("frozenset_empty".into(), fs(vec![])),
        ("frozenset_123".into(), fs(vec![i(1), i(2), i(3)])),
        ("int_9223372036854775807".into(), i(i64::MAX)),
        ("int_-9223372036854775808".into(), i(i64::MIN)),
        (
            "int_18446744073709551615".into(),
            big("18446744073709551615"),
        ),
        (
            "int_18446744073709551616".into(),
            big("18446744073709551616"),
        ),
        (
            "int_1000000000000000000000000000000".into(),
            big("1000000000000000000000000000000"),
        ),
        (
            "int_-1000000000000000000000000000000".into(),
            big("-1000000000000000000000000000000"),
        ),
    ];
    for v in [
        0i64,
        1,
        255,
        256,
        65535,
        65536,
        (1 << 31) - 1,
        -1,
        -128,
        -129,
        -32768,
        -32769,
        -(1 << 31),
        -(1i64 << 31) - 1,
        1 << 31,
        (1 << 32) - 1,
        1 << 32,
        1 << 53,
    ] {
        out.push((format!("int_{v}"), i(v)));
    }
    out
}

/// Look a case up by name.
pub fn case(name: &str) -> Arg {
    cases()
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no case {name}"))
}

/// `np.linspace(a, b, n)`: `a + step * i` with `step = (b - a) / (n - 1)`.
fn linspace(a: f64, b: f64, n: usize) -> Vec<f64> {
    let step = (b - a) / (n as f64 - 1.0);
    (0..n).map(|i| a + step * i as f64).collect()
}
fn fl(v: &[f64]) -> Vec<Scalar> {
    v.iter().map(|x| Scalar::Float(*x)).collect()
}
fn ints(v: &[i64]) -> Vec<Scalar> {
    v.iter().map(|x| Scalar::Int(*x)).collect()
}
fn nd(dtype: &str, shape: &[usize], fortran: bool, data: Vec<Scalar>) -> NDArray {
    NDArray::from_scalars(dtype, shape.to_vec(), fortran, &data).unwrap()
}

/// Every ndarray case (same names as `gen_vectors.py`).
pub fn ndarrays() -> Vec<(String, NDArray)> {
    let c = |re: f64, im: f64| Scalar::Complex(re, im);
    let c16: Vec<Scalar> = linspace(-1.0, 1.0, 6)
        .into_iter()
        .zip(linspace(0.5, -0.5, 6))
        .map(|(re, im)| c(re, im))
        .collect();
    vec![
        (
            "npy_f64".into(),
            nd("<f8", &[2, 2], false, fl(&[1.5, 2.5, 3.5, 4.5])),
        ),
        (
            "npy_i32".into(),
            nd("<i4", &[5], false, ints(&[1, 2, 3, 4, 5])),
        ),
        (
            "npy_u8".into(),
            NDArray::arange(6, "|u1")
                .unwrap()
                .reshaped(vec![2, 3])
                .unwrap(),
        ),
        (
            "npy_bool".into(),
            nd(
                "|b1",
                &[3],
                false,
                vec![Scalar::Bool(true), Scalar::Bool(false), Scalar::Bool(true)],
            ),
        ),
        ("npy_i64".into(), NDArray::arange(10, "<i8").unwrap()),
        (
            "npy_f32_3d".into(),
            NDArray::zeros(vec![2, 3, 4], "<f4").unwrap(),
        ),
        ("npy_scalar".into(), nd("<f8", &[], false, fl(&[7.5]))),
        ("npy_empty".into(), nd("<f8", &[0], false, vec![])),
        (
            "npy_fortran".into(),
            nd("<i4", &[2, 3], true, ints(&[1, 4, 2, 5, 3, 6])),
        ),
        (
            "npy_big_shape".into(),
            NDArray::zeros(vec![1; 30], "|u1").unwrap(),
        ),
        (
            "npy_f16".into(),
            nd(
                "<f2",
                &[11],
                false,
                fl(&[
                    1.0,
                    -1.0,
                    0.1,
                    65504.0,
                    6e-8,
                    f64::NAN,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    0.0,
                    -0.0,
                    1.0 / 3.0,
                ]),
            ),
        ),
        (
            "npy_f16_lin".into(),
            nd("<f2", &[2, 3, 4], false, fl(&linspace(-1.0, 1.0, 24))),
        ),
        (
            "npy_c8".into(),
            nd(
                "<c8",
                &[3],
                false,
                vec![c(1.0, 2.0), c(-3.0, 4.0), c(0.0, -1.0)],
            ),
        ),
        ("npy_c16".into(), nd("<c16", &[2, 3], false, c16)),
        (
            "npy_be_i4".into(),
            NDArray::arange(5, "<i8").unwrap().astype(">i4").unwrap(),
        ),
        (
            "npy_be_f8".into(),
            nd(">f8", &[6], false, fl(&linspace(-1.0, 1.0, 6))),
        ),
        (
            "npy_be_f8_fortran".into(),
            nd(">f8", &[2, 3], true, fl(&[0.0, 3.0, 1.0, 4.0, 2.0, 5.0])),
        ),
        (
            "npy_u8_max".into(),
            nd(
                "<u8",
                &[3],
                false,
                vec![
                    Scalar::UInt(0),
                    Scalar::UInt(1 << 63),
                    Scalar::UInt(u64::MAX),
                ],
            ),
        ),
        (
            "npy_i8_minmax".into(),
            nd("<i8", &[3], false, ints(&[i64::MIN, 0, i64::MAX])),
        ),
    ]
}

pub fn ndarray(name: &str) -> NDArray {
    ndarrays()
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no ndarray {name}"))
}

/// `Complex` as an `Arg` (for literal_eval / tolist comparisons).
pub fn complex(re: f64, im: f64) -> Arg {
    Complex::new(re, im).into_arg()
}
