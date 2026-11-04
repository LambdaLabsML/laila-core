//! Twin of the `ast.literal_eval subset` block of `codecs.test.js`, plus the
//! CPython 3.14 edge cases (values and error texts probed with `python3`).

mod fixture;

use laila::_compat::pyliteral::literal_eval;
use laila::prelude::*;
use laila::python::errors::ExcKind;
use laila::python::object::bytes;
use num_bigint::BigInt;

fn t(items: Vec<Arg>) -> Arg {
    Arg::Tuple(items.into())
}
fn l(items: Vec<Arg>) -> Arg {
    items.into_arg()
}
fn d(pairs: Vec<(Arg, Arg)>) -> Arg {
    let out = dict(Args::new()).unwrap();
    for (k, v) in pairs {
        out.__setitem__(k, v).unwrap();
    }
    out
}
fn s(items: Vec<Arg>) -> Arg {
    set(args!(l(items))).unwrap()
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
fn ev(src: &str) -> Arg {
    literal_eval(src).unwrap_or_else(|e| panic!("{src:?}: {e}"))
}
fn err(src: &str) -> (ExcKind, String) {
    let e = literal_eval(src)
        .err()
        .unwrap_or_else(|| panic!("{src:?} should fail"));
    (e.kind, e.message)
}
fn syntax(src: &str, msg: &str) {
    assert_eq!(err(src), (ExcKind::SyntaxError, msg.to_string()), "{src:?}");
}
fn malformed(src: &str, node: &str) {
    assert_eq!(
        err(src),
        (
            ExcKind::ValueError,
            format!("malformed node or string on line 1: {node}")
        ),
        "{src:?}"
    );
}

#[test]
fn values() {
    let got = ev("{'a': 1, 'b': [1, 2.0, -3], 'c': (1,), 'd': None, 'e': True, 'f': b'\\x00-_', 'g': 'it\\'s', 'h': {1: 2}}");
    let want = d(vec![
        (st("a"), i(1)),
        (st("b"), l(vec![i(1), f(2.0), i(-3)])),
        (st("c"), t(vec![i(1)])),
        (st("d"), Arg::None),
        (st("e"), Arg::Bool(true)),
        (st("f"), bytes([0u8, 0x2d, 0x5f])),
        (st("g"), st("it's")),
        (st("h"), d(vec![(i(1), i(2))])),
    ]);
    assert_eq!(got, want);
    // the 2.0 stays a float, -3 an int, True a bool
    let b = got.__getitem__("b").unwrap();
    assert!(matches!(b.__getitem__(1).unwrap(), Arg::Float(x) if x == 2.0));
    assert!(matches!(b.__getitem__(2).unwrap(), Arg::Int(-3)));
    assert!(matches!(got.__getitem__("e").unwrap(), Arg::Bool(true)));
    assert!(matches!(got.__getitem__("c").unwrap(), Arg::Tuple(_)));
    assert!(matches!(got.__getitem__("f").unwrap(), Arg::Bytes(_)));

    assert_eq!(
        ev("12345678901234567890"),
        Arg::from_bigint("12345678901234567890".parse::<BigInt>().unwrap())
    );
    assert!(matches!(ev("12345678901234567890"), Arg::Long(_)));
    assert_eq!(ev("(1, 2)"), t(vec![i(1), i(2)]));
    assert_eq!(ev("(1)"), i(1));
    assert!(matches!(ev("(1)"), Arg::Int(1)));
    assert_eq!(ev("()"), t(vec![]));
    assert!(matches!(ev("()"), Arg::Tuple(_)));
    assert_eq!(ev("{1, 2}"), s(vec![i(1), i(2)]));
    assert!(matches!(ev("{1, 2}"), Arg::Set(_)));
    assert_eq!(ev("'a' \"b\""), st("ab"));
    assert_eq!(ev("'''a\nb'''"), st("a\nb"));
    assert!(literal_eval("foo()").is_err());
    assert!(literal_eval("1 +").is_err());
}

#[test]
fn strings() {
    assert_eq!(ev("''"), st(""));
    assert_eq!(ev("\"\"\"x\"\"\""), st("x"));
    assert_eq!(ev("'''x''' 'y'"), st("xy"));
    assert_eq!(ev("'a' 'b' 'c'"), st("abc"));
    assert_eq!(ev("' ' '  '"), st("   "));
    assert_eq!(ev("'a' \\\n 'b'"), st("ab"));
    assert_eq!(ev("\"'\""), st("'"));
    assert_eq!(ev("'\"'"), st("\""));
    assert_eq!(ev("'''\"\"\"'''"), st("\"\"\""));
    assert_eq!(ev("'''\n'''"), st("\n"));
    assert_eq!(ev("u'x'"), st("x"));
    assert_eq!(ev("U'x'"), st("x"));
    assert_eq!(ev("r'\\n'"), st("\\n"));
    assert_eq!(ev("r'\\''"), st("\\'"));
    // escapes
    assert_eq!(ev("'\\d'"), st("\\d"));
    assert_eq!(ev("'\\z'"), st("\\z"));
    assert_eq!(ev("'\\8'"), st("\\8"));
    assert_eq!(ev("'\\101'"), st("A"));
    assert_eq!(ev("'\\0'"), st("\0"));
    assert_eq!(ev("'\\00'"), st("\0"));
    assert_eq!(ev("'\\000'"), st("\0"));
    assert_eq!(ev("'\\0000'"), st("\u{0}0"));
    assert_eq!(ev("'\\400'"), st("\u{100}"));
    assert_eq!(ev("'\\777'"), st("\u{1ff}"));
    assert_eq!(ev("'\\x41\\x4a'"), st("AJ"));
    assert_eq!(ev("'\\u00e9\\U0001f600'"), st("\u{e9}\u{1f600}"));
    assert_eq!(ev("'\\\\'"), st("\\"));
    assert_eq!(ev("'\\\t'"), st("\\\t"));
    assert_eq!(ev("'\\\n'"), st(""));
    assert_eq!(
        ev("'\\n\\t\\r\\a\\b\\f\\v\\'\\\"'"),
        st("\n\t\r\u{7}\u{8}\u{c}\u{b}'\"")
    );
    assert_eq!(ev("'\u{fc}n\u{ef}'"), st("\u{fc}n\u{ef}"));
    // bytes
    assert_eq!(ev("b''"), bytes(b""));
    assert_eq!(ev("B'x'"), bytes(b"x"));
    assert_eq!(ev("b'\\x00-_'"), bytes([0u8, 0x2d, 0x5f]));
    assert_eq!(ev("b'\\xff\\xfe'"), bytes([0xffu8, 0xfe]));
    assert_eq!(ev("b'\\777'"), bytes([0xffu8]));
    assert_eq!(ev("b'\\400'"), bytes([0u8]));
    assert_eq!(ev("b'\\u00e9'"), bytes(b"\\u00e9"));
    assert_eq!(ev("b'\\N{X}'"), bytes(b"\\N{X}"));
    assert_eq!(ev("b'\\u12'"), bytes(b"\\u12"));
    assert_eq!(ev("rb'\\n'"), bytes(b"\\n"));
    assert_eq!(ev("Rb'\\n'"), bytes(b"\\n"));
    assert_eq!(ev("bR'\\n'"), bytes(b"\\n"));
    assert_eq!(ev("rb'\\''"), bytes(b"\\'"));
    assert_eq!(ev("b'a' b'b'"), bytes(b"ab"));
    // errors
    syntax(
        "'abc",
        "unterminated string literal (detected at line 1) (<unknown>, line 1)",
    );
    syntax(
        "'a\nb'",
        "unterminated string literal (detected at line 1) (<unknown>, line 1)",
    );
    syntax(
        "'\n'",
        "unterminated string literal (detected at line 1) (<unknown>, line 1)",
    );
    syntax(
        "'''a''''",
        "unterminated string literal (detected at line 1) (<unknown>, line 1)",
    );
    syntax(
        "''''",
        "unterminated triple-quoted string literal (detected at line 1) (<unknown>, line 1)",
    );
    syntax(
        "\"\"\"a\nb\n",
        "unterminated triple-quoted string literal (detected at line 2) (<unknown>, line 1)",
    );
    syntax("'\\'", "unterminated string literal (detected at line 1); perhaps you escaped the end quote? (<unknown>, line 1)");
    syntax("r'\\'", "unterminated string literal (detected at line 1); perhaps you escaped the end quote? (<unknown>, line 1)");
    syntax(
        "b'\u{e9}'",
        "bytes can only contain ASCII literal characters (<unknown>, line 1)",
    );
    syntax(
        "'a' b'b'",
        "cannot mix bytes and nonbytes literals (<unknown>, line 1)",
    );
    syntax(
        "ur'a'",
        "'u' and 'r' prefixes are incompatible (<unknown>, line 1)",
    );
    syntax(
        "bu'a'",
        "'u' and 'b' prefixes are incompatible (<unknown>, line 1)",
    );
    syntax(
        "fb'a'",
        "'b' and 'f' prefixes are incompatible (<unknown>, line 1)",
    );
    syntax("rr'a'", "invalid syntax (<unknown>, line 1)");
    syntax("'\\x4'", "(unicode error) 'unicodeescape' codec can't decode bytes in position 0-2: truncated \\xXX escape (<unknown>, line 1)");
    syntax("'\\x'", "(unicode error) 'unicodeescape' codec can't decode bytes in position 0-1: truncated \\xXX escape (<unknown>, line 1)");
    syntax("'\\u12'", "(unicode error) 'unicodeescape' codec can't decode bytes in position 0-3: truncated \\uXXXX escape (<unknown>, line 1)");
    syntax("'\\U0011ffff'", "(unicode error) 'unicodeescape' codec can't decode bytes in position 0-9: illegal Unicode character (<unknown>, line 1)");
    syntax(
        "b'\\x4'",
        "(value error) invalid \\x escape at position 0 (<unknown>, line 1)",
    );
    malformed("f'x'", "JoinedStr(values=[Constant(value='x', kind=None)])");
    malformed(
        "f'a' 'b'",
        "JoinedStr(values=[Constant(value='ab', kind=None)])",
    );
    malformed(
        "'a' f'b'",
        "JoinedStr(values=[Constant(value='ab', kind=None)])",
    );
}

#[test]
fn numbers() {
    assert_eq!(ev("0"), i(0));
    assert_eq!(ev("-0"), i(0));
    assert_eq!(ev("+1"), i(1));
    assert_eq!(ev("- 1"), i(-1));
    assert_eq!(ev("-  1"), i(-1));
    assert_eq!(ev("-(1)"), i(-1));
    assert_eq!(ev("1_000_000"), i(1_000_000));
    assert_eq!(ev("0_0"), i(0));
    assert_eq!(ev("00"), i(0));
    assert_eq!(ev("0x_ff"), i(255));
    assert_eq!(ev("0XFF"), i(255));
    assert_eq!(ev("0o17"), i(15));
    assert_eq!(ev("0b101"), i(5));
    assert_eq!(ev("-9223372036854775808"), i(i64::MIN));
    assert!(matches!(ev("-9223372036854775808"), Arg::Int(_)));
    assert_eq!(
        ev("9223372036854775808"),
        Arg::from_bigint("9223372036854775808".parse::<BigInt>().unwrap())
    );
    assert_eq!(
        ev("-9223372036854775809"),
        Arg::from_bigint("-9223372036854775809".parse::<BigInt>().unwrap())
    );
    assert_eq!(
        ev("0x1_0000_0000_0000_0000"),
        Arg::from_bigint("18446744073709551616".parse::<BigInt>().unwrap())
    );
    for (src, want) in [
        ("1.", 1.0),
        (".5", 0.5),
        ("1.e5", 1e5),
        ("1e1_0", 1e10),
        ("0.", 0.0),
        ("0e5", 0.0),
        ("010.5", 10.5),
        ("1e400", f64::INFINITY),
        ("-1.5", -1.5),
        ("2.0", 2.0),
        ("1_0.5_5", 10.55),
        ("3.14E-2", 0.0314),
    ] {
        let got = ev(src);
        assert!(matches!(got, Arg::Float(x) if x == want), "{src}: {got:?}");
    }
    assert!(matches!(ev("-0.0"), Arg::Float(x) if x == 0.0 && x.is_sign_negative()));
    assert_eq!(ev("True"), Arg::Bool(true));
    assert_eq!(ev("False"), Arg::Bool(false));
    assert_eq!(ev("None"), Arg::None);
    // complex
    assert_eq!(ev("1j"), fixture::complex(0.0, 1.0));
    assert_eq!(ev("1J"), fixture::complex(0.0, 1.0));
    assert_eq!(ev("0j"), fixture::complex(0.0, 0.0));
    assert_eq!(ev("1.5j"), fixture::complex(0.0, 1.5));
    assert_eq!(ev(".5j"), fixture::complex(0.0, 0.5));
    assert_eq!(ev("1e5j"), fixture::complex(0.0, 1e5));
    assert_eq!(ev("1_000j"), fixture::complex(0.0, 1000.0));
    assert_eq!(ev("-5j"), fixture::complex(-0.0, -5.0));
    assert_eq!(ev("-5j").__repr__(), "(-0-5j)");
    assert_eq!(ev("1 - 2j"), fixture::complex(1.0, -2.0));
    assert_eq!(ev("1.5 + 2j"), fixture::complex(1.5, 2.0));
    assert_eq!(ev("(1 + 2j)"), fixture::complex(1.0, 2.0));
    assert_eq!(ev("1 + 2J"), fixture::complex(1.0, 2.0));
    assert_eq!(ev("-1 + 2j"), fixture::complex(-1.0, 2.0));
    assert_eq!(ev("-1.5 - 2.5j"), fixture::complex(-1.5, -2.5));
    assert_eq!(ev("1 \\\n + 2j"), fixture::complex(1.0, 2.0));
    assert_eq!(ev("1 + 2j,"), t(vec![fixture::complex(1.0, 2.0)]));
    assert_eq!(
        ev("[1+2j, 3j]"),
        l(vec![fixture::complex(1.0, 2.0), fixture::complex(0.0, 3.0)])
    );
    // errors
    let lz = "leading zeros in decimal integer literals are not permitted; use an 0o prefix for octal integers (<unknown>, line 1)";
    syntax("010", lz);
    syntax("09", lz);
    syntax("0_1", lz);
    for src in [
        "1__0", "1_", "0_", "0__0", "1e", "1e+", "1.5e", "1e_5", "1.5_", "1_.5", "1._5", "00_",
        "1abc",
    ] {
        syntax(src, "invalid decimal literal (<unknown>, line 1)");
    }
    syntax("0x", "invalid hexadecimal literal (<unknown>, line 1)");
    syntax("0x_", "invalid hexadecimal literal (<unknown>, line 1)");
    syntax("0x1_", "invalid hexadecimal literal (<unknown>, line 1)");
    syntax("0x1j", "invalid hexadecimal literal (<unknown>, line 1)");
    syntax("0b", "invalid binary literal (<unknown>, line 1)");
    syntax("0o", "invalid octal literal (<unknown>, line 1)");
    syntax(
        "0b2",
        "invalid digit '2' in binary literal (<unknown>, line 1)",
    );
    syntax(
        "0o8",
        "invalid digit '8' in octal literal (<unknown>, line 1)",
    );
    syntax(".", "invalid syntax (<unknown>, line 1)");
    syntax(".e5", "invalid syntax (<unknown>, line 1)");
    syntax("1.2.3", "invalid syntax (<unknown>, line 1)");
    syntax("1..2", "invalid syntax (<unknown>, line 1)");
    malformed(
        "--1",
        "UnaryOp(op=USub(), operand=Constant(value=1, kind=None))",
    );
    malformed(
        "+-1",
        "UnaryOp(op=USub(), operand=Constant(value=1, kind=None))",
    );
    malformed("-True", "Constant(value=True, kind=None)");
    malformed("-'a'", "Constant(value='a', kind=None)");
    malformed("-()", "Tuple(elts=[], ctx=Load())");
    malformed(
        "-[1, 2]",
        "List(elts=[Constant(value=1, kind=None), Constant(value=2, kind=None)], ctx=Load())",
    );
    malformed(
        "-[[1]]",
        "List(elts=[List(elts=[Constant(value=1, kind=None)], ctx=Load())], ctx=Load())",
    );
    malformed(
        "1 + 2",
        "BinOp(left=Constant(value=1, kind=None), op=Add(), right=Constant(value=2, kind=None))",
    );
    malformed(
        "1 +2",
        "BinOp(left=Constant(value=1, kind=None), op=Add(), right=Constant(value=2, kind=None))",
    );
    malformed(
        "1 - 2",
        "BinOp(left=Constant(value=1, kind=None), op=Sub(), right=Constant(value=2, kind=None))",
    );
    malformed(
        "1.0 + 2",
        "BinOp(left=Constant(value=1.0, kind=None), op=Add(), right=Constant(value=2, kind=None))",
    );
    malformed(
        "2j + 1",
        "BinOp(left=Constant(value=2j, kind=None), op=Add(), right=Constant(value=1, kind=None))",
    );
    malformed(
        "1j + 2j",
        "BinOp(left=Constant(value=1j, kind=None), op=Add(), right=Constant(value=2j, kind=None))",
    );
    malformed(
        "1 + -2j",
        "UnaryOp(op=USub(), operand=Constant(value=2j, kind=None))",
    );
    malformed(
        "1 - -2j",
        "UnaryOp(op=USub(), operand=Constant(value=2j, kind=None))",
    );
    malformed("True + 1j", "Constant(value=True, kind=None)");
    malformed(
        "1 + 2.0j - 3j",
        "BinOp(left=Constant(value=1, kind=None), op=Add(), right=Constant(value=2j, kind=None))",
    );
    malformed(
        "{1: [2, [3, [4]]]} + 1",
        "Dict(keys=[Constant(value=1, kind=None)], values=[List(elts=[Constant(value=2, kind=None), List(elts=[Constant(...), List(...)], ctx=Load(...))], ctx=Load())])",
    );
}

#[test]
fn containers() {
    assert_eq!(ev("[]"), l(vec![]));
    assert_eq!(ev("[1,]"), l(vec![i(1)]));
    assert_eq!(ev("[1, 2 ,3]"), l(vec![i(1), i(2), i(3)]));
    assert_eq!(ev("(1,)"), t(vec![i(1)]));
    assert_eq!(ev("(1, 2,)"), t(vec![i(1), i(2)]));
    assert_eq!(ev("1,"), t(vec![i(1)]));
    assert_eq!(ev("1, 2"), t(vec![i(1), i(2)]));
    assert_eq!(ev("-1,"), t(vec![i(-1)]));
    assert_eq!(ev("{}"), d(vec![]));
    assert!(matches!(ev("{}"), Arg::Dict(_)));
    assert_eq!(ev("{1: 2,}"), d(vec![(i(1), i(2))]));
    assert_eq!(ev("{1: 2 }"), d(vec![(i(1), i(2))]));
    assert_eq!(ev("{1,}"), s(vec![i(1)]));
    assert_eq!(ev("{1, 2, 1}"), s(vec![i(1), i(2)]));
    assert_eq!(ev("set()"), s(vec![]));
    assert!(matches!(ev("set()"), Arg::Set(_)));
    assert_eq!(ev("{True: 1, 1: 2}"), d(vec![(Arg::Bool(true), i(2))]));
    assert_eq!(ev("{True: 1, 1: 2}").__repr__(), "{True: 2}");
    assert_eq!(ev("{'a': 1, 'a': 2}"), d(vec![(st("a"), i(2))]));
    assert_eq!(ev("[\n1,\n2\n]"), l(vec![i(1), i(2)]));
    assert_eq!(ev("(\n1\n)"), i(1));
    assert_eq!(ev("[1,\n # c\n 2]"), l(vec![i(1), i(2)]));
    assert_eq!(ev("1 # c"), i(1));
    assert_eq!(ev(" 1"), i(1));
    assert_eq!(ev("1 "), i(1));
    assert_eq!(ev("1\n"), i(1));
    assert_eq!(ev("\n1"), i(1));
    assert_eq!(ev("\"\"\"a\nb\"\"\"\n\n"), st("a\nb"));
    assert_eq!(
        ev("((1, 2), (3, (4, 5)))"),
        t(vec![
            t(vec![i(1), i(2)]),
            t(vec![i(3), t(vec![i(4), i(5)])])
        ])
    );
    assert_eq!(
        ev("{'a': [{'b': ({'c': [1, (2, 3)]},)}]}"),
        d(vec![(
            st("a"),
            l(vec![d(vec![(
                st("b"),
                t(vec![d(vec![(st("c"), l(vec![i(1), t(vec![i(2), i(3)])]))])])
            )])]),
        )])
    );
    // the fixture's `repr`-able cases round-trip through repr -> literal_eval
    for (name, v) in fixture::cases() {
        if name.starts_with("str_70000")
            || name.starts_with("bytes_7")
            || name.starts_with("bytes_6")
        {
            continue; // large, same code path as the smaller ones
        }
        if matches!(v, Arg::Float(x) if x.is_nan() || x.is_infinite()) {
            continue; // `nan` / `inf` are names, not literals
        }
        if matches!(v, Arg::FrozenSet(_) | Arg::ByteArray(_)) {
            continue; // repr is a call, not a literal
        }
        let r = v.__repr__();
        assert_eq!(ev(&r), v, "{name}: {r}");
    }
    // errors
    syntax("", "invalid syntax (<unknown>, line 0)");
    syntax("   ", "invalid syntax (<unknown>, line 0)");
    syntax("1 +", "invalid syntax (<unknown>, line 1)");
    syntax("1 +\n", "invalid syntax (<unknown>, line 1)");
    syntax("-\n1", "invalid syntax (<unknown>, line 1)");
    syntax("1 2", "invalid syntax (<unknown>, line 1)");
    syntax("x y", "invalid syntax (<unknown>, line 1)");
    syntax("'a' 1", "invalid syntax (<unknown>, line 1)");
    syntax("1 'a'", "invalid syntax (<unknown>, line 1)");
    syntax("x = 1", "invalid syntax (<unknown>, line 1)");
    syntax("1;2", "invalid syntax (<unknown>, line 1)");
    syntax("*a", "invalid syntax (<unknown>, line 1)");
    syntax("[1,,2]", "invalid syntax (<unknown>, line 1)");
    syntax("[1,2,,]", "invalid syntax (<unknown>, line 1)");
    syntax("(,)", "invalid syntax (<unknown>, line 1)");
    syntax("{:1}", "invalid syntax (<unknown>, line 1)");
    syntax("{1,2:3}", "invalid syntax (<unknown>, line 1)");
    syntax("{1:2,,}", "invalid syntax (<unknown>, line 1)");
    syntax("(1, 2", "'(' was never closed (<unknown>, line 1)");
    syntax("(1,", "'(' was never closed (<unknown>, line 1)");
    syntax("(1", "'(' was never closed (<unknown>, line 1)");
    syntax("[", "'[' was never closed (<unknown>, line 1)");
    syntax("[1,", "'[' was never closed (<unknown>, line 1)");
    syntax("([1,", "'[' was never closed (<unknown>, line 1)");
    syntax("(1, [2", "'[' was never closed (<unknown>, line 1)");
    syntax("[(1, 2", "'(' was never closed (<unknown>, line 1)");
    syntax("{1: 2", "'{' was never closed (<unknown>, line 1)");
    syntax("{1", "'{' was never closed (<unknown>, line 1)");
    syntax("{1:", "'{' was never closed (<unknown>, line 1)");
    syntax("]", "unmatched ']' (<unknown>, line 1)");
    syntax("1)", "unmatched ')' (<unknown>, line 1)");
    syntax(
        "[1)",
        "closing parenthesis ')' does not match opening parenthesis '[' (<unknown>, line 1)",
    );
    syntax(
        "[1 2]",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "(1 2)",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "{1 2}",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "{1: 2 3: 4}",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "[1, 2 3]",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "['a' 1]",
        "invalid syntax. Perhaps you forgot a comma? (<unknown>, line 1)",
    );
    syntax(
        "{1:2, 3}",
        "':' expected after dictionary key (<unknown>, line 1)",
    );
    syntax(
        "{1:}",
        "expression expected after dictionary key and ':' (<unknown>, line 1)",
    );
    syntax(
        "{1:2,3:}",
        "expression expected after dictionary key and ':' (<unknown>, line 1)",
    );
    malformed("foo", "Name(id='foo', ctx=Load())");
    malformed("inf", "Name(id='inf', ctx=Load())");
    malformed(
        "foo()",
        "Call(func=Name(id='foo', ctx=Load()), args=[], keywords=[])",
    );
    malformed("set([1])", "Call(func=Name(id='set', ctx=Load()), args=[List(elts=[Constant(value=1, kind=None)], ctx=Load())], keywords=[])");
    malformed(
        "True()",
        "Call(func=Constant(value=True, kind=None), args=[], keywords=[])",
    );
    malformed(
        "(1)(2)",
        "Call(func=Constant(value=1, kind=None), args=[Constant(value=2, kind=None)], keywords=[])",
    );
    malformed("(1,)(2)", "Call(func=Tuple(elts=[Constant(value=1, kind=None)], ctx=Load()), args=[Constant(value=2, kind=None)], keywords=[])");
    malformed("[1, 2](3)", "Call(func=List(elts=[Constant(value=1, kind=None), Constant(value=2, kind=None)], ctx=Load()), args=[Constant(value=3, kind=None)], keywords=[])");
    malformed(
        "None.x",
        "Attribute(value=Constant(value=None, kind=None), attr='x', ctx=Load())",
    );
    malformed(
        "a.b",
        "Attribute(value=Name(id='a', ctx=Load()), attr='b', ctx=Load())",
    );
    malformed("[1][0]", "Subscript(value=List(elts=[Constant(value=1, kind=None)], ctx=Load()), slice=Constant(value=0, kind=None), ctx=Load())");
    malformed("(1,)[0]", "Subscript(value=Tuple(elts=[Constant(value=1, kind=None)], ctx=Load()), slice=Constant(value=0, kind=None), ctx=Load())");
    malformed("{1: 2}[1]", "Subscript(value=Dict(keys=[Constant(value=1, kind=None)], values=[Constant(value=2, kind=None)]), slice=Constant(value=1, kind=None), ctx=Load())");
    assert_eq!(
        err("{[1]: 2}"),
        (
            ExcKind::TypeError,
            "cannot use 'list' as a dict key (unhashable type: 'list')".to_string()
        )
    );
    assert_eq!(
        err("{[1]}"),
        (
            ExcKind::TypeError,
            "cannot use 'list' as a set element (unhashable type: 'list')".to_string()
        )
    );
    assert_eq!(
        err("{{}: 1}"),
        (
            ExcKind::TypeError,
            "cannot use 'dict' as a dict key (unhashable type: 'dict')".to_string()
        )
    );
    // line numbers
    assert_eq!(
        err("[\n1,\nfoo]"),
        (
            ExcKind::ValueError,
            "malformed node or string on line 3: Name(id='foo', ctx=Load())".to_string()
        )
    );
    syntax(
        "[\n1,\n'abc",
        "unterminated string literal (detected at line 3) (<unknown>, line 3)",
    );
}
