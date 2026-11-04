//! `laila::python!` rule tests: one compile-pass case per expression-layer
//! rule of `docs/PORT_RULES.md` (P20 - P30) plus the regressions fixed while
//! porting the suites. Each body is the Python spelling; what it lowers to
//! is checked by the assertions. The compile-*fail* half lives in
//! `tests/ui/*.rs` (`trybuild`).
#![allow(non_snake_case, unused_variables)]

use laila::entry::EntryState;
use laila::prelude::*;

laila::python! {
    #![unittest]

    // ---- P20: calls and keyword arguments -------------------------------
    fn kw(a: Arg, b: Arg, c: Arg) -> Arg {
        return [a, b, c];
    }

    py_class!(TestCalls);
    impl TestCalls {
        fn test_p20_keyword_arguments_reach_pysig_functions(&self) {
            // `laila.constant(data=..)` is a `#[pysig]` facade call: `args!`.
            let e = laila.constant(data={"k": 1}, nickname="p20");
            self.assertEqual(e.data, {"k": 1});
            self.assertEqual(e.scopes, ["ENTRY"]);
        }

        fn test_p20_positional_user_functions_take_owned_args(&self) {
            let xs = [1, 2];
            // `xs` is cloned into the call (Python shares the object) and
            // stays usable after it.
            self.assertEqual(kw(xs, "b", None), [[1, 2], "b", None]);
            self.assertEqual(len(xs), 2);
        }

        fn test_p20_star_args_and_kwargs_forward(&self) {
            let pos = (1, 2);
            let named = {"nickname": "fwd"};
            let e = laila.constant(data=list(pos), **named);
            let same = laila.constant(data="other", nickname="fwd");
            self.assertEqual(e.uuid, same.uuid);
            self.assertEqual(e.data, [1, 2]);
        }
    }

    // ---- P21 / P22: constructors, module paths, attributes --------------
    py_class!(TestAttributes);
    impl TestAttributes {
        fn test_p21_constructor_and_enum_path(&self) {
            let e = laila.Entry(payload=[1], state=EntryState.READY);
            self.assertEqual(e.state, EntryState.READY);
            self.assertEqual(e.data, [1]);
        }

        fn test_p22_attribute_read_and_setattr(&self) {
            let e = laila.variable(data=1);
            e._state = EntryState.STAGED;
            self.assertEqual(e.state, EntryState.STAGED);
            self.assertEqual(getattr(e, "state"), EntryState.STAGED);
            self.assertEqual(getattr(e, "data"), 1);
            self.assertTrue(hasattr(e, "global_id"));
            self.assertFalse(hasattr(e, "no_such_attribute"));
        }

        fn test_p22_container_method_named_attributes(&self) {
            // `index` / `keys` / `get` are container *methods* on `Arg` and
            // attributes on laila objects: both spellings resolve.
            let d = {"a": 1};
            self.assertEqual(d.get("a"), 1);
            self.assertEqual(d.get("zz", 7), 7);
            self.assertEqual(list(d.keys()), ["a"]);
            self.assertEqual([3, 4, 5].index(4), 1);
        }
    }

    // ---- P23: literals ---------------------------------------------------
    py_class!(TestLiterals);
    impl TestLiterals {
        fn test_p23_dict_list_tuple_set_bytes_none(&self) {
            let d = {"a": 1, "b": [1, 2], "c": (3, 4), "d": None, "e": b"\x00\x01"};
            self.assertEqual(d["b"], [1, 2]);
            self.assertEqual(d["c"], (3, 4));
            self.assertIsNone(d["d"]);
            self.assertEqual(len(d["e"]), 2);
            self.assertEqual(list(d.keys()), ["a", "b", "c", "d", "e"]);
            let s = {1, 2, 2, 3};
            self.assertEqual(len(s), 3);
            self.assertTrue(2 in s);
            self.assertIs(True, True);
        }

        fn test_p23_nested_literals_alias_like_python(&self) {
            let inner = [1];
            let outer = {"xs": inner};
            inner.append(2);
            // the dict holds the same list object, not a copy
            self.assertEqual(outer["xs"], [1, 2]);
        }
    }

    // ---- P24: operators --------------------------------------------------
    fn v(x: Arg) -> Arg {
        return x;
    }

    py_class!(TestOperators);
    impl TestOperators {
        fn test_p24_arithmetic_follows_python(&self) {
            self.assertEqual("ab" * 2, "abab");
            self.assertEqual([1] + [2], [1, 2]);
            self.assertEqual(7 / 2, 3.5);
            self.assertEqual(floordiv(7, 2), 3);
            self.assertEqual(-3 % 5, 2);
            self.assertEqual(2 ** 10, 1024);
            self.assertEqual("k%d" % 3, "k3");
            self.assertEqual("%s-%s" % ("a", 1), "a-1");
        }

        fn test_p24_comparisons_and_truthiness(&self) {
            // Python semantics apply to `Arg` operands; a literal-vs-literal
            // expression (`1 == 1.0`) is plain Rust and stays typed.
            let one = v(1);
            let two = v(2);
            let three = v(3);
            self.assertTrue(one == v(1.0));
            self.assertTrue(v(True) == one);
            // py2rs expands `a < b < c` to `a < b and b < c`
            self.assertTrue(one < two and two < three);
            self.assertFalse(one < two and two > three);
            self.assertTrue(not v([]));
            self.assertTrue(not v(""));
            self.assertTrue(not v(0));
            self.assertEqual(v([]) or "fallback", "fallback");
            self.assertEqual(v("x") and 5, 5);
            self.assertEqual(v(0) and 5, 0);
            // the bare operator on mixed types is `False` (no exception
            // channel); `sorted` / `min` / `max` raise like CPython.
            self.assertFalse(one < v("a"));
            with self.assertRaises(TypeError) {
                sorted([1, "a"]);
            }
            with self.assertRaises(TypeError) {
                max([1, "a"]);
            }
        }

        fn test_p24_is_in_and_ternary(&self) {
            let x = None;
            let y = 3;
            self.assertTrue(x is None);
            self.assertTrue(y is not None);
            self.assertTrue(2 in [1, 2, 3]);
            self.assertTrue("k" not in {"a": 1});
            self.assertTrue("ell" in "hello");
            let picked = "big" if y > 2 else "small";
            self.assertEqual(picked, "big");
        }

        fn test_p24_augmented_assignment_through_item_and_attr(&self) {
            let d = {"n": 1};
            d["n"] += 41;
            self.assertEqual(d["n"], 42);
            let mut xs = [1];
            xs += [2, 3];
            self.assertEqual(xs, [1, 2, 3]);
            let mut n = 1;
            n *= 5;
            self.assertEqual(n, 5);
        }

        fn test_p24_unpacking(&self) {
            let (k, v) = ("key", 1);
            self.assertEqual(k, "key");
            self.assertEqual(v, 1);
            let (a, b, c) = [1, 2, 3];
            self.assertEqual(a + b + c, 6);
            with self.assertRaises(ValueError) {
                let (p, q) = [1, 2, 3];
            }
        }

        fn test_p24_pool_chaining_shift_is_fallible(&self) {
            let cache = laila.DefaultPool(nickname="p24_cache");
            let origin = laila.DefaultPool(nickname="p24_origin");
            cache << origin;
            with self.assertRaises(ValueError) {
                origin << cache;  // a proxy cycle
            }
            // an integer-literal shift stays arithmetic
            self.assertEqual(8 << 20, 8388608);
        }
    }

    // ---- P25: indexing and slices ---------------------------------------
    py_class!(TestIndexing);
    impl TestIndexing {
        fn test_p25_getitem_errors_match_cpython(&self) {
            let d = {"a": 1};
            let xs = [1, 2, 3];
            with self.assertRaises(KeyError) as cm {
                let _ = d["zz"];
            }
            self.assertEqual(str(cm.exception), "'zz'");
            with self.assertRaises(IndexError) as cm2 {
                let _ = xs[9];
            }
            self.assertEqual(str(cm2.exception), "list index out of range");
            self.assertEqual(xs[-1], 3);
        }

        fn test_p25_slices_and_item_assignment(&self) {
            let xs = [0, 1, 2, 3, 4, 5];
            self.assertEqual(xs[1:], [1, 2, 3, 4, 5]);
            self.assertEqual(xs[:2], [0, 1]);
            self.assertEqual(xs[::2], [0, 2, 4]);
            self.assertEqual(xs[-2:], [4, 5]);
            self.assertEqual("hello"[1:-1], "ell");
            let d = {};
            d["k"] = [1];
            d["k"][0] = 9;
            self.assertEqual(d, {"k": [9]});
            del d["k"];
            self.assertEqual(d, {});
        }
    }

    // ---- P26: comprehensions, lambdas, ternaries --------------------------
    py_class!(TestComprehensions);
    impl TestComprehensions {
        fn test_p26_list_dict_set_comprehensions(&self) {
            let xs = [1, 2, 3, 4];
            self.assertEqual([x * x for x in xs if x % 2 == 0], [4, 16]);
            self.assertEqual({x: str(x) for x in xs[:2]}, {1: "1", 2: "2"});
            self.assertEqual(len({x % 2 for x in xs}), 2);
            self.assertEqual(sum(x for x in xs), 10);
            self.assertEqual([(i, c) for (i, c) in enumerate("ab")], [(0, "a"), (1, "b")]);
        }

        fn test_p26_comprehension_variable_shadows_outer_name(&self) {
            // Regression: the comprehension binder `entry` must not be
            // captured from the enclosing `for entry in ..`.
            let out = [];
            for entry in [[1, 2], [3]] {
                let doubled = [entry * 2 for entry in entry];
                out.append(doubled);
            }
            self.assertEqual(out, [[2, 4], [6]]);
        }

        fn test_p26_lambda_params_are_local(&self) {
            let x = 10;
            let f = lambda x, y: x + y;
            self.assertEqual(f(2, 1), 3);
            self.assertEqual(f(2, 5), 7);
            // the outer `x` is untouched by the parameter of the same name
            self.assertEqual(x, 10);
            let g = lambda: x * 2;
            self.assertEqual(g(), 20);
        }

        fn test_p26_sorted_with_key_lambda(&self) {
            let pairs = [("b", 2), ("a", 1), ("c", 0)];
            self.assertEqual(sorted(pairs, key=lambda p: p[1]), [("c", 0), ("a", 1), ("b", 2)]);
            self.assertEqual(sorted(pairs, key=lambda p: p[0], reverse=True)[0], ("c", 0));
            self.assertEqual(max(pairs, key=lambda p: p[1]), ("b", 2));
            self.assertEqual(min(pairs, key=lambda p: p[1]), ("c", 0));
            self.assertEqual(max([], default="empty"), "empty");
            self.assertEqual(max(3, 9), 9);
        }
    }

    // ---- P27: with -------------------------------------------------------
    py_class!(TestWith);
    impl TestWith {
        fn test_p27_with_opens_no_scope(&self) {
            with tempfile.TemporaryDirectory() as tmp {
                let p = os.path.join(tmp, "f.txt");
                with open(p, "w") as f {
                    f.write("hi");
                }
                with open(p) as f2 {
                    let text = f2.read();
                }
            }
            // names first bound inside the `with` stay visible after it
            self.assertEqual(text, "hi");
            self.assertFalse(os.path.exists(p));
        }

        fn test_p27_assert_raises_captures_the_exception(&self) {
            with self.assertRaises(ValueError) as cm {
                raise ValueError("boom");
            }
            self.assertEqual(str(cm.exception), "boom");
            with pytest.raises(KeyError) {
                let _ = {}["missing"];
            }
        }
    }

    // ---- P7 / P29: f-strings and the format mini-language ----------------
    py_class!(TestFormatting);
    impl TestFormatting {
        fn test_fstring_specs_apply_width_and_precision(&self) {
            let name = "pool";
            let n = 3;
            let ratio = 2.0 / 3.0;
            self.assertEqual(f "{name:>8}|", "    pool|");
            self.assertEqual(f "{name:<6}|", "pool  |");
            self.assertEqual(f "{n:03d}", "003");
            self.assertEqual(f "{ratio:.2f}", "0.67");
            self.assertEqual(f "{name!r}", "'pool'");
            self.assertEqual(f "{n * 2} items", "6 items");
            self.assertEqual(f "{{literal}}", "{literal}");
        }

        fn test_str_format_and_percent(&self) {
            // `str` methods are inherent on `Arg` (a literal receiver
            // stays a Rust `&str`): the template is a Python value.
            let tpl = v("{} and {name}");
            self.assertEqual(tpl.format(1, name="x"), "1 and x");
            let right = v("{:>4}");
            self.assertEqual(right.format("a"), "   a");
            self.assertEqual("%(k)05.1f" % {"k": 3.14159}, "003.1");
            let csv = v("a,b");
            self.assertEqual(csv.split(","), ["a", "b"]);
            self.assertEqual("-".join(["x", "y"]), "x-y");
        }
    }

    // ---- P16: try / except / finally; raise ------------------------------
    fn classify(x: Arg) -> Arg {
        try {
            if x is None {
                raise ValueError("none");
            }
            return x + 1;
        } except ValueError as e {
            return f "value:{e}";
        } except TypeError {
            return "type";
        }
    }

    py_class!(TestExceptions);
    impl TestExceptions {
        fn test_p16_except_clauses_bind_and_dispatch(&self) {
            self.assertEqual(classify(1), 2);
            self.assertEqual(classify(None), "value:none");
            self.assertEqual(classify("s"), "type");
        }

        fn test_p16_finally_runs_on_both_paths(&self) {
            let trace = [];
            try {
                trace.append("body");
            } finally {
                trace.append("finally");
            }
            try {
                try {
                    raise KeyError("k");
                } finally {
                    trace.append("inner-finally");
                }
            } except KeyError {
                trace.append("caught");
            }
            self.assertEqual(trace, ["body", "finally", "inner-finally", "caught"]);
        }

        fn test_p16_raise_from_laila_error_kinds(&self) {
            with self.assertRaises(NotImplementedError) {
                raise NotImplementedError("later");
            }
            with self.assertRaises(RuntimeError) as cm {
                raise RuntimeError("rt");
            }
            self.assertEqual(r#type(cm.exception).__name__, "RuntimeError");
        }
    }

    // ---- P28: builtins ---------------------------------------------------
    py_class!(TestBuiltins);
    impl TestBuiltins {
        fn test_p28_builtins_have_cpython_semantics(&self) {
            self.assertEqual(list(range(1, 7, 2)), [1, 3, 5]);
            self.assertEqual(list(zip([1, 2], "ab")), [(1, "a"), (2, "b")]);
            self.assertEqual(list(reversed([1, 2, 3])), [3, 2, 1]);
            self.assertEqual(min([3, 1, 2]), 1);
            self.assertEqual(max(3, 9), 9);
            self.assertEqual(abs(-2.5), 2.5);
            self.assertEqual(int("12") + 1, 13);
            self.assertEqual(float("0.5") * 2.0, 1.0);
            self.assertEqual(repr("x"), "'x'");
            self.assertEqual(str(None), "None");
            self.assertTrue(all([1, True, "a"]));
            self.assertFalse(any([0, None, ""]));
            self.assertTrue(isinstance(1, int));
            self.assertTrue(isinstance("s", (int, str)));
            self.assertFalse(isinstance(1.5, int));
            self.assertTrue(callable(lambda: 1));
            self.assertFalse(callable(3));
            self.assertEqual(r#type(1).__name__, "int");
        }

        fn test_p28_print_takes_sep_and_end(&self) {
            print("a", 1, sep="-", end="");
            print();
            self.assertEqual(len(str(uuid.uuid4())), 36);
        }
    }
}
