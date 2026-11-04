//! `DotMap` behaviour, checked against the Python `dotmap` 1.3.30 library
//! (expected strings are what `python3 -c "from dotmap import DotMap; ..."`
//! prints on this machine) and the JS unit test (`tests/unit/compat.test.js`,
//! `test('DotMap')`).

use laila::_compat::dotmap::{deep_eq, is_dotmap, DotMap};
use laila::python::builtins::{dict, isinstance};
use laila::python::prelude::*;
use laila::python::ExcKind;

fn err_text(e: &LailaError) -> (ExcKind, String) {
    (e.kind, e.message.clone())
}

#[test]
fn js_unit_test_translated() {
    // const d = new DotMap(); d.a.b = 1; d.c = { x: { y: 2 } };
    let d = DotMap::default();
    d.__getattr__("a").unwrap().__setattr__("b", 1).unwrap();
    d.__setattr__("c", py!({"x": {"y": 2}})).unwrap();
    assert_eq!(d.__getattr__("a").unwrap().__getattr__("b").unwrap(), 1);
    assert!(is_dotmap(&d.__getattr__("a").unwrap()));
    // plain dict stored as-is on set
    assert!(matches!(d.__getattr__("c").unwrap(), Arg::Dict(_)));
    assert_eq!(d.to_dict(), py!({"a": {"b": 1}, "c": {"x": {"y": 2}}}));
    assert_eq!(d.__len__(), 2);
    assert!(!d.__contains__("zz").unwrap());
    assert_eq!(d.get("zz", ()).unwrap(), Arg::None);
    assert!(!d.__contains__("zz").unwrap(), "get does not autovivify");
    assert_eq!(d.__str__(), "DotMap(a=DotMap(b=1), c={'x': {'y': 2}})");
    let e2 = DotMap::new(py!({"p": {"q": 1}})).unwrap();
    assert!(is_dotmap(&e2.__getattr__("p").unwrap()));
    assert!(e2.__eq__(DotMap::new(py!({"p": {"q": 1}})).unwrap()));
    assert!(e2.__eq__(py!({"p": {"q": 1}})));
    assert!(DotMap::default().__eq__(py!({})));
    assert!(!DotMap::default().__eq__(py!([])));
    assert!(deep_eq(&e2.clone().into_arg(), &py!({"p": {"q": 1}})));
    assert!(!DotMap::default().__bool__());
    assert_eq!(d.keys(), py!(["a", "c"]));
    d.__delattr__("c").unwrap();
    assert!(!d.__contains__("c").unwrap());
}

#[test]
fn constructor_and_repr_match_dotmap() {
    let dd = DotMap::new(py!({"a": 1, "b": 2, "subD": {"c": 3, "d": 4}})).unwrap();
    assert_eq!(dd.__repr__(), "DotMap(a=1, b=2, subD=DotMap(c=3, d=4))");
    assert_eq!(
        dd.copy().unwrap().__repr__(),
        "DotMap(a=1, b=2, subD=DotMap(c=3, d=4))"
    );
    assert_eq!(
        DotMap::fromkeys(py!([1, 2, 3]), "a").unwrap().__repr__(),
        "DotMap(1='a', 2='a', 3='a')"
    );
    assert_eq!(dd.get("a", ()).unwrap(), 1);
    assert_eq!(dd.get("f", 33).unwrap(), 33);
    assert_eq!(dd.get("f", ()).unwrap(), Arg::None);
    assert!(dd.has_key("a").unwrap());
    dd.update(args!(py!([("rat", 5), ("bum", 4)]), dog = 7, cat = 9))
        .unwrap();
    dd.update(py!({"lol": 1, "ba": 2})).unwrap();
    assert_eq!(
        dd.__repr__(),
        "DotMap(a=1, b=2, subD=DotMap(c=3, d=4), rat=5, bum=4, dog=7, cat=9, lol=1, ba=2)"
    );
    assert_eq!(
        dd.__iter__(),
        py!(["a", "b", "subD", "rat", "bum", "dog", "cat", "lol", "ba"])
            .__iter__()
            .unwrap()
    );
    assert!(dd.__contains__("a").unwrap());
    assert!(!dd.__contains__("c").unwrap());
    dd.__getattr__("c").unwrap().__setattr__("a", 1).unwrap();
    assert_eq!(
        dd.to_dict().__repr__(),
        "{'a': 1, 'b': 2, 'subD': {'c': 3, 'd': 4}, 'rat': 5, 'bum': 4, 'dog': 7, 'cat': 9, 'lol': 1, 'ba': 2, 'c': {'a': 1}}"
    );
    assert_eq!(
        dd.values().__repr__(),
        "[1, 2, DotMap(c=3, d=4), 5, 4, 7, 9, 1, 2, DotMap(a=1)]"
    );
    assert_eq!(dd.items().__len__().unwrap(), 10);
    assert_eq!(
        dd.items().__getitem__(2).unwrap().__repr__(),
        "('subD', DotMap(c=3, d=4))"
    );
    assert_eq!(
        DotMap::new(args!(name = "Steve", job = "programmer"))
            .unwrap()
            .__repr__(),
        "DotMap(name='Steve', job='programmer')"
    );
    // am.some.deep.path.cuz.we = 'can'
    let am = DotMap::default();
    am.__getattr__("some")
        .unwrap()
        .__getattr__("deep")
        .unwrap()
        .__getattr__("path")
        .unwrap()
        .__getattr__("cuz")
        .unwrap()
        .__setattr__("we", "can")
        .unwrap();
    assert_eq!(
        am.__repr__(),
        "DotMap(some=DotMap(deep=DotMap(path=DotMap(cuz=DotMap(we='can')))))"
    );
    // mixed values: lists convert their dict items (one level), tuples don't, scalars repr as Python
    let m = DotMap::new(py!({"a": [{"b": 1}, 2, [{"c": 3}]], "t": ({"z": 1},), "n": None, "s": "it's", "f": 1.5, "b": True})).unwrap();
    assert_eq!(
        m.__repr__(),
        "DotMap(a=[DotMap(b=1), 2, [{'c': 3}]], t=({'z': 1},), n=None, s=\"it's\", f=1.5, b=True)"
    );
    assert_eq!(m.to_dict().__repr__(), "{'a': [{'b': 1}, 2, [{'c': 3}]], 't': ({'z': 1},), 'n': None, 's': \"it's\", 'f': 1.5, 'b': True}");
    assert!(matches!(m.__getattr__("t").unwrap(), Arg::Tuple(_)));
    assert!(is_dotmap(
        &m.__getattr__("a").unwrap().__getitem__(0).unwrap()
    ));
    assert!(matches!(
        m.__getattr__("a")
            .unwrap()
            .__getitem__(2)
            .unwrap()
            .__getitem__(0)
            .unwrap(),
        Arg::Dict(_)
    ));
    assert_eq!(
        DotMap::new(py!({"a": [[{"b": 1}]]})).unwrap().__repr__(),
        "DotMap(a=[[{'b': 1}]])"
    );
    assert_eq!(
        DotMap::new(py!({1: 2, "x": 3})).unwrap().__repr__(),
        "DotMap(1=2, x=3)"
    );
    assert_eq!(
        DotMap::new(py!({"s": "x\ny", "b": laila::python::object::bytes(b"ab"), "l": [1, "a"]}))
            .unwrap()
            .__repr__(),
        "DotMap(s='x\\ny', b=b'ab', l=[1, 'a'])"
    );
    // keyword values and later assignments are stored as-is (plain dicts)
    let k = DotMap::new(args!(a = py!({"b": 1}))).unwrap();
    assert!(matches!(k.__getattr__("a").unwrap(), Arg::Dict(_)));
    assert_eq!(k.__repr__(), "DotMap(a={'b': 1})");
    k.__setattr__("z", py!({"q": 1})).unwrap();
    assert!(matches!(k.__getattr__("z").unwrap(), Arg::Dict(_)));
    let u = DotMap::default();
    u.update(py!({"a": {"b": 1}})).unwrap();
    assert!(matches!(u.__getattr__("a").unwrap(), Arg::Dict(_)));
    // iterable of pairs / non-iterable / None
    assert_eq!(
        DotMap::new(py!([("a", {"b": 1}), ("c", 2)]))
            .unwrap()
            .__repr__(),
        "DotMap(a=DotMap(b=1), c=2)"
    );
    assert_eq!(DotMap::new(5).unwrap().__repr__(), "DotMap()");
    assert_eq!(DotMap::new(()).unwrap().__repr__(), "DotMap()");
    assert_eq!(DotMap::new(Args::new()).unwrap().__repr__(), "DotMap()");
    assert_eq!(
        err_text(&DotMap::new("ab").unwrap_err()),
        (
            ExcKind::ValueError,
            "not enough values to unpack (expected 2, got 1)".into()
        )
    );
    assert_eq!(
        err_text(&DotMap::new(py!([1])).unwrap_err()),
        (
            ExcKind::TypeError,
            "cannot unpack non-iterable int object".into()
        )
    );
    assert_eq!(
        err_text(&DotMap::new(py!([(1, 2, 3)])).unwrap_err()),
        (
            ExcKind::ValueError,
            "too many values to unpack (expected 2, got 3)".into()
        )
    );
    // copy is independent (nested maps rebuilt)
    let o = DotMap::new(py!({"x": {"y": 1}})).unwrap();
    let c2 = o.copy().unwrap();
    c2.__getattr__("x").unwrap().__setattr__("y", 2).unwrap();
    assert_eq!(o.__repr__(), "DotMap(x=DotMap(y=1))");
    assert_eq!(c2.__repr__(), "DotMap(x=DotMap(y=2))");
    // a nested DotMap value is copied by the constructor (Python), not aliased
    let inner = DotMap::new(args!(q = 1)).unwrap();
    let outer = DotMap::new(py!({"p": inner.clone()})).unwrap();
    outer.__getattr__("p").unwrap().__setattr__("q", 2).unwrap();
    assert_eq!(inner.__getattr__("q").unwrap(), 1);
    assert_eq!(outer.__getattr__("p").unwrap().__getattr__("q").unwrap(), 2);
    // empty() is `not any(keys)`
    assert!(DotMap::new(py!({0: 1})).unwrap().empty());
    assert!(!DotMap::new(py!({"a": 1})).unwrap().empty());
    assert!(DotMap::default().empty());
    assert_eq!(
        DotMap::new(args!(a = 1, b = 2)).unwrap().items().__repr__(),
        "[('a', 1), ('b', 2)]"
    );
    assert_eq!(
        DotMap::new(args!(a = 1, b = 2)).unwrap().keys().__repr__(),
        "['a', 'b']"
    );
}

#[test]
fn equality_matches_dotmap() {
    let ab = DotMap::new(py!({"a": 1, "b": 2})).unwrap();
    let ba = DotMap::new(py!({"b": 2, "a": 1})).unwrap();
    assert!(
        !ab.__eq__(ba.clone()),
        "DotMap vs DotMap is order-sensitive (OrderedDict)"
    );
    assert!(
        ab.__eq__(py!({"b": 2, "a": 1})),
        "DotMap vs dict is order-insensitive"
    );
    assert!(ab != ba);
    assert!(ab == ab.clone());
    let nested = DotMap::new(py!({"a": {"b": 1}})).unwrap();
    assert!(nested.__eq__(py!({"a": {"b": 1}})));
    assert!(deep_eq(&py!({"a": {"b": 1}}), &nested.clone().into_arg()));
    assert!(DotMap::new(py!({"a": [{"b": 1}]}))
        .unwrap()
        .__eq__(py!({"a": [{"b": 1}]})));
    assert!(!nested.__eq__(py!({"a": {"b": 2}})));
    assert!(!nested.__eq__(py!({"a": {"b": 1}, "c": 0})));
    assert!(!nested.__eq__(1));
    assert!(!nested.__eq__("DotMap(a=DotMap(b=1))"));
    assert!(nested.__eq__(py!({"a": DotMap::new(py!({"b": 1})).unwrap()})));
    // through Arg (Object vs Object)
    let x: Arg = nested.clone().into_arg();
    let y: Arg = DotMap::new(py!({"a": {"b": 1}})).unwrap().into_arg();
    assert!(x == y);
    assert!(x != ab.clone().into_arg());
    // laila-core's Arg == Arg cannot ask the object about a dict operand;
    // deep_eq is the DotMap-aware equality for that case.
    assert!(deep_eq(&x, &py!({"a": {"b": 1}})));
    assert!(deep_eq(&py!([x.clone(), 1]), &py!([{"a": {"b": 1}}, 1])));
    assert!(!deep_eq(&py!([x.clone(), 1]), &py!([{"a": {"b": 1}}, 2])));
    assert!(deep_eq(
        &py!(("a", x.clone())),
        &py!(("a", {"a": {"b": 1}}))
    ));
    assert!(deep_eq(
        &py!({"k": x.clone()}),
        &py!({"k": {"a": {"b": 1}}})
    ));
    assert!(!deep_eq(
        &py!({"k": x.clone()}),
        &py!({"j": {"a": {"b": 1}}})
    ));
    assert!(deep_eq(&py!(1), &py!(1.0)));
}

#[test]
fn cycles() {
    let c = DotMap::default();
    c.__setattr__("me", c.clone()).unwrap();
    assert_eq!(c.__repr__(), "DotMap(me=DotMap(...))");
    let td = c.to_dict();
    assert!(td.__getitem__("me").unwrap().is(&td));
    // a dict shared twice in the source becomes one shared DotMap
    let shared = py!({"v": 1});
    let d = DotMap::new(py!({"a": shared.clone(), "b": shared.clone()})).unwrap();
    assert!(d.__getattr__("a").unwrap().is(&d.__getattr__("b").unwrap()));
    // dotmap's `seen` set is shared across siblings, so the second reference prints as `DotMap(...)`
    assert_eq!(d.__repr__(), "DotMap(a=DotMap(v=1), b=DotMap(...))");
    assert_eq!(d.to_dict().__repr__(), "{'a': {'v': 1}, 'b': {'v': 1}}");
    assert!(!d
        .to_dict()
        .__getitem__("a")
        .unwrap()
        .is(&d.to_dict().__getitem__("b").unwrap()));
    let td = d.to_dict();
    assert!(
        td.__getitem__("a")
            .unwrap()
            .is(&td.__getitem__("b").unwrap()),
        "toDict keeps the sharing"
    );
    // cyclic source dict
    let cyc = py!({"x": 1});
    cyc.__setitem__("self", cyc.clone()).unwrap();
    let dc = DotMap::new(cyc).unwrap();
    assert_eq!(dc.__repr__(), "DotMap(x=1, self=DotMap(...))");
    assert!(dc
        .__getattr__("self")
        .unwrap()
        .downcast_ref::<DotMap>()
        .unwrap()
        .is(&dc));
}

#[test]
fn dynamic_and_method_masking() {
    let nd = DotMap::new(args!(py!({"a": {"b": 1}}), _dynamic = false)).unwrap();
    assert!(!nd._dynamic());
    assert!(
        !nd.__getattr__("a")
            .unwrap()
            .downcast_ref::<DotMap>()
            .unwrap()
            ._dynamic(),
        "_dynamic propagates"
    );
    assert_eq!(nd.__getattr__("_dynamic").unwrap(), false);
    assert_eq!(
        err_text(&nd.__getattr__("zz").unwrap_err()),
        (
            ExcKind::AttributeError,
            "'DotMap' object has no attribute 'zz'".into()
        )
    );
    assert_eq!(
        err_text(&nd.__getitem__("zz").unwrap_err()),
        (ExcKind::KeyError, "'zz'".into())
    );
    assert_eq!(
        err_text(&nd.__getitem__(7).unwrap_err()),
        (ExcKind::KeyError, "7".into())
    );
    assert!(!nd.__contains__("zz").unwrap());
    assert_eq!(
        err_text(&DotMap::default().__getattr__("__foo__").unwrap_err()),
        (ExcKind::AttributeError, "__foo__".into())
    );
    // the canary never autovivifies
    assert!(DotMap::default()
        .__getitem__("_ipython_canary_method_should_not_exist_")
        .is_err());
    // toggling the flag at runtime
    nd.__setattr__("_dynamic", true).unwrap();
    assert!(is_dotmap(&nd.__getattr__("zz").unwrap()));
    // copy() resets the options to the defaults, like dotmap
    let nd2 = DotMap::new(args!(_dynamic = false)).unwrap();
    assert!(nd2.copy().unwrap()._dynamic());
    // _prevent_method_masking
    assert_eq!(
        err_text(
            &DotMap::new(args!(py!({"keys": 1}), _prevent_method_masking = true)).unwrap_err()
        ),
        (ExcKind::KeyError, "'\"keys\" is reserved'".into())
    );
    let pm = DotMap::new(args!(_prevent_method_masking = true)).unwrap();
    assert!(pm._prevent_method_masking());
    assert_eq!(
        err_text(&pm.__setattr__("keys", 1).unwrap_err()),
        (ExcKind::KeyError, "'\"keys\" is reserved'".into())
    );
    pm.__setitem__("keys", 1).unwrap(); // item assignment is allowed
    assert_eq!(pm.__repr__(), "DotMap(keys=1)");
    // d.keys is the bound method, never the stored value
    let bound = pm.__getattr__("keys").unwrap();
    assert!(matches!(bound, Arg::Callable(_)));
    assert_eq!(bound.__call__(Args::new()).unwrap(), py!(["keys"]));
    assert!(
        DotMap::default().__setattr__("keys", 1).is_ok(),
        "masking allowed by default"
    );
}

#[test]
fn dict_api() {
    let p = DotMap::new(args!(a = 1)).unwrap();
    assert_eq!(p.pop("a", ()).unwrap(), 1);
    assert_eq!(p.pop("a", ()).unwrap(), Arg::None);
    assert_eq!(p.pop("a", 5).unwrap(), 5);
    assert_eq!(p.__repr__(), "DotMap()");
    let p = DotMap::new(args!(a = 1, b = 2)).unwrap();
    assert_eq!(p.popitem().unwrap().__repr__(), "('b', 2)");
    assert_eq!(p.__repr__(), "DotMap(a=1)");
    assert_eq!(
        err_text(&DotMap::default().popitem().unwrap_err()),
        (ExcKind::KeyError, "'dictionary is empty'".into())
    );
    assert_eq!(
        DotMap::new(args!(a = 1))
            .unwrap()
            .setdefault("a", 2)
            .unwrap(),
        1
    );
    let s = DotMap::new(args!(a = 1)).unwrap();
    assert_eq!(s.setdefault("b", 2).unwrap(), 2);
    assert_eq!(s.__repr__(), "DotMap(a=1, b=2)");
    let p = DotMap::new(args!(a = 1)).unwrap();
    p.__delattr__("a").unwrap();
    assert_eq!(p.__repr__(), "DotMap()");
    assert_eq!(
        err_text(&p.__delitem__("a").unwrap_err()),
        (ExcKind::KeyError, "'a'".into())
    );
    assert_eq!(
        err_text(&p.__delattr__("a").unwrap_err()),
        (ExcKind::KeyError, "'a'".into())
    );
    assert_eq!(DotMap::default().__add__(5).unwrap(), 5);
    assert_eq!(
        err_text(&DotMap::new(args!(a = 1)).unwrap().__add__(5).unwrap_err()),
        (
            ExcKind::TypeError,
            "unsupported operand type(s) for +: 'DotMap' and 'int'".into()
        )
    );
    let c = DotMap::new(args!(a = 1, b = 2)).unwrap();
    c.clear();
    assert_eq!(c.__len__(), 0);
    assert!(!c.__bool__());
    // update: too many positionals / pairs / keywords / DotMap source
    assert_eq!(
        err_text(&c.update(args!(1, 2)).unwrap_err()),
        (
            ExcKind::TypeError,
            "update expected at most 1 argument, got 2".into()
        )
    );
    c.update(DotMap::new(args!(z = 26)).unwrap()).unwrap();
    assert_eq!(c.__repr__(), "DotMap(z=26)");
    // unhashable keys
    assert_eq!(
        err_text(&DotMap::default().__getitem__(py!([1])).unwrap_err()),
        (ExcKind::TypeError, "unhashable type: 'list'".into())
    );
    assert!(DotMap::default().__setitem__(py!([1]), 1).is_err());
    assert!(DotMap::default().get(py!({}), ()).is_err());
    assert_eq!(
        err_text(&DotMap::default().__hash__().unwrap_err()),
        (ExcKind::TypeError, "unhashable type: 'DotMap'".into())
    );
    assert!(dict(Args::new())
        .unwrap()
        .__setitem__(DotMap::default(), 1)
        .is_err());
    // _map attribute reads a shallow plain-dict copy
    let d = DotMap::new(py!({"a": {"b": 1}})).unwrap();
    let m = d.__getattr__("_map").unwrap();
    assert!(matches!(m, Arg::Dict(_)));
    assert!(is_dotmap(&m.__getitem__("a").unwrap()));
}

#[test]
fn bound_methods_through_getattr() {
    let d = DotMap::new(py!({"a": 1, "b": {"c": 2}})).unwrap();
    let a: Arg = d.clone().into_arg();
    let call = |name: &str, args: Args| a.__getattr__(name).unwrap().__call__(args).unwrap();
    assert_eq!(call("get", args!("a")), 1);
    assert_eq!(call("get", args!("zz")), Arg::None);
    assert_eq!(call("get", args!("zz", 3)), 3);
    assert_eq!(call("get", args!("zz", default = 4)), 4);
    assert!(
        !d.__contains__("zz").unwrap(),
        "get through getattr does not autovivify"
    );
    assert_eq!(call("keys", Args::new()), py!(["a", "b"]));
    assert_eq!(
        call("items", Args::new()).__repr__(),
        "[('a', 1), ('b', DotMap(c=2))]"
    );
    assert_eq!(call("values", Args::new()).__repr__(), "[1, DotMap(c=2)]");
    assert_eq!(call("toDict", Args::new()), py!({"a": 1, "b": {"c": 2}}));
    assert_eq!(call("has_key", args!("a")), true);
    assert_eq!(call("empty", Args::new()), false);
    assert_eq!(call("setdefault", args!("n", 9)), 9);
    assert_eq!(call("pop", args!("n")), 9);
    assert_eq!(call("pop", args!("n", "dflt")), "dflt");
    call("update", args!(py!({"q": 1}), r = 2));
    assert_eq!(d.__repr__(), "DotMap(a=1, b=DotMap(c=2), q=1, r=2)");
    assert_eq!(call("popitem", Args::new()).__repr__(), "('r', 2)");
    let cp = call("copy", Args::new());
    assert!(is_dotmap(&cp) && !cp.is(&a) && cp == a);
    call("clear", Args::new());
    assert_eq!(d.__len__(), 0);
    // CPython binding errors
    let e = a
        .__getattr__("get")
        .unwrap()
        .__call__(Args::new())
        .unwrap_err();
    assert_eq!(
        e.message,
        "get() missing 1 required positional argument: 'key'"
    );
    let e = a
        .__getattr__("keys")
        .unwrap()
        .__call__(args!(1))
        .unwrap_err();
    assert_eq!(
        e.message,
        "keys() takes 0 positional arguments but 1 was given"
    );
    // reserved names without a Rust twin are AttributeErrors, not keys
    assert!(a.__getattr__("bannerStr").is_err());
    assert!(!d.__contains__("bannerStr").unwrap());
}

#[test]
fn arg_object_protocol() {
    let d = DotMap::new(py!({"a": 1})).unwrap();
    let a: Arg = d.clone().into_arg();
    assert_eq!(a.type_name(), "DotMap");
    assert_eq!(a.__repr__(), "DotMap(a=1)");
    assert_eq!(a.__str__(), "DotMap(a=1)");
    assert!(isinstance::<DotMap>(&a));
    assert!(!isinstance::<DotMap>(&py!({})));
    assert!(DotMap::downcast(&a).unwrap().is(&d));
    assert!(!DotMap::downcast(&a).unwrap().is(&d.copy().unwrap()));
    // attribute access through Arg (autovivifying)
    a.__getattr__("x").unwrap().__setattr__("y", 2).unwrap();
    assert_eq!(a.__repr__(), "DotMap(a=1, x=DotMap(y=2))");
    assert_eq!(a.__getattr__("x").unwrap().__getattr__("y").unwrap(), 2);
    assert_eq!(
        d.__repr__(),
        "DotMap(a=1, x=DotMap(y=2))",
        "the Arg aliases the handle"
    );
    a.__setattr__("a", py!([1, 2])).unwrap();
    assert_eq!(a.__getattr__("a").unwrap(), py!([1, 2]));
    assert_eq!(
        err_text(&a.__hash__().unwrap_err()),
        (ExcKind::TypeError, "unhashable type: 'DotMap'".into())
    );
    assert!(a.__getattr__("__dict__").is_err());
    // PyArg: `arg(&d)` shares the store
    let b = arg(&d);
    b.__setattr__("z", 0).unwrap();
    assert!(d.__contains__("z").unwrap());
    assert!(a == b, "equal by value");
    assert!(
        a.is(&b),
        "separate Arc wrappers of one store are the same Python object (`__is__`)"
    );
    assert_eq!(DotMap::downcast(&a).unwrap(), DotMap::downcast(&b).unwrap());
    assert_eq!(format!("{d:?}"), d.__str__());
}
