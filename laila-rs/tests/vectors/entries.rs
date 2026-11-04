//! Cross-implementation vectors shared with `laila-c` and `laila-js`
//! (`laila-c/tests/vectors/entries.json`): entries written by Python laila
//! as `Entry.as_dict()` plus their `global_id` / `uuid`. The Rust `Entry`
//! must hydrate every case (`Entry.from_dict`) to the same identity, emit
//! the identical `as_dict()` (key order, `_creation_timestamp`,
//! `constitution: null`), mint the same global id from `Entry.constant` /
//! `Entry.variable` with the same uuid / evolution, and round-trip through
//! `Entry._build_from_dict_sync(json.dumps(as_dict))`.

use std::sync::OnceLock;

use laila::_compat::pyjson;
use laila::entry::{Entry, EntryState};
use laila::prelude::*;

const ENTRIES_JSON: &str = include_str!("../../../laila-c/tests/vectors/entries.json");

fn vectors() -> &'static Vec<serde_json::Value> {
    static V: OnceLock<Vec<serde_json::Value>> = OnceLock::new();
    V.get_or_init(|| serde_json::from_str(ENTRIES_JSON).expect("entries.json parses"))
}

/// A fixture JSON value as the Python object laila would build from it.
fn py(v: &serde_json::Value) -> Arg {
    pyjson::loads(&v.to_string()).expect("fixture json")
}

/// `json.dumps(d)` with Python's defaults (key order preserved).
fn dumps(d: &Arg) -> String {
    pyjson::dumps(d, &Default::default()).expect("json.dumps")
}

#[test]
fn fixture_has_13_cases() {
    assert_eq!(vectors().len(), 13);
}

#[test]
fn from_dict_reproduces_identity_and_as_dict() {
    for v in vectors() {
        let name = v["name"].as_str().unwrap();
        let as_dict = py(&v["as_dict"]);
        let global_id = v["global_id"].as_str().unwrap();
        let uuid = v["uuid"].as_str().unwrap();

        let e = Entry::from_dict(as_dict.clone()).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(e.global_id(), global_id, "{name}: global_id");
        assert_eq!(e.uuid(), uuid, "{name}: uuid");
        assert_eq!(e.state(), EntryState::READY, "{name}: state");
        assert!(e.constitution().is_none(), "{name}: constitution");
        assert_eq!(
            e.creation_timestamp(),
            as_dict.__getitem__("_creation_timestamp").unwrap(),
            "{name}: creation_timestamp restored"
        );
        assert_eq!(e.scopes(), py!(["ENTRY"]), "{name}: scopes");
        assert_eq!(
            e.evolution(),
            as_dict.__getitem__("_evolution").unwrap(),
            "{name}: evolution"
        );

        // The payload is the Python value, with its Python type.
        let want = as_dict.__getitem__("payload").unwrap();
        let got = e.data().unwrap();
        assert_eq!(got, want, "{name}: payload");
        assert_eq!(got.type_name(), want.type_name(), "{name}: payload type");

        // ``as_dict()`` is byte-identical: same keys, same order, same values.
        let again = e.as_dict().unwrap();
        assert_eq!(again, as_dict, "{name}: as_dict");
        assert_eq!(
            dumps(&again),
            dumps(&as_dict),
            "{name}: json.dumps(as_dict)"
        );
        let fixture_keys: Vec<String> = v["as_dict"].as_object().unwrap().keys().cloned().collect();
        let our_keys: Vec<String> = again
            .__iter__()
            .unwrap()
            .iter()
            .map(|k| k.__str__())
            .collect();
        assert_eq!(our_keys, fixture_keys, "{name}: as_dict key order");

        // ``str`` / ``repr`` are the global id; equality follows identity.
        assert_eq!(e.__str__(), global_id);
        assert_eq!(e.__repr__(), global_id);
        assert_eq!(repr(&e), global_id);
        let twin = Entry::from_dict(as_dict.clone()).unwrap();
        assert_eq!(e, twin, "{name}: identity equality");
        assert!(!e.is(&twin));
        assert_eq!(e.__hash__().unwrap(), twin.__hash__().unwrap());
    }
}

#[test]
fn factories_mint_the_same_global_id() {
    for v in vectors() {
        let name = v["name"].as_str().unwrap();
        let as_dict = py(&v["as_dict"]);
        let global_id = v["global_id"].as_str().unwrap();
        let uuid = v["uuid"].as_str().unwrap();
        let evolution = as_dict.__getitem__("_evolution").unwrap();
        let payload = as_dict.__getitem__("payload").unwrap();

        let e = if evolution.is_none() {
            Entry::constant(args!(payload.clone(), uuid = uuid)).unwrap()
        } else {
            Entry::variable(args!(
                payload.clone(),
                uuid = uuid,
                evolution = evolution.clone()
            ))
            .unwrap()
        };
        assert_eq!(e.global_id(), global_id, "{name}: factory global_id");
        assert_eq!(e.uuid(), uuid, "{name}: factory uuid");
        assert_eq!(e.state(), EntryState::READY, "{name}: factory state");
        assert_eq!(e.data().unwrap(), payload, "{name}: factory payload");
        // ... and so does the raw constructor from the global id.
        let raw = Entry::new(args!(global_id = global_id, data = payload.clone())).unwrap();
        assert_eq!(raw.global_id(), global_id, "{name}: Entry(global_id=..)");
        assert_eq!(
            raw.evolution(),
            evolution,
            "{name}: Entry(global_id=..) evolution"
        );
        // A fresh stamp, not the fixture's (the factory is a new object).
        assert!(!e.creation_timestamp().is_none());
        // The factory's ``as_dict`` differs from the fixture only in the stamp.
        let d = e.as_dict().unwrap();
        d.__setitem__(
            "_creation_timestamp",
            as_dict.__getitem__("_creation_timestamp").unwrap(),
        )
        .unwrap();
        assert_eq!(dumps(&d), dumps(&as_dict), "{name}: factory as_dict");
    }
}

#[test]
fn build_from_json_text_round_trips() {
    for v in vectors() {
        let name = v["name"].as_str().unwrap();
        let as_dict = py(&v["as_dict"]);
        let global_id = v["global_id"].as_str().unwrap();
        let text = dumps(&as_dict);

        let e = Entry::_build_from_dict_sync(Arg::from(text.as_str()))
            .unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(e.global_id(), global_id, "{name}");
        assert_eq!(e.state(), EntryState::READY, "{name}");
        assert_eq!(
            e.data().unwrap(),
            as_dict.__getitem__("payload").unwrap(),
            "{name}: payload"
        );
        assert_eq!(
            dumps(&e.as_dict().unwrap()),
            text,
            "{name}: as_dict after json round trip"
        );

        // The dict form, the Python-spelled router and the scope builder agree.
        let d = Entry::_build_from_dict_sync(as_dict.clone()).unwrap();
        assert_eq!(dumps(&d.as_dict().unwrap()), text, "{name}");
        let routed = Entry::_build_from_dict(args!(as_dict.clone(), asynchronous = true)).unwrap();
        assert_eq!(
            dumps(&routed.as_dict().unwrap()),
            text,
            "{name}: asynchronous"
        );
        let by_scope =
            laila::entry::constitution::build_maps::build_by_scope(args!(Arg::from(text.as_str())))
                .unwrap();
        assert_eq!(
            dumps(&by_scope.as_entry().unwrap().as_dict().unwrap()),
            text,
            "{name}: build_by_scope"
        );
        // A live entry passes through untouched.
        assert!(Entry::_build_from_dict_sync(e.clone()).unwrap().is(&e));
    }
}

#[test]
fn from_dict_validation_texts() {
    let good = py(&vectors()[0]["as_dict"]);
    for (bad_uuid, text) in [
        (
            Arg::from("not-a-uuid"),
            "Entry.from_dict: invalid _uuid 'not-a-uuid'",
        ),
        (Arg::Int(1), "Entry.from_dict: invalid _uuid 1"),
        (Arg::None, "Entry.from_dict: invalid _uuid None"),
    ] {
        let d = good.copy().unwrap();
        d.__setitem__("_uuid", bad_uuid).unwrap();
        let e = Entry::from_dict(d).unwrap_err();
        assert_eq!(e.kind, ExcKind::ValueError);
        assert_eq!(e.__str__(), text);
    }
    for (bad, text) in [
        (Arg::Int(-1), "Entry.from_dict: invalid _evolution -1"),
        (Arg::from("3"), "Entry.from_dict: invalid _evolution '3'"),
        (Arg::Bool(true), "Entry.from_dict: invalid _evolution True"),
        (Arg::Float(1.5), "Entry.from_dict: invalid _evolution 1.5"),
    ] {
        let d = good.copy().unwrap();
        d.__setitem__("_evolution", bad).unwrap();
        let e = Entry::from_dict(d).unwrap_err();
        assert_eq!(e.kind, ExcKind::ValueError);
        assert_eq!(e.__str__(), text);
    }
    // A missing ``_uuid`` is the dict's ``KeyError``; an unknown state the enum's.
    let e = Entry::from_dict(py!({})).unwrap_err();
    assert_eq!(e.kind, ExcKind::KeyError);
    assert_eq!(e.__str__(), "'_uuid'");
    let d = good.copy().unwrap();
    d.__setitem__("_state", "NONEXISTENT_STATE").unwrap();
    let e = Entry::from_dict(d).unwrap_err();
    assert_eq!(e.kind, ExcKind::KeyError);
    assert_eq!(e.__str__(), "'NONEXISTENT_STATE'");
    // A record without a stamp is stamped now (ISO-8601, milliseconds, +00:00).
    let d = good.copy().unwrap();
    d.__delitem__("_creation_timestamp").unwrap();
    let e = Entry::from_dict(d).unwrap();
    let stamp = e.creation_timestamp().__str__();
    assert!(stamp.ends_with("+00:00") && stamp.len() == 29, "{stamp}");
    // Invalid JSON text is ``ValueError("Invalid JSON string")``.
    let e = Entry::_build_from_dict_sync(Arg::from("{not valid json")).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(e.__str__(), "Invalid JSON string");
    let e = Entry::_build_from_dict_sync(Arg::Int(1)).unwrap_err();
    assert_eq!(e.kind, ExcKind::RuntimeError);
    assert_eq!(e.__str__(), "Invalid input for entry build.");
}
