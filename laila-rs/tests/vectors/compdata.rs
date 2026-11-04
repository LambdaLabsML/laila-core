//! Cross-implementation vectors shared with `laila-c` and `laila-js`
//! (`laila-c/tests/vectors/compdata.json`): serialized entries written by
//! Python laila (`Entry.serialize(transformation_base64)`). The Rust
//! compdata / constitution layers must rebuild every payload through its
//! constitution codes *and* re-serialise the recovered value to the
//! identical payload text and code list.
//!
//! Worked at the compdata / constitution level first, then again through
//! `Entry`: `Entry._build_from_dict_sync(serialized)` must recover the value
//! and re-`serialize(transformation_base64)` to the identical dict.

use std::sync::OnceLock;

use laila::_compat::pyjson;
use laila::entry::compdata::transformation::{Base64, Transformation, TransformationSequence};
use laila::entry::compdata::ComputationalData;
use laila::entry::constitution::{Constitution, SimpleConstitution};
use laila::entry::{transformation_base64, Entry, EntryState};
use laila::prelude::*;

const COMPDATA_JSON: &str = include_str!("../../../laila-c/tests/vectors/compdata.json");

fn vectors() -> &'static Vec<serde_json::Value> {
    static V: OnceLock<Vec<serde_json::Value>> = OnceLock::new();
    V.get_or_init(|| {
        // Keep the per-user constitution secret out of the real home.
        let dir = std::env::temp_dir().join(format!("laila-rs-compdata-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("LAILA_CONSTITUTION_KEY_FILE", dir.join("constitution.key"));
        serde_json::from_str(COMPDATA_JSON).expect("compdata.json parses")
    })
}

/// A fixture JSON value as the Python object laila would build from it.
fn py(v: &serde_json::Value) -> Arg {
    pyjson::loads(&v.to_string()).expect("fixture json")
}

fn b64(s: &str) -> Vec<u8> {
    laila::_codecs::base64::b64decode(&Arg::from(s), None, false).expect("fixture base64")
}

fn codes_of(v: &serde_json::Value) -> Vec<String> {
    v["serialized"]["constitution"]["codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect()
}

/// The trust gate must accept the Python-written constitution dicts as-is
/// (no `_hmac`: pipeline codes are accepted by template).
fn constitution_of(v: &serde_json::Value) -> SimpleConstitution {
    let d = py(&v["serialized"]["constitution"]);
    match Constitution::from_dict(d).unwrap() {
        Some(Constitution::Simple(s)) => s,
        other => panic!(
            "{}: expected a simple constitution, got {other:?}",
            v["name"]
        ),
    }
}

#[test]
fn fixture_has_22_cases() {
    assert_eq!(vectors().len(), 22);
}

#[test]
fn rebuild_every_payload_through_its_constitution() {
    for v in vectors() {
        let name = v["name"].as_str().unwrap();
        let check = v["check"].as_str().unwrap();
        let codes = codes_of(v);
        let constitution = constitution_of(v);
        assert_eq!(constitution.codes_vec(), codes, "{name}: codes round-trip");

        if check == "none" {
            assert!(v["serialized"]["payload"].is_null(), "{name}");
            assert!(codes.is_empty(), "{name}");
            // An empty chain is the identity.
            assert_eq!(constitution.build(Arg::None).unwrap(), Arg::None);
            continue;
        }

        let payload = Arg::from(v["serialized"]["payload"].as_str().unwrap());
        let cur = constitution
            .build(payload.clone())
            .unwrap_or_else(|e| panic!("{name}: build failed: {e}"));

        match check {
            "value" => {
                let want = py(&v["value"]);
                assert_eq!(cur, want, "{name}: {} != {}", repr(&cur), repr(&want));
                // Python types survive: ``True`` is a bool, not 1.
                assert_eq!(cur.type_name(), want.type_name(), "{name}: type");
            }
            "bytes" => {
                assert_eq!(
                    cur,
                    laila::python::object::bytes(b64(v["b64"].as_str().unwrap())),
                    "{name}"
                );
            }
            "nested_bytes" => {
                let inner = cur.__getitem__(v["key"].as_str().unwrap()).unwrap();
                assert_eq!(
                    inner,
                    laila::python::object::bytes(b64(v["b64"].as_str().unwrap())),
                    "{name}"
                );
            }
            "bignum_unsupported" => {
                assert!(matches!(cur, Arg::Long(_)), "{name}: {}", repr(&cur));
            }
            "numpy" => {
                let arr = cur
                    .as_ndarray()
                    .unwrap_or_else(|| panic!("{name}: not an ndarray"));
                assert_eq!(arr.dtype, v["dtype"].as_str().unwrap(), "{name}: dtype");
                let shape: Vec<usize> = v["shape"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|d| d.as_u64().unwrap() as usize)
                    .collect();
                assert_eq!(arr.shape, shape, "{name}: shape");
                assert_eq!(
                    arr.tobytes('C').unwrap(),
                    b64(v["raw_b64"].as_str().unwrap()),
                    "{name}: raw bytes"
                );
            }
            other => panic!("unknown check {other}"),
        }

        // Forward identity: value -> ComputationalData.serialize(Args::new()) ->
        // TransformationSequence([Base64()]).forward == stored payload/codes.
        let cd = ComputationalData::new(cur.clone()).unwrap();
        let (bytes_, payload_code) = cd.serialize(Args::new()).unwrap();
        let seq = TransformationSequence::from_vec(vec![Transformation::new(
            Base64::new(Args::new()).unwrap(),
        )]);
        let (text, mut inverse) = seq.forward(bytes_).unwrap();
        inverse.push(payload_code);
        assert_eq!(text, payload, "{name}: re-serialised payload");
        assert_eq!(inverse, codes, "{name}: re-serialised codes");

        // The dynamic spelling ``TransformationSequence(transformations=[Base64()])``.
        let seq2 = TransformationSequence::new(args!(
            transformations = vec![Base64::new(Args::new()).unwrap().into_arg()].into_arg()
        ))
        .unwrap();
        assert_eq!(
            seq2.forward(cd.serialize(Args::new()).unwrap().0)
                .unwrap()
                .0,
            payload
        );
    }
}

#[test]
fn constitution_dict_round_trips_with_hmac() {
    for v in vectors() {
        let c = constitution_of(v);
        let d = c.as_dict().unwrap();
        assert_eq!(d.__getitem__("_kind").unwrap(), "simple");
        assert!(matches!(d.__getitem__("_hmac").unwrap(), Arg::Str(_)));
        let again = Constitution::from_dict(d).unwrap().unwrap();
        assert_eq!(again, Constitution::Simple(c));
    }
}

/// The per-check comparison of a recovered payload against the fixture.
fn check_value(v: &serde_json::Value, cur: &Arg) {
    let name = v["name"].as_str().unwrap();
    match v["check"].as_str().unwrap() {
        "none" => assert!(cur.is_none(), "{name}"),
        "value" => {
            let want = py(&v["value"]);
            assert_eq!(*cur, want, "{name}");
            assert_eq!(cur.type_name(), want.type_name(), "{name}: type");
        }
        "bytes" => assert_eq!(
            *cur,
            laila::python::object::bytes(b64(v["b64"].as_str().unwrap())),
            "{name}"
        ),
        "nested_bytes" => assert_eq!(
            cur.__getitem__(v["key"].as_str().unwrap()).unwrap(),
            laila::python::object::bytes(b64(v["b64"].as_str().unwrap())),
            "{name}"
        ),
        "bignum_unsupported" => assert!(matches!(cur, Arg::Long(_)), "{name}"),
        "numpy" => {
            let arr = cur
                .as_ndarray()
                .unwrap_or_else(|| panic!("{name}: not an ndarray"));
            assert_eq!(arr.dtype, v["dtype"].as_str().unwrap(), "{name}: dtype");
            assert_eq!(
                arr.tobytes('C').unwrap(),
                b64(v["raw_b64"].as_str().unwrap()),
                "{name}: raw bytes"
            );
        }
        other => panic!("unknown check {other}"),
    }
}

/// The same vectors through `Entry`: the serialized dict Python wrote is
/// hydrated and built by `Entry._build_from_dict_sync`, its `.data` is the
/// original value, and `serialize(transformation_base64)` writes the
/// identical dict back (payload text, codes, identity, timestamp).
#[test]
fn entry_build_from_dict_and_reserialize() {
    for v in vectors() {
        let name = v["name"].as_str().unwrap();
        let serialized = py(&v["serialized"]);
        let codes = codes_of(v);

        let e = Entry::_build_from_dict_sync(serialized.clone())
            .unwrap_or_else(|err| panic!("{name}: build failed: {err}"));
        assert_eq!(e.state(), EntryState::READY, "{name}");
        assert!(e.constitution().is_none(), "{name}: constitution consumed");
        assert!(
            !e.locally_modified(),
            "{name}: a build is not a user change"
        );
        assert_eq!(
            e.global_id(),
            laila::basics::definitions::identifiable_object::to_global_id(
                Some(v["serialized"]["_uuid"].as_str().unwrap()),
                Some(&["ENTRY".to_string()]),
                v["serialized"]["_evolution"].as_i64(),
                None,
            ),
            "{name}: identity"
        );
        assert_eq!(
            e.creation_timestamp(),
            serialized.__getitem__("_creation_timestamp").unwrap(),
            "{name}: creation_timestamp"
        );
        let data = e.data().unwrap();
        check_value(v, &data);

        // ``from_dict`` alone leaves the stored payload encoded and the
        // simple constitution attached; ``_build_inplace`` is the same build.
        let raw = Entry::from_dict(serialized.clone()).unwrap();
        let stored = serialized.__getitem__("payload").unwrap();
        if stored.is_none() {
            // No payload yet and a constitution attached: ``.data`` refuses
            // (``EntryNotBuiltError``, a ``RuntimeError``) until built.
            let err = raw.data().unwrap_err();
            assert_eq!(err.kind, ExcKind::EntryNotBuiltError, "{name}");
            assert!(err.kind.is_subclass_of(ExcKind::RuntimeError));
            assert!(err
                .__str__()
                .starts_with(&format!("Entry {} is not built.", raw.global_id())));
        } else {
            assert_eq!(
                raw.data().unwrap(),
                stored,
                "{name}: stored payload, still encoded"
            );
        }
        assert!(
            matches!(raw.constitution_opt(), Some(Constitution::Simple(_))),
            "{name}: simple constitution attached"
        );
        raw._build_inplace().unwrap();
        assert_eq!(raw.data().unwrap(), data, "{name}: _build_inplace");

        // Forward identity: ``serialize(transformation_base64)`` reproduces
        // the dict Python wrote, byte for byte.
        let again = e
            .serialize(args!(transformations = transformation_base64()))
            .unwrap();
        assert_eq!(
            again.__getitem__("payload").unwrap(),
            serialized.__getitem__("payload").unwrap(),
            "{name}: re-serialised payload"
        );
        let again_codes: Vec<String> = again
            .__getitem__("constitution")
            .unwrap()
            .__getitem__("codes")
            .unwrap()
            .__iter__()
            .unwrap()
            .iter()
            .map(|c| c.__str__())
            .collect();
        assert_eq!(again_codes, codes, "{name}: re-serialised codes");
        for key in [
            "_uuid",
            "_evolution",
            "_scopes",
            "_state",
            "_creation_timestamp",
        ] {
            assert_eq!(
                again.__getitem__(key).unwrap(),
                serialized.__getitem__(key).unwrap(),
                "{name}: {key}"
            );
        }
        assert_eq!(
            again
                .__getitem__("constitution")
                .unwrap()
                .__getitem__("_kind")
                .unwrap(),
            "simple"
        );
        // The dict Python wrote carries no ``_hmac`` (pipeline codes are
        // trusted by template); ours does, and both hydrate to the same value.
        let back = Entry::_build_from_dict_sync(again).unwrap();
        assert_eq!(back.data().unwrap(), data, "{name}: second round trip");

        // The JSON text form and the scope router take the same path.
        let text = pyjson::dumps(&serialized, &Default::default()).unwrap();
        let from_text = Entry::_build_from_dict_sync(Arg::from(text.as_str())).unwrap();
        check_value(v, &from_text.data().unwrap());
        let routed =
            laila::entry::constitution::build_maps::build_by_scope(args!(serialized)).unwrap();
        check_value(v, &routed.as_entry().unwrap().data().unwrap());
    }
}
