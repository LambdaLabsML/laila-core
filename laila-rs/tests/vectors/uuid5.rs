//! `generate_uuid_from_nickname` / `to_global_id` against CPython's
//! `uuid.uuid5(namespace, nickname)` (`laila-c/tests/vectors/uuid5.json`).
//!
//! The fixture pins the namespace *UUID* the cases were generated under
//! (`laila._active_namespace = uuid.UUID(ns)`); the test activates it with
//! `set_active_namespace_uuid` first and restores the previous one afterwards
//! (`laila.set_active_namespace(key)` derives `uuid5(NAMESPACE_DNS, key)`).

use laila::basics::definitions::identifiable_object::{
    generate_uuid_from_nickname, process_global_id, to_global_id, Identity,
    _LAILA_IDENTIFIABLE_OBJECT,
};
use laila::prelude::*;

const UUID5_JSON: &str = include_str!("../../../laila-c/tests/vectors/uuid5.json");

#[test]
fn nickname_uuid5_and_global_id_match_cpython() {
    let _lock = laila::python::unittest::test_lock();
    let fixture: serde_json::Value = serde_json::from_str(UUID5_JSON).expect("uuid5.json parses");
    let namespace = fixture["namespace"].as_str().expect("namespace");
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());

    let previous = laila.get_active_namespace().__str__();
    laila::_compat::uuid_ns::set_active_namespace_uuid(namespace).expect("valid namespace");
    assert_eq!(laila.get_active_namespace().__str__(), namespace);

    for case in cases {
        let nickname = case["nickname"].as_str().unwrap();
        let uuid = case["uuid"].as_str().unwrap();
        let global_id = case["global_id"].as_str().unwrap();

        // free function, class method, typed identity
        assert_eq!(generate_uuid_from_nickname(nickname), uuid, "{nickname}");
        assert_eq!(
            _LAILA_IDENTIFIABLE_OBJECT
                .generate_uuid_from_nickname(nickname)
                .unwrap(),
            uuid
        );
        let id = Identity::new(None, None, None, Some(nickname), &["ENTRY"]);
        assert_eq!(id.uuid, uuid);
        assert_eq!(id.global_id(), global_id);
        assert_eq!(
            to_global_id(None, Some(&["ENTRY".to_string()]), None, Some(nickname)),
            global_id
        );
        // the Python-spelled static method
        let gid = _LAILA_IDENTIFIABLE_OBJECT
            .to_global_id(args!(nickname = nickname, scopes = py!(["ENTRY"])))
            .unwrap();
        assert_eq!(gid, global_id);
        // round trip
        let parsed = process_global_id(global_id).unwrap();
        assert_eq!(parsed.uuid, uuid);
        assert_eq!(parsed.scopes, vec!["ENTRY".to_string()]);
        assert_eq!(parsed.evolution, None);
        // entries built from the nickname carry the same identity
        let e = laila
            .constant(args!(data = 1, nickname = nickname))
            .unwrap();
        assert_eq!(e.global_id(), global_id);
    }

    laila::_compat::uuid_ns::set_active_namespace_uuid(&previous).expect("restore namespace");
}
