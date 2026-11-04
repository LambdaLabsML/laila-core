//! Byte-identity of `laila::_codecs` against CPython / msgpack-python / numpy /
//! cryptography reference output (`codecs.json`, regenerated with
//! `cd laila-js && npm run vectors`).

mod fixture;

use fixture::*;
use laila::_codecs::{base64 as b64, zlib};
use laila::prelude::*;

fn b64s(b: &[u8]) -> String {
    String::from_utf8(b64::b2a_base64(b)).unwrap()
}

#[test]
fn b64encode_altchars_and_std() {
    let all: Vec<u8> = (0..=255u8).collect();
    let fx = &fixture()["base64"];
    assert_eq!(
        String::from_utf8(b64::b64encode(&all, Some(b"-_")).unwrap()).unwrap(),
        fx["alt_0_255"].as_str().unwrap()
    );
    assert_eq!(b64s(&all), fx["std_0_255"].as_str().unwrap());
}

#[test]
fn b64decode_semantics_binascii() {
    assert_eq!(
        b64::b64decode(&Arg::from("aGV s\nbG8="), None, false).unwrap(),
        b"hello"
    );
    let e = b64::b64decode(&Arg::from("aGV s"), None, true).unwrap_err();
    assert_eq!(e.kind, laila::python::errors::ExcKind::BinasciiError);
    assert_eq!(
        b64::b64decode(&Arg::from("aGVsbG8"), None, false)
            .unwrap_err()
            .message,
        "Incorrect padding"
    );
    assert!(b64::b64decode(&Arg::from("aGVsb"), None, false)
        .unwrap_err()
        .message
        .contains("cannot be 1 more than a multiple of 4"));
    assert!(b64::b64decode(&Arg::from("\u{e9}"), None, false)
        .unwrap_err()
        .message
        .contains("only ASCII"));
}

#[test]
fn zlib_compress_levels_are_byte_identical() {
    let z = &fixture()["zlib"];
    let zin = z["input"].as_str().unwrap().as_bytes();
    assert_eq!(
        b64s(&zlib::compress(zin, -1, 15).unwrap()),
        z["default"].as_str().unwrap()
    );
    assert_eq!(
        b64s(&zlib::compress(zin, 9, 15).unwrap()),
        z["level9"].as_str().unwrap()
    );
    assert_eq!(
        b64s(&zlib::compress(zin, 1, 15).unwrap()),
        z["level1"].as_str().unwrap()
    );
    assert_eq!(
        b64s(&zlib::compress(b"abc", 0, 15).unwrap()),
        z["level0_abc"].as_str().unwrap()
    );
    assert_eq!(
        b64s(&zlib::compress(zin, -1, -15).unwrap()),
        z["raw_wbits"].as_str().unwrap()
    );
}

#[test]
fn zlib_decompress() {
    let z = &fixture()["zlib"];
    let zin = z["input"].as_str().unwrap().as_bytes();
    assert_eq!(
        zlib::decompress(&zlib::compress(zin, -1, 15).unwrap(), 15, 16384).unwrap(),
        zin
    );
    assert_eq!(
        zlib::decompress(&zlib::compress(zin, -1, -15).unwrap(), -15, 16384).unwrap(),
        zin
    );
    let e = zlib::decompress(b"nope", 15, 16384).unwrap_err();
    assert_eq!(e.kind, laila::python::errors::ExcKind::ZlibError);
    assert!(zlib::decompress(&zlib::compress(zin, -1, 15).unwrap()[..20], 15, 16384).is_err());
    // large payloads in the fixture decode through laila's own zlib
    assert_eq!(
        decode(fixture()["cases"]["str_70000"]["pickle"].as_str().unwrap()).len(),
        70009
    );
}

#[test]
fn fixture_constructions_cover_every_case() {
    let names: Vec<String> = fixture()["cases"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let have: Vec<String> = cases()
        .into_iter()
        .map(|(n, _)| n)
        .chain(ndarrays().into_iter().map(|(n, _)| n))
        .collect();
    let missing: Vec<&String> = names
        .iter()
        .filter(|n| !have.contains(n) && n.as_str() != "str_surrogate")
        .collect();
    assert!(
        missing.is_empty(),
        "missing Rust constructions: {missing:?}"
    );
}
