//! `npy` describe-block of `laila-js/tests/vectors/codecs.test.js`, one for
//! one, against `codecs.json` (numpy `np.save` output).

mod fixture;

use fixture::{decode, first_diff, fixture, ndarray, ndarrays};
use laila::_codecs::npy;
use laila::_compat::ndarray::NDArray;
use laila::prelude::*;
use laila::python::errors::ExcKind;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn header_of(b: &[u8]) -> String {
    b[..b.len().min(128)]
        .iter()
        .map(|c| *c as char)
        .collect::<String>()
        .replace("\n", "\\n")
}

#[test]
fn every_ndarray_case_is_byte_identical_and_round_trips() {
    let fx = fixture();
    let all = ndarrays();
    let mut checked = 0;
    for (name, r) in fx["cases"].as_object().unwrap() {
        let Some(payload) = r.get("npy").and_then(|v| v.as_str()) else {
            continue;
        };
        let (_, nd) = all
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("missing Rust construction for {name}"));
        let want = decode(payload);
        let got = npy::save(nd).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            got == want,
            "{name}: {}\n got header {:?}\nwant header {:?}",
            first_diff(&got, &want),
            header_of(&got),
            header_of(&want)
        );
        let back = npy::load(&want).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(back.dtype, nd.dtype, "{name}: dtype");
        assert_eq!(back.shape, nd.shape, "{name}: shape");
        assert_eq!(
            back.fortran_order, nd.fortran_order,
            "{name}: fortran_order"
        );
        assert!(back.array_equal(nd, true), "{name}: array_equal");
        assert_eq!(
            hex(&back.tobytes('C').unwrap()),
            hex(&nd.tobytes('C').unwrap()),
            "{name}: tobytes"
        );
        checked += 1;
    }
    assert_eq!(checked, 19, "every npy payload in the fixture was checked");
}

#[test]
fn fortran_tolist_is_c_ordered() {
    let want: Arg = vec![vec![1i64, 2, 3].into_arg(), vec![4i64, 5, 6].into_arg()].into_arg();
    assert!(ndarray("npy_fortran").tolist() == want);
    // and the saved payload carries the F-ordered bytes with fortran_order: True
    let saved = npy::save(&ndarray("npy_fortran")).unwrap();
    let text = header_of(&saved);
    assert!(text.contains("'fortran_order': True"), "{text}");
    let back = npy::load(&saved).unwrap();
    assert!(back.fortran_order);
    assert!(back.tolist() == want);
}

#[test]
fn rejects_bad_magic() {
    let e = npy::load(b"not an npy file at all").unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert!(e.message.contains("magic string is not correct"), "{e}");
    assert_eq!(
        e.message,
        "the magic string is not correct; expected b'\\x93NUMPY', got b'not an'"
    );
    let e = npy::load(b"short").unwrap_err();
    assert_eq!(
        e.message,
        "EOF: reading magic string, expected 8 bytes got 5"
    );
}

#[test]
fn header_texts_match_numpy() {
    // numpy writes `(5,)` for 1-d, `()` for 0-d and pads to a 64-byte boundary
    for (name, shape_text) in [
        ("npy_i32", "(5,)"),
        ("npy_scalar", "()"),
        ("npy_f64", "(2, 2)"),
        ("npy_f32_3d", "(2, 3, 4)"),
    ] {
        let saved = npy::save(&ndarray(name)).unwrap();
        let hlen = u16::from_le_bytes([saved[8], saved[9]]) as usize;
        assert_eq!((10 + hlen) % 64, 0, "{name}: aligned");
        assert_eq!(saved[10 + hlen - 1], b'\n', "{name}: newline terminated");
        let text: String = saved[10..10 + hlen].iter().map(|c| *c as char).collect();
        assert!(
            text.contains(&format!("'shape': {shape_text}, }}")),
            "{name}: {text:?}"
        );
        assert_eq!(&saved[..8], b"\x93NUMPY\x01\x00");
    }
    // version 2.0 only when the header outgrows 65535 bytes
    // (`'shape': (1, 1, ...)` with 20000 axes is ~60 KB: still 1.0; 25000 axes is not)
    let big = NDArray::zeros(vec![1; 20000], "|u1").unwrap();
    assert_eq!(&npy::save(&big).unwrap()[..8], b"\x93NUMPY\x01\x00");
    let huge = NDArray::zeros(vec![1; 25000], "|u1").unwrap();
    let saved = npy::save(&huge).unwrap();
    assert_eq!(&saved[..8], b"\x93NUMPY\x02\x00");
    let hlen = u32::from_le_bytes([saved[8], saved[9], saved[10], saved[11]]) as usize;
    assert_eq!((12 + hlen) % 64, 0);
    // np.load refuses headers over `max_header_size` (10000) unless told otherwise
    let e = npy::load(&saved).unwrap_err();
    assert!(
        e.message.starts_with(
            "Header info length (75124) is large and may not be safe to load securely.\n"
        ),
        "{e}"
    );
    let back = npy::load_with(&saved, usize::MAX).unwrap();
    assert_eq!(back.shape.len(), 25000);
    assert_eq!(back.raw_bytes(), &[0u8]);
}

#[test]
fn load_errors_match_numpy() {
    let good = npy::save(&ndarray("npy_f64")).unwrap();
    // unsupported version
    let mut v = good.clone();
    v[6] = 4;
    let e = npy::load(&v).unwrap_err();
    assert_eq!(
        e.message,
        "we only support format version (1,0), (2,0), and (3,0), not (4, 0)"
    );
    // truncated header length / header / data
    let e = npy::load(&good[..9]).unwrap_err();
    assert_eq!(
        e.message,
        "EOF: reading array header length, expected 2 bytes got 1"
    );
    let e = npy::load(&good[..40]).unwrap_err();
    assert_eq!(
        e.message,
        "EOF: reading array header, expected 118 bytes got 30"
    );
    let e = npy::load(&good[..good.len() - 1]).unwrap_err();
    assert_eq!(
        e.message,
        "EOF: reading array data, expected 32 bytes got 31"
    );
    // header that is not the literal numpy writes
    let bad = |text: &str| -> LailaError {
        let mut b = b"\x93NUMPY\x01\x00".to_vec();
        let mut h = text.as_bytes().to_vec();
        h.push(b'\n');
        b.extend_from_slice(&(h.len() as u16).to_le_bytes());
        b.extend_from_slice(&h);
        npy::load(&b).unwrap_err()
    };
    assert_eq!(
        bad("{'descr': '<f8', 'fortran_order': False").message,
        "Cannot parse header: \"{'descr': '<f8', 'fortran_order': False\\n\""
    );
    assert_eq!(
        bad("{'descr': '<f8', 'fortran_order': False, 'shape': 3}").message,
        "shape is not valid: 3"
    );
    assert_eq!(
        bad("{'descr': '<f8', 'fortran_order': 1, 'shape': (1,)}").message,
        "fortran_order is not a valid bool: 1"
    );
    assert_eq!(
        bad("{'descr': 'nope', 'fortran_order': False, 'shape': (1,)}").message,
        "descr is not a valid dtype descriptor: 'nope'"
    );
    assert_eq!(
        bad("{'descr': '|O', 'fortran_order': False, 'shape': (1,)}").message,
        "Object arrays cannot be loaded when allow_pickle=False"
    );
    assert_eq!(
        bad("{'descr': '<f8', 'fortran_order': False}").message,
        "Header does not contain the correct keys: ['descr', 'fortran_order']"
    );
    // alternate key order and double quotes are accepted (literal_eval semantics)
    let mut b = b"\x93NUMPY\x01\x00".to_vec();
    let h = b"{\"shape\": (2,), \"fortran_order\": False, \"descr\": \"<i4\"}\n";
    b.extend_from_slice(&(h.len() as u16).to_le_bytes());
    b.extend_from_slice(h);
    b.extend_from_slice(&[1, 0, 0, 0, 2, 0, 0, 0]);
    let back = npy::load(&b).unwrap();
    assert_eq!(back.dtype, "<i4");
    assert!(back.tolist() == vec![1i64, 2].into_arg());
    // big-endian descr survives the round trip with its byte order
    let be = ndarray("npy_be_i4");
    let back = npy::load(&npy::save(&be).unwrap()).unwrap();
    assert_eq!(back.dtype, ">i4");
    assert_eq!(back.raw_bytes(), be.raw_bytes());
}
