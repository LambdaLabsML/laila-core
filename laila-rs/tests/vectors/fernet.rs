//! Twin of the `fernet` describe-block of `laila-js/tests/vectors/codecs.test.js`.

mod fixture;

use fixture::{decode, fixture};
use laila::_codecs::fernet::{key_fingerprint, Fernet, MultiFernet};
use laila::python::errors::ExcKind;
use laila::python::object::bytes;
use laila::Arg;

fn fx() -> &'static serde_json::Value {
    &fixture()["fernet"]
}

fn fernet() -> Fernet {
    Fernet::from_key(&Arg::from(fx()["key"].as_str().unwrap())).unwrap()
}

fn iv() -> [u8; 16] {
    decode(fx()["iv_hex"].as_str().unwrap()).try_into().unwrap()
}

fn time() -> u64 {
    fx()["time"].as_u64().unwrap()
}

fn tokens() -> Vec<(String, String, String)> {
    fx()["tokens"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, pair)| {
            (
                name.clone(),
                pair[0].as_str().unwrap().to_string(),
                pair[1].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn kind(r: &laila::Result<Vec<u8>>) -> Option<ExcKind> {
    r.as_ref().err().map(|e| e.kind)
}

#[test]
fn tokens_are_byte_identical() {
    let f = fernet();
    let iv = iv();
    let toks = tokens();
    assert_eq!(toks.len(), 3, "fixture has hello / empty / block16");
    for (name, token, plain) in &toks {
        let got = f._encrypt_from_parts(plain.as_bytes(), time(), &iv);
        assert_eq!(String::from_utf8(got).unwrap(), *token, "token {name}");
        // decrypt accepts str and bytes tokens
        assert_eq!(
            f.decrypt_token(&Arg::from(token.as_str()), None).unwrap(),
            plain.as_bytes(),
            "decrypt str {name}"
        );
        assert_eq!(
            f.decrypt_token(&bytes(token.as_bytes()), None).unwrap(),
            plain.as_bytes(),
            "decrypt bytes {name}"
        );
        assert_eq!(
            f.extract_timestamp_of(&Arg::from(token.as_str())).unwrap(),
            time(),
            "timestamp {name}"
        );
    }
}

#[test]
fn ttl_tamper_bad_key() {
    let f = fernet();
    let (token, plain) = {
        let t = tokens().into_iter().find(|(n, _, _)| n == "hello").unwrap();
        (t.1, t.2)
    };
    let tok = Arg::from(token.as_str());
    // fixture time is 1700000000: expired long ago with ttl=10
    assert_eq!(
        kind(&f.decrypt_token(&tok, Some(10))),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        f.decrypt_token_at_time(&tok, Some(10), time() + 5).unwrap(),
        plain.as_bytes()
    );
    assert_eq!(
        f.decrypt_token_at_time(&tok, Some(10), time() + 10)
            .unwrap(),
        plain.as_bytes()
    );
    assert_eq!(
        kind(&f.decrypt_token_at_time(&tok, Some(10), time() + 11)),
        Some(ExcKind::InvalidToken)
    );
    // clock skew: token more than 60 s in the future
    assert_eq!(
        kind(&f.decrypt_token_at_time(&tok, Some(10), time() - 100)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        f.decrypt_token_at_time(&tok, Some(10), time() - 60)
            .unwrap(),
        plain.as_bytes()
    );
    assert_eq!(
        kind(&f.decrypt_token_at_time(&tok, Some(10), time() - 61)),
        Some(ExcKind::InvalidToken)
    );
    // decrypt_at_time needs a ttl
    let e = f.decrypt_token_at_time(&tok, None, time()).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(
        e.message,
        "decrypt_at_time() can only be used with a non-None ttl"
    );
    // tamper with the HMAC
    let mut t = token.clone().into_bytes();
    let n = t.len();
    t[n - 5] ^= 1;
    let r = f.decrypt_token(&bytes(&t), None);
    assert_eq!(kind(&r), Some(ExcKind::InvalidToken));
    assert_eq!(r.unwrap_err().message, "", "InvalidToken is raised bare");
    // tamper with the ciphertext (HMAC fails first)
    let mut t = token.clone().into_bytes();
    t[40] ^= 1;
    assert_eq!(
        kind(&f.decrypt_token(&bytes(&t), None)),
        Some(ExcKind::InvalidToken)
    );
    // wrong version byte / garbage / empty / truncated
    assert_eq!(
        kind(&f.decrypt_token(&Arg::from("hAAAAABlU_EA"), None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(&f.decrypt_token(&Arg::from("not base64!!"), None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(&f.decrypt_token(&Arg::from(""), None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(&f.decrypt_token(&Arg::from("gAAA"), None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(&f.decrypt_token(&Arg::from(&token[..token.len() - 8]), None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(
            &f.extract_timestamp_of(&Arg::from("gAAA"))
                .map(|_| Vec::new())
        ),
        Some(ExcKind::InvalidToken)
    );
    // token must be str or bytes
    assert_eq!(
        kind(&f.decrypt_token(&Arg::Int(1), None)),
        Some(ExcKind::TypeError)
    );
    assert_eq!(
        f.decrypt_token(&Arg::Int(1), None).unwrap_err().message,
        "token must be bytes or str"
    );
    // bad keys
    // bad base64 / wrong padding / 0 bytes / 33 bytes
    for bad in [
        "short",
        "",
        "cXdlcnR5dWlvcGFzZGZnaGprbHp4Y3Zibm0xMjM0NTY",
        "cXdlcnR5dWlvcGFzZGZnaGprbHp4Y3Zibm0xMjM0NTYx",
    ] {
        let e = Fernet::from_key(&Arg::from(bad)).unwrap_err();
        assert_eq!(e.kind, ExcKind::ValueError, "{bad:?}");
        assert_eq!(
            e.message,
            "Fernet key must be 32 url-safe base64-encoded bytes."
        );
    }
    assert!(Fernet::from_key(&bytes(fx()["key"].as_str().unwrap().as_bytes())).is_ok());
    // fresh encrypt / decrypt with the wall clock
    assert_eq!(
        f.decrypt_token(&bytes(f.encrypt_bytes(b"x")), None)
            .unwrap(),
        b"x"
    );
    assert_eq!(
        f.decrypt_token(&bytes(f.encrypt_bytes(b"")), None).unwrap(),
        b""
    );
    let long = vec![0xabu8; 1000];
    assert_eq!(
        f.decrypt_token(&bytes(f.encrypt_bytes(&long)), Some(60))
            .unwrap(),
        long
    );
    // generate_key -> usable key, 44 url-safe base64 chars
    let k = Fernet::generate_key_bytes();
    assert_eq!(k.len(), 44);
    assert!(k
        .iter()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'=')));
    let g = Fernet::from_key(&bytes(&k)).unwrap();
    assert_eq!(
        g.decrypt_token(&bytes(g.encrypt_bytes(b"hi")), None)
            .unwrap(),
        b"hi"
    );
    assert_ne!(
        Fernet::generate_key_bytes(),
        Fernet::generate_key_bytes(),
        "keys are random"
    );
    // MultiFernet: [fresh key, fixture key]
    let mf = MultiFernet::from_fernets(vec![
        Fernet::from_key(&bytes(Fernet::generate_key_bytes())).unwrap(),
        f.clone(),
    ])
    .unwrap();
    assert_eq!(mf.decrypt_token(&tok, None).unwrap(), plain.as_bytes());
    let rotated = bytes(mf.rotate_token(&tok).unwrap());
    assert_eq!(mf.decrypt_token(&rotated, None).unwrap(), plain.as_bytes());
    // now under the primary key
    assert_eq!(
        kind(&f.decrypt_token(&rotated, None)),
        Some(ExcKind::InvalidToken)
    );
    // rotate keeps the original (old) timestamp
    assert_eq!(
        kind(&mf.decrypt_token(&rotated, Some(10))),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        mf.decrypt_token_at_time(&rotated, Some(10), time() + 5)
            .unwrap(),
        plain.as_bytes()
    );
    assert_eq!(
        kind(&mf.decrypt_token_at_time(&rotated, Some(10), time() + 11)),
        Some(ExcKind::InvalidToken)
    );
    // encrypt goes through the primary key only
    let fresh = bytes(mf.encrypt_bytes_at_time(b"abc", time()));
    assert_eq!(mf.decrypt_token(&fresh, None).unwrap(), b"abc");
    assert_eq!(
        kind(&f.decrypt_token(&fresh, None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        mf.decrypt_token(&bytes(mf.encrypt_bytes(b"abc")), None)
            .unwrap(),
        b"abc"
    );
    // an unknown token fails every key
    let other = Fernet::from_key(&bytes(Fernet::generate_key_bytes())).unwrap();
    let foreign = bytes(other.encrypt_bytes(b"zzz"));
    assert_eq!(
        kind(&mf.decrypt_token(&foreign, None)),
        Some(ExcKind::InvalidToken)
    );
    assert_eq!(
        kind(&mf.rotate_token(&foreign)),
        Some(ExcKind::InvalidToken)
    );
    // non-InvalidToken errors propagate
    assert_eq!(
        kind(&mf.decrypt_token(&Arg::None, None)),
        Some(ExcKind::TypeError)
    );
    assert_eq!(
        kind(&mf.decrypt_token_at_time(&tok, None, time())),
        Some(ExcKind::ValueError)
    );
    // empty MultiFernet
    let e = MultiFernet::from_fernets(vec![]).unwrap_err();
    assert_eq!(e.kind, ExcKind::ValueError);
    assert_eq!(
        e.message,
        "MultiFernet requires at least one Fernet instance"
    );
}

#[test]
fn rotate_preserves_timestamp() {
    let f = fernet();
    let mf = MultiFernet::from_fernets(vec![f.clone()]).unwrap();
    let (_, token, plain) = tokens().into_iter().find(|(n, _, _)| n == "hello").unwrap();
    let rotated = mf.rotate_token(&Arg::from(token.as_str())).unwrap();
    assert_eq!(f.extract_timestamp_of(&bytes(&rotated)).unwrap(), time());
    assert_eq!(
        f.decrypt_token(&bytes(&rotated), None).unwrap(),
        plain.as_bytes()
    );
    // a different IV, so a different token text
    assert_ne!(rotated, token.into_bytes());
}

#[test]
fn fingerprint() {
    let key = fx()["key"].as_str().unwrap();
    assert_eq!(
        key_fingerprint(key.as_bytes()),
        fx()["fingerprint"].as_str().unwrap()
    );
    assert_eq!(key_fingerprint(key.as_bytes()), "5ae96487cb90");
}
