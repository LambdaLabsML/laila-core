//! `uuid.uuid4()` / `uuid.uuid5()` in Python's canonical hyphenated form.

use alloc::string::String;

/// 16 random bytes from the platform entropy source (`_compat::random`).
pub fn random_bytes16() -> [u8; 16] {
    let mut out = [0u8; 16];
    super::random::fill(&mut out);
    out
}

fn hyphenate(b: &[u8; 16]) -> String {
    alloc::format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

/// `str(uuid.uuid4())`
pub fn uuid4() -> String {
    let mut b = random_bytes16();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    hyphenate(&b)
}

/// Parse a hyphenated UUID into 16 bytes (`None` if malformed).
pub fn parse(s: &str) -> Option<[u8; 16]> {
    let hex: alloc::vec::Vec<u8> = s.bytes().filter(|b| *b != b'-').collect();
    if hex.len() != 32 || s.len() != 36 || s.bytes().filter(|b| *b == b'-').count() != 4 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

/// `_UUID_RE.match(s)`: 8-4-4-4-12 lowercase/uppercase hex.
pub fn is_valid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// `str(uuid.uuid5(namespace, name))`
pub fn uuid5(namespace: &[u8; 16], name: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(namespace);
    h.update(name.as_bytes());
    let d = h.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&d[..16]);
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    hyphenate(&b)
}

/// `uuid.NAMESPACE_DNS`
pub const NAMESPACE_DNS: [u8; 16] = [
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
];
