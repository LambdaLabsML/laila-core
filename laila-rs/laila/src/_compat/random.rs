//! Randomness source (`os.urandom`, `uuid4`, `Fernet.generate_key`, IVs).
//!
//! `std`: the operating system's CSPRNG (`getrandom`). `no_std`: the HAL
//! registers an entropy source with [`set_source`]; until it does, a
//! deterministic xorshift stream is used so that firmware without an RNG
//! still runs (documented as **not** cryptographically random).

use crate::python::sync::Mutex;

/// A platform entropy source: fills the buffer with random bytes.
pub type Source = fn(&mut [u8]);

static SOURCE: Mutex<Option<Source>> = Mutex::new(None);

/// Register the platform entropy source (bare-metal HALs call this once).
pub fn set_source(f: Source) {
    *SOURCE.lock() = Some(f);
}

/// `os.urandom(len(buf))` into `buf`.
pub fn fill(buf: &mut [u8]) {
    if let Some(f) = *SOURCE.lock() {
        f(buf);
        return;
    }
    #[cfg(feature = "std")]
    {
        if getrandom::fill(buf).is_ok() {
            return;
        }
    }
    fallback(buf);
}

/// `os.urandom(n)`.
pub fn urandom(n: usize) -> alloc::vec::Vec<u8> {
    let mut v = alloc::vec![0u8; n];
    fill(&mut v);
    v
}

fn fallback(buf: &mut [u8]) {
    static STATE: Mutex<u64> = Mutex::new(0x9E37_79B9_7F4A_7C15);
    let mut s = STATE.lock();
    for b in buf.iter_mut() {
        // xorshift64*
        let mut x = *s;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *s = x;
        *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8;
    }
}
