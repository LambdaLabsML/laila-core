//! The hasher behind every Python `dict` (`IndexMap`).
//!
//! `std` keeps the randomised `RandomState` (HashDoS-resistant like CPython's
//! siphash). `no_std` has no entropy source, so it uses FNV-1a.

#[cfg(feature = "std")]
pub type DictHasher = std::collections::hash_map::RandomState;

#[cfg(not(feature = "std"))]
pub type DictHasher = core::hash::BuildHasherDefault<Fnv1a>;

/// 64-bit FNV-1a.
#[derive(Clone, Copy, Debug)]
pub struct Fnv1a(u64);

impl Default for Fnv1a {
    fn default() -> Self {
        Fnv1a(0xcbf2_9ce4_8422_2325)
    }
}

impl core::hash::Hasher for Fnv1a {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}
