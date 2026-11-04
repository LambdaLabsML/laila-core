//! `f64` rounding/power helpers that exist on both `std` and `no_std`
//! (`core` has no `floor`/`powf` until `core_float_math` lands).

#[cfg(feature = "std")]
mod imp {
    #[inline]
    pub fn floor(x: f64) -> f64 {
        x.floor()
    }
    #[inline]
    pub fn trunc(x: f64) -> f64 {
        x.trunc()
    }
    #[inline]
    pub fn round(x: f64) -> f64 {
        x.round()
    }
    #[inline]
    pub fn powf(x: f64, y: f64) -> f64 {
        x.powf(y)
    }
}

#[cfg(not(feature = "std"))]
mod imp {
    #[inline]
    pub fn floor(x: f64) -> f64 {
        libm::floor(x)
    }
    #[inline]
    pub fn trunc(x: f64) -> f64 {
        libm::trunc(x)
    }
    #[inline]
    pub fn round(x: f64) -> f64 {
        libm::round(x)
    }
    #[inline]
    pub fn powf(x: f64, y: f64) -> f64 {
        libm::pow(x, y)
    }
}

pub use imp::{floor, powf, round, trunc};

/// Python's `float.is_integer()` core: the fractional part.
#[inline]
pub fn fract(x: f64) -> f64 {
    x - trunc(x)
}
