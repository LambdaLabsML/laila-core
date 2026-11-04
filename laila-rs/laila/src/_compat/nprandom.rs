//! `numpy.random.default_rng` -- `SeedSequence` + `PCG64` + `Generator`.
//!
//! Byte-identical to numpy (`numpy/random/_bit_generator.pyx`,
//! `_pcg64.pyx`, `src/pcg64/pcg64.h`, `_generator.pyx`): the same seed
//! yields the same stream, so a Python test that builds a payload from
//! `np.random.default_rng(seed).random(n)` is reproduced bit for bit.
//!
//! Only the surface laila's tests use is here: `default_rng(seed)`,
//! `Generator.random(size=None)`, `Generator.integers(low, high=None,
//! size=None)` (the 64-bit masked-rejection path numpy uses by default)
//! and `Generator.bytes(length)`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::ndarray::NDArray;
use crate::python::args::Args;
use crate::python::errors::{LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyObject};
use crate::python::sync::Mutex;

// ---------------------------------------------------------------------------
// SeedSequence

const DEFAULT_POOL_SIZE: usize = 4;
const INIT_A: u32 = 0x43b0_d7e5;
const MULT_A: u32 = 0x931e_8875;
const INIT_B: u32 = 0x8b51_f9dd;
const MULT_B: u32 = 0x58f3_8ded;
const MIX_MULT_L: u32 = 0xca01_f9dd;
const MIX_MULT_R: u32 = 0x4973_f715;
const XSHIFT: u32 = 16;

/// `np.random.SeedSequence(entropy, spawn_key=())`: the entropy pool.
#[derive(Clone, Debug)]
pub struct SeedSequence {
    pool: Vec<u32>,
}

/// `_coerce_to_uint32_array(x)`: a non-negative `int` as little-endian
/// 32-bit words (at least one), a sequence element-wise.
fn coerce_to_uint32_array(x: &Arg) -> Result<Vec<u32>> {
    match x {
        Arg::None => Ok(Vec::new()),
        Arg::Int(i) => {
            if *i < 0 {
                return Err(LailaError::value_error("expected non-negative integer"));
            }
            let mut v = *i as u64;
            let mut out = Vec::new();
            loop {
                out.push((v & 0xffff_ffff) as u32);
                v >>= 32;
                if v == 0 {
                    break;
                }
            }
            Ok(out)
        }
        Arg::Bool(b) => Ok(alloc::vec![u32::from(*b)]),
        Arg::Long(big) => {
            if big.sign() == num_bigint::Sign::Minus {
                return Err(LailaError::value_error("expected non-negative integer"));
            }
            Ok(big.to_u32_digits().1)
        }
        Arg::Str(s) => {
            // A string is parsed as an integer (`SeedSequence("123")`).
            let big: num_bigint::BigInt = s.parse().map_err(|_| {
                LailaError::type_error("SeedSequence expects int or sequence of int")
            })?;
            coerce_to_uint32_array(&Arg::from_bigint(big))
        }
        other => {
            let items = other.__iter__()?;
            let mut out = Vec::new();
            for item in items {
                out.extend(coerce_to_uint32_array(&item)?);
            }
            Ok(out)
        }
    }
}

fn hashmix(mut value: u32, hash_const: &mut u32) -> u32 {
    value ^= *hash_const;
    *hash_const = hash_const.wrapping_mul(MULT_A);
    value = value.wrapping_mul(*hash_const);
    value ^= value >> XSHIFT;
    value
}

fn mix(x: u32, y: u32) -> u32 {
    let mut result = MIX_MULT_L
        .wrapping_mul(x)
        .wrapping_sub(MIX_MULT_R.wrapping_mul(y));
    result ^= result >> XSHIFT;
    result
}

impl SeedSequence {
    /// `SeedSequence(entropy)` (no spawn key, the default pool size).
    pub fn new(entropy: &Arg) -> Result<SeedSequence> {
        let entropy_array = match entropy {
            // `None`: fresh OS entropy (`secrets.randbits(128)`).
            Arg::None => {
                let raw = crate::_compat::random::urandom(16);
                raw.chunks(4)
                    .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                    .collect()
            }
            other => coerce_to_uint32_array(other)?,
        };
        let mut pool = alloc::vec![0u32; DEFAULT_POOL_SIZE];
        Self::mix_entropy(&mut pool, &entropy_array);
        Ok(SeedSequence { pool })
    }

    /// Mix in the given entropy to the pool (numpy's `mix_entropy`).
    fn mix_entropy(mixer: &mut [u32], entropy_array: &[u32]) {
        let mut hash_const = INIT_A;
        // Add in the entropy up to the pool size.
        for i in 0..mixer.len() {
            mixer[i] = if i < entropy_array.len() {
                hashmix(entropy_array[i], &mut hash_const)
            } else {
                hashmix(0, &mut hash_const)
            };
        }
        // Mix all bits together so late bits can affect earlier bits.
        for i_src in 0..mixer.len() {
            for i_dst in 0..mixer.len() {
                if i_src != i_dst {
                    let h = hashmix(mixer[i_src], &mut hash_const);
                    mixer[i_dst] = mix(mixer[i_dst], h);
                }
            }
        }
        // Add any remaining entropy, mixing each new entropy word with each
        // pool word.
        for &word in entropy_array.iter().skip(mixer.len()) {
            for slot in mixer.iter_mut() {
                let h = hashmix(word, &mut hash_const);
                *slot = mix(*slot, h);
            }
        }
    }

    /// `generate_state(n_words, dtype=np.uint32)`.
    pub fn generate_state_u32(&self, n_words: usize) -> Vec<u32> {
        let mut hash_const = INIT_B;
        let mut state = Vec::with_capacity(n_words);
        for i_dst in 0..n_words {
            let mut data_val = self.pool[i_dst % self.pool.len()];
            data_val ^= hash_const;
            hash_const = hash_const.wrapping_mul(MULT_B);
            data_val = data_val.wrapping_mul(hash_const);
            data_val ^= data_val >> XSHIFT;
            state.push(data_val);
        }
        state
    }

    /// `generate_state(n_words, dtype=np.uint64)`: the 32-bit words viewed
    /// as little-endian 64-bit pairs.
    pub fn generate_state_u64(&self, n_words: usize) -> Vec<u64> {
        self.generate_state_u32(n_words * 2)
            .chunks(2)
            .map(|c| u64::from(c[0]) | (u64::from(c[1]) << 32))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// PCG64 (128-bit LCG, XSL-RR 128/64 output)

const PCG_DEFAULT_MULTIPLIER_128: u128 =
    ((2_549_297_995_355_413_924u128) << 64) + 4_865_540_595_714_422_341u128;

/// `np.random.PCG64` (the bit generator behind `default_rng`).
#[derive(Clone, Debug)]
pub struct PCG64 {
    state: u128,
    inc: u128,
}

impl PCG64 {
    /// `PCG64(seed)`: seeded through `SeedSequence(seed).generate_state(4,
    /// np.uint64)` -- words 0..2 are the state, 2..4 the stream.
    pub fn new(seed: &Arg) -> Result<PCG64> {
        let seq = SeedSequence::new(seed)?;
        Ok(Self::from_seed_sequence(&seq))
    }

    fn from_seed_sequence(seq: &SeedSequence) -> PCG64 {
        let s = seq.generate_state_u64(4);
        // `PCG_128BIT_CONSTANT(high, low)`: `seed[0]` high, `seed[1]` low.
        let initstate = (u128::from(s[0]) << 64) | u128::from(s[1]);
        let initseq = (u128::from(s[2]) << 64) | u128::from(s[3]);
        let mut rng = PCG64 { state: 0, inc: 0 };
        rng.srandom(initstate, initseq);
        rng
    }

    /// `pcg_setseq_128_srandom_r`.
    fn srandom(&mut self, initstate: u128, initseq: u128) {
        self.state = 0;
        self.inc = (initseq << 1) | 1;
        self.step();
        self.state = self.state.wrapping_add(initstate);
        self.step();
    }

    /// `pcg_setseq_128_step_r`.
    #[inline]
    fn step(&mut self) {
        self.state = self
            .state
            .wrapping_mul(PCG_DEFAULT_MULTIPLIER_128)
            .wrapping_add(self.inc);
    }

    /// `pcg_output_xsl_rr_128_64`.
    #[inline]
    fn output(state: u128) -> u64 {
        let rot = (state >> 122) as u32;
        let xor = ((state >> 64) as u64) ^ (state as u64);
        xor.rotate_right(rot)
    }

    /// `next_uint64`: step, then output.
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.step();
        Self::output(self.state)
    }

    /// `next_uint32`: numpy buffers one 64-bit draw into two 32-bit halves
    /// (low half first).
    #[inline]
    pub fn next_u32(&mut self, has_uint32: &mut bool, uinteger: &mut u32) -> u32 {
        if *has_uint32 {
            *has_uint32 = false;
            return *uinteger;
        }
        let next = self.next_u64();
        *has_uint32 = true;
        *uinteger = (next >> 32) as u32;
        next as u32
    }

    /// `next_double`: 53 random bits scaled into `[0, 1)`.
    #[inline]
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
    }
}

// ---------------------------------------------------------------------------
// Generator

/// `np.random.Generator` over a `PCG64` (`np.random.default_rng(seed)`).
#[derive(Debug)]
pub struct Generator {
    rng: Mutex<GeneratorState>,
}

#[derive(Debug)]
struct GeneratorState {
    bitgen: PCG64,
    has_uint32: bool,
    uinteger: u32,
}

/// `size=None` -> a scalar, `size=n` -> `(n,)`, `size=(a, b)` -> that shape.
fn shape_of(size: &Arg) -> Result<Option<Vec<usize>>> {
    match size {
        Arg::None => Ok(None),
        Arg::Int(n) => Ok(Some(alloc::vec![usize::try_from(*n).map_err(|_| {
            LailaError::value_error("negative dimensions are not allowed")
        })?])),
        other => {
            let dims = other.__iter__()?;
            dims.iter()
                .map(|d| {
                    d.as_i64()
                        .and_then(|i| usize::try_from(i).ok())
                        .ok_or_else(|| LailaError::type_error("size must be int or tuple of ints"))
                })
                .collect::<Result<Vec<usize>>>()
                .map(Some)
        }
    }
}

impl Generator {
    /// `np.random.default_rng(seed=None)`.
    pub fn default_rng(seed: &Arg) -> Result<Generator> {
        Ok(Generator {
            rng: Mutex::new(GeneratorState {
                bitgen: PCG64::new(seed)?,
                has_uint32: false,
                uinteger: 0,
            }),
        })
    }

    /// `rng.random(size=None)`: float64 in `[0, 1)`.
    pub fn random(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let size = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("size"));
        let mut st = self.rng.lock();
        match shape_of(&size)? {
            None => Ok(Arg::Float(st.bitgen.next_f64())),
            Some(shape) => {
                let n: usize = shape.iter().product();
                let values: Vec<f64> = (0..n).map(|_| st.bitgen.next_f64()).collect();
                Ok(NDArray::from_f64s("<f8", shape, &values)?.into_arg())
            }
        }
    }

    /// `rng.integers(low, high=None, size=None)`: `int64` in `[low, high)`
    /// (numpy's masked rejection sampling on 64-bit draws).
    pub fn integers(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let low = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("low"));
        let high = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("high"));
        let size = a
            .positional
            .get(2)
            .cloned()
            .unwrap_or_else(|| a.get("size"));
        let (low, high) = match (&low, &high) {
            (l, Arg::None) => (0i64, l.as_i64().unwrap_or(0)),
            (l, h) => (l.as_i64().unwrap_or(0), h.as_i64().unwrap_or(0)),
        };
        if high <= low {
            return Err(LailaError::value_error("high <= 0"));
        }
        let rng_range = (high as u64).wrapping_sub(low as u64).wrapping_sub(1);
        let mut st = self.rng.lock();
        // `random_bounded_uint64` with `use_masked=False` (the `Generator`
        // default): Lemire's multiply-and-reject on 32- or 64-bit draws.
        let draw = |st: &mut GeneratorState| -> i64 {
            if rng_range == 0 {
                return low;
            }
            if rng_range <= 0xffff_ffff {
                let next32 = |st: &mut GeneratorState| -> u32 {
                    let (mut has, mut ui) = (st.has_uint32, st.uinteger);
                    let v = st.bitgen.next_u32(&mut has, &mut ui);
                    st.has_uint32 = has;
                    st.uinteger = ui;
                    v
                };
                if rng_range == 0xffff_ffff {
                    return (low as u64).wrapping_add(u64::from(next32(st))) as i64;
                }
                // `buffered_bounded_lemire_uint32`
                let rng_excl = (rng_range as u32).wrapping_add(1);
                let mut m = u64::from(next32(st)) * u64::from(rng_excl);
                let mut leftover = m as u32;
                if leftover < rng_excl {
                    let threshold = (u32::MAX - rng_range as u32) % rng_excl;
                    while leftover < threshold {
                        m = u64::from(next32(st)) * u64::from(rng_excl);
                        leftover = m as u32;
                    }
                }
                return (low as u64).wrapping_add(m >> 32) as i64;
            }
            if rng_range == u64::MAX {
                return (low as u64).wrapping_add(st.bitgen.next_u64()) as i64;
            }
            // `bounded_lemire_uint64`
            let rng_excl = rng_range.wrapping_add(1);
            let mut m = u128::from(st.bitgen.next_u64()) * u128::from(rng_excl);
            let mut leftover = m as u64;
            if leftover < rng_excl {
                let threshold = (u64::MAX - rng_range) % rng_excl;
                while leftover < threshold {
                    m = u128::from(st.bitgen.next_u64()) * u128::from(rng_excl);
                    leftover = m as u64;
                }
            }
            (low as u64).wrapping_add((m >> 64) as u64) as i64
        };
        match shape_of(&size)? {
            None => Ok(Arg::Int(draw(&mut st))),
            Some(shape) => {
                let n: usize = shape.iter().product();
                let values: Vec<f64> = (0..n).map(|_| draw(&mut st) as f64).collect();
                Ok(NDArray::from_f64s("<i8", shape, &values)?.into_arg())
            }
        }
    }

    /// `rng.bytes(length)`: random bytes (`random_bytes` over 32-bit draws).
    pub fn bytes(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let n = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("length"))
            .as_i64()
            .unwrap_or(0)
            .max(0) as usize;
        let mut st = self.rng.lock();
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let (mut has, mut ui) = (st.has_uint32, st.uinteger);
            let word = st.bitgen.next_u32(&mut has, &mut ui);
            st.has_uint32 = has;
            st.uinteger = ui;
            for b in word.to_le_bytes() {
                if out.len() < n {
                    out.push(b);
                }
            }
        }
        Ok(crate::python::object::bytes(out))
    }
}

impl PyObject for Generator {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Generator"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "Generator(PCG64) at {:#x}",
            self as *const Generator as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Generator>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const Generator as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "random" => self.random(args),
            "integers" => self.integers(args),
            "bytes" => self.bytes(args),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'numpy.random._generator.Generator' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![
            String::from("random"),
            String::from("integers"),
            String::from("bytes")
        ]
    }
}

impl IntoArg for Generator {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floats(a: Arg) -> Vec<f64> {
        let Arg::NDArray(a) = a else {
            panic!("ndarray")
        };
        a.tolist()
            .__iter__()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect()
    }

    // Reference values: numpy 2.3.5.
    #[test]
    fn default_rng_random_matches_numpy() {
        let g = Generator::default_rng(&Arg::Int(0)).unwrap();
        assert_eq!(
            floats(g.random(crate::args!(3)).unwrap()),
            [0.6369616873214543, 0.2697867137638703, 0.04097352393619469]
        );
        let g = Generator::default_rng(&Arg::Int(42)).unwrap();
        assert_eq!(
            floats(g.random(crate::args!(2)).unwrap()),
            [0.7739560485559633, 0.4388784397520523]
        );
        // A seed above 2**64 -- several 32-bit entropy words.
        let seed = Arg::from_bigint("12345678901234567890".parse().unwrap());
        let g = Generator::default_rng(&seed).unwrap();
        assert_eq!(
            g.random(Args::new()).unwrap().as_f64().unwrap(),
            0.004224879795573511
        );
    }

    #[test]
    fn default_rng_integers_and_bytes_match_numpy() {
        let g = Generator::default_rng(&Arg::Int(7)).unwrap();
        assert_eq!(
            floats(g.integers(crate::args!(0, 10, 8)).unwrap()),
            [9.0, 6.0, 6.0, 8.0, 5.0, 7.0, 8.0, 2.0]
        );
        let g = Generator::default_rng(&Arg::Int(7)).unwrap();
        assert_eq!(
            floats(g.integers(crate::args!(0, 1i64 << 40, 3)).unwrap()),
            [687299734001.0, 986497006767.0, 852875435924.0]
        );
        let g = Generator::default_rng(&Arg::Int(3)).unwrap();
        let Arg::Bytes(b) = g.bytes(crate::args!(9)).unwrap() else {
            panic!("bytes")
        };
        assert_eq!(&b[..], b"\xf8\xc2\xbe\xcf\x93\x1a\xed\x15\xf5");
    }
}
