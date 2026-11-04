//! Python `zlib`.
//!
//! ```text
//! compress(data, /, level=-1, wbits=MAX_WBITS) -> bytes
//! decompress(data, /, wbits=MAX_WBITS, bufsize=DEF_BUF_SIZE) -> bytes
//! crc32(data, value=0), adler32(data, value=1)
//! ```
//!
//! On `std` this is the real zlib (bundled, static), so `compress` output is
//! byte-identical to CPython's for the same input/level/wbits. The `no_std`
//! build uses `miniz_oxide`: decompression is exact, compression is valid
//! zlib but not byte-identical to the C library.

use alloc::vec::Vec;

use crate::python::errors::ExcKind;
use crate::python::{LailaError, Result};

pub const Z_DEFAULT_COMPRESSION: i32 = -1;
pub const Z_BEST_SPEED: i32 = 1;
pub const Z_BEST_COMPRESSION: i32 = 9;
pub const Z_NO_COMPRESSION: i32 = 0;
pub const MAX_WBITS: i32 = 15;
pub const DEF_MEM_LEVEL: i32 = 8;
pub const DEF_BUF_SIZE: usize = 16384;

/// `zlib.error`.
pub fn error(msg: impl Into<alloc::string::String>) -> LailaError {
    LailaError::new(ExcKind::ZlibError, msg)
}

fn check_level(level: i32) -> Result<()> {
    if !(-1..=9).contains(&level) {
        return Err(error("Bad compression level"));
    }
    Ok(())
}

/// `zlib.compress(data, /, level=-1, wbits=MAX_WBITS)`.
pub fn compress(data: &[u8], level: i32, wbits: i32) -> Result<Vec<u8>> {
    check_level(level)?;
    imp::compress(data, level, wbits)
}

/// `zlib.decompress(data, /, wbits=MAX_WBITS, bufsize=DEF_BUF_SIZE)`.
pub fn decompress(data: &[u8], wbits: i32, bufsize: usize) -> Result<Vec<u8>> {
    let _ = bufsize; // only a hint in CPython
    imp::decompress(data, wbits)
}

/// `zlib.adler32(data, value=1)`.
pub fn adler32(data: &[u8], value: u32) -> u32 {
    let mut a = value & 0xffff;
    let mut b = (value >> 16) & 0xffff;
    for &x in data {
        a = (a + x as u32) % 65521;
        b = (b + a) % 65521;
    }
    (b << 16) | a
}

/// `zlib.crc32(data, value=0)`.
pub fn crc32(data: &[u8], value: u32) -> u32 {
    let mut crc = !value;
    for &x in data {
        crc ^= x as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(feature = "std")]
mod imp {
    use super::*;
    use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

    fn compression(level: i32) -> Compression {
        if level < 0 {
            Compression::default() // zlib Z_DEFAULT_COMPRESSION == 6
        } else {
            Compression::new(level as u32)
        }
    }

    pub fn compress(data: &[u8], level: i32, wbits: i32) -> Result<Vec<u8>> {
        if wbits > 15 {
            // gzip wrapper (CPython: wbits 16+w). Not byte-stable (mtime).
            if wbits > 31 {
                return Err(error("Invalid initialization option"));
            }
            use std::io::Write;
            let mut e = flate2::write::GzEncoder::new(Vec::new(), compression(level));
            e.write_all(data)
                .map_err(|e| error(alloc::format!("Error -2 while compressing data: {e}")))?;
            return e
                .finish()
                .map_err(|e| error(alloc::format!("Error -2 while compressing data: {e}")));
        }
        let (zlib_header, window) = if wbits > 0 {
            (true, wbits)
        } else if wbits < 0 {
            (false, -wbits)
        } else {
            return Err(error("Invalid initialization option"));
        };
        if !(9..=15).contains(&window) {
            return Err(error("Invalid initialization option"));
        }
        let mut c = Compress::new_with_window_bits(compression(level), zlib_header, window as u8);
        let mut out = Vec::with_capacity(data.len() / 2 + 64);
        loop {
            let before = c.total_in();
            let status = c
                .compress_vec(&data[before as usize..], &mut out, FlushCompress::Finish)
                .map_err(|e| error(alloc::format!("Error -2 while compressing data: {e}")))?;
            match status {
                Status::StreamEnd => break,
                _ => {
                    if out.len() == out.capacity() {
                        out.reserve(out.capacity().max(64));
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn decompress(data: &[u8], wbits: i32) -> Result<Vec<u8>> {
        if (16..=47).contains(&wbits) {
            // 16+w gzip, 32+w auto-detect
            use std::io::Read;
            let mut out = Vec::new();
            let res = if wbits > 31 && data.first() != Some(&0x1f) {
                flate2::read::ZlibDecoder::new(data).read_to_end(&mut out)
            } else {
                flate2::read::GzDecoder::new(data).read_to_end(&mut out)
            };
            return res
                .map(|_| out)
                .map_err(|e| error(alloc::format!("Error -3 while decompressing data: {e}")));
        }
        let (zlib_header, window) = if wbits == 0 {
            (true, 15)
        } else if wbits > 0 {
            (true, wbits)
        } else {
            (false, -wbits)
        };
        if !(8..=15).contains(&window) {
            return Err(error("Invalid initialization option"));
        }
        let mut d = Decompress::new_with_window_bits(zlib_header, window as u8);
        let mut out = Vec::with_capacity(data.len() * 3 + 64);
        loop {
            let before = d.total_in() as usize;
            let status = d
                .decompress_vec(&data[before..], &mut out, FlushDecompress::Finish)
                .map_err(|e| {
                    let msg = alloc::format!("{e}");
                    error(alloc::format!(
                        "Error -3 while decompressing data: {}",
                        zlib_msg(&msg)
                    ))
                })?;
            match status {
                Status::StreamEnd => return Ok(out),
                Status::BufError if d.total_in() as usize == data.len() => {
                    return Err(error(
                        "Error -5 while decompressing data: incomplete or truncated stream",
                    ))
                }
                _ => {
                    if d.total_in() as usize == data.len() && out.len() < out.capacity() {
                        return Err(error(
                            "Error -5 while decompressing data: incomplete or truncated stream",
                        ));
                    }
                    if out.len() == out.capacity() {
                        out.reserve(out.capacity().max(64));
                    }
                }
            }
        }
    }

    fn zlib_msg(flate_msg: &str) -> &str {
        // flate2 surfaces zlib's `msg` when present, else a generic text.
        if flate_msg.contains("header") {
            "incorrect header check"
        } else if flate_msg.is_empty() || flate_msg.contains("deflate decompression error") {
            "invalid stored block lengths"
        } else {
            flate_msg
        }
    }
}

#[cfg(not(feature = "std"))]
mod imp {
    use super::*;

    pub fn compress(data: &[u8], level: i32, wbits: i32) -> Result<Vec<u8>> {
        let lvl = if level < 0 { 6 } else { level as u8 };
        if wbits > 15 {
            return Err(error("Invalid initialization option"));
        }
        Ok(if wbits < 0 {
            miniz_oxide::deflate::compress_to_vec(data, lvl)
        } else {
            miniz_oxide::deflate::compress_to_vec_zlib(data, lvl)
        })
    }

    pub fn decompress(data: &[u8], wbits: i32) -> Result<Vec<u8>> {
        if wbits > 15 {
            return Err(error("Invalid initialization option"));
        }
        let r = if wbits < 0 {
            miniz_oxide::inflate::decompress_to_vec(data)
        } else {
            miniz_oxide::inflate::decompress_to_vec_zlib(data)
        };
        r.map_err(|e| match e.status {
            miniz_oxide::inflate::TINFLStatus::FailedCannotMakeProgress
            | miniz_oxide::inflate::TINFLStatus::NeedsMoreInput => {
                error("Error -5 while decompressing data: incomplete or truncated stream")
            }
            _ => error("Error -3 while decompressing data: incorrect header check"),
        })
    }
}
