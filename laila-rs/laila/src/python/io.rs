//! Standard output sink (HAL-swappable; `print` writes here).

#[cfg(feature = "std")]
pub fn write_stdout(s: &str) {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(s.as_bytes());
    let _ = out.flush();
}

#[cfg(not(feature = "std"))]
pub fn write_stdout(_s: &str) {}

/// Standard error sink (`logging.StreamHandler()` writes here).
#[cfg(feature = "std")]
pub fn write_stderr(s: &str) {
    use std::io::Write;
    let mut out = std::io::stderr().lock();
    let _ = out.write_all(s.as_bytes());
    let _ = out.flush();
}

#[cfg(not(feature = "std"))]
pub fn write_stderr(_s: &str) {}
