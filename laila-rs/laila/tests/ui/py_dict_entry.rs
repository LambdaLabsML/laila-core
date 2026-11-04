//! `py!` dict entries are `key: value` (`{"a"}` alone is a set; a bare
//! element mixed into a dict literal is rejected).
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

fn main() {
    let _d = laila::py!({"a": 1, "b"});
}
