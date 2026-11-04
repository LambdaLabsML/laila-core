//! Lambda parameters take no defaults (spell a `def` with `#[pysig]`).
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    fn f() -> Arg {
        let g = lambda x, y=1: x + y;
        return g(2);
    }
}

fn main() {}
