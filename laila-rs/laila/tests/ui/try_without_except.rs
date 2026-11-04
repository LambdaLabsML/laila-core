//! `try` needs an `except` or a `finally`.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    fn f() -> Arg {
        try {
            return 1;
        }
    }
}

fn main() {}
