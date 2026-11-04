//! `return value` inside `try` needs a declared return type.
#![allow(unused_imports, dead_code)]
use laila::prelude::*;

laila::python! {
    fn f() {
        try {
            return 1;
        } except ValueError {
            return 2;
        }
    }
}

fn main() {}
