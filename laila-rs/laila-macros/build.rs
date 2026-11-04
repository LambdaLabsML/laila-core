//! Generates the table of laila API names whose calls take Python-style
//! `Args` (`f(a, k=v)` -> `f(args!(a, k = v))` in `laila::python!`).
//!
//! The table is derived from `laila-core`'s own sources -- every
//! `#[pysig("name(..)")]` and every `fn name(.., args: impl Into<Args>)` --
//! so the macro and the library cannot drift apart. When the sibling crate
//! is not present (a published `laila-macros` built on its own) the
//! generated list is empty and the hand-maintained builtins table in
//! `src/rewrite/gen.rs` carries the known names.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let core_src = manifest.join("..").join("laila").join("src");
    let mut names = BTreeSet::new();
    let mut zero_arg = BTreeSet::new();
    if core_src.is_dir() {
        println!("cargo:rerun-if-changed={}", core_src.display());
        walk(&core_src, &mut names, &mut zero_arg);
    }
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("args_methods.rs");
    let mut body = String::from(
        "/// Generated from laila-core's `#[pysig]` / `impl Into<Args>` signatures.\n",
    );
    body.push_str("pub const ARGS_METHODS_GENERATED: &[&str] = &[\n");
    for n in &names {
        body.push_str(&format!("    {n:?},\n"));
    }
    body.push_str("];\n");
    body.push_str(
        "/// Generated from laila-core's `arg_methods0! { .. }`: `Arg` methods that take no arguments.\n",
    );
    body.push_str("pub const ZERO_ARG_METHODS: &[&str] = &[\n");
    for n in &zero_arg {
        body.push_str(&format!("    {n:?},\n"));
    }
    body.push_str("];\n");
    // `Arg`'s own methods with their arity (`-1`: an `Args` bag), so the
    // rewriter can tell a typed forwarder from a remote method of the same
    // name reached through a dynamic receiver.
    let mut arities: BTreeMap<String, i32> = BTreeMap::new();
    if core_src.is_dir() {
        for f in ["object.rs", "methods.rs", "ops.rs"] {
            if let Ok(src) = fs::read_to_string(core_src.join("python").join(f)) {
                scan_arg_impls(&src, &mut arities);
            }
        }
        if let Ok(src) = fs::read_to_string(core_src.join("python").join("object.rs")) {
            scan_forwarder_macros(&src, &mut arities);
        }
    }
    body.push_str(
        "/// Generated from laila-core's `impl Arg` blocks and `arg_methods*!` forwarders: `(name, arity)` (`-1`: `Args`).\n",
    );
    body.push_str("pub const ARG_METHOD_ARITY: &[(&str, i32)] = &[\n");
    for (n, k) in &arities {
        body.push_str(&format!("    ({n:?}, {k}),\n"));
    }
    body.push_str("];\n");
    // `arg_attrs!` names: attribute *reads* (`fut.status`) -- a Python
    // *call* of one on a dynamic receiver (`proxy.value()`) is a method of
    // the remote / dynamic object, not the getter.
    let mut attrs: BTreeMap<String, i32> = BTreeMap::new();
    if core_src.is_dir() {
        if let Ok(src) = fs::read_to_string(core_src.join("python").join("object.rs")) {
            scan_attr_forwarders(&src, &mut attrs);
        }
    }
    body.push_str("/// Generated from laila-core's `arg_attrs! { .. }`: attribute-read forwarders on `Arg`.\n");
    body.push_str("pub const ARG_ATTR_NAMES: &[&str] = &[\n");
    for n in attrs.keys() {
        body.push_str(&format!("    {n:?},\n"));
    }
    body.push_str("];\n");
    fs::write(out, body).unwrap();
}

/// The names of every `arg_attrs! { .. }` block of `src`.
fn scan_attr_forwarders(src: &str, out: &mut BTreeMap<String, i32>) {
    let marker = "arg_attrs! {";
    let mut rest = src;
    while let Some(i) = rest.find(marker) {
        let after = &rest[i + marker.len()..];
        let Some(end) = after.find('}') else { break };
        for line in after[..end].lines() {
            let line = line.split("//").next().unwrap_or("");
            for n in line.split(',') {
                let n = n.trim();
                if !n.is_empty() && n.chars().all(is_ident) {
                    out.insert(n.to_string(), 0);
                }
            }
        }
        rest = &after[end..];
    }
}

/// `pub fn name(&self, a, b)` inside every `impl Arg {` block of `src`.
fn scan_arg_impls(src: &str, out: &mut BTreeMap<String, i32>) {
    let mut rest = src;
    while let Some(i) = rest.find("impl Arg {") {
        let after = &rest[i + "impl Arg {".len()..];
        // the block: up to the matching close brace
        let mut depth = 1;
        let mut end = after.len();
        for (j, c) in after.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = j;
                        break;
                    }
                }
                _ => {}
            }
        }
        let block = &after[..end];
        let mut b = block;
        while let Some(k) = b.find("pub fn ") {
            let a = &b[k + "pub fn ".len()..];
            let name: String = a.chars().take_while(|c| is_ident(*c)).collect();
            let t = a[name.len()..].trim_start();
            // generics
            let t = if let Some(g) = t.strip_prefix('<') {
                let mut d = 1;
                let mut idx = 0;
                for (j, c) in g.char_indices() {
                    match c {
                        '<' => d += 1,
                        '>' => {
                            d -= 1;
                            if d == 0 {
                                idx = j + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                g[idx..].trim_start()
            } else {
                t
            };
            if let Some(params) = t.strip_prefix('(') {
                let mut d = 1;
                let mut pend = params.len();
                for (j, c) in params.char_indices() {
                    match c {
                        '(' | '<' | '[' => d += 1,
                        ')' | '>' | ']' => {
                            d -= 1;
                            if d == 0 {
                                pend = j;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let params = &params[..pend];
                if params.trim_start().starts_with("&self")
                    || params.trim_start().starts_with("&mut self")
                {
                    let arity = if params.contains("impl Into<Args>") || params.contains(": Args") {
                        -1
                    } else {
                        // top-level commas after `&self`
                        let mut d = 0;
                        let mut n = 0;
                        for c in params.chars() {
                            match c {
                                '(' | '<' | '[' => d += 1,
                                ')' | '>' | ']' => d -= 1,
                                ',' if d == 0 => n += 1,
                                _ => {}
                            }
                        }
                        if params.trim_end().ends_with(',') {
                            n -= 1;
                        }
                        n
                    };
                    if !name.is_empty() && !name.starts_with("__") {
                        out.entry(name).or_insert(arity);
                    }
                }
            }
            b = a;
        }
        rest = after;
    }
}

/// `arg_methods! { .. }` (`Args`), `arg_methods0!` .. `arg_methods4!` and
/// `arg_attrs!` (attribute reads: arity 0) forwarders.
fn scan_forwarder_macros(src: &str, out: &mut BTreeMap<String, i32>) {
    for (marker, arity) in [
        ("arg_methods! {", -1),
        ("arg_methods0! {", 0),
        ("arg_methods1! {", 1),
        ("arg_methods2! {", 2),
        ("arg_methods3! {", 3),
        ("arg_methods4! {", 4),
        ("arg_attrs! {", 0),
    ] {
        let mut rest = src;
        while let Some(i) = rest.find(marker) {
            let after = &rest[i + marker.len()..];
            let Some(end) = after.find('}') else { break };
            for line in after[..end].lines() {
                let line = line.split("//").next().unwrap_or("");
                for n in line.split(',') {
                    let n = n.trim();
                    if !n.is_empty() && n.chars().all(is_ident) {
                        out.entry(n.to_string()).or_insert(arity);
                    }
                }
            }
            rest = &after[end..];
        }
    }
}

fn walk(dir: &Path, names: &mut BTreeSet<String>, zero_arg: &mut BTreeSet<String>) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            println!("cargo:rerun-if-changed={}", p.display());
            walk(&p, names, zero_arg);
        } else if p.extension().is_some_and(|e| e == "rs") {
            println!("cargo:rerun-if-changed={}", p.display());
            if let Ok(src) = fs::read_to_string(&p) {
                // Comments (`/// fn f(args: Args)` in a doc) are not signatures.
                let code: String = src
                    .lines()
                    .filter(|l| !l.trim_start().starts_with("//"))
                    .map(|l| format!("{l}\n"))
                    .collect();
                scan(&code, names);
                scan_zero_arg(&src, zero_arg);
            }
        }
    }
}

/// `arg_methods0! { a, b, // comment \n c, }`: the zero-argument `Arg`
/// forwarders (a call with arguments is another object's method).
fn scan_zero_arg(src: &str, names: &mut BTreeSet<String>) {
    let mut rest = src;
    while let Some(i) = rest.find("arg_methods0! {") {
        let after = &rest[i + "arg_methods0! {".len()..];
        let Some(end) = after.find('}') else { break };
        for line in after[..end].lines() {
            let line = line.split("//").next().unwrap_or("");
            for n in line.split(',') {
                let n = n.trim();
                if !n.is_empty() && n.chars().all(is_ident) {
                    names.insert(n.to_string());
                }
            }
        }
        rest = &after[end..];
    }
}

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn scan(src: &str, names: &mut BTreeSet<String>) {
    // #[pysig("name(..)")]  (also `laila::pysig`, `pysig(r"..")`)
    let mut rest = src;
    while let Some(i) = rest.find("pysig(") {
        let after = &rest[i + "pysig(".len()..];
        let q = after.trim_start_matches(['r', '#']).trim_start();
        if let Some(stripped) = q.strip_prefix('"') {
            let name: String = stripped
                .chars()
                .take_while(|c| is_ident(*c) || *c == '.')
                .collect();
            if let Some(last) = name.rsplit('.').next() {
                if !last.is_empty() && !last.starts_with("__") {
                    names.insert(last.to_string());
                }
            }
        }
        rest = after;
    }
    // fn name(.., args: impl Into<Args>)  /  (.., args: Args)
    let mut rest = src;
    while let Some(i) = rest.find("fn ") {
        let after = &rest[i + 3..];
        let name: String = after.chars().take_while(|c| is_ident(*c)).collect();
        let tail = &after[name.len()..];
        let tail = tail.trim_start();
        let tail = if let Some(t) = tail.strip_prefix('<') {
            // generics: skip to the matching '>'
            let mut depth = 1;
            let mut idx = 0;
            for (j, c) in t.char_indices() {
                match c {
                    '<' => depth += 1,
                    '>' => {
                        depth -= 1;
                        if depth == 0 {
                            idx = j + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            t[idx..].trim_start()
        } else {
            tail
        };
        if let Some(params) = tail.strip_prefix('(') {
            let mut depth = 1;
            let mut end = params.len();
            for (j, c) in params.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = j;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let params = &params[..end];
            let takes_args = params.contains("impl Into<Args>")
                || params.contains("impl Into<crate::python::Args>")
                || params.contains("impl Into<super::args::Args>")
                || params.contains("impl Into<$crate::python::Args>")
                || params.trim_end().ends_with(": Args");
            if takes_args && !name.is_empty() && !name.starts_with("__") {
                names.insert(name);
            }
        }
        rest = after;
    }
}
