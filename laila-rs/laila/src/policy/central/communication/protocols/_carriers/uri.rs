//! URI helpers shared by the carriers (`urllib.parse.urlparse` subset).

use alloc::string::String;

use crate::python::{Arg, Args, IntoArg, Result};

/// `scheme://netloc/path` -> `(netloc, path)`; a URI without `://` has an
/// empty netloc and is all path (Python's `urlparse`).
fn split(uri: &str) -> (&str, &str) {
    match uri.find("://") {
        Some(i) => {
            let rest = &uri[i + 3..];
            match rest.find('/') {
                Some(j) => (&rest[..j], &rest[j..]),
                None => (rest, ""),
            }
        }
        None => ("", uri),
    }
}

/// `split_host_port(uri, default_host="127.0.0.1", default_port=0)`.
pub fn split_host_port(uri: &str, default_host: &str, default_port: u16) -> (String, u16) {
    let (netloc, _) = split(uri);
    // strip userinfo
    let hostport = match netloc.rfind('@') {
        Some(i) => &netloc[i + 1..],
        None => netloc,
    };
    let (host, port) = if let Some(stripped) = hostport.strip_prefix('[') {
        // [v6]:port
        match stripped.find(']') {
            Some(j) => {
                let host = &stripped[..j];
                let rest = &stripped[j + 1..];
                (host, rest.strip_prefix(':'))
            }
            None => (hostport, None),
        }
    } else {
        match hostport.rfind(':') {
            Some(i) => (&hostport[..i], Some(&hostport[i + 1..])),
            None => (hostport, None),
        }
    };
    let host = if host.is_empty() {
        String::from(default_host)
    } else {
        host.to_lowercase()
    };
    let port = port
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(default_port);
    (host, port)
}

/// `uri_path(uri)`: the path (`unix:///tmp/x.sock` -> `/tmp/x.sock`), or the
/// netloc when there is no path.
pub fn uri_path(uri: &str) -> String {
    let (netloc, path) = split(uri);
    if path.is_empty() {
        String::from(netloc)
    } else {
        String::from(path)
    }
}

/// `uri_authority(uri)`: the netloc (`loopback://<gid>` -> `<gid>`), or the
/// path without its leading slashes.
pub fn uri_authority(uri: &str) -> String {
    let (netloc, path) = split(uri);
    if netloc.is_empty() {
        String::from(path.trim_start_matches('/'))
    } else {
        String::from(netloc)
    }
}

/// `uri_authority(uri)` for the dynamic layer.
pub fn uri_authority_arg(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let uri = a.positional.first().cloned().unwrap_or(Arg::None).__str__();
    Ok(uri_authority(&uri).into_arg())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_port() {
        assert_eq!(
            split_host_port("tcp://127.0.0.1:5000", "127.0.0.1", 0),
            (String::from("127.0.0.1"), 5000)
        );
        assert_eq!(
            split_host_port("tcp://host", "127.0.0.1", 7),
            (String::from("host"), 7)
        );
        assert_eq!(uri_path("unix:///tmp/a.sock"), "/tmp/a.sock");
        assert_eq!(uri_authority("loopback://abc"), "abc");
        assert_eq!(uri_authority("/abc"), "abc");
    }
}
