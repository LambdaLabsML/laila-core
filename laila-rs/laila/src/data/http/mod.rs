//! The REST plumbing shared by the HTTP object-store pools (`S3Pool` and
//! its S3-compatible siblings, `GCSPool`, `AzurePool`, `HuggingFacePool`).
//!
//! Python reaches these services through vendor SDKs (`boto3`,
//! `google-cloud-storage`, `azure-storage-blob`, `huggingface_hub`); the
//! port speaks the same wire protocols directly over one small blocking
//! client (`ureq` on rustls), so this module holds what every SDK hides:
//! the request/response shape, retries, RFC 3986 encoding, the date
//! formats the signing schemes need, SHA-256 / HMAC / base64 helpers and a
//! deliberately tiny XML tag reader for the list responses.

use alloc::string::String;
use alloc::vec::Vec;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::python::errors::ExcKind;
use crate::python::{LailaError, Result};

/// One HTTP response: status, headers (lower-cased names) and the body.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// The first header named *name* (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        let lower = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == lower)
            .map(|(_, v)| v.as_str())
    }
    /// `200 <= status < 300`
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
    /// The body as (lossy) UTF-8 text.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A blocking HTTP client with connection reuse; one per pool
/// (`self._client` in Python).
pub struct Client {
    agent: ureq::Agent,
}

impl core::fmt::Debug for Client {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("http::Client")
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new()
    }
}

/// The number of attempts botocore's `standard` retry mode makes
/// (`retries={"max_attempts": 3}`); the other SDKs retry transient
/// failures comparably.
pub const MAX_ATTEMPTS: u32 = 3;

impl Client {
    /// A client that reports every status (4xx / 5xx are responses, not
    /// errors) and never follows redirects (object stores sign the exact
    /// URL).
    pub fn new() -> Client {
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(core::time::Duration::from_secs(120)))
            .build();
        Client {
            agent: config.new_agent(),
        }
    }

    /// Send one request; transport failures surface as `ConnectionError`.
    pub fn send(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let mut req = ureq::http::Request::builder().method(method).uri(url);
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let request = req
            .body(body.map(|b| b.to_vec()).unwrap_or_default())
            .map_err(|e| transport_error(url, &e.to_string()))?;
        let response = self
            .agent
            .run(request)
            .map_err(|e| transport_error(url, &e.to_string()))?;
        let status = response.status().as_u16();
        let mut out_headers = Vec::new();
        for (k, v) in response.headers() {
            out_headers.push((
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or("").into(),
            ));
        }
        let body = response
            .into_body()
            .with_config()
            .limit(u64::MAX)
            .read_to_vec()
            .map_err(|e| transport_error(url, &e.to_string()))?;
        Ok(Response {
            status,
            headers: out_headers,
            body,
        })
    }

    /// [`send`](Self::send) with the SDKs' retry policy: transport errors
    /// and 5xx / 429 responses are retried up to [`MAX_ATTEMPTS`] times
    /// with a short exponential back-off; *sign* re-signs the request for
    /// each attempt (date headers move).
    pub fn send_with_retry(
        &self,
        method: &str,
        url: &str,
        body: Option<&[u8]>,
        mut sign: impl FnMut() -> Result<Vec<(String, String)>>,
    ) -> Result<Response> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let headers = sign()?;
            match self.send(method, url, &headers, body) {
                Ok(r) if (r.status >= 500 || r.status == 429) && attempt < MAX_ATTEMPTS => {}
                Ok(r) => return Ok(r),
                Err(_) if attempt < MAX_ATTEMPTS => {}
                Err(e) => return Err(e),
            }
            std::thread::sleep(core::time::Duration::from_millis(100 << attempt));
        }
    }
}

fn transport_error(url: &str, msg: &str) -> LailaError {
    let host = url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .unwrap_or(url);
    LailaError::new(
        ExcKind::ConnectionError,
        alloc::format!("Could not connect to the endpoint URL: \"{host}\": {msg}"),
    )
}

// ---------------------------------------------------------------------------
// Encoding

/// RFC 3986 percent-encoding: the unreserved `A-Za-z0-9-_.~` pass, every
/// other byte of the UTF-8 text becomes `%XX` (upper-case hex); `/` passes
/// too when *keep_slash* (path encoding).
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let c = b as char;
        if b.is_ascii_alphanumeric()
            || matches!(c, '-' | '_' | '.' | '~')
            || (keep_slash && c == '/')
        {
            out.push(c);
        } else {
            out.push_str(&alloc::format!("%{b:02X}"));
        }
    }
    out
}

/// `urllib.parse.quote(key, safe="")` -- the logical-key -> object-key
/// encoding every pool shares.
pub fn quote_key(key: &str) -> String {
    uri_encode(key, false)
}

/// `urllib.parse.unquote(object_key)`.
pub fn unquote_key(object_key: &str) -> String {
    let bytes = object_key.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&object_key[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A canonical query string: pairs sorted by key, both sides RFC 3986
/// encoded (`a=1&b=2`).
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| alloc::format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Lower-case hex of *bytes*.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| alloc::format!("{b:02x}")).collect()
}

/// `hashlib.sha256(data).hexdigest()`
pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// `hmac.new(key, msg, hashlib.sha256).digest()`
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

/// Standard base64 (with padding).
pub fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Standard base64 decode (padding optional).
pub fn base64_decode(text: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    let trimmed: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(&trimmed)
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed.trim_end_matches('='))
        })
        .map_err(|e| LailaError::value_error(alloc::format!("invalid base64: {e}")))
}

/// URL-safe base64 without padding (JWT segments).
pub fn base64url_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

// ---------------------------------------------------------------------------
// Time

/// The current UTC time as `(year, month, day, hour, minute, second)`.
pub fn utc_now() -> (i64, u32, u32, u32, u32, u32) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    civil_utc(secs)
}

/// Seconds since the Unix epoch.
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Split Unix seconds into UTC civil fields.
pub fn civil_utc(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = crate::_compat::datetime::civil_from_days(days);
    (
        y,
        m,
        d,
        (rem / 3600) as u32,
        ((rem % 3600) / 60) as u32,
        (rem % 60) as u32,
    )
}

/// SigV4's `x-amz-date`: `YYYYMMDD'T'HHMMSS'Z'`.
pub fn amz_date() -> String {
    let (y, m, d, hh, mm, ss) = utc_now();
    alloc::format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// RFC 1123 (`Thu, 08 Oct 2026 20:00:00 GMT`), the `Date` / `x-ms-date`
/// format.
pub fn rfc1123_now() -> String {
    let (y, m, d, hh, mm, ss) = utc_now();
    let wd = crate::_compat::datetime::weekday(y, m, d) as usize;
    alloc::format!(
        "{}, {d:02} {} {y:04} {hh:02}:{mm:02}:{ss:02} GMT",
        WEEKDAYS[wd % 7],
        MONTHS[(m as usize - 1) % 12]
    )
}

// ---------------------------------------------------------------------------
// XML

/// Decode the five predefined XML entities (and numeric references).
pub fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(end) = rest.find(';') else {
            out.push_str(rest);
            return out;
        };
        let entity = &rest[1..end];
        match entity {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                match u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
                {
                    Some(c) => out.push(c),
                    None => out.push_str(&rest[..=end]),
                }
            }
            _ if entity.starts_with('#') => {
                match entity[1..].parse::<u32>().ok().and_then(char::from_u32) {
                    Some(c) => out.push(c),
                    None => out.push_str(&rest[..=end]),
                }
            }
            _ => out.push_str(&rest[..=end]),
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    out
}

/// Every `<tag>text</tag>` body in *xml*, in document order (the
/// element's attributes are ignored, entities decoded).
pub fn xml_texts(xml: &str, tag: &str) -> Vec<String> {
    let open = alloc::format!("<{tag}");
    let close = alloc::format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        // `<tag>` or `<tag attr=..>`; not `<tagname...>` of a longer name.
        let Some(gt) = after.find('>') else { break };
        let head = &after[..gt];
        if !(head.is_empty() || head.starts_with(char::is_whitespace) || head.ends_with('/')) {
            rest = after;
            continue;
        }
        let body_start = &after[gt + 1..];
        if head.ends_with('/') {
            out.push(String::new());
            rest = body_start;
            continue;
        }
        let Some(j) = body_start.find(&close) else {
            break;
        };
        out.push(xml_unescape(&body_start[..j]));
        rest = &body_start[j + close.len()..];
    }
    out
}

/// The first `<tag>text</tag>` body in *xml*.
pub fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_texts(xml, tag).into_iter().next()
}

/// `&`, `<`, `>` escaped for an XML text node.
pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ---------------------------------------------------------------------------
// JSON access helpers (responses parsed with the port's `json.loads`)

/// `obj[key]` of a parsed JSON object, `None` when absent / not an object.
pub fn json_get(obj: &crate::python::Arg, key: &str) -> Option<crate::python::Arg> {
    match obj {
        crate::python::Arg::Dict(_) => match obj.__contains__(key) {
            Ok(true) => obj.__getitem__(key).ok(),
            _ => None,
        },
        _ => None,
    }
}

/// `obj[key]` as text.
pub fn json_str(obj: &crate::python::Arg, key: &str) -> Option<String> {
    json_get(obj, key).and_then(|v| match v {
        crate::python::Arg::Str(s) => Some(String::from(&*s)),
        crate::python::Arg::None => None,
        other => Some(other.__str__()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_like_urllib_quote_safe_empty() {
        assert_eq!(
            quote_key("LAILA:ENTRY:a/b c~"),
            "LAILA%3AENTRY%3Aa%2Fb%20c~"
        );
        assert_eq!(
            unquote_key("LAILA%3AENTRY%3Aa%2Fb%20c~"),
            "LAILA:ENTRY:a/b c~"
        );
        assert_eq!(uri_encode("a/b", true), "a/b");
    }

    #[test]
    fn reads_xml_tags() {
        let xml = "<ListBucketResult><Contents><Key>a&amp;b</Key></Contents><Contents><Key>c</Key></Contents><KeyCount>2</KeyCount></ListBucketResult>";
        assert_eq!(xml_texts(xml, "Key"), vec!["a&b", "c"]);
        assert_eq!(xml_text(xml, "KeyCount").as_deref(), Some("2"));
        assert_eq!(xml_text(xml, "NextContinuationToken"), None);
    }

    #[test]
    fn sigv4_reference_hash() {
        // SHA-256 of the empty string, the `x-amz-content-sha256` of a GET.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
