/**
 * Python ``urllib.parse.urlparse`` / ``urlsplit`` subset.
 *
 * Mirrors CPython's splitting rules (not the WHATWG URL parser): the
 * scheme is everything before the first ``:`` when it is a valid scheme
 * token, the netloc is what follows ``//`` up to the next ``/``, ``?`` or
 * ``#``, and ``hostname`` / ``port`` / ``username`` / ``password`` are
 * derived lazily from the netloc exactly like ``urllib.parse``.
 */
import { ValueError } from "./errors.js";

const _SCHEME_CHARS = /^[A-Za-z][A-Za-z0-9+\-.]*$/;

export class ParseResult {
  constructor(scheme, netloc, path, params, query, fragment) {
    this.scheme = scheme;
    this.netloc = netloc;
    this.path = path;
    this.params = params;
    this.query = query;
    this.fragment = fragment;
  }
  get _userinfo() {
    const at = this.netloc.lastIndexOf("@");
    if (at === -1) return [null, null];
    const userinfo = this.netloc.slice(0, at);
    const colon = userinfo.indexOf(":");
    if (colon === -1) return [userinfo, null];
    return [userinfo.slice(0, colon), userinfo.slice(colon + 1)];
  }
  get _hostinfo() {
    const at = this.netloc.lastIndexOf("@");
    let hostinfo = at === -1 ? this.netloc : this.netloc.slice(at + 1);
    let hostname;
    let port = null;
    if (hostinfo.startsWith("[")) {
      const end = hostinfo.indexOf("]");
      hostname = hostinfo.slice(0, end + 1);
      const rest = hostinfo.slice(end + 1);
      if (rest.startsWith(":")) port = rest.slice(1);
    } else {
      const colon = hostinfo.indexOf(":");
      if (colon === -1) hostname = hostinfo;
      else {
        hostname = hostinfo.slice(0, colon);
        port = hostinfo.slice(colon + 1);
      }
    }
    return [hostname, port];
  }
  get username() {
    return this._userinfo[0];
  }
  get password() {
    return this._userinfo[1];
  }
  /** Lower-cased host, brackets stripped for IPv6; ``null`` when absent. */
  get hostname() {
    let [host] = this._hostinfo;
    if (!host) return null;
    if (host.startsWith("[") && host.endsWith("]")) host = host.slice(1, -1);
    const percent = host.indexOf("%");
    if (percent !== -1 && host.startsWith("[")) host = host.slice(0, percent);
    return host.toLowerCase();
  }
  /** Integer port or ``null``; ``ValueError`` when malformed / out of range. */
  get port() {
    const [, port] = this._hostinfo;
    if (port === null || port === "") return null;
    if (!/^\d+$/.test(port)) throw new ValueError(`Port could not be cast to integer value as '${port}'`);
    const n = parseInt(port, 10);
    if (n < 0 || n > 65535) throw new ValueError("Port out of range 0-65535");
    return n;
  }
  geturl() {
    return urlunparse(this);
  }
  /** Tuple-like access ``[scheme, netloc, path, params, query, fragment]``. */
  *[Symbol.iterator]() {
    yield this.scheme;
    yield this.netloc;
    yield this.path;
    yield this.params;
    yield this.query;
    yield this.fragment;
  }
}

/** ``urllib.parse.urlsplit`` core (no params split). */
export function urlsplit(url, scheme = "", allow_fragments = true) {
  url = url.replace(/[\t\r\n]/g, "");
  let netloc = "";
  let query = "";
  let fragment = "";
  const i = url.indexOf(":");
  if (i > 0 && _SCHEME_CHARS.test(url.slice(0, i))) {
    scheme = url.slice(0, i).toLowerCase();
    url = url.slice(i + 1);
  }
  if (url.startsWith("//")) {
    let end = url.length;
    for (const c of ["/", "?", "#"]) {
      const idx = url.indexOf(c, 2);
      if (idx !== -1 && idx < end) end = idx;
    }
    netloc = url.slice(2, end);
    url = url.slice(end);
    if ((netloc.includes("[") && !netloc.includes("]")) || (netloc.includes("]") && !netloc.includes("["))) {
      throw new ValueError("Invalid IPv6 URL");
    }
  }
  if (allow_fragments && url.includes("#")) {
    const h = url.indexOf("#");
    fragment = url.slice(h + 1);
    url = url.slice(0, h);
  }
  if (url.includes("?")) {
    const q = url.indexOf("?");
    query = url.slice(q + 1);
    url = url.slice(0, q);
  }
  return new ParseResult(scheme, netloc, url, "", query, fragment);
}

/** ``urllib.parse.urlparse`` */
export function urlparse(url, scheme = "", allow_fragments = true) {
  const r = urlsplit(url, scheme, allow_fragments);
  let path = r.path;
  let params = "";
  // params split applies to the last path segment
  const slash = path.lastIndexOf("/");
  const seg_start = slash === -1 ? 0 : slash;
  const semi = path.indexOf(";", seg_start);
  if (semi !== -1) {
    params = path.slice(semi + 1);
    path = path.slice(0, semi);
  }
  return new ParseResult(r.scheme, r.netloc, path, params, r.query, r.fragment);
}

/** ``urllib.parse.urlunparse`` */
export function urlunparse(parts) {
  const [scheme, netloc, path0, params, query, fragment] = Array.isArray(parts) ? parts : [...parts];
  let path = path0;
  if (params) path = `${path};${params}`;
  let url = path;
  if (netloc || (scheme && ["http", "https", "ftp", "file", "ws", "wss"].includes(scheme) && url.length && url[0] === "/")) {
    if (url && url[0] !== "/") url = "/" + url;
    url = "//" + (netloc || "") + url;
  }
  if (scheme) url = scheme + ":" + url;
  if (query) url = url + "?" + query;
  if (fragment) url = url + "#" + fragment;
  return url;
}

/** ``urllib.parse.parse_qs`` */
export function parse_qs(qs) {
  const out = {};
  for (const [k, v] of new URLSearchParams(qs)) {
    if (!(k in out)) out[k] = [];
    out[k].push(v);
  }
  return out;
}

/** ``urllib.parse.parse_qsl`` */
export function parse_qsl(qs) {
  return [...new URLSearchParams(qs)];
}
