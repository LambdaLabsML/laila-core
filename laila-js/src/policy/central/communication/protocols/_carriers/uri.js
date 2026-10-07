/**
 * Small URI parsing helpers shared by concrete transports.
 *
 * Transports peer via ``add_peer(uri, secret)``; these helpers turn the
 * common URI shapes (``scheme://host:port``, ``scheme:///path``) into the
 * address components a driver needs, without each transport re-deriving the
 * same ``urllib.parse`` boilerplate.
 */
import { urlparse } from "../../../../../_compat/urlparse.js";

/**
 * Return ``[host, port]`` from a ``scheme://host:port`` URI.
 * @param {string} uri
 * @param {string} [default_host]
 * @param {number} [default_port]
 * @returns {[string, number]}
 */
export function split_host_port(uri, default_host = "127.0.0.1", default_port = 0) {
  const parsed = urlparse(uri);
  const host = parsed.hostname || default_host;
  const port = parsed.port !== null ? parsed.port : default_port;
  return [host, port];
}

/**
 * Return the path component of a ``scheme:///path`` URI.
 * @param {string} uri
 */
export function uri_path(uri) {
  const parsed = urlparse(uri);
  // netloc-less URIs (unix:///tmp/x) keep the leading slash in path.
  return parsed.path || parsed.netloc;
}

/**
 * Return the authority (everything after ``scheme://``) verbatim.
 * @param {string} uri
 */
export function uri_authority(uri) {
  const parsed = urlparse(uri);
  return parsed.netloc || parsed.path.replace(/^\/+/, "");
}
