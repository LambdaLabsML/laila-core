/**
 * Python ``uuid`` module subset: ``uuid4``, ``uuid5``, ``UUID`` and the
 * standard namespaces. UUIDs are represented as canonical lower-case strings
 * (``str(uuid.UUID)``); the ``UUID`` class exists for ``isinstance`` parity.
 */
import { createHash, randomUUID } from "node:crypto";
import { TypeError as PyTypeError, ValueError } from "./errors.js";

const _HEX32 = /^[0-9a-fA-F]{32}$/;

export class UUID {
  /** @param {string} hex_or_str canonical ``8-4-4-4-12`` or bare 32 hex digits */
  constructor(hex_or_str) {
    let hex = String(hex_or_str).replace(/^urn:uuid:/, "").replace(/[{}-]/g, "");
    if (!_HEX32.test(hex)) throw new ValueError("badly formed hexadecimal UUID string");
    hex = hex.toLowerCase();
    this.hex = hex;
  }
  get bytes() {
    return Buffer.from(this.hex, "hex");
  }
  get version() {
    return parseInt(this.hex[12], 16);
  }
  toString() {
    const h = this.hex;
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  }
  __str__() {
    return this.toString();
  }
  __repr__() {
    return `UUID('${this.toString()}')`;
  }
  __eq__(other) {
    return other instanceof UUID ? other.hex === this.hex : typeof other === "string" ? _canon(other) === this.toString() : false;
  }
  __hash__() {
    return this.hex;
  }
  toJSON() {
    return this.toString();
  }
}

function _canon(s) {
  try {
    return new UUID(s).toString();
  } catch {
    return null;
  }
}

export const NAMESPACE_DNS = new UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
export const NAMESPACE_URL = new UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
export const NAMESPACE_OID = new UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8");
export const NAMESPACE_X500 = new UUID("6ba7b814-9dad-11d1-80b4-00c04fd430c8");

/** ``uuid.uuid4()`` */
export function uuid4() {
  return new UUID(randomUUID());
}

/**
 * ``uuid.uuid5(namespace, name)``: SHA-1 of ``namespace.bytes + name.encode("utf-8")``
 * with version/variant bits set.
 * @param {UUID|string} namespace
 * @param {string} name
 */
export function uuid5(namespace, name) {
  const ns = namespace instanceof UUID ? namespace : new UUID(namespace);
  // CPython: ``name.encode("utf-8")`` -- only ``str`` (or ``bytes``) is accepted.
  if (typeof name !== "string" && !(name instanceof Uint8Array)) {
    throw new PyTypeError(`can't concat ${name === null || name === undefined ? "NoneType" : typeof name} to bytes`);
  }
  const h = createHash("sha1");
  h.update(ns.bytes);
  h.update(typeof name === "string" ? Buffer.from(name, "utf8") : name);
  const d = h.digest().subarray(0, 16);
  d[6] = (d[6] & 0x0f) | 0x50;
  d[8] = (d[8] & 0x3f) | 0x80;
  return new UUID(d.toString("hex"));
}

/** ``uuid.uuid3`` (MD5), for completeness. */
export function uuid3(namespace, name) {
  const ns = namespace instanceof UUID ? namespace : new UUID(namespace);
  const h = createHash("md5");
  h.update(ns.bytes);
  h.update(Buffer.from(String(name), "utf8"));
  const d = h.digest().subarray(0, 16);
  d[6] = (d[6] & 0x0f) | 0x30;
  d[8] = (d[8] & 0x3f) | 0x80;
  return new UUID(d.toString("hex"));
}

/** True when ``s`` parses as a UUID (``uuid.UUID(s)`` would succeed). */
export function is_uuid(s) {
  return _canon(s) !== null;
}
