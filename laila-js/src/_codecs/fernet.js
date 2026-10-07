/**
 * ``cryptography.fernet`` -- byte-exact Fernet (spec v0x80) on Node ``crypto``.
 *
 * token = urlsafe_b64( 0x80 | timestamp(8, BE) | IV(16) | AES-128-CBC/PKCS7(data) | HMAC-SHA256(32) )
 * key   = urlsafe_b64( signing_key(16) | encryption_key(16) )
 *
 * Mirrors ``cryptography/fernet.py``: ``generate_key``, ``encrypt``,
 * ``encrypt_at_time``, ``decrypt(token, ttl=None)``, ``decrypt_at_time``,
 * ``extract_timestamp`` and ``MultiFernet``, raising ``InvalidToken`` under
 * exactly the same conditions (bad base64, wrong version byte, bad HMAC,
 * expired TTL, timestamp more than 60 s in the future, bad padding).
 */
import crypto from "node:crypto";
import { ValueError, TypeError as PyTypeError, PyException } from "../_compat/errors.js";

export class InvalidToken extends PyException {}

const _MAX_CLOCK_SKEW = 60;

function _urlsafe_b64decode(s) {
  // base64.urlsafe_b64decode: translate -_ to +/, then b64decode (non-strict,
  // but incorrect padding raises binascii.Error).
  const str = typeof s === "string" ? s : Buffer.from(s).toString("latin1");
  const std = str.replace(/-/g, "+").replace(/_/g, "/");
  // Python's b64decode discards non-alphabet chars (validate=False) but
  // requires correct padding.
  const clean = std.replace(/[^A-Za-z0-9+/=]/g, "");
  const body = clean.replace(/=+$/, "");
  const pad = clean.length - body.length;
  if ((body.length + pad) % 4 !== 0 || body.length % 4 === 1) throw new ValueError("Incorrect padding");
  if (pad > 2) throw new ValueError("Incorrect padding");
  return Buffer.from(body, "base64");
}

function _urlsafe_b64encode(b) {
  return Buffer.from(b).toString("base64").replace(/\+/g, "-").replace(/\//g, "_");
}

function _to_bytes(x, what) {
  if (typeof x === "string") return Buffer.from(x, "utf8");
  if (x instanceof Uint8Array) return Buffer.from(x);
  throw new PyTypeError(`${what} must be bytes`);
}

export class Fernet {
  /**
   * @param {string|Uint8Array} key  urlsafe-base64 32-byte key
   * @param {object} [backend]  ignored (API parity)
   */
  constructor(key, backend = null) {
    void backend;
    let raw;
    try {
      raw = _urlsafe_b64decode(_to_bytes(key, "key"));
    } catch (e) {
      throw new ValueError("Fernet key must be 32 url-safe base64-encoded bytes.");
    }
    if (raw.length !== 32) throw new ValueError("Fernet key must be 32 url-safe base64-encoded bytes.");
    this._signing_key = raw.subarray(0, 16);
    this._encryption_key = raw.subarray(16);
  }

  /** ``Fernet.generate_key()`` */
  static generate_key() {
    return Buffer.from(_urlsafe_b64encode(crypto.randomBytes(32)), "ascii");
  }

  encrypt(data) {
    return this.encrypt_at_time(data, Math.floor(Date.now() / 1000));
  }

  encrypt_at_time(data, current_time) {
    const iv = crypto.randomBytes(16);
    return this._encrypt_from_parts(data, current_time, iv);
  }

  _encrypt_from_parts(data, current_time, iv) {
    if (!(data instanceof Uint8Array)) throw new PyTypeError("data must be bytes.");
    const cipher = crypto.createCipheriv("aes-128-cbc", this._encryption_key, iv);
    cipher.setAutoPadding(true);
    const ciphertext = Buffer.concat([cipher.update(data), cipher.final()]);
    const ts = Buffer.alloc(8);
    ts.writeBigUInt64BE(BigInt(current_time));
    const basic_parts = Buffer.concat([Buffer.from([0x80]), ts, iv, ciphertext]);
    const h = crypto.createHmac("sha256", this._signing_key);
    h.update(basic_parts);
    const hmac = h.digest();
    return Buffer.from(_urlsafe_b64encode(Buffer.concat([basic_parts, hmac])), "ascii");
  }

  /**
   * @param {string|Uint8Array} token
   * @param {number|null} [ttl]
   */
  decrypt(token, ttl = null) {
    if (ttl !== null && ttl !== undefined && typeof ttl === "object" && !Array.isArray(ttl)) ttl = ttl.ttl ?? null; // tolerate opts object
    const [timestamp, data] = Fernet._get_unverified_token_data(token);
    let time_info = null;
    if (ttl !== null && ttl !== undefined) time_info = [ttl, Math.floor(Date.now() / 1000)];
    return this._decrypt_data(data, timestamp, time_info);
  }

  decrypt_at_time(token, ttl, current_time) {
    if (ttl === null || ttl === undefined) throw new ValueError("decrypt_at_time() can only be used with a non-None ttl");
    const [timestamp, data] = Fernet._get_unverified_token_data(token);
    return this._decrypt_data(data, timestamp, [ttl, current_time]);
  }

  extract_timestamp(token) {
    const [timestamp, data] = Fernet._get_unverified_token_data(token);
    this._verify_signature(data);
    return timestamp;
  }

  static _get_unverified_token_data(token) {
    if (!(typeof token === "string" || token instanceof Uint8Array)) throw new PyTypeError("token must be bytes or str");
    let data;
    try {
      data = _urlsafe_b64decode(token);
    } catch (e) {
      throw new InvalidToken();
    }
    if (data.length === 0 || data[0] !== 0x80) throw new InvalidToken();
    if (data.length < 9) throw new InvalidToken();
    const timestamp = Number(data.readBigUInt64BE(1));
    return [timestamp, data];
  }

  _verify_signature(data) {
    if (data.length < 32) throw new InvalidToken();
    const h = crypto.createHmac("sha256", this._signing_key);
    h.update(data.subarray(0, data.length - 32));
    const expected = h.digest();
    const given = data.subarray(data.length - 32);
    if (expected.length !== given.length || !crypto.timingSafeEqual(expected, given)) throw new InvalidToken();
  }

  _decrypt_data(data, timestamp, time_info) {
    if (time_info !== null) {
      const [ttl, current_time] = time_info;
      if (timestamp + ttl < current_time) throw new InvalidToken();
      if (current_time + _MAX_CLOCK_SKEW < timestamp) throw new InvalidToken();
    }
    this._verify_signature(data);
    const iv = data.subarray(9, 25);
    const ciphertext = data.subarray(25, data.length - 32);
    if (ciphertext.length % 16 !== 0) throw new InvalidToken();
    let plaintext;
    try {
      const decipher = crypto.createDecipheriv("aes-128-cbc", this._encryption_key, iv);
      decipher.setAutoPadding(true);
      plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    } catch (e) {
      throw new InvalidToken();
    }
    return plaintext;
  }
}

export class MultiFernet {
  constructor(fernets) {
    fernets = [...fernets];
    if (fernets.length === 0) throw new ValueError("MultiFernet requires at least one Fernet instance");
    this._fernets = fernets;
  }
  encrypt(msg) {
    return this.encrypt_at_time(msg, Math.floor(Date.now() / 1000));
  }
  encrypt_at_time(msg, current_time) {
    return this._fernets[0].encrypt_at_time(msg, current_time);
  }
  rotate(msg) {
    const [timestamp, data] = Fernet._get_unverified_token_data(msg);
    let p = null;
    for (const f of this._fernets) {
      try {
        p = f._decrypt_data(data, timestamp, null);
        break;
      } catch (e) {
        if (!(e instanceof InvalidToken)) throw e;
      }
    }
    if (p === null) throw new InvalidToken();
    const iv = crypto.randomBytes(16);
    return this._fernets[0]._encrypt_from_parts(p, timestamp, iv);
  }
  decrypt(msg, ttl = null) {
    for (const f of this._fernets) {
      try {
        return f.decrypt(msg, ttl);
      } catch (e) {
        if (!(e instanceof InvalidToken)) throw e;
      }
    }
    throw new InvalidToken();
  }
  decrypt_at_time(msg, ttl, current_time) {
    for (const f of this._fernets) {
      try {
        return f.decrypt_at_time(msg, ttl, current_time);
      } catch (e) {
        if (!(e instanceof InvalidToken)) throw e;
      }
    }
    throw new InvalidToken();
  }
}

export { _urlsafe_b64decode as urlsafe_b64decode, _urlsafe_b64encode as urlsafe_b64encode };
