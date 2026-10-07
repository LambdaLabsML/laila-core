/**
 * Python ``zlib`` on Node's bundled zlib (same upstream library, same
 * defaults: windowBits 15, memLevel 8, default strategy -> identical
 * streams for identical input/level).
 *
 *   compress(data, level=-1, wbits=15) -> bytes
 *   decompress(data, wbits=15, bufsize=16384) -> bytes
 *   crc32(data, value=0), adler32(data, value=1)
 */
import nodezlib from "node:zlib";
import { TypeError as PyTypeError, ValueError, PyException } from "../_compat/errors.js";

/** ``zlib.error`` */
export class error extends PyException {}
export { error as ZlibError };

export const Z_DEFAULT_COMPRESSION = -1;
export const Z_BEST_SPEED = 1;
export const Z_BEST_COMPRESSION = 9;
export const Z_NO_COMPRESSION = 0;
export const MAX_WBITS = 15;
export const DEF_MEM_LEVEL = 8;
export const DEF_BUF_SIZE = 16384;

function _bytes(data, fn) {
  if (typeof data === "string") throw new PyTypeError(`a bytes-like object is required, not 'str'`);
  if (data instanceof Uint8Array) return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  if (data instanceof ArrayBuffer) return Buffer.from(data);
  throw new PyTypeError(`${fn}() argument 1 must be bytes-like, not ${data === null ? "NoneType" : data?.constructor?.name ?? typeof data}`);
}

/**
 * ``zlib.compress(data, /, level=-1, wbits=MAX_WBITS)``
 * @param {Uint8Array} data
 * @param {{level?: number, wbits?: number}} [opts]
 */
export function compress(data, opts = {}) {
  const level = opts.level ?? Z_DEFAULT_COMPRESSION;
  const wbits = opts.wbits ?? MAX_WBITS;
  if (!Number.isInteger(level) || level < -1 || level > 9) throw new error("Bad compression level");
  const buf = _bytes(data, "compress");
  try {
    if (wbits > 0 && wbits <= 15) return nodezlib.deflateSync(buf, { level, windowBits: wbits, memLevel: DEF_MEM_LEVEL });
    if (wbits < 0) return nodezlib.deflateRawSync(buf, { level, windowBits: -wbits, memLevel: DEF_MEM_LEVEL });
    if (wbits > 15) return nodezlib.gzipSync(buf, { level, windowBits: wbits - 16, memLevel: DEF_MEM_LEVEL });
    throw new error("Invalid initialization option");
  } catch (e) {
    if (e instanceof error) throw e;
    throw new error(`Error ${e.errno ?? -2} while compressing data: ${e.message}`);
  }
}

/**
 * ``zlib.decompress(data, /, wbits=MAX_WBITS, bufsize=DEF_BUF_SIZE)``
 * @param {Uint8Array} data
 * @param {{wbits?: number, bufsize?: number}} [opts]
 */
export function decompress(data, opts = {}) {
  const wbits = opts.wbits ?? MAX_WBITS;
  const bufsize = opts.bufsize ?? DEF_BUF_SIZE;
  if (bufsize < 0) throw new ValueError("bufsize must be non-negative");
  const buf = _bytes(data, "decompress");
  try {
    if (wbits === 0) return nodezlib.inflateSync(buf);
    if (wbits > 0 && wbits <= 15) return nodezlib.inflateSync(buf, { windowBits: wbits });
    if (wbits < 0) return nodezlib.inflateRawSync(buf, { windowBits: -wbits });
    if (wbits > 15 && wbits <= 31) return nodezlib.gunzipSync(buf, { windowBits: wbits - 16 });
    if (wbits > 31) return nodezlib.unzipSync(buf, { windowBits: wbits - 32 });
    throw new error("Invalid initialization option");
  } catch (e) {
    if (e instanceof error) throw e;
    if (e.code === "Z_BUF_ERROR") throw new error("Error -5 while decompressing data: incomplete or truncated stream");
    if (e.code === "Z_DATA_ERROR") throw new error(`Error -3 while decompressing data: ${e.message}`);
    throw new error(`Error ${e.errno ?? -2} while decompressing data: ${e.message}`);
  }
}

export function crc32(data, value = 0) {
  return Number(nodezlib.crc32(_bytes(data, "crc32"), value >>> 0));
}

export function adler32(data, value = 1) {
  const buf = _bytes(data, "adler32");
  let a = value & 0xffff;
  let b = (value >>> 16) & 0xffff;
  for (let i = 0; i < buf.length; i++) {
    a = (a + buf[i]) % 65521;
    b = (b + a) % 65521;
  }
  return ((b << 16) | a) >>> 0;
}
