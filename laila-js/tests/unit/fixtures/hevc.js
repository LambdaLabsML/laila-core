/**
 * Synthetic H.265 / HEVC Annex B elementary stream (port of
 * ``tests/functional/policy/communication/streaming/unit_tests/_hevc.py``).
 *
 * The goal is a byte stream with the *shape* of real H.265 -- NAL unit
 * framing, start codes, parameter sets, IDR / trailing pictures, size
 * distribution, emulation-prevention-safe payloads -- so stream framing,
 * message boundaries, reassembly and parsing all get exercised without any
 * codec library. The payload bits are random; nothing here decodes video.
 *
 * ``generate`` builds a list of ``AccessUnit`` (one per frame) from a seeded
 * PRNG (the Python original uses ``random.Random``; the exact byte values
 * differ, the structure and every invariant the tests check are the same).
 * ``parse_annexb`` is the reference parser written independently of the
 * generator.
 */

export const START_CODE = Buffer.from([0x00, 0x00, 0x00, 0x01]);

export const NAL_TRAIL_N = 0;
export const NAL_TRAIL_R = 1;
export const NAL_IDR_W_RADL = 19;
export const NAL_VPS = 32;
export const NAL_SPS = 33;
export const NAL_PPS = 34;
export const NAL_AUD = 35;
export const NAL_SEI_PREFIX = 39;

const _SLICE_TYPES = new Set(Array.from({ length: 32 }, (_, i) => i)); // VCL NAL unit types
const _PARAM_TYPES = new Set([NAL_VPS, NAL_SPS, NAL_PPS]);

// eslint-disable-next-line no-control-regex
const _EP_PATTERN = /\x00\x00(?=[\x00-\x03])/g;

// ----------------------------------------------------------------------
// Seeded PRNG (stand-in for ``random.Random``)
// ----------------------------------------------------------------------

/** Deterministic PRNG with the ``random.Random`` methods the generator needs. */
export class Random {
  constructor(seed = 0) {
    // splitmix32 to expand the seed into sfc32 state
    let s = (Number(seed) >>> 0) || 0x9e3779b9;
    const next = () => {
      s = (s + 0x9e3779b9) | 0;
      let z = s;
      z = Math.imul(z ^ (z >>> 16), 0x85ebca6b);
      z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35);
      return (z ^ (z >>> 16)) >>> 0;
    };
    this._a = next();
    this._b = next();
    this._c = next();
    this._d = next();
    for (let i = 0; i < 12; i++) this._u32();
    this._spare = null;
  }

  _u32() {
    // sfc32
    let { _a: a, _b: b, _c: c, _d: d } = this;
    const t = (((a + b) | 0) + d) | 0;
    d = (d + 1) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    c = (c + t) | 0;
    this._a = a;
    this._b = b;
    this._c = c;
    this._d = d;
    return t >>> 0;
  }

  /** Float in ``[0, 1)`` with 53 random bits (like CPython). */
  random() {
    const hi = this._u32() >>> 5; // 27 bits
    const lo = this._u32() >>> 6; // 26 bits
    return (hi * 67108864 + lo) / 9007199254740992;
  }

  /** ``randint(a, b)``: integer in ``[a, b]`` inclusive. */
  randint(a, b) {
    if (b < a) throw new RangeError(`empty range for randint(${a}, ${b})`);
    return a + Math.floor(this.random() * (b - a + 1));
  }

  /** ``randbytes(n)`` */
  randbytes(n) {
    const out = Buffer.alloc(n);
    let i = 0;
    while (i + 4 <= n) {
      out.writeUInt32LE(this._u32(), i);
      i += 4;
    }
    if (i < n) {
      let v = this._u32();
      while (i < n) {
        out[i++] = v & 0xff;
        v >>>= 8;
      }
    }
    return out;
  }

  /** ``normalvariate(mu, sigma)`` (Box-Muller). */
  normalvariate(mu, sigma) {
    if (this._spare !== null) {
      const z = this._spare;
      this._spare = null;
      return mu + sigma * z;
    }
    let u1 = this.random();
    while (u1 <= Number.EPSILON) u1 = this.random();
    const u2 = this.random();
    const r = Math.sqrt(-2.0 * Math.log(u1));
    const z0 = r * Math.cos(2 * Math.PI * u2);
    this._spare = r * Math.sin(2 * Math.PI * u2);
    return mu + sigma * z0;
  }

  /** ``lognormvariate(mu, sigma)`` */
  lognormvariate(mu, sigma) {
    return Math.exp(this.normalvariate(mu, sigma));
  }
}

// ----------------------------------------------------------------------
// Generator
// ----------------------------------------------------------------------

/** Two-byte HEVC NAL header: f=0, type, layer=0, tid_plus1=1. */
export function nal_header(nal_type) {
  if (!(nal_type >= 0 && nal_type < 64)) throw new RangeError(String(nal_type));
  return Buffer.from([(nal_type << 1) & 0x7e, 0x01]);
}

/** ``nal_unit_type`` from a NAL unit (header + payload, no start code). */
export function nal_type_of(nal) {
  return (nal[0] >> 1) & 0x3f;
}

/** Insert ``0x03`` after any ``00 00`` followed by ``00..03`` (H.265 7.4.2). */
export function emulation_prevention(payload) {
  const s = Buffer.from(payload).toString("latin1");
  return Buffer.from(s.replace(_EP_PATTERN, "\x00\x00\x03"), "latin1");
}

/** Random EP-safe payload of about *n* bytes with a non-zero last byte. */
function _rbsp(rng, n, { first_slice = null } = {}) {
  const raw = rng.randbytes(Math.max(1, n));
  if (first_slice !== null) {
    // first_slice_segment_in_pic_flag lives in the MSB of byte 0;
    // keep byte 0 outside 0x00..0x03 so EP insertion never touches it.
    raw[0] = first_slice ? 0x80 | (raw[0] & 0x7f) : 0x04 | (raw[0] & 0x7b);
  }
  if (raw[raw.length - 1] === 0) raw[raw.length - 1] = 0x80;
  let out = emulation_prevention(raw);
  if (out[out.length - 1] === 0) out = Buffer.concat([out.subarray(0, out.length - 1), Buffer.from([0x80])]);
  return out;
}

/** ``start code + header + EP-safe payload`` of roughly *size* bytes. */
export function make_nal(rng, nal_type, size, { first_slice = null } = {}) {
  const payload = _rbsp(rng, Math.max(1, size - 2), { first_slice });
  return Buffer.concat([START_CODE, nal_header(nal_type), payload]);
}

/** One frame: an ordered list of ``[nal_type, nal_bytes_with_start_code]``. */
export class AccessUnit {
  constructor(index, kind, nals = []) {
    this.index = index;
    this.kind = kind; // "IDR" | "P" | "B"
    /** @type {Array<[number, Buffer]>} */
    this.nals = nals;
  }
  get data() {
    return Buffer.concat(this.nals.map(([, n]) => n));
  }
  get size() {
    return this.nals.reduce((s, [, n]) => s + n.length, 0);
  }
  get types() {
    return this.nals.map(([t]) => t);
  }
  get is_idr() {
    return this.kind === "IDR";
  }
}

/** Summary of a generated stream the receiver must reproduce. */
export class GroundTruth {
  constructor({ au_count, idr_count, nal_histogram, au_sizes, total_bytes, nal_count }) {
    this.au_count = au_count;
    this.idr_count = idr_count;
    this.nal_histogram = nal_histogram;
    this.au_sizes = au_sizes;
    this.total_bytes = total_bytes;
    this.nal_count = nal_count;
  }
}

function _lognormal_size(rng, lo, hi, median, sigma) {
  const mu = Math.log(median);
  const v = Math.trunc(rng.lognormvariate(mu, sigma));
  return Math.max(lo, Math.min(hi, v));
}

/** Generate *n_frames* access units (an IDR every *gop* frames). */
export function generate(opts = {}) {
  const { seed = 1234, n_frames = 300, gop = 30, idr_bytes = [60_000, 250_000], p_bytes = [4_000, 40_000], p_median = 12_000 } = opts;
  const rng = new Random(seed);
  const aus = [];
  for (let i = 0; i < n_frames; i++) {
    const is_idr = i % gop === 0;
    const kind = is_idr ? "IDR" : rng.random() < 0.3 ? "B" : "P";
    const au = new AccessUnit(i, kind);
    if (is_idr || rng.random() < 0.15) au.nals.push([NAL_AUD, make_nal(rng, NAL_AUD, 3)]);
    if (is_idr) {
      au.nals.push([NAL_VPS, make_nal(rng, NAL_VPS, 30)]);
      au.nals.push([NAL_SPS, make_nal(rng, NAL_SPS, 60)]);
      au.nals.push([NAL_PPS, make_nal(rng, NAL_PPS, 10)]);
    }
    if (rng.random() < 0.2) au.nals.push([NAL_SEI_PREFIX, make_nal(rng, NAL_SEI_PREFIX, 100)]);
    let total, n_slices, slice_type;
    if (is_idr) {
      total = rng.randint(idr_bytes[0], idr_bytes[1]);
      n_slices = rng.randint(1, 3);
      slice_type = NAL_IDR_W_RADL;
    } else {
      total = _lognormal_size(rng, p_bytes[0], p_bytes[1], p_median, 0.6);
      n_slices = rng.randint(1, 2);
      slice_type = kind === "B" ? NAL_TRAIL_N : NAL_TRAIL_R;
    }
    const cuts = Array.from({ length: n_slices - 1 }, () => rng.randint(1, total - 1)).sort((x, y) => x - y);
    const bounds = [0, ...cuts, total];
    for (let s = 0; s < n_slices; s++) {
      const size = Math.max(16, bounds[s + 1] - bounds[s]);
      au.nals.push([slice_type, make_nal(rng, slice_type, size, { first_slice: s === 0 })]);
    }
    aus.push(au);
  }
  return aus;
}

/** ``Counter`` of NAL types as a plain object ``{ "<type>": count }``. */
function _count(types_lists) {
  const hist = {};
  for (const types of types_lists) for (const t of types) hist[t] = (hist[t] ?? 0) + 1;
  return hist;
}

export function ground_truth(aus) {
  return new GroundTruth({
    au_count: aus.length,
    idr_count: aus.filter((au) => au.is_idr).length,
    nal_histogram: _count(aus.map((au) => au.types)),
    au_sizes: aus.map((au) => au.size),
    total_bytes: aus.reduce((s, au) => s + au.size, 0),
    nal_count: aus.reduce((s, au) => s + au.nals.length, 0),
  });
}

/** One message per access unit (all its NALs concatenated, Annex B). */
export function packetize_per_au(aus) {
  return aus.map((au) => au.data);
}

/** One message per NAL unit (with its start code). */
export function packetize_per_nal(aus) {
  return aus.flatMap((au) => au.nals.map(([, n]) => n));
}

// ----------------------------------------------------------------------
// Reference parser (independent of the generator)
// ----------------------------------------------------------------------

export class ParsedAU {
  constructor(types = [], size = 0) {
    this.types = types;
    this.size = size;
  }
  get is_idr() {
    return this.types.some((t) => t >= 16 && t <= 23);
  }
}

const _SC3 = Buffer.from([0x00, 0x00, 0x01]);

/**
 * Split an Annex B byte stream on ``00 00 01`` start codes.
 *
 * Returns NAL units (header + payload) without start codes. A ``zero_byte``
 * belonging to a following 4-byte start code is stripped from the end of
 * the preceding NAL (RBSP payloads never end in ``0x00``).
 */
export function split_nals(stream) {
  stream = Buffer.from(stream);
  const nals = [];
  const n = stream.length;
  let idx = stream.indexOf(_SC3, 0);
  while (idx !== -1) {
    const start = idx + 3;
    const nxt = stream.indexOf(_SC3, start);
    const end = nxt === -1 ? n : nxt;
    let nal = stream.subarray(start, end);
    while (nal.length && nal[nal.length - 1] === 0) nal = nal.subarray(0, nal.length - 1);
    if (nal.length) nals.push(nal);
    idx = nxt;
  }
  return nals;
}

function _is_vcl(t) {
  return _SLICE_TYPES.has(t);
}

function _first_slice_flag(nal) {
  return nal.length > 2 && !!(nal[2] & 0x80);
}

/**
 * Group NAL units into access units.
 *
 * A new access unit starts at an AUD, at a VPS/SPS/PPS/SEI that follows a
 * VCL NAL, or at a VCL NAL with ``first_slice_segment_in_pic_flag`` set when
 * the previous NAL was also VCL.
 */
export function group_access_units(nals) {
  const aus = [];
  let cur = null;
  let prev_vcl = false;
  for (const nal of nals) {
    const t = nal_type_of(nal);
    let new_au = false;
    if (cur === null) new_au = true;
    else if (t === NAL_AUD) new_au = true;
    else if ((_PARAM_TYPES.has(t) || t === NAL_SEI_PREFIX) && prev_vcl) new_au = true;
    else if (_is_vcl(t) && prev_vcl && _first_slice_flag(nal)) new_au = true;
    if (new_au) {
      cur = new ParsedAU([], 0);
      aus.push(cur);
    }
    cur.types.push(t);
    cur.size += nal.length + 4; // account for the 4-byte start code
    prev_vcl = _is_vcl(t);
  }
  return aus;
}

/** Full reference parse: NAL split + AU grouping. */
export function parse_annexb(stream) {
  return group_access_units(split_nals(stream));
}

export function histogram(aus) {
  return _count(aus.map((au) => au.types));
}
