/**
 * Python ``urllib.parse.quote`` / ``unquote`` with exact CPython semantics.
 *
 * ``quote(s, safe="/")`` percent-encodes the UTF-8 bytes of every character
 * not in ``A-Za-z0-9_.-~`` and not in ``safe``; hex digits are upper-case.
 * laila pools use ``quote(key, safe="")`` for file / object names.
 */

const _ALWAYS_SAFE = new Set(
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-~".split("").map((c) => c.charCodeAt(0)),
);

/**
 * @param {string|Uint8Array} s
 * @param {{safe?: string, encoding?: string, errors?: string}} [opts]
 */
export function quote(s, opts = {}) {
  const safe = opts.safe === undefined ? "/" : opts.safe;
  const safe_set = new Set(_ALWAYS_SAFE);
  for (const ch of safe) safe_set.add(ch.charCodeAt(0));
  const bytes = s instanceof Uint8Array ? s : Buffer.from(s, "utf8");
  let out = "";
  for (const b of bytes) {
    if (safe_set.has(b)) out += String.fromCharCode(b);
    else out += "%" + b.toString(16).toUpperCase().padStart(2, "0");
  }
  return out;
}

/** ``quote_plus``: like quote but spaces become ``+`` and ``/`` is not safe by default. */
export function quote_plus(s, opts = {}) {
  const safe = opts.safe === undefined ? "" : opts.safe;
  if (typeof s === "string" && !s.includes(" ")) return quote(s, { safe });
  const q = quote(s, { safe: safe + " " });
  return q.replace(/ /g, "+");
}

/** ``urllib.parse.unquote(s)``: decode ``%XX`` sequences as UTF-8 (errors="replace"). */
export function unquote(s) {
  if (!s.includes("%")) return s;
  const parts = s.split("%");
  let out = parts[0];
  let pending = [];
  const flush = () => {
    if (pending.length) {
      out += Buffer.from(pending).toString("utf8");
      pending = [];
    }
  };
  for (let i = 1; i < parts.length; i++) {
    const p = parts[i];
    if (p.length >= 2 && /^[0-9a-fA-F]{2}/.test(p)) {
      pending.push(parseInt(p.slice(0, 2), 16));
      const rest = p.slice(2);
      if (rest) {
        flush();
        out += rest;
      }
    } else {
      flush();
      out += "%" + p;
    }
  }
  flush();
  return out;
}

/** ``unquote_plus``: ``+`` -> space, then unquote. */
export function unquote_plus(s) {
  return unquote(s.replace(/\+/g, " "));
}
