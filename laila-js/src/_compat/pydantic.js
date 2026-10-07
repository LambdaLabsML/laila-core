/**
 * pydantic v2 ``BaseModel`` emulation -- the exact surface laila uses.
 *
 * Declaring a model:
 *
 *   class Foo extends Bar {
 *     static model_config = ConfigDict({ arbitrary_types_allowed: true });
 *     static {
 *       define_fields(this, {
 *         rank: ["int", Field({ default: 0, ge: 0, description: "..." })],
 *         name: ["str | None", null],              // ``name: str | None = None``
 *         tags: ["list[str]", Field({ default_factory: () => [] })],
 *         req:  ["str"],                           // required
 *       });
 *       define_private(this, { _lock: PrivateAttr({ default_factory: () => new RLock() }) });
 *       model_validator(this, "before", (cls, data) => data);
 *       model_validator(this, "after", (self) => self);
 *       field_validator(this, "key", (cls, v) => v);
 *     }
 *   }
 *
 * Construction mirrors pydantic-core: before-validators -> field validation
 * (lax coercion, constraints, field validators) -> extra handling ->
 * ``__pydantic_private__`` defaults -> ``model_post_init`` -> after-validators.
 * Field values live as own enumerable properties; private attributes live in
 * ``__pydantic_private__`` behind prototype accessors (``self._uuid``).
 *
 * Annotations are Python type strings (``"dict[str, Any]"``), classes
 * (BaseModel subclasses, Enums, arbitrary classes), ``Annotated(...)`` or an
 * array of alternatives. Unknown names validate as ``Any``.
 */
import { ValueError, TypeError as PyTypeError, AttributeError } from "./errors.js";
import {
  PyTuple,
  PyFloat,
  isdict,
  is_plain_object,
  dict_items,
  dict_from_entries,
  type_name,
  eq as py_eq,
  is_integral,
} from "./pytypes.js";
import { repr } from "./pyrepr.js";
import { is_enum_member, enum_value } from "./enum.js";
import { deepcopy, copy as shallow_copy } from "./copy.js";

// --------------------------------------------------------------------------
// Sentinels / descriptors
// --------------------------------------------------------------------------

export class PydanticUndefinedType {
  toString() {
    return "PydanticUndefined";
  }
  __repr__() {
    return "PydanticUndefined";
  }
}
export const PydanticUndefined = Object.freeze(new PydanticUndefinedType());

export function ConfigDict(opts = {}) {
  return { ...opts };
}

export class FieldInfo {
  constructor(opts = {}) {
    this.annotation = opts.annotation ?? null;
    this.default = "default" in opts ? opts.default : PydanticUndefined;
    this.default_factory = opts.default_factory ?? null;
    this.description = opts.description ?? null;
    this.alias = opts.alias ?? null;
    this.exclude = opts.exclude ?? null;
    this.repr = opts.repr ?? true;
    this.frozen = opts.frozen ?? null;
    this.strict = opts.strict ?? null;
    this.json_schema_extra = opts.json_schema_extra ?? null;
    this.validate_default = opts.validate_default ?? null;
    this.metadata = [];
    for (const k of ["ge", "le", "gt", "lt", "min_length", "max_length", "pattern", "multiple_of"]) {
      if (opts[k] !== undefined && opts[k] !== null) this.metadata.push({ [k]: opts[k] });
    }
    this.before_validators = opts.before_validators ?? [];
    this.after_validators = opts.after_validators ?? [];
    this._type = null; // parsed annotation (lazy)
  }
  get is_required() {
    return this.default === PydanticUndefined && this.default_factory === null;
  }
  get_default() {
    if (this.default_factory) return this.default_factory();
    return this.default === PydanticUndefined ? null : this.default;
  }
  constraint(name) {
    for (const m of this.metadata) if (name in m) return m[name];
    return undefined;
  }
  __repr__() {
    const parts = [];
    if (this.annotation !== null) parts.push(`annotation=${_ann_repr(this.annotation)}`);
    parts.push(`required=${this.is_required ? "True" : "False"}`);
    if (!this.is_required) parts.push(this.default_factory ? `default_factory=${this.default_factory.name || "<lambda>"}` : `default=${repr(this.default)}`);
    if (this.description) parts.push(`description=${repr(this.description)}`);
    return `FieldInfo(${parts.join(", ")})`;
  }
}

function _ann_repr(a) {
  if (typeof a === "string") return a;
  if (typeof a === "function") return a.name;
  if (Array.isArray(a)) return a.map(_ann_repr).join(" | ");
  return String(a);
}

/** ``pydantic.Field(...)`` -- options object instead of keyword arguments. */
export function Field(opts = {}) {
  return new FieldInfo(opts);
}

export class ModelPrivateAttr {
  constructor(opts = {}) {
    this.default = "default" in opts ? opts.default : PydanticUndefined;
    this.default_factory = opts.default_factory ?? null;
  }
  get_default() {
    if (this.default_factory) return this.default_factory();
    return this.default;
  }
}

/** ``pydantic.PrivateAttr(...)`` */
export function PrivateAttr(opts = {}) {
  return new ModelPrivateAttr(opts);
}

/** ``typing.Annotated[T, BeforeValidator(fn), ...]`` */
export function Annotated(type, ...metadata) {
  return { __annotated__: true, type, metadata };
}
export function BeforeValidator(fn) {
  return { __before_validator__: fn };
}
export function AfterValidator(fn) {
  return { __after_validator__: fn };
}

// --------------------------------------------------------------------------
// ValidationError
// --------------------------------------------------------------------------

export class ValidationError extends ValueError {
  /**
   * @param {string} title model name
   * @param {Array<{type:string, loc:any[], msg:string, input:any}>} errors
   */
  constructor(title, errors) {
    const n = errors.length;
    const lines = [`${n} validation error${n === 1 ? "" : "s"} for ${title}`];
    for (const e of errors) {
      lines.push(e.loc.join("."));
      lines.push(`  ${e.msg} [type=${e.type}, input_value=${repr(e.input)}, input_type=${type_name(e.input)}]`);
    }
    super(lines.join("\n"));
    this.title = title;
    this._errors = errors;
  }
  errors() {
    return this._errors.map((e) => ({ ...e, loc: PyTuple.from_iterable(e.loc) }));
  }
  error_count() {
    return this._errors.length;
  }
  __repr__() {
    return this.message;
  }
}

class _ValidationItem extends Error {
  constructor(type, msg, input) {
    super(msg);
    this.type = type;
    this.input = input;
  }
}

// --------------------------------------------------------------------------
// Annotation parsing & validation (lax mode)
// --------------------------------------------------------------------------

const _type_registry = new Map();

/** Register a class under a name so string annotations can reference it. */
export function register_type(name, cls) {
  _type_registry.set(name, cls);
}

function _split_top(s, sep) {
  const out = [];
  let depth = 0;
  let cur = "";
  for (const ch of s) {
    if (ch === "[" || ch === "(") depth++;
    else if (ch === "]" || ch === ")") depth--;
    if (ch === sep && depth === 0) {
      out.push(cur.trim());
      cur = "";
    } else cur += ch;
  }
  if (cur.trim() !== "") out.push(cur.trim());
  return out;
}

function _parse(ann) {
  if (ann === null || ann === undefined) return { kind: "any" };
  if (typeof ann === "string") return _parse_string(ann.trim());
  if (Array.isArray(ann)) return { kind: "union", alts: ann.map(_parse) };
  if (ann && ann.__annotated__) {
    const t = _parse(ann.type);
    const before = [];
    const after = [];
    for (const m of ann.metadata) {
      if (m && m.__before_validator__) before.push(m.__before_validator__);
      if (m && m.__after_validator__) after.push(m.__after_validator__);
    }
    return { kind: "annotated", inner: t, before, after };
  }
  if (typeof ann === "function") return _parse_class(ann);
  if (ann === null) return { kind: "none" };
  return { kind: "any" };
}

function _parse_class(cls) {
  if (cls === String) return { kind: "str" };
  if (cls === Number) return { kind: "float" };
  if (cls === Boolean) return { kind: "bool" };
  if (cls === Array) return { kind: "list", item: { kind: "any" } };
  if (cls === Object || cls === Map) return { kind: "dict", key: { kind: "any" }, value: { kind: "any" } };
  if (cls === Set) return { kind: "set", item: { kind: "any" } };
  if (cls === Uint8Array || cls === Buffer) return { kind: "bytes" };
  if (cls.__members__) return { kind: "enum", cls };
  if (cls.prototype instanceof BaseModel || cls === BaseModel) return { kind: "model", cls };
  return { kind: "class", cls };
}

function _parse_string(s) {
  if (s.includes("|")) {
    const parts = _split_top(s, "|");
    if (parts.length > 1) return { kind: "union", alts: parts.map(_parse_string) };
  }
  const m = /^([A-Za-z_][A-Za-z0-9_.]*)(?:\[(.*)\])?$/.exec(s);
  if (!m) return { kind: "any" };
  const name = m[1].replace(/^typing\./, "");
  const args = m[2] !== undefined ? _split_top(m[2], ",") : [];
  switch (name) {
    case "None":
    case "NoneType":
      return { kind: "none" };
    case "Any":
    case "object":
    case "Callable":
    case "Awaitable":
    case "Coroutine":
    case "Iterable":
    case "Iterator":
    case "Type":
    case "type":
      return { kind: "any" };
    case "int":
      return { kind: "int" };
    case "float":
      return { kind: "float" };
    case "str":
      return { kind: "str" };
    case "bool":
      return { kind: "bool" };
    case "bytes":
    case "bytearray":
      return { kind: "bytes" };
    case "list":
    case "List":
    case "Sequence":
    case "MutableSequence":
    case "deque":
      return { kind: "list", item: args.length ? _parse_string(args[0]) : { kind: "any" } };
    case "set":
    case "Set":
    case "frozenset":
    case "FrozenSet":
      return { kind: "set", item: args.length ? _parse_string(args[0]) : { kind: "any" } };
    case "tuple":
    case "Tuple":
      return { kind: "tuple", items: args.map(_parse_string) };
    case "dict":
    case "Dict":
    case "Mapping":
    case "MutableMapping":
      return {
        kind: "dict",
        key: args.length ? _parse_string(args[0]) : { kind: "any" },
        value: args.length > 1 ? _parse_string(args[1]) : { kind: "any" },
      };
    case "Optional":
      return { kind: "union", alts: [_parse_string(args[0]), { kind: "none" }] };
    case "Union":
      return { kind: "union", alts: args.map(_parse_string) };
    case "Literal":
      return { kind: "literal", values: args.map((a) => a.replace(/^["']|["']$/g, "")) };
    default: {
      const cls = _type_registry.get(name);
      if (cls) return _parse_class(cls);
      return { kind: "any", name };
    }
  }
}

const _TRUE_STRINGS = new Set(["true", "t", "yes", "y", "on", "1"]);
const _FALSE_STRINGS = new Set(["false", "f", "no", "n", "off", "0"]);

function _validate(t, v, cfg) {
  switch (t.kind) {
    case "any":
      return v === undefined ? null : v;
    case "none":
      if (v === null || v === undefined) return null;
      throw new _ValidationItem("none_required", "Input should be None", v);
    case "int":
      return _v_int(v);
    case "float":
      return _v_float(v);
    case "str":
      return _v_str(v);
    case "bool":
      return _v_bool(v);
    case "bytes":
      if (v instanceof Uint8Array) return v;
      if (typeof v === "string") return Buffer.from(v, "utf8");
      throw new _ValidationItem("bytes_type", "Input should be a valid bytes", v);
    case "list": {
      let arr;
      if (Array.isArray(v)) arr = v;
      else if (v instanceof Set) arr = [...v];
      else if (v && typeof v[Symbol.iterator] === "function" && typeof v !== "string" && !isdict(v) && !(v instanceof Uint8Array)) arr = [...v];
      else throw new _ValidationItem("list_type", "Input should be a valid list", v);
      if (t.item.kind === "any") return v instanceof PyTuple || !Array.isArray(v) ? [...arr] : arr;
      const out = [];
      for (let i = 0; i < arr.length; i++) {
        try {
          out.push(_validate(t.item, arr[i], cfg));
        } catch (e) {
          if (e instanceof _ValidationItem) {
            e.loc_suffix = [i, ...(e.loc_suffix ?? [])];
          }
          throw e;
        }
      }
      return out;
    }
    case "set": {
      let arr;
      if (v instanceof Set || Array.isArray(v)) arr = [...v];
      else throw new _ValidationItem("set_type", "Input should be a valid set", v);
      return new Set(arr.map((x) => _validate(t.item, x, cfg)));
    }
    case "tuple": {
      if (!Array.isArray(v)) throw new _ValidationItem("tuple_type", "Input should be a valid tuple", v);
      if (t.items.length === 0 || (t.items.length === 2 && t.items[1].kind === "any" && t.items[1].name === "...")) return PyTuple.from_iterable(v);
      return PyTuple.from_iterable(v.map((x, i) => _validate(t.items[Math.min(i, t.items.length - 1)], x, cfg)));
    }
    case "dict": {
      let entries;
      if (isdict(v)) entries = dict_items(v);
      else if (v && typeof v.toDict === "function" && typeof v.items === "function") entries = v.items();
      else throw new _ValidationItem("dict_type", "Input should be a valid dictionary", v);
      if (t.key.kind === "any" && t.value.kind === "any") {
        return isdict(v) ? v : dict_from_entries(entries);
      }
      const out = entries.map(([k, val]) => [_validate(t.key, k, cfg), _validate(t.value, val, cfg)]);
      if (v instanceof Map) return new Map(out);
      return dict_from_entries(out);
    }
    case "enum": {
      if (v instanceof t.cls) return cfg.use_enum_values ? v.value : v;
      try {
        const m = t.cls(v);
        return cfg.use_enum_values ? m.value : m;
      } catch {
        const vals = t.cls.values().map((x) => repr(x));
        const expected = vals.length === 1 ? vals[0] : vals.slice(0, -1).join(", ") + " or " + vals[vals.length - 1];
        throw new _ValidationItem("enum", `Input should be ${expected}`, v);
      }
    }
    case "model": {
      if (v instanceof t.cls) return v;
      if (isdict(v)) return new t.cls(v instanceof Map ? Object.fromEntries(v) : v);
      // dict subclasses (``DotMap``: the ``laila.args`` subtree) validate like dicts
      if (v && typeof v.toDict === "function" && typeof v.items === "function") return new t.cls(dict_from_entries(v.items()));
      throw new _ValidationItem("model_type", `Input should be a valid dictionary or instance of ${t.cls.name}`, v);
    }
    case "class": {
      if (v instanceof t.cls) return v;
      throw new _ValidationItem("is_instance_of", `Input should be an instance of ${t.cls.name}`, v);
    }
    case "literal": {
      if (t.values.includes(String(v))) return v;
      throw new _ValidationItem("literal_error", `Input should be ${t.values.map((x) => repr(x)).join(" or ")}`, v);
    }
    case "annotated": {
      for (const fn of t.before) v = fn(v);
      v = _validate(t.inner, v, cfg);
      for (const fn of t.after) v = fn(v);
      return v;
    }
    case "union": {
      if ((v === null || v === undefined) && t.alts.some((a) => a.kind === "none")) return null;
      // smart mode: an alternative that accepts the value *without coercion* wins
      for (const a of t.alts) if (_exact(a, v)) return _validate(a, v, cfg);
      let first = null;
      for (const a of t.alts) {
        if (a.kind === "none") continue;
        try {
          return _validate(a, v, cfg);
        } catch (e) {
          if (!(e instanceof _ValidationItem)) throw e;
          if (!first) first = e;
        }
      }
      throw first ?? new _ValidationItem("union", "Input should match one of the alternatives", v);
    }
    default:
      return v;
  }
}

function _exact(t, v) {
  switch (t.kind) {
    case "any":
      return true;
    case "int":
      return (is_integral(v) && !(v instanceof PyFloat)) || typeof v === "bigint";
    case "float":
      return typeof v === "number" || v instanceof PyFloat;
    case "str":
      return typeof v === "string";
    case "bool":
      return typeof v === "boolean";
    case "bytes":
      return v instanceof Uint8Array;
    case "list":
      return Array.isArray(v) && !(v instanceof PyTuple);
    case "tuple":
      return v instanceof PyTuple;
    case "set":
      return v instanceof Set;
    case "dict":
      return isdict(v);
    case "enum":
    case "model":
    case "class":
      return v instanceof t.cls;
    case "none":
      return v === null || v === undefined;
    default:
      return false;
  }
}

function _v_int(v) {
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "bigint") return v;
  if (v instanceof PyFloat || v instanceof Number) v = v.valueOf();
  if (typeof v === "number") {
    if (Number.isInteger(v)) return v;
    if (Number.isFinite(v)) throw new _ValidationItem("int_from_float", "Input should be a valid integer, got a number with a fractional part", v);
    throw new _ValidationItem("finite_number", "Input should be a finite number", v);
  }
  if (typeof v === "string") {
    const s = v.trim();
    if (/^[+-]?\d+$/.test(s)) {
      const n = Number(s);
      return Number.isSafeInteger(n) ? n : BigInt(s);
    }
    if (/^[+-]?\d+\.0*$/.test(s)) return Number(s);
    throw new _ValidationItem("int_parsing", "Input should be a valid integer, unable to parse string as an integer", v);
  }
  if (v instanceof Uint8Array) return _v_int(Buffer.from(v).toString("utf8"));
  throw new _ValidationItem("int_type", "Input should be a valid integer", v);
}

function _v_float(v) {
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  if (v instanceof PyFloat || v instanceof Number) return v.valueOf();
  if (typeof v === "string") {
    const s = v.trim();
    const l = s.toLowerCase();
    if (["inf", "+inf", "infinity", "+infinity"].includes(l)) return Infinity;
    if (["-inf", "-infinity"].includes(l)) return -Infinity;
    if (["nan", "+nan", "-nan"].includes(l)) return NaN;
    if (/^[+-]?(\d+\.?\d*(e[+-]?\d+)?|\.\d+(e[+-]?\d+)?)$/i.test(s)) return Number(s);
    throw new _ValidationItem("float_parsing", "Input should be a valid number, unable to parse string as a number", v);
  }
  throw new _ValidationItem("float_type", "Input should be a valid number", v);
}

function _v_str(v) {
  if (typeof v === "string") return v;
  if (is_enum_member(v)) return v; // str-mixin enum members are strs
  if (v instanceof Uint8Array) return Buffer.from(v).toString("utf8");
  throw new _ValidationItem("string_type", "Input should be a valid string", v);
}

function _v_bool(v) {
  if (typeof v === "boolean") return v;
  if (typeof v === "number" || typeof v === "bigint") {
    if (Number(v) === 1) return true;
    if (Number(v) === 0) return false;
    throw new _ValidationItem("bool_parsing", "Input should be a valid boolean, unable to interpret input", v);
  }
  if (typeof v === "string") {
    const l = v.trim().toLowerCase();
    if (_TRUE_STRINGS.has(l)) return true;
    if (_FALSE_STRINGS.has(l)) return false;
    throw new _ValidationItem("bool_parsing", "Input should be a valid boolean, unable to interpret input", v);
  }
  throw new _ValidationItem("bool_type", "Input should be a valid boolean", v);
}

function _check_constraints(info, v) {
  if (v === null || v === undefined) return;
  const n = typeof v === "number" ? v : typeof v === "bigint" ? Number(v) : v instanceof PyFloat ? v.valueOf() : null;
  for (const m of info.metadata) {
    if ("ge" in m && n !== null && !(n >= m.ge)) throw new _ValidationItem("greater_than_equal", `Input should be greater than or equal to ${_num(m.ge)}`, v);
    if ("le" in m && n !== null && !(n <= m.le)) throw new _ValidationItem("less_than_equal", `Input should be less than or equal to ${_num(m.le)}`, v);
    if ("gt" in m && n !== null && !(n > m.gt)) throw new _ValidationItem("greater_than", `Input should be greater than ${_num(m.gt)}`, v);
    if ("lt" in m && n !== null && !(n < m.lt)) throw new _ValidationItem("less_than", `Input should be less than ${_num(m.lt)}`, v);
    const length = typeof v === "string" || Array.isArray(v) ? v.length : v instanceof Map || v instanceof Set ? v.size : null;
    if ("min_length" in m && length !== null && length < m.min_length)
      throw new _ValidationItem("too_short", `Value should have at least ${m.min_length} item${m.min_length === 1 ? "" : "s"} after validation, not ${length}`, v);
    if ("max_length" in m && length !== null && length > m.max_length)
      throw new _ValidationItem("too_long", `Value should have at most ${m.max_length} item${m.max_length === 1 ? "" : "s"} after validation, not ${length}`, v);
    if ("pattern" in m && typeof v === "string" && !new RegExp(m.pattern).test(v))
      throw new _ValidationItem("string_pattern_mismatch", `String should match pattern '${m.pattern}'`, v);
  }
}
function _num(x) {
  return Number.isInteger(x) ? String(x) : String(x);
}

// --------------------------------------------------------------------------
// Class-level declaration helpers
// --------------------------------------------------------------------------

const kOwnFields = Symbol("pydantic.own_fields");
const kOwnPrivate = Symbol("pydantic.own_private");
const kBefore = Symbol("pydantic.before_validators");
const kAfter = Symbol("pydantic.after_validators");
const kFieldValidators = Symbol("pydantic.field_validators");
const kSubclasses = Symbol("pydantic.subclasses");
const kFinalized = Symbol("pydantic.finalized");
const kFieldsCache = Symbol("pydantic.fields_cache");
const kPrivateCache = Symbol("pydantic.private_cache");
const kConfigCache = Symbol("pydantic.config_cache");
const kAssignStore = Symbol("pydantic.assign_store"); // ``validate_assignment`` backing store

function _own(cls, key, make) {
  if (!Object.prototype.hasOwnProperty.call(cls, key)) Object.defineProperty(cls, key, { value: make(), writable: true });
  return cls[key];
}

/**
 * Mark ``cls`` as a model: registers it with its parent's ``__subclasses__``
 * and in the annotation type registry. Called implicitly by the declaration
 * helpers; call it explicitly (``static { finalize_model(this); }``) for
 * subclasses that declare nothing new.
 */
export function finalize_model(cls) {
  if (Object.prototype.hasOwnProperty.call(cls, kFinalized)) return cls;
  Object.defineProperty(cls, kFinalized, { value: true });
  const parent = Object.getPrototypeOf(cls);
  if (parent && parent !== Function.prototype && (parent === BaseModel || parent.prototype instanceof BaseModel)) {
    const list = _own(parent, kSubclasses, () => []);
    if (!list.includes(cls)) list.push(cls);
  }
  if (cls.name && !_type_registry.has(cls.name)) _type_registry.set(cls.name, cls);
  // invalidate caches down the chain (declaration order is static-block time,
  // before any instantiation, so this is mostly defensive)
  for (const k of [kFieldsCache, kPrivateCache, kConfigCache]) if (Object.prototype.hasOwnProperty.call(cls, k)) delete cls[k];
  return cls;
}

/**
 * Declare fields. ``spec`` maps name -> ``[annotation, FieldInfo | default]`` or
 * ``[annotation]`` (required) or a bare ``FieldInfo`` with ``annotation`` set.
 */
export function define_fields(cls, spec) {
  finalize_model(cls);
  const own = _own(cls, kOwnFields, () => new Map());
  for (const [name, decl] of Object.entries(spec)) {
    let info;
    let annotation = null;
    if (decl instanceof FieldInfo) {
      info = decl;
      annotation = decl.annotation;
    } else if (Array.isArray(decl)) {
      annotation = decl[0];
      if (decl.length === 1) info = new FieldInfo({});
      else if (decl[1] instanceof FieldInfo) info = decl[1];
      else info = new FieldInfo({ default: decl[1] });
    } else {
      throw new PyTypeError(`invalid field declaration for ${cls.name}.${name}`);
    }
    info.annotation = annotation;
    info._type = null;
    own.set(name, info);
  }
  if (Object.prototype.hasOwnProperty.call(cls, kFieldsCache)) delete cls[kFieldsCache];
  return cls;
}

/** Declare private attributes: ``{ _name: PrivateAttr({...}) }``. */
export function define_private(cls, spec) {
  finalize_model(cls);
  const own = _own(cls, kOwnPrivate, () => new Map());
  for (const [name, attr] of Object.entries(spec)) {
    const a = attr instanceof ModelPrivateAttr ? attr : new ModelPrivateAttr({ default: attr });
    own.set(name, a);
    // accessor routing ``self._name`` to ``__pydantic_private__``
    if (!Object.prototype.hasOwnProperty.call(cls.prototype, name)) {
      Object.defineProperty(cls.prototype, name, {
        get() {
          const p = this.__pydantic_private__;
          if (!p || !(name in p) || p[name] === PydanticUndefined) {
            if (p && name in p) throw new AttributeError(`'${this.constructor.name}' object has no attribute '${name}'`);
            return undefined;
          }
          return p[name];
        },
        set(v) {
          let p = this.__pydantic_private__;
          if (!p) {
            p = {};
            Object.defineProperty(this, "__pydantic_private__", { value: p, writable: true, configurable: true });
          }
          p[name] = v;
        },
        configurable: true,
      });
    }
  }
  if (Object.prototype.hasOwnProperty.call(cls, kPrivateCache)) delete cls[kPrivateCache];
  return cls;
}

/** ``@model_validator(mode="before"|"after")`` */
export function model_validator(cls, mode, fn) {
  finalize_model(cls);
  if (mode === "before") _own(cls, kBefore, () => []).push(fn);
  else if (mode === "after") _own(cls, kAfter, () => []).push(fn);
  else throw new ValueError(`unsupported model_validator mode: ${mode}`);
  return cls;
}

/** ``@field_validator("a", "b", mode="after")`` -> ``field_validator(cls, ["a","b"], fn, {mode})`` */
export function field_validator(cls, names, fn, opts = {}) {
  finalize_model(cls);
  const mode = opts.mode ?? "after";
  const map = _own(cls, kFieldValidators, () => new Map());
  for (const n of Array.isArray(names) ? names : [names]) {
    if (!map.has(n)) map.set(n, []);
    map.get(n).push({ mode, fn });
  }
  return cls;
}

function _mro(cls) {
  const chain = [];
  let c = cls;
  while (c && c !== Function.prototype && c !== Object) {
    chain.push(c);
    c = Object.getPrototypeOf(c);
  }
  return chain; // most-derived first
}

function _collect_fields(cls) {
  if (Object.prototype.hasOwnProperty.call(cls, kFieldsCache)) return cls[kFieldsCache];
  const out = new Map();
  for (const c of _mro(cls).reverse()) {
    const own = Object.prototype.hasOwnProperty.call(c, kOwnFields) ? c[kOwnFields] : null;
    if (own) for (const [k, v] of own) out.set(k, v); // overriding keeps parent position
  }
  Object.defineProperty(cls, kFieldsCache, { value: out, configurable: true });
  return out;
}

function _collect_private(cls) {
  if (Object.prototype.hasOwnProperty.call(cls, kPrivateCache)) return cls[kPrivateCache];
  const out = new Map();
  for (const c of _mro(cls).reverse()) {
    const own = Object.prototype.hasOwnProperty.call(c, kOwnPrivate) ? c[kOwnPrivate] : null;
    if (own) for (const [k, v] of own) out.set(k, v);
  }
  Object.defineProperty(cls, kPrivateCache, { value: out, configurable: true });
  return out;
}

function _collect_config(cls) {
  if (Object.prototype.hasOwnProperty.call(cls, kConfigCache)) return cls[kConfigCache];
  const out = {};
  for (const c of _mro(cls).reverse()) {
    if (Object.prototype.hasOwnProperty.call(c, "model_config") && c.model_config) Object.assign(out, c.model_config);
  }
  Object.defineProperty(cls, kConfigCache, { value: out, configurable: true });
  return out;
}

function _collect_list(cls, key) {
  const out = [];
  for (const c of _mro(cls).reverse()) if (Object.prototype.hasOwnProperty.call(c, key)) out.push(...c[key]);
  return out;
}

function _collect_field_validators(cls, name) {
  const out = [];
  for (const c of _mro(cls).reverse()) {
    if (Object.prototype.hasOwnProperty.call(c, kFieldValidators) && c[kFieldValidators].has(name)) out.push(...c[kFieldValidators].get(name));
  }
  return out;
}

// --------------------------------------------------------------------------
// BaseModel
// --------------------------------------------------------------------------

export class BaseModel {
  static model_config = ConfigDict({});

  /** ``cls.model_fields`` -> ordered ``{name: FieldInfo}`` (parents first). */
  static get model_fields() {
    return Object.fromEntries(_collect_fields(this));
  }
  /** ``cls.__private_attributes__`` */
  static get __private_attributes__() {
    return Object.fromEntries(_collect_private(this));
  }
  /** Effective (inherited + own) config. */
  static get model_config_effective() {
    return _collect_config(this);
  }
  /** ``cls.__subclasses__()`` -- direct subclasses that were finalized. */
  static __subclasses__() {
    return Object.prototype.hasOwnProperty.call(this, kSubclasses) ? [...this[kSubclasses]] : [];
  }
  static get __name__() {
    return this.name;
  }

  /**
   * ``Model(**data)`` -> ``new Model({...})``.
   *
   * Passing :data:`SKIP_VALIDATION` reproduces ``cls.__new__(cls)`` for
   * subclasses whose ``__init__`` deliberately bypasses pydantic (they
   * populate the instance themselves, e.g. through :func:`construct_into`).
   * @param {object|Map|null|symbol} [data]
   */
  constructor(data = {}) {
    const cls = new.target;
    if (data === SKIP_VALIDATION) return;
    _validate_into(this, cls, normalize_kwargs(data, cls));
  }

  /** Hook called after validation; subclasses chain ``super.model_post_init(ctx)``. */
  model_post_init(_context) {}

  /** ``model_fields_set`` */
  get model_fields_set() {
    return this.__pydantic_fields_set__;
  }
  get model_extra() {
    return this.__pydantic_extra__;
  }

  /**
   * ``model_dump(mode="python", include=None, exclude=None, exclude_none=False, ...)``
   */
  model_dump(opts = {}) {
    const { mode = "python", include = null, exclude = null, exclude_none = false, exclude_unset = false, exclude_defaults = false } = opts;
    const inc = include ? new Set(include) : null;
    const exc = exclude ? new Set(exclude) : new Set();
    const out = {};
    const fields = _collect_fields(this.constructor);
    for (const [name, info] of fields) {
      if (info.exclude) continue;
      if (inc && !inc.has(name)) continue;
      if (exc.has(name)) continue;
      if (exclude_unset && !this.__pydantic_fields_set__.has(name)) continue;
      const v = this[name];
      if (exclude_none && (v === null || v === undefined)) continue;
      if (exclude_defaults && !info.is_required && py_eq(v, info.get_default())) continue;
      out[name] = _dump_value(v, mode);
    }
    if (this.__pydantic_extra__) for (const [k, v] of Object.entries(this.__pydantic_extra__)) out[k] = _dump_value(v, mode);
    return out;
  }

  /** ``model_dump_json(indent=None)`` */
  model_dump_json(opts = {}) {
    return JSON.stringify(this.model_dump({ ...opts, mode: "json" }), null, opts.indent ?? undefined);
  }

  /** ``model_copy(update=None, deep=False)`` */
  model_copy(opts = {}) {
    const { update = null, deep = false } = opts;
    const cls = this.constructor;
    const m = Object.create(cls.prototype);
    const fields = _collect_fields(cls);
    for (const name of fields.keys()) {
      if (!(name in this)) continue;
      m[name] = deep ? deepcopy(this[name]) : this[name];
    }
    // undeclared own properties (e.g. ``_local_lock`` set via object.__setattr__)
    for (const k of Object.keys(this)) if (!fields.has(k) && !(k in m)) m[k] = deep ? deepcopy(this[k]) : this[k];
    const fs = new Set(this.__pydantic_fields_set__);
    const priv = this.__pydantic_private__ ? (deep ? deepcopy(this.__pydantic_private__) : { ...this.__pydantic_private__ }) : null;
    const extra = this.__pydantic_extra__ ? (deep ? deepcopy(this.__pydantic_extra__) : { ...this.__pydantic_extra__ }) : null;
    _define_hidden(m, fs, extra, priv);
    if (update) {
      for (const [k, v] of dict_items(update)) {
        if (fields.has(k)) {
          m[k] = v;
          fs.add(k);
        } else if (extra) extra[k] = v;
        else m[k] = v;
      }
    }
    return m;
  }

  /** ``Model.model_construct(_fields_set=None, **values)`` -> ``Model.model_construct(values, {_fields_set})`` */
  static model_construct(values = {}, opts = {}) {
    return construct_into(Object.create(this.prototype), this, values, opts);
  }

  /** ``Model.model_validate(obj)`` */
  static model_validate(obj) {
    if (obj instanceof this) return obj;
    return new this(obj);
  }

  /** Field-wise equality (same class, same fields, same private state). */
  __eq__(other) {
    if (other === this) return true;
    if (!(other instanceof BaseModel) || other.constructor !== this.constructor) return false;
    for (const name of _collect_fields(this.constructor).keys()) if (!py_eq(this[name], other[name])) return false;
    const pa = this.__pydantic_private__ ?? {};
    const pb = other.__pydantic_private__ ?? {};
    if (!py_eq(pa, pb)) return false;
    return py_eq(this.__pydantic_extra__ ?? null, other.__pydantic_extra__ ?? null);
  }

  __repr_args__() {
    const parts = [];
    for (const [name, info] of _collect_fields(this.constructor)) {
      if (info.repr === false) continue;
      parts.push([name, this[name]]);
    }
    if (this.__pydantic_extra__) for (const [k, v] of Object.entries(this.__pydantic_extra__)) parts.push([k, v]);
    return parts;
  }
  __repr__() {
    return `${this.constructor.name}(${this.__repr_args__().map(([k, v]) => `${k}=${repr(v)}`).join(", ")})`;
  }
  __str__() {
    return this.__repr_args__().map(([k, v]) => `${k}=${repr(v)}`).join(" ");
  }
  toString() {
    return this.__str__();
  }
  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }

  /** ``copy.copy(model)`` */
  __copy__() {
    return this.model_copy();
  }
  /** ``copy.deepcopy(model)`` */
  __deepcopy__(memo) {
    const m = this.model_copy({ deep: false });
    memo?.set(this, m);
    const fields = _collect_fields(this.constructor);
    for (const name of fields.keys()) if (name in m) m[name] = deepcopy(this[name], memo);
    for (const k of Object.keys(m)) if (!fields.has(k)) m[k] = deepcopy(this[k], memo);
    if (m.__pydantic_private__) {
      const p = {};
      for (const [k, v] of Object.entries(this.__pydantic_private__)) p[k] = deepcopy(v, memo);
      m.__pydantic_private__ = p;
    }
    return m;
  }

  /** Python ``__getstate__`` (pickle support hook used by subclasses). */
  __getstate__() {
    const d = {};
    for (const k of Object.keys(this)) d[k] = this[k];
    return {
      __dict__: d,
      __pydantic_extra__: this.__pydantic_extra__,
      __pydantic_fields_set__: new Set(this.__pydantic_fields_set__),
      __pydantic_private__: this.__pydantic_private__ ? { ...this.__pydantic_private__ } : null,
    };
  }
  __setstate__(state) {
    for (const [k, v] of Object.entries(state.__dict__ ?? {})) this[k] = v;
    _define_hidden(this, new Set(state.__pydantic_fields_set__ ?? []), state.__pydantic_extra__ ?? null, state.__pydantic_private__ ?? null);
  }
}

/** Sentinel accepted by ``new Model(SKIP_VALIDATION)`` (see the constructor). */
export const SKIP_VALIDATION = Symbol("pydantic.skip_validation");

/**
 * Turn whatever was passed as ``**data`` into a fresh plain object.
 * @param {object|Map|null|undefined} data
 * @param {Function} [cls] for the error message
 */
export function normalize_kwargs(data, cls = null) {
  if (data === null || data === undefined) return {};
  if (data instanceof Map) return Object.fromEntries(data);
  if (data && typeof data.toDict === "function" && typeof data.items === "function") return Object.fromEntries(data.items());
  if (!is_plain_object(data)) {
    // ``Model(**obj)`` with a model or arbitrary object: take own enumerable props
    if (typeof data === "object") return { ...data };
    throw new PyTypeError(`${cls ? cls.name : "Model"}() argument after ** must be a mapping, not ${type_name(data)}`);
  }
  return { ...data };
}

/**
 * The body of ``model_construct`` applied onto an existing object ``m``
 * (so a constructor that bypasses validation can populate ``this``).
 */
export function construct_into(m, cls, values = {}, opts = {}) {
  values = normalize_kwargs(values, cls);
  const fields = _collect_fields(cls);
  const fs = new Set(opts._fields_set ?? Object.keys(values).filter((k) => fields.has(k)));
  const extra = _collect_config(cls).extra === "allow" ? {} : null;
  for (const [name, info] of fields) {
    if (name in values) m[name] = values[name];
    else if (!info.is_required) m[name] = info.get_default();
  }
  for (const [k, v] of Object.entries(values)) {
    if (!fields.has(k)) {
      if (extra) extra[k] = v;
      else m[k] = v;
    }
  }
  const priv = _init_private(cls);
  _define_hidden(m, fs, extra, priv);
  if (cls.prototype.model_post_init !== BaseModel.prototype.model_post_init) m.model_post_init(null);
  return m;
}

/** ``Object.defineProperty``-based install of the three hidden pydantic slots. */
export function define_hidden(m, fields_set, extra, priv) {
  _define_hidden(m, fields_set, extra, priv);
}

/** Collected (inherited + own) private attribute declarations as a Map. */
export function private_attributes(cls) {
  return _collect_private(cls);
}

function _dump_value(v, mode) {
  if (v === null || v === undefined) return null;
  if (v instanceof BaseModel) return v.model_dump({ mode });
  if (is_enum_member(v)) return mode === "json" ? v.value : v;
  if (v instanceof PyTuple) return mode === "json" ? v.map((x) => _dump_value(x, mode)) : PyTuple.from_iterable(v.map((x) => _dump_value(x, mode)));
  if (Array.isArray(v)) return v.map((x) => _dump_value(x, mode));
  if (v instanceof Map) {
    const m = new Map();
    for (const [k, x] of v) m.set(k, _dump_value(x, mode));
    return mode === "json" ? Object.fromEntries(m) : m;
  }
  if (v instanceof Set) return mode === "json" ? [...v].map((x) => _dump_value(x, mode)) : new Set([...v].map((x) => _dump_value(x, mode)));
  if (is_plain_object(v)) {
    const o = {};
    for (const [k, x] of Object.entries(v)) o[k] = _dump_value(x, mode);
    return o;
  }
  if (mode === "json" && v instanceof Uint8Array) return Buffer.from(v).toString("utf8");
  return v;
}

function _define_hidden(m, fields_set, extra, priv) {
  Object.defineProperty(m, "__pydantic_fields_set__", { value: fields_set, writable: true, configurable: true });
  Object.defineProperty(m, "__pydantic_extra__", { value: extra, writable: true, configurable: true });
  Object.defineProperty(m, "__pydantic_private__", { value: priv, writable: true, configurable: true });
}

function _init_private(cls) {
  const privs = _collect_private(cls);
  if (privs.size === 0) return null;
  const p = {};
  for (const [name, attr] of privs) {
    const v = attr.get_default();
    if (v !== PydanticUndefined) p[name] = v;
  }
  return p;
}

function _validate_into(self, cls, data) {
  const cfg = _collect_config(cls);
  // 1. before validators (classmethods): most-derived last, like pydantic's wrapping
  for (const fn of _collect_list(cls, kBefore)) {
    const r = fn.call(cls, cls, data);
    if (r !== undefined) data = r;
    if (data instanceof Map) data = Object.fromEntries(data);
  }
  // 2. fields
  const fields = _collect_fields(cls);
  const values = {};
  const fields_set = new Set();
  const errors = [];
  for (const [name, info] of fields) {
    const key = Object.prototype.hasOwnProperty.call(data, name) ? name : info.alias && Object.prototype.hasOwnProperty.call(data, info.alias) ? info.alias : null;
    if (key !== null) {
      let raw = data[key];
      delete data[key];
      fields_set.add(name);
      try {
        values[name] = _validate_field(cls, name, info, raw, cfg);
      } catch (e) {
        if (e instanceof _ValidationItem) errors.push({ type: e.type, loc: [name, ...(e.loc_suffix ?? [])], msg: e.message, input: e.input });
        else throw e;
      }
    } else if (!info.is_required) {
      values[name] = info.get_default();
    } else {
      errors.push({ type: "missing", loc: [name], msg: "Field required", input: data });
    }
  }
  // 3. extra
  const remaining = Object.keys(data);
  let extra = null;
  const extra_mode = cfg.extra ?? "ignore";
  if (remaining.length) {
    if (extra_mode === "forbid") for (const k of remaining) errors.push({ type: "extra_forbidden", loc: [k], msg: "Extra inputs are not permitted", input: data[k] });
    else if (extra_mode === "allow") {
      extra = {};
      for (const k of remaining) extra[k] = data[k];
    }
  } else if (extra_mode === "allow") extra = {};
  if (errors.length) throw new ValidationError(cls.name, errors);
  // 4. assign
  if (cfg.validate_assignment) _install_validated_fields(self, fields, values);
  else Object.assign(self, values);
  _define_hidden(self, fields_set, extra, _init_private(cls));
  // 5. model_post_init
  self.model_post_init(null);
  // 6. after validators (instance methods)
  for (const fn of _collect_list(cls, kAfter)) fn.call(self, self);
}

function _validate_field(cls, name, info, raw, cfg) {
  if (!info._type) info._type = _parse(info.annotation);
  let v = raw;
  const validators = _collect_field_validators(cls, name);
  for (const { mode, fn } of validators) if (mode === "before") v = fn.call(cls, cls, v);
  for (const fn of info.before_validators) v = fn(v);
  v = _validate(info._type, v, cfg);
  _check_constraints(info, v);
  for (const fn of info.after_validators) v = fn(v);
  for (const { mode, fn } of validators) if (mode === "after") v = fn.call(cls, cls, v);
  return v;
}

/**
 * ``ConfigDict(validate_assignment=True)``: fields live in a hidden store
 * behind enumerable accessors so ``model.field = value`` re-validates the
 * field (``ValidationError`` on failure), records it in ``model_fields_set``
 * and re-runs the model's ``after`` validators, like pydantic.
 */
function _install_validated_fields(self, fields, values) {
  const store = {};
  Object.defineProperty(self, kAssignStore, { value: store, writable: true, configurable: true });
  for (const [name] of fields) {
    store[name] = values[name];
    Object.defineProperty(self, name, {
      enumerable: true,
      configurable: true,
      get() {
        return this[kAssignStore][name];
      },
      set(v) {
        _validated_assign(this, name, v);
      },
    });
  }
}

function _validated_assign(self, name, raw) {
  const cls = self.constructor;
  const info = _collect_fields(cls).get(name);
  let v;
  try {
    v = _validate_field(cls, name, info, raw, _collect_config(cls));
  } catch (e) {
    if (e instanceof _ValidationItem) throw new ValidationError(cls.name, [{ type: e.type, loc: [name, ...(e.loc_suffix ?? [])], msg: e.message, input: e.input }]);
    throw e;
  }
  self[kAssignStore][name] = v;
  if (self.__pydantic_fields_set__) self.__pydantic_fields_set__.add(name);
  for (const fn of _collect_list(cls, kAfter)) fn.call(self, self);
}

/**
 * ``object.__setattr__(model, name, value)`` -- writes straight to the field
 * store of a ``validate_assignment`` model (skipping validation); a plain
 * property write everywhere else.
 */
export function object_setattr(obj, name, value) {
  const store = obj[kAssignStore];
  if (store && Object.prototype.hasOwnProperty.call(store, name)) store[name] = value;
  else obj[name] = value;
}

/** Validate a single value against a Python type annotation (``TypeAdapter``-like). */
export function validate_value(annotation, value, cfg = {}) {
  try {
    return _validate(_parse(annotation), value, cfg);
  } catch (e) {
    if (e instanceof _ValidationItem) throw new ValidationError(_ann_repr(annotation), [{ type: e.type, loc: [], msg: e.message, input: e.input }]);
    throw e;
  }
}

export { shallow_copy as _copy, enum_value as _enum_value };
