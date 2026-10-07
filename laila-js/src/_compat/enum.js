/**
 * Python ``enum`` for the ``class X(str, Enum)`` pattern laila uses.
 *
 * Members are frozen ``String`` wrapper objects carrying ``name`` and
 * ``value``, so they behave like CPython's str-mixin members:
 *   - ``member == "finished"`` is true (``valueOf`` -> value)
 *   - ``String(member)`` / template literals give ``"FutureStatus.FINISHED"``
 *     (``str()`` of a 3.11+ str-mixin enum)
 *   - members are singletons, usable as Map keys
 *   - ``FutureStatus("finished")`` looks a member up by value
 *   - ``member instanceof FutureStatus`` works
 *
 * Usage:  ``export const FutureStatus = Enum("FutureStatus", { NOT_STARTED: "not_started", ... })``
 */
import { ValueError, KeyError } from "./errors.js";
import { repr } from "./pyrepr.js";

const kEnumClass = Symbol("enum.class");

class EnumMember extends String {
  constructor(cls, name, value) {
    super(value);
    Object.defineProperty(this, "name", { value: name, enumerable: true });
    Object.defineProperty(this, "value", { value, enumerable: true });
    Object.defineProperty(this, kEnumClass, { value: cls });
    Object.freeze(this);
  }
  toString() {
    return `${this[kEnumClass].__name__}.${this.name}`;
  }
  __str__() {
    return this.toString();
  }
  __repr__() {
    return `<${this[kEnumClass].__name__}.${this.name}: ${repr(this.value)}>`;
  }
  __eq__(other) {
    if (other instanceof EnumMember) return other === this;
    return other === this.value;
  }
  __hash__() {
    return this.value;
  }
  toJSON() {
    return this.value;
  }
  get [Symbol.toStringTag]() {
    return this[kEnumClass].__name__;
  }
}

/**
 * Build an enum class.
 * @param {string} name
 * @param {Record<string, any>} members ``{ NAME: value }`` in definition order
 */
export function Enum(name, members) {
  const by_value = new Map();
  const by_name = new Map();

  function cls(value) {
    // ``FutureStatus("finished")`` / ``FutureStatus(member)``
    if (value instanceof EnumMember && value[kEnumClass] === cls) return value;
    const key = value instanceof String ? value.valueOf() : value;
    if (by_value.has(key)) return by_value.get(key);
    throw new ValueError(`${repr(value)} is not a valid ${name}`);
  }
  Object.defineProperty(cls, "name", { value: name });
  Object.defineProperty(cls, "__name__", { value: name });
  Object.defineProperty(cls, Symbol.hasInstance, {
    value: (x) => x instanceof EnumMember && x[kEnumClass] === cls,
  });
  for (const [k, v] of Object.entries(members)) {
    const m = new EnumMember(cls, k, v);
    by_name.set(k, m);
    if (!by_value.has(v)) by_value.set(v, m); // aliases map to the first member
    Object.defineProperty(cls, k, { value: m, enumerable: true });
  }
  Object.defineProperty(cls, "__members__", { value: Object.freeze(Object.fromEntries(by_name)) });
  Object.defineProperty(cls, "members", { value: Object.freeze([...by_name.values()]) });
  Object.defineProperty(cls, Symbol.iterator, {
    value: function* () {
      for (const m of by_name.values()) yield m;
    },
  });
  Object.defineProperty(cls, "__getitem__", {
    value: (k) => {
      if (!by_name.has(k)) throw new KeyError(k);
      return by_name.get(k);
    },
  });
  Object.defineProperty(cls, "values", { value: () => [...by_value.keys()] });
  Object.defineProperty(cls, "names", { value: () => [...by_name.keys()] });
  Object.defineProperty(cls, "has_value", { value: (v) => by_value.has(v instanceof String ? v.valueOf() : v) });
  return Object.freeze(cls);
}

/** True for any enum member of any Enum created here. */
export function is_enum_member(x) {
  return x instanceof EnumMember;
}

/** ``member.value`` if ``x`` is a member, otherwise ``x`` (``use_enum_values`` helper). */
export function enum_value(x) {
  return x instanceof EnumMember ? x.value : x;
}

export { EnumMember };
