/**
 * ``abc.ABC`` / ``@abstractmethod`` instantiation guard.
 *
 * Python refuses to instantiate a class that still has abstract methods
 * (``TypeError: Can't instantiate abstract class X without an implementation
 * for abstract methods 'a', 'b'``). JS has no ABCMeta, so abstract bases call
 * ``check_abstract(new.target, Base, ...)`` as the first statement of their
 * constructor: a method is still abstract when the instantiated class
 * inherits the base's placeholder unchanged.
 */
import { TypeError as PyTypeError } from "./errors.js";

/**
 * @param {Function} cls The class being instantiated (``new.target``).
 * @param {Function} base The abstract base declaring the placeholders.
 * @param {string[]} methods Abstract instance-method names.
 * @param {string[]} [classmethods] Abstract classmethod (static) names.
 */
export function check_abstract(cls, base, methods, classmethods = []) {
  const missing = [];
  for (const name of methods) if (cls.prototype[name] === base.prototype[name]) missing.push(name);
  for (const name of classmethods) if (cls[name] === base[name]) missing.push(name);
  if (!missing.length) return;
  missing.sort();
  const quoted = missing.map((n) => `'${n}'`).join(", ");
  throw new PyTypeError(`Can't instantiate abstract class ${cls.name} without an implementation for abstract method${missing.length > 1 ? "s" : ""} ${quoted}`);
}
