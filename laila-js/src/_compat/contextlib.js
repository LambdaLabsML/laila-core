/**
 * Python ``contextlib`` / ``with`` statement helpers.
 *
 * Every laila context manager exposes ``__enter__()`` / ``__exit__(exc_type,
 * exc, tb)`` (plus ``enter()`` / ``exit()`` aliases and ``Symbol.dispose`` so
 * ``using`` works). ``with_(cm, body)`` is the statement itself::
 *
 *   with obj.atomic() as locked:      ->   with_(obj.atomic(), (locked) => { ... })
 *       ...
 *
 * ``contextmanager(genfn)`` turns a one-``yield`` generator function into a
 * context-manager factory exactly like the Python decorator (the yielded
 * value is what ``__enter__`` returns; an exception raised in the body is
 * thrown into the generator so ``try/finally`` around the ``yield`` runs).
 */
import { RuntimeError } from "./errors.js";

/** Reproduce ``exc_type, exc, tb`` for ``__exit__``. */
export function exc_info(e) {
  if (e === null || e === undefined) return [null, null, null];
  return [e.constructor ?? null, e, e.stack ?? null];
}

/**
 * ``with cm as target: body(target)``.
 * @template T
 * @param {{__enter__: () => any, __exit__: (t: any, e: any, tb: any) => any}} cm
 * @param {(target: any) => T} body
 * @returns {T}
 */
export function with_(cm, body) {
  const target = cm.__enter__();
  let result;
  try {
    result = body(target);
  } catch (e) {
    const suppressed = cm.__exit__(...exc_info(e));
    if (suppressed === true) return undefined;
    throw e;
  }
  cm.__exit__(null, null, null);
  return result;
}

/** ``async with cm as target: await body(target)`` (``__aenter__`` / ``__aexit__``). */
export async function with_async(cm, body) {
  const target = await cm.__aenter__();
  let result;
  try {
    result = await body(target);
  } catch (e) {
    const suppressed = await cm.__aexit__(...exc_info(e));
    if (suppressed === true) return undefined;
    throw e;
  }
  await cm.__aexit__(null, null, null);
  return result;
}

class _GeneratorContextManager {
  constructor(gen) {
    this._gen = gen;
  }
  __enter__() {
    const r = this._gen.next();
    if (r.done) throw new RuntimeError("generator didn't yield");
    return r.value;
  }
  __exit__(exc_type, exc, _tb) {
    if (exc_type === null || exc_type === undefined) {
      const r = this._gen.next();
      if (!r.done) throw new RuntimeError("generator didn't stop");
      return false;
    }
    let r;
    try {
      r = this._gen.throw(exc);
    } catch (e) {
      if (e === exc) return false; // re-raised unchanged -> propagate
      throw e;
    }
    if (!r.done) throw new RuntimeError("generator didn't stop after throw()");
    return true; // the generator swallowed the exception
  }
  enter() {
    return this.__enter__();
  }
  exit(...a) {
    return this.__exit__(...a);
  }
  [Symbol.dispose]() {
    this.__exit__(null, null, null);
  }
}

/**
 * ``@contextlib.contextmanager``: wrap a generator function so each call
 * returns a context manager.
 */
export function contextmanager(genfn) {
  const factory = function (...args) {
    return new _GeneratorContextManager(genfn.apply(this, args));
  };
  Object.defineProperty(factory, "name", { value: genfn.name });
  return factory;
}

class _AsyncGeneratorContextManager {
  constructor(agen) {
    this._gen = agen;
  }
  async __aenter__() {
    const r = await this._gen.next();
    if (r.done) throw new RuntimeError("generator didn't yield");
    return r.value;
  }
  async __aexit__(exc_type, exc, _tb) {
    if (exc_type === null || exc_type === undefined) {
      const r = await this._gen.next();
      if (!r.done) throw new RuntimeError("generator didn't stop");
      return false;
    }
    let r;
    try {
      r = await this._gen.throw(exc);
    } catch (e) {
      if (e === exc) return false; // re-raised unchanged -> propagate
      throw e;
    }
    if (!r.done) throw new RuntimeError("generator didn't stop after athrow()");
    return true; // the generator swallowed the exception
  }
  async [Symbol.asyncDispose]() {
    await this.__aexit__(null, null, null);
  }
}

/**
 * ``@contextlib.asynccontextmanager``: wrap an async generator function so
 * each call returns an async context manager (``with_async(cm, body)``).
 */
export function asynccontextmanager(agenfn) {
  const factory = function (...args) {
    return new _AsyncGeneratorContextManager(agenfn.apply(this, args));
  };
  Object.defineProperty(factory, "name", { value: agenfn.name });
  return factory;
}

/** ``contextlib.ExitStack`` (synchronous). */
export class ExitStack {
  constructor() {
    this._callbacks = [];
  }
  enter_context(cm) {
    const r = cm.__enter__();
    this._callbacks.push((t, e, tb) => cm.__exit__(t, e, tb));
    return r;
  }
  push(exit_fn) {
    this._callbacks.push(exit_fn);
    return exit_fn;
  }
  callback(fn, ...args) {
    this._callbacks.push(() => {
      fn(...args);
      return false;
    });
    return fn;
  }
  pop_all() {
    const s = new ExitStack();
    s._callbacks = this._callbacks;
    this._callbacks = [];
    return s;
  }
  close() {
    this.__exit__(null, null, null);
  }
  __enter__() {
    return this;
  }
  __exit__(exc_type, exc, tb) {
    let suppressed = false;
    let pending = exc_type !== null && exc_type !== undefined ? exc : null;
    while (this._callbacks.length) {
      const cb = this._callbacks.pop();
      try {
        const args = pending !== null ? exc_info(pending) : [null, null, null];
        if (cb(...args) === true) {
          pending = null;
          suppressed = true;
        }
      } catch (e) {
        pending = e;
        suppressed = false;
      }
    }
    if (pending !== null && pending !== exc) throw pending;
    return suppressed;
  }
  enter() {
    return this.__enter__();
  }
  exit(...a) {
    return this.__exit__(...a);
  }
  [Symbol.dispose]() {
    this.close();
  }
}

/** ``contextlib.suppress(*exceptions)`` */
export function suppress(...exceptions) {
  return {
    __enter__() {
      return null;
    },
    __exit__(exc_type, exc) {
      if (exc_type === null || exc_type === undefined) return false;
      return exceptions.some((E) => exc instanceof E);
    },
    [Symbol.dispose]() {},
  };
}

/** ``contextlib.nullcontext(enter_result=None)`` */
export function nullcontext(enter_result = null) {
  return {
    __enter__() {
      return enter_result;
    },
    __exit__() {
      return false;
    },
    async __aenter__() {
      return enter_result;
    },
    async __aexit__() {
      return false;
    },
    [Symbol.dispose]() {},
  };
}
