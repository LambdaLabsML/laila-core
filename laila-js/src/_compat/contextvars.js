/**
 * Python ``contextvars`` over ``AsyncLocalStorage`` plus the notion of a
 * "current thread" for the single-threaded port.
 *
 * A Python *thread* maps to a JS *execution context*: the main synchronous
 * flow (no store), or an ``AsyncLocalStorage`` store entered by a taskforce
 * task / ``Thread.start()``. Everything that Python keys by thread identity
 * (RLock ownership, ``threading.local``, ``_CURRENT_SLOT`` ...) keys by the
 * current context here.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { LookupError, RuntimeError, ValueError } from "./errors.js";

export const _als = new AsyncLocalStorage();

let _next_ident = 1;

/**
 * One Python-"thread"-like execution context.
 *
 * Every context has its own ``ident``. A context also belongs to a *thread*:
 * by default it is a fresh thread of its own (``Thread.start()``, executor
 * hops), but ``{same_thread: true}`` makes it a *task* on the parent's thread
 * (``asyncio.create_task``): it then shares the parent's ``thread_ident`` and
 * ``threading.local`` storage, exactly like asyncio tasks that interleave on
 * one OS thread in Python.
 */
export class Context {
  constructor(parent = null, name = null, opts = {}) {
    this.ident = _next_ident++;
    this.vars = new Map(parent ? parent.vars : []);
    if (parent && opts.same_thread) {
      this.thread_ident = parent.thread_ident;
      this.thread_root = parent.thread_root;
    } else {
      this.thread_ident = this.ident;
      this.thread_root = this;
    }
    this.locals = new Map(); // threading.local storage, keyed by local() instance
    this.name = name ?? (parent ? `Thread-${this.ident}` : "MainThread");
  }
  /** ``Context.run(callable, *args)`` */
  run(fn, ...args) {
    return _als.run(this, fn, ...args);
  }
  /**
   * ``Context.run`` the way CPython does it: *this* context's variables but
   * the *calling thread's* identity, name and ``threading.local`` storage
   * (``ctx.run(fn)`` on an executor thread does not change which thread runs).
   */
  run_on_current_thread(fn, ...args) {
    const cur = current_context();
    const c = new Context(cur, cur.name, { same_thread: true });
    c.vars = this.vars;
    c.locals = cur.locals;
    return c.run(fn, ...args);
  }
  /** Mapping protocol (``ctx[var]``, ``var in ctx``). */
  get(v, dflt = null) {
    return this.vars.has(v) ? this.vars.get(v) : dflt;
  }
  has(v) {
    return this.vars.has(v);
  }
  items() {
    return [...this.vars.entries()];
  }
}

const _MAIN = new Context(null, "MainThread");
_MAIN.ident = 0;
_MAIN.thread_ident = 0;

/** The current context (main when no store is active). */
export function current_context() {
  return _als.getStore() ?? _MAIN;
}

/**
 * Python ``threading.get_ident()`` analogue: the identity of the current
 * *thread* (shared by every asyncio task interleaving on it).
 */
export function get_ident() {
  return current_context().thread_ident;
}

export function main_context() {
  return _MAIN;
}

/** ``contextvars.copy_context()`` -> a new Context with a copy of the vars. */
export function copy_context() {
  return new Context(current_context());
}

/**
 * Run ``fn`` in a *fresh* context (new identity, vars copied from the
 * current one, like ``asyncio.create_task`` / ``Thread.start``).
 */
export function run_in_new_context(fn, ...args) {
  return new Context(current_context()).run(fn, ...args);
}

const _NO_DEFAULT = Symbol("contextvars.NO_DEFAULT");

export class Token {
  constructor(v, old) {
    this.var = v;
    this.old_value = old;
    this._used = false;
  }
  static get MISSING() {
    return _NO_DEFAULT;
  }
}

/** ``contextvars.ContextVar`` */
export class ContextVar {
  constructor(name, opts = {}) {
    if (typeof name !== "string") throw new ValueError("context variable name must be a str");
    this.name = name;
    this._default = "default" in opts ? opts.default : _NO_DEFAULT;
  }
  get(dflt = _NO_DEFAULT) {
    const ctx = current_context();
    if (ctx.vars.has(this)) return ctx.vars.get(this);
    if (dflt !== _NO_DEFAULT) return dflt;
    if (this._default !== _NO_DEFAULT) return this._default;
    throw new LookupError(this);
  }
  set(value) {
    const ctx = current_context();
    const old = ctx.vars.has(this) ? ctx.vars.get(this) : _NO_DEFAULT;
    ctx.vars.set(this, value);
    return new Token(this, old);
  }
  reset(token) {
    if (!(token instanceof Token) || token.var !== this) throw new ValueError("Token was created by a different ContextVar");
    if (token._used) throw new RuntimeError("Token has already been used once");
    token._used = true;
    const ctx = current_context();
    if (token.old_value === _NO_DEFAULT) ctx.vars.delete(this);
    else ctx.vars.set(this, token.old_value);
  }
  __repr__() {
    return `<ContextVar name='${this.name}'>`;
  }
}
