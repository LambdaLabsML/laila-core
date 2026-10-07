/**
 * Python ``queue`` module: ``Queue``, ``PriorityQueue``, ``LifoQueue``,
 * ``Empty``, ``Full``. Blocking ``get`` / ``put`` pump the event loop like the
 * other threading primitives; the ``*_async`` forms are coroutine-friendly.
 */
import { PyException } from "./errors.js";
import { blocking_wait } from "./threading.js";
import { compare } from "./pytypes.js";

export class Empty extends PyException {}
export class Full extends PyException {}

export class Queue {
  constructor(maxsize = 0) {
    this.maxsize = maxsize;
    this.queue = [];
    this._unfinished = 0;
  }
  _put(item) {
    this.queue.push(item);
  }
  _get() {
    return this.queue.shift();
  }
  _qsize() {
    return this.queue.length;
  }
  qsize() {
    return this._qsize();
  }
  empty() {
    return this._qsize() === 0;
  }
  full() {
    return this.maxsize > 0 && this._qsize() >= this.maxsize;
  }
  /** ``put(item, block=True, timeout=None)`` */
  put(item, block = true, timeout = null) {
    if (this.maxsize > 0 && this.full()) {
      if (!block) throw new Full();
      if (!blocking_wait(() => !this.full(), timeout)) throw new Full();
    }
    this._put(item);
    this._unfinished += 1;
  }
  put_nowait(item) {
    return this.put(item, false);
  }
  /** ``get(block=True, timeout=None)`` */
  get(block = true, timeout = null) {
    if (this.empty()) {
      if (!block) throw new Empty();
      if (!blocking_wait(() => !this.empty(), timeout)) throw new Empty();
    }
    return this._get();
  }
  get_nowait() {
    return this.get(false);
  }
  async get_async(timeout = null) {
    const deadline = timeout === null ? null : Date.now() + timeout * 1000;
    while (this.empty()) {
      if (deadline !== null && Date.now() >= deadline) throw new Empty();
      await new Promise((r) => setTimeout(r, 1));
    }
    return this._get();
  }
  task_done() {
    if (this._unfinished <= 0) throw new Error("task_done() called too many times");
    this._unfinished -= 1;
  }
  join() {
    blocking_wait(() => this._unfinished === 0, null);
  }
}

export class LifoQueue extends Queue {
  _get() {
    return this.queue.pop();
  }
}

/** Binary heap ordered by Python comparison of the items (tuples compare lexicographically). */
export class PriorityQueue extends Queue {
  _put(item) {
    const q = this.queue;
    q.push(item);
    let i = q.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (compare(q[i], q[parent]) < 0) {
        [q[i], q[parent]] = [q[parent], q[i]];
        i = parent;
      } else break;
    }
  }
  _get() {
    const q = this.queue;
    const top = q[0];
    const last = q.pop();
    if (q.length) {
      q[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < q.length && compare(q[l], q[m]) < 0) m = l;
        if (r < q.length && compare(q[r], q[m]) < 0) m = r;
        if (m === i) break;
        [q[i], q[m]] = [q[m], q[i]];
        i = m;
      }
    }
    return top;
  }
}
