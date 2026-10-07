/**
 * Module-level picklable tasks for the process-pool taskforce tests (the
 * Python tests define these at the top of the test module; a Node worker
 * must import them from a module that does *not* run the tests).
 */
export function _return_1() {
  return 1;
}
export function _return_2() {
  return 2;
}
export function _return_3() {
  return 3;
}
export function _return_7() {
  return 7;
}
export function _return_a() {
  return "a";
}
export function _return_b() {
  return "b";
}
export function _return_10() {
  return 10;
}
export function _return_20() {
  return 20;
}

for (const fn of [_return_1, _return_2, _return_3, _return_7, _return_a, _return_b, _return_10, _return_20]) {
  fn.__module__ = import.meta.url;
  fn.__qualname__ = fn.name;
}
