/**
 * ``laila`` root for the memory / data / communication test suites.
 *
 * Re-exports the real top-level module (``src/index.js``); importing it
 * registers ``lazy("laila")`` so laila's own internals resolve the active
 * policy, ``laila.args`` and the memory shims exactly as user code does.
 * ``S`` is the ``src/`` URL prefix used by the suites to import internals.
 */
export const S = new URL("../../../src/", import.meta.url).href;

import LAILA from "../../../src/index.js";

export default LAILA;
export { LAILA as laila };
