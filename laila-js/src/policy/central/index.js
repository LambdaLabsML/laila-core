/**
 * Central sub-systems of a Laila policy.
 *
 * Each policy owns a small bundle of "central" subsystems that handle
 * concerns that cut across an entire policy:
 *
 * - ``command`` -- task-force registry, work submission, futures, and
 *   shutdown coordination.
 * - ``memory`` -- the ``memorize`` / ``remember`` / ``forget`` API, the pool
 *   router, and ``Manifest`` / ``Hint`` machinery.
 * - ``communication`` -- transport protocols, peer registry, and the
 *   inbound/outbound RPC dispatch used by remote policy proxies.
 * - ``logic`` (placeholder) -- reserved for higher-level orchestration
 *   hooks; not yet implemented.
 *
 * These four are bundled inside ``_LAILA_IDENTIFIABLE_POLICY.Central`` on
 * every policy instance.
 */

export * as command from "./command/index.js";
export * as memory from "./memory/index.js";
export * as communication from "./communication/index.js";
